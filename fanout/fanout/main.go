package main

import (
	"flag"
	kafka "github.com/Shopify/sarama"
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/nanoservice/core-fanout/fanout/log"
	"github.com/nanoservice/core-fanout/fanout/messages"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

type clientInbox struct {
	inbox chan messages.Message
	count int
}

type roundRobinT struct {
	clients []clientInbox
	next    int
	mux     *sync.Mutex
}

var (
	clients    = make(map[string]clientInbox)
	roundRobin = roundRobinT{
		clients: make([]clientInbox, 0),
		next:    0,
		mux:     &sync.Mutex{},
	}
	blackHole = clientInbox{make(chan messages.Message), 1}
	acks      = make(map[messages.MessageAck]chan bool)

	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to a file")
)

var (
	kafkas = []string{
		os.Getenv("KAFKA_1_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_1_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_2_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_2_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_3_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_3_PORT_9092_TCP_PORT"),
	}

	myId  = ""
	topic = ""
)

const (
	CHANNEL_BUFFER_SIZE = 100
)

func main() {
	flag.StringVar(&myId, "id", "", "cluster id, typically a consuming service name")
	flag.StringVar(&topic, "topic", "", "topic to consume")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Printf("Unable to create cpuprofile file: %v, moving on\n", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	server, err := comm.Listen(":4987")
	if err != nil {
		log.Printf("Unable to listen on port :4987: %v", err)
		os.Exit(1)
	}

	_, consumers := newConsumer()

	for _, consumer := range consumers {
		go handleConsumer(consumer)
	}

	go func() {
		for message := range blackHole.inbox {
			log.V(2).Printf("Got message in blackhole: %v\n", message)
			go func(message messages.Message) {
				log.V(2).Printf("Gonna re-send message from blackhole: %v\n", message)
				nextRoundRobinClient().inbox <- message
			}(message)
		}
	}()

	log.Println("Listening on port :4987")

	for {
		conn, err := server.Accept()
		if err != nil {
			log.Printf("Unable to accept connection: %v", err)
			continue
		}

		go handleClient(conn)
	}
}

func handleConsumer(consumer kafka.PartitionConsumer) {
	for message := range consumer.Messages() {
		log.V(2).Printf("Got message: %v\n", message)
		nextRoundRobinClient().inbox <- messages.Message{
			Value:     message.Value,
			Partition: message.Partition,
			Offset:    message.Offset,
		}
	}
}

func newConsumer() (masterConsumer kafka.Consumer, consumers []kafka.PartitionConsumer) {
	config := kafka.NewConfig()
	config.Net.KeepAlive = 30 * time.Second
	config.Consumer.Retry.Backoff = 25 * time.Millisecond

	consumers = make([]kafka.PartitionConsumer, 0)

	retry(func() (err error) {
		var consumer kafka.PartitionConsumer
		var partitions []int32

		masterConsumer, err = kafka.NewConsumer(kafkas, config)
		if err != nil {
			return
		}

		partitions, err = masterConsumer.Partitions(topic)
		if err != nil {
			return
		}

		for _, partition := range partitions {
			consumer, err = masterConsumer.ConsumePartition(topic, partition, kafka.OffsetNewest)
			if err != nil {
				return
			}

			consumers = append(consumers, consumer)
		}
		return
	})

	return
}

func retry(fn func() error) error {
	return retryCustom(10, 1*time.Second, fn)
}

func retryCustom(times int, interval time.Duration, fn func() error) (err error) {
	for i := 0; i < times; i++ {
		err = fn()
		if err == nil {
			return
		}
		time.Sleep(interval)
	}
	return
}

func rebuildRoundRobin() {
	roundRobin.clients = make([]clientInbox, 0)
	for _, v := range clients {
		roundRobin.clients = append(roundRobin.clients, v)
	}
}

func addClient(instanceId string, client clientInbox) {
	roundRobin.mux.Lock()
	if oldClient, found := clients[instanceId]; found {
		client.count += oldClient.count
	}
	clients[instanceId] = client
	rebuildRoundRobin()
	roundRobin.mux.Unlock()
}

func clientDead(instanceId string) {
	roundRobin.mux.Lock()
	if client, found := clients[instanceId]; found {
		client.count -= 1
		if client.count < 1 {
			delete(clients, instanceId)
			rebuildRoundRobin()
		}
	}
	roundRobin.mux.Unlock()
}

func nextRoundRobinClient() (client clientInbox) {
	if len(roundRobin.clients) == 0 {
		return blackHole
	}

	roundRobin.mux.Lock()
	client = roundRobin.clients[roundRobin.next%len(roundRobin.clients)]
	roundRobin.next = (roundRobin.next + 1) % len(roundRobin.clients)
	roundRobin.mux.Unlock()
	return
}

func handleClient(stream *comm.Stream) {
	defer stream.Close()
	var instanceId string

	instanceId, err := stream.ReadLine()
	if err != nil {
		log.Printf("Unable to identify client: %v\n", err)
		return
	}

	if instanceId == "-PING\n" {
		log.V(1).Printf("got ping: -PING; answering with: +PONG\n")
		err = stream.WriteLine("+PONG")
		if err != nil {
			log.Printf("Unable to answer with +PONG: %v\n", err)
		}
		return
	}

	log.V(1).Printf("got client: %s\n", instanceId)

	inbox := make(chan messages.Message, CHANNEL_BUFFER_SIZE)
	client := clientInbox{
		inbox: inbox,
		count: 1,
	}
	addClient(instanceId, client)

	ackErrors := make(chan error)

	go func() {
		for {
			var ack messages.MessageAck
			err = stream.ReadMessage(&ack)
			if err != nil {
				ackErrors <- err
				continue
			}

			if _, found := acks[ack]; found {
				log.V(2).Printf("Got ack: %v\n", ack)
				acks[ack] <- true
			}
		}
	}()

	dead := make(chan bool)
	for {
		select {
		case message := <-inbox:
			go func(message messages.Message, dead chan bool) {
				err := stream.WriteMessage(&message)
				if err != nil {
					log.Printf("Unable to send message to client: %v\n", err)
					dead <- true
					return
				}

				expectedAck := messages.MessageAck{
					Partition: message.Partition,
					Offset:    message.Offset,
				}
				acks[expectedAck] = make(chan bool)

				go func() {
					select {
					case _ = <-acks[expectedAck]:
						break
					case <-time.After(50 * time.Millisecond):
						blackHole.inbox <- message
						dead <- true
					}
					delete(acks, expectedAck)
				}()
			}(message, dead)

		case <-ackErrors:
		case <-dead:
			clientDead(instanceId)
		}
	}
}
