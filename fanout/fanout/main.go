package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	kafka "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/nanoservice/core-fanout/fanout/messages"
	"log"
	"net"
	"os"
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
)

var (
	kafkas = []string{
		os.Getenv("KAFKA_1_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_1_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_2_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_2_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_3_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_3_PORT_9092_TCP_PORT"),
	}
)

const (
	CHANNEL_BUFFER_SIZE = 100
)

func main() {
	server, err := net.Listen("tcp", ":4987")
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
			log.Printf("Got message in blackhole: %v\n", message)
			go func(message messages.Message) {
				log.Printf("Gonna re-send message from blackhole: %v\n", message)
				nextRoundRobinClient().inbox <- message
			}(message)
		}
	}()

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
		log.Printf("Got message: %v\n", message)
		nextRoundRobinClient().inbox <- messages.Message{
			Value:     message.Value,
			Partition: message.Partition,
			Offset:    message.Offset,
		}
	}
}

func newConsumer() (masterConsumer kafka.Consumer, consumers []kafka.PartitionConsumer) {
	topic := "test_topic"
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

func handleClient(conn net.Conn) {
	defer conn.Close()
	var instanceId string

	stream, err := comm.NewStream(conn)
	if err != nil {
		log.Printf("Unable to create stream: %v\n", err)
		return
	}

	stream.ReadWith(func() (err error) {
		instanceId, err = stream.Reader.ReadString(byte('\n'))
		return
	})

	if instanceId == "-PING\n" {
		log.Printf("got ping: -PING; answering with: +PONG\n")
		fmt.Fprint(conn, "+PONG\n")
		return
	}

	log.Printf("got client: %s\n", instanceId)

	inbox := make(chan messages.Message, CHANNEL_BUFFER_SIZE)
	client := clientInbox{
		inbox: inbox,
		count: 1,
	}
	addClient(instanceId, client)

	ackErrors := make(chan error)

	go func() {
		for {
			var messageSize int32
			err := stream.ReadWith(func() error {
				return binary.Read(stream.Reader, binary.LittleEndian, &messageSize)
			})
			if err != nil {
				ackErrors <- err
				continue
			}

			err = stream.ReadWith(func() error {
				if int32(stream.Reader.Len()) < messageSize {
					return comm.EOFError
				}
				return nil
			})
			if err != nil {
				ackErrors <- err
				continue
			}

			rawAck := stream.Reader.Next(int(messageSize))
			if err != nil {
				ackErrors <- err
				continue
			}

			var ack messages.MessageAck
			err = proto.Unmarshal(rawAck, &ack)
			if err != nil {
				ackErrors <- err
				continue
			}

			if _, found := acks[ack]; found {
				log.Printf("Got ack: %v\n", ack)
				acks[ack] <- true
			}
		}
	}()

	dead := make(chan bool)
	for {
		select {
		case message := <-inbox:
			go func(message messages.Message, dead chan bool) {
				buf := new(bytes.Buffer)
				rawMessage, err := proto.Marshal(&message)
				if err != nil {
					log.Printf("Unable to marshal message: %v\n", err)
					return
				}

				var size int32 = int32(len(rawMessage))
				err = binary.Write(buf, binary.LittleEndian, size)
				if err != nil {
					log.Printf("Unable to dump message size to buffer: %v\n", err)
					dead <- true
					return
				}

				_, err = buf.Write(rawMessage)
				if err != nil {
					log.Printf("Unable to dump raw message to buffer: %v\n", err)
					dead <- true
					return
				}

				log.Printf("Gonna send message: %v\n", buf.Bytes())
				_, err = buf.WriteTo(conn)
				if err != nil {
					log.Printf("Unable to write to client connection: %v\n", err)
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
