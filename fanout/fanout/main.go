package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	kafka "github.com/Shopify/sarama"
	"net"
	"os"
	"sync"
	"time"
)

type Message struct {
	value []byte
}

type clientInbox struct {
	inbox chan Message
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
	blackHole = clientInbox{make(chan Message)}
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
		fmt.Printf("Unable to listen on port :4987: %v", err)
		os.Exit(1)
	}

	_, consumers := newConsumer()

	for _, consumer := range consumers {
		go handleConsumer(consumer)
	}

	go func() {
		for message := range blackHole.inbox {
			fmt.Printf("Got message in blackhole: %v\n", message)
		}
	}()

	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Printf("Unable to accept connection: %v", err)
			continue
		}

		go handleClient(conn)
	}
}

func handleConsumer(consumer kafka.PartitionConsumer) {
	for message := range consumer.Messages() {
		fmt.Printf("Got message: %v\n", message)
		nextRoundRobinClient().inbox <- Message{message.Value}
	}
}

func newConsumer() (masterConsumer kafka.Consumer, consumers []kafka.PartitionConsumer) {
	topic := "test_topic"
	config := kafka.NewConfig()
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

func addClient(instanceId string, client clientInbox) {
	roundRobin.mux.Lock()
	clients[instanceId] = client
	roundRobin.clients = make([]clientInbox, 0)
	for _, v := range clients {
		roundRobin.clients = append(roundRobin.clients, v)
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
	var autoReRead func(fn func() error) error
	var instanceId string

	data := make([]byte, 4096)

	n, err := conn.Read(data)
	if err != nil {
		return
	}

	reader := bytes.NewBuffer(data[0:n])

	autoReRead = func(fn func() error) error {
		bytesBefore := reader.Bytes()

		err := fn()
		if err == nil {
			return nil
		}

		fmt.Printf("Re-reading, cause: %v\n", err)

		n, err = conn.Read(data)
		if err != nil {
			fmt.Printf("Unable to read, cause: %v\n", err)
			return err
		}

		reader = bytes.NewBuffer(
			append(bytesBefore, data[0:n]...),
		)

		return autoReRead(fn)
	}

	autoReRead(func() (err error) {
		instanceId, err = reader.ReadString(byte('\n'))
		fmt.Printf("got client: %s\n", instanceId)
		return
	})

	inbox := make(chan Message, CHANNEL_BUFFER_SIZE)
	client := clientInbox{
		inbox: inbox,
	}
	addClient(instanceId, client)

	for {
		for message := range inbox {
			go func(message Message) {
				buf := new(bytes.Buffer)

				var size int32 = int32(len(message.value))
				err := binary.Write(buf, binary.LittleEndian, size)
				if err != nil {
					fmt.Printf("Unable to dump message size to buffer: %v\n", err)
					return
				}

				_, err = buf.Write(message.value)
				if err != nil {
					fmt.Printf("Unable to dump raw message to buffer: %v\n", err)
					return
				}

				fmt.Printf("Gonna send message: %v\n", buf.Bytes())
				_, err = buf.WriteTo(conn)
				if err != nil {
					fmt.Printf("Unable to write to client connection: %v\n", err)
					return
				}
			}(message)
		}
	}
}
