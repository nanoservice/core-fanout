package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	kafka "github.com/Shopify/sarama"
	"net"
	"os"
	"time"
)

type Message struct {
	value []byte
}

type clientInbox chan Message

var (
	clients = make(map[string]clientInbox)
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

	consumer := newConsumer()

	go func() {
		for message := range consumer.Messages() {
			for _, v := range clients {
				v <- Message{message.Value}
			}
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

func newConsumer() (consumer kafka.PartitionConsumer) {
	config := kafka.NewConfig()

	retry(func() (err error) {
		var masterConsumer kafka.Consumer

		masterConsumer, err = kafka.NewConsumer(kafkas, config)
		if err != nil {
			return
		}

		consumer, err = masterConsumer.ConsumePartition("test_topic", 0, kafka.OffsetNewest)
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

		if fn() == nil {
			return nil
		}

		n, err := conn.Read(data)
		if err != nil {
			return err
		}

		reader = bytes.NewBuffer(
			append(bytesBefore, data[0:n]...),
		)

		return autoReRead(fn)
	}

	autoReRead(func() (err error) {
		instanceId, err = reader.ReadString(byte('\n'))
		return
	})

	inbox := make(chan Message, CHANNEL_BUFFER_SIZE)
	clients[instanceId] = inbox

	for {
		for message := range inbox {
			go func() {
				buf := new(bytes.Buffer)

				var size int = len(message.value)
				err := binary.Write(buf, binary.LittleEndian, size)
				if err != nil {
					fmt.Println("Unable to dump message size to buffer")
					return
				}

				_, err = buf.Write(message.value)
				if err != nil {
					fmt.Println("Unable to dump raw message to buffer")
					return
				}
			}()
		}
	}
}
