package fanout

import (
	"errors"
	"fmt"
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/nanoservice/core-fanout/fanout/messages"
	"log"
	"net"
)

type Consumer struct {
	fanouts    []string
	instanceId string
	messages   chan messages.Message
	SendAcks   bool
}

const (
	CHANNEL_BUFFER_SIZE = 100
)

var (
	NoPongFromServer = errors.New("No +PONG from server")
)

func Ping(fanouts []string) error {
	var response string

	conn, err := net.Dial("tcp", fanouts[0])
	if err != nil {
		log.Println("Unable to connect to fanout :(")
		return err
	}

	fmt.Fprint(conn, "-PING\n")

	stream, err := comm.NewStream(conn)
	if err != nil {
		log.Printf("Unable to create stream: %v\n", err)
		return err
	}

	response, err = stream.ReadLine()
	if err != nil {
		return err
	}

	if response != "+PONG\n" {
		return NoPongFromServer
	}

	return nil
}

func NewConsumer(fanouts []string, instanceId string) (*Consumer, error) {
	consumer := &Consumer{
		fanouts:    fanouts,
		instanceId: instanceId,
		messages:   make(chan messages.Message, CHANNEL_BUFFER_SIZE),
		SendAcks:   true,
	}

	return consumer, consumer.listen()
}

func (c *Consumer) Subscribe(fn func(raw messages.Message)) {
	go func() {
		for message := range c.messages {
			fn(message)
		}
	}()
}

func (c *Consumer) listen() error {
	conn, err := net.Dial("tcp", c.fanouts[0])
	if err != nil {
		log.Println("Unable to connect to fanout :(")
		return err
	}

	fmt.Fprintf(conn, "%s\n", c.instanceId)

	go func() {
		defer conn.Close()

		stream, err := comm.NewStream(conn)
		if err != nil {
			log.Printf("Unable to obtain stream: %v\n", err)
			return
		}

		for {
			var message messages.Message
			err := stream.ReadMessage(&message)
			if err != nil {
				log.Printf("Unable to unmarshal message: %v\n", err)
				continue
			}

			go func(message messages.Message) {
				if c.SendAcks {
					ack := &messages.MessageAck{
						Partition: message.Partition,
						Offset:    message.Offset,
					}

					err := stream.WriteMessage(ack)
					if err != nil {
						log.Printf("Unable to send message ack: %v\n", err)
					}

					log.Printf("Sent ack from %s: %v\n", c.instanceId, *ack)
				}
			}(message)

			c.messages <- message
		}
	}()

	return nil
}
