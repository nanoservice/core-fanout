package fanout

import (
	"errors"
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/nanoservice/core-fanout/fanout/log"
	"github.com/nanoservice/core-fanout/fanout/messages"
)

type Consumer struct {
	SendAcks bool

	fanouts    []string
	instanceId string
	messages   chan messages.Message
	stream     *comm.Stream
	closeChan  chan bool
}

const (
	CHANNEL_BUFFER_SIZE = 100
)

var (
	NoPongFromServer = errors.New("No +PONG from server")
)

func Ping(fanouts []string) error {
	var response string

	stream, err := comm.Dial(fanouts[0])
	if err != nil {
		log.Printf("Unable to create stream: %v\n", err)
		return err
	}

	defer stream.Close()

	err = stream.WriteLine("-PING")
	if err != nil {
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

func (c *Consumer) Close() {
	go func() { c.closeChan <- true }()
}

func (c *Consumer) listen() error {
	go func() {
		var err error
		c.stream, err = comm.Dial(c.fanouts[0])
		if err != nil {
			log.Printf("Unable to obtain stream: %v\n", err)
			return
		}

		defer c.stream.Close()

		err = c.stream.WriteLine(c.instanceId)
		if err != nil {
			log.Printf("Unable to send own instance id: %v\n", err)
			return
		}

		for {
			select {
			case _, ok := <-c.closeChan:
				if ok {
					return
				}
			default:
			}

			var message messages.Message
			err := c.stream.ReadMessage(&message)
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

					err := c.stream.WriteMessage(ack)
					if err != nil {
						log.Printf("Unable to send message ack: %v\n", err)
					}

					log.V(2).Printf("Sent ack from %s: %v\n", c.instanceId, *ack)
				}
			}(message)

			if !message.IsAckRequest {
				c.messages <- message
			}
		}
	}()

	return nil
}
