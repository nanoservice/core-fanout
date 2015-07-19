package fanout

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type Message struct {
	Value []byte
}

type Consumer struct {
	fanouts    []string
	instanceId string
	messages   chan Message
}

type listenState struct {
	state    int
	sizeLeft int
}

const (
	STATE_WAIT_SIZE     = 0
	STATE_WAIT_VALUE    = 1
	CHANNEL_BUFFER_SIZE = 100
)

func NewConsumer(fanouts []string, instanceId string) (Consumer, error) {
	consumer := Consumer{
		fanouts:    fanouts,
		instanceId: instanceId,
		messages:   make(chan Message, CHANNEL_BUFFER_SIZE),
	}

	return consumer, consumer.listen()
}

func (c Consumer) Subscribe(fn func(raw Message)) {
	go func() {
		for message := range c.messages {
			fn(message)
		}
	}()
}

func (c Consumer) listen() error {
	conn, err := net.Dial("tcp", c.fanouts[0])
	if err != nil {
		fmt.Println("Unable to connect to fanout :(")
		return err
	}

	fmt.Fprintf(conn, "%s\n", c.instanceId)

	go func() {
		defer conn.Close()
		var autoReRead func(fn func() error) error

		data := make([]byte, 4096)
		state := listenState{STATE_WAIT_SIZE, 0}

		for {
			n, err := conn.Read(data)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				return
			}

			reader := bytes.NewBuffer(data[0:n])

			eobError := errors.New("short buffer")
			autoReRead = func(fn func() error) error {
				bytesBefore := reader.Bytes()

				if fn() == nil {
					return nil
				}

				n, err := conn.Read(data)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					return err
				}

				reader = bytes.NewBuffer(
					append(bytesBefore, data[0:n]...),
				)

				return autoReRead(fn)
			}

			if state.state == STATE_WAIT_SIZE {
				autoReRead(func() error {
					return binary.Read(reader, binary.LittleEndian, &state.sizeLeft)
				})
				state.state = STATE_WAIT_VALUE
			} else if state.state == STATE_WAIT_VALUE {
				autoReRead(func() error {
					if reader.Len() < state.sizeLeft {
						return eobError
					}
					return nil
				})
				readData := reader.Next(state.sizeLeft)
				c.messages <- Message{readData}
			}
		}
	}()

	return nil
}
