package fanout

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/nanoservice/core-fanout/fanout/messages"
	"net"
)

type Consumer struct {
	fanouts    []string
	instanceId string
	messages   chan messages.Message
	SendAcks   bool
}

type listenState struct {
	state    int
	sizeLeft int32
}

const (
	STATE_WAIT_SIZE     = 0
	STATE_WAIT_VALUE    = 1
	CHANNEL_BUFFER_SIZE = 100
)

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
		fmt.Println("Unable to connect to fanout :(")
		return err
	}

	fmt.Fprintf(conn, "%s\n", c.instanceId)

	go func() {
		defer conn.Close()

		stream, err := comm.NewStream(conn)
		if err != nil {
			fmt.Printf("Unable to obtain stream: %v\n", err)
			return
		}

		state := listenState{STATE_WAIT_SIZE, 0}

		for {
			if state.state == STATE_WAIT_SIZE {
				stream.ReadWith(func() error {
					return binary.Read(stream.Reader, binary.LittleEndian, &state.sizeLeft)
				})
				fmt.Printf("Got message size: %d\n", state.sizeLeft)
				state.state = STATE_WAIT_VALUE
			} else if state.state == STATE_WAIT_VALUE {
				stream.ReadWith(func() error {
					if stream.Reader.Len() < int(state.sizeLeft) {
						return comm.EOFError
					}
					return nil
				})
				readData := stream.Reader.Next(int(state.sizeLeft))
				fmt.Printf("Got message: %v\n", readData)
				state.state = STATE_WAIT_SIZE

				var message messages.Message
				err := proto.Unmarshal(readData, &message)
				if err != nil {
					fmt.Printf("Unable to unmarshal message: %v\n", err)
					continue
				}

				go func(message messages.Message) {
					if c.SendAcks {
						buf := new(bytes.Buffer)

						ack := &messages.MessageAck{
							Partition: message.Partition,
							Offset:    message.Offset,
						}

						rawAck, err := proto.Marshal(ack)
						if err != nil {
							fmt.Printf("Unable to marshal message ack: %v\n", err)
							return
						}

						var size int32 = int32(len(rawAck))
						err = binary.Write(buf, binary.LittleEndian, size)
						if err != nil {
							fmt.Printf("Unable to dump message ack size: %v\n", err)
							return
						}

						_, err = buf.Write(rawAck)
						if err != nil {
							fmt.Printf("Unable to dump message ack: %v\n", err)
							return
						}

						_, err = buf.WriteTo(conn)
						if err != nil {
							fmt.Printf("Unable to send message ack: %v\n", err)
						}

						fmt.Printf("Sent ack from %s: %v\n", c.instanceId, rawAck)
					}
				}(message)

				c.messages <- message
			}
		}
	}()

	return nil
}
