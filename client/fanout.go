package fanout

import ()

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
	STATE_WAIT_SIZE  = 0
	STATE_WAIT_VALUE = 1
)

func NewConsumer(fanouts []string, instanceId string) (Consumer, error) {
	consumer := Consumer{
		fanouts:    fanouts,
		instanceId: instanceId,
		messages:   make(chan Message),
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
		return err
	}

	go func() {
		defer conn.Close()
		data := make([]byte, 4096)
		state := listenState{STATE_WAIT_SIZE, 0}

		for {
			n, err := conn.Read(data)
			if err != nil {
				return
			}

			if state.state == STATE_WAIT_SIZE {

			} else if state.state == STATE_WAIT_VALUE {

			}
		}
	}()

	return nil
}
