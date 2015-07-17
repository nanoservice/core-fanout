package fanout

import (
	_ "github.com/golang/protobuf/proto"
)

type Message struct {
	Value []byte
}

type Consumer struct {
	fanouts   []string
	serviceId string
	topic     string
}

func NewConsumer(fanouts []string, serviceId string, topic string) (Consumer, error) {
	return Consumer{
		fanouts:   fanouts,
		serviceId: serviceId,
		topic:     topic,
	}, nil
}

func (c Consumer) Subscribe(fn func(raw Message)) {}
