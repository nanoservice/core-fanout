package fanout

import (
	"fmt"
	kafka "github.com/Shopify/sarama"
	_ "github.com/golang/protobuf/proto"
	"os"
)

var (
	kafkas = []string{
		os.Getenv("KAFKA_1_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_1_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_2_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_2_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_3_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_3_PORT_9092_TCP_PORT"),
	}
)

type Message struct {
	Value []byte
}

type Consumer struct {
	fanouts   []string
	serviceId string
	topic     string
	consumer  kafka.PartitionConsumer
}

func NewConsumer(fanouts []string, serviceId string, topic string) (Consumer, error) {
	config := kafka.NewConfig()

	masterConsumer, err := kafka.NewConsumer(kafkas, config)
	if err != nil {
		return Consumer{}, err
	}

	consumer, err := masterConsumer.ConsumePartition(topic, 0, kafka.OffsetNewest)
	if err != nil {
		return Consumer{}, err
	}

	return Consumer{
		fanouts:   fanouts,
		serviceId: serviceId,
		topic:     topic,
		consumer:  consumer,
	}, nil
}

func (c Consumer) Subscribe(fn func(raw Message)) {
	go func() {
		for message := range c.consumer.Messages() {
			fn(Message{message.Value})
		}
	}()
}
