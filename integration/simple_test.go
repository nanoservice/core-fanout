// +build integration

package integration

import (
	kafka "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	fanout "github.com/nanoservice/core-fanout/client"
	"github.com/nanoservice/core-fanout/integration/userneed"
	"os"
	"reflect"
	"testing"
	"time"
)

var (
	messageA = &userneed.UserNeed{
		NeedId: 7,
		UserId: 17,
		Sender: "test_producer",
	}

	messageB = &userneed.UserNeed{
		NeedId: 9,
		UserId: 3,
		Sender: "test_producer",
	}

	messageC = &userneed.UserNeed{
		NeedId: 84,
		UserId: 94743,
		Sender: "another_test_producer",
	}

	kafkas = []string{
		os.Getenv("KAFKA_1_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_1_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_2_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_2_PORT_9092_TCP_PORT"),
		os.Getenv("KAFKA_3_PORT_9092_TCP_ADDR") + ":" + os.Getenv("KAFKA_3_PORT_9092_TCP_PORT"),
	}

	fanouts = []string{
		os.Getenv("FANOUT_1_PORT_4987_TCP_ADDR") + ":" + os.Getenv("FANOUT_1_PORT_4987_TCP_PORT"),
		os.Getenv("FANOUT_2_PORT_4987_TCP_ADDR") + ":" + os.Getenv("FANOUT_2_PORT_4987_TCP_PORT"),
		os.Getenv("FANOUT_3_PORT_4987_TCP_ADDR") + ":" + os.Getenv("FANOUT_3_PORT_4987_TCP_PORT"),
	}
)

func TestOneConsumer(t *testing.T) {
	inbox := subscriptionInbox(t)
	producer := newProducer()

	producer.Publish(messageA)
	producer.Publish(messageB)
	producer.Publish(messageC)

	assertReceived(t, inbox, messageA)
	assertReceived(t, inbox, messageB)
	assertReceived(t, inbox, messageC)
}

func subscriptionInbox(t *testing.T) (inbox chan *userneed.UserNeed) {
	inbox = make(chan *userneed.UserNeed)

	newConsumer().Subscribe(func(raw fanout.Message) {
		message := &userneed.UserNeed{}
		err := proto.Unmarshal(raw.Value, message)
		if err != nil {
			t.Errorf("Unmarshal error: %v", err)
		}
		inbox <- message
	})

	return
}

func assertReceived(t *testing.T, inbox chan *userneed.UserNeed, expected *userneed.UserNeed) {
	select {
	case actual := <-inbox:
		if !reflect.DeepEqual(*actual, *expected) {
			t.Errorf("Expected %v to equal to %v", *actual, *expected)
		}

	case <-time.After(50 * time.Millisecond):
		t.Errorf("Expected to receive %v, got nothing", *expected)
	}
}

type AsyncProducer struct {
	delegatee kafka.AsyncProducer
}

func (producer AsyncProducer) Publish(message proto.Message) {
	topic := "test_topic"
	raw, _ := proto.Marshal(message)
	producer.delegatee.Input() <- &kafka.ProducerMessage{
		Topic: topic,
		Value: kafka.ByteEncoder(raw),
	}
}

func newProducer() AsyncProducer {
	var producer kafka.AsyncProducer
	config := kafka.NewConfig()

	_ = retry(func() (err error) {
		producer, err = kafka.NewAsyncProducer(kafkas, config)
		return
	})

	return AsyncProducer{producer}
}

func newConsumer() (consumer fanout.Consumer) {
	_ = retry(func() (err error) {
		consumer, err = fanout.NewConsumer(fanouts, "test_service", "test_topic")
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
