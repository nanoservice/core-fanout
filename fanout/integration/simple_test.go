// +build integration

package integration

import (
	"fmt"
	kafka "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	fanout "github.com/nanoservice/core-fanout/fanout/client"
	"github.com/nanoservice/core-fanout/fanout/integration/userneed"
	"github.com/nanoservice/core-fanout/fanout/messages"
	"os"
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
	inbox := subscriptionInbox(t, "INSTANCE_0")
	producer := newProducer()

	producer.Publish(messageA)
	producer.Publish(messageB)
	producer.Publish(messageC)

	expected := userNeedSet{
		*messageA: true,
		*messageB: true,
		*messageC: true,
	}

	assertReceived(t, inbox, expected)
	assertReceived(t, inbox, expected)
	assertReceived(t, inbox, expected)
}

func TestMultipleConsumers(t *testing.T) {
	inboxA := subscriptionInbox(t, "INSTANCE_0")
	inboxB := subscriptionInbox(t, "INSTANCE_1")
	producer := newProducer()

	producer.Publish(messageA)
	producer.Publish(messageB)
	producer.Publish(messageC)

	expected := userNeedSet{
		*messageA: true,
		*messageB: true,
		*messageC: true,
	}

	assertReceived(t, inboxA, expected)
	assertReceived(t, inboxB, expected)
	assertReceived(t, inboxA, expected)
}

func subscriptionInbox(t *testing.T, instanceId string) (inbox chan *userneed.UserNeed) {
	inbox = make(chan *userneed.UserNeed)

	newConsumer(instanceId).Subscribe(func(raw messages.Message) {
		message := &userneed.UserNeed{}
		fmt.Printf("Got raw message: %v with id=%d:%d\n", raw.Value, raw.Partition, raw.Offset)
		err := proto.Unmarshal(raw.Value, message)
		if err != nil {
			t.Errorf("Unmarshal error: %v", err)
		}
		inbox <- message
	})

	return
}

type userNeedSet map[userneed.UserNeed]bool

func assertReceived(t *testing.T, inbox chan *userneed.UserNeed, expected userNeedSet) *userneed.UserNeed {
	select {
	case actual := <-inbox:
		if present, found := expected[*actual]; !found || !present {
			t.Errorf("Expected %v to be in %v", *actual, expected)
			return nil
		}
		expected[*actual] = false
		return actual

	case <-time.After(5000 * time.Millisecond):
		t.Errorf("Expected to receive one of %v, got nothing", expected)
	}

	return nil
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

func newConsumer(instanceId string) (consumer fanout.Consumer) {
	_ = retry(func() (err error) {
		consumer, err = fanout.NewConsumer(fanouts, instanceId)
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
		fmt.Printf("Got error: %v, retrying..\n", err)
		time.Sleep(interval)
	}
	fmt.Println("Gave up :(")
	return
}
