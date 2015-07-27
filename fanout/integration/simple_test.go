// +build integration

package integration

import (
	kafka "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	fanout "github.com/nanoservice/core-fanout/fanout/client"
	"github.com/nanoservice/core-fanout/fanout/integration/userneed"
	"github.com/nanoservice/core-fanout/fanout/log"
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

func TestMain(m *testing.M) {
	err := retry(func() error {
		return fanout.Ping(fanouts)
	})

	if err != nil {
		log.Println("Unable to ping fanout :(")
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestOneConsumer(t *testing.T) {
	log.Printf("===== TestOneConsumer =====")
	inbox, consumer := subscriptionInbox(t, "INSTANCE_0")
	producer := newProducer()

	defer consumer.Close()

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
	log.Printf("===== TestMultipleConsumers =====")
	inboxA, consumerA := subscriptionInbox(t, "INSTANCE_0")
	inboxB, consumerB := subscriptionInbox(t, "INSTANCE_1")
	producer := newProducer()

	defer consumerA.Close()
	defer consumerB.Close()

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

func TestDeadConsumer(t *testing.T) {
	log.Printf("===== TestDeadConsumer =====")
	deadSubscriptionInbox(t, "INSTANCE_0")
	inbox, consumer := subscriptionInbox(t, "INSTANCE_1")
	producer := newProducer()

	defer consumer.Close()

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

func deadSubscriptionInbox(t *testing.T, instanceId string) {
	consumer := newConsumer(instanceId)
	defer consumer.Close()
	consumer.SendAcks = false
	consumer.Subscribe(func(raw messages.Message) {})
}

func subscriptionInbox(t *testing.T, instanceId string) (inbox chan *userneed.UserNeed, consumer *fanout.Consumer) {
	inbox = make(chan *userneed.UserNeed)
	consumer = newConsumer(instanceId)

	consumer.Subscribe(func(raw messages.Message) {
		message := &userneed.UserNeed{}
		log.Printf("Got raw message: %v with id=%d:%d\n", raw.Value, raw.Partition, raw.Offset)
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
		log.Printf("Got something: %v :)\n", *actual)
		if present, found := expected[*actual]; !found || !present {
			t.Errorf("Expected %v to be in %v", *actual, expected)
			return nil
		}
		expected[*actual] = false
		return actual

	case <-time.After(2000 * time.Millisecond):
		log.Printf("Timed out :(\n")
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

func newConsumer(instanceId string) (consumer *fanout.Consumer) {
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
		log.Printf("Got error: %v, retrying..\n", err)
		time.Sleep(interval)
	}
	log.Println("Gave up :(")
	return
}
