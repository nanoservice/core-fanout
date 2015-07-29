package main

import (
	"github.com/nanoservice/core-fanout/fanout/comm"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

var (
	port = ":4987"
)

func TestClusterListenHappyPath(t *testing.T) {
	c := NewCluster(port)
	assert.Nil(t, c.Listen())
	c.Server.Close()
}

func TestClusterListenOnTheSamePortFails(t *testing.T) {
	c := NewCluster(port)
	c.Listen()
	defer c.Server.Close()

	c2 := NewCluster(port)
	assert.NotNil(t, c2.Listen())
}

func TestClusterHandle(t *testing.T) {
	c := NewCluster(port)
	c.Listen()
	defer c.Server.Close()

	got := make(Messages, 0)

	go func() {
		c.Handle(func(stream comm.Stream) {
			defer stream.Close()
			line, _ := stream.ReadLine()
			got = append(got, line)
		})
	}()

	stream_a, _ := comm.Dial("0.0.0.0" + port)
	defer stream_a.Close()

	stream_b, _ := comm.Dial("0.0.0.0" + port)
	defer stream_b.Close()

	stream_a.WriteLine("hello world")
	stream_b.WriteLine("I am john smith")

	time.Sleep(100 * time.Millisecond)

	expected := Messages{"hello world\n", "I am john smith\n"}
	sort.Sort(expected)
	sort.Sort(got)
	assert.Equal(t, expected, got)
}

type Messages []string

func (a Messages) Len() int           { return len(a) }
func (a Messages) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Messages) Less(i, j int) bool { return a[i] < a[j] }
