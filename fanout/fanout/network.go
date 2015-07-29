package main

import (
	"github.com/nanoservice/core-fanout/fanout/comm"
	Error "github.com/nanoservice/monad.go/error"
)

type ClusterT struct {
	Server comm.Server
	Port   string
}

var (
	DefaultPort = ":4987"
	Cluster     = NewCluster(DefaultPort)
)

func NewCluster(port string) *ClusterT {
	return &ClusterT{Port: port}
}

func (c *ClusterT) Listen() (err error) {
	c.Server, err = comm.Listen(c.Port)
	return
}

func (c *ClusterT) Handle(fn func(comm.Stream)) {
	for {
		NewClient(c).Handle(fn)
	}
}

type Client struct {
	Stream  comm.Stream
	Cluster *ClusterT
}

func NewClient(cluster *ClusterT) *Client {
	return &Client{Cluster: cluster}
}

func (c *Client) Handle(fn func(comm.Stream)) {
	Error.Chain(
		c.accept,
		c.handle(fn),
	).OnErrorFn(ReportClientAcceptError)
}

func (c *Client) accept() (err error) {
	c.Stream, err = c.Cluster.Server.Accept()
	return
}

func (c *Client) handle(fn func(comm.Stream)) func() error {
	return func() error {
		go fn(c.Stream)
		return nil
	}
}
