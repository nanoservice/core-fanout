package comm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/nanoservice/core-fanout/fanout/log"
	Error "github.com/nanoservice/monad.go/error"
	"io"
	"net"
	"time"
)

const (
	BufferSize = 4096
)

var (
	EOFError = errors.New("Buffer is exhausted")
)

type Server interface {
	Accept() (stream Stream, err error)
	Close()
}

type ServerT struct {
	tcpServer net.Listener
}

func Listen(port string) (server Server, err error) {
	var tcpServer net.Listener

	err = Error.Bind(func() (err error) {
		tcpServer, err = net.Listen("tcp", port)
		return

	}).Bind(func() error {
		server = &ServerT{tcpServer}
		return nil

	}).Err()

	return
}

func (s *ServerT) Accept() (stream Stream, err error) {
	var conn net.Conn

	err = Error.Bind(func() (err error) {
		conn, err = s.tcpServer.Accept()
		return

	}).Bind(func() (err error) {
		stream, err = NewStream(conn)
		return

	}).Err()

	return
}

func (s *ServerT) Close() { s.tcpServer.Close() }

type Stream interface {
	Close()
	ReadWith(fn func() error) error
	ReadLine() (result string, err error)
	WriteLine(line string) (err error)
	ReadMessage(message proto.Message) error
	WriteMessage(message proto.Message) error
}

type StreamT struct {
	data   []byte
	reader *bytes.Buffer
	conn   io.ReadWriteCloser
}

func NewStream(conn io.ReadWriteCloser) (stream Stream, err error) {
	data := make([]byte, BufferSize)

	stream = &StreamT{
		data:   data,
		reader: bytes.NewBuffer(data[0:0]),
		conn:   conn,
	}

	return
}

func Dial(tcpAddr string) (stream Stream, err error) {
	var conn net.Conn

	err = Error.Bind(func() (err error) {
		conn, err = net.Dial("tcp", tcpAddr)
		return

	}).Bind(func() (err error) {
		stream, err = NewStream(conn)
		return

	}).Err()

	return
}

func (s *StreamT) Close() {
	s.conn.Close()
}

func (s *StreamT) ReadWith(fn func() error) error {
	return errorTrampoline(s.readWith(fn))
}

func (s *StreamT) ReadLine() (result string, err error) {
	err = s.ReadWith(func() (err error) {
		result, err = s.reader.ReadString(byte('\n'))
		return
	})
	return
}

func (s *StreamT) WriteLine(line string) (err error) {
	_, err = fmt.Fprintf(s.conn, "%s\n", line)
	return
}

func (s *StreamT) ReadMessage(message proto.Message) error {
	var messageSize int32

	return Error.Bind(func() error {
		return s.ReadWith(func() error {
			return binary.Read(s.reader, binary.LittleEndian, &messageSize)
		})

	}).Bind(func() error {
		return s.ReadWith(func() error {
			if int32(s.reader.Len()) < messageSize {
				return EOFError
			}
			return nil
		})

	}).Bind(func() error {
		rawMessage := s.reader.Next(int(messageSize))
		return proto.Unmarshal(rawMessage, message)

	}).Err()
}

func (s *StreamT) WriteMessage(message proto.Message) error {
	buf := new(bytes.Buffer)
	var rawMessage []byte

	return Error.Bind(func() (err error) {
		rawMessage, err = proto.Marshal(message)
		return

	}).Bind(func() error {
		size := int32(len(rawMessage))
		return binary.Write(buf, binary.LittleEndian, size)

	}).Bind(func() (err error) {
		_, err = buf.Write(rawMessage)
		return

	}).Bind(func() (err error) {
		_, err = buf.WriteTo(s.conn)
		return

	}).Err()
}

func (s *StreamT) readWith(fn func() error) (newFn errorTrampolineFunc, err error) {
	var n int
	bytesBefore := make([]byte, s.reader.Len())
	copy(bytesBefore, s.reader.Bytes())

	e := Error.Bind(fn)

	err = e.OnError().Bind(func() (err error) {
		log.V(4).Println("Going to read from connection")
		n, err = s.conn.Read(s.data)
		log.V(4).Printf("Read from connection: %d, %v\n", n, err)
		if n == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		return

	}).Bind(func() error {
		log.V(2).Printf("Re-read %d bytes\n", n)

		s.reader = bytes.NewBuffer(
			append(bytesBefore, s.data[0:n]...),
		)
		newFn = func() (errorTrampolineFunc, error) {
			return s.readWith(fn)
		}
		return nil

	}).Err()

	e.Bind(func() error {
		err = nil
		return nil
	})

	return
}

type errorTrampolineFunc func() (errorTrampolineFunc, error)

func errorTrampoline(fn errorTrampolineFunc, err error) error {
	if err != nil {
		return err
	}

	if fn == nil {
		return nil
	}

	for {
		fn, err = fn()
		if err != nil {
			return err
		}
		if fn == nil {
			return nil
		}
	}
}
