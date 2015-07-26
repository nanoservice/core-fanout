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
)

const (
	BufferSize = 4096
)

var (
	EOFError = errors.New("Buffer is exhausted")
)

type Stream struct {
	data   []byte
	reader *bytes.Buffer
	conn   io.ReadWriteCloser
}

type Server struct {
	tcpServer net.Listener
}

type errorTrampolineFunc func() (errorTrampolineFunc, error)

func Listen(port string) (server *Server, err error) {
	var tcpServer net.Listener

	err = Error.Bind(func() (err error) {
		tcpServer, err = net.Listen("tcp", port)
		return

	}).Bind(func() error {
		server = &Server{tcpServer}
		return nil

	}).Err()

	return
}

func NewStream(conn io.ReadWriteCloser) (stream *Stream, err error) {
	data := make([]byte, BufferSize)

	stream = &Stream{
		data:   data,
		reader: bytes.NewBuffer(data[0:0]),
		conn:   conn,
	}

	return
}

func Dial(tcpAddr string) (stream *Stream, err error) {
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

func (s *Server) Accept() (stream *Stream, err error) {
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

func (s *Stream) Close() {
	s.conn.Close()
}

func (s *Stream) ReadWith(fn func() error) error {
	return errorTrampoline(s.readWith(fn))
}

func (s *Stream) ReadLine() (result string, err error) {
	err = s.ReadWith(func() (err error) {
		result, err = s.reader.ReadString(byte('\n'))
		return
	})
	return
}

func (s *Stream) WriteLine(line string) (err error) {
	_, err = fmt.Fprintf(s.conn, "%s\n", line)
	return
}

func (s *Stream) ReadMessage(message proto.Message) error {
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

func (s *Stream) WriteMessage(message proto.Message) error {
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

func (s *Stream) readWith(fn func() error) (newFn errorTrampolineFunc, err error) {
	var n int
	bytesBefore := make([]byte, s.reader.Len())
	copy(bytesBefore, s.reader.Bytes())

	e := Error.Bind(fn)

	err = e.OnError().Bind(func() (err error) {
		n, err = s.conn.Read(s.data)
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
