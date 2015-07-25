package comm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	Error "github.com/nanoservice/monad.go/error"
	"io"
	"log"
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
	conn   io.ReadWriter
}

type errorTrampolineFunc func() (errorTrampolineFunc, error)

func NewStream(conn io.ReadWriter) (stream *Stream, err error) {
	var n int
	data := make([]byte, BufferSize)

	err = Error.Bind(func() (err error) {
		n, err = conn.Read(data)
		return

	}).Bind(func() error {
		stream = &Stream{
			data:   data,
			reader: bytes.NewBuffer(data[0:n]),
			conn:   conn,
		}

		return nil
	}).Err()

	return
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

func (s *Stream) readWith(fn func() error) (errorTrampolineFunc, error) {
	bytesBefore := make([]byte, s.reader.Len())
	copy(bytesBefore, s.reader.Bytes())

	err := fn()
	if err == nil {
		return nil, nil
	}

	n, err := s.conn.Read(s.data)
	if err != nil {
		return nil, err
	}

	log.Printf("Read %d bytes\n", n)

	s.reader = bytes.NewBuffer(
		append(bytesBefore, s.data[0:n]...),
	)

	return func() (errorTrampolineFunc, error) {
		return s.readWith(fn)
	}, nil
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
