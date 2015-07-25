package comm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
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

func NewStream(conn io.ReadWriter) (*Stream, error) {
	data := make([]byte, BufferSize)

	n, err := conn.Read(data)
	if err != nil {
		return nil, err
	}

	log.Printf("Read %d bytes\n", n)

	stream := &Stream{
		data:   data,
		reader: bytes.NewBuffer(data[0:n]),
		conn:   conn,
	}

	return stream, nil
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

func (s *Stream) ReadMessage(message proto.Message) (err error) {
	var messageSize int32
	err = s.ReadWith(func() error {
		return binary.Read(s.reader, binary.LittleEndian, &messageSize)
	})
	if err != nil {
		return
	}

	err = s.ReadWith(func() error {
		if int32(s.reader.Len()) < messageSize {
			return EOFError
		}
		return nil
	})
	if err != nil {
		return
	}

	rawMessage := s.reader.Next(int(messageSize))
	err = proto.Unmarshal(rawMessage, message)
	return
}

func (s *Stream) WriteMessage(message proto.Message) (err error) {
	buf := new(bytes.Buffer)

	rawMessage, err := proto.Marshal(message)
	if err != nil {
		return
	}

	size := int32(len(rawMessage))
	err = binary.Write(buf, binary.LittleEndian, size)
	if err != nil {
		return
	}

	_, err = buf.Write(rawMessage)
	if err != nil {
		return
	}

	_, err = buf.WriteTo(s.conn)
	return
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
