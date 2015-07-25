package comm

import (
	"bytes"
	"errors"
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
	Reader *bytes.Buffer
	conn   io.Reader
}

type errorTrampolineFunc func() (errorTrampolineFunc, error)

func NewStream(conn io.Reader) (*Stream, error) {
	data := make([]byte, BufferSize)

	n, err := conn.Read(data)
	if err != nil {
		return nil, err
	}

	log.Printf("Read %d bytes\n", n)

	stream := &Stream{
		data:   data,
		Reader: bytes.NewBuffer(data[0:n]),
		conn:   conn,
	}

	return stream, nil
}

func (s *Stream) ReadWith(fn func() error) error {
	return errorTrampoline(s.readWith(fn))
}

func (s *Stream) readWith(fn func() error) (errorTrampolineFunc, error) {
	bytesBefore := make([]byte, s.Reader.Len())
	copy(bytesBefore, s.Reader.Bytes())

	err := fn()
	if err == nil {
		return nil, nil
	}

	n, err := s.conn.Read(s.data)
	if err != nil {
		return nil, err
	}

	log.Printf("Read %d bytes\n", n)

	s.Reader = bytes.NewBuffer(
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
