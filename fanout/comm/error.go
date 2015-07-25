package comm

type Error struct {
	Err error
}

func (e Error) Bind(fn func() error) Error {
	if e.Err != nil {
		return e
	}

	return Bind(fn)
}

func Bind(fn func() error) Error {
	return Error{fn()}
}
