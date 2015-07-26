package log

import (
	"flag"
	"log"
)

type loggerT struct{}
type nullLoggerT struct{}

type loggerI interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var (
	currentV   = -1
	logger     = &loggerT{}
	nullLogger = &nullLoggerT{}
	flagV      = false
	flagVV     = false
	flagVVV    = false
)

func V(lvl int) loggerI {
	if currentLevel() >= lvl {
		return logger
	}
	return nullLogger
}

func Print(v ...interface{})                 { V(0).Print(v...) }
func Printf(format string, v ...interface{}) { V(0).Printf(format, v...) }
func Println(v ...interface{})               { V(0).Println(v...) }

func (l *loggerT) Print(v ...interface{})                 { log.Print(v...) }
func (l *loggerT) Printf(format string, v ...interface{}) { log.Printf(format, v...) }
func (l *loggerT) Println(v ...interface{})               { log.Println(v...) }

func (*nullLoggerT) Print(v ...interface{})                 {}
func (*nullLoggerT) Printf(format string, v ...interface{}) {}
func (*nullLoggerT) Println(v ...interface{})               {}

func init() {
	flag.BoolVar(&flagV, "v", false, "verbose mode")
	flag.BoolVar(&flagVV, "vv", false, "very verbose mode")
	flag.BoolVar(&flagVVV, "vvv", false, "debug mode")
}

func currentLevel() int {
	if currentV < 0 {
		currentV = calcCurrentV()
	}
	return currentV
}

func calcCurrentV() int {
	if flagVVV {
		return 3
	}

	if flagVV {
		return 2
	}

	if flagV {
		return 1
	}

	return 0
}
