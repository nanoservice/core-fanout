package main

import (
	"flag"
	"github.com/nanoservice/core-fanout/fanout/log"
	Error "github.com/nanoservice/monad.go/error"
	"os"
	"os/signal"
	"runtime/pprof"
)

type CPUProfileT struct {
	Filename *string
	file     *os.File
	success  bool
}

var (
	CPUProfile = NewCPUProfile(
		flag.String("cpuprofile", "", "write cpu profile to a file"),
	)
)

func NewCPUProfile(filename *string) *CPUProfileT {
	return &CPUProfileT{
		Filename: filename,
		success:  false,
	}
}

func (p *CPUProfileT) Start() error {
	if *p.Filename == "" {
		return nil
	}

	return Error.Chain(
		p.createCPUProfileFile,
		p.start,
	).Err()
}

func (p *CPUProfileT) Stop() {
	if !p.success {
		return
	}

	dumpCPUProfile()
}

func (p *CPUProfileT) start() error {
	pprof.StartCPUProfile(p.file)
	p.stopOnInterrupt()
	p.success = true
	return nil
}

func (p *CPUProfileT) stopOnInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		p.Stop()
		os.Exit(0)
	}()
}

func (p *CPUProfileT) createCPUProfileFile() (err error) {
	p.file, err = os.Create(*p.Filename)
	return
}

func dumpCPUProfile() {
	log.Println("Dumping cpu profile")
	pprof.StopCPUProfile()
}
