package main

import (
	"github.com/nanoservice/core-fanout/fanout/log"
	"os"
)

func ReportCPUProfileError(err error) {
	log.Printf("Unable to start CPU profile: %v; moving on\n", err)
}

func ReportServerListenFatalError(err error) {
	log.Printf("Unable to listen on port :4987: %v\n", err)
	os.Exit(1)
}

func ReportClientAcceptError(err error) {
	log.Printf("Unable to accept client connection: %v\n", err)
}
