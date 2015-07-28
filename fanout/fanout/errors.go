package main

import (
	"github.com/nanoservice/core-fanout/fanout/log"
)

func ReportCPUProfileError(err error) {
	log.Printf("Unable to start CPU profile: %v; moving on\n", err)
}
