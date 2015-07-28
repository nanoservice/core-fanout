package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	happyFilename = "/tmp/fanout_test_cpu_profile_happy_path"
	badFilename   = "/tmp"
)

func TestCPUProfileHappyPath(t *testing.T) {
	p := NewCPUProfile(&happyFilename)
	assert.Nil(t, p.Start())
	assert.Equal(t, true, p.success)
	p.Stop()
}

func TestCPUProfileUnableToCreateFile(t *testing.T) {
	p := NewCPUProfile(&badFilename)
	assert.NotNil(t, p.Start())
	assert.Equal(t, false, p.success)
	p.Stop()
}
