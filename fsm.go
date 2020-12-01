package main

import (
	"github.com/hashicorp/raft"
	"io"
	"log"
	"sync"
)

type fsm struct{
	data []string
	m sync.Mutex

	logger log.Logger
}

func (f *fsm) Apply() interface{} {
	return nil
}

func (f *fsm) Restore(c io.ReadCloser) error {
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}