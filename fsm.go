package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"sync"
)

type fsm struct {
	data []string
	m    sync.Mutex

	logger log.Logger
}

type operation struct {
	value string
}

func (f *fsm) Apply(log raft.Log) interface{} {
	var op operation
	err := json.Unmarshal(log.Data, &op)
	if err != nil {
		//TODO wypisywać do loggera
		fmt.Println("error")
		return err
	}
	f.m.Lock()
	defer f.m.Unlock()
	f.data = append(f.data, op.value)
	return nil
}

func (f *fsm) Restore(c io.ReadCloser) error {
	var b []byte
	if _, err := c.Read(b); err != nil {
		//TODO wypisywać do loggera
		fmt.Println("error")
		return err
	}

	var data []string
	if err := json.Unmarshal(b, &data); err != nil {
		//TODO wpisywać do loggera
		fmt.Println("error")
		return err
	}

	f.data = data
	return nil
}

type Snap struct {
	DataCopy []string
}

func (s *Snap) Persist(sink raft.SnapshotSink) error {
	encodedData, err := json.Marshal(s.DataCopy)
	if err != nil {
		//TODO wypisywac do loggera
		fmt.Println("error")
		sink.Cancel()
		return err
	}
	_, err = sink.Write(encodedData)
	if err != nil {
		//TODO wypisywać do loggera
		fmt.Println("error")
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (s *Snap) Release() {}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	//TODO sprawdzic mutex
	f.m.Lock()
	defer f.m.Unlock()

	dataCopy := make([]string, len(f.data))
	copy(dataCopy, f.data)
	return &Snap{DataCopy: dataCopy}, nil
}
