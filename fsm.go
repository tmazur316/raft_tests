package main

import (
	"bytes"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type fsm struct {
	dbPath string

	m    sync.Mutex
	data map[string]string

	r *raft.Raft

	l *log.Logger
}

func newFSM(BackendPath string) *fsm {
	return &fsm{
		dbPath: BackendPath,
		l:      log.New(os.Stderr, "Log", log.LstdFlags),
		data:   make(map[string]string),
	}
}

type operation struct {
	OpType string
	Key    string
	Value  string
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	var op operation
	err := json.Unmarshal(log.Data, &op)
	if err != nil {
		f.l.Printf("Data unmarshalling error. Log %d\n in term %d\n not applied", log.Index, log.Term)
		return err
	}

	switch op.OpType {
	case "Ins":
		f.m.Lock()
		defer f.m.Unlock()
		f.data[op.Key] = op.Value

	case "Del":
		f.m.Lock()
		defer f.m.Unlock()
		delete(f.data, op.Key)

	default:
		f.l.Printf("Wrong operation type: %s\n", op.OpType)
		return err
	}
	return nil
}

func (f *fsm) Restore(c io.ReadCloser) error {
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(c); err != nil {
		f.l.Printf("Restore failure. Data was not read properly")
		return err
	}

	var data = make(map[string]string)
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil {
		f.l.Printf("Restore failure. Data was not unmarshalled properly")
		return err
	}

	f.data = data
	return nil
}

type Snap struct {
	DataCopy map[string]string
}

func (s *Snap) Persist(sink raft.SnapshotSink) error {
	encodedData, err := json.Marshal(s.DataCopy)
	if err != nil {
		sink.Cancel()
		return err
	}
	_, err = sink.Write(encodedData)
	if err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (s *Snap) Release() {}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.m.Lock()
	defer f.m.Unlock()

	dataCopy := make(map[string]string, len(f.data))
	for k, v := range f.data {
		dataCopy[k] = v
	}
	return &Snap{DataCopy: dataCopy}, nil
}

func (f *fsm) SetUpBoltDb() (*raftboltdb.BoltStore, error) {
	path := filepath.Join(f.dbPath, "bolt.db")
	return raftboltdb.NewBoltStore(path)
}

func (f *fsm) Insert(k, v string) error {
	op := operation{
		OpType: "Ins",
		Key:    k,
		Value:  v,
	}

	d, err := json.Marshal(&op)
	if err != nil {
		f.l.Printf("Data marshalling error. Insert failed")
		return err
	}

	f.r.Apply(d, 5*time.Second)

	return nil
}

func (f *fsm) Delete(k string) error {
	op := operation{
		OpType: "Del",
		Key:    k,
	}

	d, err := json.Marshal(&op)
	if err != nil {
		f.l.Printf("Data marshalling error. Insert failed")
		return err
	}

	f.r.Apply(d, 5*time.Second)

	return nil
}

func (f *fsm) Get(k string) string {
	f.m.Lock()
	defer f.m.Unlock()
	return f.data[k]
}
