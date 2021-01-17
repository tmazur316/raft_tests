package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type RaftNode struct {
	id      string
	backend *raft.Raft
	handler Handler
}

func NewRaftNode(nodeID, raftAddr, httpAddr string) (*RaftNode, error) {
	var err error

	if len(nodeID) == 0 {
		return nil, err
	}

	//create new FSM
	dbPath := filepath.Join("./boltDB", nodeID)

	if err = os.MkdirAll(dbPath, 0700); err != nil {
		return nil, err
	}

	f := newFSM(dbPath)

	//create new raft backend
	r, err := createRaft(f, nodeID, raftAddr)
	if err != nil {
		return nil, err
	}

	h := Handler{
		f:        f,
		httpAddr: httpAddr,
	}

	return &RaftNode{
		id:      nodeID,
		backend: r,
		handler: h,
	}, nil
}

func createRaft(FSM *fsm, nodeID, raftAddr string) (*raft.Raft, error) {
	var err error

	//initialize TCP transport
	rAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, err
	}

	trans, err := raft.NewTCPTransport(raftAddr, rAddr, 3, 10*time.Second, os.Stderr)

	//use default raft server config
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.ShutdownOnRemove = true

	//use boltDB as a LogStore and StableStore
	boltStore, err := FSM.SetUpBoltDb()
	if err != nil {
		return nil, err
	}

	//create SnapshotStore
	dbPath := filepath.Join("./boltDB", nodeID)

	snapStore, err := raft.NewFileSnapshotStore(dbPath, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	//initialize raft node
	r, err := raft.NewRaft(config, FSM, boltStore, boltStore, snapStore, trans)

	FSM.r = r

	return r, err
}

func JoinCluster(joinAddr, nodeID, raftAddr string) error {
	if _, err := net.ResolveTCPAddr("tcp", joinAddr); err != nil {
		return fmt.Errorf("invalid joinAddr argument: %s\n", joinAddr)
	}

	joinData := map[string]string{"Id": nodeID, "Address": raftAddr}

	body, err := json.Marshal(joinData)
	if err != nil {
		return fmt.Errorf("join failed because of data marshalling error")
	}

	url := fmt.Sprintf("http://%s/join", joinAddr)
	resp, _ := http.Post(url, "application/json", bytes.NewReader(body))
	defer resp.Body.Close()

	return nil
}
