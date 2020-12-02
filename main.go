package main

import (
	"flag"
	"fmt"
	"github.com/hashicorp/raft"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

func main() {
	nodeAddr := flag.String("nAddr", "127.0.0.1:5000", "Address of a node")
	nodeID := flag.String("id", "", "serverID of a node")

	flag.Parse()

	TCPAddr, err := net.ResolveTCPAddr("tcp", *nodeAddr)
	if err != nil {
		//TODO do logu
		fmt.Println("Bad IP address")
		return
	}

	if *nodeID == "" {
		//TODO log
		fmt.Println("No server id was specified")
		return
	}

	backAddr := filepath.Join("/home/tomek/Pulpit/boltDB", *nodeID)

	err = os.MkdirAll(backAddr, 0700)
	if err != nil {
		//TODO log
		fmt.Println("Error while creating raft backend directory")
		return
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	FSM := NewFsm(*nodeAddr, backAddr)
	boltStore, err := FSM.SetUpBoltDb()

	if err != nil {
		//TODO log
		fmt.Println("error while creating boltdb backend")
	}

	snaps, err := raft.NewFileSnapshotStore(backAddr, 2, os.Stderr)

	if err != nil {
		//TODO log
		fmt.Println("error creating Snapshot store")
	}

	trans, err := raft.NewTCPTransport(*nodeAddr, TCPAddr, 3, 10*time.Second, os.Stderr)
	r, err := raft.NewRaft(config, FSM, boltStore, boltStore, snaps, trans)

	if err != nil {
		//TODO log
		fmt.Println("error initializing raft backend")
		return
	}
	FSM.r = r

	r.BootstrapCluster(raft.Configuration{
		[]raft.Server{
			{
				ID:      config.LocalID,
				Address: trans.LocalAddr(),
			},
		},
	})

	shutChan := make(chan os.Signal, 1)
	signal.Notify(shutChan, os.Interrupt)
	<-shutChan
}
