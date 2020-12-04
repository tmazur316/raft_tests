package main

import (
	"flag"
	"github.com/hashicorp/raft"
	"log"
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
	l := log.New(os.Stderr, "[RAFT INIT]", log.LstdFlags|log.Lshortfile)

	TCPAddr, err := net.ResolveTCPAddr("tcp", *nodeAddr)
	if err != nil {
		l.Panicf("Invalid IP address %s\n", *nodeAddr)
	}

	if *nodeID == "" {
		l.Panicln("id was not specified")
	}

	backAddr := filepath.Join("/home/tomek/Pulpit/boltDB", *nodeID)

	err = os.MkdirAll(backAddr, 0700)
	if err != nil {
		l.Panicln("Error while creating raft backend directory")
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	FSM := NewFsm(*nodeAddr, backAddr)
	boltStore, err := FSM.SetUpBoltDb()

	if err != nil {
		l.Panicln("error while creating boltdb backend")
	}

	snaps, err := raft.NewFileSnapshotStore(backAddr, 2, os.Stderr)

	if err != nil {
		l.Panicln("Snapshot store creation failed")
	}

	trans, err := raft.NewTCPTransport(*nodeAddr, TCPAddr, 3, 10*time.Second, os.Stderr)
	r, err := raft.NewRaft(config, FSM, boltStore, boltStore, snaps, trans)

	if err != nil {
		l.Fatalf("Raft backend creation error")
	}
	FSM.r = r

	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: trans.LocalAddr(),
			},
		},
	})

	h := Handler{
		addr: "127.0.0.1:5500",
		f:    FSM,
	}

	h.StartServer()

	shutChan := make(chan os.Signal, 1)
	signal.Notify(shutChan, os.Interrupt)
	<-shutChan
}
