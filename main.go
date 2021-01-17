package main

import (
	"flag"
	"github.com/hashicorp/raft"
	"log"
	"os"
)

func main() {
	nodeID := flag.String("id", "", "serverID of a node")
	raftAddr := flag.String("rAddr", "127.0.0.1:5000", "Address of a raft node")
	httpAddr := flag.String("httpAddr", "127.0.0.1:5500", "HTTP address to listen on")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap flag")
	joinAddr := flag.String("joinAddr", "", "Cluster address to join")

	flag.Parse()
	l := log.New(os.Stderr, "[RAFT]", log.LstdFlags|log.Lshortfile)

	node, err := NewRaftNode(*nodeID, *raftAddr, *httpAddr)
	if err != nil {
		l.Fatal(err)
	}

	if *bootstrap == true {
		node.backend.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(*nodeID),
					Address: raft.ServerAddress(*httpAddr),
				},
			},
		})
	}

	if len(*joinAddr) > 0 {
		err = JoinCluster(*joinAddr, *nodeID, *raftAddr)
		if err != nil {
			l.Fatal(err)
		}
	}

	if err := node.handler.StartServer(); err != nil {
		l.Print(err)
	}
}
