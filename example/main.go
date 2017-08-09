package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/go-nats"
	"github.com/tylertreat/nats-on-a-log"
)

func main() {
	var (
		id           = flag.String("id", "", "node id")
		kvPath       = flag.String("kv-path", "", "KV store path")
		logPath      = flag.String("log-path", "", "Raft log path")
		snapshotPath = flag.String("sn-path", "", "Log snapshot path")
		peersPath    = flag.String("peers-path", "", "Peers JSON path")
		peers        = flag.String("peers", "", "List of peers")
	)
	flag.Parse()

	if *kvPath == "" || *logPath == "" || *snapshotPath == "" || *peersPath == "" {
		panic("path not provided")
	}

	if *peers == "" {
		panic("peers not provided")
	}

	p := strings.Split(*peers, ",")

	var (
		config   = raft.DefaultConfig()
		fsm, err = NewFSM(*kvPath)
		natsOpts = nats.DefaultOptions
	)
	if err != nil {
		panic(err)
	}

	store, err := raftboltdb.NewBoltStore(*logPath)
	if err != nil {
		panic(err)
	}

	cacheStore, err := raft.NewLogCache(512, store)
	if err != nil {
		panic(err)
	}

	snapshots, err := raft.NewFileSnapshotStore(*snapshotPath, 2, os.Stdout)
	if err != nil {
		panic(err)
	}

	conn, err := natsOpts.Connect()
	if err != nil {
		panic(err)
	}

	trans, err := natslog.NewNATSTransport(*id, conn, 2*time.Second, os.Stdout)
	if err != nil {
		panic(err)
	}

	peersStore := raft.NewJSONPeers(*peersPath, trans)
	if err := peersStore.SetPeers(p); err != nil {
		panic(err)
	}

	r, err := raft.NewRaft(config, fsm, cacheStore, store, snapshots, peersStore, trans)
	if err != nil {
		panic(err)
	}

	go func() {
		leaderCh := r.LeaderCh()
		for {
			select {
			case isLeader := <-leaderCh:
				if isLeader {
					fmt.Println("*** LEADERSHIP ACQUIRED ***")
					bar := r.Barrier(0)
					if err := bar.Error(); err != nil {
						fmt.Printf("Failed applying barrier when becoming leader: %s\n", err)
					}
				} else {
					fmt.Println("*** LEADERSHIP LOST ***")
				}
			}
		}
	}()

	select {}
}
