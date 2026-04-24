// helixd is the Helix node daemon.
//
// Usage:
//
//	helixd --id node1 --raft :7001 --http :8001 \
//	        --peers node2=:7002,node3=:7003
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/primelj/helix/internal/raft"
	"github.com/primelj/helix/internal/server"
	"github.com/primelj/helix/internal/store"
	"github.com/primelj/helix/internal/transport"
)

func main() {
	id := flag.String("id", "", "node ID (required)")
	raftAddr := flag.String("raft", ":7001", "raft TCP listen address")
	httpAddr := flag.String("http", ":8001", "HTTP API listen address")
	peersFlag := flag.String("peers", "", "comma-separated id=addr pairs, e.g. node2=:7002,node3=:7003")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		os.Exit(1)
	}

	peerAddrs, peerIDs := parsePeers(*peersFlag)

	cfg := raft.DefaultConfig()
	cfg.NodeID = raft.NodeID(*id)
	cfg.Peers = peerIDs

	l := raft.NewLog()
	sm := store.New()
	storage := raft.NewMemStorage() // swap for WAL-backed storage in prod
	trans := transport.NewTCPTransport(*raftAddr, peerAddrs)

	node := raft.NewNode(cfg, l, storage, trans, sm)
	defer node.Stop()

	api := server.New(node, sm, cfg.NodeID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("[%s] helixd starting — raft %s  http %s", *id, *raftAddr, *httpAddr)
	if err := server.ListenAndServe(ctx, *httpAddr, api); err != nil {
		log.Printf("server stopped: %v", err)
	}
}

func parsePeers(s string) (map[raft.NodeID]string, []raft.NodeID) {
	addrs := make(map[raft.NodeID]string)
	var ids []raft.NodeID
	if s == "" {
		return addrs, ids
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer spec %q", pair)
		}
		id := raft.NodeID(strings.TrimSpace(parts[0]))
		addr := strings.TrimSpace(parts[1])
		addrs[id] = addr
		ids = append(ids, id)
	}
	return addrs, ids
}
