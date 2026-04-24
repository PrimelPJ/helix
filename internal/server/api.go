// Package server exposes the KV store over a simple HTTP API. This is
// the user-facing layer; inter-node communication uses the TCP transport.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/primelj/helix/internal/raft"
	"github.com/primelj/helix/internal/store"
)

// API is the HTTP handler for Helix.
//
// Routes:
//
//	GET    /kv/{key}   → read key (served locally, not linearisable unless
//	                     you add a read-index check)
//	PUT    /kv/{key}   → set key (body is the raw value string)
//	DELETE /kv/{key}   → delete key
//	GET    /status     → cluster status JSON
type API struct {
	node    *raft.Node
	kv      *store.KVStore
	nodeID  raft.NodeID
	mux     *http.ServeMux
}

// New returns an API handler wired to the given node and state machine.
func New(node *raft.Node, kv *store.KVStore, nodeID raft.NodeID) *API {
	a := &API{node: node, kv: kv, nodeID: nodeID, mux: http.NewServeMux()}
	a.mux.HandleFunc("/kv/", a.handleKV)
	a.mux.HandleFunc("/status", a.handleStatus)
	return a
}

// ServeHTTP implements http.Handler.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		val, err := a.kv.Get(key)
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(val))

	case http.MethodPut:
		if a.node.Role() != raft.Leader {
			a.redirectToLeader(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		cmd, err := store.EncodeSet(key, string(body))
		if err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		idx, err := a.node.Propose(ctx, cmd)
		if err != nil {
			if raft.IsNotLeader(err) {
				a.redirectToLeader(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Raft-Index", indexStr(idx))
		w.WriteHeader(http.StatusNoContent)

	case http.MethodDelete:
		if a.node.Role() != raft.Leader {
			a.redirectToLeader(w, r)
			return
		}
		cmd, err := store.EncodeDelete(key)
		if err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, err := a.node.Propose(ctx, cmd); err != nil {
			if raft.IsNotLeader(err) {
				a.redirectToLeader(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	type statusResp struct {
		NodeID   string `json:"node_id"`
		Role     string `json:"role"`
		LeaderID string `json:"leader_id"`
		Keys     int    `json:"key_count"`
	}
	resp := statusResp{
		NodeID:   string(a.nodeID),
		Role:     a.node.Role().String(),
		LeaderID: string(a.node.LeaderID()),
		Keys:     len(a.kv.Keys()),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *API) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.node.LeaderID()
	if leader == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}
	// In a real deployment you'd maintain a leader→address map; here we
	// return a header so the client can handle it.
	w.Header().Set("X-Raft-Leader", string(leader))
	http.Error(w, "not leader", http.StatusTemporaryRedirect)
}

func indexStr(i raft.Index) string {
	var b [20]byte
	pos := len(b)
	if i == 0 {
		return "0"
	}
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// ListenAndServe starts the HTTP API on addr. It blocks until the
// server fails or ctx is cancelled.
func ListenAndServe(ctx context.Context, addr string, handler http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()
	log.Printf("helix API listening on %s", addr)
	return srv.ListenAndServe()
}
