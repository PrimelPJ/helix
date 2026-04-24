// Package raft is a from-scratch implementation of the Raft consensus
// algorithm as described in "In Search of an Understandable Consensus
// Algorithm" (Ongaro & Ousterhout, 2014).
//
// This package provides the consensus core. It does not know about the
// wire protocol or the state machine; those are injected via interfaces
// (Transport, StateMachine).
package raft

import (
	"sync"
	"time"
)

// Role is one of the three roles a Raft node can occupy at any moment.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// NodeID identifies a node within a Raft cluster.
type NodeID string

// Term is a logical clock that increases monotonically. Every leader is
// elected within exactly one term, and at most one leader can exist per
// term.
type Term uint64

// Index is a position within the replicated log. Indices are 1-based; an
// index of 0 means "no entry".
type Index uint64

// LogEntry is a single committed (or pending) command in the replicated
// log. Command is opaque to Raft; it is interpreted by the state machine.
type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte
}

// PersistentState is what every node must durably write to stable
// storage before responding to any RPC. Losing this is equivalent to
// losing the node identity.
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID // empty if no vote cast in this term
}

// VolatileState is rebuilt on restart from the persistent log + snapshot.
type VolatileState struct {
	CommitIndex Index
	LastApplied Index
}

// LeaderState is only valid while Role == Leader. Reinitialized on every
// election win.
type LeaderState struct {
	NextIndex  map[NodeID]Index // for each peer, next entry to send
	MatchIndex map[NodeID]Index // for each peer, highest entry known replicated
}

// Config holds the tunables for a Raft node. Defaults are conservative
// and suitable for an LAN deployment.
type Config struct {
	NodeID          NodeID
	Peers           []NodeID
	HeartbeatPeriod time.Duration // leader sends heartbeats this often
	ElectionTimeout time.Duration // base; randomized between [t, 2t)
	SnapshotEvery   Index         // log entries between snapshots; 0 disables
}

// DefaultConfig returns a Config with reasonable defaults; the caller
// must still set NodeID and Peers.
func DefaultConfig() Config {
	return Config{
		HeartbeatPeriod: 50 * time.Millisecond,
		ElectionTimeout: 300 * time.Millisecond,
		SnapshotEvery:   1024,
	}
}

// safeMu is a tiny helper to make it impossible to forget to unlock in
// the (extremely common) "do one read under the lock" pattern. It is
// not used everywhere — only in spots where the critical section is a
// single expression.
type safeMu struct{ sync.RWMutex }

func (m *safeMu) read(fn func()) {
	m.RLock()
	defer m.RUnlock()
	fn()
}

func (m *safeMu) write(fn func()) {
	m.Lock()
	defer m.Unlock()
	fn()
}
