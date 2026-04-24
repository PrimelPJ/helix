// Package store implements the Helix key-value state machine. It is the
// user-visible layer that sits on top of the Raft log.
package store

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/primelj/helix/internal/raft"
)

// Op is the type of a state machine operation.
type Op string

const (
	OpSet    Op = "SET"
	OpDelete Op = "DEL"
)

// Command is the payload stored in each Raft log entry.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("store: key not found")

// KVStore is a linearisable key-value store backed by Raft. It
// implements raft.StateMachine.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string

	// applied is signalled every time the state machine advances.
	applied chan struct{}
}

// New returns an empty KVStore.
func New() *KVStore {
	return &KVStore{
		data:    make(map[string]string),
		applied: make(chan struct{}, 256),
	}
}

// Get returns the value for key, or ErrNotFound.
func (s *KVStore) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

// Keys returns all keys in sorted-ish (map) order. For a real
// implementation you'd maintain a sorted structure; this is fine for a
// demo.
func (s *KVStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Apply implements raft.StateMachine. It is called (serially) for every
// committed log entry.
func (s *KVStore) Apply(entry raft.LogEntry) ([]byte, error) {
	var cmd Command
	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		return nil, err
	}
	s.mu.Lock()
	switch cmd.Op {
	case OpSet:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
	s.mu.Unlock()
	// notify any watchers
	select {
	case s.applied <- struct{}{}:
	default:
	}
	return nil, nil
}

// Snapshot serialises the entire state machine into a byte slice for
// log compaction.
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

// Restore replaces the state machine state from a snapshot produced by
// Snapshot().
func (s *KVStore) Restore(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.mu.Lock()
	s.data = m
	s.mu.Unlock()
	return nil
}

// EncodeSet returns a log-entry payload for a SET operation.
func EncodeSet(key, value string) ([]byte, error) {
	return json.Marshal(Command{Op: OpSet, Key: key, Value: value})
}

// EncodeDelete returns a log-entry payload for a DEL operation.
func EncodeDelete(key string) ([]byte, error) {
	return json.Marshal(Command{Op: OpDelete, Key: key})
}
