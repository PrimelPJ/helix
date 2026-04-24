package raft

import "sync"

// MemStorage is a non-durable Storage implementation. Suitable for
// tests and demos; do not use in production.
type MemStorage struct {
	mu      sync.Mutex
	state   PersistentState
	entries []LogEntry
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) SaveState(s PersistentState) error {
	m.mu.Lock()
	m.state = s
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) LoadState() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

func (m *MemStorage) SaveLog(entries []LogEntry) error {
	m.mu.Lock()
	m.entries = append(m.entries[:0], entries...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) LoadLog() ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LogEntry, len(m.entries))
	copy(out, m.entries)
	return out, nil
}
