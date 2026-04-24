package raft

import (
	"errors"
	"sync"
)

// ErrCompacted is returned when a caller asks for an entry that has
// already been merged into a snapshot.
var ErrCompacted = errors.New("raft: log entry has been compacted")

// ErrUnavailable is returned when a caller asks for an entry beyond the
// end of the log.
var ErrUnavailable = errors.New("raft: log entry not yet available")

// Log is the replicated, append-mostly log of commands. It supports
// truncation from the head (via snapshots) and from the tail (via
// conflict resolution during replication).
//
// The log is conceptually 1-indexed. Entries before the snapshot index
// are not stored; calls referencing them return ErrCompacted.
type Log struct {
	mu sync.RWMutex

	// entries holds entries with index strictly greater than snapshotIndex.
	// entries[0] has index snapshotIndex+1.
	entries []LogEntry

	// snapshotIndex is the highest index covered by the most recent
	// snapshot. snapshotTerm is its term.
	snapshotIndex Index
	snapshotTerm  Term
}

// NewLog returns an empty log.
func NewLog() *Log {
	return &Log{entries: make([]LogEntry, 0, 256)}
}

// LastIndex returns the index of the last entry in the log, or the
// snapshot index if the log is empty.
func (l *Log) LastIndex() Index {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

func (l *Log) lastIndexLocked() Index {
	if len(l.entries) == 0 {
		return l.snapshotIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log.
func (l *Log) LastTerm() Term {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return l.snapshotTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at idx.
func (l *Log) TermAt(idx Index) (Term, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if idx < l.snapshotIndex {
		return 0, ErrCompacted
	}
	if idx == l.snapshotIndex {
		return l.snapshotTerm, nil
	}
	off := int(idx - l.snapshotIndex - 1)
	if off >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[off].Term, nil
}

// Slice returns entries in the inclusive range [from, to]. It returns
// ErrCompacted if from is below the snapshot.
func (l *Log) Slice(from, to Index) ([]LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if from <= l.snapshotIndex {
		return nil, ErrCompacted
	}
	if to > l.lastIndexLocked() {
		to = l.lastIndexLocked()
	}
	if from > to {
		return nil, nil
	}
	a := int(from - l.snapshotIndex - 1)
	b := int(to - l.snapshotIndex - 1)
	out := make([]LogEntry, b-a+1)
	copy(out, l.entries[a:b+1])
	return out, nil
}

// Append adds entries to the end of the log. The caller must ensure
// indices are strictly contiguous starting at LastIndex()+1.
func (l *Log) Append(entries ...LogEntry) {
	if len(entries) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// TruncateFrom removes all entries with index >= from. This is invoked
// when a follower receives entries that conflict with its local log.
func (l *Log) TruncateFrom(from Index) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from <= l.snapshotIndex+1 {
		l.entries = l.entries[:0]
		return
	}
	off := int(from - l.snapshotIndex - 1)
	if off >= len(l.entries) {
		return
	}
	l.entries = l.entries[:off]
}

// CompactTo discards entries up to and including idx, recording the
// term of that entry as the new snapshot term.
func (l *Log) CompactTo(idx Index, term Term) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if idx <= l.snapshotIndex {
		return
	}
	off := int(idx - l.snapshotIndex)
	if off >= len(l.entries) {
		l.entries = l.entries[:0]
	} else {
		l.entries = append(l.entries[:0], l.entries[off:]...)
	}
	l.snapshotIndex = idx
	l.snapshotTerm = term
}

// SnapshotMeta returns the (index, term) of the current snapshot
// boundary.
func (l *Log) SnapshotMeta() (Index, Term) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snapshotIndex, l.snapshotTerm
}
