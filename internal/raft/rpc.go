package raft

// This file defines the four RPC message pairs that flow between Raft
// nodes. The on-the-wire encoding lives in package transport; this
// package only knows about the logical shape.

// RequestVoteArgs is sent by a candidate to gather votes for a new term.
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

// RequestVoteReply is the response to a vote request.
type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs is sent by a leader to replicate log entries (or as
// a heartbeat when Entries is empty).
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

// AppendEntriesReply is the response to an AppendEntries.
type AppendEntriesReply struct {
	Term    Term
	Success bool
	// ConflictIndex/ConflictTerm let the leader fast-rewind nextIndex
	// when a follower has a divergent log, instead of decrementing by 1
	// each round-trip. This is the optimization described in §5.3 of
	// the Raft paper.
	ConflictIndex Index
	ConflictTerm  Term
}

// InstallSnapshotArgs is sent by a leader when a follower has fallen
// so far behind that the relevant log entries have already been
// compacted away.
type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Data              []byte // serialized state machine
}

// InstallSnapshotReply is the response to an InstallSnapshot.
type InstallSnapshotReply struct {
	Term Term
}

// Transport abstracts the network. Implementations are responsible for
// serializing the args/reply types above and delivering them to the
// named peer. A transport implementation must be safe for concurrent
// use.
type Transport interface {
	SendRequestVote(peer NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(peer NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// StateMachine is the user-supplied state machine that consumes
// committed log entries.
type StateMachine interface {
	Apply(entry LogEntry) ([]byte, error)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// Storage abstracts persistent storage of term, vote, and log. A
// real implementation would back this with an fsync'd write-ahead log.
type Storage interface {
	SaveState(state PersistentState) error
	LoadState() (PersistentState, error)
	SaveLog(entries []LogEntry) error
	LoadLog() ([]LogEntry, error)
}
