package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/primelj/helix/internal/raft"
)

// noopSM is a state machine that discards all entries.
type noopSM struct{}

func (n *noopSM) Apply(e raft.LogEntry) ([]byte, error)  { return nil, nil }
func (n *noopSM) Snapshot() ([]byte, error)              { return []byte("{}"), nil }
func (n *noopSM) Restore(_ []byte) error                 { return nil }

// noopTransport silently drops all outbound RPCs.
type noopTransport struct{}

func (t *noopTransport) SendRequestVote(_ raft.NodeID, _ *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{}, nil
}
func (t *noopTransport) SendAppendEntries(_ raft.NodeID, _ *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{}, nil
}
func (t *noopTransport) SendInstallSnapshot(_ raft.NodeID, _ *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

func TestLog_AppendAndSlice(t *testing.T) {
	l := raft.NewLog()
	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}
	l.Append(entries...)

	if got := l.LastIndex(); got != 3 {
		t.Fatalf("LastIndex = %d, want 3", got)
	}

	slice, err := l.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(slice) != 3 {
		t.Fatalf("len(slice) = %d, want 3", len(slice))
	}
}

func TestLog_TruncateFrom(t *testing.T) {
	l := raft.NewLog()
	for i := 1; i <= 5; i++ {
		l.Append(raft.LogEntry{Term: 1, Index: raft.Index(i)})
	}
	l.TruncateFrom(3)
	if got := l.LastIndex(); got != 2 {
		t.Fatalf("after truncate, LastIndex = %d, want 2", got)
	}
}

func TestLog_CompactTo(t *testing.T) {
	l := raft.NewLog()
	for i := 1; i <= 10; i++ {
		l.Append(raft.LogEntry{Term: 1, Index: raft.Index(i)})
	}
	l.CompactTo(5, 1)
	si, _ := l.SnapshotMeta()
	if si != 5 {
		t.Fatalf("snapshot index = %d, want 5", si)
	}
	if got := l.LastIndex(); got != 10 {
		t.Fatalf("LastIndex after compact = %d, want 10", got)
	}
	_, err := l.Slice(3, 5)
	if err != raft.ErrCompacted {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
}

func TestNode_StartsAsFollower(t *testing.T) {
	cfg := raft.DefaultConfig()
	cfg.NodeID = "solo"
	cfg.ElectionTimeout = 500 * time.Millisecond

	node := raft.NewNode(cfg, raft.NewLog(), raft.NewMemStorage(), &noopTransport{}, &noopSM{})
	defer node.Stop()

	if role := node.Role(); role != raft.Follower {
		t.Fatalf("expected Follower on start, got %s", role)
	}
}

func TestNode_ProposeReturnsNotLeader_WhenFollower(t *testing.T) {
	cfg := raft.DefaultConfig()
	cfg.NodeID = "follower"
	cfg.ElectionTimeout = 10 * time.Second // prevent election during test

	node := raft.NewNode(cfg, raft.NewLog(), raft.NewMemStorage(), &noopTransport{}, &noopSM{})
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := node.Propose(ctx, []byte("hello"))
	if err == nil {
		t.Fatal("expected error from Propose on a follower")
	}
	if !raft.IsNotLeader(err) {
		t.Fatalf("expected NotLeaderError, got %T: %v", err, err)
	}
}

func TestNotLeaderError(t *testing.T) {
	err := &raft.NotLeaderError{Leader: "node1"}
	if !raft.IsNotLeader(err) {
		t.Fatal("IsNotLeader should be true")
	}
	if err.Error() == "" {
		t.Fatal("error string should not be empty")
	}
}
