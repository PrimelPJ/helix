package raft

import (
	"errors"
	"fmt"
)

// ErrStopped is returned when an operation is attempted on a stopped node.
var ErrStopped = errors.New("raft: node is stopped")

// NotLeaderError is returned when a non-leader node receives a write
// request. The Leader field (if known) tells the client where to redirect.
type NotLeaderError struct {
	Leader NodeID
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not leader (no known leader)"
	}
	return fmt.Sprintf("raft: not leader; current leader is %s", e.Leader)
}

// IsNotLeader reports whether err is a NotLeaderError.
func IsNotLeader(err error) bool {
	var nle *NotLeaderError
	return errors.As(err, &nle)
}
