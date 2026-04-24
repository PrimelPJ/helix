package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Node is the central Raft actor. Spawn one per server process.
type Node struct {
	cfg     Config
	log     *Log
	storage Storage
	trans   Transport
	sm      StateMachine

	mu          sync.Mutex
	role        Role
	currentTerm Term
	votedFor    NodeID
	leaderID    NodeID

	commitIndex Index
	lastApplied Index

	// leader-only state
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// channels for incoming RPCs (fed by the transport layer)
	rvCh  chan rvReq
	aeCh  chan aeReq
	isCh  chan isReq
	propC chan propReq

	// apply loop
	applyCh chan LogEntry

	stopCh chan struct{}
	wg     sync.WaitGroup

	// monotonic election timer reset
	lastContact atomic.Int64
}

type rvReq struct {
	args  *RequestVoteArgs
	reply chan *RequestVoteReply
}
type aeReq struct {
	args  *AppendEntriesArgs
	reply chan *AppendEntriesReply
}
type isReq struct {
	args  *InstallSnapshotArgs
	reply chan *InstallSnapshotReply
}
type propReq struct {
	cmd    []byte
	result chan propResult
}
type propResult struct {
	index Index
	err   error
}

// NewNode creates and starts a Raft node.
func NewNode(cfg Config, l *Log, storage Storage, trans Transport, sm StateMachine) *Node {
	ps, _ := storage.LoadState()
	n := &Node{
		cfg:         cfg,
		log:         l,
		storage:     storage,
		trans:       trans,
		sm:          sm,
		currentTerm: ps.CurrentTerm,
		votedFor:    ps.VotedFor,
		role:        Follower,
		rvCh:        make(chan rvReq, 16),
		aeCh:        make(chan aeReq, 64),
		isCh:        make(chan isReq, 4),
		propC:       make(chan propReq, 256),
		applyCh:     make(chan LogEntry, 256),
		stopCh:      make(chan struct{}),
	}
	n.lastContact.Store(time.Now().UnixNano())
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
	return n
}

// Propose submits a command to the cluster. Returns the log index where
// the command was appended, or an error if this node is not the leader.
func (n *Node) Propose(ctx context.Context, cmd []byte) (Index, error) {
	req := propReq{cmd: cmd, result: make(chan propResult, 1)}
	select {
	case n.propC <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
	select {
	case r := <-req.result:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
}

// Stop shuts down the node gracefully.
func (n *Node) Stop() {
	close(n.stopCh)
	n.wg.Wait()
}

// Role returns the node's current role (follower/candidate/leader).
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// LeaderID returns the node ID of the most recently known leader.
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// ── main event loop ──────────────────────────────────────────────────

func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}
		n.mu.Lock()
		role := n.role
		n.mu.Unlock()
		switch role {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) electionTimeout() time.Duration {
	base := n.cfg.ElectionTimeout
	jitter := time.Duration(rand.Int63n(int64(base)))
	return base + jitter
}

func (n *Node) runFollower() {
	timeout := time.NewTimer(n.electionTimeout())
	defer timeout.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.rvCh:
			req.reply <- n.handleRequestVote(req.args)
		case req := <-n.aeCh:
			req.reply <- n.handleAppendEntries(req.args)
		case req := <-n.isCh:
			req.reply <- n.handleInstallSnapshot(req.args)
		case req := <-n.propC:
			n.mu.Lock()
			leader := n.leaderID
			n.mu.Unlock()
			req.result <- propResult{err: &NotLeaderError{Leader: leader}}
		case <-timeout.C:
			since := time.Since(time.Unix(0, n.lastContact.Load()))
			if since >= n.cfg.ElectionTimeout {
				n.mu.Lock()
				n.role = Candidate
				n.mu.Unlock()
				return
			}
			timeout.Reset(n.electionTimeout())
		}
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.cfg.NodeID
	term := n.currentTerm
	n.mu.Unlock()
	n.persist()

	log.Printf("[%s] starting election for term %d", n.cfg.NodeID, term)

	votes := 1 // vote for self
	quorum := (len(n.cfg.Peers)+1)/2 + 1
	voteCh := make(chan bool, len(n.cfg.Peers))

	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	for _, peer := range n.cfg.Peers {
		p := peer
		go func() {
			reply, err := n.trans.SendRequestVote(p, &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.cfg.NodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
			}
			n.mu.Unlock()
			voteCh <- reply.VoteGranted
		}()
	}

	timeout := time.NewTimer(n.electionTimeout())
	defer timeout.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.rvCh:
			req.reply <- n.handleRequestVote(req.args)
			n.mu.Lock()
			r := n.role
			n.mu.Unlock()
			if r == Follower {
				return
			}
		case req := <-n.aeCh:
			req.reply <- n.handleAppendEntries(req.args)
			n.mu.Lock()
			r := n.role
			n.mu.Unlock()
			if r == Follower {
				return
			}
		case req := <-n.isCh:
			req.reply <- n.handleInstallSnapshot(req.args)
		case req := <-n.propC:
			req.result <- propResult{err: &NotLeaderError{}}
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= quorum {
					n.becomeLeader()
					return
				}
			}
		case <-timeout.C:
			return // restart election
		}
	}
}

func (n *Node) runLeader() {
	hb := time.NewTicker(n.cfg.HeartbeatPeriod)
	defer hb.Stop()
	// send immediate heartbeat
	n.broadcastHeartbeat()
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.rvCh:
			req.reply <- n.handleRequestVote(req.args)
			n.mu.Lock()
			r := n.role
			n.mu.Unlock()
			if r != Leader {
				return
			}
		case req := <-n.aeCh:
			req.reply <- n.handleAppendEntries(req.args)
			n.mu.Lock()
			r := n.role
			n.mu.Unlock()
			if r != Leader {
				return
			}
		case req := <-n.isCh:
			req.reply <- n.handleInstallSnapshot(req.args)
		case req := <-n.propC:
			idx, err := n.appendCommand(req.cmd)
			req.result <- propResult{index: idx, err: err}
			if err == nil {
				n.replicateToAll()
			}
		case <-hb.C:
			n.broadcastHeartbeat()
		}
	}
}

// ── RPC handlers ─────────────────────────────────────────────────────

func (n *Node) handleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return reply
	}
	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	if args.LastLogTerm < lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx) {
		return reply
	}
	n.votedFor = args.CandidateID
	n.persist()
	n.touchContact()
	reply.VoteGranted = true
	return reply
}

func (n *Node) handleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.leaderID = args.LeaderID
	n.touchContact()

	prevTerm, err := n.log.TermAt(args.PrevLogIndex)
	if err != nil || prevTerm != args.PrevLogTerm {
		reply.ConflictIndex = n.log.LastIndex() + 1
		return reply
	}
	for i, entry := range args.Entries {
		existing, err := n.log.TermAt(entry.Index)
		if err != nil || existing != entry.Term {
			n.log.TruncateFrom(entry.Index)
			n.log.Append(args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > n.commitIndex {
		last := n.log.LastIndex()
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.maybeApply()
	}
	reply.Success = true
	return reply
}

func (n *Node) handleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.touchContact()
	if err := n.sm.Restore(args.Data); err != nil {
		log.Printf("[%s] snapshot restore error: %v", n.cfg.NodeID, err)
		return reply
	}
	n.log.CompactTo(args.LastIncludedIndex, args.LastIncludedTerm)
	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	return reply
}

// ── helpers ───────────────────────────────────────────────────────────

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = Leader
	n.nextIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	n.matchIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	next := n.log.LastIndex() + 1
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = next
		n.matchIndex[p] = 0
	}
	log.Printf("[%s] became leader for term %d", n.cfg.NodeID, n.currentTerm)
}

func (n *Node) stepDown(term Term) {
	n.currentTerm = term
	n.votedFor = ""
	n.role = Follower
	n.persist()
}

func (n *Node) persist() {
	_ = n.storage.SaveState(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	})
}

func (n *Node) touchContact() {
	n.lastContact.Store(time.Now().UnixNano())
}

func (n *Node) appendCommand(cmd []byte) (Index, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	idx := n.log.LastIndex() + 1
	n.log.Append(LogEntry{Term: n.currentTerm, Index: idx, Command: cmd})
	return idx, nil
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	term := n.currentTerm
	id := n.cfg.NodeID
	commit := n.commitIndex
	peers := append([]NodeID(nil), n.cfg.Peers...)
	n.mu.Unlock()
	for _, peer := range peers {
		p := peer
		go func() {
			reply, err := n.trans.SendAppendEntries(p, &AppendEntriesArgs{
				Term:         term,
				LeaderID:     id,
				LeaderCommit: commit,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
			}
			n.mu.Unlock()
		}()
	}
}

func (n *Node) replicateToAll() {
	n.mu.Lock()
	peers := append([]NodeID(nil), n.cfg.Peers...)
	n.mu.Unlock()
	for _, p := range peers {
		go n.replicateTo(p)
	}
}

func (n *Node) replicateTo(peer NodeID) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevIdx := nextIdx - 1
	prevTerm, _ := n.log.TermAt(prevIdx)
	entries, _ := n.log.Slice(nextIdx, n.log.LastIndex())
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.NodeID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.trans.SendAppendEntries(peer, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	if reply.Success {
		if len(entries) > 0 {
			last := entries[len(entries)-1].Index
			n.matchIndex[peer] = last
			n.nextIndex[peer] = last + 1
		}
		n.advanceCommitIndex()
	} else {
		if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) advanceCommitIndex() {
	// Find the highest N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm.
	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		t, err := n.log.TermAt(idx)
		if err != nil || t != n.currentTerm {
			continue
		}
		count := 1
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count > (len(n.cfg.Peers)+1)/2 {
			n.commitIndex = idx
			n.maybeApply()
			break
		}
	}
}

func (n *Node) maybeApply() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entries, _ := n.log.Slice(n.lastApplied, n.lastApplied)
		if len(entries) > 0 {
			n.applyCh <- entries[0]
		}
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case entry := <-n.applyCh:
			if _, err := n.sm.Apply(entry); err != nil {
				log.Printf("[%s] state machine Apply error at index %d: %v",
					n.cfg.NodeID, entry.Index, err)
			}
		}
	}
}

// IncomingRequestVote is called by the transport when a peer sends us a
// vote request.
func (n *Node) IncomingRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := make(chan *RequestVoteReply, 1)
	n.rvCh <- rvReq{args: args, reply: reply}
	return <-reply
}

// IncomingAppendEntries is called by the transport when a peer sends us
// log entries / a heartbeat.
func (n *Node) IncomingAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := make(chan *AppendEntriesReply, 1)
	n.aeCh <- aeReq{args: args, reply: reply}
	return <-reply
}

// IncomingInstallSnapshot is called by the transport when a leader
// wants to bring us up to date via a snapshot.
func (n *Node) IncomingInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	reply := make(chan *InstallSnapshotReply, 1)
	n.isCh <- isReq{args: args, reply: reply}
	return <-reply
}
