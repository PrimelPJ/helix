// Package transport implements the Helix inter-node wire protocol.
//
// Every message is framed as:
//
//	+--------+--------+----------+------ - - -+
//	|  type  | flags  |  length  |  payload   |
//	| 1 byte | 1 byte | 4 bytes  |  N bytes   |
//	+--------+--------+----------+------ - - -+
//
// The payload is a JSON-encoded struct whose shape is determined by type.
// A future version may swap JSON for a tighter encoding; the frame header
// is stable.
package transport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/primelj/helix/internal/raft"
)

// Message type bytes — these values are part of the stable wire protocol.
const (
	msgRequestVote        byte = 0x01
	msgRequestVoteResp    byte = 0x02
	msgAppendEntries      byte = 0x03
	msgAppendEntriesResp  byte = 0x04
	msgInstallSnapshot    byte = 0x05
	msgInstallSnapshotResp byte = 0x06
)

const (
	headerSize    = 6 // type(1) + flags(1) + length(4)
	maxFrameBytes = 32 * 1024 * 1024 // 32 MiB hard cap
	dialTimeout   = 3 * time.Second
	rwTimeout     = 5 * time.Second
)

// Frame is a decoded message off the wire.
type Frame struct {
	Type    byte
	Flags   byte
	Payload []byte
}

// WriteFrame serialises a Frame onto w.
func WriteFrame(w io.Writer, f Frame) error {
	if len(f.Payload) > maxFrameBytes {
		return fmt.Errorf("transport: payload too large (%d bytes)", len(f.Payload))
	}
	hdr := [headerSize]byte{f.Type, f.Flags}
	binary.BigEndian.PutUint32(hdr[2:], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(f.Payload)
	return err
}

// ReadFrame deserialises the next Frame from r.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	length := binary.BigEndian.Uint32(hdr[2:])
	if length > maxFrameBytes {
		return Frame{}, fmt.Errorf("transport: oversized frame (%d bytes)", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}
	return Frame{Type: hdr[0], Flags: hdr[1], Payload: payload}, nil
}

// TCPTransport implements raft.Transport over persistent TCP connections.
// It maintains one outbound connection per peer (lazily created, re-dialed
// on error).
type TCPTransport struct {
	localAddr string

	mu    sync.Mutex
	conns map[raft.NodeID]net.Conn

	// peerAddrs maps peer NodeID → "host:port"
	peerAddrs map[raft.NodeID]string
}

// NewTCPTransport creates a transport. peerAddrs maps each peer NodeID
// to its TCP address.
func NewTCPTransport(localAddr string, peerAddrs map[raft.NodeID]string) *TCPTransport {
	return &TCPTransport{
		localAddr: localAddr,
		conns:     make(map[raft.NodeID]net.Conn),
		peerAddrs: peerAddrs,
	}
}

func (t *TCPTransport) dial(peer raft.NodeID) (net.Conn, error) {
	addr, ok := t.peerAddrs[peer]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %s", peer)
	}
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (t *TCPTransport) getConn(peer raft.NodeID) (net.Conn, error) {
	t.mu.Lock()
	conn, ok := t.conns[peer]
	t.mu.Unlock()
	if ok {
		return conn, nil
	}
	conn, err := t.dial(peer)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conns[peer] = conn
	t.mu.Unlock()
	return conn, nil
}

func (t *TCPTransport) invalidate(peer raft.NodeID) {
	t.mu.Lock()
	if c, ok := t.conns[peer]; ok {
		c.Close()
		delete(t.conns, peer)
	}
	t.mu.Unlock()
}

func (t *TCPTransport) rpc(peer raft.NodeID, reqType byte, req, resp interface{}) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	conn, err := t.getConn(peer)
	if err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(rwTimeout))
	if err := WriteFrame(conn, Frame{Type: reqType, Payload: payload}); err != nil {
		t.invalidate(peer)
		return err
	}
	frame, err := ReadFrame(conn)
	if err != nil {
		t.invalidate(peer)
		return err
	}
	return json.Unmarshal(frame.Payload, resp)
}

func (t *TCPTransport) SendRequestVote(peer raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := t.rpc(peer, msgRequestVote, args, &reply)
	return &reply, err
}

func (t *TCPTransport) SendAppendEntries(peer raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := t.rpc(peer, msgAppendEntries, args, &reply)
	return &reply, err
}

func (t *TCPTransport) SendInstallSnapshot(peer raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	err := t.rpc(peer, msgInstallSnapshot, args, &reply)
	return &reply, err
}
