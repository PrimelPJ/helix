# Helix Wire Protocol

Every inter-node RPC uses a simple length-prefixed binary framing. The
same format is used for every message direction.

## Frame layout

```
Offset  Size  Field
------  ----  -----
0       1     message type
1       1     flags (reserved; must be 0x00)
2       4     payload length (big-endian uint32)
6       N     payload (JSON-encoded body)
```

### Message type constants

| Hex    | Name                  | Direction              |
|--------|-----------------------|------------------------|
| `0x01` | RequestVote           | candidate → peer       |
| `0x02` | RequestVoteResp       | peer → candidate       |
| `0x03` | AppendEntries         | leader → follower      |
| `0x04` | AppendEntriesResp     | follower → leader      |
| `0x05` | InstallSnapshot       | leader → follower      |
| `0x06` | InstallSnapshotResp   | follower → leader      |

## RequestVote (`0x01`)

```json
{
  "Term": 4,
  "CandidateID": "node2",
  "LastLogIndex": 17,
  "LastLogTerm": 3
}
```

## RequestVoteResp (`0x02`)

```json
{
  "Term": 4,
  "VoteGranted": true
}
```

## AppendEntries (`0x03`)

```json
{
  "Term": 4,
  "LeaderID": "node1",
  "PrevLogIndex": 16,
  "PrevLogTerm": 3,
  "Entries": [
    { "Term": 4, "Index": 17, "Command": "eyJvcCI6ICJTRVQiLCAia2V5IjogImZvbyIsICJ2YWx1ZSI6ICJiYXIifQ==" }
  ],
  "LeaderCommit": 15
}
```

`Command` is base64-encoded when embedded in JSON. The underlying bytes
are a JSON-encoded `store.Command` struct.

## AppendEntriesResp (`0x04`)

```json
{
  "Term": 4,
  "Success": true,
  "ConflictIndex": 0,
  "ConflictTerm": 0
}
```

When `Success` is false, `ConflictIndex` and `ConflictTerm` carry the
fast-rewind hint (§5.3 optimisation).

## InstallSnapshot (`0x05`)

```json
{
  "Term": 4,
  "LeaderID": "node1",
  "LastIncludedIndex": 1024,
  "LastIncludedTerm": 3,
  "Data": "<base64-encoded snapshot bytes>"
}
```

## InstallSnapshotResp (`0x06`)

```json
{
  "Term": 4
}
```

## Connection management

The transport layer maintains one persistent TCP connection per
(source, destination) node pair. If a connection is broken, the next
RPC attempt re-dials. There is no multiplexing — each connection
carries only one in-flight request at a time (request, then response,
then next request). A future version can pipeline multiple requests
using a correlation ID in the flags byte.
