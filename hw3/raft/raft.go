// Package raft implements the Raft consensus algorithm.
//
// A Raft instance manages one node's participation in a Raft cluster. It:
//   - elects a leader via RequestVote RPCs (Stage 1)
//   - replicates log entries via AppendEntries RPCs (Stage 2)
//   - commits entries when a majority have confirmed receipt (Stage 2)
//   - sends committed entries to the server via commitCh (Stage 2)
//
// References:
//   - Ongaro & Ousterhout — In Search of an Understandable Consensus Algorithm (2014)
//   - https://raft.github.io/raft.pdf
//
// Read the paper before implementing. Every field in this struct corresponds
// directly to a variable defined in Figure 2 of the paper.
package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"kvraft/config"
	ilog "kvraft/internal/log"
	pb "kvraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftState represents the current role of this Raft node.
type RaftState int

const (
	Follower  RaftState = iota
	Candidate           // running an election
	Leader              // currently the leader
)

// Timing constants. Students may tune these but must justify their choices.
// The election timeout MUST be significantly larger than the heartbeat interval.
const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	RPCTimeout         = 50 * time.Millisecond
)

// ApplyMsg is sent on commitCh when a log entry is committed and ready to be
// applied to the KV store. The server reads from this channel.
type ApplyMsg struct {
	Index   int64  // 1-indexed log index of the committed entry
	Term    int64  // term in which the entry was created
	Command string // encoded command (e.g. "put:key:value")
}

// Raft is the core consensus state machine.
//
// All exported methods are safe to call from multiple goroutines.
// Internal methods must be called with mu held, unless noted otherwise.
type Raft struct {
	mu sync.Mutex

	// ── Persistent state (Figure 2) ────────────────────────────────────────
	// In a real system these would be written to disk before responding to RPCs.
	// For this assignment, storing them in memory is acceptable.

	// currentTerm is the latest term this server has seen.
	// Initialized to 0 on first boot, increases monotonically.
	// INVARIANT: currentTerm is never decreased.
	currentTerm int64

	// votedFor is the candidateId this server voted for in currentTerm,
	// or -1 if it has not voted in the current term.
	// INVARIANT: a server votes for at most one candidate per term.
	votedFor int32

	// log contains all log entries. 1-indexed (index 0 is a sentinel).
	// INVARIANT: log[i].Term <= log[i+1].Term for all i.
	log *ilog.Log

	// ── Volatile state on all servers (Figure 2) ───────────────────────────

	// commitIndex is the index of the highest log entry known to be committed.
	// Initialized to 0, increases monotonically.
	// INVARIANT: commitIndex <= lastApplied is never true (apply must follow commit).
	commitIndex int64

	// lastApplied is the index of the highest log entry applied to the state machine.
	// Initialized to 0, increases monotonically.
	// INVARIANT: lastApplied <= commitIndex always.
	lastApplied int64

	// ── Volatile state on leaders (Figure 2) ───────────────────────────────
	// Reinitialized to their starting values after every election.

	// nextIndex[i] is the index of the next log entry to send to peer i.
	// Initialized to leader's lastIndex+1 after election.
	// INVARIANT: nextIndex[i] >= 1 always.
	nextIndex []int64

	// matchIndex[i] is the index of the highest log entry known to be replicated
	// on peer i. Initialized to 0 after election.
	// INVARIANT: matchIndex[i] < nextIndex[i] always.
	matchIndex []int64

	// ── Node identity ──────────────────────────────────────────────────────

	id    int32
	peers []config.NodeConfig // all nodes including self

	// ── Role and synchronization ───────────────────────────────────────────

	state         RaftState
	electionTimer *time.Timer

	// ── Communication ──────────────────────────────────────────────────────

	// peerConns holds gRPC connections to peer nodes.
	// Initialized in New(); reused across RPCs.
	peerConns map[int32]pb.RaftRPCClient

	// commitCh is written by the apply goroutine (see applyLoop).
	// The server reads from this channel to apply committed entries.
	commitCh chan ApplyMsg

	// dead is set to true by Kill(); used to stop background goroutines.
	dead bool
}

// New creates and starts a Raft instance for the node with the given id.
// commitCh will receive ApplyMsg values for each committed log entry.
// New starts background goroutines; call Kill() to stop them.
func New(id int32, cfg *config.ClusterConfig, commitCh chan ApplyMsg) *Raft {
	rf := NewPaused(id, cfg, commitCh)
	for _, p := range cfg.Peers(id) {
		conn, err := grpc.NewClient(p.PeerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("raft: dial peer %d at %s: %v", p.ID, p.PeerAddr, err)
		}
		rf.peerConns[p.ID] = pb.NewRaftRPCClient(conn)
	}
	rf.Resume()
	return rf
}

// NewPaused creates a Raft instance without starting background goroutines or
// the election timer. Call SetPeerClient for each peer, then Resume to begin
// normal operation. Intended for tests that need to inject in-process transports
// before any RPCs are attempted.
func NewPaused(id int32, cfg *config.ClusterConfig, commitCh chan ApplyMsg) *Raft {
	peers := cfg.Nodes
	n := len(peers)
	return &Raft{
		id:         id,
		peers:      peers,
		log:        ilog.New(),
		votedFor:   -1,
		state:      Follower,
		nextIndex:  make([]int64, n),
		matchIndex: make([]int64, n),
		peerConns:  make(map[int32]pb.RaftRPCClient),
		commitCh: commitCh,
	}
}

// Resume starts the election timer and apply goroutine. Must be called exactly
// once after NewPaused, after all peer clients have been wired via SetPeerClient.
func (rf *Raft) Resume() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()
	go rf.applyLoop()
}

// SetPeerClient replaces the gRPC client used to reach peer id. Used in tests
// to inject bufconn-backed or partitioning proxy clients.
func (rf *Raft) SetPeerClient(id int32, client pb.RaftRPCClient) {
	rf.mu.Lock()
	rf.peerConns[id] = client
	rf.mu.Unlock()
}

// ── Public API ────────────────────────────────────────────────────────────

// Start submits a command to the Raft log. If this node is the leader, the
// command will be replicated to a majority and eventually committed.
//
// Returns:
//   - index:    the log index at which the command will appear if committed
//   - term:     the current term
//   - isLeader: true if this node is currently the leader
//
// If isLeader is false, the command was not submitted and the caller should
// redirect the client to the current leader.
//
// The caller must watch commitCh for an ApplyMsg with the returned index AND
// term. If a different entry appears at that index (different term), the
// original command was lost due to a leadership change.
func (rf *Raft) Start(command string) (index int64, term int64, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}

	// TODO (Stage 2): append command to log, kick off replication
	return 0, rf.currentTerm, false // placeholder
}

// GetState returns the current term and whether this node believes it is leader.
func (rf *Raft) GetState() (term int64, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Kill signals all background goroutines to stop. Used by tests.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.dead = true
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.dead
}

// ── RPC handlers (called by the gRPC server) ──────────────────────────────

// RequestVote handles a vote request from a candidate.
//
// Grant the vote if ALL of the following hold (Raft paper §5.2, §5.4):
//  1. req.Term >= rf.currentTerm (if >, update term and step down to follower)
//  2. rf.votedFor is -1 or req.CandidateId (haven't voted for someone else)
//  3. Candidate's log is at least as up-to-date as ours (election restriction):
//     req.LastLogTerm > rf.log.LastTerm(), OR
//     req.LastLogTerm == rf.log.LastTerm() && req.LastLogIndex >= rf.log.LastIndex()
//
// If the vote is granted, reset the election timer.
func (rf *Raft) RequestVote(_ context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO (Stage 1) - Implement the Raft Vote Handling Logic here
	// Replace the following placeholder return statement with your implementation.
	return &pb.RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}, nil
}

// AppendEntries handles a log replication RPC from the leader.
//
// Stage 1: Use this as a heartbeat — reset the election timer and update term.
// Stage 2: Implement full log replication (consistency check, truncate, append, commit).
//
// Reject if req.Term < rf.currentTerm.
// If req.Term > rf.currentTerm: update term, step down to follower.
func (rf *Raft) AppendEntries(_ context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO Stage 1: Perform the actions when a leader heartbeat is received.
	// TODO Stage 2: Implement the log replication logic here.
	return &pb.AppendEntriesReply{Term: rf.currentTerm, Success: false}, nil
}

// InstallSnapshot handles a snapshot from the leader (optional extension only).
func (rf *Raft) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &pb.InstallSnapshotReply{Term: rf.currentTerm}, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────

// resetElectionTimer resets the election timer with a new random timeout.
// Must be called with mu held.
func (rf *Raft) resetElectionTimer() {
	d := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	if rf.electionTimer == nil {
		rf.electionTimer = time.AfterFunc(d, rf.startElection)
	} else {
		rf.electionTimer.Reset(d)
	}
}

// becomeFollower transitions to follower state and updates the term.
// Must be called with mu held.
func (rf *Raft) becomeFollower(term int64) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.resetElectionTimer()
}

// startElection is called when the election timer fires. It runs in its own
// goroutine (time.AfterFunc). The node increments its term, votes for itself,
// and sends RequestVote RPCs to all peers in parallel.
//
// TODO (Stage 1): implement this method.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader || rf.dead {
		rf.mu.Unlock()
		return
	}
	// TODO: increment term, set state=Candidate, vote for self, build RequestVoteArgs

	rf.mu.Unlock()

	// TODO: send RequestVote to all peers in parallel goroutines
	// TODO: count votes; if majority, call becomeLeader()
}

// becomeLeader transitions to leader state, initializes nextIndex/matchIndex,
// and starts the heartbeat loop.
//

func (rf *Raft) becomeLeader() {
	// Must be called with mu held.
	rf.state = Leader

	// TODO (Stage 1): implement this method.
	// TODO: start sendHeartbeats goroutine
}

// sendHeartbeats sends empty AppendEntries to all peers every HeartbeatInterval.
// Runs until this node is no longer leader or is killed.
//
// TODO (Stage 1): implement. Stage 2: send actual log entries instead of empty.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		// TODO: compile the list of peers to send heartbeats to (all except self)
		rf.mu.Unlock()
		// TODO: send the Heartbeat messages (AppendEntries) to all peers
		time.Sleep(HeartbeatInterval)
	}
}

// sendAppendEntries sends an AppendEntries RPC to one peer. Used by both
// the heartbeat loop (Stage 1) and the replication loop (Stage 2).
//
// TODO (Stage 2): implement log replication logic here.
func (rf *Raft) sendAppendEntries(peerID int32) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// TODO (Stage 2): build args using nextIndex[peerID]; send RPC; handle response
	rf.mu.Unlock()
}

// advanceCommitIndex checks whether any new entries can be committed.
// An entry at index N is committed if:
//   - log[N].Term == rf.currentTerm (leader only commits its own term's entries)
//   - A majority of nodes have matchIndex[i] >= N
//
// Must be called with mu held.
//
// TODO (Stage 2): implement this.
func (rf *Raft) advanceCommitIndex() {
	// TODO (Stage 2)
}

// applyLoop runs in a goroutine, draining entries from lastApplied to
// commitIndex and sending them on commitCh. This is the only goroutine that
// writes to commitCh and updates lastApplied.
//
// TODO (Stage 2): implement this.
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		// TODO: sleep, check if lastApplied < commitIndex, send ApplyMsg
		time.Sleep(10 * time.Millisecond)
	}
}

// quorum returns the minimum number of nodes (including self) needed for a majority.
func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}

// peerClient returns the gRPC client for the given peer ID, or nil if not found.
func (rf *Raft) peerClient(id int32) pb.RaftRPCClient {
	return rf.peerConns[id]
}

// callRequestVote sends a RequestVote RPC to peer and returns the reply.
// Must NOT be called with mu held (RPC can block).
func (rf *Raft) callRequestVote(peerID int32, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	client := rf.peerClient(peerID)
	if client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	return client.RequestVote(ctx, args)
}

// callAppendEntries sends an AppendEntries RPC to peer and returns the reply.
// Must NOT be called with mu held (RPC can block).
func (rf *Raft) callAppendEntries(peerID int32, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	client := rf.peerClient(peerID)
	if client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	return client.AppendEntries(ctx, args)
}
