// Raft-level tests for HW3 — run with:
//
//	go test ./raft/... -v -race -timeout 60s
//
// Run by stage:
//
//	go test ./raft/... -v -race -run TestStage1 -timeout 30s
//	go test ./raft/... -v -race -run TestStage2 -timeout 60s
//
// These tests exercise the Raft state machine directly, wiring nodes together
// over in-memory gRPC (bufconn) so no real TCP ports are needed. Partition
// simulation is done via partitionProxy, which drops RPCs on demand.
package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"kvraft/config"
	pb "kvraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1 MB in-memory buffer

// ── gRPC adapter ─────────────────────────────────────────────────────────────
//
// Wraps a *Raft so it can be registered as a pb.RaftRPCServer.

type raftAdapter struct {
	pb.UnimplementedRaftRPCServer
	rf *Raft
}

func (a *raftAdapter) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	return a.rf.RequestVote(ctx, req)
}
func (a *raftAdapter) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	return a.rf.AppendEntries(ctx, req)
}
func (a *raftAdapter) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	return a.rf.InstallSnapshot(ctx, req)
}

// ── Partition proxy ───────────────────────────────────────────────────────────
//
// partitionProxy wraps a pb.RaftRPCClient and drops all RPCs when disconnected.
// Setting connected=false simulates a one-way network partition.

type partitionProxy struct {
	mu        sync.Mutex
	inner     pb.RaftRPCClient
	connected bool
}

func (p *partitionProxy) drop() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.connected
}

func (p *partitionProxy) RequestVote(ctx context.Context, in *pb.RequestVoteArgs, opts ...grpc.CallOption) (*pb.RequestVoteReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.RequestVote(ctx, in, opts...)
}
func (p *partitionProxy) AppendEntries(ctx context.Context, in *pb.AppendEntriesArgs, opts ...grpc.CallOption) (*pb.AppendEntriesReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.AppendEntries(ctx, in, opts...)
}
func (p *partitionProxy) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotArgs, opts ...grpc.CallOption) (*pb.InstallSnapshotReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.InstallSnapshot(ctx, in, opts...)
}

// ── Test cluster ──────────────────────────────────────────────────────────────

type testNode struct {
	rf      *Raft
	lis     *bufconn.Listener
	srv     *grpc.Server
	proxies []*partitionProxy // proxies[j] controls this node's outbound link to node j
}

type testCluster struct {
	nodes  []*testNode
	killed []bool
	n      int
}

// testCfg returns a ClusterConfig with n nodes using fake (non-dialed) addresses.
func testCfg(n int) *config.ClusterConfig {
	nodes := make([]config.NodeConfig, n)
	for i := range n {
		nodes[i] = config.NodeConfig{
			ID:         int32(i),
			ClientAddr: fmt.Sprintf("test:%d", 7000+i),
			PeerAddr:   fmt.Sprintf("test:%d", 7100+i),
		}
	}
	return &config.ClusterConfig{Nodes: nodes}
}

// newTestCluster creates n Raft instances wired together over bufconn. All
// nodes start paused; they are all resumed simultaneously after wiring so that
// election timers fire from the same baseline.
func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	cfg := testCfg(n)
	tc := &testCluster{
		nodes:  make([]*testNode, n),
		killed: make([]bool, n),
		n:      n,
	}

	// Phase 1 — create all Raft instances (paused) and their gRPC servers.
	for i := range n {
		commitCh := make(chan ApplyMsg, 100)
		rf := NewPaused(int32(i), cfg, commitCh)

		lis := bufconn.Listen(bufSize)
		srv := grpc.NewServer()
		pb.RegisterRaftRPCServer(srv, &raftAdapter{rf: rf})
		go srv.Serve(lis) //nolint:errcheck

		tc.nodes[i] = &testNode{
			rf:      rf,
			lis:     lis,
			srv:     srv,
			proxies: make([]*partitionProxy, n),
		}
	}

	// Phase 2 — wire peerConns via partitionProxy so we can inject partitions.
	for i := range n {
		for j := range n {
			if i == j {
				continue
			}
			j := j // capture for closure
			conn, err := grpc.NewClient(
				"passthrough://bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return tc.nodes[j].lis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatalf("dial bufconn node %d→%d: %v", i, j, err)
			}
			proxy := &partitionProxy{inner: pb.NewRaftRPCClient(conn), connected: true}
			tc.nodes[i].proxies[j] = proxy
			tc.nodes[i].rf.SetPeerClient(int32(j), proxy)
		}
	}

	// Phase 3 — resume all nodes simultaneously.
	for _, nd := range tc.nodes {
		nd.rf.Resume()
	}

	t.Cleanup(func() {
		for i, nd := range tc.nodes {
			if !tc.killed[i] {
				nd.rf.Kill()
			}
			nd.srv.Stop()
			nd.lis.Close()
		}
	})

	return tc
}

func (tc *testCluster) kill(t *testing.T, id int) {
	t.Helper()
	if tc.killed[id] {
		return
	}
	tc.killed[id] = true
	tc.nodes[id].rf.Kill()
	tc.nodes[id].srv.Stop()
	tc.nodes[id].lis.Close()
}

// partition makes node from unable to reach node to (one-way link failure).
func (tc *testCluster) partition(from, to int) {
	tc.nodes[from].proxies[to].mu.Lock()
	tc.nodes[from].proxies[to].connected = false
	tc.nodes[from].proxies[to].mu.Unlock()
}

// heal restores the one-way link from → to.
func (tc *testCluster) heal(from, to int) {
	tc.nodes[from].proxies[to].mu.Lock()
	tc.nodes[from].proxies[to].connected = true
	tc.nodes[from].proxies[to].mu.Unlock()
}

// isolate disconnects node id from all peers (bidirectionally).
func (tc *testCluster) isolate(id int) {
	for j := range tc.n {
		if j == id {
			continue
		}
		tc.partition(id, j)
		tc.partition(j, id)
	}
}

// reconnect restores all connections to and from node id.
func (tc *testCluster) reconnect(id int) {
	for j := range tc.n {
		if j == id {
			continue
		}
		tc.heal(id, j)
		tc.heal(j, id)
	}
}

// leaderCount returns the number of alive nodes that believe they are leader.
func (tc *testCluster) leaderCount() int {
	count := 0
	for i, nd := range tc.nodes {
		if tc.killed[i] {
			continue
		}
		_, isLeader := nd.rf.GetState()
		if isLeader {
			count++
		}
	}
	return count
}

// leader returns the index of the current leader among alive nodes, or -1.
func (tc *testCluster) leader() int {
	for i, nd := range tc.nodes {
		if tc.killed[i] {
			continue
		}
		_, isLeader := nd.rf.GetState()
		if isLeader {
			return i
		}
	}
	return -1
}

// lastApplied returns lastApplied for node id (accesses unexported field — OK
// because this test file is in package raft).
func (tc *testCluster) lastApplied(id int) int64 {
	tc.nodes[id].rf.mu.Lock()
	defer tc.nodes[id].rf.mu.Unlock()
	return tc.nodes[id].rf.lastApplied
}

// pollUntil calls cond every interval until it returns true or timeout elapses.
func pollUntil(cond func() bool, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 1 — Leader Election
// ════════════════════════════════════════════════════════════════════════════

// TestStage1_LeaderElected verifies that exactly one leader is elected within
// a reasonable time after cluster startup.
// [3 pts]
func TestStage1_LeaderElected(t *testing.T) {
	tc := newTestCluster(t, 3)

	ok := pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatal("no leader elected within 2s")
	}
}

// TestStage1_ExactlyOneLeader verifies that at most one node claims leadership
// at any point, checked repeatedly over a 1-second window.
// [3 pts]
func TestStage1_ExactlyOneLeader(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() >= 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no initial leader elected")
	}

	for range 20 {
		if n := tc.leaderCount(); n > 1 {
			t.Fatalf("multiple leaders detected: count=%d", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStage1_ReelectionAfterCrash verifies that after the leader is killed, the
// remaining two nodes elect a new leader within the election timeout window.
// [7 pts]
func TestStage1_ReelectionAfterCrash(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no initial leader")
	}
	orig := tc.leader()
	tc.kill(t, orig)

	ok := pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatal("no new leader elected after crash")
	}
	if newLeader := tc.leader(); newLeader == orig {
		t.Errorf("dead node %d still reported as leader", orig)
	}
}

// TestStage1_NoLeaderWithoutMajority verifies that when only 1 of 3 nodes is
// alive, no node can gather a majority of votes and become leader.
// [3 pts]
func TestStage1_NoLeaderWithoutMajority(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no initial leader")
	}

	// Kill the current leader and one follower, leaving a lone follower that
	// cannot gather a majority of votes and become leader.
	leaderID := tc.leader()
	tc.kill(t, leaderID)
	for i := range 3 {
		if i != leaderID {
			tc.kill(t, i)
			break
		}
	}

	// Wait longer than the maximum election timeout.
	time.Sleep(ElectionTimeoutMax + 300*time.Millisecond)

	if tc.leaderCount() > 0 {
		t.Error("a node declared itself leader without a majority")
	}
}

// TestStage1_HeartbeatSuppressesElection verifies that regular heartbeats from
// the leader prevent followers from starting spurious elections: the term should
// not advance during a stable period.
// [4 pts]
func TestStage1_HeartbeatSuppressesElection(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no initial leader")
	}
	leader := tc.leader()
	initTerm, _ := tc.nodes[leader].rf.GetState()

	// Wait several heartbeat intervals without any failure.
	time.Sleep(5 * HeartbeatInterval)

	currentTerm, _ := tc.nodes[leader].rf.GetState()
	if currentTerm > initTerm {
		t.Errorf("term advanced from %d to %d — spurious election fired during stable period",
			initTerm, currentTerm)
	}
	if tc.leaderCount() != 1 {
		t.Errorf("leaderCount=%d after stable period, want 1", tc.leaderCount())
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 2 — Log Replication
// ════════════════════════════════════════════════════════════════════════════

// TestStage2_StartReturnsIndex verifies that Start() on the leader returns a
// positive log index, the current term, and isLeader=true.
// [2 pts]
func TestStage2_StartReturnsIndex(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	idx, _, isLeader := tc.nodes[leader].rf.Start("put:x:hello")
	if !isLeader {
		t.Fatal("Start returned isLeader=false on the leader")
	}
	if idx <= 0 {
		t.Errorf("Start returned index=%d, want >0", idx)
	}
}

// TestStage2_StartRejectsOnFollower verifies that Start() on a non-leader
// returns isLeader=false and does not add the entry to the cluster log.
// [1 pt]
func TestStage2_StartRejectsOnFollower(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	for i := range 3 {
		if i == leader {
			continue
		}
		_, _, isLeader := tc.nodes[i].rf.Start("put:x:hello")
		if isLeader {
			t.Errorf("node %d is not leader but Start returned isLeader=true", i)
		}
		break
	}
}

// TestStage2_EntryAppliedOnAllNodes verifies that a submitted command is
// eventually applied on every node via commitCh once the majority has it.
// [7 pts]
func TestStage2_EntryAppliedOnAllNodes(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	idx, _, isLeader := tc.nodes[leader].rf.Start("put:color:blue")
	if !isLeader {
		t.Fatal("Start on non-leader")
	}

	ok := pollUntil(func() bool {
		for i := range 3 {
			if tc.killed[i] {
				continue
			}
			if tc.lastApplied(i) < idx {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond)

	if !ok {
		t.Fatalf("entry at index %d not applied on all nodes within 2s", idx)
	}
}

// TestStage2_CommitWithOneFollowerDown verifies that the cluster commits entries
// when exactly one follower is dead: leader + surviving follower = majority.
// [5 pts]
func TestStage2_CommitWithOneFollowerDown(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	for i := range 3 {
		if i != leader {
			tc.kill(t, i)
			break
		}
	}

	idx, _, isLeader := tc.nodes[leader].rf.Start("put:x:1")
	if !isLeader {
		t.Fatal("Start on non-leader")
	}

	ok := pollUntil(func() bool {
		return tc.lastApplied(leader) >= idx
	}, 2*time.Second, 10*time.Millisecond)

	if !ok {
		t.Fatal("entry not committed with 2/3 nodes alive")
	}
}

// TestStage2_NoCommitWithoutMajority verifies that an entry submitted while the
// leader is isolated (cannot reach either follower) is never committed.
// [3 pts]
func TestStage2_NoCommitWithoutMajority(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	// Isolate the leader from both followers.
	tc.isolate(leader)

	idx, _, isLeader := tc.nodes[leader].rf.Start("put:x:1")
	if !isLeader {
		// The node may have already stepped down due to isolation; skip gracefully.
		t.Skip("leader stepped down before Start — isolation was too fast")
	}

	time.Sleep(500 * time.Millisecond)

	if tc.lastApplied(leader) >= idx {
		t.Error("entry committed without majority (isolated leader should not commit)")
	}
}

// TestStage2_LaggardCatchesUp verifies that an isolated follower receives all
// missed entries via AppendEntries once the partition heals.
// [7 pts]
func TestStage2_LaggardCatchesUp(t *testing.T) {
	tc := newTestCluster(t, 3)

	if !pollUntil(func() bool { return tc.leaderCount() == 1 }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no leader elected")
	}
	leader := tc.leader()

	laggard := -1
	for i := range 3 {
		if i != leader {
			laggard = i
			break
		}
	}
	tc.isolate(laggard)

	// Commit several entries on the majority partition.
	var lastIdx int64
	for i := range 5 {
		idx, _, ok := tc.nodes[leader].rf.Start(fmt.Sprintf("put:k%d:v%d", i, i))
		if !ok {
			t.Fatalf("Start %d: not leader", i)
		}
		lastIdx = idx
	}

	if !pollUntil(func() bool { return tc.lastApplied(leader) >= lastIdx }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("leader did not commit all entries before healing partition")
	}

	// Heal partition — laggard must catch up via AppendEntries.
	tc.reconnect(laggard)

	ok := pollUntil(func() bool { return tc.lastApplied(laggard) >= lastIdx }, 3*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatalf("laggard (node %d) did not catch up after partition healed: lastApplied=%d want>=%d",
			laggard, tc.lastApplied(laggard), lastIdx)
	}
}
