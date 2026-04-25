// Test infrastructure for HW3 server tests.
//
// Uses bufconn (in-memory gRPC transport) for both the KVStore client-facing
// service and the RaftRPC peer-to-peer service, so tests run without real TCP
// ports and work cleanly on Gradescope.
//
// TestServer wraps a Server with two bufconn listeners (KV and Raft) and a
// pre-wired KVStore gRPC client. TestCluster wires three TestServers together
// via proxyClient so that Raft peer RPCs flow over bufconn, and partitions can
// be injected by toggling the proxy.
package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"kvraft/config"
	pb "kvraft/proto"
	"kvraft/raft"
	"kvraft/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1 MB

// ── proxyClient ───────────────────────────────────────────────────────────────
//
// Implements pb.RaftRPCClient; drops all RPCs when connected=false.

type proxyClient struct {
	mu        sync.Mutex
	inner     pb.RaftRPCClient
	connected bool
}

func (p *proxyClient) drop() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.connected
}

func (p *proxyClient) RequestVote(ctx context.Context, in *pb.RequestVoteArgs, opts ...grpc.CallOption) (*pb.RequestVoteReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.RequestVote(ctx, in, opts...)
}
func (p *proxyClient) AppendEntries(ctx context.Context, in *pb.AppendEntriesArgs, opts ...grpc.CallOption) (*pb.AppendEntriesReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.AppendEntries(ctx, in, opts...)
}
func (p *proxyClient) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotArgs, opts ...grpc.CallOption) (*pb.InstallSnapshotReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.InstallSnapshot(ctx, in, opts...)
}

// ── TestServer ────────────────────────────────────────────────────────────────

type TestServer struct {
	srv      *Server
	kvLis    *bufconn.Listener
	raftLis  *bufconn.Listener
	kvGRPC   *grpc.Server
	raftGRPC *grpc.Server
	KVClient pb.KVStoreClient
	kvConn   *grpc.ClientConn
	proxies  [3]*proxyClient // proxies[j] controls this server's Raft link to node j
}

// ── TestCluster ───────────────────────────────────────────────────────────────

type TestCluster struct {
	Servers [3]*TestServer
}

func testCfg() *config.ClusterConfig {
	return &config.ClusterConfig{
		Nodes: []config.NodeConfig{
			{ID: 0, ClientAddr: "test:7000", PeerAddr: "test:7100"},
			{ID: 1, ClientAddr: "test:7001", PeerAddr: "test:7101"},
			{ID: 2, ClientAddr: "test:7002", PeerAddr: "test:7102"},
		},
	}
}

// newTestCluster creates a 3-node cluster wired over bufconn.
//
// Raft instances are created via raft.NewPaused so that peerConns can be fully
// wired before any election timers fire. All three nodes are resumed together.
func newTestCluster(t *testing.T) *TestCluster {
	t.Helper()
	cfg := testCfg()
	tc := &TestCluster{}

	// Phase 1 — create all servers (Raft paused) and their gRPC listeners.
	for i := range 3 {
		tc.Servers[i] = newTestServer(t, int32(i), cfg)
	}

	// Phase 2 — wire Raft peer connections via proxyClient.
	for i := range 3 {
		for j := range 3 {
			if i == j {
				continue
			}
			j := j
			conn, err := grpc.NewClient(
				"passthrough://bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					ts := tc.Servers[j]
					if ts == nil {
						return nil, fmt.Errorf("node %d has been killed", j)
					}
					return ts.raftLis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatalf("dial raft bufconn %d→%d: %v", i, j, err)
			}
			proxy := &proxyClient{inner: pb.NewRaftRPCClient(conn), connected: true}
			tc.Servers[i].proxies[j] = proxy
			tc.Servers[i].srv.rf.SetPeerClient(int32(j), proxy)
		}
	}

	// Phase 3 — resume all nodes simultaneously.
	for _, ts := range tc.Servers {
		ts.srv.rf.Resume()
	}

	t.Cleanup(func() {
		for _, ts := range tc.Servers {
			if ts != nil {
				ts.srv.rf.Kill()
				ts.kvGRPC.Stop()
				ts.raftGRPC.Stop()
				ts.kvConn.Close()
				ts.kvLis.Close()
				ts.raftLis.Close()
			}
		}
	})

	return tc
}

// newTestServer creates a single server node with paused Raft and two bufconn
// gRPC servers. The caller is responsible for wiring peerConns and calling Resume.
func newTestServer(t *testing.T, id int32, cfg *config.ClusterConfig) *TestServer {
	t.Helper()

	commitCh := make(chan raft.ApplyMsg, 100)
	rf := raft.NewPaused(id, cfg, commitCh)

	// Construct Server directly (package-internal access) so we control Raft.
	s := &Server{
		id:      id,
		cfg:     cfg,
		rf:      rf,
		st:      store.New(),
		pending: make(map[int64]*pendingPut),
	}
	go s.applyLoop(commitCh)

	kvLis := bufconn.Listen(bufSize)
	raftLis := bufconn.Listen(bufSize)

	kvGRPC := grpc.NewServer()
	pb.RegisterKVStoreServer(kvGRPC, s)

	raftGRPC := grpc.NewServer()
	pb.RegisterRaftRPCServer(raftGRPC, s)

	go kvGRPC.Serve(kvLis)   //nolint:errcheck
	go raftGRPC.Serve(raftLis) //nolint:errcheck

	kvConn := dialBufconn(t, kvLis)

	return &TestServer{
		srv:      s,
		kvLis:    kvLis,
		raftLis:  raftLis,
		kvGRPC:   kvGRPC,
		raftGRPC: raftGRPC,
		KVClient: pb.NewKVStoreClient(kvConn),
		kvConn:   kvConn,
	}
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialBufconn: %v", err)
	}
	return conn
}

// ── Cluster helpers ───────────────────────────────────────────────────────────

// KillServer simulates a hard crash: stops the Raft instance and both gRPC servers.
func (tc *TestCluster) KillServer(id int) {
	ts := tc.Servers[id]
	ts.srv.rf.Kill()
	ts.kvGRPC.Stop()
	ts.raftGRPC.Stop()
	ts.kvLis.Close()
	ts.raftLis.Close()
	tc.Servers[id] = nil
}

// partition makes server from unable to send Raft RPCs to server to (one-way).
func (tc *TestCluster) partition(from, to int) {
	tc.Servers[from].proxies[to].mu.Lock()
	tc.Servers[from].proxies[to].connected = false
	tc.Servers[from].proxies[to].mu.Unlock()
}

// heal restores the one-way Raft RPC link from → to.
func (tc *TestCluster) heal(from, to int) {
	tc.Servers[from].proxies[to].mu.Lock()
	tc.Servers[from].proxies[to].connected = true
	tc.Servers[from].proxies[to].mu.Unlock()
}

// isolate disconnects server id from all others (bidirectionally).
func (tc *TestCluster) isolate(id int) {
	for j := range 3 {
		if j == id {
			continue
		}
		tc.partition(id, j)
		tc.partition(j, id)
	}
}

// reconnect restores all connections to and from server id.
func (tc *TestCluster) reconnect(id int) {
	for j := range 3 {
		if j == id || tc.Servers[j] == nil {
			continue
		}
		tc.heal(id, j)
		tc.heal(j, id)
	}
}

// leaderServer returns the TestServer that is currently the Raft leader, or nil.
func (tc *TestCluster) leaderServer() *TestServer {
	for _, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		_, isLeader := ts.srv.rf.GetState()
		if isLeader {
			return ts
		}
	}
	return nil
}

// leaderID returns the index of the current leader, or -1.
func (tc *TestCluster) leaderID() int {
	for i, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		_, isLeader := ts.srv.rf.GetState()
		if isLeader {
			return i
		}
	}
	return -1
}

// waitForLeader polls until a leader is found, failing the test on timeout.
func (tc *TestCluster) waitForLeader(t *testing.T) *TestServer {
	t.Helper()
	var leader *TestServer
	ok := pollUntil(func() bool {
		leader = tc.leaderServer()
		return leader != nil
	}, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatal("no leader elected within 2s")
	}
	return leader
}

// ── Assertion helpers ─────────────────────────────────────────────────────────

func requirePutOK(t *testing.T, resp *pb.PutResponse, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Put RPC error: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("Put: got ok=false (redirect_addr=%q), want ok=true", resp.RedirectAddr)
	}
}

func requireGetFound(t *testing.T, resp *pb.GetResponse, err error, wantValue string) {
	t.Helper()
	if err != nil {
		t.Fatalf("Get RPC error: %v", err)
	}
	if !resp.Found {
		t.Fatal("Get: got found=false, want true")
	}
	if resp.Value != wantValue {
		t.Fatalf("Get: got value=%q, want %q", resp.Value, wantValue)
	}
}

// ── Timing helpers ────────────────────────────────────────────────────────────

// testCtx returns a context that times out after 10 seconds.
// Tests with multiple sequential operations (election + put + verification)
// need headroom beyond the individual operation timeouts.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
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
