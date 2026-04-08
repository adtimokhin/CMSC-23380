// Test infrastructure for kvstore server tests.
//
// Uses bufconn (in-memory gRPC transport) so tests run without real TCP ports
// and work cleanly on Gradescope without port conflicts.
//
// Expected student-added fields on Node (tests won't compile without these):
//
//	peerConns       map[int32]*grpc.ClientConn  // connections to peer Replication servers
//	mu              sync.Mutex                  // guards role, lastSeen, currentPrimaryID
//	lastSeen        map[int32]time.Time          // last heartbeat time per peer ID
//	currentPrimaryID int32                       // ID of the node we believe is primary
//	ctx             context.Context              // cancelled on node shutdown
//	cancel          context.CancelFunc           // call to stop background goroutines
//	onPeerDeadFunc  func(int32)                  // injectable hook (defaults to onPeerDead)
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"kvstore/config"
	pb "kvstore/proto"
)

const bufSize = 1 << 20 // 1 MB in-memory buffer

// testCfg is the shared cluster config used by all tests. Addresses are fake
// strings — real TCP is never used; bufconn handles all transport.
func testCfg() *config.ClusterConfig {
	return &config.ClusterConfig{
		Nodes: []config.NodeConfig{
			{ID: 0, ClientAddr: "test:7000", PeerAddr: "test:7100"},
			{ID: 1, ClientAddr: "test:7001", PeerAddr: "test:7101"},
			{ID: 2, ClientAddr: "test:7002", PeerAddr: "test:7102"},
		},
	}
}

// TestNode wraps a Node with in-process gRPC servers and pre-dialed clients.
// The KV (client-facing) and Replication (peer-to-peer) services each get
// their own bufconn listener, mirroring the production two-port layout.
type TestNode struct {
	node       *Node
	kvLis      *bufconn.Listener
	replLis    *bufconn.Listener
	kvSrv      *grpc.Server
	replSrv    *grpc.Server
	KVClient   pb.KVStoreClient
	ReplClient pb.ReplicationClient
	kvConn     *grpc.ClientConn
	replConn   *grpc.ClientConn
}

// newTestNode creates a Node and starts its two gRPC servers over bufconn.
// Both servers are stopped and connections closed on t.Cleanup.
func newTestNode(t *testing.T, id int32, cfg *config.ClusterConfig) *TestNode {
	t.Helper()

	n := newNode(id, cfg)

	// Give the node a cancellable context for background goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	n.ctx = ctx
	n.cancel = cancel

	kvLis := bufconn.Listen(bufSize)
	replLis := bufconn.Listen(bufSize)

	kvSrv := grpc.NewServer()
	pb.RegisterKVStoreServer(kvSrv, n)

	replSrv := grpc.NewServer()
	pb.RegisterReplicationServer(replSrv, n)

	go kvSrv.Serve(kvLis)   //nolint:errcheck
	go replSrv.Serve(replLis) //nolint:errcheck

	dialBufconn := func(lis *bufconn.Listener) *grpc.ClientConn {
		conn, err := grpc.NewClient(
			"passthrough://bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial bufconn: %v", err)
		}
		return conn
	}

	kvConn := dialBufconn(kvLis)
	replConn := dialBufconn(replLis)

	tn := &TestNode{
		node:       n,
		kvLis:      kvLis,
		replLis:    replLis,
		kvSrv:      kvSrv,
		replSrv:    replSrv,
		KVClient:   pb.NewKVStoreClient(kvConn),
		ReplClient: pb.NewReplicationClient(replConn),
		kvConn:     kvConn,
		replConn:   replConn,
	}

	t.Cleanup(func() {
		cancel()
		kvConn.Close()
		replConn.Close()
		kvSrv.Stop()
		replSrv.Stop()
	})

	return tn
}

// TestCluster wires three TestNodes together so that each node's peerConns
// points to the Replication ClientConns of the other two nodes — matching
// what production code would set up by dialing the peer addresses in config.
type TestCluster struct {
	Nodes [3]*TestNode
}

func newTestCluster(t *testing.T) *TestCluster {
	t.Helper()
	cfg := testCfg()
	tc := &TestCluster{}
	for i := range 3 {
		tc.Nodes[i] = newTestNode(t, int32(i), cfg)
	}
	// Wire peerConns: each node gets the replConn of every other node.
	for i := range 3 {
		conns := make(map[int32]*grpc.ClientConn)
		for j := range 3 {
			if i != j {
				conns[int32(j)] = tc.Nodes[j].replConn
			}
		}
		tc.Nodes[i].node.peerConns = conns
	}
	return tc
}

// KillNode simulates a hard crash of node id by closing its listeners.
// Subsequent RPC attempts to that node will fail with a connection error.
func (tc *TestCluster) KillNode(id int32) {
	tn := tc.Nodes[id]
	tn.node.cancel()
	tn.kvLis.Close()
	tn.replLis.Close()
}

// ── Context helpers ──────────────────────────────────────────────────────────

// testCtx returns a context that times out after 3 seconds.
// Suitable for individual RPC calls in tests.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ── Assertion helpers ────────────────────────────────────────────────────────

func requirePutOK(t *testing.T, resp *pb.PutResponse, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Put RPC error: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("Put: got ok=false (redirect_addr=%q), want ok=true", resp.RedirectAddr)
	}
	if resp.LamportTs <= 0 {
		t.Fatalf("Put: got lamport_ts=%d, want >0", resp.LamportTs)
	}
}

func requireGetFound(t *testing.T, resp *pb.GetResponse, err error, wantValue string) {
	t.Helper()
	if err != nil {
		t.Fatalf("Get RPC error: %v", err)
	}
	if !resp.Found {
		t.Fatalf("Get: got found=false, want true (key should exist)")
	}
	if resp.Value != wantValue {
		t.Fatalf("Get: got value=%q, want %q", resp.Value, wantValue)
	}
}

// pollUntil calls cond every interval until it returns true or deadline elapses.
// Returns true if cond succeeded, false on timeout.
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
