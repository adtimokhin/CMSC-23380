// kvstore_server — primary-backup replicated key-value store node.
//
// Each node acts as either a PRIMARY or BACKUP. The primary handles all Put
// requests from clients, replicates to both backups before acknowledging,
// and sends heartbeats to peers. Backups apply replicated writes and respond
// to heartbeats; when they detect the primary has stopped responding they
// elect a new primary.
//
// Run:
//
//	go run ./server --id=0 --config=nodeconfig.json
//	go run ./server --id=1 --config=nodeconfig.json
//	go run ./server --id=2 --config=nodeconfig.json
//
// Node 0 starts as the primary. Nodes 1 and 2 start as backups.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"kvstore/config"
	"kvstore/internal/clock"
	"kvstore/internal/store"
	pb "kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Role represents the current role of this node.
type Role int32

const (
	RoleBackup    Role = 0
	RolePrimary   Role = 1
	RoleCandidate Role = 2
)

// TODO (Stage 3): tune these constants or make them flags.
var (
	HeartbeatInterval = 500 * time.Millisecond
	HeartbeatTimeout  = 1000 * time.Millisecond
	ReplicateTimeout  = 1000 * time.Millisecond
)

// Node is the core data structure for a kvstore node. It embeds both gRPC
// service implementations (KVStore and Replication) and holds all shared
// state behind a single mutex.
//
// All fields are declared here upfront so the test binary compiles at every
// stage. You implement each field's behaviour as you progress:
//   - Stage 1: clk, store, id, role, cfg
//   - Stage 2: peerConns (set up in main before starting servers)
//   - Stage 3: mu, lastSeen, currentPrimaryID, ctx, cancel, onPeerDeadFunc
//   - Stage 4: electionTs
type Node struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedReplicationServer

	id   int32
	role Role
	cfg  *config.ClusterConfig

	clk   *clock.LamportClock
	store *store.Store

	// Stage 2: pre-established gRPC connections to peer Replication servers.
	// Populate in main() by dialing each peer's PeerAddr before starting servers.
	peerConns map[int32]*grpc.ClientConn

	// Stage 3: shared state protected by mu.
	mu               sync.Mutex
	lastSeen         map[int32]time.Time // wall-clock time of last heartbeat per peer
	currentPrimaryID int32               // ID of the node we currently believe is primary

	// Stage 3: context for background goroutines (startHeartbeatLoop, monitorPeers).
	// Create with context.WithCancel in main() and assign here; the test helper
	// sets these directly so tests clean up goroutines on t.Cleanup.
	ctx    context.Context
	cancel context.CancelFunc

	// Stage 3: called by monitorPeers when a peer times out. Defaults to
	// n.onPeerDead; overridden in tests to observe detection without side effects.
	onPeerDeadFunc func(int32)

	// Stage 4: Lamport clock snapshot taken at the start of runElection (after
	// clock.Tick()). Use this in AnnounceLeader instead of n.clk.Now() — a
	// concurrent heartbeat handler can advance the live clock mid-election.
	electionTs int64
}

func newNode(id int32, cfg *config.ClusterConfig) *Node {
	role := RoleBackup
	if id == 0 {
		role = RolePrimary
	}
	return &Node{
		id:               id,
		role:             role,
		cfg:              cfg,
		clk:              clock.New(),
		store:            store.New(),
		lastSeen:         make(map[int32]time.Time),
		currentPrimaryID: 0, // node 0 is the initial primary
	}
}

// ── KVStore service (client-facing) ──────────────────────────────────────

// Put handles a write from a client.
//
// Stage 1: Implement for a single node (no replication).
//   - Tick the Lamport clock.
//   - Store the key/value locally.
//   - Return PutResponse{ok: true, lamport_ts: ts}.
//
// Stage 2: Add replication fan-out to both backups before writing locally.
//
// Stage 4: Return redirect_addr when this node is not the primary.
func (n *Node) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	ts := n.clk.Tick()
	n.store.Put(req.Key, req.Value, ts)
	return &pb.PutResponse{Ok: true, LamportTs: ts}, nil
}

// Get handles a read from a client.
//
// Stage 1: Return the value from the local store (reads may be stale on backups).
func (n *Node) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	entry, ok := n.store.Get(req.Key)
	if !ok {
		return &pb.GetResponse{Found: false}, nil
	}
	return &pb.GetResponse{Found: true, Value: entry.Value, LamportTs: entry.Ts}, nil
}

// GetPrimary returns the client-facing address and ID of the current primary.
//
// Stage 1: Return this node's own address (assume single-node).
// Stage 4: Return currentPrimaryID and its client address.
func (n *Node) GetPrimary(_ context.Context, _ *pb.Empty) (*pb.GetPrimaryResponse, error) {
	self, err := n.cfg.Self(n.id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "config: %v", err)
	}
	return &pb.GetPrimaryResponse{PrimaryAddr: self.ClientAddr, PrimaryId: n.id}, nil
}

// ── Replication service (internal, peer-to-peer) ─────────────────────────

// Replicate applies a write forwarded by the primary.
//
// Stage 2:
//   - Apply the Lamport max-rule: clock.Update(req.LamportTs).
//   - Write to the local store.
//   - Return ReplicateResponse{ok: true, lamport_ts: updated_clock}.
func (n *Node) Replicate(_ context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	return nil, status.Error(codes.Unimplemented, "TODO: implement Replicate (Stage 2)")
}

// Heartbeat responds to a heartbeat from a peer.
//
// Stage 3:
//   - Update lastSeen[req.SenderID] = time.Now().
//   - Apply Lamport max-rule on req.LamportTs.
//   - Return own {id, lamport_ts, role}.
func (n *Node) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return nil, status.Error(codes.Unimplemented, "TODO: implement Heartbeat (Stage 3)")
}

// AnnounceLeader handles a leader announcement from a candidate backup.
//
// Stage 4:
//   - Determine localTs: if this node is also a candidate this round, use
//     n.electionTs (the clock snapshot from when YOU started your election).
//     Otherwise use n.clk.Now(). This prevents a concurrent heartbeat from
//     advancing the clock and producing a wrong comparison result.
//   - Accept (return accepted=true, set currentPrimaryID = req.NewPrimaryId) if:
//       req.ElectionLamport > localTs
//       OR (req.ElectionLamport == localTs AND req.NewPrimaryId > n.id)
//   - Otherwise reject (return accepted=false). The caller wins or yields on its own.
//
// Note: two backups frequently start elections simultaneously with equal clocks
// because heartbeat exchanges synchronize clocks. Node ID is the tiebreaker:
// higher ID wins.
func (n *Node) AnnounceLeader(_ context.Context, req *pb.AnnounceLeaderRequest) (*pb.AnnounceLeaderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "TODO: implement AnnounceLeader (Stage 4)")
}

// ── Background goroutines ─────────────────────────────────────────────────

// startHeartbeatLoop runs in a goroutine, sending Heartbeat RPCs to all
// peers every HeartbeatInterval. Use context.WithTimeout on each RPC so that
// a slow/hung peer does not block the loop.
//
// Stage 3: implement this.
func (n *Node) startHeartbeatLoop() {
	// TODO (Stage 3)
}

// monitorPeers runs in a goroutine, checking lastSeen for each peer.
// If time.Since(lastSeen[peer]) > HeartbeatTimeout, call onPeerDead(peerID).
//
// Stage 3: implement detection logic.
// Stage 4: implement election trigger in onPeerDead.
func (n *Node) monitorPeers() {
	// TODO (Stage 3)
}

// onPeerDead is called when monitorPeers decides a peer has failed.
// If the dead peer was the current primary, trigger runElection().
//
// Stage 4: implement this.
func (n *Node) onPeerDead(peerID int32) {
	log.Printf("[node %d] suspect: peer %d unresponsive", n.id, peerID)
	// TODO (Stage 4): if peerID == currentPrimaryID && n.role == RoleBackup { go n.runElection() }
}

// runElection implements the simplified Lamport-bully election:
//  1. Under the mutex: if role != RoleBackup, return immediately — another
//     goroutine already started an election. Set role = RoleCandidate, tick
//     the clock, capture electionTs = clk.Now(), and store it in n.electionTs
//     so AnnounceLeader can use it as a stable comparison point.
//  2. Send AnnounceLeader(id=self, election_lamport=electionTs) to the ONE
//     other backup. To find it: iterate peerConns and skip currentPrimaryID
//     (that is the node that just died).
//  3. If accepted: set role = RolePrimary and currentPrimaryID = n.id. Then
//     broadcast AnnounceLeader to ALL peers so the losing backup learns who
//     won and can update its own currentPrimaryID.
//  4. If rejected or unreachable: set role back to RoleBackup. The other
//     backup has a stronger claim and will broadcast the result to inform you.
//
// Stage 4: implement this.
func (n *Node) runElection() {
	// TODO (Stage 4)
}

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	idFlag := flag.Int("id", -1, "node ID (0, 1, or 2)")
	cfgPath := flag.String("config", "nodeconfig.json", "path to nodeconfig.json")
	flag.Parse()

	if *idFlag < 0 {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: kvstore_server --id=<0|1|2> [--config=nodeconfig.json]")
		flag.PrintDefaults()
		log.Fatal("--id is required")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	self, err := cfg.Self(int32(*idFlag))
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	node := newNode(self.ID, cfg)

	// TODO (Stage 2): dial peer Replication servers and populate node.peerConns.
	// Use cfg.Peers(self.ID) to get the list of peer NodeConfigs, then call
	// grpc.NewClient(peer.PeerAddr, ...) for each and store in node.peerConns.

	// TODO (Stage 3): create a context for background goroutines:
	//   ctx, cancel := context.WithCancel(context.Background())
	//   node.ctx = ctx
	//   node.cancel = cancel
	//   defer cancel()

	// Start the KVStore gRPC server (client-facing).
	clientLis, err := net.Listen("tcp", self.ClientAddr)
	if err != nil {
		log.Fatalf("listen client addr %s: %v", self.ClientAddr, err)
	}
	clientSrv := grpc.NewServer()
	pb.RegisterKVStoreServer(clientSrv, node)

	// Start the Replication gRPC server (peer-to-peer).
	peerLis, err := net.Listen("tcp", self.PeerAddr)
	if err != nil {
		log.Fatalf("listen peer addr %s: %v", self.PeerAddr, err)
	}
	peerSrv := grpc.NewServer()
	pb.RegisterReplicationServer(peerSrv, node)

	roleStr := "backup"
	if node.role == RolePrimary {
		roleStr = "PRIMARY"
	}
	log.Printf("[node %d] starting as %s | client=%s peer=%s",
		node.id, roleStr, self.ClientAddr, self.PeerAddr)

	// TODO (Stage 3): go node.startHeartbeatLoop()
	// TODO (Stage 3): go node.monitorPeers()

	// Run both servers concurrently.
	errCh := make(chan error, 2)
	go func() { errCh <- clientSrv.Serve(clientLis) }()
	go func() { errCh <- peerSrv.Serve(peerLis) }()

	log.Fatal(<-errCh)
}
