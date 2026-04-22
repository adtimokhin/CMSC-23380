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
	"google.golang.org/grpc/credentials/insecure"
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
	n := &Node{
		id:               id,
		role:             role,
		cfg:              cfg,
		clk:              clock.New(),
		store:            store.New(),
		lastSeen:         make(map[int32]time.Time),
		currentPrimaryID: 0, // node 0 is the initial primary
	}
	n.onPeerDeadFunc = n.onPeerDead
	return n
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
	n.mu.Lock()
	role := n.role
	primaryID := n.currentPrimaryID
	peerConnsCopy := make(map[int32]*grpc.ClientConn, len(n.peerConns))
	for k, v := range n.peerConns {
		peerConnsCopy[k] = v
	}
	n.mu.Unlock()
	if role != RolePrimary {
		primary, err := n.cfg.Self(primaryID)
		if err != nil {
			return &pb.PutResponse{Ok: false}, nil
		}
		return &pb.PutResponse{Ok: false, RedirectAddr: primary.ClientAddr}, nil
	}

	ts := n.clk.Tick()

	var (
		errMu    sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	for _, conn := range peerConnsCopy {
		wg.Add(1)
		go func(c *grpc.ClientConn) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), ReplicateTimeout)
			defer cancel()
			_, err := pb.NewReplicationClient(c).Replicate(ctx, &pb.ReplicateRequest{
				Key:       req.Key,
				Value:     req.Value,
				LamportTs: ts,
				OriginId:  n.id,
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(conn)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, status.Errorf(codes.Unavailable, "replication failed: %v", firstErr)
	}

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
	n.mu.Lock()
	primaryID := n.currentPrimaryID
	n.mu.Unlock()
	primary, err := n.cfg.Self(primaryID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "config: %v", err)
	}
	return &pb.GetPrimaryResponse{PrimaryAddr: primary.ClientAddr, PrimaryId: primaryID}, nil
}

// ── Replication service (internal, peer-to-peer) ─────────────────────────

// Replicate applies a write forwarded by the primary.
//
// Stage 2:
//   - Apply the Lamport max-rule: clock.Update(req.LamportTs).
//   - Write to the local store.
//   - Return ReplicateResponse{ok: true, lamport_ts: updated_clock}.
func (n *Node) Replicate(_ context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	updatedTs := n.clk.Update(req.LamportTs)
	n.store.Put(req.Key, req.Value, req.LamportTs)
	return &pb.ReplicateResponse{Ok: true, LamportTs: updatedTs}, nil
}

// Heartbeat responds to a heartbeat from a peer.
//
// Stage 3:
//   - Update lastSeen[req.SenderID] = time.Now().
//   - Apply Lamport max-rule on req.LamportTs.
//   - Return own {id, lamport_ts, role}.
func (n *Node) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	n.mu.Lock()
	n.lastSeen[req.SenderId] = time.Now()
	role := n.role
	n.mu.Unlock()
	updatedTs := n.clk.Update(req.LamportTs)
	return &pb.HeartbeatResponse{SenderId: n.id, LamportTs: updatedTs, Role: int32(role)}, nil
}

// AnnounceLeader handles a leader announcement from a candidate backup.
//
// Stage 4:
//   - Determine localTs: if this node is also a candidate this round, use
//     n.electionTs (the clock snapshot from when YOU started your election).
//     Otherwise use n.clk.Now(). This prevents a concurrent heartbeat from
//     advancing the clock and producing a wrong comparison result.
//   - Accept (return accepted=true, set currentPrimaryID = req.NewPrimaryId) if:
//     req.ElectionLamport > localTs
//     OR (req.ElectionLamport == localTs AND req.NewPrimaryId > n.id)
//   - Otherwise reject (return accepted=false). The caller wins or yields on its own.
//
// Note: two backups frequently start elections simultaneously with equal clocks
// because heartbeat exchanges synchronize clocks. Node ID is the tiebreaker:
// higher ID wins.
func (n *Node) AnnounceLeader(_ context.Context, req *pb.AnnounceLeaderRequest) (*pb.AnnounceLeaderResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var localTs int64
	if n.role == RoleCandidate {
		localTs = n.electionTs
	} else {
		localTs = n.clk.Now()
	}
	accept := req.ElectionLamport > localTs ||
		(req.ElectionLamport == localTs && req.NewPrimaryId > n.id)
	if accept {
		n.currentPrimaryID = req.NewPrimaryId
	}
	return &pb.AnnounceLeaderResponse{Accepted: accept}, nil
}

// ── Background goroutines ─────────────────────────────────────────────────

// startHeartbeatLoop runs in a goroutine, sending Heartbeat RPCs to all
// peers every HeartbeatInterval. Use context.WithTimeout on each RPC so that
// a slow/hung peer does not block the loop.
//
// Stage 3: implement this.
func (n *Node) startHeartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			peersCopy := make(map[int32]*grpc.ClientConn, len(n.peerConns))
			for id, conn := range n.peerConns {
				peersCopy[id] = conn
			}
			n.mu.Unlock()
			for peerID, conn := range peersCopy {
				go func(id int32, c *grpc.ClientConn) {
					ctx, cancel := context.WithTimeout(n.ctx, HeartbeatInterval*4/5)
					defer cancel()
					ts := n.clk.Tick()
					n.mu.Lock()
					role := int32(n.role)
					n.mu.Unlock()
					pb.NewReplicationClient(c).Heartbeat(ctx, &pb.HeartbeatRequest{ //nolint:errcheck
						SenderId:  n.id,
						LamportTs: ts,
						Role:      role,
					})
				}(peerID, conn)
			}
		}
	}
}

// monitorPeers runs in a goroutine, checking lastSeen for each peer.
// If time.Since(lastSeen[peer]) > HeartbeatTimeout, call onPeerDead(peerID).
//
// Stage 3: implement detection logic.
// Stage 4: implement election trigger in onPeerDead.
func (n *Node) monitorPeers() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			peers := make([]int32, 0, len(n.peerConns))
			for id := range n.peerConns {
				peers = append(peers, id)
			}
			n.mu.Unlock()
			for _, peerID := range peers {
				n.mu.Lock()
				ts, ok := n.lastSeen[peerID]
				n.mu.Unlock()
				if ok && time.Since(ts) > HeartbeatTimeout {
					n.onPeerDeadFunc(peerID)
				}
			}
		}
	}
}

// onPeerDead is called when monitorPeers decides a peer has failed.
// If the dead peer was the current primary, trigger runElection().
//
// Stage 4: implement this.
func (n *Node) onPeerDead(peerID int32) {
	log.Printf("[node %d] suspect: peer %d unresponsive", n.id, peerID)
	n.mu.Lock()
	isPrimary := peerID == n.currentPrimaryID
	isBackup := n.role == RoleBackup
	n.mu.Unlock()
	if isPrimary && isBackup {
		go n.runElection()
	}
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
	n.mu.Lock()
	if n.role != RoleBackup {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.electionTs = n.clk.Tick()
	electionTs := n.electionTs
	deadPrimaryID := n.currentPrimaryID
	peerConnsCopy := make(map[int32]*grpc.ClientConn, len(n.peerConns))
	for k, v := range n.peerConns {
		peerConnsCopy[k] = v
	}
	n.mu.Unlock()

	accepted := false
	var survivingPeerID int32 = -1
	var explicitlyRejected bool
	for id, conn := range peerConnsCopy {
		if id == deadPrimaryID {
			continue
		}
		survivingPeerID = id
		ctx, cancel := context.WithTimeout(context.Background(), ReplicateTimeout)
		resp, err := pb.NewReplicationClient(conn).AnnounceLeader(ctx, &pb.AnnounceLeaderRequest{
			NewPrimaryId:    n.id,
			ElectionLamport: electionTs,
		})
		cancel()
		if err == nil {
			accepted = resp.Accepted
			explicitlyRejected = !accepted
		}
		break
	}

	if accepted {
		n.mu.Lock()
		n.role = RolePrimary
		n.currentPrimaryID = n.id
		n.mu.Unlock()
		log.Printf("[node %d] won election, becoming PRIMARY", n.id)
		for _, conn := range peerConnsCopy {
			ctx, cancel := context.WithTimeout(context.Background(), ReplicateTimeout)
			pb.NewReplicationClient(conn).AnnounceLeader(ctx, &pb.AnnounceLeaderRequest{ //nolint:errcheck
				NewPrimaryId:    n.id,
				ElectionLamport: electionTs,
			})
			cancel()
		}
	} else {
		n.mu.Lock()
		n.role = RoleBackup
		// If the peer explicitly rejected us, they won the election — update currentPrimaryID
		// so Put redirects to the correct new primary without waiting for the broadcast.
		if explicitlyRejected && survivingPeerID >= 0 {
			n.currentPrimaryID = survivingPeerID
		}
		n.mu.Unlock()
		log.Printf("[node %d] lost election, staying BACKUP", n.id)
	}
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

	node.peerConns = make(map[int32]*grpc.ClientConn)
	for _, peer := range cfg.Peers(self.ID) {
		conn, err := grpc.NewClient(peer.PeerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial peer %d at %s: %v", peer.ID, peer.PeerAddr, err)
		}
		node.peerConns[peer.ID] = conn
	}

	ctx, cancel := context.WithCancel(context.Background())
	node.ctx = ctx
	node.cancel = cancel
	node.onPeerDeadFunc = node.onPeerDead
	defer cancel()

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

	go node.startHeartbeatLoop()
	go node.monitorPeers()

	// Run both servers concurrently.
	errCh := make(chan error, 2)
	go func() { errCh <- clientSrv.Serve(clientLis) }()
	go func() { errCh <- peerSrv.Serve(peerLis) }()

	log.Fatal(<-errCh)
}
