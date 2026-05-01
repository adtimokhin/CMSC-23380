// kvraft_server — Raft-backed replicated key-value store node.
//
// The KVStore gRPC interface is identical to HW2. The difference is in the
// backend: instead of primary-backup fan-out, all writes go through the Raft
// consensus module before being applied to the local KV store.
//
// Architecture:
//
//	Client ──Put(k,v)──▶ Leader.KVStore.Put
//	                          │
//	                          ▼
//	                     raft.Start(command)
//	                          │
//	             ┌────────────┼────────────┐
//	             ▼            ▼            ▼
//	         Follower1   Follower2     (self)
//	         AppendEntries              local log
//	             │            │
//	             └─────┬──────┘
//	               majority ACK → commit
//	                    │
//	              commitCh ◀── applyLoop
//	                    │
//	              store.Put(k,v)
//	                    │
//	              Put returns ok=true to client
//
// Run:
//
//	go run ./server --id=0 --config=nodeconfig.json
//	go run ./server --id=1 --config=nodeconfig.json
//	go run ./server --id=2 --config=nodeconfig.json
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"kvraft/config"
	"kvraft/internal/store"
	pb "kvraft/proto"
	"kvraft/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pendingPut tracks an in-flight Put that is waiting for its log entry to be committed.
type pendingPut struct {
	term int64
	ch   chan struct{} // closed when the entry is committed (or leadership is lost)
}

// Server wraps the Raft instance and implements the KVStore gRPC service.
type Server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedRaftRPCServer

	id  int32
	cfg *config.ClusterConfig
	rf  *raft.Raft

	mu       sync.Mutex
	st       *store.Store
	pending  map[int64]*pendingPut // log index → waiting Put handler
	leaderID int32                 // last known leader, -1 if unknown
}

func newServer(id int32, cfg *config.ClusterConfig) *Server {
	commitCh := make(chan raft.ApplyMsg, 100)
	s := &Server{
		id:       id,
		cfg:      cfg,
		st:       store.New(),
		pending:  make(map[int64]*pendingPut),
		leaderID: -1,
	}
	s.rf = raft.New(id, cfg, commitCh)
	go s.applyLoop(commitCh)
	return s
}

// ── KVStore service ───────────────────────────────────────────────────────

// Put routes the write through the Raft log. The call blocks until the entry
// is committed (or leadership is lost).
//
// Stage 2: implement this.
// Stage 3: return redirect_addr when not leader.
func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	cmd := encodeCommand(req.Key, req.Value)
	index, submittedTerm, isLeader := s.rf.Start(cmd)
	if !isLeader {
		return &pb.PutResponse{Ok: false, RedirectAddr: s.currentLeaderAddr()}, nil
	}

	pp := &pendingPut{term: submittedTerm, ch: make(chan struct{})}
	s.mu.Lock()
	s.pending[index] = pp
	s.mu.Unlock()

	select {
	case <-pp.ch:
		// applyLoop wrote the committed term into pp.term before closing.
		if pp.term != submittedTerm {
			return nil, status.Error(codes.Aborted, "leadership changed: entry may not have committed")
		}
		return &pb.PutResponse{Ok: true}, nil
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		delete(s.pending, index)
		s.mu.Unlock()
		return nil, status.Error(codes.DeadlineExceeded, "timed out waiting for commit")
	}
}

// Get reads from the local store. Reads are "stale" — they do not go through
// Raft and may return an older value if this node is a partitioned leader.
//
// Stage 2: implement this.
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	entry, found := s.st.Get(req.Key)
	return &pb.GetResponse{Found: found, Value: entry.Value}, nil
}

// GetPrimary returns the ID and client address of the current Raft leader.
// Every node must answer correctly, not just the leader itself.
//
// Stage 3: implement this. You need to track the leader ID as nodes receive
// AppendEntries RPCs (the leader always sends its own ID in LeaderId).
func (s *Server) GetPrimary(_ context.Context, _ *pb.Empty) (*pb.GetPrimaryResponse, error) {
	s.mu.Lock()
	id := s.leaderID
	s.mu.Unlock()

	if id < 0 || int(id) >= len(s.cfg.Nodes) {
		return nil, status.Error(codes.Unavailable, "leader unknown")
	}
	return &pb.GetPrimaryResponse{
		PrimaryId:   id,
		PrimaryAddr: s.cfg.Nodes[id].ClientAddr,
	}, nil
}

// ── RaftRPC service (forwarded to raft.Raft) ─────────────────────────────

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	return s.rf.RequestVote(ctx, req)
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	reply, err := s.rf.AppendEntries(ctx, req)
	// Update leaderID only when the sender's term was accepted (not a stale leader).
	// If the sender was stale, reply.Term > req.Term.
	if err == nil && reply != nil && reply.Term == req.Term {
		s.mu.Lock()
		s.leaderID = req.LeaderId
		s.mu.Unlock()
	}
	return reply, err
}

func (s *Server) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	return s.rf.InstallSnapshot(ctx, req)
}

// ── Apply loop ────────────────────────────────────────────────────────────

// applyLoop reads committed entries from commitCh, applies them to the store,
// and wakes any Put handler waiting on that index.
//
// Stage 2: implement this.
func (s *Server) applyLoop(commitCh <-chan raft.ApplyMsg) {
	for msg := range commitCh {
		op, key, value := decodeCommand(msg.Command)
		if op == "put" {
			s.st.Put(key, value)
		}

		s.mu.Lock()
		if pp, ok := s.pending[msg.Index]; ok {
			// Overwrite term with the committed term so Put can detect leader changes.
			pp.term = msg.Term
			delete(s.pending, msg.Index)
			close(pp.ch)
		}
		s.mu.Unlock()
	}
}

// encodeCommand produces the wire format for a KV put command sent through Raft.
// Format: "put:<key>:<value>"
func encodeCommand(key, value string) string {
	return fmt.Sprintf("put:%s:%s", key, value)
}

// decodeCommand parses a command string back into its components.
// Returns (op, key, value).
func decodeCommand(cmd string) (op, key, value string) {
	parts := strings.SplitN(cmd, ":", 3)
	if len(parts) < 2 {
		return "", "", ""
	}
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return parts[0], parts[1], ""
}

// currentLeaderAddr returns the client address of the current Raft leader,
// or empty string if the leader is unknown.
//
// TODO (Stage 3): implement using GetState() and scanning cfg.Nodes.
func (s *Server) currentLeaderAddr() string {
	s.mu.Lock()
	id := s.leaderID
	s.mu.Unlock()
	if id < 0 || int(id) >= len(s.cfg.Nodes) {
		return ""
	}
	return s.cfg.Nodes[id].ClientAddr
}

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	idFlag := flag.Int("id", -1, "node ID (0, 1, or 2)")
	cfgPath := flag.String("config", "nodeconfig.json", "path to nodeconfig.json")
	flag.Parse()

	if *idFlag < 0 {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: kvraft_server --id=<0|1|2> [--config=nodeconfig.json]")
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

	srv := newServer(self.ID, cfg)

	// KVStore gRPC server (client-facing).
	// Bind on all interfaces so Docker port mapping works; the advertised address
	// (self.ClientAddr) is what GetPrimary returns to clients.
	_, clientPort, _ := net.SplitHostPort(self.ClientAddr)
	clientLis, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		log.Fatalf("listen client %s: %v", self.ClientAddr, err)
	}
	clientGRPC := grpc.NewServer()
	pb.RegisterKVStoreServer(clientGRPC, srv)

	// RaftRPC gRPC server (peer-to-peer).
	_, peerPort, _ := net.SplitHostPort(self.PeerAddr)
	peerLis, err := net.Listen("tcp", ":"+peerPort)
	if err != nil {
		log.Fatalf("listen peer %s: %v", self.PeerAddr, err)
	}
	peerGRPC := grpc.NewServer()
	pb.RegisterRaftRPCServer(peerGRPC, srv)

	log.Printf("[node %d] listening | client=%s peer=%s", self.ID, self.ClientAddr, self.PeerAddr)

	errCh := make(chan error, 2)
	go func() { errCh <- clientGRPC.Serve(clientLis) }()
	go func() { errCh <- peerGRPC.Serve(peerLis) }()

	log.Fatal(<-errCh)
}

// Suppress "imported and not used" errors on helpers students haven't wired up yet.
var (
	_ = encodeCommand
	_ = decodeCommand
	_ = time.Second
	_ = strings.SplitN
)
