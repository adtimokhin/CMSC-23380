// Local tests for the kvstore server — run with:
//
//	go test ./server/... -v -race -timeout 120s
//
// Run by stage:
//
//	go test ./server/... -v -race -run TestStage1
//	go test ./server/... -v -race -run TestStage2
//	go test ./server/... -v -race -run TestStage3
//	go test ./server/... -v -race -run TestStage4
//	go test ./server/... -v -race -run TestStage5
//
// Point values are noted per test; total automated score = 85 pts.
//
// Note: a small number of additional tests run only on Gradescope. These
// tests cover concurrent election and redirect scenarios that require a
// correct, race-free implementation to pass reliably. A correct implementation
// will pass them; one that "works most of the time" locally will not.
// The autograder provides hints when these tests fail.
package main

import (
	"testing"
	"time"

	pb "kvstore/proto"
)

// ════════════════════════════════════════════════════════════════════════════
// Stage 1 — Single-Node KV Store (25 pts)
// ════════════════════════════════════════════════════════════════════════════

// TestStage1_PutReturnsOK verifies the basic Put contract on a single primary
// node: ok=true, a positive Lamport timestamp, and no redirect address.
// [5 pts]
func TestStage1_PutReturnsOK(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	resp, err := tn.KVClient.Put(ctx, &pb.PutRequest{Key: "x", Value: "hello"})
	requirePutOK(t, resp, err)

	if resp.RedirectAddr != "" {
		t.Errorf("Put on primary: got redirect_addr=%q, want empty", resp.RedirectAddr)
	}
}

// TestStage1_PutTicksLamportClock verifies that the Lamport clock increments
// on every Put and that the response timestamp matches the node's internal
// clock state.
// [5 pts]
func TestStage1_PutTicksLamportClock(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	var prevTs int64
	for i := range 3 {
		resp, err := tn.KVClient.Put(ctx, &pb.PutRequest{
			Key:   "k",
			Value: "v",
		})
		requirePutOK(t, resp, err)

		if resp.LamportTs <= prevTs {
			t.Fatalf("Put #%d: lamport_ts=%d not strictly greater than previous %d",
				i+1, resp.LamportTs, prevTs)
		}
		// Internal clock must match what was returned.
		if got := tn.node.clk.Now(); got != resp.LamportTs {
			t.Fatalf("Put #%d: internal clock=%d != resp.lamport_ts=%d",
				i+1, got, resp.LamportTs)
		}
		prevTs = resp.LamportTs
	}
}

// TestStage1_GetExistingKey verifies that Get returns the correct value and the
// same Lamport timestamp that was assigned by Put.
// [5 pts]
func TestStage1_GetExistingKey(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	putResp, err := tn.KVClient.Put(ctx, &pb.PutRequest{Key: "color", Value: "blue"})
	requirePutOK(t, putResp, err)

	getResp, err := tn.KVClient.Get(ctx, &pb.GetRequest{Key: "color"})
	requireGetFound(t, getResp, err, "blue")

	if getResp.LamportTs != putResp.LamportTs {
		t.Errorf("Get lamport_ts=%d, want %d (same as Put)", getResp.LamportTs, putResp.LamportTs)
	}
}

// TestStage1_GetMissingKey verifies that Get for an unknown key returns
// found=false with an empty value.
// [4 pts]
func TestStage1_GetMissingKey(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	resp, err := tn.KVClient.Get(ctx, &pb.GetRequest{Key: "no-such-key"})
	if err != nil {
		t.Fatalf("Get RPC error: %v", err)
	}
	if resp.Found {
		t.Errorf("Get missing key: got found=true, want false")
	}
	if resp.Value != "" {
		t.Errorf("Get missing key: got value=%q, want empty", resp.Value)
	}
}

// TestStage1_GetPrimary verifies that GetPrimary returns the correct ID and
// client address for a single-node cluster.
// [3 pts]
func TestStage1_GetPrimary(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	resp, err := tn.KVClient.GetPrimary(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("GetPrimary RPC error: %v", err)
	}
	if resp.PrimaryId != 0 {
		t.Errorf("GetPrimary: got primary_id=%d, want 0", resp.PrimaryId)
	}
	if resp.PrimaryAddr != "test:7000" {
		t.Errorf("GetPrimary: got primary_addr=%q, want %q", resp.PrimaryAddr, "test:7000")
	}
}

// TestStage1_PutGetMultipleKeys verifies correct round-trip for five distinct
// keys with distinct values.
// [3 pts]
func TestStage1_PutGetMultipleKeys(t *testing.T) {
	tn := newTestNode(t, 0, testCfg())
	ctx := testCtx(t)

	pairs := [][2]string{
		{"a", "apple"}, {"b", "banana"}, {"c", "cherry"}, {"d", "date"}, {"e", "elderberry"},
	}
	tss := make(map[string]int64)
	for _, kv := range pairs {
		resp, err := tn.KVClient.Put(ctx, &pb.PutRequest{Key: kv[0], Value: kv[1]})
		requirePutOK(t, resp, err)
		tss[kv[0]] = resp.LamportTs
	}
	for _, kv := range pairs {
		resp, err := tn.KVClient.Get(ctx, &pb.GetRequest{Key: kv[0]})
		requireGetFound(t, resp, err, kv[1])
		if resp.LamportTs != tss[kv[0]] {
			t.Errorf("Get %q: lamport_ts=%d, want %d", kv[0], resp.LamportTs, tss[kv[0]])
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 2 — Replication Fan-Out (20 pts)
// ════════════════════════════════════════════════════════════════════════════

// TestStage2_PutReplicatesToBothBackups verifies that after a successful Put on
// the primary, both backup nodes store the same value under the same Lamport
// timestamp that the primary returned to the client.
// [10 pts]
func TestStage2_PutReplicatesToBothBackups(t *testing.T) {
	tc := newTestCluster(t)
	ctx := testCtx(t)

	putResp, err := tc.Nodes[0].KVClient.Put(ctx, &pb.PutRequest{Key: "msg", Value: "hello"})
	requirePutOK(t, putResp, err)

	wantTs := putResp.LamportTs

	for _, id := range []int{1, 2} {
		entry, ok := tc.Nodes[id].node.store.Get("msg")
		if !ok {
			t.Errorf("node %d: key %q not found in store after replication", id, "msg")
			continue
		}
		if entry.Value != "hello" {
			t.Errorf("node %d: got value=%q, want %q", id, entry.Value, "hello")
		}
		if entry.Ts != wantTs {
			t.Errorf("node %d: got Ts=%d, want %d (primary's lamport_ts)", id, entry.Ts, wantTs)
		}
	}
}

// TestStage2_BackupAppliesMaxRule verifies that a backup applies the Lamport
// max-rule when it receives a Replicate RPC: max(local, received) + 1.
// [5 pts]
func TestStage2_BackupAppliesMaxRule(t *testing.T) {
	tc := newTestCluster(t)
	ctx := testCtx(t)

	// Advance node 1's clock to 5 to simulate prior activity.
	for range 5 {
		tc.Nodes[1].node.clk.Tick()
	}
	// node 0 starts at 0; Put ticks to 1 and sends LamportTs=1 to backups.
	_, err := tc.Nodes[0].KVClient.Put(ctx, &pb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("Put RPC error: %v", err)
	}

	// node 1 applies max(5, 1) + 1 = 6.
	gotClock := tc.Nodes[1].node.clk.Now()
	if gotClock != 6 {
		t.Errorf("node 1 clock after Replicate: got %d, want 6 (max(5,1)+1)", gotClock)
	}
}

// TestStage2_ReplicateDirectly calls the Replicate RPC directly on a backup
// node and verifies: (a) ok=true, (b) resp.LamportTs reflects the max-rule
// update, (c) the store records the primary's assigned timestamp (not the
// backup's updated clock).
// [3 pts]
func TestStage2_ReplicateDirectly(t *testing.T) {
	tn := newTestNode(t, 1, testCfg()) // standalone backup
	ctx := testCtx(t)

	resp, err := tn.ReplClient.Replicate(ctx, &pb.ReplicateRequest{
		Key:       "z",
		Value:     "99",
		LamportTs: 10,
		OriginId:  0,
	})
	if err != nil {
		t.Fatalf("Replicate RPC error: %v", err)
	}
	if !resp.Ok {
		t.Errorf("Replicate: got ok=false, want true")
	}
	// Backup's updated clock = max(0, 10) + 1 = 11.
	if resp.LamportTs != 11 {
		t.Errorf("Replicate resp.LamportTs: got %d, want 11 (max(0,10)+1)", resp.LamportTs)
	}
	// Store must record the primary's ts (10), not the backup's updated clock (11).
	entry, ok := tn.node.store.Get("z")
	if !ok {
		t.Fatal("key 'z' not found in backup store after Replicate")
	}
	if entry.Ts != 10 {
		t.Errorf("store entry Ts: got %d, want 10 (primary's lamport_ts)", entry.Ts)
	}
	if entry.Value != "99" {
		t.Errorf("store entry Value: got %q, want %q", entry.Value, "99")
	}
}

// TestStage2_PutFailsIfBackupsUnreachable verifies that the primary does NOT
// return ok=true when it cannot reach either backup. Either ok=false or a gRPC
// error is acceptable; ok=true is not.
// [2 pts]
func TestStage2_PutFailsIfBackupsUnreachable(t *testing.T) {
	tc := newTestCluster(t)
	ctx := testCtx(t)

	// Prerequisite: Put must succeed with live backups. This rules out empty
	// stubs that return an error unconditionally (which would trivially satisfy
	// the unreachability check below).
	preresp, preerr := tc.Nodes[0].KVClient.Put(ctx, &pb.PutRequest{Key: "prereq", Value: "v"})
	if preerr != nil || !preresp.Ok {
		t.Fatal("Put failed with live backups — implement Stage 2 replication before this test is meaningful")
	}

	// Kill both backups before the Put.
	tc.KillNode(1)
	tc.KillNode(2)

	// Give the OS a moment to propagate the closed connections.
	time.Sleep(10 * time.Millisecond)

	resp, err := tc.Nodes[0].KVClient.Put(ctx, &pb.PutRequest{Key: "k", Value: "v"})
	if err == nil && resp.Ok {
		t.Error("Put with unreachable backups: got ok=true, want ok=false or error")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 3 — Heartbeat & Failure Detection (13 pts)
//
// Note: HeartbeatInterval and HeartbeatTimeout must be exported package-level
// vars (not consts) in server/main.go so tests can override them:
//
//	var (
//	    HeartbeatInterval = 500 * time.Millisecond
//	    HeartbeatTimeout  = 1000 * time.Millisecond
//	)
// ════════════════════════════════════════════════════════════════════════════

// shortTimings overrides HeartbeatInterval and HeartbeatTimeout to small values
// for fast tests, restoring the originals on cleanup.
func shortTimings(t *testing.T) {
	t.Helper()
	origInterval, origTimeout := HeartbeatInterval, HeartbeatTimeout
	HeartbeatInterval = 20 * time.Millisecond
	HeartbeatTimeout = 60 * time.Millisecond
	t.Cleanup(func() {
		HeartbeatInterval = origInterval
		HeartbeatTimeout = origTimeout
	})
}

// TestStage3_HeartbeatUpdatesLastSeen verifies that calling the Heartbeat RPC
// updates the receiver's lastSeen map for the sender's ID.
// [4 pts]
func TestStage3_HeartbeatUpdatesLastSeen(t *testing.T) {
	tn := newTestNode(t, 1, testCfg())
	ctx := testCtx(t)

	before := time.Now()
	_, err := tn.ReplClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		SenderId:  0,
		LamportTs: 5,
		Role:      int32(RolePrimary),
	})
	if err != nil {
		t.Fatalf("Heartbeat RPC error: %v", err)
	}

	tn.node.mu.Lock()
	ts, ok := tn.node.lastSeen[0]
	tn.node.mu.Unlock()

	if !ok {
		t.Fatal("lastSeen[0] not set after Heartbeat from sender 0")
	}
	if ts.Before(before) {
		t.Errorf("lastSeen[0]=%v is before the RPC was sent (%v)", ts, before)
	}
}

// TestStage3_HeartbeatUpdatesClockAndReturnsRole verifies that:
//   - The receiver applies the Lamport max-rule to the sender's timestamp.
//   - The response includes the receiver's own ID, updated clock, and role.
//
// [2 pts]
func TestStage3_HeartbeatUpdatesClockAndReturnsRole(t *testing.T) {
	tn := newTestNode(t, 1, testCfg()) // node 1 = backup
	ctx := testCtx(t)

	resp, err := tn.ReplClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		SenderId:  0,
		LamportTs: 7,
		Role:      int32(RolePrimary),
	})
	if err != nil {
		t.Fatalf("Heartbeat RPC error: %v", err)
	}
	if resp.SenderId != 1 {
		t.Errorf("Heartbeat resp.SenderId: got %d, want 1", resp.SenderId)
	}
	// max(0, 7) + 1 = 8
	if resp.LamportTs < 8 {
		t.Errorf("Heartbeat resp.LamportTs: got %d, want >=8 (max-rule applied)", resp.LamportTs)
	}
	if resp.Role != int32(RoleBackup) {
		t.Errorf("Heartbeat resp.Role: got %d, want %d (RoleBackup)", resp.Role, int32(RoleBackup))
	}
}

// TestStage3_HeartbeatLoopDelivers verifies that startHeartbeatLoop actually
// sends heartbeats to peers: after a few intervals, a peer's lastSeen entry
// for the sender should be populated and recent.
// [3 pts]
func TestStage3_HeartbeatLoopDelivers(t *testing.T) {
	shortTimings(t)
	tc := newTestCluster(t)

	// Start node 0's heartbeat loop (runs until ctx is cancelled).
	go tc.Nodes[0].node.startHeartbeatLoop()

	// Wait for several heartbeat intervals.
	time.Sleep(3 * HeartbeatInterval)

	// Node 1 and node 2 should have received at least one heartbeat from node 0.
	for _, id := range []int{1, 2} {
		tc.Nodes[id].node.mu.Lock()
		ts, ok := tc.Nodes[id].node.lastSeen[0]
		tc.Nodes[id].node.mu.Unlock()
		if !ok {
			t.Errorf("node %d: lastSeen[0] not set; no heartbeat received from node 0", id)
			continue
		}
		if time.Since(ts) > 3*HeartbeatInterval {
			t.Errorf("node %d: lastSeen[0] too old (%v ago); heartbeat not recent", id, time.Since(ts))
		}
	}
}

// TestStage3_FailureDetectionFires verifies that monitorPeers eventually calls
// onPeerDeadFunc for a peer that stops sending heartbeats.
//
// Requires Node to have an onPeerDeadFunc field:
//
//	onPeerDeadFunc func(int32)  // defaults to n.onPeerDead; overridable in tests
//
// [4 pts]
func TestStage3_FailureDetectionFires(t *testing.T) {
	shortTimings(t)
	tc := newTestCluster(t)
	ctx := testCtx(t)

	// Seed lastSeen[0] on node 1 so the monitor starts tracking.
	_, err := tc.Nodes[1].ReplClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		SenderId:  0,
		LamportTs: 1,
		Role:      int32(RolePrimary),
	})
	if err != nil {
		t.Fatalf("seed Heartbeat: %v", err)
	}

	// Inject a detection hook before starting the monitor.
	detected := make(chan int32, 1)
	tc.Nodes[1].node.onPeerDeadFunc = func(id int32) {
		select {
		case detected <- id:
		default:
		}
	}

	// Start node 1's monitor — node 0 will never send another heartbeat.
	go tc.Nodes[1].node.monitorPeers()

	// Allow time for the timeout to fire: HeartbeatTimeout + a few check intervals.
	select {
	case id := <-detected:
		if id != 0 {
			t.Errorf("onPeerDeadFunc called with id=%d, want 0", id)
		}
	case <-time.After(HeartbeatTimeout + 3*HeartbeatInterval):
		t.Error("failure not detected within expected window")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 4 — Leader Election (17 pts)
// ════════════════════════════════════════════════════════════════════════════

// TestStage4_AnnounceLeaderAccepted verifies that a backup accepts an
// AnnounceLeader RPC when the candidate's ElectionLamport exceeds the
// receiver's current clock, and updates currentPrimaryID accordingly.
// [4 pts]
func TestStage4_AnnounceLeaderAccepted(t *testing.T) {
	tn := newTestNode(t, 1, testCfg()) // node 1, clock starts at 0
	ctx := testCtx(t)

	resp, err := tn.ReplClient.AnnounceLeader(ctx, &pb.AnnounceLeaderRequest{
		NewPrimaryId:    2,
		ElectionLamport: 10,
	})
	if err != nil {
		t.Fatalf("AnnounceLeader RPC error: %v", err)
	}
	if !resp.Accepted {
		t.Error("AnnounceLeader: got accepted=false, want true (candidate clock > local clock)")
	}

	tn.node.mu.Lock()
	gotID := tn.node.currentPrimaryID
	tn.node.mu.Unlock()
	if gotID != 2 {
		t.Errorf("currentPrimaryID: got %d, want 2", gotID)
	}
}

// TestStage4_AnnounceLeaderRejected verifies that a backup rejects an
// AnnounceLeader RPC when the candidate's ElectionLamport is lower than the
// receiver's clock (the receiver has a stronger claim to leadership).
// [4 pts]
func TestStage4_AnnounceLeaderRejected(t *testing.T) {
	tn := newTestNode(t, 1, testCfg())
	ctx := testCtx(t)

	// Advance node 1's clock to 15 (simulating more recent activity).
	for range 15 {
		tn.node.clk.Tick()
	}
	origPrimaryID := tn.node.currentPrimaryID

	resp, err := tn.ReplClient.AnnounceLeader(ctx, &pb.AnnounceLeaderRequest{
		NewPrimaryId:    2,
		ElectionLamport: 10, // lower than local clock (15)
	})
	if err != nil {
		t.Fatalf("AnnounceLeader RPC error: %v", err)
	}
	if resp.Accepted {
		t.Error("AnnounceLeader: got accepted=true, want false (local clock > candidate clock)")
	}

	tn.node.mu.Lock()
	gotRole := tn.node.role
	gotID := tn.node.currentPrimaryID
	tn.node.mu.Unlock()

	if gotRole != RoleBackup {
		t.Errorf("role after rejection: got %d, want RoleBackup (%d)", gotRole, RoleBackup)
	}
	if gotID != origPrimaryID {
		t.Errorf("currentPrimaryID changed after rejection: got %d, want %d", gotID, origPrimaryID)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 5 — Client Redirect (10 pts)
// ════════════════════════════════════════════════════════════════════════════

// TestStage5_BackupReturnsRedirect verifies that a Put sent directly to a
// backup returns ok=false with a redirect_addr pointing to the primary.
// [5 pts]
func TestStage5_BackupReturnsRedirect(t *testing.T) {
	tc := newTestCluster(t)
	ctx := testCtx(t)

	// Node 1 is a backup; all Puts should be redirected.
	resp, err := tc.Nodes[1].KVClient.Put(ctx, &pb.PutRequest{Key: "x", Value: "y"})
	if err != nil {
		t.Fatalf("Put RPC error: %v", err)
	}
	if resp.Ok {
		t.Error("Put on backup: got ok=true, want false (should redirect)")
	}
	if resp.RedirectAddr != "test:7000" {
		t.Errorf("Put on backup: redirect_addr=%q, want %q (node 0's client addr)",
			resp.RedirectAddr, "test:7000")
	}
}
