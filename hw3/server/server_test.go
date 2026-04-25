// Server-level tests for HW3 — run with:
//
//	go test ./server/... -v -race -timeout 120s
//
// Run by stage:
//
//	go test ./server/... -v -race -run TestStage2 -timeout 60s
//	go test ./server/... -v -race -run TestStage3 -timeout 120s
//
// These tests exercise the full KVStore gRPC interface wired to a real Raft
// cluster. See test_helpers_test.go for the cluster infrastructure.
package main

import (
	"testing"
	"time"

	pb "kvraft/proto"
)

// ════════════════════════════════════════════════════════════════════════════
// Stage 2 — Log Replication (KVStore layer)
// ════════════════════════════════════════════════════════════════════════════

// TestStage2_PutReturnsOK verifies the basic Put contract: ok=true and no
// redirect address when called on the leader.
// [4 pts]
func TestStage2_PutReturnsOK(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	resp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: "x", Value: "hello"})
	requirePutOK(t, resp, err)

	if resp.RedirectAddr != "" {
		t.Errorf("Put on leader: got redirect_addr=%q, want empty", resp.RedirectAddr)
	}
}

// TestStage2_GetReturnsValue verifies that Get returns the value written by a
// preceding Put.
// [4 pts]
func TestStage2_GetReturnsValue(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	putResp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: "color", Value: "blue"})
	requirePutOK(t, putResp, err)

	getResp, err := leader.KVClient.Get(ctx, &pb.GetRequest{Key: "color"})
	requireGetFound(t, getResp, err, "blue")
}

// TestStage2_GetMissingKey verifies that Get for an unknown key returns
// found=false with an empty value.
// [2 pts]
func TestStage2_GetMissingKey(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	resp, err := leader.KVClient.Get(ctx, &pb.GetRequest{Key: "no-such-key"})
	if err != nil {
		t.Fatalf("Get RPC error: %v", err)
	}
	if resp.Found {
		t.Error("Get missing key: got found=true, want false")
	}
	if resp.Value != "" {
		t.Errorf("Get missing key: got value=%q, want empty", resp.Value)
	}
}

// TestStage2_MultipleKeys verifies correct round-trips for several distinct keys.
// [4 pts]
func TestStage2_MultipleKeys(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	pairs := [][2]string{
		{"a", "apple"}, {"b", "banana"}, {"c", "cherry"},
	}
	for _, kv := range pairs {
		resp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: kv[0], Value: kv[1]})
		requirePutOK(t, resp, err)
	}
	for _, kv := range pairs {
		resp, err := leader.KVClient.Get(ctx, &pb.GetRequest{Key: kv[0]})
		requireGetFound(t, resp, err, kv[1])
	}
}

// TestStage2_PutReplicatesToFollowers verifies that after a Put on the leader,
// all follower nodes eventually return the same value from Get.
// [6 pts]
func TestStage2_PutReplicatesToFollowers(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	putResp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: "msg", Value: "raft"})
	requirePutOK(t, putResp, err)

	// Poll all nodes until each returns the replicated value.
	for i, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		ok := pollUntil(func() bool {
			resp, err := ts.KVClient.Get(ctx, &pb.GetRequest{Key: "msg"})
			return err == nil && resp.Found && resp.Value == "raft"
		}, 2*time.Second, 20*time.Millisecond)
		if !ok {
			t.Errorf("node %d: value not replicated within 2s", i)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Stage 3 — Safety Under Failures
// ════════════════════════════════════════════════════════════════════════════

// TestStage3_PutOnNonLeaderReturnsRedirect verifies that a Put sent to a follower
// returns ok=false with a non-empty redirect_addr pointing to the current leader.
// [3 pts]
func TestStage3_PutOnNonLeaderReturnsRedirect(t *testing.T) {
	tc := newTestCluster(t)
	tc.waitForLeader(t)
	ctx := testCtx(t)

	leaderID := tc.leaderID()
	for i, ts := range tc.Servers {
		if ts == nil || i == leaderID {
			continue
		}
		resp, err := ts.KVClient.Put(ctx, &pb.PutRequest{Key: "x", Value: "y"})
		if err != nil {
			t.Fatalf("Put on node %d: RPC error: %v", i, err)
		}
		if resp.Ok {
			t.Errorf("node %d (follower): got ok=true, want false", i)
		}
		if resp.RedirectAddr == "" {
			t.Errorf("node %d (follower): got empty redirect_addr, want leader's client addr", i)
		}
		break // one follower is enough
	}
}

// TestStage3_GetPrimaryReturnsLeader verifies that GetPrimary returns the
// correct leader address and ID after election.
// [2 pts]
func TestStage3_GetPrimaryReturnsLeader(t *testing.T) {
	tc := newTestCluster(t)
	tc.waitForLeader(t)
	ctx := testCtx(t)

	leaderID := tc.leaderID()
	wantAddr := testCfg().Nodes[leaderID].ClientAddr

	for _, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		resp, err := ts.KVClient.GetPrimary(ctx, &pb.Empty{})
		if err != nil {
			t.Fatalf("GetPrimary RPC error: %v", err)
		}
		if resp.PrimaryId != int32(leaderID) {
			t.Errorf("GetPrimary: got primary_id=%d, want %d", resp.PrimaryId, leaderID)
		}
		if resp.PrimaryAddr != wantAddr {
			t.Errorf("GetPrimary: got primary_addr=%q, want %q", resp.PrimaryAddr, wantAddr)
		}
	}
}

// TestStage3_CommitSurvivesLeaderCrash verifies that writes acknowledged by the
// leader before it crashes are still readable after a new leader is elected.
// [6 pts]
func TestStage3_CommitSurvivesLeaderCrash(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	// Write several keys through the current leader.
	pairs := [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}
	for _, kv := range pairs {
		resp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: kv[0], Value: kv[1]})
		requirePutOK(t, resp, err)
	}

	// Kill the leader.
	oldLeaderID := tc.leaderID()
	tc.KillServer(oldLeaderID)

	// Wait for re-election.
	if !pollUntil(func() bool { return tc.leaderServer() != nil }, 2*time.Second, 10*time.Millisecond) {
		t.Fatal("no new leader elected after crash")
	}
	newLeader := tc.waitForLeader(t)

	// All committed keys must still be readable.
	for _, kv := range pairs {
		resp, err := newLeader.KVClient.Get(ctx, &pb.GetRequest{Key: kv[0]})
		requireGetFound(t, resp, err, kv[1])
	}
}

// TestStage3_PartitionAndRecovery verifies that a node isolated during a burst
// of writes fully catches up after the partition heals, serving the same values
// as the rest of the cluster.
// [4 pts]
func TestStage3_PartitionAndRecovery(t *testing.T) {
	tc := newTestCluster(t)
	tc.waitForLeader(t)
	ctx := testCtx(t)

	leaderID := tc.leaderID()

	// Isolate a follower (not the leader).
	isolated := -1
	for i, ts := range tc.Servers {
		if ts != nil && i != leaderID {
			isolated = i
			break
		}
	}
	tc.isolate(isolated)

	// Commit writes on the majority partition.
	leader := tc.Servers[leaderID]
	for i := range 5 {
		resp, err := leader.KVClient.Put(ctx, &pb.PutRequest{
			Key:   "k",
			Value: string(rune('0' + i)),
		})
		requirePutOK(t, resp, err)
	}

	// Heal the partition — isolated node must catch up.
	tc.reconnect(isolated)

	ok := pollUntil(func() bool {
		resp, err := tc.Servers[isolated].KVClient.Get(ctx, &pb.GetRequest{Key: "k"})
		return err == nil && resp.Found && resp.Value == "4"
	}, 3*time.Second, 20*time.Millisecond)
	if !ok {
		t.Errorf("isolated node did not catch up after partition healed")
	}
}

// TestStage3_RepeatedLeaderFailures verifies that the cluster remains consistent
// after repeated leader crashes. A 3-node cluster tolerates one simultaneous
// failure (quorum = 2), so the loop runs until the cluster drops below quorum.
// [5 pts]
func TestStage3_RepeatedLeaderFailures(t *testing.T) {
	tc := newTestCluster(t)
	ctx := testCtx(t)

	putThroughLeader := func(key, value string) {
		t.Helper()
		if !pollUntil(func() bool { return tc.leaderServer() != nil }, 2*time.Second, 10*time.Millisecond) {
			t.Fatal("no leader found before Put")
		}
		leader := tc.leaderServer()
		resp, err := leader.KVClient.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		requirePutOK(t, resp, err)
	}

	putThroughLeader("round", "0")
	lastValue := "0"

	for round := range 3 {
		id := tc.leaderID()
		if id < 0 {
			break
		}
		tc.KillServer(id)

		// If the remaining nodes cannot form a majority, stop here.
		// This is expected once enough nodes have been killed.
		if !pollUntil(func() bool { return tc.leaderServer() != nil }, 2*time.Second, 10*time.Millisecond) {
			break
		}
		v := string(rune('1' + round))
		putThroughLeader("round", v)
		lastValue = v
	}

	// All surviving nodes must agree on the last committed value.
	for i, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		ok := pollUntil(func() bool {
			resp, err := ts.KVClient.Get(ctx, &pb.GetRequest{Key: "round"})
			return err == nil && resp.Found && resp.Value == lastValue
		}, 2*time.Second, 20*time.Millisecond)
		if !ok {
			t.Errorf("node %d: final value not replicated (want %q)", i, lastValue)
		}
	}
}
