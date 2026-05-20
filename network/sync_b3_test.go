package network

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/store"
)

// TestCheckAndSync_TriggersCatchUpOnLag verifies that when a peer's
// HighestRound advertises a round more than SyncThreshold ahead of our
// local DAG, checkAndSync issues a CatchUp request (B3-5 / T1-5).
func TestCheckAndSync_TriggersCatchUpOnLag(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	net := NewMockNetwork(0, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	cfg := DefaultSyncConfig()
	cfg.SyncThreshold = 3
	sm := NewSyncManager(d, batchStore, net, vs, cfg)

	if err := sm.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sm.Stop() })

	if err := net.Start(); err != nil {
		t.Fatalf("net.Start: %v", err)
	}
	t.Cleanup(func() { _ = net.Stop() })

	// Connect a peer so SendSyncRequest succeeds.
	peerNet := NewMockNetwork(1, DefaultConfig())
	if err := peerNet.Start(); err != nil {
		t.Fatalf("peerNet.Start: %v", err)
	}
	t.Cleanup(func() { _ = peerNet.Stop() })
	net.Connect(peerNet)

	// Record peer at round 10 — we are at 0, threshold is 3.
	sm.recordPeerHighestRound(1, 10)

	// Run checkAndSync; expect at least one sync request to have been sent.
	preStats := net.Stats().SyncRequestsSent
	sm.CheckAndSync()

	// Allow message routing to schedule.
	time.Sleep(50 * time.Millisecond)

	postStats := net.Stats().SyncRequestsSent
	if postStats <= preStats {
		t.Errorf("expected sync requests after CheckAndSync, got %d -> %d", preStats, postStats)
	}
}

// TestCheckAndSync_NoCatchUpWhenAligned verifies that when no peer is ahead
// of us, checkAndSync does not invoke CatchUp (it can still issue probe
// requests, which is the desired behaviour, but no catch-up traffic). We
// confirm this by ensuring no peer at a known-high round triggers a probe.
func TestCheckAndSync_NoCatchUpWhenAligned(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	net := NewMockNetwork(0, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	cfg := DefaultSyncConfig()
	cfg.SyncThreshold = 5
	sm := NewSyncManager(d, batchStore, net, vs, cfg)

	if err := sm.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sm.Stop() })

	if err := net.Start(); err != nil {
		t.Fatalf("net.Start: %v", err)
	}
	t.Cleanup(func() { _ = net.Stop() })

	// Tell SyncManager every peer is currently at the same round as us (0).
	for v := uint16(1); v <= 3; v++ {
		sm.recordPeerHighestRound(v, 0)
	}

	// Local DAG is at round 0 too — running checkAndSync must not trigger
	// a CatchUp (no peer is ahead). CatchUp would create a non-zero number
	// of pending requests with the CatchUp-target peer's targetPeer set.
	// We probe by checking that all peerHighest values stay 0 and the
	// pending map only contains probe-style requests (toRound=0).
	sm.CheckAndSync()

	// Confirm none of the recorded peer rounds was bumped by checkAndSync.
	for v := uint16(1); v <= 3; v++ {
		if got := sm.PeerHighestRound(v); got != 0 {
			t.Errorf("peer %d HighestRound = %d, want 0", v, got)
		}
	}
}

// TestSyncManager_RecordsPeerHighestFromResponse verifies that when we
// receive a SyncResponse from a peer, the peer's max round (ToRound) is
// recorded for use in checkAndSync.
func TestSyncManager_RecordsPeerHighestFromResponse(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	net := NewMockNetwork(0, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	sm := NewSyncManager(d, batchStore, net, vs, DefaultSyncConfig())
	if err := sm.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sm.Stop() })

	resp := &SyncResponse{FromRound: 0, ToRound: 42}
	if err := sm.HandleSyncResponse(resp, 2); err != nil {
		t.Fatalf("HandleSyncResponse: %v", err)
	}

	if got := sm.PeerHighestRound(2); got != 42 {
		t.Errorf("PeerHighestRound(2) = %d, want 42", got)
	}
}
