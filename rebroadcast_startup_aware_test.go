package looseberry

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
)

// TestRebroadcast_StartupGraceSuppresses verifies gate (a): during the
// first rebroadcastStartupGrace seconds after Start, the rebroadcast
// loop NEVER fires — even if the cluster appears stuck. This is the
// fix for the PLAN §E7c regression where an earlier gated-on-time
// variant tripped during the libp2p mesh-warmup window and pushed
// fast-path runs into slow-path.
//
// Setup: start a looseberry, submit a batch's worth of txs so a
// broadcast happens, NotifyCommitted with round 0 only (so
// lastCommittedRound stays 0). The other gates would all fire, but
// startup-grace must suppress.
func TestRebroadcast_StartupGraceSuppresses(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)
	lb.SetStores(
		store.NewMemoryBatchStore(),
		store.NewMemoryCertificateStore(),
		store.NewMemoryTxIndex(),
	)
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	for i := 0; i < 500; i++ {
		_ = lb.AddTx([]byte("tx-" + string(rune('a'+i%26)) + string(rune('0'+i%10))))
	}

	// Wait for the initial batch broadcast to fire so there's something
	// the rebroadcast loop COULD re-emit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if net.Stats().BatchesBroadcast >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	initial := net.Stats().BatchesBroadcast
	if initial < 1 {
		t.Fatalf("setup broken: expected at least 1 broadcast, got %d", initial)
	}

	// Drive lastCommitNanos to "long ago" by NEVER calling NotifyCommitted
	// with a positive round. lastCommittedRound stays at 0, so gate (b)
	// blocks rebroadcast even though the cluster looks stuck on gate (c).
	// Wait past the stuck threshold + a couple loop ticks; the startup
	// grace ALSO suppresses.
	time.Sleep(stuckRebroadcastThreshold + 2*rebroadcastInterval)

	if got := net.Stats().BatchesBroadcast; got > initial {
		t.Fatalf("rebroadcast fired during startup grace: initial=%d got=%d "+
			"(this is the regression the gate is supposed to prevent)", initial, got)
	}
}

// TestRebroadcast_FiresOnHardStallPastStartupGrace pins the
// hard-stall recovery case: a cluster that never forms round-0
// (lastCommittedRound stays 0 forever) SHOULD see the rebroadcast
// fire once past the startup grace. The earlier "requires first
// commit" gate blocked this case — but the 15 s startup grace
// already prevents the startup-window false-positive, and a stuck
// cluster past 15 s benefits from re-emission attempts. This pins
// the relaxed behaviour.
func TestRebroadcast_FiresOnHardStallPastStartupGrace(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)
	lb.SetStores(store.NewMemoryBatchStore(), store.NewMemoryCertificateStore(), store.NewMemoryTxIndex())
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Back-date startNanos AND lastCommitNanos so startup grace has
	// expired AND committedRound has been "frozen" longer than the
	// stuck threshold. Simulates a hard-stalled cluster that never
	// formed round-0 (lastCommittedRound is still 0).
	staleNanos := time.Now().Add(-2 * rebroadcastStartupGrace).UnixNano()
	lb.startNanos.Store(staleNanos)
	lb.lastCommitNanos.Store(staleNanos)

	// Submit batch.
	for i := 0; i < 500; i++ {
		_ = lb.AddTx([]byte("rx-" + string(rune('a'+i%26)) + string(rune('0'+i%10))))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if net.Stats().BatchesBroadcast >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	initial := net.Stats().BatchesBroadcast
	if initial < 1 {
		t.Fatalf("setup broken: expected at least 1 broadcast, got %d", initial)
	}

	// Wait for the rebroadcast loop to tick at least once with stuck
	// conditions satisfied.
	time.Sleep(stuckRebroadcastThreshold + 2*rebroadcastInterval)

	if got := net.Stats().BatchesBroadcast; got <= initial {
		t.Fatalf("rebroadcast did NOT fire on hard-stall past startup grace: "+
			"initial=%d got=%d (the relaxed gating must allow this case so "+
			"hard-stalled clusters get a recovery path)", initial, got)
	}
}

// TestRebroadcast_FiresWhenAllGatesSatisfied is the positive-case
// regression: once the cluster is past startup grace AND has made
// at least one commit AND committedRound has stalled for the stuck
// threshold, the rebroadcast loop DOES fire.
//
// This pins that the gating doesn't accidentally suppress the
// legitimate use case (mid-run stall → unstick via re-emission).
func TestRebroadcast_FiresWhenAllGatesSatisfied(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)
	lb.SetStores(store.NewMemoryBatchStore(), store.NewMemoryCertificateStore(), store.NewMemoryTxIndex())
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Bypass the startup grace window for this test by back-dating
	// startNanos. (In production, only real wall-clock passage clears
	// the grace; this is the same shape, just compressed for test
	// runtime.)
	lb.startNanos.Store(time.Now().Add(-2 * rebroadcastStartupGrace).UnixNano())

	// Submit a batch's worth so there's pending traffic for the loop
	// to re-emit.
	for i := 0; i < 500; i++ {
		_ = lb.AddTx([]byte("yz-" + string(rune('a'+i%26)) + string(rune('0'+i%10))))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if net.Stats().BatchesBroadcast >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	initial := net.Stats().BatchesBroadcast
	if initial < 1 {
		t.Fatalf("setup broken: expected at least 1 broadcast, got %d", initial)
	}

	// Satisfy gate (b): commit a round, then let lastCommitNanos go cold.
	lb.NotifyCommitted(1)
	time.Sleep(stuckRebroadcastThreshold + 2*rebroadcastInterval)

	if got := net.Stats().BatchesBroadcast; got <= initial {
		t.Fatalf("rebroadcast did NOT fire when all three gates are satisfied: "+
			"initial=%d got=%d (gate composition is too strict — the cluster "+
			"would be stuck forever)", initial, got)
	}
}

// TestRebroadcast_MetricsCounters verifies the observability hook:
// Metrics().RebroadcastFires and RebroadcastedBatches start at zero
// on a healthy cluster and increment when the loop actually fires.
// Operators rely on these to spot stuck-detection in production.
func TestRebroadcast_MetricsCounters(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)
	lb.SetStores(store.NewMemoryBatchStore(), store.NewMemoryCertificateStore(), store.NewMemoryTxIndex())
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Healthy baseline: counters at zero before any stuck-detection
	// firing.
	if m := lb.Metrics(); m.RebroadcastFires != 0 || m.RebroadcastedBatches != 0 {
		t.Fatalf("rebroadcast counters non-zero on healthy startup: fires=%d batches=%d",
			m.RebroadcastFires, m.RebroadcastedBatches)
	}

	// Drive the cluster into the stuck-detection path: bypass startup
	// grace, leave lastCommitNanos stale, submit batches.
	staleNanos := time.Now().Add(-2 * rebroadcastStartupGrace).UnixNano()
	lb.startNanos.Store(staleNanos)
	lb.lastCommitNanos.Store(staleNanos)
	for i := 0; i < 500; i++ {
		_ = lb.AddTx([]byte("metric-tx-" + string(rune('a'+i%26)) + string(rune('0'+i%10))))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if net.Stats().BatchesBroadcast >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait through one rebroadcast tick.
	time.Sleep(stuckRebroadcastThreshold + 2*rebroadcastInterval)

	m := lb.Metrics()
	if m.RebroadcastFires == 0 {
		t.Fatalf("RebroadcastFires stayed at zero across a stuck-detection firing")
	}
	// At least ONE of batches or headers should have been re-emitted.
	// In this test the worker created a batch but no header was
	// authored locally (no primary signer wired), so RebroadcastedBatches
	// is the one that grows; RebroadcastedHeaders stays zero. Production
	// runs see header re-emission too (PLAN §E7c — extended past the
	// initial batch-only variant after the 100K hard-stall sample
	// showed batches reaching quorum without headers getting certified).
	if m.RebroadcastedBatches == 0 && m.RebroadcastedHeaders == 0 {
		t.Fatalf("neither RebroadcastedBatches nor RebroadcastedHeaders grew across a stuck firing")
	}
}

// TestRebroadcast_OngoingCommitsKeepSuppression is the symmetric
// negative case: a healthy mid-run cluster (past startup, committing
// regularly) must never trigger the rebroadcast loop. lastCommitNanos
// keeps refreshing under regular NotifyCommitted, so gate (c) never
// elapses.
func TestRebroadcast_OngoingCommitsKeepSuppression(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)
	lb.SetStores(store.NewMemoryBatchStore(), store.NewMemoryCertificateStore(), store.NewMemoryTxIndex())
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Compress startup so the loop is allowed to fire if anything tripped.
	lb.startNanos.Store(time.Now().Add(-2 * rebroadcastStartupGrace).UnixNano())

	for i := 0; i < 500; i++ {
		_ = lb.AddTx([]byte("ab-" + string(rune('a'+i%26)) + string(rune('0'+i%10))))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if net.Stats().BatchesBroadcast >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	initial := net.Stats().BatchesBroadcast
	if initial < 1 {
		t.Fatalf("setup broken: expected at least 1 broadcast, got %d", initial)
	}

	// Tick NotifyCommitted at a healthy 1-Hz cadence past 2x the stuck
	// threshold. Each call refreshes lastCommitNanos; gate (c) should
	// never elapse.
	round := uint64(1)
	totalDuration := 2 * stuckRebroadcastThreshold
	deadline = time.Now().Add(totalDuration)
	for time.Now().Before(deadline) {
		lb.NotifyCommitted(round)
		round++
		time.Sleep(1 * time.Second)
	}

	if got := net.Stats().BatchesBroadcast; got > initial {
		t.Fatalf("rebroadcast fired during healthy commit cadence: initial=%d got=%d "+
			"(gate (c) stuck-threshold is leaking — would regress fast-path runs)",
			initial, got)
	}
}
