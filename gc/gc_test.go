package gc

import (
	"sync"
	"testing"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func createTestBatch(t *testing.T, workerID, validatorID uint16, round uint64) *types.Batch {
	t.Helper()
	txs := []types.Transaction{
		types.Transaction([]byte("tx1")),
		types.Transaction([]byte("tx2")),
	}
	return types.NewBatch(workerID, validatorID, round, txs)
}

func createTestCertificate(t *testing.T, author uint16, round uint64, batchDigests []types.Hash) *types.Certificate {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	batchRefs := make([]types.BatchDigest, len(batchDigests))
	for i, d := range batchDigests {
		batchRefs[i] = types.BatchDigest{Digest: d}
	}

	header := types.NewHeader(author, round, 0, batchRefs, nil)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	vote := types.NewVote(header.Digest, author)
	if err := vote.Sign(signer); err != nil {
		t.Fatalf("Failed to sign vote: %v", err)
	}

	return types.NewCertificate(header, []types.Vote{*vote})
}

func TestGCManagerStartStop(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	cfg := DefaultConfig()
	cfg.GCInterval = 100 * time.Millisecond
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	if err := gc.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !gc.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := gc.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := gc.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if gc.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := gc.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestGCManagerNotifyCommitted(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	cfg := DefaultConfig()
	cfg.GCDepth = 10
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Notify committed round
	err := gc.NotifyCommitted(50)
	if err != nil {
		t.Fatalf("NotifyCommitted failed: %v", err)
	}

	if gc.CommittedRound() != 50 {
		t.Errorf("Expected committed round 50, got %d", gc.CommittedRound())
	}

	// GC round should be 50 - 10 = 40
	gcRound := gc.GetGCRound()
	if gcRound != 40 {
		t.Errorf("Expected GC round 40, got %d", gcRound)
	}
}

func TestGCManagerNotifyCommittedNoGC(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	cfg := DefaultConfig()
	cfg.GCDepth = 100
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Committed round less than GC depth - no GC
	err := gc.NotifyCommitted(50)
	if err != nil {
		t.Fatalf("NotifyCommitted failed: %v", err)
	}

	gcRound := gc.GetGCRound()
	if gcRound != 0 {
		t.Errorf("Expected GC round 0 (no GC), got %d", gcRound)
	}
}

func TestGCManagerPruning(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add batches and certificates
	for round := uint64(0); round < 20; round++ {
		batch := createTestBatch(t, 0, 0, round)
		_ = batchStore.SaveBatch(batch)

		cert := createTestCertificate(t, 0, round, []types.Hash{batch.Digest})
		_ = d.AddCertificate(cert)
	}

	cfg := DefaultConfig()
	cfg.GCDepth = 10
	cfg.RecoverUncommittedTxs = false // Disable for this test
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Notify committed at round 15
	// GC should prune rounds < 5
	err := gc.NotifyCommitted(15)
	if err != nil {
		t.Fatalf("NotifyCommitted failed: %v", err)
	}

	// Rounds 0-4 should be pruned from DAG memory
	for round := uint64(0); round < 5; round++ {
		if d.GetRound(round) != nil {
			t.Errorf("Round %d should be pruned from DAG", round)
		}
	}

	// Rounds 5-19 should still exist
	for round := uint64(5); round < 20; round++ {
		if d.GetRound(round) == nil {
			t.Errorf("Round %d should still exist in DAG", round)
		}
	}
}

func TestGCManagerTxRecovery(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Create batches - some will be committed, some won't
	committedBatch := createTestBatch(t, 0, 0, 0)
	uncommittedBatch := createTestBatch(t, 1, 0, 0)

	_ = batchStore.SaveBatch(committedBatch)
	_ = batchStore.SaveBatch(uncommittedBatch)

	// Create certificate that only references committedBatch
	cert := createTestCertificate(t, 0, 0, []types.Hash{committedBatch.Digest})
	_ = d.AddCertificate(cert)

	cfg := DefaultConfig()
	cfg.GCDepth = 0 // GC immediately
	cfg.RecoverUncommittedTxs = true
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Track recovered transactions
	var recoveredTxs []types.Transaction
	var mu sync.Mutex
	gc.SetTxRecoveryCallback(func(txs []types.Transaction) {
		mu.Lock()
		recoveredTxs = append(recoveredTxs, txs...)
		mu.Unlock()
	})

	// Notify committed - should trigger GC and tx recovery
	err := gc.NotifyCommitted(1)
	if err != nil {
		t.Fatalf("NotifyCommitted failed: %v", err)
	}

	mu.Lock()
	recoveredCount := len(recoveredTxs)
	mu.Unlock()

	// Should recover transactions from uncommittedBatch
	if recoveredCount != 2 {
		t.Errorf("Expected 2 recovered transactions, got %d", recoveredCount)
	}
}

func TestGCManagerForceGC(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add some data
	for round := uint64(0); round < 10; round++ {
		cert := createTestCertificate(t, 0, round, nil)
		_ = d.AddCertificate(cert)
	}

	cfg := DefaultConfig()
	cfg.GCDepth = 5
	cfg.RecoverUncommittedTxs = false
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Set committed round
	gc.committedRound.Store(10)

	// Force GC
	err := gc.ForceGC()
	if err != nil {
		t.Fatalf("ForceGC failed: %v", err)
	}

	// Rounds 0-4 should be pruned
	for round := uint64(0); round < 5; round++ {
		if d.GetRound(round) != nil {
			t.Errorf("Round %d should be pruned", round)
		}
	}
}

func TestGCManagerGCLoop(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add some data
	for round := uint64(0); round < 20; round++ {
		cert := createTestCertificate(t, 0, round, nil)
		_ = d.AddCertificate(cert)
	}

	cfg := DefaultConfig()
	cfg.GCDepth = 5
	cfg.GCInterval = 50 * time.Millisecond
	cfg.RecoverUncommittedTxs = false
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	_ = gc.Start()
	defer func() { _ = gc.Stop() }()

	// Set committed round
	gc.committedRound.Store(15)

	// Wait for GC loop to run
	time.Sleep(100 * time.Millisecond)

	// Rounds 0-9 should be pruned (15 - 5 = 10, prune < 10)
	for round := uint64(0); round < 10; round++ {
		if d.GetRound(round) != nil {
			t.Errorf("Round %d should be pruned", round)
		}
	}
}

func TestGCManagerDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.GCDepth == 0 {
		t.Error("GCDepth should be non-zero")
	}

	if cfg.GCInterval <= 0 {
		t.Error("GCInterval should be positive")
	}
}
