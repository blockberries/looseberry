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

// TestGCManagerTxRecovery_ClearsTxIndex is the regression test for
// the burst-load tx loss reported as PLAN §E6
// (TestPhaseE_MultiBlockTPS). When a batch is uncommitted at GC time,
// its transactions are extracted and passed to the recovery callback.
// The downstream callback re-adds the txs via workerPool.AddTx, but
// Worker.AddTx short-circuits with nil if the tx hash is already
// indexed ("Already batched" idempotency path). Before this fix
// gc.extractUncommittedTxs did NOT clear the index entries first, so
// every recovered tx hit the idempotency path and was silently
// dropped. Net effect: 19% tx loss in the multi-block burst test.
//
// The fix: extractUncommittedTxs now calls txIndex.RemoveTxsForBatch
// for each uncommitted batch before returning. This test pins that
// behaviour — txIndex.HasTx must return false for the recovered tx
// hashes once extraction has run.
func TestGCManagerTxRecovery_ClearsTxIndex(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer certStore.Close()
	defer batchStore.Close()
	defer txIndex.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Build one committed and one uncommitted batch with DISTINCT tx
	// contents so the test can tell which txs come back.
	committedTxs := []types.Transaction{
		types.Transaction([]byte("committed-tx-1")),
		types.Transaction([]byte("committed-tx-2")),
	}
	uncommittedTxs := []types.Transaction{
		types.Transaction([]byte("uncommitted-tx-1")),
		types.Transaction([]byte("uncommitted-tx-2")),
	}
	committedBatch := types.NewBatch(0, 0, 0, committedTxs)
	uncommittedBatch := types.NewBatch(1, 0, 0, uncommittedTxs)
	_ = batchStore.SaveBatch(committedBatch)
	_ = batchStore.SaveBatch(uncommittedBatch)

	// Index every tx — this mirrors what Worker.tryCreateBatch does
	// when the worker hands a batch out.
	if err := txIndex.AddBatch(committedBatch); err != nil {
		t.Fatalf("txIndex.AddBatch(committed): %v", err)
	}
	if err := txIndex.AddBatch(uncommittedBatch); err != nil {
		t.Fatalf("txIndex.AddBatch(uncommitted): %v", err)
	}

	// Sanity: every tx is in the index.
	for _, tx := range uncommittedTxs {
		if !txIndex.HasTx(tx.Hash()) {
			t.Fatalf("setup broken: tx %q not indexed", string(tx))
		}
	}

	// Only the committed batch has a certificate.
	cert := createTestCertificate(t, 0, 0, []types.Hash{committedBatch.Digest})
	_ = d.AddCertificate(cert)

	cfg := DefaultConfig()
	cfg.GCDepth = 0
	cfg.RecoverUncommittedTxs = true
	gcm := NewGCManager(d, batchStore, certStore, txIndex, cfg)

	// Mirror what looseberry does in Start — register the uncommitted
	// batch so the fast-path index has it.
	gcm.TrackBatch(uncommittedBatch.Digest, 0)

	// Capture state AT THE MOMENT the recovery callback fires. The bug
	// is about ordering inside performGC: extractUncommittedTxs returns
	// the txs, then the callback runs (which would re-add via
	// workerPool.AddTx → Worker.AddTx → HasTx check). For the re-add
	// path to succeed, the txIndex entries must already be gone by the
	// time the callback fires. A later performGC step (PruneOlderThan)
	// also clears the index, but that's too late — the callback's
	// re-adds have already been silently dropped.
	var recovered []types.Transaction
	var hasTxAtCallback []bool
	var mu sync.Mutex
	gcm.SetTxRecoveryCallback(func(txs []types.Transaction) {
		mu.Lock()
		defer mu.Unlock()
		recovered = append(recovered, txs...)
		for _, tx := range txs {
			hasTxAtCallback = append(hasTxAtCallback, txIndex.HasTx(tx.Hash()))
		}
	})

	if err := gcm.NotifyCommitted(1); err != nil {
		t.Fatalf("NotifyCommitted: %v", err)
	}

	// We should see the uncommitted batch's txs come back…
	mu.Lock()
	gotN := len(recovered)
	hasSnap := append([]bool(nil), hasTxAtCallback...)
	mu.Unlock()
	if gotN != len(uncommittedTxs) {
		t.Fatalf("recovered %d txs, want %d", gotN, len(uncommittedTxs))
	}

	// …AND at the recovery-callback moment, txIndex.HasTx must already
	// return false for every recovered tx. Otherwise Worker.AddTx will
	// short-circuit with nil ("Already batched") and the tx is lost.
	for i, present := range hasSnap {
		if present {
			t.Fatalf("at recovery-callback time, txIndex still has recovered tx[%d] — "+
				"Worker.AddTx will silently drop this tx via the HasTx idempotency "+
				"path (PLAN §E6 burst-load tx loss)", i)
		}
	}

	// The committed batch's txs shouldn't be observable in recovered.
	for _, tx := range committedTxs {
		for _, rt := range recovered {
			if string(rt) == string(tx) {
				t.Fatalf("committed tx %q unexpectedly returned via recovery", string(tx))
			}
		}
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

// Test for Bug Fix #4: TxIndex Garbage Collection
func TestGCManagerTxIndexPruning(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer certStore.Close()
	defer batchStore.Close()
	defer txIndex.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add batches and index them
	for round := uint64(0); round < 10; round++ {
		batch := createTestBatch(t, 0, 0, round)
		_ = batchStore.SaveBatch(batch)
		_ = txIndex.AddBatch(batch)

		cert := createTestCertificate(t, 0, round, []types.Hash{batch.Digest})
		_ = d.AddCertificate(cert)
	}

	// Verify txIndex has entries
	initialLen := txIndex.Len()
	if initialLen == 0 {
		t.Fatal("TxIndex should have entries before GC")
	}

	cfg := DefaultConfig()
	cfg.GCDepth = 5
	cfg.RecoverUncommittedTxs = false
	gc := NewGCManager(d, batchStore, certStore, txIndex, cfg)

	// Set committed round and trigger GC
	gc.committedRound.Store(10)
	err := gc.ForceGC()
	if err != nil {
		t.Fatalf("ForceGC failed: %v", err)
	}

	// TxIndex should have fewer entries after pruning
	finalLen := txIndex.Len()
	if finalLen >= initialLen {
		t.Errorf("TxIndex should have fewer entries after pruning, had %d, now %d",
			initialLen, finalLen)
	}
}

// Test for Bug Fix #8: GC Error Logging and Metrics
func TestGCManagerMetrics(t *testing.T) {
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

	// Initial metrics
	metrics := gc.Metrics()
	if metrics.TotalGCRuns != 0 {
		t.Error("Initial TotalGCRuns should be 0")
	}

	_ = gc.Start()
	defer func() { _ = gc.Stop() }()

	// Set committed round to trigger GC
	gc.committedRound.Store(15)

	// Wait for GC loop to run
	time.Sleep(100 * time.Millisecond)

	// Check metrics
	metrics = gc.Metrics()
	if metrics.TotalGCRuns == 0 {
		t.Error("TotalGCRuns should be incremented after GC")
	}
	if metrics.LastGCRound == 0 {
		t.Error("LastGCRound should be set after GC")
	}
}

// mockLogger captures log messages for testing
type mockLogger struct {
	mu          sync.Mutex
	errorCalls  []string
	infoCalls   []string
	debugCalls  []string
}

func (l *mockLogger) Error(msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	l.errorCalls = append(l.errorCalls, msg)
	l.mu.Unlock()
}

func (l *mockLogger) Info(msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	l.infoCalls = append(l.infoCalls, msg)
	l.mu.Unlock()
}

func (l *mockLogger) Debug(msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	l.debugCalls = append(l.debugCalls, msg)
	l.mu.Unlock()
}

func TestGCManagerCustomLogger(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	for round := uint64(0); round < 10; round++ {
		cert := createTestCertificate(t, 0, round, nil)
		_ = d.AddCertificate(cert)
	}

	cfg := DefaultConfig()
	cfg.GCDepth = 5
	cfg.GCInterval = 50 * time.Millisecond
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	logger := &mockLogger{}
	gc.SetLogger(logger)

	_ = gc.Start()
	defer func() { _ = gc.Stop() }()

	gc.committedRound.Store(10)

	// Wait for GC to run
	time.Sleep(100 * time.Millisecond)

	logger.mu.Lock()
	debugCount := len(logger.debugCalls)
	logger.mu.Unlock()

	if debugCount == 0 {
		t.Error("Expected debug log message after successful GC")
	}
}

// Test Optimized GC Extraction (Performance Optimization #4)
func TestGCManagerOptimizedExtraction(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	cfg := DefaultConfig()
	cfg.GCDepth = 0 // GC immediately
	cfg.RecoverUncommittedTxs = true
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Create uncommitted batches and track them in the index
	uncommittedBatch1 := createTestBatch(t, 0, 0, 0)
	uncommittedBatch2 := createTestBatch(t, 1, 0, 1)
	_ = batchStore.SaveBatch(uncommittedBatch1)
	_ = batchStore.SaveBatch(uncommittedBatch2)

	// Track uncommitted batches in the optimized index
	gc.TrackBatch(uncommittedBatch1.Digest, 0)
	gc.TrackBatch(uncommittedBatch2.Digest, 1)

	if gc.UncommittedBatchCount() != 2 {
		t.Errorf("Expected 2 uncommitted batches, got %d", gc.UncommittedBatchCount())
	}

	// Mark one batch as committed
	gc.MarkBatchCommitted(uncommittedBatch1.Digest)

	if gc.UncommittedBatchCount() != 1 {
		t.Errorf("Expected 1 uncommitted batch after marking committed, got %d", gc.UncommittedBatchCount())
	}

	// Track recovered transactions
	var recoveredTxs []types.Transaction
	var mu sync.Mutex
	gc.SetTxRecoveryCallback(func(txs []types.Transaction) {
		mu.Lock()
		recoveredTxs = append(recoveredTxs, txs...)
		mu.Unlock()
	})

	// Add a certificate at round 2 to avoid parent validation issues
	cert := createTestCertificate(t, 0, 2, nil)
	_ = d.AddCertificate(cert)

	// Notify committed - should recover only uncommittedBatch2
	err := gc.NotifyCommitted(3)
	if err != nil {
		t.Fatalf("NotifyCommitted failed: %v", err)
	}

	mu.Lock()
	recoveredCount := len(recoveredTxs)
	mu.Unlock()

	// Should recover transactions from uncommittedBatch2 (2 txs based on createTestBatch)
	if recoveredCount != 2 {
		t.Errorf("Expected 2 recovered transactions from uncommittedBatch2, got %d", recoveredCount)
	}

	// Index should be empty after GC
	if gc.UncommittedBatchCount() != 0 {
		t.Errorf("Expected 0 uncommitted batches after GC, got %d", gc.UncommittedBatchCount())
	}
}

func TestGCManagerMarkBatchesCommitted(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	cfg := DefaultConfig()
	gc := NewGCManager(d, batchStore, certStore, nil, cfg)

	// Track multiple batches
	batch1 := createTestBatch(t, 0, 0, 0)
	batch2 := createTestBatch(t, 1, 0, 0)
	batch3 := createTestBatch(t, 2, 0, 0)

	gc.TrackBatch(batch1.Digest, 0)
	gc.TrackBatch(batch2.Digest, 0)
	gc.TrackBatch(batch3.Digest, 0)

	if gc.UncommittedBatchCount() != 3 {
		t.Errorf("Expected 3 uncommitted batches, got %d", gc.UncommittedBatchCount())
	}

	// Mark multiple as committed
	gc.MarkBatchesCommitted([]types.Hash{batch1.Digest, batch3.Digest})

	if gc.UncommittedBatchCount() != 1 {
		t.Errorf("Expected 1 uncommitted batch, got %d", gc.UncommittedBatchCount())
	}
}
