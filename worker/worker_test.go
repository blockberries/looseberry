package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func TestWorkerAddTx(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.BatchTimeout = 1 * time.Second // Slow to avoid auto-batching
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	tx := types.Transaction([]byte("test_tx"))
	if err := w.AddTx(tx); err != nil {
		t.Fatalf("AddTx failed: %v", err)
	}

	if w.PendingCount() != 1 {
		t.Errorf("Expected 1 pending tx, got %d", w.PendingCount())
	}

	if !w.HasTx(tx.Hash()) {
		t.Error("Should have tx after add")
	}
}

func TestWorkerAddTxDeduplication(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.BatchTimeout = 1 * time.Second
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	tx := types.Transaction([]byte("test_tx"))

	// Add same tx twice
	_ = w.AddTx(tx)
	_ = w.AddTx(tx)

	if w.PendingCount() != 1 {
		t.Errorf("Duplicate tx should be deduplicated, got %d pending", w.PendingCount())
	}
}

func TestWorkerAddTxBackpressure(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.MaxPendingTxs = 5
	cfg.BatchTimeout = 1 * time.Second
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Add up to limit
	for i := range 5 {
		tx := types.Transaction([]byte{byte(i)})
		if err := w.AddTx(tx); err != nil {
			t.Fatalf("AddTx %d failed: %v", i, err)
		}
	}

	// Next should fail
	tx := types.Transaction([]byte("overflow"))
	if err := w.AddTx(tx); err != types.ErrWorkerBackpressure {
		t.Errorf("Expected ErrWorkerBackpressure, got: %v", err)
	}
}

func TestWorkerAddTxBytesBackpressure(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.MaxPendingBytes = 100
	cfg.BatchTimeout = 1 * time.Second
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Add transactions until bytes limit
	tx1 := types.Transaction(make([]byte, 50))
	if err := w.AddTx(tx1); err != nil {
		t.Fatalf("AddTx 1 failed: %v", err)
	}

	// This should work (90 bytes total)
	tx2 := types.Transaction(make([]byte, 40))
	if err := w.AddTx(tx2); err != nil {
		t.Fatalf("AddTx 2 failed: %v", err)
	}

	// This should fail (would exceed 100 bytes)
	tx3 := types.Transaction(make([]byte, 20))
	if err := w.AddTx(tx3); err != types.ErrWorkerBackpressure {
		t.Errorf("Expected ErrWorkerBackpressure, got: %v", err)
	}
}

func TestWorkerBatchCreation(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var createdBatch *types.Batch
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.BatchTimeout = 50 * time.Millisecond
	w := New(0, 0, cfg, batchStore, txIndex, 3)
	w.SetBatchCallback(func(batch *types.Batch) {
		mu.Lock()
		createdBatch = batch
		mu.Unlock()
	})

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Add transaction
	tx := types.Transaction([]byte("test_tx"))
	if err := w.AddTx(tx); err != nil {
		t.Fatalf("AddTx failed: %v", err)
	}

	// Wait for batch creation
	time.Sleep(cfg.BatchTimeout + 50*time.Millisecond)

	mu.Lock()
	batch := createdBatch
	mu.Unlock()

	if batch == nil {
		t.Fatal("Batch should be created after timeout")
	}

	if len(batch.Transactions) != 1 {
		t.Errorf("Expected 1 tx in batch, got %d", len(batch.Transactions))
	}

	// Batch should be stored
	if !batchStore.HasBatch(batch.Digest) {
		t.Error("Batch should be stored in batch store")
	}

	// Transaction should be indexed
	if !txIndex.HasTx(tx.Hash()) {
		t.Error("Transaction should be indexed")
	}

	// Pending should be empty
	if w.PendingCount() != 0 {
		t.Errorf("Pending should be empty after batch creation, got %d", w.PendingCount())
	}
}

func TestWorkerBatchCreationSizeLimit(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var batches []*types.Batch
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.BatchSize = 3
	cfg.BatchTimeout = 50 * time.Millisecond
	w := New(0, 0, cfg, batchStore, txIndex, 3)
	w.SetBatchCallback(func(batch *types.Batch) {
		mu.Lock()
		batches = append(batches, batch)
		mu.Unlock()
	})

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Add 5 transactions
	for i := range 5 {
		tx := types.Transaction([]byte{byte(i)})
		if err := w.AddTx(tx); err != nil {
			t.Fatalf("AddTx %d failed: %v", i, err)
		}
	}

	// Wait for batch creation
	time.Sleep(cfg.BatchTimeout*2 + 50*time.Millisecond)

	mu.Lock()
	batchCount := len(batches)
	mu.Unlock()

	// Should create 2 batches (3 + 2)
	if batchCount < 2 {
		t.Errorf("Expected at least 2 batches, got %d", batchCount)
	}
}

func TestWorkerTxValidator(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.BatchTimeout = 1 * time.Second
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	// Set validator that rejects empty transactions
	w.SetTxValidator(func(tx []byte) error {
		if len(tx) == 0 {
			return types.ErrTxValidationFailed
		}
		return nil
	})

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Empty tx should fail validation
	emptyTx := types.Transaction([]byte{})
	if err := w.AddTx(emptyTx); err != types.ErrTxValidationFailed {
		t.Errorf("Expected ErrTxValidationFailed, got: %v", err)
	}

	// Non-empty tx should succeed
	validTx := types.Transaction([]byte("valid"))
	if err := w.AddTx(validTx); err != nil {
		t.Fatalf("AddTx for valid tx failed: %v", err)
	}
}

func TestWorkerNotRunning(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	// AddTx should fail when not running
	tx := types.Transaction([]byte("test"))
	if err := w.AddTx(tx); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestWorkerStartStop(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if w.IsRunning() {
		t.Error("Should not be running initially")
	}

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !w.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := w.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := w.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if w.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := w.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestWorkerRoundAndEpoch(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if w.Round() != 0 {
		t.Error("Initial round should be 0")
	}

	if w.Epoch() != 0 {
		t.Error("Initial epoch should be 0")
	}

	w.SetRound(10)
	if w.Round() != 10 {
		t.Errorf("Expected round 10, got %d", w.Round())
	}

	w.SetEpoch(5)
	if w.Epoch() != 5 {
		t.Errorf("Expected epoch 5, got %d", w.Epoch())
	}
}

func TestWorkerRecordAck(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var createdBatch *types.Batch
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.BatchTimeout = 50 * time.Millisecond
	w := New(0, 0, cfg, batchStore, txIndex, 2) // quorum = 2
	w.SetBatchCallback(func(batch *types.Batch) {
		mu.Lock()
		createdBatch = batch
		mu.Unlock()
	})

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	tx := types.Transaction([]byte("test"))
	_ = w.AddTx(tx)

	// Wait for batch
	time.Sleep(cfg.BatchTimeout + 50*time.Millisecond)

	mu.Lock()
	batch := createdBatch
	mu.Unlock()

	if batch == nil {
		t.Fatal("Batch should be created")
	}

	// Record acks
	quorum := w.RecordAck(batch.Digest, 0)
	if quorum {
		t.Error("Should not have quorum after 1 ack")
	}

	quorum = w.RecordAck(batch.Digest, 1)
	if !quorum {
		t.Error("Should have quorum after 2 acks")
	}
}

func TestWorkerConcurrency(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultConfig()
	cfg.MaxPendingTxs = 10000
	cfg.BatchTimeout = 200 * time.Millisecond
	w := New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Concurrent adds
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tx := types.Transaction([]byte{byte(n)})
			_ = w.AddTx(tx)
		}(i)
	}
	wg.Wait()

	// All should be pending or batched
	// Can't easily check exact count due to timing
}

func TestWorkerStopFlushes(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var createdBatch *types.Batch

	cfg := DefaultConfig()
	cfg.BatchTimeout = 10 * time.Second // Long timeout to ensure we don't auto-batch
	w := New(0, 0, cfg, batchStore, txIndex, 3)
	w.SetBatchCallback(func(batch *types.Batch) {
		createdBatch = batch
	})

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	tx := types.Transaction([]byte("test"))
	_ = w.AddTx(tx)

	// Stop should flush pending
	_ = w.Stop()

	if createdBatch == nil {
		t.Fatal("Stop should create final batch with pending transactions")
	}

	if len(createdBatch.Transactions) != 1 {
		t.Errorf("Expected 1 tx in final batch, got %d", len(createdBatch.Transactions))
	}
}

func TestWorkerDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BatchSize <= 0 {
		t.Error("BatchSize should be positive")
	}

	if cfg.BatchTimeout <= 0 {
		t.Error("BatchTimeout should be positive")
	}

	if cfg.MaxPendingTxs <= 0 {
		t.Error("MaxPendingTxs should be positive")
	}

	if cfg.MaxPendingBytes <= 0 {
		t.Error("MaxPendingBytes should be positive")
	}

	if cfg.AckTimeout <= 0 {
		t.Error("AckTimeout should be positive")
	}
}
