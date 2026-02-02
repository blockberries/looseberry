package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func TestPoolStartStop(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 2
	cfg.MaxWorkers = 4
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)

	if pool.IsRunning() {
		t.Error("Should not be running initially")
	}

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !pool.IsRunning() {
		t.Error("Should be running after start")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers (min), got %d", pool.WorkerCount())
	}

	// Double start should fail
	if err := pool.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := pool.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if pool.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := pool.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestPoolAddTx(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 2
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	tx := types.Transaction([]byte("test_tx"))
	if err := pool.AddTx(tx); err != nil {
		t.Fatalf("AddTx failed: %v", err)
	}

	if pool.PendingCount() != 1 {
		t.Errorf("Expected 1 pending tx, got %d", pool.PendingCount())
	}

	if !pool.HasTx(tx.Hash()) {
		t.Error("Should have tx after add")
	}
}

func TestPoolAddTxRouting(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 4
	cfg.MaxWorkers = 4
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Add multiple transactions
	for i := range 100 {
		tx := types.Transaction([]byte{byte(i)})
		if err := pool.AddTx(tx); err != nil {
			t.Fatalf("AddTx %d failed: %v", i, err)
		}
	}

	// Transactions should be distributed across workers
	totalPending := 0
	for i := range 4 {
		w := pool.GetWorker(i)
		totalPending += w.PendingCount()
	}

	if totalPending != 100 {
		t.Errorf("Expected 100 total pending, got %d", totalPending)
	}

	if pool.PendingCount() != 100 {
		t.Errorf("Pool pending count should be 100, got %d", pool.PendingCount())
	}
}

func TestPoolAddTxNotRunning(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)

	tx := types.Transaction([]byte("test"))
	if err := pool.AddTx(tx); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestPoolScaleUp(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 3
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	if pool.WorkerCount() != 1 {
		t.Fatalf("Expected 1 worker, got %d", pool.WorkerCount())
	}

	// Scale up
	if !pool.ScaleUp() {
		t.Error("ScaleUp should succeed")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	// Scale up again
	if !pool.ScaleUp() {
		t.Error("ScaleUp should succeed")
	}

	if pool.WorkerCount() != 3 {
		t.Errorf("Expected 3 workers, got %d", pool.WorkerCount())
	}

	// Scale up at max should fail
	if pool.ScaleUp() {
		t.Error("ScaleUp should fail at max workers")
	}

	if pool.WorkerCount() != 3 {
		t.Errorf("Worker count should still be 3, got %d", pool.WorkerCount())
	}
}

func TestPoolScaleDown(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 3
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Scale up to 3
	pool.ScaleUp()
	pool.ScaleUp()

	if pool.WorkerCount() != 3 {
		t.Fatalf("Expected 3 workers, got %d", pool.WorkerCount())
	}

	// Scale down
	if !pool.ScaleDown() {
		t.Error("ScaleDown should succeed")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	// Scale down again
	if !pool.ScaleDown() {
		t.Error("ScaleDown should succeed")
	}

	if pool.WorkerCount() != 1 {
		t.Errorf("Expected 1 worker, got %d", pool.WorkerCount())
	}

	// Scale down at min should fail
	if pool.ScaleDown() {
		t.Error("ScaleDown should fail at min workers")
	}

	if pool.WorkerCount() != 1 {
		t.Errorf("Worker count should still be 1, got %d", pool.WorkerCount())
	}
}

func TestPoolSetRoundEpoch(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 2
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	pool.SetRound(10)
	if pool.Round() != 10 {
		t.Errorf("Expected round 10, got %d", pool.Round())
	}

	// All workers should have updated round
	for i := range 2 {
		w := pool.GetWorker(i)
		if w.Round() != 10 {
			t.Errorf("Worker %d round should be 10, got %d", i, w.Round())
		}
	}

	pool.SetEpoch(5)
	if pool.Epoch() != 5 {
		t.Errorf("Expected epoch 5, got %d", pool.Epoch())
	}
}

func TestPoolBatchCallback(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var batches []*types.Batch
	var mu sync.Mutex

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 2
	cfg.Worker.BatchTimeout = 50 * time.Millisecond

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	pool.SetBatchCallback(func(batch *types.Batch) {
		mu.Lock()
		batches = append(batches, batch)
		mu.Unlock()
	})

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Add transactions
	for i := range 5 {
		tx := types.Transaction([]byte{byte(i)})
		_ = pool.AddTx(tx)
	}

	// Wait for batches
	time.Sleep(cfg.Worker.BatchTimeout + 100*time.Millisecond)

	mu.Lock()
	batchCount := len(batches)
	mu.Unlock()

	// Should have at least one batch
	if batchCount == 0 {
		t.Error("Expected at least one batch to be created")
	}
}

func TestPoolTxValidator(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 2
	cfg.Worker.BatchTimeout = 1 * time.Second

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	pool.SetTxValidator(func(tx []byte) error {
		if len(tx) == 0 {
			return types.ErrTxValidationFailed
		}
		return nil
	})

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Empty tx should fail
	emptyTx := types.Transaction([]byte{})
	if err := pool.AddTx(emptyTx); err != types.ErrTxValidationFailed {
		t.Errorf("Expected ErrTxValidationFailed, got: %v", err)
	}

	// Valid tx should succeed
	validTx := types.Transaction([]byte("valid"))
	if err := pool.AddTx(validTx); err != nil {
		t.Fatalf("AddTx for valid tx failed: %v", err)
	}
}

func TestPoolConcurrency(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 4
	cfg.MaxWorkers = 4
	cfg.Worker.MaxPendingTxs = 10000
	cfg.Worker.BatchTimeout = 200 * time.Millisecond

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Concurrent adds
	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tx := types.Transaction([]byte{byte(n), byte(n >> 8)})
			_ = pool.AddTx(tx)
		}(i)
	}
	wg.Wait()

	// All transactions should be pending or batched
	// Can't check exact count due to timing
}

func TestPoolRecordAck(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	var createdBatch *types.Batch
	var mu sync.Mutex

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.Worker.BatchTimeout = 50 * time.Millisecond

	pool := NewPool(cfg, 0, batchStore, txIndex, 2) // quorum = 2
	pool.SetBatchCallback(func(batch *types.Batch) {
		mu.Lock()
		createdBatch = batch
		mu.Unlock()
	})

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	tx := types.Transaction([]byte("test"))
	_ = pool.AddTx(tx)

	// Wait for batch
	time.Sleep(cfg.Worker.BatchTimeout + 50*time.Millisecond)

	mu.Lock()
	batch := createdBatch
	mu.Unlock()

	if batch == nil {
		t.Fatal("Batch should be created")
	}

	// Record acks
	quorum := pool.RecordAck(batch.Digest, 0)
	if quorum {
		t.Error("Should not have quorum after 1 ack")
	}

	quorum = pool.RecordAck(batch.Digest, 1)
	if !quorum {
		t.Error("Should have quorum after 2 acks")
	}
}

func TestPoolDefaultConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.MinWorkers <= 0 {
		t.Error("MinWorkers should be positive")
	}

	if cfg.MaxWorkers < cfg.MinWorkers {
		t.Error("MaxWorkers should be >= MinWorkers")
	}

	if cfg.Worker.BatchSize <= 0 {
		t.Error("Worker.BatchSize should be positive")
	}
}

// Test for Bug Fix #3: Worker Scale-Down Drain
func TestPoolScaleDownRedistributesTxs(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 2
	cfg.Worker.BatchTimeout = 10 * time.Second // Long timeout to keep txs pending
	cfg.Worker.MaxPendingTxs = 1000

	pool := NewPool(cfg, 0, batchStore, txIndex, 3)

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Scale up to 2 workers
	pool.ScaleUp()
	if pool.WorkerCount() != 2 {
		t.Fatalf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	// Add many transactions - some will go to each worker
	for i := range 100 {
		tx := types.Transaction([]byte{byte(i), byte(i >> 8)})
		_ = pool.AddTx(tx)
	}

	initialPending := pool.PendingCount()
	if initialPending == 0 {
		t.Fatal("Expected pending transactions")
	}

	// Scale down - should redistribute transactions from removed worker
	if !pool.ScaleDown() {
		t.Fatal("ScaleDown should succeed")
	}

	if pool.WorkerCount() != 1 {
		t.Errorf("Expected 1 worker after scale down, got %d", pool.WorkerCount())
	}

	// Pending count should be preserved (no transaction loss)
	finalPending := pool.PendingCount()
	if finalPending < initialPending {
		t.Errorf("Expected at least %d pending after scale down, got %d (transaction loss!)",
			initialPending, finalPending)
	}
}
