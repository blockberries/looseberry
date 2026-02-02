package primary

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func TestBatchFetcherStartStop(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	// Start
	if err := bf.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !bf.IsRunning() {
		t.Error("Expected IsRunning() to be true")
	}

	// Double start should fail
	if err := bf.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got %v", err)
	}

	// Stop
	if err := bf.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if bf.IsRunning() {
		t.Error("Expected IsRunning() to be false")
	}

	// Double stop should fail
	if err := bf.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got %v", err)
	}
}

func TestBatchFetcherAllBatchesAvailable(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	// Create and store a batch
	tx := types.Transaction([]byte("test tx"))
	batch := types.NewBatch(0, 0, 0, []types.Transaction{tx})
	_ = batchStore.SaveBatch(batch)

	// Create a header referencing the batch
	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	// All batches should be available
	if !bf.RequestBatchesForHeader(header) {
		t.Error("Expected RequestBatchesForHeader to return true when all batches available")
	}
}

func TestBatchFetcherMissingBatchRequest(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	// Track batch requests
	var requestedBatches []types.Hash
	var mu sync.Mutex
	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		mu.Lock()
		requestedBatches = append(requestedBatches, digest)
		mu.Unlock()
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	// Create a header referencing a missing batch
	tx := types.Transaction([]byte("test tx"))
	batch := types.NewBatch(0, 0, 0, []types.Transaction{tx})

	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	// Should return false (batches missing)
	if bf.RequestBatchesForHeader(header) {
		t.Error("Expected RequestBatchesForHeader to return false when batches missing")
	}

	// Wait for request
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(requestedBatches) != 1 {
		t.Errorf("Expected 1 batch request, got %d", len(requestedBatches))
	}
	if len(requestedBatches) > 0 && requestedBatches[0] != batch.Digest {
		t.Error("Requested wrong batch digest")
	}
	mu.Unlock()
}

func TestBatchFetcherBatchReceived(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	// Track ready headers
	var readyHeaders []*types.Header
	var mu sync.Mutex
	bf.SetHeaderReadyCallback(func(header *types.Header) {
		mu.Lock()
		readyHeaders = append(readyHeaders, header)
		mu.Unlock()
	})

	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	// Create a header referencing a missing batch
	tx := types.Transaction([]byte("test tx"))
	batch := types.NewBatch(0, 0, 0, []types.Transaction{tx})

	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	// Request batches (returns false)
	bf.RequestBatchesForHeader(header)

	// Verify header is pending
	if bf.PendingHeaderCount() != 1 {
		t.Errorf("Expected 1 pending header, got %d", bf.PendingHeaderCount())
	}

	// Simulate batch being received
	bf.NotifyBatchReceived(batch)

	// Wait for callback
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(readyHeaders) != 1 {
		t.Errorf("Expected 1 ready header, got %d", len(readyHeaders))
	}
	if len(readyHeaders) > 0 && readyHeaders[0].Digest != header.Digest {
		t.Error("Wrong header marked as ready")
	}
	mu.Unlock()

	// Verify header is no longer pending
	if bf.PendingHeaderCount() != 0 {
		t.Errorf("Expected 0 pending headers, got %d", bf.PendingHeaderCount())
	}
}

func TestBatchFetcherMultipleBatches(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	readyCount := atomic.Int32{}
	bf.SetHeaderReadyCallback(func(header *types.Header) {
		readyCount.Add(1)
	})

	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	// Create header with 3 missing batches
	tx := types.Transaction([]byte("test tx"))
	batch1 := types.NewBatch(0, 0, 0, []types.Transaction{tx})
	batch2 := types.NewBatch(1, 0, 0, []types.Transaction{tx})
	batch3 := types.NewBatch(2, 0, 0, []types.Transaction{tx})

	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{
		batch1.GetDigest(),
		batch2.GetDigest(),
		batch3.GetDigest(),
	}, nil)
	_ = header.Sign(signer)

	// Request batches
	bf.RequestBatchesForHeader(header)

	// Receive first 2 batches - header shouldn't be ready
	bf.NotifyBatchReceived(batch1)
	bf.NotifyBatchReceived(batch2)

	time.Sleep(10 * time.Millisecond)
	if readyCount.Load() != 0 {
		t.Error("Header should not be ready with only 2/3 batches")
	}

	// Receive third batch - header should be ready
	bf.NotifyBatchReceived(batch3)

	time.Sleep(10 * time.Millisecond)
	if readyCount.Load() != 1 {
		t.Errorf("Expected 1 ready header, got %d", readyCount.Load())
	}
}

func TestBatchFetcherHandleBatchResponse(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	readyCount := atomic.Int32{}
	bf.SetHeaderReadyCallback(func(header *types.Header) {
		readyCount.Add(1)
	})

	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	// Create header with missing batch
	tx := types.Transaction([]byte("test tx"))
	batch := types.NewBatch(0, 0, 0, []types.Transaction{tx})

	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	// Request batches
	bf.RequestBatchesForHeader(header)

	// Handle batch response
	bf.HandleBatchResponse(batch, true, 1)

	time.Sleep(10 * time.Millisecond)

	// Verify batch was stored
	if !batchStore.HasBatch(batch.Digest) {
		t.Error("Batch should have been stored")
	}

	// Verify header is ready
	if readyCount.Load() != 1 {
		t.Errorf("Expected 1 ready header, got %d", readyCount.Load())
	}
}

func TestBatchFetcherMaxPendingHeaders(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	cfg.MaxPendingHeaders = 2
	bf := NewBatchFetcher(cfg, batchStore)

	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	signer, _ := types.GenerateEd25519Signer(0)

	// Add 2 headers (should succeed)
	for i := 0; i < 2; i++ {
		tx := types.Transaction([]byte{byte(i)})
		batch := types.NewBatch(uint16(i), 0, 0, []types.Transaction{tx})
		header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
		_ = header.Sign(signer)

		bf.RequestBatchesForHeader(header)
	}

	// Third header should be rejected
	tx := types.Transaction([]byte{99})
	batch := types.NewBatch(99, 0, 0, []types.Transaction{tx})
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	if bf.RequestBatchesForHeader(header) {
		t.Error("Expected third header to be rejected when max pending reached")
	}

	if bf.PendingHeaderCount() != 2 {
		t.Errorf("Expected 2 pending headers, got %d", bf.PendingHeaderCount())
	}
}

func TestBatchFetcherMetrics(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	bf.SetRequestCallback(func(validator uint16, digest types.Hash) error {
		return nil
	})

	_ = bf.Start()
	defer bf.Stop()

	// Create header with missing batch
	tx := types.Transaction([]byte("test tx"))
	batch := types.NewBatch(0, 0, 0, []types.Transaction{tx})

	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{batch.GetDigest()}, nil)
	_ = header.Sign(signer)

	// Request batches
	bf.RequestBatchesForHeader(header)

	metrics := bf.Metrics()
	if metrics.PendingHeaders != 1 {
		t.Errorf("Expected 1 pending header, got %d", metrics.PendingHeaders)
	}
	if metrics.PendingRequests != 1 {
		t.Errorf("Expected 1 pending request, got %d", metrics.PendingRequests)
	}
	if metrics.BatchesRequested != 1 {
		t.Errorf("Expected 1 batch requested, got %d", metrics.BatchesRequested)
	}

	// Receive batch
	bf.NotifyBatchReceived(batch)

	time.Sleep(10 * time.Millisecond)

	metrics = bf.Metrics()
	if metrics.PendingHeaders != 0 {
		t.Errorf("Expected 0 pending headers, got %d", metrics.PendingHeaders)
	}
	if metrics.BatchesReceived != 1 {
		t.Errorf("Expected 1 batch received, got %d", metrics.BatchesReceived)
	}
	if metrics.HeadersReady != 1 {
		t.Errorf("Expected 1 header ready, got %d", metrics.HeadersReady)
	}
}

func TestBatchFetcherEmptyHeader(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	_ = bf.Start()
	defer bf.Stop()

	// Empty header should return true immediately
	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signer)

	if !bf.RequestBatchesForHeader(header) {
		t.Error("Expected empty header to return true")
	}

	// Nil header should return true
	if !bf.RequestBatchesForHeader(nil) {
		t.Error("Expected nil header to return true")
	}
}

func TestBatchFetcherUpdateValidators(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	cfg := DefaultBatchFetcherConfig()
	bf := NewBatchFetcher(cfg, batchStore)

	bf.UpdateValidators([]uint16{0, 1, 2, 3})

	bf.validatorsMu.RLock()
	if len(bf.validators) != 4 {
		t.Errorf("Expected 4 validators, got %d", len(bf.validators))
	}
	bf.validatorsMu.RUnlock()
}
