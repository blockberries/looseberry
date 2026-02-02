package looseberry_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockberries/looseberry"
	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
	"github.com/blockberries/looseberry/worker"
)

// ============================================================================
// Worker Pool Stress Tests
// ============================================================================

func TestStressWorkerPoolHighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := worker.PoolConfig{
		MinWorkers: 4,
		MaxWorkers: 8,
		Worker:     worker.DefaultConfig(),
	}
	pool := worker.NewPool(cfg, 0, batchStore, txIndex, 4)

	var batchCount atomic.Int64
	pool.SetBatchCallback(func(batch *types.Batch) {
		batchCount.Add(1)
	})

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Submit 100k transactions
	const txCount = 100000
	start := time.Now()

	for i := 0; i < txCount; i++ {
		tx := types.Transaction(fmt.Appendf(nil, "stress-tx-%d", i))
		// Backpressure is expected under high load, just ignore errors
		_ = pool.AddTx(tx)
	}

	elapsed := time.Since(start)
	txPerSec := float64(txCount) / elapsed.Seconds()

	t.Logf("Submitted %d transactions in %v (%.0f tx/sec)", txCount, elapsed, txPerSec)
	t.Logf("Batches created: %d", batchCount.Load())

	// Should have reasonable throughput
	if txPerSec < 10000 {
		t.Errorf("Throughput too low: %.0f tx/sec (expected >10000)", txPerSec)
	}
}

func TestStressWorkerPoolConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	workerCfg := worker.DefaultConfig()
	workerCfg.MaxPendingTxs = 50000 // Increase to handle high concurrent load
	workerCfg.MaxPendingBytes = 100 * 1024 * 1024

	cfg := worker.PoolConfig{
		MinWorkers: 4,
		MaxWorkers: 8,
		Worker:     workerCfg,
	}
	pool := worker.NewPool(cfg, 0, batchStore, txIndex, 4)

	var batchCount atomic.Int64
	pool.SetBatchCallback(func(batch *types.Batch) {
		batchCount.Add(1)
	})

	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Submit from multiple goroutines
	const numGoroutines = 10
	const txPerGoroutine = 10000

	var wg sync.WaitGroup
	var totalSubmitted atomic.Int64

	start := time.Now()

	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range txPerGoroutine {
				tx := types.Transaction(fmt.Appendf(nil, "stress-tx-%d-%d", gid, i))
				if err := pool.AddTx(tx); err == nil {
					totalSubmitted.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	submitted := totalSubmitted.Load()
	txPerSec := float64(submitted) / elapsed.Seconds()

	t.Logf("Submitted %d/%d transactions in %v (%.0f tx/sec)",
		submitted, numGoroutines*txPerGoroutine, elapsed, txPerSec)
	t.Logf("Batches created: %d", batchCount.Load())

	// Should have most transactions submitted (some may hit backpressure)
	if submitted < int64(numGoroutines*txPerGoroutine*6/10) {
		t.Errorf("Too many rejected transactions: %d/%d",
			numGoroutines*txPerGoroutine-int(submitted), numGoroutines*txPerGoroutine)
	}
}

// ============================================================================
// DAG Stress Tests
// ============================================================================

func TestStressDAGHighRoundCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	const numRounds = 1000
	const numValidators = 4

	signers := make([]*types.Ed25519Signer, numValidators)
	for i := range numValidators {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
	}

	start := time.Now()

	// Add certificates for many rounds
	for round := uint64(0); round < numRounds; round++ {
		for val := uint16(0); val < numValidators; val++ {
			header := types.NewHeader(val, round, 0, nil, nil)
			_ = header.Sign(signers[val])
			vote := types.NewVote(header.Digest, val)
			_ = vote.Sign(signers[val])
			cert := types.NewCertificate(header, []types.Vote{*vote})
			if err := d.AddCertificate(cert); err != nil {
				t.Fatalf("AddCertificate failed at round %d: %v", round, err)
			}
		}
	}

	elapsed := time.Since(start)
	certsPerSec := float64(numRounds*numValidators) / elapsed.Seconds()

	t.Logf("Added %d certificates (%d rounds × %d validators) in %v (%.0f certs/sec)",
		numRounds*numValidators, numRounds, numValidators, elapsed, certsPerSec)

	// Verify DAG state
	if d.HighestRound() != numRounds-1 {
		t.Errorf("HighestRound = %d, expected %d", d.HighestRound(), numRounds-1)
	}

	// Test retrieval performance
	start = time.Now()
	for i := 0; i < 10000; i++ {
		round := uint64(i % numRounds)
		certs := d.GetCertificatesForRound(round)
		if len(certs) != numValidators {
			t.Errorf("Round %d has %d certificates, expected %d", round, len(certs), numValidators)
		}
	}
	retrievalElapsed := time.Since(start)
	t.Logf("10000 round retrievals in %v", retrievalElapsed)
}

func TestStressDAGConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	const numGoroutines = 8
	const operationsPerGoroutine = 10000

	signers := make([]*types.Ed25519Signer, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
	}

	var wg sync.WaitGroup
	var round atomic.Uint64

	// Writers
	for g := 0; g < numGoroutines/2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				r := round.Add(1) - 1
				val := uint16(gid % 4)
				header := types.NewHeader(val, r, 0, nil, nil)
				_ = header.Sign(signers[val])
				vote := types.NewVote(header.Digest, val)
				_ = vote.Sign(signers[val])
				cert := types.NewCertificate(header, []types.Vote{*vote})
				_ = d.AddCertificate(cert)
			}
		}(g)
	}

	// Readers
	for g := numGoroutines / 2; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				hr := d.HighestRound()
				if hr > 0 {
					_ = d.GetCertificatesForRound(hr - 1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("Concurrent operations completed. Final round: %d", d.HighestRound())
}

// ============================================================================
// Store Stress Tests
// ============================================================================

func TestStressBatchStoreConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	batchStore := store.NewMemoryBatchStore()
	defer batchStore.Close()

	const numGoroutines = 8
	const operationsPerGoroutine = 5000

	var wg sync.WaitGroup
	var savedDigests sync.Map

	// Writers
	for g := 0; g < numGoroutines/2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				batch := types.NewBatch(0, 0, uint64(gid*operationsPerGoroutine+i), []types.Transaction{
					types.Transaction(fmt.Appendf(nil, "tx-%d-%d", gid, i)),
				})
				if err := batchStore.SaveBatch(batch); err != nil {
					t.Errorf("SaveBatch failed: %v", err)
					return
				}
				savedDigests.Store(batch.Digest, true)
			}
		}(g)
	}

	// Readers (after a brief delay to have some data)
	time.Sleep(10 * time.Millisecond)
	for g := numGoroutines / 2; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				// Try to read random stored digests
				savedDigests.Range(func(key, _ any) bool {
					digest := key.(types.Hash)
					_, _ = batchStore.GetBatch(digest)
					return false // Just check one
				})
			}
		}()
	}

	wg.Wait()

	// Count saved batches
	var count int
	savedDigests.Range(func(_, _ any) bool {
		count++
		return true
	})

	t.Logf("Concurrent batch store operations completed. Batches saved: %d", count)
}

func TestStressTxIndexConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	txIndex := store.NewMemoryTxIndex()
	defer txIndex.Close()

	const numGoroutines = 8
	const operationsPerGoroutine = 10000

	var wg sync.WaitGroup
	var addedHashes sync.Map

	// Writers
	for g := 0; g < numGoroutines/2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				tx := types.Transaction(fmt.Appendf(nil, "tx-%d-%d", gid, i))
				h := tx.Hash()
				if err := txIndex.AddTx(h, h); err != nil {
					t.Errorf("AddTx failed: %v", err)
					return
				}
				addedHashes.Store(h, true)
			}
		}(g)
	}

	// Readers
	for g := numGoroutines / 2; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				addedHashes.Range(func(key, _ any) bool {
					h := key.(types.Hash)
					_ = txIndex.HasTx(h)
					return false
				})
			}
		}()
	}

	wg.Wait()

	var count int
	addedHashes.Range(func(_, _ any) bool {
		count++
		return true
	})

	t.Logf("Concurrent tx index operations completed. Txs indexed: %d", count)
}

// ============================================================================
// Full System Stress Tests
// ============================================================================

func TestStressLooseberryHighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)

	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	vs := types.NewSimpleValidatorSet(validators, 0)

	cfg := looseberry.DefaultConfig()
	cfg.Signer = signers[0]
	cfg.ValidatorIndex = 0
	cfg.Storage.InMemory = true
	cfg.Worker.MaxPendingTxs = 100000
	cfg.Worker.MaxPendingBytes = 500 * 1024 * 1024 // 500MB

	lb, err := looseberry.New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	const txCount = 50000
	start := time.Now()

	var submitted int
	for i := 0; i < txCount; i++ {
		tx := fmt.Appendf(nil, "stress-tx-%d", i)
		if err := lb.AddTx(tx); err == nil {
			submitted++
		}
	}

	elapsed := time.Since(start)
	txPerSec := float64(submitted) / elapsed.Seconds()

	metrics := lb.Metrics()
	t.Logf("Submitted %d/%d transactions in %v (%.0f tx/sec)", submitted, txCount, elapsed, txPerSec)
	t.Logf("Metrics: TotalTxAdded=%d, TotalBatches=%d, PendingTx=%d",
		metrics.TotalTxAdded, metrics.TotalBatches, metrics.PendingTxCount)

	// Should have reasonable throughput
	if txPerSec < 5000 {
		t.Errorf("Throughput too low: %.0f tx/sec (expected >5000)", txPerSec)
	}
}

func TestStressLooseberryConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)

	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	vs := types.NewSimpleValidatorSet(validators, 0)

	cfg := looseberry.DefaultConfig()
	cfg.Signer = signers[0]
	cfg.ValidatorIndex = 0
	cfg.Storage.InMemory = true
	cfg.Worker.MaxPendingTxs = 100000
	cfg.Worker.MaxPendingBytes = 500 * 1024 * 1024

	lb, err := looseberry.New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	numGoroutines := runtime.NumCPU()
	const txPerGoroutine = 10000

	var wg sync.WaitGroup
	var totalSubmitted atomic.Int64

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < txPerGoroutine; i++ {
				tx := fmt.Appendf(nil, "stress-tx-%d-%d", gid, i)
				if err := lb.AddTx(tx); err == nil {
					totalSubmitted.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	submitted := totalSubmitted.Load()
	txPerSec := float64(submitted) / elapsed.Seconds()

	metrics := lb.Metrics()
	t.Logf("Submitted %d/%d transactions from %d goroutines in %v (%.0f tx/sec)",
		submitted, numGoroutines*txPerGoroutine, numGoroutines, elapsed, txPerSec)
	t.Logf("Metrics: TotalTxAdded=%d, TotalBatches=%d, PendingTx=%d, Workers=%d",
		metrics.TotalTxAdded, metrics.TotalBatches, metrics.PendingTxCount, metrics.WorkerCount)
}

func TestStressLooseberryMemoryStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)

	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	vs := types.NewSimpleValidatorSet(validators, 0)

	cfg := looseberry.DefaultConfig()
	cfg.Signer = signers[0]
	cfg.ValidatorIndex = 0
	cfg.Storage.InMemory = true

	lb, err := looseberry.New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Force GC and record initial memory
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Run for a period with continuous load
	const duration = 2 * time.Second
	deadline := time.Now().Add(duration)
	var totalTx int64

	for time.Now().Before(deadline) {
		for i := 0; i < 1000; i++ {
			tx := fmt.Appendf(nil, "mem-stress-tx-%d", totalTx)
			_ = lb.AddTx(tx)
			totalTx++
		}
		// Periodic flush to prevent memory buildup
		lb.Flush()
	}

	// Force GC and record final memory
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	heapGrowthMB := float64(m2.HeapAlloc-m1.HeapAlloc) / (1024 * 1024)
	t.Logf("Submitted %d transactions over %v", totalTx, duration)
	t.Logf("Heap growth: %.2f MB (before: %.2f MB, after: %.2f MB)",
		heapGrowthMB, float64(m1.HeapAlloc)/(1024*1024), float64(m2.HeapAlloc)/(1024*1024))

	// Memory growth should be bounded
	// Allow up to 100MB of heap growth
	if heapGrowthMB > 100 {
		t.Errorf("Excessive heap growth: %.2f MB", heapGrowthMB)
	}
}

// ============================================================================
// Multi-Node Stress Tests
// ============================================================================

func TestStressMultiNodeTransactionLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tn := NewTestNetwork(t, 4)
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = tn.Stop() }()

	const txPerNode = 5000
	var wg sync.WaitGroup

	start := time.Now()

	// Each node submits transactions concurrently
	for nodeIdx := 0; nodeIdx < tn.NodeCount(); nodeIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < txPerNode; i++ {
				tx := []byte(fmt.Sprintf("node%d-tx-%d", idx, i))
				_ = tn.SubmitTxToNode(idx, tx)
			}
		}(nodeIdx)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalTx := txPerNode * tn.NodeCount()
	txPerSec := float64(totalTx) / elapsed.Seconds()

	t.Logf("Submitted %d total transactions from %d nodes in %v (%.0f tx/sec)",
		totalTx, tn.NodeCount(), elapsed, txPerSec)

	// Log metrics from each node
	metrics := tn.GetMetrics()
	for i, m := range metrics {
		t.Logf("Node %d: TotalTxAdded=%d, PendingTx=%d, TotalBatches=%d",
			i, m.TotalTxAdded, m.PendingTxCount, m.TotalBatches)
	}
}
