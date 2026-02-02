package looseberry_test

import (
	"fmt"
	"testing"

	"github.com/blockberries/looseberry"
	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
	"github.com/blockberries/looseberry/worker"
)

// ============================================================================
// Worker Benchmarks
// ============================================================================

func BenchmarkWorkerAddTx(b *testing.B) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := worker.DefaultConfig()
	w := worker.New(0, 0, cfg, batchStore, txIndex, 3)

	if err := w.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Pre-generate transactions
	txs := make([]types.Transaction, b.N)
	for i := range b.N {
		txs[i] = types.Transaction(fmt.Appendf(nil, "benchmark-tx-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.AddTx(txs[i])
	}
}

func BenchmarkWorkerPoolAddTx(b *testing.B) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := worker.PoolConfig{
		MinWorkers: 4,
		MaxWorkers: 4,
		Worker:     worker.DefaultConfig(),
	}
	pool := worker.NewPool(cfg, 0, batchStore, txIndex, 3)

	if err := pool.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	// Pre-generate transactions
	txs := make([]types.Transaction, b.N)
	for i := range b.N {
		txs[i] = types.Transaction(fmt.Appendf(nil, "benchmark-tx-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pool.AddTx(txs[i])
	}
}

func BenchmarkWorkerPoolAddTxParallel(b *testing.B) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := worker.PoolConfig{
		MinWorkers: 4,
		MaxWorkers: 4,
		Worker:     worker.DefaultConfig(),
	}
	pool := worker.NewPool(cfg, 0, batchStore, txIndex, 3)

	if err := pool.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = pool.Stop() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := types.Transaction(fmt.Appendf(nil, "benchmark-tx-%d", i))
			_ = pool.AddTx(tx)
			i++
		}
	})
}

// ============================================================================
// DAG Benchmarks
// ============================================================================

func BenchmarkDAGAddCertificate(b *testing.B) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Pre-generate certificates
	certs := make([]*types.Certificate, b.N)
	for i := range b.N {
		signer, _ := types.GenerateEd25519Signer(0)
		header := types.NewHeader(0, uint64(i), 0, nil, nil)
		_ = header.Sign(signer)
		vote := types.NewVote(header.Digest, 0)
		_ = vote.Sign(signer)
		certs[i] = types.NewCertificate(header, []types.Vote{*vote})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.AddCertificate(certs[i])
	}
}

func BenchmarkDAGGetCertificate(b *testing.B) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add some certificates
	signer, _ := types.GenerateEd25519Signer(0)
	var digests []types.Hash
	for i := range 1000 {
		header := types.NewHeader(0, uint64(i), 0, nil, nil)
		_ = header.Sign(signer)
		vote := types.NewVote(header.Digest, 0)
		_ = vote.Sign(signer)
		cert := types.NewCertificate(header, []types.Vote{*vote})
		_ = d.AddCertificate(cert)
		digests = append(digests, cert.Digest())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.GetCertificate(digests[i%len(digests)])
	}
}

func BenchmarkDAGGetOrderedCertificates(b *testing.B) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())

	// Add certificates for multiple rounds
	for round := uint64(0); round < 100; round++ {
		for val := uint16(0); val < 4; val++ {
			signer, _ := types.GenerateEd25519Signer(val)
			header := types.NewHeader(val, round, 0, nil, nil)
			_ = header.Sign(signer)
			vote := types.NewVote(header.Digest, val)
			_ = vote.Sign(signer)
			cert := types.NewCertificate(header, []types.Vote{*vote})
			_ = d.AddCertificate(cert)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.GetOrderedCertificates(0, 99)
	}
}

// ============================================================================
// Store Benchmarks
// ============================================================================

func BenchmarkBatchStoreSave(b *testing.B) {
	batchStore := store.NewMemoryBatchStore()
	defer batchStore.Close()

	// Pre-generate batches
	batches := make([]*types.Batch, b.N)
	for i := range b.N {
		batches[i] = types.NewBatch(0, 0, uint64(i), []types.Transaction{
			types.Transaction(fmt.Appendf(nil, "tx-%d", i)),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batchStore.SaveBatch(batches[i])
	}
}

func BenchmarkBatchStoreGet(b *testing.B) {
	batchStore := store.NewMemoryBatchStore()
	defer batchStore.Close()

	// Save some batches
	var digests []types.Hash
	for i := range 1000 {
		batch := types.NewBatch(0, 0, uint64(i), []types.Transaction{
			types.Transaction(fmt.Appendf(nil, "tx-%d", i)),
		})
		_ = batchStore.SaveBatch(batch)
		digests = append(digests, batch.Digest)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = batchStore.GetBatch(digests[i%len(digests)])
	}
}

func BenchmarkTxIndexHas(b *testing.B) {
	txIndex := store.NewMemoryTxIndex()
	defer txIndex.Close()

	// Add some transactions
	var hashes []types.Hash
	for i := range 10000 {
		tx := types.Transaction(fmt.Appendf(nil, "tx-%d", i))
		h := tx.Hash()
		_ = txIndex.AddTx(h, h) // Use hash as batch hash for simplicity
		hashes = append(hashes, h)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = txIndex.HasTx(hashes[i%len(hashes)])
	}
}

// ============================================================================
// Types Benchmarks
// ============================================================================

func BenchmarkTransactionHash(b *testing.B) {
	tx := types.Transaction(make([]byte, 256))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tx.Hash()
	}
}

func BenchmarkBatchComputeDigest(b *testing.B) {
	txs := make([]types.Transaction, 100)
	for i := range 100 {
		txs[i] = types.Transaction(fmt.Appendf(nil, "tx-data-%d", i))
	}

	batch := &types.Batch{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        1,
		Transactions: txs,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batch.ComputeDigest()
	}
}

func BenchmarkHeaderSign(b *testing.B) {
	signer, _ := types.GenerateEd25519Signer(0)
	header := types.NewHeader(0, 1, 0, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		header.Signature = types.Signature{}
		_ = header.Sign(signer)
	}
}

func BenchmarkHeaderVerify(b *testing.B) {
	signer, _ := types.GenerateEd25519Signer(0)
	pk := signer.PublicKey()

	header := types.NewHeader(0, 1, 0, nil, nil)
	_ = header.Sign(signer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = header.Verify(pk)
	}
}

func BenchmarkCertificateVerify(b *testing.B) {
	signer, _ := types.GenerateEd25519Signer(0)

	validators := []*types.Validator{{Index: 0, PublicKey: signer.PublicKey()}}
	vs := types.NewSimpleValidatorSet(validators, 0)

	header := types.NewHeader(0, 1, 0, nil, nil)
	_ = header.Sign(signer)

	vote := types.NewVote(header.Digest, 0)
	_ = vote.Sign(signer)

	cert := types.NewCertificate(header, []types.Vote{*vote})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cert.Verify(vs)
	}
}

// ============================================================================
// Looseberry Benchmarks
// ============================================================================

func BenchmarkLooseberryAddTx(b *testing.B) {
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

	lb, _ := looseberry.New(cfg)
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	_ = lb.Start()
	defer func() { _ = lb.Stop() }()

	// Pre-generate transactions
	txs := make([][]byte, b.N)
	for i := range b.N {
		txs[i] = fmt.Appendf(nil, "benchmark-tx-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lb.AddTx(txs[i])
	}
}

func BenchmarkLooseberryAddTxParallel(b *testing.B) {
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

	lb, _ := looseberry.New(cfg)
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	_ = lb.Start()
	defer func() { _ = lb.Stop() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := fmt.Appendf(nil, "benchmark-tx-%d", i)
			_ = lb.AddTx(tx)
			i++
		}
	})
}

func BenchmarkLooseberryMetrics(b *testing.B) {
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

	lb, _ := looseberry.New(cfg)
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	_ = lb.Start()
	defer func() { _ = lb.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lb.Metrics()
	}
}
