package store

import (
	"testing"

	"github.com/blockberries/looseberry/types"
)

func TestMemoryTxIndexAddAndGet(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))

	// Add
	err := idx.AddTx(txHash, batchHash)
	if err != nil {
		t.Fatalf("AddTx failed: %v", err)
	}

	// Get
	retrieved, err := idx.GetBatchForTx(txHash)
	if err != nil {
		t.Fatalf("GetBatchForTx failed: %v", err)
	}

	if !retrieved.Equal(batchHash) {
		t.Error("Retrieved batch hash mismatch")
	}
}

func TestMemoryTxIndexHasTx(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))

	if idx.HasTx(txHash) {
		t.Error("Should not have tx before add")
	}

	_ = idx.AddTx(txHash, batchHash)

	if !idx.HasTx(txHash) {
		t.Error("Should have tx after add")
	}
}

func TestMemoryTxIndexIdempotent(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))

	// Add twice with same batch
	err := idx.AddTx(txHash, batchHash)
	if err != nil {
		t.Fatalf("First AddTx failed: %v", err)
	}

	err = idx.AddTx(txHash, batchHash)
	if err != nil {
		t.Fatalf("Second AddTx should be idempotent: %v", err)
	}

	if idx.Len() != 1 {
		t.Errorf("Expected 1 tx, got %d", idx.Len())
	}
}

func TestMemoryTxIndexDifferentBatch(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash1 := types.HashBytes([]byte("batch1"))
	batchHash2 := types.HashBytes([]byte("batch2"))

	err := idx.AddTx(txHash, batchHash1)
	if err != nil {
		t.Fatalf("First AddTx failed: %v", err)
	}

	// Adding same tx to different batch should fail
	err = idx.AddTx(txHash, batchHash2)
	if err != types.ErrTxAlreadyExists {
		t.Errorf("Expected ErrTxAlreadyExists, got: %v", err)
	}
}

func TestMemoryTxIndexRemoveTxsForBatch(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	batchHash := types.HashBytes([]byte("batch1"))
	tx1 := types.HashBytes([]byte("tx1"))
	tx2 := types.HashBytes([]byte("tx2"))
	tx3 := types.HashBytes([]byte("tx3"))

	// Add txs to batch
	_ = idx.AddTx(tx1, batchHash)
	_ = idx.AddTx(tx2, batchHash)
	_ = idx.AddTx(tx3, types.HashBytes([]byte("other_batch")))

	if idx.Len() != 3 {
		t.Fatalf("Expected 3 txs, got %d", idx.Len())
	}

	// Remove txs for batch
	err := idx.RemoveTxsForBatch(batchHash)
	if err != nil {
		t.Fatalf("RemoveTxsForBatch failed: %v", err)
	}

	// tx1 and tx2 should be removed
	if idx.HasTx(tx1) {
		t.Error("tx1 should be removed")
	}
	if idx.HasTx(tx2) {
		t.Error("tx2 should be removed")
	}

	// tx3 should remain
	if !idx.HasTx(tx3) {
		t.Error("tx3 should remain")
	}

	if idx.Len() != 1 {
		t.Errorf("Expected 1 tx after remove, got %d", idx.Len())
	}
}

func TestMemoryTxIndexRemoveNonExistent(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	// Remove from non-existent batch should not error
	err := idx.RemoveTxsForBatch(types.HashBytes([]byte("nonexistent")))
	if err != nil {
		t.Errorf("RemoveTxsForBatch for non-existent should not error: %v", err)
	}
}

func TestMemoryTxIndexAddBatch(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	batch := types.NewBatch(0, 0, 10, []types.Transaction{
		types.Transaction([]byte("tx1")),
		types.Transaction([]byte("tx2")),
		types.Transaction([]byte("tx3")),
	})

	err := idx.AddBatch(batch)
	if err != nil {
		t.Fatalf("AddBatch failed: %v", err)
	}

	if idx.Len() != 3 {
		t.Errorf("Expected 3 indexed txs, got %d", idx.Len())
	}

	// Verify each tx is indexed
	for _, tx := range batch.Transactions {
		if !idx.HasTx(tx.Hash()) {
			t.Errorf("Transaction should be indexed")
		}

		batchHash, err := idx.GetBatchForTx(tx.Hash())
		if err != nil {
			t.Fatalf("GetBatchForTx failed: %v", err)
		}

		if !batchHash.Equal(batch.Digest) {
			t.Error("Batch hash mismatch")
		}
	}
}

func TestMemoryTxIndexClose(t *testing.T) {
	idx := NewMemoryTxIndex()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))
	_ = idx.AddTx(txHash, batchHash)

	err := idx.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should fail
	err = idx.AddTx(txHash, batchHash)
	if err != types.ErrNotRunning {
		t.Errorf("AddTx after close should return ErrNotRunning, got: %v", err)
	}

	_, err = idx.GetBatchForTx(txHash)
	if err != types.ErrNotRunning {
		t.Errorf("GetBatchForTx after close should return ErrNotRunning, got: %v", err)
	}
}

// Test for Bug Fix #4: TxIndex Garbage Collection
func TestMemoryTxIndexPruneOlderThan(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	// Add batches at different rounds
	batch0 := types.NewBatch(0, 0, 0, []types.Transaction{
		types.Transaction([]byte("tx_round0_1")),
		types.Transaction([]byte("tx_round0_2")),
	})
	batch5 := types.NewBatch(0, 0, 5, []types.Transaction{
		types.Transaction([]byte("tx_round5_1")),
		types.Transaction([]byte("tx_round5_2")),
	})
	batch10 := types.NewBatch(0, 0, 10, []types.Transaction{
		types.Transaction([]byte("tx_round10_1")),
		types.Transaction([]byte("tx_round10_2")),
	})

	_ = idx.AddBatch(batch0)
	_ = idx.AddBatch(batch5)
	_ = idx.AddBatch(batch10)

	if idx.Len() != 6 {
		t.Fatalf("Expected 6 indexed txs, got %d", idx.Len())
	}

	// Prune batches older than round 6 (should remove batch0 and batch5)
	pruned, err := idx.PruneOlderThan(6, nil)
	if err != nil {
		t.Fatalf("PruneOlderThan failed: %v", err)
	}

	if pruned != 2 {
		t.Errorf("Expected 2 batches pruned, got %d", pruned)
	}

	// batch0 and batch5 transactions should be gone
	for _, tx := range batch0.Transactions {
		if idx.HasTx(tx.Hash()) {
			t.Error("Round 0 transactions should be pruned")
		}
	}
	for _, tx := range batch5.Transactions {
		if idx.HasTx(tx.Hash()) {
			t.Error("Round 5 transactions should be pruned")
		}
	}

	// batch10 transactions should remain
	for _, tx := range batch10.Transactions {
		if !idx.HasTx(tx.Hash()) {
			t.Error("Round 10 transactions should remain")
		}
	}

	if idx.Len() != 2 {
		t.Errorf("Expected 2 indexed txs after prune, got %d", idx.Len())
	}
}

func TestMemoryTxIndexPruneOlderThanEmpty(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	// Prune empty index should not error
	pruned, err := idx.PruneOlderThan(10, nil)
	if err != nil {
		t.Fatalf("PruneOlderThan on empty index failed: %v", err)
	}

	if pruned != 0 {
		t.Errorf("Expected 0 batches pruned from empty index, got %d", pruned)
	}
}

func TestMemoryTxIndexPruneOlderThanPreservesNewer(t *testing.T) {
	idx := NewMemoryTxIndex()
	defer idx.Close()

	// Add batch at round 100
	batch := types.NewBatch(0, 0, 100, []types.Transaction{
		types.Transaction([]byte("tx1")),
	})
	_ = idx.AddBatch(batch)

	// Prune rounds older than 50 (should not affect round 100)
	pruned, err := idx.PruneOlderThan(50, nil)
	if err != nil {
		t.Fatalf("PruneOlderThan failed: %v", err)
	}

	if pruned != 0 {
		t.Errorf("Expected 0 batches pruned, got %d", pruned)
	}

	if idx.Len() != 1 {
		t.Errorf("Expected 1 indexed tx, got %d", idx.Len())
	}
}
