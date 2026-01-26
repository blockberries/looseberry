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
