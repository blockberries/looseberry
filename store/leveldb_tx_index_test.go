package store

import (
	"path/filepath"
	"testing"

	"github.com/blockberries/looseberry/types"
)

func TestLevelDBTxIndexAddAndGet(t *testing.T) {
	idx := newTestLevelDBTxIndex(t)
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))

	if err := idx.AddTx(txHash, batchHash); err != nil {
		t.Fatalf("AddTx: %v", err)
	}

	got, err := idx.GetBatchForTx(txHash)
	if err != nil {
		t.Fatalf("GetBatchForTx: %v", err)
	}
	if !got.Equal(batchHash) {
		t.Errorf("got %s, want %s", got, batchHash)
	}
	if !idx.HasTx(txHash) {
		t.Error("HasTx should return true after AddTx")
	}
}

func TestLevelDBTxIndexIdempotent(t *testing.T) {
	idx := newTestLevelDBTxIndex(t)
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash := types.HashBytes([]byte("batch1"))

	if err := idx.AddTx(txHash, batchHash); err != nil {
		t.Fatalf("first AddTx: %v", err)
	}
	if err := idx.AddTx(txHash, batchHash); err != nil {
		t.Fatalf("second AddTx (idempotent) failed: %v", err)
	}
	if idx.Len() != 1 {
		t.Errorf("Len: got %d, want 1", idx.Len())
	}
}

func TestLevelDBTxIndexDifferentBatchErrors(t *testing.T) {
	idx := newTestLevelDBTxIndex(t)
	defer idx.Close()

	txHash := types.HashBytes([]byte("tx1"))
	batchHash1 := types.HashBytes([]byte("batch1"))
	batchHash2 := types.HashBytes([]byte("batch2"))

	if err := idx.AddTx(txHash, batchHash1); err != nil {
		t.Fatalf("AddTx: %v", err)
	}
	err := idx.AddTx(txHash, batchHash2)
	if err != types.ErrTxAlreadyExists {
		t.Errorf("expected ErrTxAlreadyExists, got %v", err)
	}
}

func TestLevelDBTxIndexAddBatchAndRemove(t *testing.T) {
	idx := newTestLevelDBTxIndex(t)
	defer idx.Close()

	batch := types.NewBatch(0, 0, 5, []types.Transaction{
		types.Transaction([]byte("a")),
		types.Transaction([]byte("b")),
		types.Transaction([]byte("c")),
	})
	if err := idx.AddBatch(batch); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	if idx.Len() != 3 {
		t.Errorf("after AddBatch, Len = %d, want 3", idx.Len())
	}
	for _, tx := range batch.Transactions {
		if !idx.HasTx(tx.Hash()) {
			t.Errorf("missing tx %x", tx.Hash())
		}
	}

	if err := idx.RemoveTxsForBatch(batch.Digest); err != nil {
		t.Fatalf("RemoveTxsForBatch: %v", err)
	}
	if idx.Len() != 0 {
		t.Errorf("after Remove, Len = %d, want 0", idx.Len())
	}
	for _, tx := range batch.Transactions {
		if idx.HasTx(tx.Hash()) {
			t.Errorf("HasTx still true for removed %x", tx.Hash())
		}
	}
}

// TestLevelDBTxIndexSurvivesRestart is the load-bearing test for B3-7:
// dedup state must survive a process restart.
func TestLevelDBTxIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "txindex")

	batch := types.NewBatch(2, 3, 7, []types.Transaction{
		types.Transaction([]byte("persistent-tx")),
	})

	{
		idx, err := NewLevelDBTxIndex(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := idx.AddBatch(batch); err != nil {
			t.Fatalf("AddBatch: %v", err)
		}
		if err := idx.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	idx, err := NewLevelDBTxIndex(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer idx.Close()

	for _, tx := range batch.Transactions {
		if !idx.HasTx(tx.Hash()) {
			t.Fatalf("tx %x missing after restart", tx.Hash())
		}
		got, gerr := idx.GetBatchForTx(tx.Hash())
		if gerr != nil {
			t.Fatalf("GetBatchForTx after restart: %v", gerr)
		}
		if !got.Equal(batch.Digest) {
			t.Errorf("wrong batch mapping after restart: got %s, want %s", got, batch.Digest)
		}
	}
}

func TestLevelDBTxIndexPruneOlderThan(t *testing.T) {
	idx := newTestLevelDBTxIndex(t)
	defer idx.Close()

	old := types.NewBatch(0, 0, 1, []types.Transaction{types.Transaction([]byte("old"))})
	mid := types.NewBatch(0, 0, 2, []types.Transaction{types.Transaction([]byte("mid"))})
	new1 := types.NewBatch(0, 0, 5, []types.Transaction{types.Transaction([]byte("new"))})

	for _, b := range []*types.Batch{old, mid, new1} {
		if err := idx.AddBatch(b); err != nil {
			t.Fatalf("AddBatch: %v", err)
		}
	}

	n, err := idx.PruneOlderThan(3, nil)
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}
	if n != 2 {
		t.Errorf("pruned count: got %d, want 2", n)
	}

	if idx.HasTx(old.Transactions[0].Hash()) {
		t.Error("old tx still indexed after prune")
	}
	if idx.HasTx(mid.Transactions[0].Hash()) {
		t.Error("mid tx still indexed after prune")
	}
	if !idx.HasTx(new1.Transactions[0].Hash()) {
		t.Error("new tx pruned despite being above threshold")
	}
}

func newTestLevelDBTxIndex(t *testing.T) *LevelDBTxIndex {
	t.Helper()
	idx, err := NewLevelDBTxIndex(filepath.Join(t.TempDir(), "txindex"))
	if err != nil {
		t.Fatalf("NewLevelDBTxIndex: %v", err)
	}
	return idx
}
