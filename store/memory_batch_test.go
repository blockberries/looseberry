package store

import (
	"testing"

	"github.com/blockberries/looseberry/types"
)

func TestMemoryBatchStoreSaveAndGet(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	batch := types.NewBatch(0, 0, 10, []types.Transaction{
		types.Transaction([]byte("tx1")),
	})

	// Save
	err := store.SaveBatch(batch)
	if err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}

	// Get
	retrieved, err := store.GetBatch(batch.Digest)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}

	if !retrieved.Digest.Equal(batch.Digest) {
		t.Error("Retrieved batch digest mismatch")
	}

	// Modify retrieved should not affect stored
	retrieved.WorkerID = 99
	original, _ := store.GetBatch(batch.Digest)
	if original.WorkerID == 99 {
		t.Error("Modifying retrieved batch should not affect stored")
	}
}

func TestMemoryBatchStoreHasBatch(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	batch := types.NewBatch(0, 0, 10, nil)

	if store.HasBatch(batch.Digest) {
		t.Error("Should not have batch before save")
	}

	_ = store.SaveBatch(batch)

	if !store.HasBatch(batch.Digest) {
		t.Error("Should have batch after save")
	}
}

func TestMemoryBatchStoreGetBatchesByRound(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	// Create batches for multiple rounds
	batch1 := types.NewBatch(0, 0, 10, []types.Transaction{types.Transaction([]byte("1"))})
	batch2 := types.NewBatch(1, 0, 10, []types.Transaction{types.Transaction([]byte("2"))})
	batch3 := types.NewBatch(0, 0, 11, []types.Transaction{types.Transaction([]byte("3"))})

	_ = store.SaveBatch(batch1)
	_ = store.SaveBatch(batch2)
	_ = store.SaveBatch(batch3)

	// Get round 10
	batches, err := store.GetBatchesByRound(10)
	if err != nil {
		t.Fatalf("GetBatchesByRound failed: %v", err)
	}

	if len(batches) != 2 {
		t.Errorf("Expected 2 batches for round 10, got %d", len(batches))
	}

	// Get round 11
	batches, err = store.GetBatchesByRound(11)
	if err != nil {
		t.Fatalf("GetBatchesByRound failed: %v", err)
	}

	if len(batches) != 1 {
		t.Errorf("Expected 1 batch for round 11, got %d", len(batches))
	}

	// Get non-existent round
	batches, err = store.GetBatchesByRound(99)
	if err != nil {
		t.Fatalf("GetBatchesByRound failed: %v", err)
	}

	if len(batches) != 0 {
		t.Error("Expected empty slice for non-existent round")
	}
}

func TestMemoryBatchStoreDeleteBatchesBefore(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	// Create batches
	batch1 := types.NewBatch(0, 0, 5, nil)
	batch2 := types.NewBatch(0, 0, 10, nil)
	batch3 := types.NewBatch(0, 0, 15, nil)

	_ = store.SaveBatch(batch1)
	_ = store.SaveBatch(batch2)
	_ = store.SaveBatch(batch3)

	if store.Len() != 3 {
		t.Fatalf("Expected 3 batches, got %d", store.Len())
	}

	// Delete before round 10
	err := store.DeleteBatchesBefore(10)
	if err != nil {
		t.Fatalf("DeleteBatchesBefore failed: %v", err)
	}

	// Batch from round 5 should be deleted
	if store.HasBatch(batch1.Digest) {
		t.Error("Batch from round 5 should be deleted")
	}

	// Batches from round 10 and 15 should remain
	if !store.HasBatch(batch2.Digest) {
		t.Error("Batch from round 10 should remain")
	}
	if !store.HasBatch(batch3.Digest) {
		t.Error("Batch from round 15 should remain")
	}

	if store.Len() != 2 {
		t.Errorf("Expected 2 batches after delete, got %d", store.Len())
	}
}

func TestMemoryBatchStoreIdempotent(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	batch := types.NewBatch(0, 0, 10, nil)

	// Save twice
	_ = store.SaveBatch(batch)
	_ = store.SaveBatch(batch)

	if store.Len() != 1 {
		t.Errorf("SaveBatch should be idempotent, got %d batches", store.Len())
	}
}

func TestMemoryBatchStoreNotFound(t *testing.T) {
	store := NewMemoryBatchStore()
	defer store.Close()

	_, err := store.GetBatch(types.HashBytes([]byte("nonexistent")))
	if err != types.ErrBatchNotFound {
		t.Errorf("Expected ErrBatchNotFound, got: %v", err)
	}
}

func TestMemoryBatchStoreClose(t *testing.T) {
	store := NewMemoryBatchStore()

	batch := types.NewBatch(0, 0, 10, nil)
	_ = store.SaveBatch(batch)

	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should fail
	err = store.SaveBatch(batch)
	if err != types.ErrNotRunning {
		t.Errorf("SaveBatch after close should return ErrNotRunning, got: %v", err)
	}

	_, err = store.GetBatch(batch.Digest)
	if err != types.ErrNotRunning {
		t.Errorf("GetBatch after close should return ErrNotRunning, got: %v", err)
	}
}
