package types

import (
	"testing"
)

func TestNewBatch(t *testing.T) {
	txs := []Transaction{
		Transaction([]byte("tx1")),
		Transaction([]byte("tx2")),
	}

	batch := NewBatch(1, 2, 10, txs)

	if batch.WorkerID != 1 {
		t.Errorf("WorkerID mismatch: expected 1, got %d", batch.WorkerID)
	}
	if batch.ValidatorID != 2 {
		t.Errorf("ValidatorID mismatch: expected 2, got %d", batch.ValidatorID)
	}
	if batch.Round != 10 {
		t.Errorf("Round mismatch: expected 10, got %d", batch.Round)
	}
	if len(batch.Transactions) != 2 {
		t.Errorf("Transaction count mismatch: expected 2, got %d", len(batch.Transactions))
	}
	if batch.Digest.IsEmpty() {
		t.Error("Digest should not be empty")
	}
	if batch.Timestamp == 0 {
		t.Error("Timestamp should be set")
	}
}

func TestBatchComputeDigest(t *testing.T) {
	txs := []Transaction{
		Transaction([]byte("tx1")),
		Transaction([]byte("tx2")),
	}

	batch := NewBatch(1, 2, 10, txs)

	// Digest should be deterministic
	digest1 := batch.ComputeDigest()
	digest2 := batch.ComputeDigest()
	if !digest1.Equal(digest2) {
		t.Error("ComputeDigest should be deterministic")
	}

	// Digest should match stored digest
	if !batch.Digest.Equal(digest1) {
		t.Error("Stored digest should match computed digest")
	}

	// Different content should produce different digest
	batch2 := NewBatch(1, 2, 10, []Transaction{Transaction([]byte("different"))})
	if batch.Digest.Equal(batch2.Digest) {
		t.Error("Different batches should have different digests")
	}
}

func TestBatchSize(t *testing.T) {
	txs := []Transaction{
		Transaction([]byte("hello")), // 5 bytes
		Transaction([]byte("world")), // 5 bytes
	}

	batch := NewBatch(0, 0, 0, txs)
	if batch.Size() != 10 {
		t.Errorf("Size mismatch: expected 10, got %d", batch.Size())
	}

	emptyBatch := NewBatch(0, 0, 0, nil)
	if emptyBatch.Size() != 0 {
		t.Errorf("Empty batch size should be 0, got %d", emptyBatch.Size())
	}
}

func TestBatchTxCount(t *testing.T) {
	txs := []Transaction{
		Transaction([]byte("tx1")),
		Transaction([]byte("tx2")),
		Transaction([]byte("tx3")),
	}

	batch := NewBatch(0, 0, 0, txs)
	if batch.TxCount() != 3 {
		t.Errorf("TxCount mismatch: expected 3, got %d", batch.TxCount())
	}
}

func TestBatchIsEmpty(t *testing.T) {
	emptyBatch := NewBatch(0, 0, 0, nil)
	if !emptyBatch.IsEmpty() {
		t.Error("Batch with no transactions should be empty")
	}

	emptyBatch2 := NewBatch(0, 0, 0, []Transaction{})
	if !emptyBatch2.IsEmpty() {
		t.Error("Batch with empty transaction slice should be empty")
	}

	nonEmptyBatch := NewBatch(0, 0, 0, []Transaction{Transaction([]byte("tx"))})
	if nonEmptyBatch.IsEmpty() {
		t.Error("Batch with transactions should not be empty")
	}
}

func TestBatchGetDigest(t *testing.T) {
	batch := NewBatch(1, 2, 10, []Transaction{Transaction([]byte("tx"))})
	digest := batch.GetDigest()

	if !digest.Digest.Equal(batch.Digest) {
		t.Error("GetDigest should return matching digest")
	}
	if digest.WorkerID != batch.WorkerID {
		t.Error("GetDigest should return matching WorkerID")
	}
	if digest.ValidatorID != batch.ValidatorID {
		t.Error("GetDigest should return matching ValidatorID")
	}
}

func TestBatchVerify(t *testing.T) {
	batch := NewBatch(1, 2, 10, []Transaction{Transaction([]byte("tx"))})

	if !batch.Verify() {
		t.Error("Valid batch should verify")
	}

	// Tamper with digest
	batch.Digest[0] ^= 0xFF
	if batch.Verify() {
		t.Error("Tampered batch should not verify")
	}
}

func TestBatchClone(t *testing.T) {
	original := NewBatch(1, 2, 10, []Transaction{Transaction([]byte("tx"))})
	clone := original.Clone()

	if !original.Digest.Equal(clone.Digest) {
		t.Error("Clone should have same digest")
	}
	if original.WorkerID != clone.WorkerID {
		t.Error("Clone should have same WorkerID")
	}
	if original.ValidatorID != clone.ValidatorID {
		t.Error("Clone should have same ValidatorID")
	}
	if original.Round != clone.Round {
		t.Error("Clone should have same Round")
	}
	if original.Timestamp != clone.Timestamp {
		t.Error("Clone should have same Timestamp")
	}
	if len(original.Transactions) != len(clone.Transactions) {
		t.Error("Clone should have same number of transactions")
	}

	// Modify clone should not affect original
	clone.Transactions[0][0] = 'X'
	if original.Transactions[0][0] == 'X' {
		t.Error("Modifying clone should not affect original")
	}
}

func TestBatchDigestStability(t *testing.T) {
	// Test that changing fields changes the digest
	base := NewBatch(1, 2, 10, []Transaction{Transaction([]byte("tx"))})
	baseDigest := base.Digest

	// Different WorkerID
	b1 := &Batch{WorkerID: 99, ValidatorID: 2, Round: 10, Timestamp: base.Timestamp, Transactions: base.Transactions}
	if baseDigest.Equal(b1.ComputeDigest()) {
		t.Error("Different WorkerID should produce different digest")
	}

	// Different ValidatorID
	b2 := &Batch{WorkerID: 1, ValidatorID: 99, Round: 10, Timestamp: base.Timestamp, Transactions: base.Transactions}
	if baseDigest.Equal(b2.ComputeDigest()) {
		t.Error("Different ValidatorID should produce different digest")
	}

	// Different Round
	b3 := &Batch{WorkerID: 1, ValidatorID: 2, Round: 99, Timestamp: base.Timestamp, Transactions: base.Transactions}
	if baseDigest.Equal(b3.ComputeDigest()) {
		t.Error("Different Round should produce different digest")
	}

	// Different Timestamp
	b4 := &Batch{WorkerID: 1, ValidatorID: 2, Round: 10, Timestamp: 999999, Transactions: base.Transactions}
	if baseDigest.Equal(b4.ComputeDigest()) {
		t.Error("Different Timestamp should produce different digest")
	}
}
