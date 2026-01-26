package types

import (
	"encoding/binary"
	"time"
)

// BatchDigest is a reference to a batch by its digest.
type BatchDigest struct {
	Digest      Hash
	WorkerID    uint16
	ValidatorID uint16
}

// Batch represents a collection of transactions created by a worker.
type Batch struct {
	// WorkerID identifies which worker created this batch.
	WorkerID uint16

	// ValidatorID identifies which validator's worker created this batch.
	ValidatorID uint16

	// Round is the DAG round when this batch was created.
	Round uint64

	// Transactions are the raw transactions in this batch.
	Transactions []Transaction

	// Digest is the computed hash of this batch.
	Digest Hash

	// Timestamp is when this batch was created (Unix nanoseconds).
	Timestamp int64
}

// NewBatch creates a new batch with the given parameters.
// The digest is computed automatically.
func NewBatch(workerID, validatorID uint16, round uint64, txs []Transaction) *Batch {
	b := &Batch{
		WorkerID:     workerID,
		ValidatorID:  validatorID,
		Round:        round,
		Transactions: txs,
		Timestamp:    time.Now().UnixNano(),
	}
	b.Digest = b.ComputeDigest()
	return b
}

// ComputeDigest computes and returns the batch digest.
// The digest is: SHA256(workerID || validatorID || round || timestamp || tx1_hash || tx2_hash || ...)
func (b *Batch) ComputeDigest() Hash {
	// Estimate size: 2 + 2 + 8 + 8 + (len(txs) * 32)
	size := 20 + len(b.Transactions)*HashSize
	data := make([]byte, 0, size)

	// Append fixed fields
	buf := make([]byte, 8)
	binary.BigEndian.PutUint16(buf[:2], b.WorkerID)
	data = append(data, buf[:2]...)

	binary.BigEndian.PutUint16(buf[:2], b.ValidatorID)
	data = append(data, buf[:2]...)

	binary.BigEndian.PutUint64(buf, b.Round)
	data = append(data, buf...)

	binary.BigEndian.PutUint64(buf, uint64(b.Timestamp))
	data = append(data, buf...)

	// Append transaction hashes
	for _, tx := range b.Transactions {
		txHash := tx.Hash()
		data = append(data, txHash[:]...)
	}

	return HashBytes(data)
}

// Size returns the total size of all transactions in bytes.
func (b *Batch) Size() int64 {
	var total int64
	for _, tx := range b.Transactions {
		total += int64(tx.Size())
	}
	return total
}

// TxCount returns the number of transactions in this batch.
func (b *Batch) TxCount() int {
	return len(b.Transactions)
}

// IsEmpty returns true if the batch has no transactions.
func (b *Batch) IsEmpty() bool {
	return len(b.Transactions) == 0
}

// GetDigest returns a BatchDigest reference to this batch.
func (b *Batch) GetDigest() BatchDigest {
	return BatchDigest{
		Digest:      b.Digest,
		WorkerID:    b.WorkerID,
		ValidatorID: b.ValidatorID,
	}
}

// Verify checks if the batch digest is valid.
func (b *Batch) Verify() bool {
	return b.Digest.Equal(b.ComputeDigest())
}

// Clone returns a deep copy of the batch.
func (b *Batch) Clone() *Batch {
	clone := &Batch{
		WorkerID:     b.WorkerID,
		ValidatorID:  b.ValidatorID,
		Round:        b.Round,
		Transactions: make([]Transaction, len(b.Transactions)),
		Digest:       b.Digest,
		Timestamp:    b.Timestamp,
	}
	for i, tx := range b.Transactions {
		clone.Transactions[i] = tx.Clone()
	}
	return clone
}
