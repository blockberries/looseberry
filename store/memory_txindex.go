package store

import (
	"sync"

	"github.com/blockberries/looseberry/types"
)

// MemoryTxIndex is an in-memory implementation of TxIndex.
// Thread-safe with RWMutex. Suitable for testing.
type MemoryTxIndex struct {
	txToBatch    map[types.Hash]types.Hash   // txHash -> batchHash
	batchToTxs   map[types.Hash][]types.Hash // batchHash -> []txHash
	batchRounds  map[types.Hash]uint64       // batchHash -> round (for GC)
	mu           sync.RWMutex
	closed       bool
}

// NewMemoryTxIndex creates a new in-memory transaction index.
func NewMemoryTxIndex() *MemoryTxIndex {
	return &MemoryTxIndex{
		txToBatch:   make(map[types.Hash]types.Hash),
		batchToTxs:  make(map[types.Hash][]types.Hash),
		batchRounds: make(map[types.Hash]uint64),
	}
}

// AddTx adds a transaction hash to batch mapping.
func (idx *MemoryTxIndex) AddTx(txHash, batchHash types.Hash) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	// Check if already indexed (idempotent)
	if existing, exists := idx.txToBatch[txHash]; exists {
		if existing.Equal(batchHash) {
			return nil // Same mapping already exists
		}
		// Transaction exists in different batch - should not happen
		return types.ErrTxAlreadyExists
	}

	// Add mappings
	idx.txToBatch[txHash] = batchHash
	idx.batchToTxs[batchHash] = append(idx.batchToTxs[batchHash], txHash)

	return nil
}

// GetBatchForTx returns the batch hash containing the transaction.
func (idx *MemoryTxIndex) GetBatchForTx(txHash types.Hash) (types.Hash, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return types.EmptyHash, types.ErrNotRunning
	}

	batchHash, exists := idx.txToBatch[txHash]
	if !exists {
		return types.EmptyHash, types.ErrBatchNotFound
	}

	return batchHash, nil
}

// HasTx returns true if the transaction exists.
func (idx *MemoryTxIndex) HasTx(txHash types.Hash) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return false
	}

	_, exists := idx.txToBatch[txHash]
	return exists
}

// RemoveTxsForBatch removes all transaction mappings for a batch.
func (idx *MemoryTxIndex) RemoveTxsForBatch(batchHash types.Hash) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	// Get all transactions in this batch
	txHashes, exists := idx.batchToTxs[batchHash]
	if !exists {
		return nil // Nothing to remove
	}

	// Remove each transaction mapping
	for _, txHash := range txHashes {
		delete(idx.txToBatch, txHash)
	}

	// Remove batch mapping
	delete(idx.batchToTxs, batchHash)

	return nil
}

// Close closes the index.
func (idx *MemoryTxIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.closed = true
	idx.txToBatch = nil
	idx.batchToTxs = nil
	idx.batchRounds = nil

	return nil
}

// Len returns the number of indexed transactions (for testing).
func (idx *MemoryTxIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.txToBatch)
}

// AddBatch indexes all transactions in a batch.
func (idx *MemoryTxIndex) AddBatch(batch *types.Batch) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	batchHash := batch.Digest

	// Store round for GC
	idx.batchRounds[batchHash] = batch.Round

	// Index each transaction
	for _, tx := range batch.Transactions {
		txHash := tx.Hash()

		// Check if already indexed
		if existing, exists := idx.txToBatch[txHash]; exists {
			if !existing.Equal(batchHash) {
				// Transaction exists in different batch
				continue // Skip, don't error
			}
			continue // Already indexed
		}

		idx.txToBatch[txHash] = batchHash
		idx.batchToTxs[batchHash] = append(idx.batchToTxs[batchHash], txHash)
	}

	return nil
}

// PruneOlderThan removes all transaction mappings for batches older than the given round.
// The batchStore parameter is not needed for the in-memory implementation but is
// required by the interface for implementations that don't track rounds internally.
func (idx *MemoryTxIndex) PruneOlderThan(round uint64, _ BatchStore) (int, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return 0, types.ErrNotRunning
	}

	var prunedCount int
	var batchesToPrune []types.Hash

	// Find batches older than the given round
	for batchHash, batchRound := range idx.batchRounds {
		if batchRound < round {
			batchesToPrune = append(batchesToPrune, batchHash)
		}
	}

	// Remove transaction mappings for each batch
	for _, batchHash := range batchesToPrune {
		txHashes, exists := idx.batchToTxs[batchHash]
		if exists {
			for _, txHash := range txHashes {
				delete(idx.txToBatch, txHash)
			}
			delete(idx.batchToTxs, batchHash)
		}
		delete(idx.batchRounds, batchHash)
		prunedCount++
	}

	return prunedCount, nil
}

// Verify interface compliance
var _ TxIndex = (*MemoryTxIndex)(nil)
