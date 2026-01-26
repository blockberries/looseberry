package store

import (
	"sync"

	"github.com/blockberries/looseberry/types"
)

// MemoryBatchStore is an in-memory implementation of BatchStore.
// Thread-safe with RWMutex. Suitable for testing.
type MemoryBatchStore struct {
	batches      map[types.Hash]*types.Batch
	byRound      map[uint64][]types.Hash // round -> batch digests
	mu           sync.RWMutex
	closed       bool
}

// NewMemoryBatchStore creates a new in-memory batch store.
func NewMemoryBatchStore() *MemoryBatchStore {
	return &MemoryBatchStore{
		batches: make(map[types.Hash]*types.Batch),
		byRound: make(map[uint64][]types.Hash),
	}
}

// SaveBatch stores a batch.
func (s *MemoryBatchStore) SaveBatch(batch *types.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Check if already exists
	if _, exists := s.batches[batch.Digest]; exists {
		return nil // Idempotent
	}

	// Store batch
	s.batches[batch.Digest] = batch.Clone()

	// Index by round
	s.byRound[batch.Round] = append(s.byRound[batch.Round], batch.Digest)

	return nil
}

// GetBatch retrieves a batch by digest.
func (s *MemoryBatchStore) GetBatch(digest types.Hash) (*types.Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	batch, exists := s.batches[digest]
	if !exists {
		return nil, types.ErrBatchNotFound
	}

	return batch.Clone(), nil
}

// HasBatch returns true if the batch exists.
func (s *MemoryBatchStore) HasBatch(digest types.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	_, exists := s.batches[digest]
	return exists
}

// GetBatchesByRound retrieves all batches for a round.
func (s *MemoryBatchStore) GetBatchesByRound(round uint64) ([]*types.Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	digests, exists := s.byRound[round]
	if !exists {
		return nil, nil
	}

	batches := make([]*types.Batch, 0, len(digests))
	for _, digest := range digests {
		if batch, ok := s.batches[digest]; ok {
			batches = append(batches, batch.Clone())
		}
	}

	return batches, nil
}

// DeleteBatchesBefore deletes all batches before the given round.
func (s *MemoryBatchStore) DeleteBatchesBefore(round uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Find rounds to delete
	var roundsToDelete []uint64
	for r := range s.byRound {
		if r < round {
			roundsToDelete = append(roundsToDelete, r)
		}
	}

	// Delete batches and round index
	for _, r := range roundsToDelete {
		for _, digest := range s.byRound[r] {
			delete(s.batches, digest)
		}
		delete(s.byRound, r)
	}

	return nil
}

// Close closes the store.
func (s *MemoryBatchStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.batches = nil
	s.byRound = nil

	return nil
}

// Len returns the number of stored batches (for testing).
func (s *MemoryBatchStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.batches)
}

// Verify interface compliance
var _ BatchStore = (*MemoryBatchStore)(nil)
