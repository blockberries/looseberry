package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/blockberries/looseberry/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Key prefixes for LevelDB batch store.
var (
	batchPrefix      = []byte("B:")  // B:{digest} -> batch data
	batchRoundPrefix = []byte("BR:") // BR:{round}:{digest} -> empty (index)
	batchHighestKey  = []byte("BM:highest_round")
)

// LevelDBBatchStore is a persistent implementation of BatchStore using LevelDB.
type LevelDBBatchStore struct {
	db     *leveldb.DB
	mu     sync.RWMutex
	closed bool
}

// NewLevelDBBatchStore creates a new LevelDB batch store.
func NewLevelDBBatchStore(path string) (*LevelDBBatchStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	return &LevelDBBatchStore{
		db: db,
	}, nil
}

// SaveBatch stores a batch.
func (s *LevelDBBatchStore) SaveBatch(batch *types.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Check if already exists
	key := makeBatchKey(batch.Digest)
	has, err := s.db.Has(key, nil)
	if err != nil {
		return fmt.Errorf("failed to check batch existence: %w", err)
	}
	if has {
		return nil // Idempotent
	}

	// Encode batch
	data, err := encodeBatch(batch)
	if err != nil {
		return fmt.Errorf("failed to encode batch: %w", err)
	}

	// Create write batch for atomicity
	wb := new(leveldb.Batch)

	// Store batch data
	wb.Put(key, data)

	// Store round index
	indexKey := makeBatchRoundKey(batch.Round, batch.Digest)
	wb.Put(indexKey, nil)

	// Update highest round if needed
	currentHighest := s.getHighestRound()
	if batch.Round > currentHighest {
		roundBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(roundBytes, batch.Round)
		wb.Put(batchHighestKey, roundBytes)
	}

	// Write atomically
	if err := s.db.Write(wb, nil); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	return nil
}

// GetBatch retrieves a batch by digest.
func (s *LevelDBBatchStore) GetBatch(digest types.Hash) (*types.Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	key := makeBatchKey(digest)
	data, err := s.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, types.ErrBatchNotFound
		}
		return nil, fmt.Errorf("failed to get batch: %w", err)
	}

	batch, err := decodeBatch(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode batch: %w", err)
	}

	return batch.Clone(), nil
}

// HasBatch returns true if the batch exists.
func (s *LevelDBBatchStore) HasBatch(digest types.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	key := makeBatchKey(digest)
	has, _ := s.db.Has(key, nil)
	return has
}

// GetBatchesByRound retrieves all batches for a round.
func (s *LevelDBBatchStore) GetBatchesByRound(round uint64) ([]*types.Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	// Create prefix for round
	prefix := makeBatchRoundPrefix(round)

	var batches []*types.Batch
	iter := s.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		// Extract digest from key
		key := iter.Key()
		digest, err := extractDigestFromRoundKey(key, prefix)
		if err != nil {
			continue
		}

		// Get batch data
		batch, err := s.getBatchInternal(digest)
		if err != nil {
			continue
		}

		batches = append(batches, batch.Clone())
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iteration error: %w", err)
	}

	return batches, nil
}

// DeleteBatchesBefore deletes all batches before the given round.
func (s *LevelDBBatchStore) DeleteBatchesBefore(round uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Find all batches to delete
	var toDelete []struct {
		batchKey []byte
		indexKey []byte
	}

	// Iterate through all round indices
	iter := s.db.NewIterator(util.BytesPrefix(batchRoundPrefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		r, digest, err := parseRoundKey(key)
		if err != nil {
			continue
		}

		if r < round {
			toDelete = append(toDelete, struct {
				batchKey []byte
				indexKey []byte
			}{
				batchKey: makeBatchKey(digest),
				indexKey: append([]byte{}, key...), // Copy key
			})
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iteration error: %w", err)
	}

	// Delete in batch
	if len(toDelete) > 0 {
		wb := new(leveldb.Batch)
		for _, item := range toDelete {
			wb.Delete(item.batchKey)
			wb.Delete(item.indexKey)
		}

		if err := s.db.Write(wb, nil); err != nil {
			return fmt.Errorf("failed to delete batches: %w", err)
		}
	}

	return nil
}

// Close closes the store.
func (s *LevelDBBatchStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.db.Close()
}

// Len returns the number of stored batches (for testing).
func (s *LevelDBBatchStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0
	}

	count := 0
	iter := s.db.NewIterator(util.BytesPrefix(batchPrefix), nil)
	defer iter.Release()

	for iter.Next() {
		count++
	}

	return count
}

// HighestRound returns the highest round with stored batches.
func (s *LevelDBBatchStore) HighestRound() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getHighestRound()
}

// Internal helpers

func (s *LevelDBBatchStore) getHighestRound() uint64 {
	data, err := s.db.Get(batchHighestKey, nil)
	if err != nil {
		return 0
	}
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func (s *LevelDBBatchStore) getBatchInternal(digest types.Hash) (*types.Batch, error) {
	key := makeBatchKey(digest)
	data, err := s.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, types.ErrBatchNotFound
		}
		return nil, err
	}

	return decodeBatch(data)
}

// Key construction helpers

func makeBatchKey(digest types.Hash) []byte {
	key := make([]byte, len(batchPrefix)+types.HashSize)
	copy(key, batchPrefix)
	copy(key[len(batchPrefix):], digest[:])
	return key
}

func makeBatchRoundPrefix(round uint64) []byte {
	prefix := make([]byte, len(batchRoundPrefix)+8)
	copy(prefix, batchRoundPrefix)
	binary.BigEndian.PutUint64(prefix[len(batchRoundPrefix):], round)
	return prefix
}

func makeBatchRoundKey(round uint64, digest types.Hash) []byte {
	key := make([]byte, len(batchRoundPrefix)+8+types.HashSize)
	copy(key, batchRoundPrefix)
	binary.BigEndian.PutUint64(key[len(batchRoundPrefix):], round)
	copy(key[len(batchRoundPrefix)+8:], digest[:])
	return key
}

func extractDigestFromRoundKey(key, prefix []byte) (types.Hash, error) {
	if len(key) < len(prefix)+types.HashSize {
		return types.EmptyHash, fmt.Errorf("key too short")
	}

	var digest types.Hash
	copy(digest[:], key[len(prefix):len(prefix)+types.HashSize])
	return digest, nil
}

func parseRoundKey(key []byte) (uint64, types.Hash, error) {
	prefixLen := len(batchRoundPrefix)
	if len(key) < prefixLen+8+types.HashSize {
		return 0, types.EmptyHash, fmt.Errorf("key too short")
	}

	round := binary.BigEndian.Uint64(key[prefixLen : prefixLen+8])

	var digest types.Hash
	copy(digest[:], key[prefixLen+8:])

	return round, digest, nil
}

// Encoding helpers

func encodeBatch(batch *types.Batch) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(batch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBatch(data []byte) (*types.Batch, error) {
	var batch types.Batch
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&batch); err != nil {
		return nil, err
	}
	return &batch, nil
}

// Verify interface compliance
var _ BatchStore = (*LevelDBBatchStore)(nil)
