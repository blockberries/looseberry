package store

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/blockberries/looseberry/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Key prefixes for the LevelDB transaction index.
//
//   T:{txHash}            -> batchHash               (tx → batch lookup)
//   TB:{batchHash}:{N}    -> txHash                  (per-batch tx list, N=uint32 LE)
//   TR:{batchHash}        -> round (uint64 BE)       (batch → round, for pruning)
var (
	txToBatchPrefix     = []byte("T:")
	batchToTxsPrefix    = []byte("TB:")
	batchRoundIdxPrefix = []byte("TR:")
)

// LevelDBTxIndex is a LevelDB-backed implementation of TxIndex. It mirrors
// the in-memory MemoryTxIndex so that tx-deduplication state survives
// validator restarts — otherwise the looseberry GC's `RecoverUncommittedTxs`
// would silently re-admit transactions that committed in earlier batches.
type LevelDBTxIndex struct {
	db     *leveldb.DB
	mu     sync.RWMutex
	closed bool
}

// NewLevelDBTxIndex creates a new LevelDB-backed transaction index.
func NewLevelDBTxIndex(path string) (*LevelDBTxIndex, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}
	return &LevelDBTxIndex{db: db}, nil
}

// AddTx adds a transaction hash to batch mapping.
func (idx *LevelDBTxIndex) AddTx(txHash, batchHash types.Hash) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	txKey := makeTxKey(txHash)

	// Idempotent: if we already have this exact mapping, succeed.
	existing, err := idx.db.Get(txKey, nil)
	if err == nil {
		if len(existing) == types.HashSize {
			var h types.Hash
			copy(h[:], existing)
			if h.Equal(batchHash) {
				return nil
			}
			return types.ErrTxAlreadyExists
		}
	} else if err != leveldb.ErrNotFound {
		return fmt.Errorf("failed to check tx existence: %w", err)
	}

	// Find the next slot in TB:{batchHash}.
	slot, err := idx.nextBatchTxSlot(batchHash)
	if err != nil {
		return err
	}

	wb := new(leveldb.Batch)
	wb.Put(txKey, batchHash[:])
	wb.Put(makeBatchTxKey(batchHash, slot), txHash[:])

	if err := idx.db.Write(wb, nil); err != nil {
		return fmt.Errorf("failed to write tx index entry: %w", err)
	}
	return nil
}

// AddBatch indexes all transactions in a batch.
func (idx *LevelDBTxIndex) AddBatch(batch *types.Batch) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	batchHash := batch.Digest

	// Persist the batch's round (for pruning) — overwrite is fine.
	roundBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(roundBytes, batch.Round)

	// Determine current slot count.
	slot, err := idx.nextBatchTxSlot(batchHash)
	if err != nil {
		return err
	}

	wb := new(leveldb.Batch)
	wb.Put(makeBatchRoundIdxKey(batchHash), roundBytes)

	for _, tx := range batch.Transactions {
		txHash := tx.Hash()
		txKey := makeTxKey(txHash)

		// Skip if already mapped (idempotent across re-adds, foreign batches).
		existing, gerr := idx.db.Get(txKey, nil)
		if gerr == nil {
			if len(existing) == types.HashSize {
				var h types.Hash
				copy(h[:], existing)
				if h.Equal(batchHash) {
					continue
				}
				// Mapped to a different batch — skip silently to match the
				// in-memory implementation's behaviour.
				continue
			}
		} else if gerr != leveldb.ErrNotFound {
			return fmt.Errorf("failed to check tx existence: %w", gerr)
		}

		wb.Put(txKey, batchHash[:])
		wb.Put(makeBatchTxKey(batchHash, slot), txHash[:])
		slot++
	}

	if err := idx.db.Write(wb, nil); err != nil {
		return fmt.Errorf("failed to index batch: %w", err)
	}
	return nil
}

// GetBatchForTx returns the batch hash containing the transaction.
func (idx *LevelDBTxIndex) GetBatchForTx(txHash types.Hash) (types.Hash, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return types.EmptyHash, types.ErrNotRunning
	}

	data, err := idx.db.Get(makeTxKey(txHash), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return types.EmptyHash, types.ErrBatchNotFound
		}
		return types.EmptyHash, fmt.Errorf("failed to read tx index: %w", err)
	}
	if len(data) != types.HashSize {
		return types.EmptyHash, types.ErrBatchNotFound
	}
	var h types.Hash
	copy(h[:], data)
	return h, nil
}

// HasTx returns true if the transaction is indexed.
func (idx *LevelDBTxIndex) HasTx(txHash types.Hash) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return false
	}

	has, _ := idx.db.Has(makeTxKey(txHash), nil)
	return has
}

// RemoveTxsForBatch removes all transaction mappings for a batch.
func (idx *LevelDBTxIndex) RemoveTxsForBatch(batchHash types.Hash) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return types.ErrNotRunning
	}

	return idx.removeBatchLocked(batchHash)
}

// PruneOlderThan removes all transaction mappings for batches older than the
// given round. batchStore is accepted for interface parity but not required —
// we keep our own (batchHash → round) index.
func (idx *LevelDBTxIndex) PruneOlderThan(round uint64, _ BatchStore) (int, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return 0, types.ErrNotRunning
	}

	var toPrune []types.Hash

	iter := idx.db.NewIterator(util.BytesPrefix(batchRoundIdxPrefix), nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if len(key) < len(batchRoundIdxPrefix)+types.HashSize || len(value) < 8 {
			continue
		}
		if binary.BigEndian.Uint64(value) >= round {
			continue
		}
		var h types.Hash
		copy(h[:], key[len(batchRoundIdxPrefix):])
		toPrune = append(toPrune, h)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iteration error: %w", err)
	}

	for _, h := range toPrune {
		if err := idx.removeBatchLocked(h); err != nil {
			return 0, err
		}
	}
	return len(toPrune), nil
}

// Close closes the index.
func (idx *LevelDBTxIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}
	idx.closed = true
	return idx.db.Close()
}

// Len returns the number of indexed transactions (for testing).
func (idx *LevelDBTxIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0
	}

	count := 0
	iter := idx.db.NewIterator(util.BytesPrefix(txToBatchPrefix), nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	return count
}

// Internal helpers.

// removeBatchLocked removes every tx→batch and batch→tx record for the given
// batch, and the batch→round record. Caller must hold idx.mu.
func (idx *LevelDBTxIndex) removeBatchLocked(batchHash types.Hash) error {
	prefix := makeBatchTxPrefix(batchHash)

	wb := new(leveldb.Batch)

	iter := idx.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		txBytes := iter.Value()
		if len(txBytes) == types.HashSize {
			var txHash types.Hash
			copy(txHash[:], txBytes)
			wb.Delete(makeTxKey(txHash))
		}
		// Copy key — Delete reuses iterator buffer if not snapshot.
		wb.Delete(append([]byte{}, iter.Key()...))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iteration error: %w", err)
	}

	wb.Delete(makeBatchRoundIdxKey(batchHash))

	if err := idx.db.Write(wb, nil); err != nil {
		return fmt.Errorf("failed to remove batch tx mappings: %w", err)
	}
	return nil
}

// nextBatchTxSlot returns the next free slot number under TB:{batchHash}.
func (idx *LevelDBTxIndex) nextBatchTxSlot(batchHash types.Hash) (uint32, error) {
	var max uint32
	found := false

	prefix := makeBatchTxPrefix(batchHash)
	iter := idx.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		key := iter.Key()
		if len(key) < len(prefix)+4 {
			continue
		}
		slot := binary.BigEndian.Uint32(key[len(prefix):])
		if !found || slot > max {
			max = slot
			found = true
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iteration error: %w", err)
	}
	if !found {
		return 0, nil
	}
	return max + 1, nil
}

// Key construction helpers.

func makeTxKey(txHash types.Hash) []byte {
	k := make([]byte, len(txToBatchPrefix)+types.HashSize)
	copy(k, txToBatchPrefix)
	copy(k[len(txToBatchPrefix):], txHash[:])
	return k
}

func makeBatchTxPrefix(batchHash types.Hash) []byte {
	k := make([]byte, len(batchToTxsPrefix)+types.HashSize)
	copy(k, batchToTxsPrefix)
	copy(k[len(batchToTxsPrefix):], batchHash[:])
	return k
}

func makeBatchTxKey(batchHash types.Hash, slot uint32) []byte {
	k := make([]byte, len(batchToTxsPrefix)+types.HashSize+4)
	copy(k, batchToTxsPrefix)
	copy(k[len(batchToTxsPrefix):], batchHash[:])
	binary.BigEndian.PutUint32(k[len(batchToTxsPrefix)+types.HashSize:], slot)
	return k
}

func makeBatchRoundIdxKey(batchHash types.Hash) []byte {
	k := make([]byte, len(batchRoundIdxPrefix)+types.HashSize)
	copy(k, batchRoundIdxPrefix)
	copy(k[len(batchRoundIdxPrefix):], batchHash[:])
	return k
}

// Verify interface compliance.
var _ TxIndex = (*LevelDBTxIndex)(nil)
