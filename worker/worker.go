package worker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Config contains worker configuration.
type Config struct {
	// BatchSize is the maximum number of transactions per batch.
	// When pending transactions reach this count, a batch is created immediately.
	BatchSize int
	// BatchBytes is the maximum size in bytes for a batch.
	// When pending bytes reach this limit, a batch is created immediately.
	BatchBytes int64
	// BatchTimeout is the maximum time to wait before creating a batch.
	BatchTimeout time.Duration
	// MaxPendingTxs is the maximum number of pending transactions.
	MaxPendingTxs int
	// MaxPendingBytes is the maximum size of pending transactions.
	MaxPendingBytes int64
	// AckTimeout is the timeout for batch acknowledgments.
	AckTimeout time.Duration
}

// DefaultConfig returns default worker configuration.
func DefaultConfig() Config {
	return Config{
		BatchSize:       1000,
		BatchBytes:      512 * 1024, // 512KB max batch size
		BatchTimeout:    100 * time.Millisecond,
		MaxPendingTxs:   10000,
		MaxPendingBytes: 50 * 1024 * 1024, // 50MB
		AckTimeout:      30 * time.Second,
	}
}

// TxValidator validates a transaction before adding it to pending.
type TxValidator func(tx []byte) error

// BatchCallback is called when a batch is created and ready to broadcast.
type BatchCallback func(batch *types.Batch)

// Worker batches transactions and manages their lifecycle.
// Thread-safe with mutex protection.
type Worker struct {
	id            uint16
	validatorID   uint16
	cfg           Config
	round         atomic.Uint64
	epoch         atomic.Uint64

	// Pending transactions
	pending      []types.Transaction
	pendingSet   map[types.Hash]bool
	pendingBytes int64
	pendingMu    sync.Mutex

	// Storage
	batchStore store.BatchStore
	txIndex    store.TxIndex

	// Tracking
	ackTracker *AckTracker

	// Callbacks
	txValidator   TxValidator
	batchCallback BatchCallback

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
	triggerCh chan struct{} // Triggers immediate batch creation when size/bytes limits reached
}

// New creates a new Worker.
func New(
	id, validatorID uint16,
	cfg Config,
	batchStore store.BatchStore,
	txIndex store.TxIndex,
	quorum int,
) *Worker {
	return &Worker{
		id:          id,
		validatorID: validatorID,
		cfg:         cfg,
		pending:     make([]types.Transaction, 0, cfg.BatchSize),
		pendingSet:  make(map[types.Hash]bool),
		batchStore:  batchStore,
		txIndex:     txIndex,
		ackTracker:  NewAckTracker(quorum, cfg.AckTimeout),
		stopCh:      make(chan struct{}),
		stoppedCh:   make(chan struct{}),
		triggerCh:   make(chan struct{}, 1), // Buffered to prevent blocking
	}
}

// SetTxValidator sets the transaction validator.
func (w *Worker) SetTxValidator(validator TxValidator) {
	w.txValidator = validator
}

// SetBatchCallback sets the callback for created batches.
func (w *Worker) SetBatchCallback(callback BatchCallback) {
	w.batchCallback = callback
}

// Start starts the worker's batch creation loop.
func (w *Worker) Start() error {
	if w.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Reset channels for restart capability
	w.stopCh = make(chan struct{})
	w.stoppedCh = make(chan struct{})
	w.triggerCh = make(chan struct{}, 1)

	go w.batchLoop()
	return nil
}

// Stop stops the worker gracefully.
func (w *Worker) Stop() error {
	if !w.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(w.stopCh)
	<-w.stoppedCh

	w.ackTracker.Close()
	return nil
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// ID returns the worker ID.
func (w *Worker) ID() uint16 {
	return w.id
}

// ValidatorID returns the validator ID.
func (w *Worker) ValidatorID() uint16 {
	return w.validatorID
}

// SetRound updates the current round.
func (w *Worker) SetRound(round uint64) {
	w.round.Store(round)
}

// Round returns the current round.
func (w *Worker) Round() uint64 {
	return w.round.Load()
}

// SetEpoch updates the current epoch.
func (w *Worker) SetEpoch(epoch uint64) {
	w.epoch.Store(epoch)
}

// Epoch returns the current epoch.
func (w *Worker) Epoch() uint64 {
	return w.epoch.Load()
}

// AddTx adds a transaction to the pending pool.
// Returns ErrWorkerBackpressure if limits are exceeded.
// Returns ErrTxAlreadyExists if the transaction is a duplicate.
func (w *Worker) AddTx(tx types.Transaction) error {
	if !w.running.Load() {
		return types.ErrNotRunning
	}

	// Validate transaction if validator is set
	if w.txValidator != nil {
		if err := w.txValidator(tx); err != nil {
			return types.ErrTxValidationFailed
		}
	}

	w.pendingMu.Lock()

	txHash := tx.Hash()

	// Check for duplicates in pending
	if w.pendingSet[txHash] {
		w.pendingMu.Unlock()
		return nil // Idempotent - already have it
	}

	// Check if already indexed (in a previous batch)
	if w.txIndex != nil && w.txIndex.HasTx(txHash) {
		w.pendingMu.Unlock()
		return nil // Already batched
	}

	// Check backpressure
	if len(w.pending) >= w.cfg.MaxPendingTxs {
		w.pendingMu.Unlock()
		return types.ErrWorkerBackpressure
	}
	if w.pendingBytes+int64(tx.Size()) > w.cfg.MaxPendingBytes {
		w.pendingMu.Unlock()
		return types.ErrWorkerBackpressure
	}

	// Add to pending
	w.pending = append(w.pending, tx.Clone())
	w.pendingSet[txHash] = true
	w.pendingBytes += int64(tx.Size())

	// Check if we should trigger immediate batch creation
	shouldTrigger := len(w.pending) >= w.cfg.BatchSize ||
		(w.cfg.BatchBytes > 0 && w.pendingBytes >= w.cfg.BatchBytes)

	w.pendingMu.Unlock()

	// Trigger batch creation if size or byte limits reached
	if shouldTrigger {
		select {
		case w.triggerCh <- struct{}{}:
			// Signal sent
		default:
			// Channel already has a signal, skip
		}
	}

	return nil
}

// PendingCount returns the number of pending transactions.
func (w *Worker) PendingCount() int {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	return len(w.pending)
}

// DrainPending removes and returns all pending transactions.
// Used during worker scale-down to prevent transaction loss.
func (w *Worker) DrainPending() []types.Transaction {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	if len(w.pending) == 0 {
		return nil
	}

	// Clone the transactions to return
	txs := make([]types.Transaction, len(w.pending))
	for i, tx := range w.pending {
		txs[i] = tx.Clone()
	}

	// Clear pending state
	w.pending = make([]types.Transaction, 0, w.cfg.BatchSize)
	w.pendingSet = make(map[types.Hash]bool)
	w.pendingBytes = 0

	return txs
}

// PendingBytes returns the total size of pending transactions.
func (w *Worker) PendingBytes() int64 {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	return w.pendingBytes
}

// HasTx returns true if the transaction is pending or indexed.
func (w *Worker) HasTx(txHash types.Hash) bool {
	w.pendingMu.Lock()
	pending := w.pendingSet[txHash]
	w.pendingMu.Unlock()

	if pending {
		return true
	}

	if w.txIndex != nil {
		return w.txIndex.HasTx(txHash)
	}

	return false
}

// RecordAck records a batch acknowledgment from a validator.
// Returns true if quorum is reached.
func (w *Worker) RecordAck(batchDigest types.Hash, validator uint16) bool {
	return w.ackTracker.RecordAck(batchDigest, validator)
}

// GetAckTracker returns the ack tracker for testing.
func (w *Worker) GetAckTracker() *AckTracker {
	return w.ackTracker
}

// UpdateQuorum updates the quorum requirement.
func (w *Worker) UpdateQuorum(quorum int) {
	w.ackTracker.UpdateQuorum(quorum)
}

// batchLoop is the main loop that creates batches.
func (w *Worker) batchLoop() {
	defer close(w.stoppedCh)

	ticker := time.NewTicker(w.cfg.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.tryCreateBatch()
		case <-w.triggerCh:
			// Immediate batch creation triggered by size/byte limit
			w.tryCreateBatch()
		case <-w.stopCh:
			// Create final batch if there are pending transactions
			w.tryCreateBatch()
			return
		}
	}
}

// tryCreateBatch creates a batch if there are enough pending transactions.
// On storage failure, transactions are requeued to prevent data loss.
func (w *Worker) tryCreateBatch() {
	w.pendingMu.Lock()

	if len(w.pending) == 0 {
		w.pendingMu.Unlock()
		return
	}

	// Take up to BatchSize transactions, also respecting BatchBytes limit
	count := min(len(w.pending), w.cfg.BatchSize)

	// If BatchBytes is configured, limit batch size by bytes
	if w.cfg.BatchBytes > 0 {
		var batchBytes int64
		for i := 0; i < count; i++ {
			txBytes := int64(w.pending[i].Size())
			if batchBytes+txBytes > w.cfg.BatchBytes && i > 0 {
				count = i
				break
			}
			batchBytes += txBytes
		}
	}

	txs := make([]types.Transaction, count)
	copy(txs, w.pending[:count])

	// Remove from pending
	w.pending = w.pending[count:]
	for _, tx := range txs {
		delete(w.pendingSet, tx.Hash())
	}

	// Update pending bytes
	var newBytes int64
	for _, tx := range w.pending {
		newBytes += int64(tx.Size())
	}
	w.pendingBytes = newBytes

	w.pendingMu.Unlock()

	// Create batch
	batch := types.NewBatch(w.id, w.validatorID, w.round.Load(), txs)

	// Store batch
	if w.batchStore != nil {
		if err := w.batchStore.SaveBatch(batch); err != nil {
			// Storage failure - requeue transactions to prevent data loss
			w.requeueTransactions(txs)
			return
		}
	}

	// Index transactions
	if w.txIndex != nil {
		_ = w.txIndex.AddBatch(batch)
	}

	// Track for acks
	w.ackTracker.TrackBatch(batch)

	// Notify callback
	if w.batchCallback != nil {
		w.batchCallback(batch)
	}
}

// requeueTransactions adds transactions back to the pending pool after a storage failure.
// This prevents transaction loss when storage operations fail.
func (w *Worker) requeueTransactions(txs []types.Transaction) {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	for _, tx := range txs {
		txHash := tx.Hash()
		// Only add if not already pending (could happen with concurrent operations)
		if !w.pendingSet[txHash] {
			// Check backpressure - if at limit, we have to drop transactions
			if len(w.pending) >= w.cfg.MaxPendingTxs {
				break
			}
			if w.pendingBytes+int64(tx.Size()) > w.cfg.MaxPendingBytes {
				break
			}
			w.pending = append(w.pending, tx)
			w.pendingSet[txHash] = true
			w.pendingBytes += int64(tx.Size())
		}
	}
}
