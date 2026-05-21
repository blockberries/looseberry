package worker

import (
	"container/heap"
	"fmt"
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

// TxValidator validates a transaction before adding it to pending and
// reports the application's mempool-ordering hints. A nil error admits
// the tx; the returned TxAdmission's Priority and Sender are used by
// the worker to order pending txs (max-priority first, FIFO within
// equal priorities). A non-nil error rejects the tx; the returned
// admission is ignored.
//
// Apps that don't care about priority can return a zero TxAdmission
// and rely on the worker's FIFO fallback.
type TxValidator func(tx []byte) (types.TxAdmission, error)

// pendingEntry holds a transaction together with the application-supplied
// ordering hints from TxValidator.
type pendingEntry struct {
	tx       types.Transaction
	priority int64
	// sequence is a monotonic per-worker counter, used to break ties
	// between equal-priority txs so admission order is preserved.
	sequence uint64
}

// priorityQueue is a max-heap of pendingEntry sorted by priority (high
// first), with FIFO arrival order as the tiebreaker. Implements
// container/heap.Interface.
type priorityQueue []*pendingEntry

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority
	}
	return pq[i].sequence < pq[j].sequence
}
func (pq priorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(x any)        { *pq = append(*pq, x.(*pendingEntry)) }
func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}

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

	// Pending transactions. The queue is a max-heap of pendingEntry by
	// priority (FIFO tiebreaker) so tryCreateBatch can pop the highest-
	// priority txs first. pendingSet is the O(1) hash-membership index.
	pending      *priorityQueue
	pendingSet   map[types.Hash]bool
	pendingBytes int64
	pendingSeq   uint64
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
		pending:     &priorityQueue{},
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

// SetAckQuorumCallback installs a callback fired when this worker's
// AckTracker first observes quorum for a tracked batch. Forwards to
// the AckTracker; pass nil to clear.
func (w *Worker) SetAckQuorumCallback(cb QuorumCallback) {
	w.ackTracker.SetQuorumCallback(cb)
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

	// Validate the transaction and capture the app's ordering hints.
	// When no validator is wired, treat the tx as priority 0 / no sender —
	// equivalent to pure FIFO.
	var admission types.TxAdmission
	if w.txValidator != nil {
		var err error
		admission, err = w.txValidator(tx)
		if err != nil {
			return fmt.Errorf("%w: %v", types.ErrTxValidationFailed, err)
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
	if w.pending.Len() >= w.cfg.MaxPendingTxs {
		w.pendingMu.Unlock()
		return types.ErrWorkerBackpressure
	}
	if w.pendingBytes+int64(tx.Size()) > w.cfg.MaxPendingBytes {
		w.pendingMu.Unlock()
		return types.ErrWorkerBackpressure
	}

	// Add to pending heap with the app's priority hint. Sequence is a
	// per-worker monotonic counter so equal-priority entries drain in
	// FIFO arrival order.
	w.pendingSeq++
	heap.Push(w.pending, &pendingEntry{
		tx:       tx.Clone(),
		priority: admission.Priority,
		sequence: w.pendingSeq,
	})
	w.pendingSet[txHash] = true
	w.pendingBytes += int64(tx.Size())

	// Check if we should trigger immediate batch creation
	shouldTrigger := w.pending.Len() >= w.cfg.BatchSize ||
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
	return w.pending.Len()
}

// DrainPending removes and returns all pending transactions.
// Used during worker scale-down to prevent transaction loss.
//
// The returned slice is NOT in priority order — drain is for
// re-routing to other workers, not for inclusion. tryCreateBatch is
// the only path that pops in priority order.
func (w *Worker) DrainPending() []types.Transaction {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	if w.pending.Len() == 0 {
		return nil
	}

	// Clone the transactions to return
	txs := make([]types.Transaction, 0, w.pending.Len())
	for _, e := range *w.pending {
		txs = append(txs, e.tx.Clone())
	}

	// Clear pending state
	w.pending = &priorityQueue{}
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
//
// Selection is highest-priority-first via heap.Pop; equal-priority txs
// drain in arrival order (the sequence tiebreaker baked into
// priorityQueue.Less). The BatchBytes cap applies as txs are popped: if
// adding the next-priority tx would exceed the cap, stop and keep it for
// the next batch.
func (w *Worker) tryCreateBatch() {
	w.pendingMu.Lock()

	if w.pending.Len() == 0 {
		w.pendingMu.Unlock()
		return
	}

	maxCount := w.cfg.BatchSize
	if maxCount > w.pending.Len() {
		maxCount = w.pending.Len()
	}

	txs := make([]types.Transaction, 0, maxCount)
	var batchBytes int64
	for len(txs) < maxCount {
		// Peek at the highest-priority entry without popping yet so we
		// can honor the BatchBytes cap.
		top := (*w.pending)[0]
		txBytes := int64(top.tx.Size())
		if w.cfg.BatchBytes > 0 && len(txs) > 0 && batchBytes+txBytes > w.cfg.BatchBytes {
			break
		}
		entry := heap.Pop(w.pending).(*pendingEntry)
		txs = append(txs, entry.tx)
		batchBytes += txBytes
		delete(w.pendingSet, entry.tx.Hash())
	}

	w.pendingBytes -= batchBytes

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

	// Self-ack: the creator implicitly acks its own batch — the data is
	// trivially available locally. Without this, in topologies where the
	// quorum count equals or exceeds the number of remote acks the primary
	// could observe (e.g. 1-of-1, 2-of-2 development setups), the batch
	// would never reach quorum and tryCreateHeader would never include it
	// (B3-2).
	w.ackTracker.RecordAck(batch.Digest, w.validatorID)

	// Notify callback
	if w.batchCallback != nil {
		w.batchCallback(batch)
	}
}

// requeueTransactions adds transactions back to the pending pool after a
// storage failure. Re-admitted txs lose their original priority hint and
// get priority=0 (they'll re-batch in FIFO order). This is acceptable
// because requeueing only happens on a rare storage-fault path; the cost
// of preserving exact priority would be threading admission metadata
// through tryCreateBatch's failure handling for a code path that nobody
// hits in steady state.
func (w *Worker) requeueTransactions(txs []types.Transaction) {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	for _, tx := range txs {
		txHash := tx.Hash()
		// Only add if not already pending (could happen with concurrent operations)
		if !w.pendingSet[txHash] {
			// Check backpressure - if at limit, we have to drop transactions
			if w.pending.Len() >= w.cfg.MaxPendingTxs {
				break
			}
			if w.pendingBytes+int64(tx.Size()) > w.cfg.MaxPendingBytes {
				break
			}
			w.pendingSeq++
			heap.Push(w.pending, &pendingEntry{
				tx:       tx,
				priority: 0,
				sequence: w.pendingSeq,
			})
			w.pendingSet[txHash] = true
			w.pendingBytes += int64(tx.Size())
		}
	}
}
