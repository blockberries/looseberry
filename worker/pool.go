package worker

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// PoolConfig contains worker pool configuration.
type PoolConfig struct {
	// MinWorkers is the minimum number of workers.
	MinWorkers int
	// MaxWorkers is the maximum number of workers.
	MaxWorkers int
	// Worker is the configuration for individual workers.
	Worker Config
}

// DefaultPoolConfig returns default pool configuration.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinWorkers: 1,
		MaxWorkers: 4,
		Worker:     DefaultConfig(),
	}
}

// Pool manages a pool of workers.
// Thread-safe with mutex protection.
type Pool struct {
	cfg         PoolConfig
	validatorID uint16
	quorum      int

	workers   []*Worker
	workersMu sync.RWMutex

	// Storage (shared by all workers)
	batchStore store.BatchStore
	txIndex    store.TxIndex

	// Callbacks
	txValidator    TxValidator
	batchCallback  BatchCallback
	quorumCallback QuorumCallback

	// Round and epoch (shared state)
	round atomic.Uint64
	epoch atomic.Uint64

	// Lifecycle
	running   atomic.Bool
	nextID    atomic.Uint32
}

// NewPool creates a new worker pool.
func NewPool(
	cfg PoolConfig,
	validatorID uint16,
	batchStore store.BatchStore,
	txIndex store.TxIndex,
	quorum int,
) *Pool {
	return &Pool{
		cfg:         cfg,
		validatorID: validatorID,
		quorum:      quorum,
		workers:     make([]*Worker, 0, cfg.MaxWorkers),
		batchStore:  batchStore,
		txIndex:     txIndex,
	}
}

// SetTxValidator sets the transaction validator for all workers.
func (p *Pool) SetTxValidator(validator TxValidator) {
	p.txValidator = validator

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.SetTxValidator(validator)
	}
}

// SetBatchCallback sets the callback for created batches.
func (p *Pool) SetBatchCallback(callback BatchCallback) {
	p.batchCallback = callback

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.SetBatchCallback(callback)
	}
}

// SetAckQuorumCallback installs an ack-quorum callback on every current
// and future worker's AckTracker. The callback fires once per tracked
// batch the moment its ack count first reaches quorum and is intended
// for observability (drives raspberry_batch_ack_latency_seconds). Pass
// nil to clear.
func (p *Pool) SetAckQuorumCallback(callback QuorumCallback) {
	p.quorumCallback = callback

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.SetAckQuorumCallback(callback)
	}
}

// Start starts the worker pool with the minimum number of workers.
func (p *Pool) Start() error {
	if p.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Create initial workers
	for range p.cfg.MinWorkers {
		if err := p.addWorker(); err != nil {
			_ = p.Stop()
			return err
		}
	}

	return nil
}

// Stop stops all workers in the pool.
func (p *Pool) Stop() error {
	if !p.running.Swap(false) {
		return types.ErrNotRunning
	}

	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	for _, w := range p.workers {
		_ = w.Stop()
	}
	p.workers = nil

	return nil
}

// IsRunning returns true if the pool is running.
func (p *Pool) IsRunning() bool {
	return p.running.Load()
}

// WorkerCount returns the number of active workers.
func (p *Pool) WorkerCount() int {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	return len(p.workers)
}

// AddTx adds a transaction to the pool.
// The transaction is routed to a worker based on its hash.
func (p *Pool) AddTx(tx types.Transaction) error {
	if !p.running.Load() {
		return types.ErrNotRunning
	}

	p.workersMu.RLock()
	if len(p.workers) == 0 {
		p.workersMu.RUnlock()
		return types.ErrNotRunning
	}

	// Route to worker based on tx hash using more bytes for better distribution
	txHash := tx.Hash()
	hashValue := binary.BigEndian.Uint64(txHash[:8])
	workerIdx := int(hashValue % uint64(len(p.workers)))
	worker := p.workers[workerIdx]
	p.workersMu.RUnlock()

	return worker.AddTx(tx)
}

// PendingCount returns the total number of pending transactions.
func (p *Pool) PendingCount() int {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	total := 0
	for _, w := range p.workers {
		total += w.PendingCount()
	}
	return total
}

// PendingBytes returns the total size of pending transactions.
func (p *Pool) PendingBytes() int64 {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	var total int64
	for _, w := range p.workers {
		total += w.PendingBytes()
	}
	return total
}

// HasTx returns true if any worker has the transaction.
func (p *Pool) HasTx(txHash types.Hash) bool {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	for _, w := range p.workers {
		if w.HasTx(txHash) {
			return true
		}
	}
	return false
}

// SetRound updates the current round for all workers.
func (p *Pool) SetRound(round uint64) {
	p.round.Store(round)

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.SetRound(round)
	}
}

// Round returns the current round.
func (p *Pool) Round() uint64 {
	return p.round.Load()
}

// SetEpoch updates the current epoch for all workers.
func (p *Pool) SetEpoch(epoch uint64) {
	p.epoch.Store(epoch)

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.SetEpoch(epoch)
	}
}

// Epoch returns the current epoch.
func (p *Pool) Epoch() uint64 {
	return p.epoch.Load()
}

// UpdateQuorum updates the quorum requirement for all workers.
func (p *Pool) UpdateQuorum(quorum int) {
	p.quorum = quorum

	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	for _, w := range p.workers {
		w.UpdateQuorum(quorum)
	}
}

// RecordAck records a batch acknowledgment.
// Returns true if quorum is reached.
func (p *Pool) RecordAck(batchDigest types.Hash, validator uint16) bool {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	for _, w := range p.workers {
		if w.GetAckTracker().RecordAck(batchDigest, validator) {
			return true
		}
	}
	return false
}

// PendingBatchesBelowQuorum aggregates pending-below-quorum batches
// across every worker. Each batch is at least minAge old. Drives the
// looseberry instance's startup-aware rebroadcast loop (PLAN §E7c).
func (p *Pool) PendingBatchesBelowQuorum(minAge time.Duration) []*types.Batch {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()
	var out []*types.Batch
	for _, w := range p.workers {
		out = append(out, w.GetAckTracker().PendingBelowQuorum(minAge)...)
	}
	return out
}

// HasBatchAckQuorum reports whether the batch with the given digest has
// reached 2f+1 acknowledgments on any worker. Used by Primary to gate
// batch inclusion in new headers (B3-2 / T1-2).
//
// A digest unknown to every worker returns false: this is the correct
// behaviour because such a batch cannot have any acks counted yet either
// (the local node hasn't tracked it), and including it in a header would
// trigger data-availability failures downstream.
func (p *Pool) HasBatchAckQuorum(batchDigest types.Hash) bool {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	for _, w := range p.workers {
		if w.GetAckTracker().HasQuorum(batchDigest) {
			return true
		}
	}
	return false
}

// ScaleUp adds a new worker if below maximum.
// Returns true if a worker was added.
func (p *Pool) ScaleUp() bool {
	if !p.running.Load() {
		return false
	}

	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	if len(p.workers) >= p.cfg.MaxWorkers {
		return false
	}

	if err := p.addWorkerLocked(); err != nil {
		return false
	}

	return true
}

// ScaleDown removes a worker if above minimum.
// Pending transactions from the removed worker are redistributed to remaining workers.
// Returns true if a worker was removed.
func (p *Pool) ScaleDown() bool {
	if !p.running.Load() {
		return false
	}

	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	if len(p.workers) <= p.cfg.MinWorkers {
		return false
	}

	// Remove last worker
	idx := len(p.workers) - 1
	w := p.workers[idx]
	p.workers = p.workers[:idx]

	// Drain pending transactions before stopping
	pending := w.DrainPending()

	// Stop the worker
	_ = w.Stop()

	// Redistribute pending transactions to remaining workers
	if len(pending) > 0 && len(p.workers) > 0 {
		p.redistributeTxsLocked(pending)
	}

	return true
}

// redistributeTxsLocked redistributes transactions across remaining workers.
// Caller must hold workersMu lock.
func (p *Pool) redistributeTxsLocked(txs []types.Transaction) {
	if len(p.workers) == 0 {
		return
	}

	for _, tx := range txs {
		// Route to worker based on tx hash
		txHash := tx.Hash()
		hashValue := binary.BigEndian.Uint64(txHash[:8])
		workerIdx := int(hashValue % uint64(len(p.workers)))
		worker := p.workers[workerIdx]

		// Add transaction - ignore errors (backpressure, duplicates)
		_ = worker.AddTx(tx)
	}
}

// GetWorker returns a worker by index (for testing).
func (p *Pool) GetWorker(idx int) *Worker {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	if idx < 0 || idx >= len(p.workers) {
		return nil
	}
	return p.workers[idx]
}

// addWorker adds a new worker (requires external lock).
func (p *Pool) addWorker() error {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	return p.addWorkerLocked()
}

// addWorkerLocked adds a new worker (caller must hold lock).
func (p *Pool) addWorkerLocked() error {
	id := uint16(p.nextID.Add(1) - 1)

	w := New(id, p.validatorID, p.cfg.Worker, p.batchStore, p.txIndex, p.quorum)
	w.SetRound(p.round.Load())
	w.SetEpoch(p.epoch.Load())

	if p.txValidator != nil {
		w.SetTxValidator(p.txValidator)
	}
	if p.batchCallback != nil {
		w.SetBatchCallback(p.batchCallback)
	}
	if p.quorumCallback != nil {
		w.SetAckQuorumCallback(p.quorumCallback)
	}

	if err := w.Start(); err != nil {
		return err
	}

	p.workers = append(p.workers, w)
	return nil
}
