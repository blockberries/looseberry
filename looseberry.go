package looseberry

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/blockberries/looseberry/types"
)

// CertifiedBatch represents a batch that has been certified in the DAG.
type CertifiedBatch struct {
	Batch       *types.Batch
	Certificate *types.Certificate
}

// DAGMempool is the interface for the DAG-based mempool.
type DAGMempool interface {
	// AddTx adds a transaction to the mempool.
	// The transaction is validated via TxValidator before being accepted.
	// Returns ErrTxValidationFailed if validation fails.
	// Returns ErrWorkerBackpressure if the worker is overloaded.
	AddTx(tx []byte) error

	// ReapCertifiedBatches returns certified batches for block building.
	// Returns batches in deterministic order: by round ASC, then by validator index ASC.
	// Only returns batches from rounds > lastCommittedRound.
	ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

	// NotifyCommitted notifies that consensus has committed up to a round.
	// Enables garbage collection of older rounds.
	NotifyCommitted(round uint64)

	// UpdateValidatorSet updates the validator set (epoch change).
	UpdateValidatorSet(validators types.ValidatorSet)

	// HasTx returns true if the transaction is in the mempool.
	HasTx(hash []byte) bool

	// Size returns the number of pending transactions.
	Size() int

	// SizeBytes returns the total size of pending transactions in bytes.
	SizeBytes() int64

	// Flush removes all transactions from the mempool.
	Flush()

	// CurrentRound returns the current DAG round.
	CurrentRound() uint64
}

// Looseberry is the main DAG-based mempool implementation.
type Looseberry struct {
	cfg          *Config
	validatorSet types.ValidatorSet

	// Transaction validation
	txValidator TxValidator

	// Lifecycle
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// New creates a new Looseberry instance.
func New(cfg *Config) (*Looseberry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	l := &Looseberry{
		cfg:         cfg,
		txValidator: cfg.TxValidator,
		stopCh:      make(chan struct{}),
	}

	return l, nil
}

// SetValidatorSet sets the validator set.
func (l *Looseberry) SetValidatorSet(validators types.ValidatorSet) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.validatorSet = validators
}

// Start starts the Looseberry instance.
func (l *Looseberry) Start() error {
	if l.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	l.mu.Lock()
	l.stopCh = make(chan struct{})
	l.mu.Unlock()

	// TODO: Start workers, primary, sync manager, etc.

	return nil
}

// Stop stops the Looseberry instance.
func (l *Looseberry) Stop() error {
	if !l.running.Swap(false) {
		return types.ErrNotRunning
	}

	l.mu.Lock()
	close(l.stopCh)
	l.mu.Unlock()

	l.wg.Wait()

	// TODO: Stop workers, primary, sync manager, etc.

	return nil
}

// IsRunning returns true if Looseberry is running.
func (l *Looseberry) IsRunning() bool {
	return l.running.Load()
}

// AddTx implements DAGMempool.
func (l *Looseberry) AddTx(tx []byte) error {
	if !l.running.Load() {
		return types.ErrNotRunning
	}

	// Validate transaction
	if l.txValidator != nil {
		if err := l.txValidator(tx); err != nil {
			return fmt.Errorf("%w: %v", types.ErrTxValidationFailed, err)
		}
	}

	// TODO: Route to worker based on tx hash

	return nil
}

// ReapCertifiedBatches implements DAGMempool.
func (l *Looseberry) ReapCertifiedBatches(maxBytes int64) []CertifiedBatch {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// TODO: Implement batch reaping from DAG

	return nil
}

// NotifyCommitted implements DAGMempool.
func (l *Looseberry) NotifyCommitted(round uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO: Trigger GC, update committed round

}

// UpdateValidatorSet implements DAGMempool.
func (l *Looseberry) UpdateValidatorSet(validators types.ValidatorSet) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.validatorSet = validators

	// TODO: Handle epoch transition
}

// HasTx implements DAGMempool.
func (l *Looseberry) HasTx(hash []byte) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// TODO: Check tx index

	return false
}

// Size implements DAGMempool.
func (l *Looseberry) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// TODO: Return total pending tx count

	return 0
}

// SizeBytes implements DAGMempool.
func (l *Looseberry) SizeBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// TODO: Return total pending tx bytes

	return 0
}

// Flush implements DAGMempool.
func (l *Looseberry) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO: Clear all pending transactions

}

// CurrentRound implements DAGMempool.
func (l *Looseberry) CurrentRound() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// TODO: Return current DAG round

	return 0
}

// Verify interface compliance
var _ DAGMempool = (*Looseberry)(nil)
