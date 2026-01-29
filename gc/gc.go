package gc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Config contains GC configuration.
type Config struct {
	// GCDepth is the number of rounds to keep before pruning.
	GCDepth uint64
	// GCInterval is the interval between GC runs.
	GCInterval time.Duration
	// RecoverUncommittedTxs enables recovery of uncommitted transactions.
	RecoverUncommittedTxs bool
}

// DefaultConfig returns default GC configuration.
func DefaultConfig() Config {
	return Config{
		GCDepth:               100,
		GCInterval:            30 * time.Second,
		RecoverUncommittedTxs: true,
	}
}

// TxRecoveryCallback is called with transactions from uncommitted batches.
type TxRecoveryCallback func(txs []types.Transaction)

// GCManager handles garbage collection of old rounds and transaction recovery.
type GCManager struct {
	dag        *dag.DAG
	batchStore store.BatchStore
	certStore  store.CertificateStore
	txIndex    store.TxIndex
	cfg        Config

	// Committed round tracking
	committedRound atomic.Uint64
	// Last GC'd round - used to avoid re-scanning already GC'd rounds
	lastGCRound atomic.Uint64

	// Transaction recovery
	txRecoveryCallback TxRecoveryCallback

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
	mu        sync.Mutex
}

// NewGCManager creates a new GC manager.
func NewGCManager(
	d *dag.DAG,
	batchStore store.BatchStore,
	certStore store.CertificateStore,
	txIndex store.TxIndex,
	cfg Config,
) *GCManager {
	return &GCManager{
		dag:        d,
		batchStore: batchStore,
		certStore:  certStore,
		txIndex:    txIndex,
		cfg:        cfg,
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
}

// SetTxRecoveryCallback sets the callback for recovered transactions.
func (gc *GCManager) SetTxRecoveryCallback(cb TxRecoveryCallback) {
	gc.txRecoveryCallback = cb
}

// Start starts the GC manager background loop.
func (gc *GCManager) Start() error {
	if gc.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Reset channels for restart capability
	gc.stopCh = make(chan struct{})
	gc.stoppedCh = make(chan struct{})

	go gc.gcLoop()
	return nil
}

// Stop stops the GC manager.
func (gc *GCManager) Stop() error {
	if !gc.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(gc.stopCh)
	<-gc.stoppedCh
	return nil
}

// IsRunning returns true if the GC manager is running.
func (gc *GCManager) IsRunning() bool {
	return gc.running.Load()
}

// NotifyCommitted notifies the GC manager of a newly committed round.
// This triggers GC for rounds older than committed - gcDepth.
func (gc *GCManager) NotifyCommitted(round uint64) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	// Update committed round
	current := gc.committedRound.Load()
	if round > current {
		gc.committedRound.Store(round)
		gc.dag.SetCommittedRound(round)
	}

	// Calculate GC round
	gcRound := gc.calculateGCRound()
	if gcRound == 0 {
		return nil // Nothing to GC
	}

	// Perform GC
	return gc.performGC(gcRound)
}

// CommittedRound returns the current committed round.
func (gc *GCManager) CommittedRound() uint64 {
	return gc.committedRound.Load()
}

// calculateGCRound calculates the round up to which we should GC.
func (gc *GCManager) calculateGCRound() uint64 {
	committed := gc.committedRound.Load()
	if committed <= gc.cfg.GCDepth {
		return 0
	}
	return committed - gc.cfg.GCDepth
}

// performGC performs garbage collection up to the specified round.
func (gc *GCManager) performGC(beforeRound uint64) error {
	// Recover uncommitted transactions first
	if gc.cfg.RecoverUncommittedTxs && gc.txRecoveryCallback != nil {
		txs := gc.extractUncommittedTxs(beforeRound)
		if len(txs) > 0 {
			gc.txRecoveryCallback(txs)
		}
	}

	// Prune from DAG memory
	gc.dag.PruneRoundsBefore(beforeRound)

	// Delete from persistent storage
	if gc.batchStore != nil {
		if err := gc.batchStore.DeleteBatchesBefore(beforeRound); err != nil {
			return err
		}
	}

	if gc.certStore != nil {
		if err := gc.certStore.DeleteCertificatesBefore(beforeRound); err != nil {
			return err
		}
	}

	// Update last GC'd round
	if beforeRound > 0 {
		gc.lastGCRound.Store(beforeRound - 1)
	}

	return nil
}

// extractUncommittedTxs extracts transactions from batches that were not
// included in committed certificates.
func (gc *GCManager) extractUncommittedTxs(beforeRound uint64) []types.Transaction {
	var uncommittedTxs []types.Transaction

	// Start from last GC'd round to avoid re-scanning already processed rounds
	startRound := gc.lastGCRound.Load()
	if startRound > 0 {
		startRound++ // Start from the round after the last GC'd round
	}

	for round := startRound; round < beforeRound; round++ {
		// Get all batches for this round
		batches, err := gc.batchStore.GetBatchesByRound(round)
		if err != nil || len(batches) == 0 {
			continue
		}

		// Get committed certificates for this round
		certs := gc.dag.GetCertificatesForRound(round)

		// Build set of committed batch digests
		committedBatches := make(map[types.Hash]bool)
		for _, cert := range certs {
			for _, batchRef := range cert.Header.BatchRefs {
				committedBatches[batchRef.Digest] = true
			}
		}

		// Extract transactions from uncommitted batches
		for _, batch := range batches {
			if !committedBatches[batch.Digest] {
				uncommittedTxs = append(uncommittedTxs, batch.Transactions...)
			}
		}
	}

	return uncommittedTxs
}

// gcLoop is the background GC loop.
func (gc *GCManager) gcLoop() {
	defer close(gc.stoppedCh)

	ticker := time.NewTicker(gc.cfg.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gc.mu.Lock()
			gcRound := gc.calculateGCRound()
			if gcRound > 0 {
				_ = gc.performGC(gcRound)
			}
			gc.mu.Unlock()
		case <-gc.stopCh:
			return
		}
	}
}

// ForceGC forces an immediate GC operation.
func (gc *GCManager) ForceGC() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gcRound := gc.calculateGCRound()
	if gcRound == 0 {
		return nil
	}

	return gc.performGC(gcRound)
}

// GetGCRound returns the current GC cutoff round.
func (gc *GCManager) GetGCRound() uint64 {
	return gc.calculateGCRound()
}
