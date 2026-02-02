package gc

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Logger interface for GC logging. If not set, uses standard library log.
type Logger interface {
	Error(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Debug(msg string, keysAndValues ...interface{})
}

// defaultLogger uses the standard library log package.
type defaultLogger struct{}

func (l *defaultLogger) Error(msg string, keysAndValues ...interface{}) {
	log.Printf("ERROR: %s %v", msg, keysAndValues)
}

func (l *defaultLogger) Info(msg string, keysAndValues ...interface{}) {
	log.Printf("INFO: %s %v", msg, keysAndValues)
}

func (l *defaultLogger) Debug(msg string, keysAndValues ...interface{}) {
	log.Printf("DEBUG: %s %v", msg, keysAndValues)
}

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

// GCMetrics tracks garbage collection statistics.
type GCMetrics struct {
	TotalGCRuns      uint64
	TotalGCFailures  uint64
	LastGCRound      uint64
	LastGCDuration   time.Duration
	TotalTxRecovered uint64
}

// uncommittedBatchEntry tracks an uncommitted batch.
type uncommittedBatchEntry struct {
	digest types.Hash
	round  uint64
}

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

	// Uncommitted batch index for fast GC extraction
	// Maps batch digest to round, only contains uncommitted batches
	uncommittedBatches   map[types.Hash]uint64
	uncommittedBatchesMu sync.Mutex

	// Transaction recovery
	txRecoveryCallback TxRecoveryCallback

	// Logging
	logger Logger

	// Metrics
	totalGCRuns      atomic.Uint64
	totalGCFailures  atomic.Uint64
	lastGCDuration   atomic.Int64 // nanoseconds
	totalTxRecovered atomic.Uint64

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
		dag:                d,
		batchStore:         batchStore,
		certStore:          certStore,
		txIndex:            txIndex,
		cfg:                cfg,
		uncommittedBatches: make(map[types.Hash]uint64),
		logger:             &defaultLogger{},
		stopCh:             make(chan struct{}),
		stoppedCh:          make(chan struct{}),
	}
}

// SetLogger sets a custom logger.
func (gc *GCManager) SetLogger(logger Logger) {
	gc.logger = logger
}

// SetTxRecoveryCallback sets the callback for recovered transactions.
func (gc *GCManager) SetTxRecoveryCallback(cb TxRecoveryCallback) {
	gc.txRecoveryCallback = cb
}

// TrackBatch adds a batch to the uncommitted batch index.
// Called when a new batch is created by a worker.
func (gc *GCManager) TrackBatch(digest types.Hash, round uint64) {
	gc.uncommittedBatchesMu.Lock()
	defer gc.uncommittedBatchesMu.Unlock()
	gc.uncommittedBatches[digest] = round
}

// MarkBatchCommitted removes a batch from the uncommitted index.
// Called when a certificate is formed that includes this batch.
func (gc *GCManager) MarkBatchCommitted(digest types.Hash) {
	gc.uncommittedBatchesMu.Lock()
	defer gc.uncommittedBatchesMu.Unlock()
	delete(gc.uncommittedBatches, digest)
}

// MarkBatchesCommitted removes multiple batches from the uncommitted index.
func (gc *GCManager) MarkBatchesCommitted(digests []types.Hash) {
	gc.uncommittedBatchesMu.Lock()
	defer gc.uncommittedBatchesMu.Unlock()
	for _, digest := range digests {
		delete(gc.uncommittedBatches, digest)
	}
}

// UncommittedBatchCount returns the number of uncommitted batches being tracked.
func (gc *GCManager) UncommittedBatchCount() int {
	gc.uncommittedBatchesMu.Lock()
	defer gc.uncommittedBatchesMu.Unlock()
	return len(gc.uncommittedBatches)
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
			gc.totalTxRecovered.Add(uint64(len(txs)))
			gc.logger.Info("Recovered uncommitted transactions",
				"count", len(txs),
				"before_round", beforeRound,
			)
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

	// Prune TxIndex to prevent unbounded memory growth
	if gc.txIndex != nil {
		if _, err := gc.txIndex.PruneOlderThan(beforeRound, gc.batchStore); err != nil {
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
// Uses the uncommitted batch index for O(n) extraction instead of scanning all batches.
func (gc *GCManager) extractUncommittedTxs(beforeRound uint64) []types.Transaction {
	var uncommittedTxs []types.Transaction
	var batchesToRemove []types.Hash

	// Use the uncommitted batch index for fast extraction
	gc.uncommittedBatchesMu.Lock()
	for digest, round := range gc.uncommittedBatches {
		if round < beforeRound {
			// Get the batch and extract transactions
			batch, err := gc.batchStore.GetBatch(digest)
			if err == nil && batch != nil {
				uncommittedTxs = append(uncommittedTxs, batch.Transactions...)
			}
			batchesToRemove = append(batchesToRemove, digest)
		}
	}

	// Clean up processed entries from the index
	for _, digest := range batchesToRemove {
		delete(gc.uncommittedBatches, digest)
	}
	gc.uncommittedBatchesMu.Unlock()

	// If index was empty, fall back to scanning (for backwards compatibility)
	if len(uncommittedTxs) == 0 && len(batchesToRemove) == 0 {
		uncommittedTxs = gc.extractUncommittedTxsFallback(beforeRound)
	}

	return uncommittedTxs
}

// extractUncommittedTxsFallback is the original extraction method used when the
// uncommitted batch index is not populated (e.g., after restart).
func (gc *GCManager) extractUncommittedTxsFallback(beforeRound uint64) []types.Transaction {
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
				startTime := time.Now()
				if err := gc.performGC(gcRound); err != nil {
					gc.totalGCFailures.Add(1)
					gc.logger.Error("Garbage collection failed",
						"round", gcRound,
						"error", err,
						"committed_round", gc.committedRound.Load(),
					)
				} else {
					gc.totalGCRuns.Add(1)
					duration := time.Since(startTime)
					gc.lastGCDuration.Store(int64(duration))
					gc.logger.Debug("Garbage collection completed",
						"round", gcRound,
						"duration", duration,
					)
				}
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

// Metrics returns current GC metrics.
func (gc *GCManager) Metrics() GCMetrics {
	return GCMetrics{
		TotalGCRuns:      gc.totalGCRuns.Load(),
		TotalGCFailures:  gc.totalGCFailures.Load(),
		LastGCRound:      gc.lastGCRound.Load(),
		LastGCDuration:   time.Duration(gc.lastGCDuration.Load()),
		TotalTxRecovered: gc.totalTxRecovered.Load(),
	}
}
