package looseberry

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/gc"
	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/primary"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
	"github.com/blockberries/looseberry/worker"
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

	// Metrics returns current metrics.
	Metrics() *Metrics
}

// Metrics contains metrics for the Looseberry instance.
type Metrics struct {
	// Transaction metrics
	PendingTxCount   int
	PendingTxBytes   int64
	TotalTxAdded     uint64
	TotalTxRejected  uint64
	TotalTxCommitted uint64

	// Batch metrics
	TotalBatches          uint64
	TotalBatchesCertified uint64

	// Round metrics
	CurrentRound   uint64
	CommittedRound uint64
	HighestRound   uint64

	// Worker metrics
	WorkerCount int
	WorkerLoad  float64

	// Flow control metrics
	IsPaused       bool
	PauseCount     uint64
	ResumeCount    uint64
	UncommittedGap uint64

	// Certificate metrics
	TotalCertificates uint64

	// Rebroadcast metrics (PLAN §E7c). Each time the stuck-detection
	// loop fires, RebroadcastFires increments by one and the per-item
	// counters increment by the number of batches/headers re-emitted
	// on that fire. All stay at zero on healthy clusters; non-zero
	// counts are the signal that the cluster is hitting the slow-path
	// recovery code path.
	RebroadcastFires     uint64
	RebroadcastedBatches uint64
	RebroadcastedHeaders uint64
}

// Looseberry is the main DAG-based mempool implementation.
type Looseberry struct {
	cfg          *Config
	validatorSet types.ValidatorSet

	// Transaction validation
	txValidator TxValidator

	// Observability hooks (optional).
	// ackQuorumCallback fires when any tracked batch reaches ack quorum;
	// raspberry wires it to a Prometheus histogram.
	ackQuorumCallback worker.QuorumCallback

	// Core components
	workerPool     *worker.Pool
	workerScaler   *worker.Scaler
	primaryNode    *primary.Primary
	batchFetcher   *primary.BatchFetcher
	dag            *dag.DAG
	syncManager    *network.SyncManager
	gcManager      *gc.GCManager
	flowController *gc.FlowController

	// Storage
	batchStore store.BatchStore
	certStore  store.CertificateStore
	txIndex    store.TxIndex

	// Network
	network network.Network

	// Metrics
	totalTxAdded    atomic.Uint64
	totalTxRejected atomic.Uint64
	totalBatches    atomic.Uint64

	// Startup-aware stuck-detection state for the rebroadcast loop
	// (PLAN §E7c). Three preconditions gate the rebroadcast trigger so
	// it can't fire during the cluster's libp2p mesh-warmup window:
	//
	//   (a) lastCommittedRound > 0 — at least one commit has happened.
	//       A cluster that's still forming its first round-0 cert is
	//       NOT a candidate for rebroadcast (an earlier gated-on-time
	//       attempt mis-fired here and pushed fast-path runs into
	//       slow-path because the trigger flooded streams just as they
	//       were establishing).
	//
	//   (b) time.Since(startNanos) > rebroadcastStartupGrace — past the
	//       libp2p mesh-formation window.
	//
	//   (c) time.Since(lastCommitNanos) > stuckRebroadcastThreshold —
	//       no commit progress in long enough to look stuck.
	//
	// All three must be true for the loop to fire. Net: healthy
	// fast-path runs (which commit early and continuously) never
	// trigger; slow-path runs that recovered initial commit but then
	// stalled DO trigger, accelerating their drain.
	startNanos         atomic.Int64
	lastCommitNanos    atomic.Int64
	lastCommittedRound atomic.Uint64

	// Observability counters for the rebroadcast loop. Stay zero on
	// healthy clusters; non-zero means stuck-detection has fired and
	// the cluster has been auto-recovering via re-emission. Exposed
	// via Metrics().
	totalRebroadcastFires     atomic.Uint64
	totalRebroadcastedBatches atomic.Uint64
	totalRebroadcastedHeaders atomic.Uint64

	// Out-of-order cert buffer (PLAN §E7).
	//
	// Cert messages and the cert chain they reference can arrive out of
	// order under burst load — round-N+1 cert showing up before its
	// round-N parents because broadcasts are async and per-stream
	// independent. dag.AddCertificate rejects orphan certs with
	// ErrMissingParents, and handleCertificateMessage previously
	// discarded that error silently — so a peer would never get the
	// orphan cert again. The DAG would stall at the round that the
	// missing parents would otherwise have populated, and every block
	// thereafter would be empty because ReapCertifiedBatches couldn't
	// see past the stuck round.
	//
	// orphanCerts buffers those orphans. Whenever a cert lands
	// successfully, we replay every orphan: any whose parents are now
	// present will add this pass; the rest stay buffered for the next
	// successful add. orphanCertCap bounds the buffer so a malicious
	// peer can't pump orphans forever.
	orphanCerts   map[types.Hash]*types.Certificate
	orphanCertsMu sync.Mutex

	// Lifecycle
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// orphanCertCap bounds the number of orphan certificates the looseberry
// instance will hold awaiting their parents. Far higher than any healthy
// scenario needs; small enough that a malicious peer can't bloat memory.
const orphanCertCap = 4096

// New creates a new Looseberry instance.
func New(cfg *Config) (*Looseberry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	l := &Looseberry{
		cfg:         cfg,
		txValidator: cfg.TxValidator,
		stopCh:      make(chan struct{}),
		orphanCerts: make(map[types.Hash]*types.Certificate),
	}

	return l, nil
}

// SetValidatorSet sets the validator set.
func (l *Looseberry) SetValidatorSet(validators types.ValidatorSet) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.validatorSet = validators
}

// SetNetwork sets the network implementation.
func (l *Looseberry) SetNetwork(net network.Network) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.network = net
}

// SetAckQuorumCallback installs an observability hook fired the moment
// any tracked batch's ack count first reaches quorum. Passes the batch
// digest and the wall-clock latency from batch creation to quorum. Must
// be cheap and non-blocking — the callback runs under the AckTracker
// mutex.
//
// Used by raspberry's observability layer to drive the
// raspberry_batch_ack_latency_seconds histogram. Must be called before
// Start() to attach to the initial worker set; the same callback is
// applied to any worker created later via ScaleUp.
func (l *Looseberry) SetAckQuorumCallback(cb worker.QuorumCallback) {
	l.mu.Lock()
	l.ackQuorumCallback = cb
	pool := l.workerPool
	l.mu.Unlock()
	if pool != nil {
		pool.SetAckQuorumCallback(cb)
	}
}

// SetStores sets the storage implementations.
func (l *Looseberry) SetStores(batchStore store.BatchStore, certStore store.CertificateStore, txIndex store.TxIndex) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batchStore = batchStore
	l.certStore = certStore
	l.txIndex = txIndex
}

// Start starts the Looseberry instance.
func (l *Looseberry) Start() error {
	if l.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Reset stop channel
	l.stopCh = make(chan struct{})

	// Validate required dependencies
	if l.validatorSet == nil {
		l.running.Store(false)
		return fmt.Errorf("validator set not set")
	}
	if l.network == nil {
		l.running.Store(false)
		return fmt.Errorf("network not set")
	}

	// Initialize stores if not set
	if err := l.initializeStores(); err != nil {
		l.running.Store(false)
		return fmt.Errorf("initialize stores: %w", err)
	}

	// Initialize components
	if err := l.initializeComponents(); err != nil {
		l.running.Store(false)
		return fmt.Errorf("initialize components: %w", err)
	}

	// Start components in order
	if err := l.startComponents(); err != nil {
		l.running.Store(false)
		return fmt.Errorf("start components: %w", err)
	}

	// Start network
	if err := l.network.Start(); err != nil {
		l.stopComponents()
		l.running.Store(false)
		return fmt.Errorf("start network: %w", err)
	}

	// Start message processing loop
	l.wg.Add(1)
	go l.messageLoop()

	// Capture Start time for the startup-aware rebroadcast grace
	// (PLAN §E7c). Reset on every Start so Stop/Start cycles get a
	// fresh grace window. Seed lastCommitNanos to the same value so
	// the stuck-detection check has a meaningful baseline even when
	// no commit has happened yet — important for hard-stall clusters
	// (round-0 never forms) where rebroadcast should still fire after
	// the startup grace + stuck threshold has elapsed.
	now := time.Now().UnixNano()
	l.startNanos.Store(now)
	l.lastCommitNanos.Store(now)

	// Start the startup-aware rebroadcast loop.
	l.wg.Add(1)
	go l.rebroadcastLoop()

	return nil
}

// Rebroadcast-loop tuning. The three thresholds compose:
//
//   - rebroadcastStartupGrace: don't fire for the first 15 s after
//     Start. This is the window during which libp2p streams establish,
//     batches get their first acks, and the round-0 cert forms. Firing
//     during this window adds a synchronized re-emission burst onto
//     streams that are still warming up — observed to push fast-path
//     runs into slow-path on TestPhaseE_HighBurstSweep/100k-4-source.
//
//   - stuckRebroadcastThreshold: don't fire unless committedRound has
//     been frozen for at least 3 s past its last advance. Healthy
//     clusters commit every ~1 s, so this never fires on a normally-
//     progressing cluster.
//
//   - rebroadcastMinAge: don't re-emit a batch whose acks may still
//     legitimately be in flight (the AckTracker's freshness filter).
//
// Plus: lastCommittedRound must be > 0 before the loop can fire AT
// ALL — a cluster that's never committed isn't a candidate.
const (
	rebroadcastInterval       = 1 * time.Second
	rebroadcastStartupGrace   = 15 * time.Second
	stuckRebroadcastThreshold = 3 * time.Second
	rebroadcastMinAge         = 2 * time.Second
)

// rebroadcastLoop polls the gate every tick and, when the cluster has
// (a) cleared startup grace, (b) made at least one commit, and (c)
// gone too long without commit progress, re-broadcasts the locally-
// pending batches that have not yet reached ack quorum. Peers receive
// the duplicate batch, store it idempotently, and send fresh signed
// acks; the batch reaches quorum and the cluster resumes.
func (l *Looseberry) rebroadcastLoop() {
	defer l.wg.Done()

	ticker := time.NewTicker(rebroadcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.rebroadcastIfStuck()
		}
	}
}

func (l *Looseberry) rebroadcastIfStuck() {
	// Gate (a): past the startup grace window. Reading the atomic is
	// cheap; this comparison is hit on every tick of every healthy
	// looseberry, so the check stays fast.
	startNanos := l.startNanos.Load()
	if startNanos == 0 {
		return
	}
	if time.Since(time.Unix(0, startNanos)) < rebroadcastStartupGrace {
		return
	}
	// Gate (b): committedRound has been frozen long enough to look stuck.
	// lastCommitNanos is seeded at Start, so if no commit has happened
	// yet, this is time.Since(start) — which is >= rebroadcastStartupGrace
	// by gate (a). Hard-stalled clusters (no round-0 cert formed) hit
	// this path; we re-broadcast their pending batches in case the
	// original acks got dropped during mesh warm-up. Healthy clusters
	// commit every ~1 s so this never elapses on them.
	lastNanos := l.lastCommitNanos.Load()
	if time.Since(time.Unix(0, lastNanos)) < stuckRebroadcastThreshold {
		return
	}

	l.mu.RLock()
	pool := l.workerPool
	net := l.network
	l.mu.RUnlock()
	if pool == nil || net == nil {
		return
	}

	// Look up candidates at BOTH levels (PLAN §E7c). The 100K hard-stall
	// case showed batches reaching quorum but headers not getting
	// certified — a batch-only rebroadcast misses that failure mode.
	batches := pool.PendingBatchesBelowQuorum(rebroadcastMinAge)
	l.mu.RLock()
	primary := l.primaryNode
	l.mu.RUnlock()
	var headers []*types.Header
	if primary != nil {
		headers = primary.PendingHeadersOlderThan(rebroadcastMinAge)
	}

	if len(batches) == 0 && len(headers) == 0 {
		return
	}

	// Observability: bump the fire counter once per stuck-detection
	// firing and bump the work counters by however many items we
	// re-emit. These flow through Metrics() so production can see
	// whether the cluster is hitting the recovery path.
	l.totalRebroadcastFires.Add(1)
	l.totalRebroadcastedBatches.Add(uint64(len(batches)))
	l.totalRebroadcastedHeaders.Add(uint64(len(headers)))

	// Dispatch via the existing async helper so a backpressured stream
	// can't park this loop. Glueberry's per-stream flow control is the
	// natural concurrency limit.
	for _, b := range batches {
		batch := b
		l.dispatchAsync(func() {
			_ = net.BroadcastBatch(batch)
		})
	}
	for _, h := range headers {
		header := h
		l.dispatchAsync(func() {
			_ = net.BroadcastHeader(header)
		})
	}
}

// initializeStores creates storage if not already set.
func (l *Looseberry) initializeStores() error {
	if l.batchStore == nil {
		if l.cfg.Storage.InMemory {
			l.batchStore = store.NewMemoryBatchStore()
		} else {
			bs, err := store.NewLevelDBBatchStore(l.cfg.Storage.DataDir + "/batches")
			if err != nil {
				return fmt.Errorf("create batch store: %w", err)
			}
			l.batchStore = bs
		}
	}

	if l.certStore == nil {
		if l.cfg.Storage.InMemory {
			l.certStore = store.NewMemoryCertificateStore()
		} else {
			cs, err := store.NewLevelDBCertificateStore(l.cfg.Storage.DataDir + "/certs")
			if err != nil {
				return fmt.Errorf("create cert store: %w", err)
			}
			l.certStore = cs
		}
	}

	if l.txIndex == nil {
		if l.cfg.Storage.InMemory {
			l.txIndex = store.NewMemoryTxIndex()
		} else {
			ti, err := store.NewLevelDBTxIndex(l.cfg.Storage.DataDir + "/txindex")
			if err != nil {
				return fmt.Errorf("create tx index: %w", err)
			}
			l.txIndex = ti
		}
	}

	return nil
}

// initializeComponents creates all internal components.
func (l *Looseberry) initializeComponents() error {
	// Create DAG
	dagCfg := dag.DefaultConfig()
	l.dag = dag.New(l.certStore, dagCfg)

	// Create flow controller
	flowCfg := gc.FlowConfig{
		MaxUncommittedRounds: uint64(l.cfg.FlowControl.MaxUncommittedRounds),
		MaxPendingBatches:    1000,
		MaxPendingHeaders:    100,
	}
	l.flowController = gc.NewFlowController(flowCfg)

	// Create worker pool
	poolCfg := worker.PoolConfig{
		MinWorkers: l.cfg.Worker.MinWorkers,
		MaxWorkers: l.cfg.Worker.MaxWorkers,
		Worker: worker.Config{
			BatchSize:       l.cfg.Worker.BatchSize,
			BatchTimeout:    l.cfg.Worker.BatchTimeout,
			MaxPendingTxs:   l.cfg.Worker.MaxPendingTxs,
			MaxPendingBytes: l.cfg.Worker.MaxPendingBytes,
			AckTimeout:      30 * time.Second,
		},
	}
	l.workerPool = worker.NewPool(poolCfg, l.cfg.ValidatorIndex, l.batchStore, l.txIndex, l.validatorSet.Quorum())

	// Set batch callback
	l.workerPool.SetBatchCallback(l.onBatchCreated)

	// Forward any pre-Start ack-quorum observability hook to the pool.
	if l.ackQuorumCallback != nil {
		l.workerPool.SetAckQuorumCallback(l.ackQuorumCallback)
	}

	// Set tx validator
	if l.txValidator != nil {
		l.workerPool.SetTxValidator(worker.TxValidator(l.txValidator))
	}

	// Create worker scaler
	scalerCfg := worker.ScalerConfig{
		ScaleUpThreshold:   l.cfg.Worker.ScaleUpThreshold,
		ScaleDownThreshold: l.cfg.Worker.ScaleDownThreshold,
		ScalingInterval:    l.cfg.Worker.ScalingInterval,
		ScaleCooldown:      5 * time.Second,
	}
	l.workerScaler = worker.NewScaler(l.workerPool, scalerCfg)

	// Create primary
	primaryCfg := primary.Config{
		HeaderTimeout:       l.cfg.Primary.HeaderTimeout,
		MaxBatchesPerHeader: l.cfg.Primary.MaxBatchesPerHeader,
		MaxRoundGap:         l.cfg.Primary.MaxRoundGap,
		VoteTimeout:         l.cfg.Primary.VoteTimeout,
		AllowEmptyHeaders:   l.cfg.Primary.AllowEmptyHeaders,
	}
	l.primaryNode = primary.New(
		l.cfg.ValidatorIndex,
		l.cfg.Signer,
		primaryCfg,
		l.certStore,
		l.batchStore,
		l.validatorSet,
	)

	// Set primary callbacks
	l.primaryNode.SetHeaderCallback(l.onHeaderCreated)
	l.primaryNode.SetVoteCallback(l.onVoteCreated)
	l.primaryNode.SetCertificateCallback(l.onCertificateFormed)

	// Wire the data-availability gate: the primary will only include a
	// batch digest in a new header after 2f+1 acks (B3-2 / T1-2).
	l.primaryNode.SetAckQuorumChecker(l.workerPool)

	// Create batch fetcher. It owns the "missing batch → request from peer"
	// loop that was previously a TODO in HandleHeader (T1-3 / B3-1, B3-3).
	bfCfg := primary.DefaultBatchFetcherConfig()
	l.batchFetcher = primary.NewBatchFetcher(bfCfg, l.batchStore)
	l.batchFetcher.SetRequestCallback(l.onBatchRequest)
	l.batchFetcher.SetHeaderReadyCallback(l.onHeaderReady)
	l.primaryNode.SetBatchFetcher(l.batchFetcher)
	l.batchFetcher.UpdateValidators(l.peerValidatorIndices())

	// Create sync manager
	syncCfg := network.SyncConfig{
		SyncInterval:  l.cfg.Sync.SyncInterval,
		SyncThreshold: l.cfg.Sync.SyncThreshold,
		SyncBatchSize: l.cfg.Sync.SyncBatchSize,
		SyncTimeout:   l.cfg.Sync.SyncTimeout,
	}
	l.syncManager = network.NewSyncManager(l.dag, l.batchStore, l.network, l.validatorSet, syncCfg)

	// Create GC manager
	gcCfg := gc.Config{
		GCDepth:               uint64(l.cfg.GC.GCDepth),
		GCInterval:            30 * time.Second,
		RecoverUncommittedTxs: l.cfg.GC.RecoverTxs,
	}
	l.gcManager = gc.NewGCManager(l.dag, l.batchStore, l.certStore, l.txIndex, gcCfg)

	// Set tx recovery callback
	if l.cfg.GC.RecoverTxs {
		l.gcManager.SetTxRecoveryCallback(l.onTxRecovered)
	}

	return nil
}

// startComponents starts all internal components.
func (l *Looseberry) startComponents() error {
	// Start worker pool
	if err := l.workerPool.Start(); err != nil {
		return fmt.Errorf("start worker pool: %w", err)
	}

	// Start worker scaler
	if err := l.workerScaler.Start(); err != nil {
		_ = l.workerPool.Stop()
		return fmt.Errorf("start worker scaler: %w", err)
	}

	// Start primary
	if err := l.primaryNode.Start(); err != nil {
		_ = l.workerScaler.Stop()
		_ = l.workerPool.Stop()
		return fmt.Errorf("start primary: %w", err)
	}

	// Start batch fetcher
	if err := l.batchFetcher.Start(); err != nil {
		_ = l.primaryNode.Stop()
		_ = l.workerScaler.Stop()
		_ = l.workerPool.Stop()
		return fmt.Errorf("start batch fetcher: %w", err)
	}

	// Start sync manager
	if err := l.syncManager.Start(); err != nil {
		_ = l.batchFetcher.Stop()
		_ = l.primaryNode.Stop()
		_ = l.workerScaler.Stop()
		_ = l.workerPool.Stop()
		return fmt.Errorf("start sync manager: %w", err)
	}

	// Start GC manager
	if err := l.gcManager.Start(); err != nil {
		_ = l.syncManager.Stop()
		_ = l.batchFetcher.Stop()
		_ = l.primaryNode.Stop()
		_ = l.workerScaler.Stop()
		_ = l.workerPool.Stop()
		return fmt.Errorf("start gc manager: %w", err)
	}

	return nil
}

// stopComponents stops all internal components.
func (l *Looseberry) stopComponents() {
	if l.gcManager != nil {
		_ = l.gcManager.Stop()
	}
	if l.syncManager != nil {
		_ = l.syncManager.Stop()
	}
	if l.batchFetcher != nil {
		_ = l.batchFetcher.Stop()
	}
	if l.primaryNode != nil {
		_ = l.primaryNode.Stop()
	}
	if l.workerScaler != nil {
		_ = l.workerScaler.Stop()
	}
	if l.workerPool != nil {
		_ = l.workerPool.Stop()
	}
}

// Stop stops the Looseberry instance.
func (l *Looseberry) Stop() error {
	if !l.running.Swap(false) {
		return types.ErrNotRunning
	}

	l.mu.Lock()
	close(l.stopCh)
	l.mu.Unlock()

	// Wait for message loop to finish
	l.wg.Wait()

	// Stop network first (without lock to allow callbacks to complete)
	if l.network != nil {
		_ = l.network.Stop()
	}

	// Stop components in reverse order (without lock to avoid deadlock with callbacks)
	l.stopComponents()

	// Now acquire lock to close stores
	l.mu.Lock()
	defer l.mu.Unlock()

	// Close stores
	if l.batchStore != nil {
		l.batchStore.Close()
	}
	if l.certStore != nil {
		l.certStore.Close()
	}
	if l.txIndex != nil {
		l.txIndex.Close()
	}

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

	// Check flow control. The flowController is allocated in Start(); a
	// caller can race with Start/Stop and observe a nil pointer here, so
	// the guard mirrors the pattern used elsewhere in this file (e.g.
	// UpdateCommittedRound at line 586). Pre-existing nil-deref flagged by
	// raspberry's tests/integration/{chaos,e2e}_test.go.
	if l.flowController != nil && l.flowController.IsPaused() {
		l.totalTxRejected.Add(1)
		return types.ErrFlowControlPaused
	}

	// Worker pool is allocated in Start(); same race window as flowController
	// above. Without this guard a tx submission racing Start triggers a nil
	// receiver panic in (*Pool).AddTx (seen by raspberry tests/integration
	// e2e_test.go::TestE2E_TransactionSubmission).
	if l.workerPool == nil {
		l.totalTxRejected.Add(1)
		return types.ErrNotRunning
	}
	if err := l.workerPool.AddTx(types.Transaction(tx)); err != nil {
		l.totalTxRejected.Add(1)
		return err
	}

	l.totalTxAdded.Add(1)
	return nil
}

// ReapCertifiedBatches implements DAGMempool.
func (l *Looseberry) ReapCertifiedBatches(maxBytes int64) []CertifiedBatch {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.dag == nil {
		return nil
	}

	// Determine which rounds to reap.
	// committedRound starts at 0 (zero-value), so we use HasCommitted()
	// to distinguish "nothing committed yet" from "round 0 committed".
	currentRound := l.dag.HighestRound()
	var fromRound uint64
	if l.dag.HasCommitted() {
		committedRound := l.dag.CommittedRound()
		if currentRound <= committedRound {
			return nil
		}
		fromRound = committedRound + 1
	}
	// When !HasCommitted(), fromRound stays 0 — include all rounds from the start

	// Get ordered certificates
	certs := l.dag.GetOrderedCertificates(fromRound, currentRound)
	if len(certs) == 0 {
		return nil
	}

	var result []CertifiedBatch
	var totalBytes int64
	seen := make(map[types.Hash]bool)

	for _, cert := range certs {
		// Get batches for this certificate
		for _, batchRef := range cert.Header.BatchRefs {
			// Dedupe by digest — the same batch can be referenced by multiple
			// certificates (e.g. when several primaries cite the same batch
			// in their headers). Returning duplicates would cause double
			// execution at the block-application layer.
			if seen[batchRef.Digest] {
				continue
			}

			batch, err := l.batchStore.GetBatch(batchRef.Digest)
			if err != nil || batch == nil {
				continue
			}

			batchSize := int64(batch.Size())
			if totalBytes+batchSize > maxBytes && len(result) > 0 {
				return result
			}

			seen[batchRef.Digest] = true
			result = append(result, CertifiedBatch{
				Batch:       batch,
				Certificate: cert,
			})
			totalBytes += batchSize
		}
	}

	return result
}

// NotifyCommitted implements DAGMempool.
func (l *Looseberry) NotifyCommitted(round uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.dag != nil {
		l.dag.SetCommittedRound(round)
	}

	if l.flowController != nil {
		l.flowController.UpdateCommittedRound(round)
	}

	if l.gcManager != nil {
		_ = l.gcManager.NotifyCommitted(round)
	}

	// Startup-aware stuck-detection (PLAN §E7c): record the wall-clock
	// instant of every strictly-higher committedRound so rebroadcastLoop
	// can decide whether the cluster is making progress.
	if round > l.lastCommittedRound.Load() {
		l.lastCommittedRound.Store(round)
		l.lastCommitNanos.Store(time.Now().UnixNano())
	}
}

// CatchUpPeerCerts re-broadcasts every cert in this validator's local
// DAG. Designed to be called after a new validator peer registers
// post-handshake — without it, late-joining peers (the last validator
// to start, typically v3 in a 4-validator stagger) permanently miss
// the foundational round-0 certs that were broadcast before they were
// in our registry. v3 then can't satisfy
// `primary.tryAdvanceRound()`'s quorum-of-round-0-certs check and
// stays stuck at CurrentRound=0 forever, even while passively
// receiving later-round certs via SyncManager catchup.
//
// Idempotent on existing peers: dag.AddCertificate dedupes by digest
// so the re-broadcast adds nothing for peers that already have the
// certs. The expected fan-out is small at startup (a few certs × a
// few peers) so the extra load is negligible.
//
// Each re-broadcast is dispatched via the existing dispatchAsync
// helper so a slow peer can't park this call.
func (l *Looseberry) CatchUpPeerCerts() {
	l.mu.RLock()
	dag := l.dag
	net := l.network
	l.mu.RUnlock()
	if dag == nil || net == nil {
		return
	}
	highest := dag.HighestRound()
	certs := dag.GetOrderedCertificates(0, highest)
	if len(certs) == 0 {
		return
	}
	for _, cert := range certs {
		c := cert
		l.dispatchAsync(func() {
			_ = net.BroadcastCertificate(c)
		})
	}
}

// HighestRound returns the highest certificate round in the DAG.
func (l *Looseberry) HighestRound() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.dag == nil {
		return 0
	}
	return l.dag.HighestRound()
}

// UpdateValidatorSet implements DAGMempool.
func (l *Looseberry) UpdateValidatorSet(validators types.ValidatorSet) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.validatorSet = validators

	// Update quorum in worker pool
	if l.workerPool != nil {
		l.workerPool.UpdateQuorum(validators.Quorum())
	}

	// Update validator set in primary
	if l.primaryNode != nil {
		l.primaryNode.UpdateValidatorSet(validators)
	}

	// Update validator set in sync manager
	if l.syncManager != nil {
		l.syncManager.UpdateValidatorSet(validators)
	}

	// Refresh the batch fetcher's peer list so retries pick the new set.
	if l.batchFetcher != nil {
		l.batchFetcher.UpdateValidators(l.peerValidatorIndices())
	}
}

// HasTx implements DAGMempool.
func (l *Looseberry) HasTx(hash []byte) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.txIndex == nil {
		return false
	}

	var h types.Hash
	copy(h[:], hash)
	return l.txIndex.HasTx(h)
}

// Size implements DAGMempool.
func (l *Looseberry) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.workerPool == nil {
		return 0
	}

	return l.workerPool.PendingCount()
}

// SizeBytes implements DAGMempool.
func (l *Looseberry) SizeBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.workerPool == nil {
		return 0
	}

	return l.workerPool.PendingBytes()
}

// Flush implements DAGMempool.
func (l *Looseberry) Flush() {
	if !l.running.Load() {
		return
	}

	l.mu.RLock()
	pool := l.workerPool
	l.mu.RUnlock()

	// Stop and restart workers to flush pending transactions
	// Don't hold lock during stop/start to avoid deadlock with callbacks
	if pool != nil {
		_ = pool.Stop()
		_ = pool.Start()
	}
}

// CurrentRound implements DAGMempool.
func (l *Looseberry) CurrentRound() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.primaryNode == nil {
		return 0
	}

	return l.primaryNode.Round()
}

// Metrics returns current metrics.
func (l *Looseberry) Metrics() *Metrics {
	l.mu.RLock()
	defer l.mu.RUnlock()

	m := &Metrics{
		TotalTxAdded:         l.totalTxAdded.Load(),
		TotalTxRejected:      l.totalTxRejected.Load(),
		TotalBatches:         l.totalBatches.Load(),
		RebroadcastFires:     l.totalRebroadcastFires.Load(),
		RebroadcastedBatches: l.totalRebroadcastedBatches.Load(),
		RebroadcastedHeaders: l.totalRebroadcastedHeaders.Load(),
	}

	if l.workerPool != nil {
		m.PendingTxCount = l.workerPool.PendingCount()
		m.PendingTxBytes = l.workerPool.PendingBytes()
		m.WorkerCount = l.workerPool.WorkerCount()
	}

	if l.workerScaler != nil {
		scalerMetrics := l.workerScaler.Metrics()
		m.WorkerLoad = scalerMetrics.CurrentLoad
	}

	if l.dag != nil {
		m.CurrentRound = l.dag.HighestRound()
		m.CommittedRound = l.dag.CommittedRound()
		m.HighestRound = l.dag.HighestRound()
	}

	if l.primaryNode != nil {
		m.CurrentRound = l.primaryNode.Round()
	}

	if l.flowController != nil {
		flowMetrics := l.flowController.Metrics()
		m.IsPaused = flowMetrics.IsPaused
		m.PauseCount = flowMetrics.PauseCount
		m.ResumeCount = flowMetrics.ResumeCount
		m.UncommittedGap = flowMetrics.UncommittedGap
	}

	return m
}

// messageLoop processes incoming network messages.
func (l *Looseberry) messageLoop() {
	defer l.wg.Done()

	for {
		select {
		case <-l.stopCh:
			return

		case msg := <-l.network.BatchMessages():
			l.handleBatchMessage(msg)

		case msg := <-l.network.HeaderMessages():
			l.handleHeaderMessage(msg)

		case msg := <-l.network.VoteMessages():
			l.handleVoteMessage(msg)

		case msg := <-l.network.CertificateMessages():
			l.handleCertificateMessage(msg)

		case msg := <-l.network.BatchAckMessages():
			l.handleBatchAckMessage(msg)

		case req := <-l.network.BatchRequestMessages():
			l.handleBatchRequestMessage(req)

		case resp := <-l.network.BatchResponseMessages():
			l.handleBatchResponseMessage(resp)

		case req := <-l.network.SyncRequests():
			l.handleSyncRequest(req)

		case msg := <-l.network.SyncResponses():
			l.handleSyncResponse(msg)
		}
	}
}

// handleBatchMessage processes incoming batch messages.
func (l *Looseberry) handleBatchMessage(msg *network.BatchMessage) {
	if msg == nil || msg.Batch == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	// Store the batch
	if l.batchStore != nil {
		_ = l.batchStore.SaveBatch(msg.Batch)
	}

	// Index transactions
	if l.txIndex != nil {
		_ = l.txIndex.AddBatch(msg.Batch)
	}

	// Notify the BatchFetcher: a header may have been pending on this batch.
	if l.batchFetcher != nil {
		l.batchFetcher.NotifyBatchReceived(msg.Batch)
	}

	// Send signed acknowledgment. The signature binds (batchDigest, validator,
	// round) so peers cannot forge or replay acks (T1-4).
	//
	// PLAN §E7b: the ack send is dispatched off the messageLoop. A
	// blocking libp2p Send on a backpressured "looseberry-batch-acks"
	// stream wedges every other incoming-message handler on this node,
	// and under multi-source burst that was enough to keep batches
	// from ever reaching 2f+1 acks → no headers → no certs → empty
	// blocks forever.
	if l.network != nil && l.cfg.Signer != nil {
		ack := &network.BatchAckMessage{
			BatchDigest: msg.Batch.Digest,
			Validator:   l.cfg.ValidatorIndex,
			Round:       msg.Batch.Round,
		}
		signBytes := network.BatchAckSignBytes(ack.BatchDigest, ack.Validator, ack.Round)
		sig, err := l.cfg.Signer.Sign(signBytes)
		if err == nil {
			ack.Signature = sig
			from := msg.From
			l.dispatchAsync(func() {
				_ = l.network.SendBatchAck(from, ack)
			})
		}
	}
}

// handleHeaderMessage processes incoming header messages.
func (l *Looseberry) handleHeaderMessage(msg *network.HeaderMessage) {
	if msg == nil || msg.Header == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.primaryNode != nil {
		_ = l.primaryNode.HandleHeader(msg.Header)
	}
}

// handleVoteMessage processes incoming vote messages.
func (l *Looseberry) handleVoteMessage(msg *network.VoteMessage) {
	if msg == nil || msg.Vote == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.primaryNode != nil {
		l.primaryNode.HandleVote(msg.Vote)
	}
}

// handleCertificateMessage processes incoming certificate messages.
//
// Cert ordering note (PLAN §E7): under burst load, certs and their
// parent certs arrive on separate streams and can land out of order —
// dag.AddCertificate then returns ErrMissingParents. Previously that
// error was swallowed and the orphan was permanently dropped, stalling
// every downstream round at any peer that missed the parent broadcast.
// We now buffer such orphans and replay every orphan whenever a cert
// adds cleanly — the new add may have been the missing parent, which
// unblocks one or more buffered children.
func (l *Looseberry) handleCertificateMessage(msg *network.CertificateMessage) {
	if msg == nil || msg.Certificate == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.primaryNode != nil {
		_ = l.primaryNode.HandleCertificate(msg.Certificate)
	}

	// Add to DAG; buffer orphans whose parents haven't arrived yet so
	// they can be replayed once the parents land.
	if l.dag != nil {
		if err := l.dag.AddCertificate(msg.Certificate); err != nil {
			if errors.Is(err, types.ErrMissingParents) {
				l.bufferOrphanCert(msg.Certificate)
			}
		} else {
			// New cert landed — may have been a missing parent for one or
			// more buffered orphans. Replay them.
			l.replayOrphanCertsLocked()
		}
	}

	// Update flow control
	if l.flowController != nil {
		l.flowController.UpdateCurrentRound(msg.Certificate.Header.Round)
	}
}

// bufferOrphanCert stashes a cert whose parents haven't yet arrived.
// Capped at orphanCertCap so a malicious peer can't OOM us by pumping
// orphans; once full, additional orphans are silently dropped.
func (l *Looseberry) bufferOrphanCert(cert *types.Certificate) {
	l.orphanCertsMu.Lock()
	defer l.orphanCertsMu.Unlock()
	if len(l.orphanCerts) >= orphanCertCap {
		return
	}
	digest := cert.Digest()
	if _, dup := l.orphanCerts[digest]; dup {
		return
	}
	l.orphanCerts[digest] = cert
}

// replayOrphanCertsLocked attempts to add every buffered orphan to the
// DAG and removes those that succeeded. Iterates until no progress is
// made so a chain of stacked orphans drains in a single call. Caller
// must hold l.mu (RLock or Lock).
func (l *Looseberry) replayOrphanCertsLocked() {
	if l.dag == nil {
		return
	}
	for {
		l.orphanCertsMu.Lock()
		if len(l.orphanCerts) == 0 {
			l.orphanCertsMu.Unlock()
			return
		}
		batch := make([]*types.Certificate, 0, len(l.orphanCerts))
		for _, c := range l.orphanCerts {
			batch = append(batch, c)
		}
		l.orphanCertsMu.Unlock()

		var added int
		for _, c := range batch {
			if err := l.dag.AddCertificate(c); err == nil {
				l.orphanCertsMu.Lock()
				delete(l.orphanCerts, c.Digest())
				l.orphanCertsMu.Unlock()
				added++
			}
		}
		if added == 0 {
			return // no progress; remaining orphans wait for future adds
		}
	}
}

// handleBatchAckMessage processes incoming batch acknowledgment messages.
//
// The signature is verified before the ack is recorded — without this gate
// any peer can fabricate acks attributed to other validators and bypass
// the 2f+1 availability requirement (T1-4).
func (l *Looseberry) handleBatchAckMessage(msg *network.BatchAckMessage) {
	if msg == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.validatorSet == nil {
		return
	}
	validator := l.validatorSet.GetByIndex(msg.Validator)
	if validator == nil {
		return
	}
	signBytes := network.BatchAckSignBytes(msg.BatchDigest, msg.Validator, msg.Round)
	if !validator.PublicKey.Verify(signBytes, msg.Signature) {
		return
	}

	if l.workerPool != nil {
		l.workerPool.RecordAck(msg.BatchDigest, msg.Validator)
	}
}

// handleBatchRequestMessage serves a peer asking for a batch by digest.
func (l *Looseberry) handleBatchRequestMessage(req *network.BatchRequestMessage) {
	if req == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.network == nil {
		return
	}

	resp := &network.BatchResponseMessage{
		From: l.cfg.ValidatorIndex,
	}
	if l.batchStore != nil {
		if batch, err := l.batchStore.GetBatch(req.BatchDigest); err == nil && batch != nil {
			resp.Batch = batch
			resp.Found = true
		}
	}
	_ = l.network.SendBatchResponse(req.Requester, resp)
}

// handleBatchResponseMessage delivers a batch fetched in response to one of
// our outstanding BatchFetcher requests.
func (l *Looseberry) handleBatchResponseMessage(resp *network.BatchResponseMessage) {
	if resp == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if resp.Found && resp.Batch != nil {
		if l.batchStore != nil {
			_ = l.batchStore.SaveBatch(resp.Batch)
		}
		if l.txIndex != nil {
			_ = l.txIndex.AddBatch(resp.Batch)
		}
	}
	if l.batchFetcher != nil {
		l.batchFetcher.HandleBatchResponse(resp.Batch, resp.Found, resp.From)
	}
}

// onBatchRequest is the BatchFetcher's request callback. It maps the
// (preferred validator, digest) pair to a BatchRequest on the wire.
func (l *Looseberry) onBatchRequest(validator uint16, digest types.Hash) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.network == nil {
		return types.ErrNotRunning
	}
	return l.network.SendBatchRequest(validator, &network.BatchRequestMessage{
		BatchDigest: digest,
		Requester:   l.cfg.ValidatorIndex,
	})
}

// onHeaderReady is invoked by the BatchFetcher once every batch referenced
// by the header has arrived locally. We re-feed the header through the
// primary, which now finds the batches present and proceeds to vote.
//
// Lock note: deliberately does NOT take l.mu — invoked synchronously
// from batchFetcher.NotifyBatchReceived, which is itself called from
// handleBatchMessage / handleBatchResponseMessage while those hold
// l.mu.RLock. Same reentrant-RLock deadlock as onHeaderCreated (PLAN §E4).
// l.primaryNode is immutable post-init; recoverCallback handles
// Stop-race nil derefs.
func (l *Looseberry) onHeaderReady(header *types.Header) {
	defer recoverCallback("onHeaderReady")

	if l.primaryNode != nil {
		// Re-deliver without going through the fetcher again: store now
		// holds every batch, so primary's local fast-path will fire.
		_ = l.primaryNode.HandleHeader(header)
	}
}

// peerValidatorIndices returns every validator index other than ours.
// Used to seed the BatchFetcher's retry pool.
func (l *Looseberry) peerValidatorIndices() []uint16 {
	if l.validatorSet == nil {
		return nil
	}
	me := l.cfg.ValidatorIndex
	all := l.validatorSet.Validators()
	out := make([]uint16, 0, len(all))
	for _, v := range all {
		if v.Index == me {
			continue
		}
		out = append(out, v.Index)
	}
	return out
}

// handleSyncRequest processes incoming sync request messages.
func (l *Looseberry) handleSyncRequest(req *network.SyncRequest) {
	if req == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.syncManager != nil {
		_ = l.syncManager.HandleSyncRequest(req)
	}
}

// handleSyncResponse processes incoming sync response messages.
func (l *Looseberry) handleSyncResponse(msg *network.SyncResponseMessage) {
	if msg == nil || msg.Response == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.syncManager != nil {
		_ = l.syncManager.HandleSyncResponse(msg.Response, msg.From)
	}
}

// onBatchCreated is called when a batch is created by a worker.
//
// The digest is enqueued on the primary, but the primary's tryCreateHeader
// will not actually include it in a header until the worker's AckTracker
// reports 2f+1 acks (B3-2 / T1-2). The historical behaviour — immediate
// inclusion — let a single primary unilaterally certify batches that
// hadn't been replicated, breaking data-availability.
func (l *Looseberry) onBatchCreated(batch *types.Batch) {
	defer recoverCallback("onBatchCreated")

	l.totalBatches.Add(1)

	l.mu.RLock()
	defer l.mu.RUnlock()

	// Enqueue digest for the next eligible header. Inclusion is gated by
	// the AckQuorumChecker wired into the primary; the worker self-acks
	// inside tryCreateBatch so quorum-of-one topologies still progress.
	if l.primaryNode != nil {
		l.primaryNode.AddBatchDigest(types.BatchDigest{
			WorkerID:    batch.WorkerID,
			ValidatorID: batch.ValidatorID,
			Digest:      batch.Digest,
		})
	}

	// Broadcast batch to network
	if l.network != nil {
		_ = l.network.BroadcastBatch(batch)
	}
}

// onHeaderCreated is called when a header is created by the primary.
//
// Lock note: deliberately does NOT take l.mu — these inner callbacks
// fire synchronously from primary.HandleVote/HandleHeader/tryCreateHeader,
// which themselves are invoked from handleVoteMessage/handleHeaderMessage
// while those hold l.mu.RLock. Go's sync.RWMutex is not reentrant: if
// any concurrent goroutine (e.g. NotifyCommitted) queues a writer in
// the meantime, a second RLock attempt here blocks behind that writer,
// while the outer reader can't release until this callback returns —
// classic 3-way deadlock. Observed in Phase E under burst load (see
// PLAN §E4). The fields accessed here (l.network) are set during
// SetNetwork / initializeComponents before any goroutine that could
// invoke this callback is spawned, so no synchronization is required.
// recoverCallback handles the (extremely unlikely) Stop-race nil deref.
func (l *Looseberry) onHeaderCreated(header *types.Header) {
	defer recoverCallback("onHeaderCreated")

	// Broadcast header to network
	if l.network != nil {
		_ = l.network.BroadcastHeader(header)
	}
}

// onVoteCreated is called when a vote is created for a header.
//
// Lock note: see onHeaderCreated. Same reentrant-RLock deadlock pattern.
//
// The vote send is dispatched off the calling goroutine (PLAN §E7b):
// onVoteCreated fires synchronously from handleHeaderMessage, which
// runs in messageLoop. A blocking libp2p send on a backpressured
// "looseberry-votes" stream would otherwise wedge the entire
// messageLoop and starve every other incoming-message handler — under
// burst, that's enough to keep round-0 from ever forming a cert.
func (l *Looseberry) onVoteCreated(vote *types.Vote, targetValidator uint16) {
	defer recoverCallback("onVoteCreated")

	if l.network == nil {
		return
	}
	l.dispatchAsync(func() {
		_ = l.network.SendVote(targetValidator, vote)
	})
}

// dispatchAsync runs fn in a goroutine tracked by l.wg so Stop drains
// cleanly. Used to keep libp2p send latency off the messageLoop hot
// path (PLAN §E7b). The goroutine is intentionally unbounded — glueberry
// already applies per-stream backpressure with a 1000-message high
// watermark, which gives us natural concurrency limits without adding
// another bookkeeping layer here.
func (l *Looseberry) dispatchAsync(fn func()) {
	if !l.running.Load() {
		// Don't spawn during shutdown — Stop has already started draining.
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer recoverCallback("dispatchAsync")
		fn()
	}()
}

// onCertificateFormed is called when a certificate is formed.
//
// Lock note: see onHeaderCreated. Same reentrant-RLock deadlock pattern.
// l.dag, l.flowController and l.network are all set during init and
// not modified at runtime; recoverCallback catches any Stop-race panic.
func (l *Looseberry) onCertificateFormed(cert *types.Certificate) {
	defer recoverCallback("onCertificateFormed")

	// Add to DAG
	if l.dag != nil {
		_ = l.dag.AddCertificate(cert)
	}

	// Update flow control
	if l.flowController != nil {
		l.flowController.UpdateCurrentRound(cert.Header.Round)
	}

	// Broadcast certificate to network
	if l.network != nil {
		_ = l.network.BroadcastCertificate(cert)
	}
}

// onTxRecovered is called when transactions are recovered during GC.
func (l *Looseberry) onTxRecovered(txs []types.Transaction) {
	defer recoverCallback("onTxRecovered")

	// Re-add recovered transactions to the pool
	for _, tx := range txs {
		// Don't validate recovered transactions - they were already validated
		if l.workerPool != nil {
			_ = l.workerPool.AddTx(tx)
		}
	}
}

// Verify interface compliance
var _ DAGMempool = (*Looseberry)(nil)

// recoverCallback recovers from panics in callback functions.
// It logs the panic and stack trace, allowing the system to continue operating.
func recoverCallback(callbackName string) {
	if r := recover(); r != nil {
		log.Printf("ERROR: Panic in %s callback: %v\nStack trace:\n%s",
			callbackName, r, string(debug.Stack()))
	}
}
