package looseberry

import (
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
}

// Looseberry is the main DAG-based mempool implementation.
type Looseberry struct {
	cfg          *Config
	validatorSet types.ValidatorSet

	// Transaction validation
	txValidator TxValidator

	// Core components
	workerPool     *worker.Pool
	workerScaler   *worker.Scaler
	primaryNode    *primary.Primary
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

// SetNetwork sets the network implementation.
func (l *Looseberry) SetNetwork(net network.Network) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.network = net
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

	return nil
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
		l.txIndex = store.NewMemoryTxIndex()
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

	// Start sync manager
	if err := l.syncManager.Start(); err != nil {
		_ = l.primaryNode.Stop()
		_ = l.workerScaler.Stop()
		_ = l.workerPool.Stop()
		return fmt.Errorf("start sync manager: %w", err)
	}

	// Start GC manager
	if err := l.gcManager.Start(); err != nil {
		_ = l.syncManager.Stop()
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

	// Check flow control
	if l.flowController.IsPaused() {
		l.totalTxRejected.Add(1)
		return types.ErrFlowControlPaused
	}

	// Add to worker pool (validation happens in pool)
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

	// Get committed round
	committedRound := l.dag.CommittedRound()

	// Get certificates from committed round + 1 to current
	currentRound := l.dag.HighestRound()
	if currentRound <= committedRound {
		return nil
	}

	// Get ordered certificates
	certs := l.dag.GetOrderedCertificates(committedRound+1, currentRound)
	if len(certs) == 0 {
		return nil
	}

	var result []CertifiedBatch
	var totalBytes int64

	for _, cert := range certs {
		// Get batches for this certificate
		for _, batchRef := range cert.Header.BatchRefs {
			batch, err := l.batchStore.GetBatch(batchRef.Digest)
			if err != nil || batch == nil {
				continue
			}

			batchSize := int64(batch.Size())
			if totalBytes+batchSize > maxBytes && len(result) > 0 {
				return result
			}

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
		TotalTxAdded:    l.totalTxAdded.Load(),
		TotalTxRejected: l.totalTxRejected.Load(),
		TotalBatches:    l.totalBatches.Load(),
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

	// Send acknowledgment
	if l.network != nil {
		_ = l.network.SendBatchAck(msg.From, &network.BatchAckMessage{
			BatchDigest: msg.Batch.Digest,
			Validator:   l.cfg.ValidatorIndex,
		})
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
func (l *Looseberry) handleCertificateMessage(msg *network.CertificateMessage) {
	if msg == nil || msg.Certificate == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.primaryNode != nil {
		_ = l.primaryNode.HandleCertificate(msg.Certificate)
	}

	// Add to DAG
	if l.dag != nil {
		_ = l.dag.AddCertificate(msg.Certificate)
	}

	// Update flow control
	if l.flowController != nil {
		l.flowController.UpdateCurrentRound(msg.Certificate.Header.Round)
	}
}

// handleBatchAckMessage processes incoming batch acknowledgment messages.
func (l *Looseberry) handleBatchAckMessage(msg *network.BatchAckMessage) {
	if msg == nil {
		return
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.workerPool != nil {
		l.workerPool.RecordAck(msg.BatchDigest, msg.Validator)
	}
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
func (l *Looseberry) onBatchCreated(batch *types.Batch) {
	defer recoverCallback("onBatchCreated")

	l.totalBatches.Add(1)

	l.mu.RLock()
	defer l.mu.RUnlock()

	// Add batch digest to primary for header creation
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
func (l *Looseberry) onHeaderCreated(header *types.Header) {
	defer recoverCallback("onHeaderCreated")

	l.mu.RLock()
	defer l.mu.RUnlock()

	// Broadcast header to network
	if l.network != nil {
		_ = l.network.BroadcastHeader(header)
	}
}

// onVoteCreated is called when a vote is created for a header.
func (l *Looseberry) onVoteCreated(vote *types.Vote, targetValidator uint16) {
	defer recoverCallback("onVoteCreated")

	l.mu.RLock()
	defer l.mu.RUnlock()

	// Send vote to target validator
	if l.network != nil {
		_ = l.network.SendVote(targetValidator, vote)
	}
}

// onCertificateFormed is called when a certificate is formed.
func (l *Looseberry) onCertificateFormed(cert *types.Certificate) {
	defer recoverCallback("onCertificateFormed")

	l.mu.RLock()
	defer l.mu.RUnlock()

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
