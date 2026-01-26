package network

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/dag"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// SyncConfig contains sync manager configuration.
type SyncConfig struct {
	// SyncInterval is the interval for periodic sync checks.
	SyncInterval time.Duration
	// SyncThreshold is the number of rounds behind before triggering sync.
	SyncThreshold uint64
	// SyncBatchSize is the max certificates per sync request.
	SyncBatchSize int
	// SyncTimeout is the timeout for sync requests.
	SyncTimeout time.Duration
}

// DefaultSyncConfig returns default sync configuration.
func DefaultSyncConfig() SyncConfig {
	return SyncConfig{
		SyncInterval:  10 * time.Second,
		SyncThreshold: 5,
		SyncBatchSize: 100,
		SyncTimeout:   30 * time.Second,
	}
}

// SyncManager handles synchronization of certificates between nodes.
type SyncManager struct {
	dag          *dag.DAG
	batchStore   store.BatchStore
	network      Network
	validatorSet types.ValidatorSet
	cfg          SyncConfig

	// Pending sync requests
	pendingReqs   map[uint16]*pendingSyncRequest
	pendingReqsMu sync.Mutex

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// Callbacks
	onSyncComplete func(fromRound, toRound uint64)
}

type pendingSyncRequest struct {
	fromRound uint64
	toRound   uint64
	sentAt    time.Time
}

// NewSyncManager creates a new sync manager.
func NewSyncManager(
	d *dag.DAG,
	batchStore store.BatchStore,
	network Network,
	validatorSet types.ValidatorSet,
	cfg SyncConfig,
) *SyncManager {
	return &SyncManager{
		dag:          d,
		batchStore:   batchStore,
		network:      network,
		validatorSet: validatorSet,
		cfg:          cfg,
		pendingReqs:  make(map[uint16]*pendingSyncRequest),
		stopCh:       make(chan struct{}),
		stoppedCh:    make(chan struct{}),
	}
}

// SetSyncCompleteCallback sets the callback for sync completion.
func (s *SyncManager) SetSyncCompleteCallback(cb func(fromRound, toRound uint64)) {
	s.onSyncComplete = cb
}

// Start starts the sync manager.
func (s *SyncManager) Start() error {
	if s.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	go s.syncLoop()
	go s.handleMessages()

	return nil
}

// Stop stops the sync manager.
func (s *SyncManager) Stop() error {
	if !s.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(s.stopCh)
	<-s.stoppedCh

	return nil
}

// IsRunning returns true if the sync manager is running.
func (s *SyncManager) IsRunning() bool {
	return s.running.Load()
}

// RequestSync requests certificates from a peer.
func (s *SyncManager) RequestSync(validator uint16, fromRound, toRound uint64) error {
	if !s.running.Load() {
		return types.ErrNotRunning
	}

	req := &SyncRequest{
		FromRound: fromRound,
		ToRound:   toRound,
		Requester: s.network.ValidatorID(),
	}

	s.pendingReqsMu.Lock()
	s.pendingReqs[validator] = &pendingSyncRequest{
		fromRound: fromRound,
		toRound:   toRound,
		sentAt:    time.Now(),
	}
	s.pendingReqsMu.Unlock()

	return s.network.SendSyncRequest(validator, req)
}

// HandleSyncRequest handles an incoming sync request.
func (s *SyncManager) HandleSyncRequest(req *SyncRequest) error {
	if !s.running.Load() {
		return types.ErrNotRunning
	}

	fromRound := req.FromRound
	toRound := req.ToRound

	// If toRound is 0, use our highest round
	if toRound == 0 {
		toRound = s.dag.HighestRound()
	}

	// Limit batch size
	if toRound-fromRound+1 > uint64(s.cfg.SyncBatchSize) {
		toRound = fromRound + uint64(s.cfg.SyncBatchSize) - 1
	}

	// Collect certificates
	certs := s.dag.GetOrderedCertificates(fromRound, toRound)

	// Collect referenced batches
	var batches []*types.Batch
	seenBatches := make(map[types.Hash]bool)

	for _, cert := range certs {
		for _, batchRef := range cert.Header.BatchRefs {
			if seenBatches[batchRef.Digest] {
				continue
			}
			seenBatches[batchRef.Digest] = true

			batch, err := s.batchStore.GetBatch(batchRef.Digest)
			if err == nil && batch != nil {
				batches = append(batches, batch)
			}
		}
	}

	resp := &SyncResponse{
		Certificates: certs,
		Batches:      batches,
		FromRound:    fromRound,
		ToRound:      toRound,
	}

	return s.network.SendSyncResponse(req.Requester, resp)
}

// HandleSyncResponse handles an incoming sync response.
func (s *SyncManager) HandleSyncResponse(resp *SyncResponse, from uint16) error {
	if !s.running.Load() {
		return types.ErrNotRunning
	}

	// Clear pending request
	s.pendingReqsMu.Lock()
	delete(s.pendingReqs, from)
	s.pendingReqsMu.Unlock()

	// Store batches first (certificates may reference them)
	for _, batch := range resp.Batches {
		if err := s.batchStore.SaveBatch(batch); err != nil {
			return err
		}
	}

	// Store certificates
	for _, cert := range resp.Certificates {
		if err := s.dag.AddCertificate(cert); err != nil {
			// Ignore duplicate errors
			if err != types.ErrDuplicateHeader {
				return err
			}
		}
	}

	// Notify callback
	if s.onSyncComplete != nil && len(resp.Certificates) > 0 {
		s.onSyncComplete(resp.FromRound, resp.ToRound)
	}

	return nil
}

// CatchUp synchronizes with peers until we reach the target round.
func (s *SyncManager) CatchUp(targetRound uint64) error {
	if !s.running.Load() {
		return types.ErrNotRunning
	}

	currentRound := s.dag.HighestRound()
	if currentRound >= targetRound {
		return nil // Already caught up
	}

	// Request sync from a random peer
	// In a real implementation, would try multiple peers
	validators := s.validatorSet.Validators()
	myID := s.network.ValidatorID()

	for _, v := range validators {
		if v.Index == myID {
			continue // Skip self
		}

		err := s.RequestSync(v.Index, currentRound+1, targetRound)
		if err == nil {
			return nil // Request sent, will handle response asynchronously
		}
	}

	return types.ErrValidatorNotFound
}

// syncLoop periodically checks for sync opportunities.
func (s *SyncManager) syncLoop() {
	defer close(s.stoppedCh)

	ticker := time.NewTicker(s.cfg.SyncInterval)
	defer ticker.Stop()

	timeoutTicker := time.NewTicker(s.cfg.SyncTimeout / 2)
	defer timeoutTicker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkAndSync()
		case <-timeoutTicker.C:
			s.cleanupTimedOutRequests()
		case <-s.stopCh:
			return
		}
	}
}

// checkAndSync checks if we need to sync and initiates if necessary.
func (s *SyncManager) checkAndSync() {
	// This would typically compare our round with peers
	// For now, just a placeholder
}

// cleanupTimedOutRequests removes timed out pending requests.
func (s *SyncManager) cleanupTimedOutRequests() {
	s.pendingReqsMu.Lock()
	defer s.pendingReqsMu.Unlock()

	cutoff := time.Now().Add(-s.cfg.SyncTimeout)
	for validator, req := range s.pendingReqs {
		if req.sentAt.Before(cutoff) {
			delete(s.pendingReqs, validator)
		}
	}
}

// handleMessages handles incoming sync messages.
func (s *SyncManager) handleMessages() {
	syncReqs := s.network.SyncRequests()
	syncResps := s.network.SyncResponses()

	for {
		select {
		case req := <-syncReqs:
			if req != nil {
				_ = s.HandleSyncRequest(req)
			}
		case resp := <-syncResps:
			if resp != nil {
				// Note: we don't have the sender in SyncResponse
				// In a real implementation, the message would include sender
				_ = s.handleSyncResponseInternal(resp)
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *SyncManager) handleSyncResponseInternal(resp *SyncResponse) error {
	// Store batches first
	for _, batch := range resp.Batches {
		if err := s.batchStore.SaveBatch(batch); err != nil {
			return err
		}
	}

	// Store certificates
	for _, cert := range resp.Certificates {
		if err := s.dag.AddCertificate(cert); err != nil {
			if err != types.ErrDuplicateHeader {
				return err
			}
		}
	}

	// Notify callback
	if s.onSyncComplete != nil && len(resp.Certificates) > 0 {
		s.onSyncComplete(resp.FromRound, resp.ToRound)
	}

	return nil
}

// GetPendingRequestCount returns the number of pending sync requests.
func (s *SyncManager) GetPendingRequestCount() int {
	s.pendingReqsMu.Lock()
	defer s.pendingReqsMu.Unlock()
	return len(s.pendingReqs)
}

// UpdateValidatorSet updates the validator set.
func (s *SyncManager) UpdateValidatorSet(vs types.ValidatorSet) {
	s.validatorSet = vs
}
