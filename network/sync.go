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
	// MaxRetries is the maximum number of retry attempts for failed sync requests.
	MaxRetries int
	// InitialBackoff is the initial backoff duration for retries.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration for retries.
	MaxBackoff time.Duration
}

// DefaultSyncConfig returns default sync configuration.
func DefaultSyncConfig() SyncConfig {
	return SyncConfig{
		SyncInterval:   10 * time.Second,
		SyncThreshold:  5,
		SyncBatchSize:  100,
		SyncTimeout:    30 * time.Second,
		MaxRetries:     5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
	}
}

// SyncManager handles synchronization of certificates between nodes.
type SyncManager struct {
	dag          *dag.DAG
	batchStore   store.BatchStore
	network      Network
	validatorSet types.ValidatorSet
	validatorMu  sync.RWMutex // Protects validatorSet
	cfg          SyncConfig

	// Pending sync requests
	pendingReqs   map[uint16]*pendingSyncRequest
	pendingReqsMu sync.Mutex

	// Peer-observed highest rounds. Updated whenever we see a sync response
	// from a peer (the response's ToRound is the peer's max). Used by
	// checkAndSync to detect that we're lagging and trigger CatchUp.
	peerHighest   map[uint16]uint64
	peerHighestMu sync.RWMutex

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
	wg        sync.WaitGroup // Tracks all goroutines

	// Callbacks
	onSyncComplete   func(fromRound, toRound uint64)
	onCertReceived   func(cert *types.Certificate)
}

type pendingSyncRequest struct {
	fromRound    uint64
	toRound      uint64
	sentAt       time.Time
	retryCount   int
	lastRetryAt  time.Time
	nextRetryAt  time.Time
	targetPeer   uint16
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
		peerHighest:  make(map[uint16]uint64),
		stopCh:       make(chan struct{}),
		stoppedCh:    make(chan struct{}),
	}
}

// SetSyncCompleteCallback sets the callback for sync completion.
func (s *SyncManager) SetSyncCompleteCallback(cb func(fromRound, toRound uint64)) {
	s.onSyncComplete = cb
}

// SetCertReceivedCallback wires a delivery callback for individual certs in
// a sync response. When set, HandleSyncResponse routes each verified cert
// through this callback INSTEAD of writing directly to the DAG. Looseberry
// uses this so that a synced cert lands in the certStore (via
// primary.HandleCertificate) and triggers tryAdvanceRound — without this
// callback, sync responses populate the DAG but the primary's currentRound
// never advances because tryAdvanceRound reads certStore, not DAG.
// See PLAN.md §E7c (v3-stuck residual race) for the motivating scenario.
func (s *SyncManager) SetCertReceivedCallback(cb func(cert *types.Certificate)) {
	s.onCertReceived = cb
}

// Start starts the sync manager.
func (s *SyncManager) Start() error {
	if s.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Reset channels for restart capability
	s.stopCh = make(chan struct{})
	s.stoppedCh = make(chan struct{})

	s.wg.Add(2)
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
	s.wg.Wait() // Wait for all goroutines to finish

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

	now := time.Now()
	s.pendingReqsMu.Lock()
	s.pendingReqs[validator] = &pendingSyncRequest{
		fromRound:   fromRound,
		toRound:     toRound,
		sentAt:      now,
		retryCount:  0,
		lastRetryAt: now,
		nextRetryAt: now.Add(s.cfg.InitialBackoff),
		targetPeer:  validator,
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

	// Record the peer's max round so checkAndSync can detect lag without
	// issuing further probes.
	s.recordPeerHighestRound(from, resp.ToRound)

	// Store batches first (certificates may reference them)
	for _, batch := range resp.Batches {
		if err := s.batchStore.SaveBatch(batch); err != nil {
			return err
		}
	}

	// Verify and store certificates
	s.validatorMu.RLock()
	vs := s.validatorSet
	s.validatorMu.RUnlock()

	for _, cert := range resp.Certificates {
		// Verify certificate before storing (security critical)
		if err := cert.Verify(vs); err != nil {
			// Skip this cert but continue with the rest — sync responses
			// from a single bad peer must not abort the whole catchup.
			continue
		}
		if s.onCertReceived != nil {
			// Routed delivery: the consumer (Looseberry) handles certStore
			// save, primary tryAdvanceRound, DAG add, and orphan buffering
			// — same path peer cert broadcasts take.
			s.onCertReceived(cert)
			continue
		}
		// Fallback when no callback is wired: legacy DAG-only behavior.
		// Don't abort the batch on ErrMissingParents — later certs in the
		// batch may include the missing parents, and dropped certs are
		// effectively lost (a future sync would re-fetch but only after a
		// SyncInterval delay).
		if err := s.dag.AddCertificate(cert); err != nil {
			if err != types.ErrDuplicateHeader {
				continue
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
	s.validatorMu.RLock()
	validators := s.validatorSet.Validators()
	s.validatorMu.RUnlock()
	myID := s.network.ValidatorID()

	// Same fromRound semantics as checkAndSync: when our DAG is empty
	// (HighestRound==0), we can't tell whether we already have round-0
	// certs or have none at all, so conservatively start from round 0.
	// Without this, a node with localRound=0 ALWAYS asks for round 1+
	// via CatchUp, and CatchUp picks the same peer every time (first
	// non-self in the validators slice) — so that peer never gets a
	// fromRound=0 request from us, and its round-0 cert never arrives.
	// PLAN §E7c residual race.
	fromRound := currentRound + 1
	if currentRound == 0 {
		fromRound = 0
	}

	for _, v := range validators {
		if v.Index == myID {
			continue // Skip self
		}

		err := s.RequestSync(v.Index, fromRound, targetRound)
		if err == nil {
			return nil // Request sent, will handle response asynchronously
		}
	}

	return types.ErrValidatorNotFound
}

// syncLoop periodically checks for sync opportunities.
func (s *SyncManager) syncLoop() {
	defer s.wg.Done()

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

// checkAndSync polls peers for their highest round and, if local lags by
// more than SyncThreshold rounds, triggers CatchUp(target).
//
// Replaces the previous placeholder (T1-5 / B3-5). Polling is best-effort:
//   - We dispatch a ToRound=0 probe to every peer that has no pending
//     request. The response updates peerHighest via HandleSyncResponse.
//   - On every tick we also consult the latest peerHighest map; if any peer
//     advertises a round ≥ local+threshold we kick off CatchUp to that peer
//     immediately. CatchUp itself sends the data-fetching request.
func (s *SyncManager) checkAndSync() {
	if !s.running.Load() {
		return
	}

	localRound := s.dag.HighestRound()
	threshold := s.cfg.SyncThreshold
	if threshold == 0 {
		threshold = 1
	}

	// 1) Look at peer rounds we already know. If anyone is significantly
	// ahead, catch up to the highest observed round.
	var target uint64
	s.peerHighestMu.RLock()
	for _, r := range s.peerHighest {
		if r > target {
			target = r
		}
	}
	s.peerHighestMu.RUnlock()

	if target > localRound && target-localRound >= threshold {
		// CatchUp picks a peer and issues the request internally.
		_ = s.CatchUp(target)
	}

	// 2) Probe peers that don't have an outstanding request, so peerHighest
	// stays fresh. Using ToRound=0 makes the responder fill in its highest
	// round on the response, which our HandleSyncResponse records.
	s.validatorMu.RLock()
	validators := s.validatorSet.Validators()
	s.validatorMu.RUnlock()

	myID := s.network.ValidatorID()
	for _, v := range validators {
		if v.Index == myID {
			continue
		}

		s.pendingReqsMu.Lock()
		_, busy := s.pendingReqs[v.Index]
		s.pendingReqsMu.Unlock()
		if busy {
			continue
		}

		// fromRound semantics: ask for everything strictly after what we
		// have. When localRound==0 we cannot tell whether that's "no certs
		// at all" or "only round-0 certs" from HighestRound alone — so
		// conservatively request from round 0. Duplicates on the responder
		// side are deduped by the receive path. Without this, a node that
		// missed every broadcast for round 0 receives rounds 1+ as orphans
		// and stays stuck forever (PLAN §E7c).
		fromRound := localRound + 1
		if localRound == 0 {
			fromRound = 0
		}
		_ = s.RequestSync(v.Index, fromRound, 0)
	}
}

// RequestPeerCatchUp sends a from-round-0 sync request to a single peer.
// Intended to be called immediately after a peer is registered, so a node
// that came up late doesn't have to wait the full SyncInterval to discover
// it's missing the foundational round-0 certs. Safe to call on an
// already-caught-up node — the peer dedupes and the response is small.
func (s *SyncManager) RequestPeerCatchUp(peerIdx uint16) error {
	if !s.running.Load() {
		return types.ErrNotRunning
	}
	s.pendingReqsMu.Lock()
	_, busy := s.pendingReqs[peerIdx]
	s.pendingReqsMu.Unlock()
	if busy {
		return nil
	}
	return s.RequestSync(peerIdx, 0, 0)
}

// recordPeerHighestRound updates our local view of a peer's max round. The
// stored value is monotonically non-decreasing per peer.
func (s *SyncManager) recordPeerHighestRound(peer uint16, round uint64) {
	s.peerHighestMu.Lock()
	defer s.peerHighestMu.Unlock()
	if cur, ok := s.peerHighest[peer]; !ok || round > cur {
		s.peerHighest[peer] = round
	}
}

// PeerHighestRound returns the most recent observed highest round for the
// given peer (zero if never seen). Exposed for tests.
func (s *SyncManager) PeerHighestRound(peer uint16) uint64 {
	s.peerHighestMu.RLock()
	defer s.peerHighestMu.RUnlock()
	return s.peerHighest[peer]
}

// SyncDiagnostics captures sync-manager state for debugging stuck-validator
// scenarios (PLAN §E7c). All fields are point-in-time snapshots.
type SyncDiagnostics struct {
	// PeerHighest is a copy of the peer-observed highest-round map. A
	// non-zero value here means we received a sync response from that
	// peer at some point. Zero either means we never asked or we asked
	// and the response was lost.
	PeerHighest map[uint16]uint64
	// PendingReqs maps validator → retry count for any in-flight sync
	// requests. A long-pending request indicates the response never
	// came back (peer didn't respond, or the response was dropped).
	PendingReqs map[uint16]int
}

// Diagnostics returns a snapshot of sync state useful for diagnosing
// stuck-validator hard-stalls (PLAN §E7c).
func (s *SyncManager) Diagnostics() SyncDiagnostics {
	d := SyncDiagnostics{
		PeerHighest: make(map[uint16]uint64),
		PendingReqs: make(map[uint16]int),
	}
	s.peerHighestMu.RLock()
	for k, v := range s.peerHighest {
		d.PeerHighest[k] = v
	}
	s.peerHighestMu.RUnlock()
	s.pendingReqsMu.Lock()
	for k, v := range s.pendingReqs {
		d.PendingReqs[k] = v.retryCount
	}
	s.pendingReqsMu.Unlock()
	return d
}

// CheckAndSync is the externally-callable form of checkAndSync (for tests).
func (s *SyncManager) CheckAndSync() {
	s.checkAndSync()
}

// cleanupTimedOutRequests handles timed out pending requests with retry logic.
func (s *SyncManager) cleanupTimedOutRequests() {
	now := time.Now()

	s.pendingReqsMu.Lock()
	var toRetry []*pendingSyncRequest
	var toRemove []uint16

	for validator, req := range s.pendingReqs {
		// Check if request has timed out
		if now.After(req.nextRetryAt) {
			if req.retryCount >= s.cfg.MaxRetries {
				// Max retries reached, remove the request
				toRemove = append(toRemove, validator)
			} else {
				// Schedule retry with exponential backoff
				toRetry = append(toRetry, req)
			}
		}
	}

	// Remove failed requests
	for _, validator := range toRemove {
		delete(s.pendingReqs, validator)
	}

	// Update retry counts and schedule for retrying requests
	for _, req := range toRetry {
		req.retryCount++
		req.lastRetryAt = now

		// Calculate exponential backoff: initialBackoff * 2^retryCount, capped at maxBackoff
		backoff := s.cfg.InitialBackoff
		for i := 0; i < req.retryCount && backoff < s.cfg.MaxBackoff; i++ {
			backoff *= 2
			if backoff > s.cfg.MaxBackoff {
				backoff = s.cfg.MaxBackoff
			}
		}
		req.nextRetryAt = now.Add(backoff)
	}

	s.pendingReqsMu.Unlock()

	// Retry requests outside the lock
	for _, req := range toRetry {
		s.retrySyncRequest(req)
	}
}

// retrySyncRequest retries a sync request, potentially to a different peer.
func (s *SyncManager) retrySyncRequest(req *pendingSyncRequest) {
	if !s.running.Load() {
		return
	}

	// Try to find a different peer
	s.validatorMu.RLock()
	validators := s.validatorSet.Validators()
	s.validatorMu.RUnlock()

	myID := s.network.ValidatorID()
	targetPeer := req.targetPeer

	// Try a different peer on each retry (round-robin through validators)
	if len(validators) > 1 {
		for _, v := range validators {
			if v.Index != myID && v.Index != req.targetPeer {
				targetPeer = v.Index
				break
			}
		}
	}

	// Update target peer in the request
	s.pendingReqsMu.Lock()
	if existing, ok := s.pendingReqs[req.targetPeer]; ok && existing == req {
		// Move to new peer
		delete(s.pendingReqs, req.targetPeer)
		req.targetPeer = targetPeer
		s.pendingReqs[targetPeer] = req
	}
	s.pendingReqsMu.Unlock()

	// Send retry request
	syncReq := &SyncRequest{
		FromRound: req.fromRound,
		ToRound:   req.toRound,
		Requester: s.network.ValidatorID(),
	}
	_ = s.network.SendSyncRequest(targetPeer, syncReq)
}

// GetRetryCount returns the retry count for a pending request to a validator.
func (s *SyncManager) GetRetryCount(validator uint16) int {
	s.pendingReqsMu.Lock()
	defer s.pendingReqsMu.Unlock()

	if req, ok := s.pendingReqs[validator]; ok {
		return req.retryCount
	}
	return -1 // Not found
}

// handleMessages handles incoming sync messages.
func (s *SyncManager) handleMessages() {
	defer s.wg.Done()

	syncReqs := s.network.SyncRequests()
	syncResps := s.network.SyncResponses()

	for {
		select {
		case req := <-syncReqs:
			if req != nil {
				_ = s.HandleSyncRequest(req)
			}
		case msg := <-syncResps:
			if msg != nil && msg.Response != nil {
				_ = s.HandleSyncResponse(msg.Response, msg.From)
			}
		case <-s.stopCh:
			return
		}
	}
}

// GetPendingRequestCount returns the number of pending sync requests.
func (s *SyncManager) GetPendingRequestCount() int {
	s.pendingReqsMu.Lock()
	defer s.pendingReqsMu.Unlock()
	return len(s.pendingReqs)
}

// UpdateValidatorSet updates the validator set.
func (s *SyncManager) UpdateValidatorSet(vs types.ValidatorSet) {
	s.validatorMu.Lock()
	defer s.validatorMu.Unlock()
	s.validatorSet = vs
}
