package primary

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Config contains primary configuration.
type Config struct {
	// HeaderTimeout is the maximum time to wait before creating a header.
	HeaderTimeout time.Duration
	// MaxBatchesPerHeader is the maximum number of batch refs per header.
	MaxBatchesPerHeader int
	// VoteTimeout is the timeout for vote collection.
	VoteTimeout time.Duration
	// MaxRoundGap is the maximum allowed round gap for headers.
	MaxRoundGap uint64
	// AllowEmptyHeaders allows creating headers with no batches for liveness.
	AllowEmptyHeaders bool
}

// DefaultConfig returns default primary configuration.
func DefaultConfig() Config {
	return Config{
		HeaderTimeout:       500 * time.Millisecond,
		MaxBatchesPerHeader: 100,
		VoteTimeout:         30 * time.Second,
		MaxRoundGap:         10,
		AllowEmptyHeaders:   true,
	}
}

// CertificateCallback is called when a certificate is formed.
type CertificateCallback func(cert *types.Certificate)

// HeaderCallback is called when a header is created.
type HeaderCallback func(header *types.Header)

// VoteCallback is called when a vote should be sent.
type VoteCallback func(vote *types.Vote, to uint16)

// BatchAvailabilityChecker is the subset of *BatchFetcher that Primary needs
// in order to ask "are all batches for this header available, and if not,
// please fetch them and call me back when they are". An interface keeps the
// dependency one-way (primary doesn't import a concrete batch fetcher).
type BatchAvailabilityChecker interface {
	// RequestBatchesForHeader returns true if every batch referenced by the
	// header is already present locally. If it returns false the checker
	// MUST schedule a fetch and, on success, invoke its HeaderReady callback
	// with the original header.
	RequestBatchesForHeader(header *types.Header) bool
}

// AckQuorumChecker reports whether a batch has been acknowledged by 2f+1
// validators. Used by Primary.tryCreateHeader to gate inclusion of a batch
// digest behind data-availability (T1-2 / B3-2).
type AckQuorumChecker interface {
	HasBatchAckQuorum(digest types.Hash) bool
}

// pendingVoteEntry tracks buffered votes with their creation time for cleanup.
type pendingVoteEntry struct {
	votes     []types.Vote
	createdAt time.Time
}

// Primary handles header creation, voting, and certificate formation.
type Primary struct {
	validatorID uint16
	signer      types.Signer
	cfg         Config

	// Current state
	currentRound atomic.Uint64
	epoch        atomic.Uint64

	// Batch digests waiting to be included in next header
	batchDigests []types.BatchDigest
	digestsMu    sync.Mutex

	// Vote tracking
	voteTracker *VoteTracker

	// Pending votes for unknown headers (with timestamps for cleanup)
	pendingVotes   map[types.Hash]*pendingVoteEntry
	pendingMu      sync.Mutex
	maxPendingAge  time.Duration // Max age before cleanup (default: 2 * VoteTimeout)

	// Storage
	certStore  store.CertificateStore
	batchStore store.BatchStore

	// Validator set
	validatorSet types.ValidatorSet
	validatorMu  sync.RWMutex // Protects validatorSet

	// Callbacks
	certCallback   CertificateCallback
	headerCallback HeaderCallback
	voteCallback   VoteCallback

	// BatchFetcher is consulted when a header references a batch we don't
	// have locally; instead of silently skipping the vote (the historical
	// T1-3 behaviour) the primary now hands the header to the fetcher and
	// expects it to call us back via the HeaderReadyCallback once every
	// referenced batch has arrived. Nil means: fall back to the legacy
	// skip-when-missing behaviour (used by tests that don't wire a fetcher).
	batchFetcher BatchAvailabilityChecker

	// AckTracker is the worker pool's ack tracker, consulted before
	// including a batch digest in a new header — see B3-2.
	ackQuorumChecker AckQuorumChecker

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// New creates a new Primary.
func New(
	validatorID uint16,
	signer types.Signer,
	cfg Config,
	certStore store.CertificateStore,
	batchStore store.BatchStore,
	validatorSet types.ValidatorSet,
) *Primary {
	return &Primary{
		validatorID:   validatorID,
		signer:        signer,
		cfg:           cfg,
		batchDigests:  make([]types.BatchDigest, 0),
		voteTracker:   NewVoteTracker(cfg.VoteTimeout),
		pendingVotes:  make(map[types.Hash]*pendingVoteEntry),
		maxPendingAge: 2 * cfg.VoteTimeout,
		certStore:     certStore,
		batchStore:    batchStore,
		validatorSet:  validatorSet,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

// SetCertificateCallback sets the callback for formed certificates.
func (p *Primary) SetCertificateCallback(cb CertificateCallback) {
	p.certCallback = cb
}

// SetHeaderCallback sets the callback for created headers.
func (p *Primary) SetHeaderCallback(cb HeaderCallback) {
	p.headerCallback = cb
}

// SetVoteCallback sets the callback for sending votes.
func (p *Primary) SetVoteCallback(cb VoteCallback) {
	p.voteCallback = cb
}

// SetBatchFetcher installs a checker used by HandleHeader to request missing
// batches instead of silently skipping the vote. The checker is expected to
// invoke HandleHeader (or the equivalent) again once every batch has arrived.
func (p *Primary) SetBatchFetcher(bf BatchAvailabilityChecker) {
	p.batchFetcher = bf
}

// SetAckQuorumChecker installs the data-availability gate used by
// tryCreateHeader: a batch digest is only included in the next header once
// 2f+1 validators have acknowledged it.
func (p *Primary) SetAckQuorumChecker(c AckQuorumChecker) {
	p.ackQuorumChecker = c
}

// Start starts the primary's header creation loop.
func (p *Primary) Start() error {
	if p.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Reset channels for restart capability
	p.stopCh = make(chan struct{})
	p.stoppedCh = make(chan struct{})

	go p.headerLoop()
	return nil
}

// Stop stops the primary gracefully.
func (p *Primary) Stop() error {
	if !p.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(p.stopCh)
	<-p.stoppedCh

	p.voteTracker.Close()
	return nil
}

// IsRunning returns true if the primary is running.
func (p *Primary) IsRunning() bool {
	return p.running.Load()
}

// ValidatorID returns the validator ID.
func (p *Primary) ValidatorID() uint16 {
	return p.validatorID
}

// SetRound updates the current round.
func (p *Primary) SetRound(round uint64) {
	p.currentRound.Store(round)
}

// Round returns the current round.
func (p *Primary) Round() uint64 {
	return p.currentRound.Load()
}

// SetEpoch updates the current epoch.
func (p *Primary) SetEpoch(epoch uint64) {
	p.epoch.Store(epoch)
}

// Epoch returns the current epoch.
func (p *Primary) Epoch() uint64 {
	return p.epoch.Load()
}

// UpdateValidatorSet updates the validator set.
func (p *Primary) UpdateValidatorSet(vs types.ValidatorSet) {
	p.validatorMu.Lock()
	defer p.validatorMu.Unlock()
	p.validatorSet = vs
}

// AddBatchDigest adds a batch digest to be included in the next header.
func (p *Primary) AddBatchDigest(digest types.BatchDigest) {
	p.digestsMu.Lock()
	defer p.digestsMu.Unlock()

	p.batchDigests = append(p.batchDigests, digest)
}

// GetVoteTracker returns the vote tracker (for testing).
func (p *Primary) GetVoteTracker() *VoteTracker {
	return p.voteTracker
}

// PendingHeadersOlderThan returns headers locally authored by this
// primary that are still awaiting quorum votes AND are at least minAge
// old. Used by the looseberry stuck-detection rebroadcast loop
// (PLAN §E7c) to retransmit headers whose votes may have been dropped
// on the wire — the cluster's hard-stall mode at 100K+ shows batches
// reaching quorum but headers not getting certified.
func (p *Primary) PendingHeadersOlderThan(minAge time.Duration) []*types.Header {
	if p.voteTracker == nil {
		return nil
	}
	return p.voteTracker.PendingHeadersOlderThan(minAge)
}

// HandleHeader processes a received header from another validator.
// Returns error if header is invalid.
//
// When a referenced batch is missing locally, the header is handed to the
// installed BatchFetcher (if any). The fetcher requests the batch from
// peers and re-invokes HandleHeader once all batches are available — at
// which point the second pass falls through to vote-and-send. Without a
// fetcher (test harness) we fall back to the legacy skip-vote behaviour.
func (p *Primary) HandleHeader(header *types.Header) error {
	if !p.running.Load() {
		return types.ErrNotRunning
	}

	// Validate header
	if err := p.validateHeader(header); err != nil {
		return err
	}

	// Check batch availability. If anything is missing, prefer to schedule
	// fetches via the BatchFetcher rather than dropping the vote silently.
	if p.batchFetcher != nil {
		if !p.batchFetcher.RequestBatchesForHeader(header) {
			// Fetch in progress; the fetcher's HeaderReady callback will
			// route the header back here once every batch has arrived.
			return nil
		}
	} else {
		for _, batchRef := range header.BatchRefs {
			if !p.batchStore.HasBatch(batchRef.Digest) {
				// No fetcher installed — preserve historical behaviour.
				return nil
			}
		}
	}

	// Create and send vote
	vote := types.NewVote(header.Digest, p.validatorID)
	if err := vote.Sign(p.signer); err != nil {
		return err
	}

	// Send vote to header author
	if p.voteCallback != nil {
		p.voteCallback(vote, header.Author)
	}

	return nil
}

// HandleVote processes a received vote.
// Returns (certificate, true) if quorum reached.
func (p *Primary) HandleVote(vote *types.Vote) (*types.Certificate, bool) {
	if vote == nil {
		return nil, false
	}

	if !p.running.Load() {
		return nil, false
	}

	// Validate vote signature
	p.validatorMu.RLock()
	validator := p.validatorSet.GetByIndex(vote.Validator)
	p.validatorMu.RUnlock()
	if validator == nil {
		return nil, false
	}
	if !vote.Verify(validator.PublicKey) {
		return nil, false
	}

	// Check if we have the header
	if !p.voteTracker.HasHeader(vote.HeaderDigest) {
		// Buffer vote for later
		p.pendingMu.Lock()
		entry := p.pendingVotes[vote.HeaderDigest]
		if entry == nil {
			entry = &pendingVoteEntry{
				votes:     make([]types.Vote, 0),
				createdAt: time.Now(),
			}
			p.pendingVotes[vote.HeaderDigest] = entry
		}
		entry.votes = append(entry.votes, *vote)
		p.pendingMu.Unlock()
		return nil, false
	}

	// Record vote
	p.validatorMu.RLock()
	quorum := p.validatorSet.Quorum()
	p.validatorMu.RUnlock()
	cert, formed := p.voteTracker.RecordVote(vote, quorum)
	if formed {
		// Store certificate
		if p.certStore != nil {
			_ = p.certStore.SaveCertificate(cert)
		}

		// Remove from tracking
		p.voteTracker.RemoveHeader(vote.HeaderDigest)

		// Notify callback
		if p.certCallback != nil {
			p.certCallback(cert)
		}

		return cert, true
	}

	return nil, false
}

// HandleCertificate processes a received certificate.
func (p *Primary) HandleCertificate(cert *types.Certificate) error {
	if !p.running.Load() {
		return types.ErrNotRunning
	}

	// Verify certificate
	p.validatorMu.RLock()
	vs := p.validatorSet
	p.validatorMu.RUnlock()
	if err := cert.Verify(vs); err != nil {
		return err
	}

	// Store certificate
	if p.certStore != nil {
		if err := p.certStore.SaveCertificate(cert); err != nil {
			return err
		}
	}

	// Try to advance round
	p.tryAdvanceRound()

	return nil
}

// TryAdvanceRound checks if we can advance to the next round.
func (p *Primary) TryAdvanceRound() bool {
	return p.tryAdvanceRound()
}

// headerLoop is the main loop that creates headers.
func (p *Primary) headerLoop() {
	defer close(p.stoppedCh)

	ticker := time.NewTicker(p.cfg.HeaderTimeout)
	defer ticker.Stop()

	// Cleanup ticker runs at 2x the cleanup interval
	cleanupTicker := time.NewTicker(p.maxPendingAge)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ticker.C:
			p.tryCreateHeader()
		case <-cleanupTicker.C:
			p.cleanupPendingVotes()
		case <-p.stopCh:
			return
		}
	}
}

// tryAdvanceRound checks if we can advance to the next round.
func (p *Primary) tryAdvanceRound() bool {
	currentRound := p.currentRound.Load()

	// Get certificates for current round
	certs, err := p.certStore.GetCertificatesByRound(currentRound)
	if err != nil {
		return false
	}

	// Need quorum to advance
	p.validatorMu.RLock()
	quorum := p.validatorSet.Quorum()
	p.validatorMu.RUnlock()
	if len(certs) >= quorum {
		p.currentRound.Store(currentRound + 1)
		return true
	}

	return false
}

// tryCreateHeader attempts to create a header.
//
// Per B3-2 (T1-2), a batch digest is only included once an external
// AckQuorumChecker reports 2f+1 acks for it. Pending digests that have
// not yet reached quorum are left in p.batchDigests for a future tick.
// Digests are kept in FIFO order so older batches are preferred.
//
// We deliberately release p.digestsMu before calling HasBatchAckQuorum —
// the checker may take its own locks (e.g. worker.Pool.workersMu) and
// holding digestsMu across an external call would invert the lock order
// elsewhere in the system.
func (p *Primary) tryCreateHeader() {
	if !p.running.Load() {
		return
	}

	// Snapshot the queue. We'll re-acquire the lock later to merge the
	// remainder back in.
	p.digestsMu.Lock()
	if len(p.batchDigests) == 0 && !p.cfg.AllowEmptyHeaders {
		p.digestsMu.Unlock()
		return
	}
	snapshot := make([]types.BatchDigest, len(p.batchDigests))
	copy(snapshot, p.batchDigests)
	p.batchDigests = p.batchDigests[:0]
	p.digestsMu.Unlock()

	var digests []types.BatchDigest
	if len(snapshot) > 0 {
		// Walk the queue once, splitting into "ready" (quorum acked) and
		// "still waiting".
		remaining := make([]types.BatchDigest, 0, len(snapshot))
		ready := make([]types.BatchDigest, 0, p.cfg.MaxBatchesPerHeader)

		for _, d := range snapshot {
			if len(ready) >= p.cfg.MaxBatchesPerHeader {
				remaining = append(remaining, d)
				continue
			}
			if p.ackQuorumChecker == nil || p.ackQuorumChecker.HasBatchAckQuorum(d.Digest) {
				ready = append(ready, d)
				continue
			}
			remaining = append(remaining, d)
		}

		digests = ready

		// Restore the still-waiting digests. They go at the front of any
		// newer digests that may have arrived while we were classifying.
		if len(remaining) > 0 {
			p.digestsMu.Lock()
			p.batchDigests = append(remaining, p.batchDigests...)
			p.digestsMu.Unlock()
		}
	}

	// Build header
	round := p.currentRound.Load()
	parents := p.selectParents(round)

	// For round 0 or when we don't have enough parents, skip unless we have batches
	// Empty headers for liveness still need valid parent references (except for round 0)
	if round > 0 && len(parents) == 0 && len(digests) == 0 {
		return
	}

	header := types.NewHeader(p.validatorID, round, p.epoch.Load(), digests, parents)
	if err := header.Sign(p.signer); err != nil {
		return
	}

	// Track header for votes
	p.voteTracker.TrackHeader(header)

	// Self-vote: every validator votes on its own headers
	selfVote := types.NewVote(header.Digest, p.validatorID)
	if err := selfVote.Sign(p.signer); err == nil {
		p.validatorMu.RLock()
		quorum := p.validatorSet.Quorum()
		p.validatorMu.RUnlock()

		cert, formed := p.voteTracker.RecordVote(selfVote, quorum)
		if formed {
			if p.certStore != nil {
				_ = p.certStore.SaveCertificate(cert)
			}
			p.voteTracker.RemoveHeader(header.Digest)
			if p.certCallback != nil {
				p.certCallback(cert)
			}
			// Advance round after self-certification so the next header
			// moves to the next round (critical for single-validator setups)
			p.tryAdvanceRound()
		}
	}

	// Process any pending votes (from other validators that voted early)
	p.processPendingVotes(header.Digest)

	// Notify callback (broadcast header to other validators)
	if p.headerCallback != nil {
		p.headerCallback(header)
	}
}

// selectParents selects parent certificates for a header.
func (p *Primary) selectParents(round uint64) []types.CertificateRef {
	if round == 0 {
		return nil
	}

	// Get certificates from previous round
	certs, err := p.certStore.GetCertificatesByRound(round - 1)
	if err != nil || len(certs) == 0 {
		return nil
	}

	// Sort by validator index for determinism
	sort.Slice(certs, func(i, j int) bool {
		return certs[i].Author() < certs[j].Author()
	})

	// Take up to quorum
	p.validatorMu.RLock()
	quorum := p.validatorSet.Quorum()
	p.validatorMu.RUnlock()
	if len(certs) > quorum {
		certs = certs[:quorum]
	}

	// Convert to refs
	refs := make([]types.CertificateRef, len(certs))
	for i, c := range certs {
		refs[i] = c.GetRef()
	}

	return refs
}

// processPendingVotes processes any buffered votes for a header.
func (p *Primary) processPendingVotes(headerDigest types.Hash) {
	p.pendingMu.Lock()
	entry, exists := p.pendingVotes[headerDigest]
	if exists {
		delete(p.pendingVotes, headerDigest)
	}
	p.pendingMu.Unlock()

	if !exists || entry == nil {
		return
	}

	p.validatorMu.RLock()
	quorum := p.validatorSet.Quorum()
	p.validatorMu.RUnlock()
	for _, vote := range entry.votes {
		cert, formed := p.voteTracker.RecordVote(&vote, quorum)
		if formed {
			// Store certificate
			if p.certStore != nil {
				_ = p.certStore.SaveCertificate(cert)
			}

			// Remove from tracking
			p.voteTracker.RemoveHeader(headerDigest)

			// Notify callback
			if p.certCallback != nil {
				p.certCallback(cert)
			}

			return
		}
	}
}

// cleanupPendingVotes removes old pending vote entries to prevent memory leaks.
func (p *Primary) cleanupPendingVotes() {
	// Clean up pending votes (votes received before their headers)
	p.pendingMu.Lock()
	cutoff := time.Now().Add(-p.maxPendingAge)
	for digest, entry := range p.pendingVotes {
		if entry.createdAt.Before(cutoff) {
			delete(p.pendingVotes, digest)
		}
	}
	p.pendingMu.Unlock()

	// Clean up timed-out headers in the vote tracker
	// This prevents memory leaks from headers that never reached quorum
	p.voteTracker.RemoveTimedOut()
}

// validateHeader validates a header.
func (p *Primary) validateHeader(header *types.Header) error {
	// Check round is within acceptable range
	currentRound := p.currentRound.Load()
	if header.Round < currentRound {
		return types.ErrRoundMismatch
	}
	if header.Round > currentRound+p.cfg.MaxRoundGap {
		return types.ErrRoundMismatch
	}

	// Check epoch
	if header.Epoch != p.epoch.Load() {
		return types.ErrEpochMismatch
	}

	// Verify author signature
	p.validatorMu.RLock()
	author := p.validatorSet.GetByIndex(header.Author)
	quorum := p.validatorSet.Quorum()
	p.validatorMu.RUnlock()
	if author == nil {
		return types.ErrValidatorNotFound
	}
	if !header.Verify(author.PublicKey) {
		return types.ErrInvalidSignature
	}

	// For round > 0, verify parent count (should have quorum)
	if header.Round > 0 {
		if len(header.Parents) < quorum {
			return types.ErrMissingParents
		}

		// Verify we have all parent certificates
		for _, parent := range header.Parents {
			if !p.certStore.HasCertificate(parent.Digest) {
				return types.ErrMissingParents
			}
		}
	}

	return nil
}
