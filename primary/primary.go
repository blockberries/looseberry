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

	// Pending votes for unknown headers
	pendingVotes map[types.Hash][]types.Vote
	pendingMu    sync.Mutex

	// Storage
	certStore  store.CertificateStore
	batchStore store.BatchStore

	// Validator set
	validatorSet types.ValidatorSet

	// Callbacks
	certCallback   CertificateCallback
	headerCallback HeaderCallback
	voteCallback   VoteCallback

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
		validatorID:  validatorID,
		signer:       signer,
		cfg:          cfg,
		batchDigests: make([]types.BatchDigest, 0),
		voteTracker:  NewVoteTracker(cfg.VoteTimeout),
		pendingVotes: make(map[types.Hash][]types.Vote),
		certStore:    certStore,
		batchStore:   batchStore,
		validatorSet: validatorSet,
		stopCh:       make(chan struct{}),
		stoppedCh:    make(chan struct{}),
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

// HandleHeader processes a received header from another validator.
// Returns error if header is invalid.
func (p *Primary) HandleHeader(header *types.Header) error {
	if !p.running.Load() {
		return types.ErrNotRunning
	}

	// Validate header
	if err := p.validateHeader(header); err != nil {
		return err
	}

	// Check batch availability (simplified - just check store)
	for _, batchRef := range header.BatchRefs {
		if !p.batchStore.HasBatch(batchRef.Digest) {
			// In full implementation, would request missing batches
			// For now, skip voting on headers with missing batches
			return nil
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
	if !p.running.Load() {
		return nil, false
	}

	// Validate vote signature
	validator := p.validatorSet.GetByIndex(vote.Validator)
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
		p.pendingVotes[vote.HeaderDigest] = append(
			p.pendingVotes[vote.HeaderDigest], *vote)
		p.pendingMu.Unlock()
		return nil, false
	}

	// Record vote
	quorum := p.validatorSet.Quorum()
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
	if err := cert.Verify(p.validatorSet); err != nil {
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

	for {
		select {
		case <-ticker.C:
			p.tryCreateHeader()
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
	if len(certs) >= p.validatorSet.Quorum() {
		p.currentRound.Store(currentRound + 1)
		return true
	}

	return false
}

// tryCreateHeader attempts to create a header.
func (p *Primary) tryCreateHeader() {
	if !p.running.Load() {
		return
	}

	// Get batch digests
	p.digestsMu.Lock()
	if len(p.batchDigests) == 0 && !p.cfg.AllowEmptyHeaders {
		p.digestsMu.Unlock()
		return
	}

	// Take up to MaxBatchesPerHeader
	var digests []types.BatchDigest
	if len(p.batchDigests) > 0 {
		count := min(len(p.batchDigests), p.cfg.MaxBatchesPerHeader)
		digests = make([]types.BatchDigest, count)
		copy(digests, p.batchDigests[:count])
		p.batchDigests = p.batchDigests[count:]
	}
	p.digestsMu.Unlock()

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

	// Process any pending votes
	p.processPendingVotes(header.Digest)

	// Notify callback
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
	quorum := p.validatorSet.Quorum()
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
	votes, exists := p.pendingVotes[headerDigest]
	if exists {
		delete(p.pendingVotes, headerDigest)
	}
	p.pendingMu.Unlock()

	if !exists {
		return
	}

	quorum := p.validatorSet.Quorum()
	for _, vote := range votes {
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
	author := p.validatorSet.GetByIndex(header.Author)
	if author == nil {
		return types.ErrValidatorNotFound
	}
	if !header.Verify(author.PublicKey) {
		return types.ErrInvalidSignature
	}

	// For round > 0, verify parent count (should have quorum)
	if header.Round > 0 {
		if len(header.Parents) < p.validatorSet.Quorum() {
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
