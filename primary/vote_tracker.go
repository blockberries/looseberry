package primary

import (
	"sync"
	"time"

	"github.com/blockberries/looseberry/types"
)

// DoubleVoteEvidence records evidence of a validator sending conflicting votes.
type DoubleVoteEvidence struct {
	ValidatorID uint16
	HeaderID    types.Hash
	Vote1       *types.Vote
	Vote2       *types.Vote
	Timestamp   time.Time
}

// PendingHeader tracks a header waiting for votes.
type PendingHeader struct {
	Header    *types.Header
	Votes     map[uint16]*types.Vote // validator -> vote
	CreatedAt time.Time
}

// VoteTracker tracks pending headers and their votes.
// Thread-safe with RWMutex.
type VoteTracker struct {
	pending map[types.Hash]*PendingHeader
	mu      sync.RWMutex
	timeout time.Duration
	closed  bool

	// Double vote detection: tracks all votes per validator across all headers
	// Key: validatorID, Value: map of headerDigest -> vote
	validatorVotes map[uint16]map[types.Hash]*types.Vote
	doubleVotes    []DoubleVoteEvidence
}

// NewVoteTracker creates a new vote tracker.
func NewVoteTracker(timeout time.Duration) *VoteTracker {
	return &VoteTracker{
		pending:        make(map[types.Hash]*PendingHeader),
		timeout:        timeout,
		validatorVotes: make(map[uint16]map[types.Hash]*types.Vote),
		doubleVotes:    make([]DoubleVoteEvidence, 0),
	}
}

// TrackHeader adds a header to the tracker.
func (vt *VoteTracker) TrackHeader(header *types.Header) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	if vt.closed {
		return
	}

	// Don't track if already tracked
	if _, exists := vt.pending[header.Digest]; exists {
		return
	}

	vt.pending[header.Digest] = &PendingHeader{
		Header:    header.Clone(),
		Votes:     make(map[uint16]*types.Vote),
		CreatedAt: time.Now(),
	}
}

// HasHeader returns true if the header is being tracked.
func (vt *VoteTracker) HasHeader(digest types.Hash) bool {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	_, exists := vt.pending[digest]
	return exists
}

// GetHeader returns a tracked header if it exists.
func (vt *VoteTracker) GetHeader(digest types.Hash) (*types.Header, bool) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	pending, exists := vt.pending[digest]
	if !exists {
		return nil, false
	}

	return pending.Header.Clone(), true
}

// RecordVote records a vote for a header.
// Returns (certificate, true) if quorum is reached with this vote.
// Returns (nil, false) if header not tracked or quorum not reached.
// Detects and records double voting (same validator voting for conflicting headers).
func (vt *VoteTracker) RecordVote(vote *types.Vote, quorum int) (*types.Certificate, bool) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	if vt.closed {
		return nil, false
	}

	pending, exists := vt.pending[vote.HeaderDigest]
	if !exists {
		return nil, false
	}

	// Check for double voting (Byzantine behavior)
	if existingVotes, hasValidator := vt.validatorVotes[vote.Validator]; hasValidator {
		if existingVote, hasHeader := existingVotes[vote.HeaderDigest]; hasHeader {
			// Vote for same header already exists
			// Check if it's a conflicting vote (different signature could mean different content)
			if !existingVote.Signature.Equal(vote.Signature) {
				// Record double vote evidence
				vt.doubleVotes = append(vt.doubleVotes, DoubleVoteEvidence{
					ValidatorID: vote.Validator,
					HeaderID:    vote.HeaderDigest,
					Vote1:       existingVote.Clone(),
					Vote2:       vote.Clone(),
					Timestamp:   time.Now(),
				})
			}
			// Already have vote from this validator for this header
			if len(pending.Votes) >= quorum {
				return vt.formCertificateLocked(pending, quorum), true
			}
			return nil, false
		}
	} else {
		// Initialize validator's vote map
		vt.validatorVotes[vote.Validator] = make(map[types.Hash]*types.Vote)
	}

	// Don't double-count in pending
	if _, hasVote := pending.Votes[vote.Validator]; hasVote {
		// Already have vote, check if we have quorum
		if len(pending.Votes) >= quorum {
			return vt.formCertificateLocked(pending, quorum), true
		}
		return nil, false
	}

	// Add vote
	clonedVote := vote.Clone()
	pending.Votes[vote.Validator] = clonedVote
	vt.validatorVotes[vote.Validator][vote.HeaderDigest] = clonedVote

	// Check quorum
	if len(pending.Votes) >= quorum {
		return vt.formCertificateLocked(pending, quorum), true
	}

	return nil, false
}

// VoteCount returns the number of votes for a header.
func (vt *VoteTracker) VoteCount(digest types.Hash) int {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	pending, exists := vt.pending[digest]
	if !exists {
		return 0
	}

	return len(pending.Votes)
}

// RemoveHeader removes a header from tracking.
func (vt *VoteTracker) RemoveHeader(digest types.Hash) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// Clean up validator votes for this header
	for validatorID, votes := range vt.validatorVotes {
		delete(votes, digest)
		// If validator has no more votes, remove the entry
		if len(votes) == 0 {
			delete(vt.validatorVotes, validatorID)
		}
	}

	delete(vt.pending, digest)
}

// GetPendingCount returns the number of pending headers.
func (vt *VoteTracker) GetPendingCount() int {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	return len(vt.pending)
}

// GetTimedOut returns all headers that have timed out.
func (vt *VoteTracker) GetTimedOut() []*types.Header {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	var timedOut []*types.Header
	cutoff := time.Now().Add(-vt.timeout)

	for _, pending := range vt.pending {
		if pending.CreatedAt.Before(cutoff) {
			timedOut = append(timedOut, pending.Header.Clone())
		}
	}

	return timedOut
}

// RemoveTimedOut removes all headers that have timed out.
func (vt *VoteTracker) RemoveTimedOut() []*types.Header {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	var timedOut []*types.Header
	cutoff := time.Now().Add(-vt.timeout)

	for digest, pending := range vt.pending {
		if pending.CreatedAt.Before(cutoff) {
			timedOut = append(timedOut, pending.Header.Clone())
			delete(vt.pending, digest)
		}
	}

	return timedOut
}

// Close closes the tracker.
func (vt *VoteTracker) Close() {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.closed = true
	vt.pending = nil
	vt.validatorVotes = nil
	vt.doubleVotes = nil
}

// GetDoubleVoteEvidence returns all recorded double vote evidence.
// This evidence can be used for slashing Byzantine validators.
func (vt *VoteTracker) GetDoubleVoteEvidence() []DoubleVoteEvidence {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	if len(vt.doubleVotes) == 0 {
		return nil
	}

	// Return a copy to prevent external modification
	evidence := make([]DoubleVoteEvidence, len(vt.doubleVotes))
	copy(evidence, vt.doubleVotes)
	return evidence
}

// ClearDoubleVoteEvidence clears all recorded double vote evidence.
// Call this after evidence has been processed (e.g., submitted for slashing).
func (vt *VoteTracker) ClearDoubleVoteEvidence() {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.doubleVotes = make([]DoubleVoteEvidence, 0)
}

// HasDoubleVoteEvidence returns true if there is any double vote evidence.
func (vt *VoteTracker) HasDoubleVoteEvidence() bool {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	return len(vt.doubleVotes) > 0
}

// formCertificateLocked forms a certificate from the pending header.
// Must be called with lock held.
func (vt *VoteTracker) formCertificateLocked(pending *PendingHeader, quorum int) *types.Certificate {
	// Collect votes (up to quorum)
	votes := make([]types.Vote, 0, quorum)
	for _, vote := range pending.Votes {
		votes = append(votes, *vote)
		if len(votes) >= quorum {
			break
		}
	}

	return types.NewCertificate(pending.Header, votes)
}
