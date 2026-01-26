package primary

import (
	"sync"
	"time"

	"github.com/blockberries/looseberry/types"
)

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
}

// NewVoteTracker creates a new vote tracker.
func NewVoteTracker(timeout time.Duration) *VoteTracker {
	return &VoteTracker{
		pending: make(map[types.Hash]*PendingHeader),
		timeout: timeout,
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

	// Don't double-count
	if _, hasVote := pending.Votes[vote.Validator]; hasVote {
		// Already have vote, check if we have quorum
		if len(pending.Votes) >= quorum {
			return vt.formCertificateLocked(pending, quorum), true
		}
		return nil, false
	}

	// Add vote
	pending.Votes[vote.Validator] = vote.Clone()

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
