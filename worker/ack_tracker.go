package worker

import (
	"sync"
	"time"

	"github.com/blockberries/looseberry/types"
)

// PendingBatch tracks a batch waiting for acknowledgments.
type PendingBatch struct {
	Batch     *types.Batch
	Acks      map[uint16]bool // validator -> acked
	CreatedAt time.Time
}

// AckTracker tracks pending batches and their acknowledgments.
// Thread-safe with RWMutex.
type AckTracker struct {
	pending   map[types.Hash]*PendingBatch
	mu        sync.RWMutex
	quorum    int           // Required acks for quorum
	timeout   time.Duration // Timeout for pending batches
	closed    bool
	cleanupCh chan struct{} // Signal to stop cleanup goroutine
}

// NewAckTracker creates a new acknowledgment tracker.
func NewAckTracker(quorum int, timeout time.Duration) *AckTracker {
	at := &AckTracker{
		pending:   make(map[types.Hash]*PendingBatch),
		quorum:    quorum,
		timeout:   timeout,
		cleanupCh: make(chan struct{}),
	}
	go at.cleanupLoop()
	return at
}

// TrackBatch adds a batch to the tracker.
func (at *AckTracker) TrackBatch(batch *types.Batch) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.closed {
		return
	}

	// Don't track if already tracked
	if _, exists := at.pending[batch.Digest]; exists {
		return
	}

	at.pending[batch.Digest] = &PendingBatch{
		Batch:     batch.Clone(),
		Acks:      make(map[uint16]bool),
		CreatedAt: time.Now(),
	}
}

// RecordAck records an acknowledgment from a validator.
// Returns true if quorum is reached with this ack.
func (at *AckTracker) RecordAck(batchDigest types.Hash, validator uint16) bool {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.closed {
		return false
	}

	pending, exists := at.pending[batchDigest]
	if !exists {
		return false
	}

	// Don't double-count
	if pending.Acks[validator] {
		return len(pending.Acks) >= at.quorum
	}

	pending.Acks[validator] = true
	return len(pending.Acks) >= at.quorum
}

// HasQuorum returns true if the batch has received enough acks.
func (at *AckTracker) HasQuorum(batchDigest types.Hash) bool {
	at.mu.RLock()
	defer at.mu.RUnlock()

	pending, exists := at.pending[batchDigest]
	if !exists {
		return false
	}

	return len(pending.Acks) >= at.quorum
}

// GetPending returns a pending batch if it exists.
func (at *AckTracker) GetPending(batchDigest types.Hash) (*PendingBatch, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()

	pending, exists := at.pending[batchDigest]
	if !exists {
		return nil, false
	}

	return pending, true
}

// RemoveBatch removes a batch from tracking.
func (at *AckTracker) RemoveBatch(batchDigest types.Hash) {
	at.mu.Lock()
	defer at.mu.Unlock()

	delete(at.pending, batchDigest)
}

// AckCount returns the number of acks for a batch.
func (at *AckTracker) AckCount(batchDigest types.Hash) int {
	at.mu.RLock()
	defer at.mu.RUnlock()

	pending, exists := at.pending[batchDigest]
	if !exists {
		return 0
	}

	return len(pending.Acks)
}

// PendingCount returns the number of pending batches.
func (at *AckTracker) PendingCount() int {
	at.mu.RLock()
	defer at.mu.RUnlock()

	return len(at.pending)
}

// GetTimedOut returns all batches that have timed out.
func (at *AckTracker) GetTimedOut() []*types.Batch {
	at.mu.RLock()
	defer at.mu.RUnlock()

	var timedOut []*types.Batch
	cutoff := time.Now().Add(-at.timeout)

	for _, pending := range at.pending {
		if pending.CreatedAt.Before(cutoff) {
			timedOut = append(timedOut, pending.Batch.Clone())
		}
	}

	return timedOut
}

// RemoveTimedOut removes all batches that have timed out and returns them.
func (at *AckTracker) RemoveTimedOut() []*types.Batch {
	at.mu.Lock()
	defer at.mu.Unlock()

	var timedOut []*types.Batch
	cutoff := time.Now().Add(-at.timeout)

	for digest, pending := range at.pending {
		if pending.CreatedAt.Before(cutoff) {
			timedOut = append(timedOut, pending.Batch.Clone())
			delete(at.pending, digest)
		}
	}

	return timedOut
}

// UpdateQuorum updates the quorum requirement.
func (at *AckTracker) UpdateQuorum(quorum int) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.quorum = quorum
}

// Close stops the tracker and releases resources.
func (at *AckTracker) Close() {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.closed {
		return
	}

	at.closed = true
	close(at.cleanupCh)
	at.pending = nil
}

// cleanupLoop periodically removes timed out batches.
func (at *AckTracker) cleanupLoop() {
	ticker := time.NewTicker(at.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			at.RemoveTimedOut()
		case <-at.cleanupCh:
			return
		}
	}
}
