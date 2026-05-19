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

// QuorumCallback is invoked the moment a tracked batch's ack count first
// reaches the configured quorum. latency is wall-clock from TrackBatch
// (batch creation) to the quorum-reaching ack. Used by the looseberry
// observability hook to drive the raspberry_batch_ack_latency_seconds
// histogram (PLAN follow-up).
type QuorumCallback func(digest types.Hash, latency time.Duration)

// AckTracker tracks pending batches and their acknowledgments.
// Thread-safe with RWMutex.
type AckTracker struct {
	pending        map[types.Hash]*PendingBatch
	mu             sync.RWMutex
	quorum         int           // Required acks for quorum
	timeout        time.Duration // Timeout for pending batches
	closed         bool
	cleanupCh      chan struct{} // Signal to stop cleanup goroutine
	quorumCallback QuorumCallback
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
//
// When this ack is the one that first lifts the batch's ack count to the
// configured quorum, the registered QuorumCallback (if any) is invoked
// synchronously with the batch digest and the wall-clock latency from
// TrackBatch (batch creation) to now. The callback runs under at.mu, so
// it must be cheap and non-blocking — typical use is incrementing a
// Prometheus histogram. Subsequent acks that keep the count at-or-above
// quorum do NOT re-fire the callback.
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

	wasBelow := len(pending.Acks) < at.quorum
	pending.Acks[validator] = true
	atQuorum := len(pending.Acks) >= at.quorum
	if wasBelow && atQuorum && at.quorumCallback != nil {
		at.quorumCallback(batchDigest, time.Since(pending.CreatedAt))
	}
	return atQuorum
}

// SetQuorumCallback installs a callback fired when a tracked batch first
// reaches quorum. Pass nil to clear.
func (at *AckTracker) SetQuorumCallback(cb QuorumCallback) {
	at.mu.Lock()
	defer at.mu.Unlock()
	at.quorumCallback = cb
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

// GetPending returns a copy of a pending batch if it exists.
// The returned copy is safe to use without affecting internal state.
func (at *AckTracker) GetPending(batchDigest types.Hash) (*PendingBatch, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()

	pending, exists := at.pending[batchDigest]
	if !exists {
		return nil, false
	}

	// Clone to prevent external modification of internal state
	acksCopy := make(map[uint16]bool, len(pending.Acks))
	for k, v := range pending.Acks {
		acksCopy[k] = v
	}

	return &PendingBatch{
		Batch:     pending.Batch.Clone(),
		Acks:      acksCopy,
		CreatedAt: pending.CreatedAt,
	}, true
}

// PendingBelowQuorum returns deep copies of every batch this AckTracker
// is still holding that has NOT reached quorum AND was created at least
// minAge ago. Drives the looseberry instance's startup-aware
// rebroadcast loop (PLAN §E7c). The minAge filter prevents re-broadcast
// from racing fresh batches whose first-attempt acks are still in flight.
func (at *AckTracker) PendingBelowQuorum(minAge time.Duration) []*types.Batch {
	at.mu.RLock()
	defer at.mu.RUnlock()
	if at.closed {
		return nil
	}
	cutoff := time.Now().Add(-minAge)
	var out []*types.Batch
	for _, p := range at.pending {
		if len(p.Acks) >= at.quorum {
			continue
		}
		if !p.CreatedAt.Before(cutoff) {
			continue
		}
		out = append(out, p.Batch.Clone())
	}
	return out
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
