package worker

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/types"
)

func TestAckTrackerTrackBatch(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, []types.Transaction{
		types.Transaction([]byte("tx1")),
	})

	at.TrackBatch(batch)

	if at.PendingCount() != 1 {
		t.Errorf("Expected 1 pending batch, got %d", at.PendingCount())
	}

	pending, exists := at.GetPending(batch.Digest)
	if !exists {
		t.Error("Batch should be tracked")
	}

	if !pending.Batch.Digest.Equal(batch.Digest) {
		t.Error("Tracked batch digest mismatch")
	}
}

func TestAckTrackerRecordAck(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	// Record acks
	quorum := at.RecordAck(batch.Digest, 0)
	if quorum {
		t.Error("Should not have quorum after 1 ack")
	}

	quorum = at.RecordAck(batch.Digest, 1)
	if quorum {
		t.Error("Should not have quorum after 2 acks")
	}

	quorum = at.RecordAck(batch.Digest, 2)
	if !quorum {
		t.Error("Should have quorum after 3 acks")
	}

	if at.AckCount(batch.Digest) != 3 {
		t.Errorf("Expected 3 acks, got %d", at.AckCount(batch.Digest))
	}
}

func TestAckTrackerDuplicateAck(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	// Record same ack twice
	at.RecordAck(batch.Digest, 0)
	at.RecordAck(batch.Digest, 0)

	if at.AckCount(batch.Digest) != 1 {
		t.Errorf("Duplicate ack should not be counted, got %d acks", at.AckCount(batch.Digest))
	}
}

func TestAckTrackerHasQuorum(t *testing.T) {
	at := NewAckTracker(2, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	if at.HasQuorum(batch.Digest) {
		t.Error("Should not have quorum initially")
	}

	at.RecordAck(batch.Digest, 0)
	if at.HasQuorum(batch.Digest) {
		t.Error("Should not have quorum after 1 ack")
	}

	at.RecordAck(batch.Digest, 1)
	if !at.HasQuorum(batch.Digest) {
		t.Error("Should have quorum after 2 acks")
	}
}

func TestAckTrackerRemoveBatch(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	if at.PendingCount() != 1 {
		t.Error("Batch should be tracked")
	}

	at.RemoveBatch(batch.Digest)

	if at.PendingCount() != 0 {
		t.Error("Batch should be removed")
	}

	_, exists := at.GetPending(batch.Digest)
	if exists {
		t.Error("Batch should not exist after removal")
	}
}

func TestAckTrackerTimedOut(t *testing.T) {
	// Use a long timeout to prevent the cleanup loop from removing batches
	timeout := 5 * time.Second
	at := NewAckTracker(3, timeout)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)

	// Manually set a pending batch with a past timestamp
	at.mu.Lock()
	at.pending[batch.Digest] = &PendingBatch{
		Batch:     batch.Clone(),
		Acks:      make(map[uint16]bool),
		CreatedAt: time.Now().Add(-6 * time.Second), // Already expired
	}
	at.mu.Unlock()

	timedOut := at.GetTimedOut()
	if len(timedOut) != 1 {
		t.Errorf("Expected 1 timed out batch, got %d", len(timedOut))
	}
}

func TestAckTrackerRemoveTimedOut(t *testing.T) {
	// Use a long timeout to prevent the cleanup loop from interfering
	timeout := 5 * time.Second
	at := NewAckTracker(3, timeout)
	defer at.Close()

	batch1 := types.NewBatch(0, 0, 1, []types.Transaction{types.Transaction([]byte("1"))})
	batch2 := types.NewBatch(0, 0, 2, []types.Transaction{types.Transaction([]byte("2"))})

	// Manually add batch1 with expired timestamp
	at.mu.Lock()
	at.pending[batch1.Digest] = &PendingBatch{
		Batch:     batch1.Clone(),
		Acks:      make(map[uint16]bool),
		CreatedAt: time.Now().Add(-6 * time.Second), // Already expired
	}
	at.mu.Unlock()

	// Add batch2 normally (not expired)
	at.TrackBatch(batch2)

	// Remove timed out
	removed := at.RemoveTimedOut()
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed batch, got %d", len(removed))
	}

	// batch1 should be removed, batch2 should remain
	if at.PendingCount() != 1 {
		t.Errorf("Expected 1 pending batch, got %d", at.PendingCount())
	}

	_, exists := at.GetPending(batch1.Digest)
	if exists {
		t.Error("batch1 should be removed")
	}

	_, exists = at.GetPending(batch2.Digest)
	if !exists {
		t.Error("batch2 should still be pending")
	}
}

func TestAckTrackerUpdateQuorum(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	at.RecordAck(batch.Digest, 0)
	at.RecordAck(batch.Digest, 1)

	if at.HasQuorum(batch.Digest) {
		t.Error("Should not have quorum with 2 acks and quorum=3")
	}

	// Lower quorum requirement
	at.UpdateQuorum(2)

	if !at.HasQuorum(batch.Digest) {
		t.Error("Should have quorum with 2 acks and quorum=2")
	}
}

func TestAckTrackerIdempotentTrack(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)

	at.TrackBatch(batch)
	at.TrackBatch(batch)

	if at.PendingCount() != 1 {
		t.Error("Should only track batch once")
	}
}

func TestAckTrackerAckUnknownBatch(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	unknownDigest := types.HashBytes([]byte("unknown"))
	quorum := at.RecordAck(unknownDigest, 0)

	if quorum {
		t.Error("Recording ack for unknown batch should return false")
	}

	if at.AckCount(unknownDigest) != 0 {
		t.Error("Unknown batch should have 0 acks")
	}
}

func TestAckTrackerClose(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	at.Close()

	// Operations after close should be no-ops
	at.TrackBatch(batch)
	if at.pending != nil {
		t.Error("pending should be nil after close")
	}

	quorum := at.RecordAck(batch.Digest, 0)
	if quorum {
		t.Error("RecordAck after close should return false")
	}
}

func TestAckTrackerConcurrency(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	// Concurrent acks
	done := make(chan bool)
	for i := range 10 {
		go func(validator uint16) {
			at.RecordAck(batch.Digest, validator)
			done <- true
		}(uint16(i))
	}

	for range 10 {
		<-done
	}

	// Should have exactly 10 unique acks
	if at.AckCount(batch.Digest) != 10 {
		t.Errorf("Expected 10 acks, got %d", at.AckCount(batch.Digest))
	}
}

func TestAckTrackerQuorumCallback(t *testing.T) {
	at := NewAckTracker(3, 30*time.Second)
	defer at.Close()

	var fires int
	var lastDigest types.Hash
	var lastLatency time.Duration
	at.SetQuorumCallback(func(digest types.Hash, latency time.Duration) {
		fires++
		lastDigest = digest
		lastLatency = latency
	})

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	at.RecordAck(batch.Digest, 0)
	at.RecordAck(batch.Digest, 1)
	if fires != 0 {
		t.Fatalf("callback fired before quorum: %d", fires)
	}

	at.RecordAck(batch.Digest, 2)
	if fires != 1 {
		t.Fatalf("callback should fire once at quorum, got %d", fires)
	}
	if !lastDigest.Equal(batch.Digest) {
		t.Fatal("callback received wrong digest")
	}
	if lastLatency < 0 {
		t.Fatalf("latency must be non-negative, got %v", lastLatency)
	}

	// Above-quorum acks do not re-fire.
	at.RecordAck(batch.Digest, 3)
	at.RecordAck(batch.Digest, 4)
	if fires != 1 {
		t.Fatalf("callback re-fired above quorum: %d", fires)
	}
}

func TestAckTrackerQuorumCallback_DuplicateAck(t *testing.T) {
	at := NewAckTracker(2, 30*time.Second)
	defer at.Close()

	var fires int
	at.SetQuorumCallback(func(digest types.Hash, latency time.Duration) {
		fires++
	})

	batch := types.NewBatch(0, 0, 1, nil)
	at.TrackBatch(batch)

	at.RecordAck(batch.Digest, 0)
	at.RecordAck(batch.Digest, 0) // duplicate — must not change state
	if fires != 0 {
		t.Fatalf("callback fired on duplicate ack: %d", fires)
	}

	at.RecordAck(batch.Digest, 1)
	if fires != 1 {
		t.Fatalf("callback should fire exactly once, got %d", fires)
	}
}

