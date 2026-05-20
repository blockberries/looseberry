package looseberry

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/types"
)

// TestSignedBatchAck_RejectsForgeries verifies that handleBatchAckMessage
// drops acks whose Signature was not produced over BatchAckSignBytes by the
// claimed validator (B3-4 / T1-4). Forged acks must not advance quorum.
func TestSignedBatchAck_RejectsForgeries(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)

	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))

	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lb.Stop() })

	digest := types.HashBytes([]byte("phantom-batch"))

	// Seed every worker's ack tracker so RecordAck can register acks.
	batch := types.NewBatch(0, 0, 0, []types.Transaction{types.Transaction([]byte("x"))})
	batch.Digest = digest
	for i := 0; ; i++ {
		w := lb.workerPool.GetWorker(i)
		if w == nil {
			break
		}
		w.GetAckTracker().TrackBatch(batch)
	}

	// 1) Forged ack: zero signature attributed to validator 1.
	lb.handleBatchAckMessage(&network.BatchAckMessage{
		BatchDigest: digest,
		Validator:   1,
		Round:       0,
	})

	// 2) Wrong-key signature: signer 2 signs but the message claims validator 1.
	sig, _ := signers[2].Sign(network.BatchAckSignBytes(digest, 1, 0))
	lb.handleBatchAckMessage(&network.BatchAckMessage{
		BatchDigest: digest,
		Validator:   1,
		Round:       0,
		Signature:   sig,
	})

	if lb.workerPool.HasBatchAckQuorum(digest) {
		t.Errorf("HasBatchAckQuorum true after forged acks (quorum=%d)", vs.Quorum())
	}

	// 3) Genuine acks from validators 1, 2, 3 should clear quorum=3.
	for _, idx := range []uint16{1, 2, 3} {
		signBytes := network.BatchAckSignBytes(digest, idx, 0)
		s, _ := signers[idx].Sign(signBytes)
		lb.handleBatchAckMessage(&network.BatchAckMessage{
			BatchDigest: digest,
			Validator:   idx,
			Round:       0,
			Signature:   s,
		})
	}

	time.Sleep(20 * time.Millisecond)

	if !lb.workerPool.HasBatchAckQuorum(digest) {
		t.Error("HasBatchAckQuorum false after genuine acks")
	}
}

// TestBatchAckSignBytes_BindsRound asserts that the sign-bytes change when
// the round changes — preventing replay of an old ack against a new batch
// with the same digest (theoretical; digests are content-bound).
func TestBatchAckSignBytes_BindsRound(t *testing.T) {
	digest := types.HashBytes([]byte("d"))
	a := network.BatchAckSignBytes(digest, 1, 0)
	b := network.BatchAckSignBytes(digest, 1, 1)
	if a.Equal(b) {
		t.Error("sign bytes are identical for different rounds")
	}
	c := network.BatchAckSignBytes(digest, 2, 0)
	if a.Equal(c) {
		t.Error("sign bytes are identical for different validator indices")
	}
}
