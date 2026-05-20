package looseberry

import (
	"testing"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/types"
)

// TestReapCertifiedBatches_Dedupes asserts that when two certificates
// reference the same batch digest, ReapCertifiedBatches returns the batch
// exactly once (B3-8 / T2-5).
func TestReapCertifiedBatches_Dedupes(t *testing.T) {
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

	// Build a shared batch and persist it.
	batch := types.NewBatch(0, 0, 0, []types.Transaction{
		types.Transaction([]byte("shared")),
	})
	if err := lb.batchStore.SaveBatch(batch); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	// Two distinct headers that both reference the same batch digest.
	refs := []types.BatchDigest{batch.GetDigest()}
	headerA := types.NewHeader(0, 0, 0, refs, nil)
	if err := headerA.Sign(signers[0]); err != nil {
		t.Fatalf("sign A: %v", err)
	}
	headerB := types.NewHeader(1, 0, 0, refs, nil)
	if err := headerB.Sign(signers[1]); err != nil {
		t.Fatalf("sign B: %v", err)
	}

	// Quorum of 3 votes per certificate.
	mkCert := func(h *types.Header) *types.Certificate {
		votes := make([]types.Vote, 0, 3)
		for i := 0; i < 3; i++ {
			v := types.NewVote(h.Digest, uint16(i))
			if err := v.Sign(signers[i]); err != nil {
				t.Fatalf("sign vote: %v", err)
			}
			votes = append(votes, *v)
		}
		return types.NewCertificate(h, votes)
	}
	certA := mkCert(headerA)
	certB := mkCert(headerB)
	if err := lb.dag.AddCertificate(certA); err != nil {
		t.Fatalf("AddCertificate A: %v", err)
	}
	if err := lb.dag.AddCertificate(certB); err != nil {
		t.Fatalf("AddCertificate B: %v", err)
	}

	got := lb.ReapCertifiedBatches(1 << 30)
	if len(got) != 1 {
		t.Errorf("ReapCertifiedBatches returned %d entries, want 1 (dedup of shared digest)", len(got))
	}
}
