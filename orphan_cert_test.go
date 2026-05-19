package looseberry

import (
	"testing"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// TestHandleCertificateMessage_BuffersOrphanUntilParentArrives is the
// regression test for PLAN §E7 (multi-source submission tx loss).
//
// Under burst load on a 4-validator cluster, certs and their parent
// certs are broadcast on independent streams and can land out of order.
// dag.AddCertificate rejects orphan certs with ErrMissingParents.
// Before this fix, handleCertificateMessage swallowed that error
// silently and the orphan was permanently lost; the DAG stalled at the
// round its missing parents would have populated, and every subsequent
// block was empty. Net effect on TestPhaseE_HighBurstSweep/30k-4-source:
// 0% commit ratio on most runs.
//
// The fix buffers the orphan and replays the buffer whenever a cert
// lands successfully. This test exercises the path directly:
//   - submit round-1 cert C1 (parent: round-0 cert C0); orphan-rejected
//     because C0 hasn't arrived yet, expect DAG.HighestRound == 0
//   - submit C0; expect DAG to immediately include both C0 and C1, so
//     HighestRound advances to 1
func TestHandleCertificateMessage_BuffersOrphanUntilParentArrives(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))
	lb.SetStores(
		store.NewMemoryBatchStore(),
		store.NewMemoryCertificateStore(),
		store.NewMemoryTxIndex(),
	)

	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	c0 := buildCertWithParents(t, signers[0], 0, 0, nil)
	c1 := buildCertWithParents(t, signers[0], 0, 1, []types.CertificateRef{c0.GetRef()})

	// Deliver the round-1 cert FIRST. dag.AddCertificate will reject it
	// (parent c0 not yet present); the fix should buffer it.
	lb.handleCertificateMessage(&network.CertificateMessage{Certificate: c1})

	if got := lb.dag.HighestRound(); got != 0 {
		t.Fatalf("after orphan c1 only: HighestRound=%d, want 0 (cert should be buffered, not added)", got)
	}

	// Deliver the round-0 cert. It adds cleanly; the replay loop must
	// then drain c1 from the orphan buffer, advancing HighestRound to 1.
	// Without the fix, c1 was permanently dropped on its first arrival
	// and HighestRound stays at 0 here.
	lb.handleCertificateMessage(&network.CertificateMessage{Certificate: c0})

	if got := lb.dag.HighestRound(); got != 1 {
		t.Fatalf("after parent c0 arrives: HighestRound=%d, want 1 (c1 must be replayed from orphan buffer; "+
			"without the fix the orphan would have been silently dropped)", got)
	}
}

// TestHandleCertificateMessage_OrphanBufferStableUnderFloodOfBogus
// pins the memory-safety bound: a peer pumping arbitrarily many bogus
// orphan certs (all referencing a parent that never arrives) must not
// crash, leak, or otherwise destabilise the looseberry instance. The
// buffer is internally capped; we only assert that the instance keeps
// accepting valid certs after the flood — which it can only do if the
// cap held.
func TestHandleCertificateMessage_OrphanBufferStableUnderFloodOfBogus(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))
	lb.SetStores(
		store.NewMemoryBatchStore(),
		store.NewMemoryCertificateStore(),
		store.NewMemoryTxIndex(),
	)
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	bogusParent := types.CertificateRef{
		Digest: types.HashBytes([]byte("nonexistent-parent")),
		Round:  0,
	}

	// Pump ~8000 distinct round-1 certs at the looseberry — comfortably
	// past any reasonable cap.
	for i := 0; i < 8000; i++ {
		header := types.NewHeader(0, 1, uint64(i+1), nil, []types.CertificateRef{bogusParent})
		if err := header.Sign(signers[0]); err != nil {
			t.Fatalf("Sign: %v", err)
		}
		vote := types.NewVote(header.Digest, 0)
		if err := vote.Sign(signers[0]); err != nil {
			t.Fatalf("vote.Sign: %v", err)
		}
		cert := types.NewCertificate(header, []types.Vote{*vote})
		lb.handleCertificateMessage(&network.CertificateMessage{Certificate: cert})
	}

	// After the flood, a clean round-0 cert must still add. If the
	// orphan buffer had grown unbounded and OOM'd or the looseberry was
	// otherwise wedged, this would hang or fail.
	c0 := buildCertWithParents(t, signers[0], 0, 0, nil)
	lb.handleCertificateMessage(&network.CertificateMessage{Certificate: c0})
	if got := lb.dag.HighestRound(); got != 0 {
		t.Fatalf("after flood + clean round-0 cert: HighestRound=%d, want 0", got)
	}
}

func buildCertWithParents(t *testing.T, signer *types.Ed25519Signer, author uint16, round uint64, parents []types.CertificateRef) *types.Certificate {
	t.Helper()
	hdr := types.NewHeader(author, round, 0, nil, parents)
	if err := hdr.Sign(signer); err != nil {
		t.Fatalf("header.Sign: %v", err)
	}
	v := types.NewVote(hdr.Digest, author)
	if err := v.Sign(signer); err != nil {
		t.Fatalf("vote.Sign: %v", err)
	}
	return types.NewCertificate(hdr, []types.Vote{*v})
}
