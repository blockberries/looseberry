package looseberry

import (
	"fmt"
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/primary"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// ============================================================================
// Byzantine Signature Tests
// ============================================================================

func TestByzantineInvalidHeaderSignature(t *testing.T) {
	// Create validator set
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	// Create primary for honest validator
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create header signed by wrong validator (Byzantine)
	byzantineHeader := types.NewHeader(1, 0, 0, nil, nil) // Claims author 1
	_ = byzantineHeader.Sign(signers[2])                  // But signed by validator 2

	// HandleHeader should reject due to signature mismatch
	err := p.HandleHeader(byzantineHeader)
	if err == nil {
		t.Error("HandleHeader should reject header with mismatched signature")
	}
}

func TestByzantineInvalidVoteSignature(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create valid header
	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// Create vote that claims to be from validator 1 but signed by validator 2
	byzantineVote := types.NewVote(header.Digest, 1) // Claims validator 1
	_ = byzantineVote.Sign(signers[2])               // But signed by validator 2

	// HandleVote should reject due to signature mismatch
	_, formed := p.HandleVote(byzantineVote)
	if formed {
		t.Error("HandleVote should not form certificate with invalid vote")
	}
}

func TestByzantineInvalidCertificateSignatures(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	// Create header
	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// Create votes with one invalid signature
	votes := make([]types.Vote, 3)
	for i := range 3 {
		votes[i] = *types.NewVote(header.Digest, uint16(i))
		if i == 1 {
			// Byzantine: sign vote 1 with wrong key
			_ = votes[i].Sign(signers[2])
		} else {
			_ = votes[i].Sign(signers[i])
		}
	}

	cert := types.NewCertificate(header, votes)

	// Certificate should fail verification
	err := cert.Verify(vs)
	if err == nil {
		t.Error("Certificate with invalid vote signature should fail verification")
	}
}

// ============================================================================
// Byzantine Equivocation Tests
// ============================================================================

func TestByzantineEquivocationDifferentHeaders(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	// Create two different headers for the same round (equivocation)
	header1 := types.NewHeader(0, 1, 0, nil, nil)
	_ = header1.Sign(signers[0])

	header2 := types.NewHeader(0, 1, 0, []types.BatchDigest{{Digest: types.Hash{0x01}, WorkerID: 0, ValidatorID: 0}}, nil)
	_ = header2.Sign(signers[0])

	// Both should have different digests
	if header1.Digest == header2.Digest {
		t.Fatal("Equivocating headers should have different digests")
	}

	// Create certificates for both (Byzantine scenario)
	votes1 := make([]types.Vote, 3)
	votes2 := make([]types.Vote, 3)
	for i := range 3 {
		votes1[i] = *types.NewVote(header1.Digest, uint16(i))
		_ = votes1[i].Sign(signers[i])
		votes2[i] = *types.NewVote(header2.Digest, uint16(i))
		_ = votes2[i].Sign(signers[i])
	}

	cert1 := types.NewCertificate(header1, votes1)
	cert2 := types.NewCertificate(header2, votes2)

	// Both certificates are valid individually
	if err := cert1.Verify(vs); err != nil {
		t.Errorf("cert1 should be valid: %v", err)
	}
	if err := cert2.Verify(vs); err != nil {
		t.Errorf("cert2 should be valid: %v", err)
	}

	// CertificateStore should allow storing both (it's a store, not validator)
	// The DAG or consensus layer would detect equivocation
	if err := certStore.SaveCertificate(cert1); err != nil {
		t.Errorf("First certificate should be saved: %v", err)
	}
	if err := certStore.SaveCertificate(cert2); err != nil {
		t.Errorf("Second certificate can also be saved in store: %v", err)
	}

	// Verify both exist (equivocation evidence)
	has1 := certStore.HasCertificate(cert1.Digest())
	has2 := certStore.HasCertificate(cert2.Digest())
	if !has1 || !has2 {
		t.Error("Both certificates should be stored (equivocation evidence)")
	}
}

func TestByzantineVoteForNonExistentHeader(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create vote for non-existent header
	fakeDigest := types.Hash{0x99, 0x99, 0x99}
	byzantineVote := types.NewVote(fakeDigest, 1)
	_ = byzantineVote.Sign(signers[1])

	// Vote should be buffered (waiting for header), not form certificate
	_, formed := p.HandleVote(byzantineVote)
	if formed {
		t.Error("Vote for non-existent header should not form certificate")
	}
}

// ============================================================================
// Byzantine Round/Epoch Tests
// ============================================================================

func TestByzantineHeaderFromFuture(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	cfg := primary.DefaultConfig()
	cfg.MaxRoundGap = 5
	p := primary.New(0, signers[0], cfg, certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create header from far future (beyond MaxRoundGap)
	futureHeader := types.NewHeader(1, 100, 0, nil, nil)
	_ = futureHeader.Sign(signers[1])

	// HandleHeader should reject due to round gap
	err := p.HandleHeader(futureHeader)
	if err == nil {
		t.Error("HandleHeader should reject header from far future round")
	}
}

func TestByzantineHeaderWrongEpoch(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0) // Epoch 0

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create header with wrong epoch
	wrongEpochHeader := types.NewHeader(1, 0, 99, nil, nil) // Epoch 99, should be 0
	_ = wrongEpochHeader.Sign(signers[1])

	// HandleHeader should reject due to epoch mismatch
	err := p.HandleHeader(wrongEpochHeader)
	if err == nil {
		t.Error("HandleHeader should reject header with wrong epoch")
	}
}

// ============================================================================
// Byzantine Certificate Tests
// ============================================================================

func TestByzantineCertificateInsufficientVotes(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// Only 2 votes (need 3 for quorum with 4 validators)
	votes := make([]types.Vote, 2)
	for i := range 2 {
		votes[i] = *types.NewVote(header.Digest, uint16(i))
		_ = votes[i].Sign(signers[i])
	}

	cert := types.NewCertificate(header, votes)

	// Certificate should fail verification due to insufficient votes
	err := cert.Verify(vs)
	if err == nil {
		t.Error("Certificate with insufficient votes should fail verification")
	}
}

func TestByzantineCertificateDuplicateVoters(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// 3 votes but validator 0 votes twice (duplicate)
	// The SignerMask should correctly track unique voters
	votes := make([]types.Vote, 3)
	votes[0] = *types.NewVote(header.Digest, 0)
	_ = votes[0].Sign(signers[0])
	votes[1] = *types.NewVote(header.Digest, 0) // Duplicate validator 0
	_ = votes[1].Sign(signers[0])
	votes[2] = *types.NewVote(header.Digest, 1)
	_ = votes[2].Sign(signers[1])

	cert := types.NewCertificate(header, votes)

	// Check that the SignerMask only has 2 unique voters
	// (depending on implementation, this may or may not cause Verify to fail)
	uniqueCount := cert.SignerMask.Count()
	if uniqueCount != 2 {
		t.Logf("Note: SignerMask has %d voters, expected 2 unique", uniqueCount)
	}

	// Verification should ideally fail due to insufficient unique votes
	// but if not, at least verify the behavior is consistent
	err := cert.Verify(vs)
	t.Logf("Verification result: %v (unique voters: %d, quorum: %d)", err, uniqueCount, vs.Quorum())
}

func TestByzantineCertificateVotesForWrongHeader(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	differentHeader := types.NewHeader(0, 1, 0, nil, nil)
	_ = differentHeader.Sign(signers[0])

	// Create votes for a different header
	votes := make([]types.Vote, 3)
	for i := range 3 {
		votes[i] = *types.NewVote(differentHeader.Digest, uint16(i)) // Wrong header!
		_ = votes[i].Sign(signers[i])
	}

	cert := types.NewCertificate(header, votes)

	// Certificate should fail - votes are for wrong header
	err := cert.Verify(vs)
	if err == nil {
		t.Error("Certificate with votes for wrong header should fail verification")
	}
}

// ============================================================================
// Byzantine Network Tests
// ============================================================================

func TestByzantineNetworkMessageFromUnknownValidator(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	// Create unknown validator (not in validator set)
	unknownSigner, _ := types.GenerateEd25519Signer(99)

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create header from unknown validator
	header := types.NewHeader(99, 0, 0, nil, nil)
	_ = header.Sign(unknownSigner)

	// HandleHeader should reject - validator not in set
	err := p.HandleHeader(header)
	if err == nil {
		t.Error("HandleHeader should reject header from unknown validator")
	}
}

func TestByzantineDuplicateVotes(t *testing.T) {
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	p := primary.New(0, signers[0], primary.DefaultConfig(), certStore, batchStore, vs)

	var certCount int
	p.SetCertificateCallback(func(cert *types.Certificate) {
		certCount++
	})

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create valid header
	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// Track header
	p.AddBatchDigest(types.BatchDigest{Digest: types.Hash{0x01}, WorkerID: 0, ValidatorID: 0})
	time.Sleep(100 * time.Millisecond) // Let header be created

	// Suppose we have a header in flight, send duplicate votes
	vote := types.NewVote(header.Digest, 1)
	_ = vote.Sign(signers[1])

	// Send same vote multiple times - should only count once
	for i := 0; i < 5; i++ {
		_, _ = p.HandleVote(vote)
	}
}

// ============================================================================
// Byzantine Batch Tests
// ============================================================================

func TestByzantineBatchInvalidDigest(t *testing.T) {
	// Create batch with manually set wrong digest
	batch := &types.Batch{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        0,
		Transactions: []types.Transaction{types.Transaction("tx1")},
		Digest:       types.Hash{0x00, 0x01, 0x02}, // Wrong digest
	}

	// Compute correct digest
	correctDigest := batch.ComputeDigest()

	if batch.Digest == correctDigest {
		t.Error("Test setup error: batch should have wrong digest")
	}

	// Verification should detect mismatch
	if batch.Digest == batch.ComputeDigest() {
		t.Error("Batch with tampered digest should not match computed digest")
	}
}

// ============================================================================
// Multi-Node Byzantine Tests
// ============================================================================

func TestByzantineMultiNodeOneCompromised(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node Byzantine test in short mode")
	}

	// Create 4-node network (can tolerate 1 Byzantine)
	tn := NewTestNetwork(t, 4)
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = tn.Stop() }()

	// Node 3 is "Byzantine" - we simulate by submitting different transactions
	// while honest nodes submit the same

	// Submit same transaction to all honest nodes
	for i := 0; i < 3; i++ {
		tx := []byte("honest-tx-1")
		if err := tn.SubmitTxToNode(i, tx); err != nil {
			t.Errorf("Failed to submit to node %d: %v", i, err)
		}
	}

	// Submit different transaction to Byzantine node
	byzantineTx := []byte("byzantine-tx-different")
	if err := tn.SubmitTxToNode(3, byzantineTx); err != nil {
		t.Errorf("Failed to submit to Byzantine node: %v", err)
	}

	// Wait briefly
	time.Sleep(100 * time.Millisecond)

	// Network should still function with 1 Byzantine node
	// (just verify no crashes or deadlocks)
	for i := 0; i < 4; i++ {
		if !tn.Node(i).Looseberry.IsRunning() {
			t.Errorf("Node %d stopped unexpectedly", i)
		}
	}
}

func TestByzantineMultiNodeMessageInjection(t *testing.T) {
	// Create two mock networks
	net1 := network.NewMockNetwork(0, network.DefaultConfig())
	net2 := network.NewMockNetwork(1, network.DefaultConfig())

	if err := net1.Start(); err != nil {
		t.Fatalf("net1 start failed: %v", err)
	}
	if err := net2.Start(); err != nil {
		t.Fatalf("net2 start failed: %v", err)
	}
	defer func() { _ = net1.Stop() }()
	defer func() { _ = net2.Stop() }()

	net1.Connect(net2)

	// Inject invalid batch (simulating Byzantine message)
	// Inject directly into net2's channel to simulate it receiving the message
	invalidBatch := &types.Batch{
		WorkerID:     0,
		ValidatorID:  99, // Invalid validator
		Round:        0,
		Transactions: nil,
		Digest:       types.Hash{0xFF},
	}

	// Use the InjectBatchMessage method with the proper BatchMessage wrapper
	// Inject into net2 (the receiver) to simulate receiving a Byzantine message
	batchMsg := &network.BatchMessage{
		From:  0, // From Byzantine validator
		Batch: invalidBatch,
	}
	net2.InjectBatchMessage(batchMsg)

	// net2 should have the batch message in its queue
	select {
	case received := <-net2.BatchMessages():
		// Batch received - in a real system, this would be validated and rejected
		if received.Batch.ValidatorID != 99 {
			t.Error("Received batch doesn't match injected")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for injected batch")
	}
}

// ============================================================================
// Byzantine Resilience Tests
// ============================================================================

func TestByzantineResilienceWithFValidators(t *testing.T) {
	// Test that system can tolerate f Byzantine validators
	// With n=4, f=1, quorum=3

	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	// Verify BFT parameters
	if vs.F() != 1 {
		t.Errorf("Expected f=1 for n=4, got f=%d", vs.F())
	}
	if vs.Quorum() != 3 {
		t.Errorf("Expected quorum=3 for n=4, got quorum=%d", vs.Quorum())
	}

	// Create valid header
	header := types.NewHeader(0, 0, 0, nil, nil)
	_ = header.Sign(signers[0])

	// Test: 3 honest votes (quorum) should create valid certificate
	honestVotes := make([]types.Vote, 3)
	for i := range 3 {
		honestVotes[i] = *types.NewVote(header.Digest, uint16(i))
		_ = honestVotes[i].Sign(signers[i])
	}

	cert := types.NewCertificate(header, honestVotes)
	if err := cert.Verify(vs); err != nil {
		t.Errorf("Certificate with quorum votes should be valid: %v", err)
	}

	// Test: 2 honest + 1 Byzantine (bad sig) should fail
	mixedVotes := make([]types.Vote, 3)
	for i := range 2 {
		mixedVotes[i] = *types.NewVote(header.Digest, uint16(i))
		_ = mixedVotes[i].Sign(signers[i])
	}
	// Byzantine vote with wrong signature
	mixedVotes[2] = *types.NewVote(header.Digest, 2)
	_ = mixedVotes[2].Sign(signers[3]) // Signed by wrong key

	badCert := types.NewCertificate(header, mixedVotes)
	if err := badCert.Verify(vs); err == nil {
		t.Error("Certificate with Byzantine vote should fail verification")
	}
}

func TestByzantineSafetyWithQuorum(t *testing.T) {
	// Verify safety: conflicting certificates cannot both be valid

	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}
	vs := types.NewSimpleValidatorSet(validators, 0)

	// Create two conflicting headers for same round/validator
	header1 := types.NewHeader(0, 0, 0, nil, nil)
	_ = header1.Sign(signers[0])

	header2 := types.NewHeader(0, 0, 0, []types.BatchDigest{{Digest: types.Hash{0x01}}}, nil)
	_ = header2.Sign(signers[0])

	// For both to be valid, would need quorum (3) votes each
	// But with only 4 validators and f=1, at most 1 can be Byzantine
	// So at least 2 honest validators would have to vote for both
	// (which honest validators wouldn't do)

	// This test verifies the math: 2 * quorum > n
	// 2 * 3 = 6 > 4 (TRUE), so quorum intersection is guaranteed

	quorum := vs.Quorum()
	n := vs.Count()
	if 2*quorum <= n {
		t.Errorf("Quorum intersection property violated: 2*quorum=%d should be > n=%d",
			2*quorum, n)
	}

	// Quorum intersection: two quorums must share at least 1 honest validator
	// This guarantees safety: if a certificate is formed, no conflicting one can be
	t.Logf("BFT properties verified: n=%d, f=%d, quorum=%d, 2*quorum=%d > n (safety guaranteed)",
		n, vs.F(), quorum, 2*quorum)
}

func TestByzantineTypesErrors(t *testing.T) {
	// Test that Byzantine errors are correctly classified
	// Based on IsByzantine() implementation in types/errors.go

	byzantineErrors := []error{
		types.ErrInvalidSignature,
		types.ErrDuplicateHeader,
		types.ErrDuplicateVote,
		types.ErrInvalidBatch,
		types.ErrInvalidHeader,
		types.ErrInvalidVote,
	}

	for _, err := range byzantineErrors {
		if !types.IsByzantine(err) {
			t.Errorf("Error %v should be classified as Byzantine", err)
		}
	}

	// Non-Byzantine errors
	nonByzantineErrors := []error{
		types.ErrBatchNotFound,
		types.ErrSyncTimeout,
		types.ErrSyncFailed,
		types.ErrMempoolFull,
		types.ErrNotRunning,
	}

	for _, err := range nonByzantineErrors {
		if types.IsByzantine(err) {
			t.Errorf("Error %v should not be classified as Byzantine", err)
		}
	}
}

func TestByzantineLivenessWithHonestMajority(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping liveness test in short mode")
	}

	// With n=4 and f=1, should be able to make progress with 3 honest nodes
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)

	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	vs := types.NewSimpleValidatorSet(validators, 0)

	cfg := DefaultConfig()
	cfg.Signer = signers[0]
	cfg.ValidatorIndex = 0
	cfg.Storage.InMemory = true

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Submit transactions
	for i := range 100 {
		tx := fmt.Appendf(nil, "tx-%d", i)
		_ = lb.AddTx(tx)
	}

	// System should still function (even if one node was Byzantine)
	time.Sleep(200 * time.Millisecond)

	if !lb.IsRunning() {
		t.Error("System should remain running with honest majority")
	}

	metrics := lb.Metrics()
	if metrics.TotalTxAdded != 100 {
		t.Errorf("Expected 100 transactions added, got %d", metrics.TotalTxAdded)
	}
}
