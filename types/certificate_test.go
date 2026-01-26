package types

import (
	"testing"
)

func createTestValidatorSet(n int) (*SimpleValidatorSet, []*Ed25519Signer) {
	validators := make([]*Validator, n)
	signers := make([]*Ed25519Signer, n)

	for i := 0; i < n; i++ {
		signer, _ := GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
			Power:     1,
		}
	}

	return NewSimpleValidatorSet(validators, 1), signers
}

func TestNewCertificate(t *testing.T) {
	validatorSet, signers := createTestValidatorSet(4)

	// Create header
	header := NewHeader(0, 10, 1, nil, nil)
	_ = header.Sign(signers[0])

	// Create votes (need 2f+1 = 3 for n=4)
	votes := make([]Vote, 3)
	for i := 0; i < 3; i++ {
		vote := NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	cert := NewCertificate(header, votes)

	if cert.Author() != 0 {
		t.Errorf("Author mismatch: expected 0, got %d", cert.Author())
	}
	if cert.Round() != 10 {
		t.Errorf("Round mismatch: expected 10, got %d", cert.Round())
	}
	if cert.Epoch() != 1 {
		t.Errorf("Epoch mismatch: expected 1, got %d", cert.Epoch())
	}
	if cert.VoteCount() != 3 {
		t.Errorf("VoteCount mismatch: expected 3, got %d", cert.VoteCount())
	}
	if !cert.HasQuorum(validatorSet.Count()) {
		t.Error("Certificate should have quorum")
	}
}

func TestCertificateHasVoteFrom(t *testing.T) {
	_, signers := createTestValidatorSet(4)

	header := NewHeader(0, 10, 1, nil, nil)
	_ = header.Sign(signers[0])

	// Only first 3 validators vote
	votes := make([]Vote, 3)
	for i := 0; i < 3; i++ {
		vote := NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	cert := NewCertificate(header, votes)

	for i := 0; i < 3; i++ {
		if !cert.HasVoteFrom(uint16(i)) {
			t.Errorf("Certificate should have vote from validator %d", i)
		}
	}

	// Validator 3 did not vote
	if cert.HasVoteFrom(3) {
		t.Error("Certificate should not have vote from validator 3")
	}
}

func TestCertificateVerify(t *testing.T) {
	validatorSet, signers := createTestValidatorSet(4)

	header := NewHeader(0, 10, 1, nil, nil)
	_ = header.Sign(signers[0])

	votes := make([]Vote, 3)
	for i := 0; i < 3; i++ {
		vote := NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	cert := NewCertificate(header, votes)

	err := cert.Verify(validatorSet)
	if err != nil {
		t.Errorf("Valid certificate should verify: %v", err)
	}
}

func TestCertificateVerifyInsufficientQuorum(t *testing.T) {
	validatorSet, signers := createTestValidatorSet(4)

	header := NewHeader(0, 10, 1, nil, nil)
	_ = header.Sign(signers[0])

	// Only 2 votes (need 3 for quorum)
	votes := make([]Vote, 2)
	for i := 0; i < 2; i++ {
		vote := NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	cert := NewCertificate(header, votes)

	err := cert.Verify(validatorSet)
	if err != ErrInsufficientQuorum {
		t.Errorf("Expected ErrInsufficientQuorum, got: %v", err)
	}
}

func TestCertificateClone(t *testing.T) {
	_, signers := createTestValidatorSet(4)

	header := NewHeader(0, 10, 1, nil, nil)
	_ = header.Sign(signers[0])

	votes := make([]Vote, 3)
	for i := 0; i < 3; i++ {
		vote := NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	original := NewCertificate(header, votes)
	clone := original.Clone()

	if !original.Digest().Equal(clone.Digest()) {
		t.Error("Clone should have same digest")
	}
	if original.VoteCount() != clone.VoteCount() {
		t.Error("Clone should have same vote count")
	}

	// Modify clone should not affect original
	clone.Votes[0].Validator = 99
	if original.Votes[0].Validator == 99 {
		t.Error("Modifying clone should not affect original")
	}
}

func TestBitSet(t *testing.T) {
	bs := NewBitSet(100)

	// Test Set and IsSet
	bs.Set(5)
	bs.Set(42)
	bs.Set(99)

	if !bs.IsSet(5) {
		t.Error("Bit 5 should be set")
	}
	if !bs.IsSet(42) {
		t.Error("Bit 42 should be set")
	}
	if !bs.IsSet(99) {
		t.Error("Bit 99 should be set")
	}
	if bs.IsSet(0) {
		t.Error("Bit 0 should not be set")
	}

	// Test Count
	if bs.Count() != 3 {
		t.Errorf("Count mismatch: expected 3, got %d", bs.Count())
	}

	// Test Clear
	bs.Clear(42)
	if bs.IsSet(42) {
		t.Error("Bit 42 should be cleared")
	}
	if bs.Count() != 2 {
		t.Errorf("Count after clear: expected 2, got %d", bs.Count())
	}

	// Test Clone
	clone := bs.Clone()
	if !clone.IsSet(5) || !clone.IsSet(99) {
		t.Error("Clone should preserve set bits")
	}

	// Modify clone should not affect original
	clone.Clear(5)
	if !bs.IsSet(5) {
		t.Error("Modifying clone should not affect original")
	}
}
