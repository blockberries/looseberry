package types

import (
	"testing"
)

func TestNewVote(t *testing.T) {
	headerDigest := HashBytes([]byte("header"))
	vote := NewVote(headerDigest, 5)

	if !vote.HeaderDigest.Equal(headerDigest) {
		t.Error("HeaderDigest mismatch")
	}
	if vote.Validator != 5 {
		t.Errorf("Validator mismatch: expected 5, got %d", vote.Validator)
	}
	if vote.IsSigned() {
		t.Error("New vote should not be signed")
	}
}

func TestVoteSignAndVerify(t *testing.T) {
	signer, err := GenerateEd25519Signer(0)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	headerDigest := HashBytes([]byte("header"))
	vote := NewVote(headerDigest, 0)

	err = vote.Sign(signer)
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	if !vote.IsSigned() {
		t.Error("Vote should be signed after Sign()")
	}

	if !vote.Verify(signer.PublicKey()) {
		t.Error("Signature should verify with correct public key")
	}

	// Wrong public key should fail
	otherSigner, _ := GenerateEd25519Signer(1)
	if vote.Verify(otherSigner.PublicKey()) {
		t.Error("Signature should not verify with wrong public key")
	}
}

func TestVoteClone(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	headerDigest := HashBytes([]byte("header"))
	original := NewVote(headerDigest, 0)
	_ = original.Sign(signer)

	clone := original.Clone()

	if !original.HeaderDigest.Equal(clone.HeaderDigest) {
		t.Error("Clone should have same HeaderDigest")
	}
	if original.Validator != clone.Validator {
		t.Error("Clone should have same Validator")
	}
	if original.Signature != clone.Signature {
		t.Error("Clone should have same Signature")
	}
}
