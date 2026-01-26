package types

import (
	"testing"
)

func TestNewHeader(t *testing.T) {
	batchRefs := []BatchDigest{
		{Digest: HashBytes([]byte("batch1")), WorkerID: 0, ValidatorID: 0},
	}
	parents := []CertificateRef{
		{Digest: HashBytes([]byte("parent1")), Round: 9},
	}

	header := NewHeader(1, 10, 1, batchRefs, parents)

	if header.Author != 1 {
		t.Errorf("Author mismatch: expected 1, got %d", header.Author)
	}
	if header.Round != 10 {
		t.Errorf("Round mismatch: expected 10, got %d", header.Round)
	}
	if header.Epoch != 1 {
		t.Errorf("Epoch mismatch: expected 1, got %d", header.Epoch)
	}
	if len(header.BatchRefs) != 1 {
		t.Errorf("BatchRefs count mismatch: expected 1, got %d", len(header.BatchRefs))
	}
	if len(header.Parents) != 1 {
		t.Errorf("Parents count mismatch: expected 1, got %d", len(header.Parents))
	}
	if header.Digest.IsEmpty() {
		t.Error("Digest should not be empty")
	}
	if header.Timestamp == 0 {
		t.Error("Timestamp should be set")
	}
}

func TestHeaderComputeDigest(t *testing.T) {
	header := NewHeader(1, 10, 1, nil, nil)

	// Digest should be deterministic
	digest1 := header.ComputeDigest()
	digest2 := header.ComputeDigest()
	if !digest1.Equal(digest2) {
		t.Error("ComputeDigest should be deterministic")
	}

	// Different headers should have different digests
	header2 := NewHeader(2, 10, 1, nil, nil)
	if header.Digest.Equal(header2.Digest) {
		t.Error("Different headers should have different digests")
	}
}

func TestHeaderSignAndVerify(t *testing.T) {
	signer, err := GenerateEd25519Signer(0)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	header := NewHeader(0, 10, 1, nil, nil)

	if header.IsSigned() {
		t.Error("New header should not be signed")
	}

	err = header.Sign(signer)
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	if !header.IsSigned() {
		t.Error("Header should be signed after Sign()")
	}

	if !header.Verify(signer.PublicKey()) {
		t.Error("Signature should verify with correct public key")
	}

	// Wrong public key should fail
	otherSigner, _ := GenerateEd25519Signer(1)
	if header.Verify(otherSigner.PublicKey()) {
		t.Error("Signature should not verify with wrong public key")
	}
}

func TestHeaderIsEmpty(t *testing.T) {
	empty := NewHeader(0, 0, 0, nil, nil)
	if !empty.IsEmpty() {
		t.Error("Header with no batches should be empty")
	}

	nonEmpty := NewHeader(0, 0, 0, []BatchDigest{{Digest: HashBytes([]byte("batch"))}}, nil)
	if nonEmpty.IsEmpty() {
		t.Error("Header with batches should not be empty")
	}
}

func TestHeaderClone(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	original := NewHeader(1, 10, 1,
		[]BatchDigest{{Digest: HashBytes([]byte("batch"))}},
		[]CertificateRef{{Digest: HashBytes([]byte("parent")), Round: 9}})
	_ = original.Sign(signer)

	clone := original.Clone()

	if !original.Digest.Equal(clone.Digest) {
		t.Error("Clone should have same digest")
	}
	if original.Author != clone.Author {
		t.Error("Clone should have same Author")
	}
	if !original.Signature.IsEmpty() && !clone.Signature.IsEmpty() {
		if original.Signature != clone.Signature {
			t.Error("Clone should have same Signature")
		}
	}

	// Modify clone should not affect original
	clone.BatchRefs[0].WorkerID = 99
	if original.BatchRefs[0].WorkerID == 99 {
		t.Error("Modifying clone should not affect original")
	}
}
