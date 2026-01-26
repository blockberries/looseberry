package store

import (
	"testing"

	"github.com/blockberries/looseberry/types"
)

func createTestCertificate(t *testing.T, author uint16, round uint64) *types.Certificate {
	t.Helper()

	// Create signers
	signers := make([]*types.Ed25519Signer, 4)
	validators := make([]*types.Validator, 4)
	for i := 0; i < 4; i++ {
		signer, err := types.GenerateEd25519Signer(uint16(i))
		if err != nil {
			t.Fatalf("Failed to generate signer: %v", err)
		}
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	// Create header
	header := types.NewHeader(author, round, 1, nil, nil)
	_ = header.Sign(signers[author])

	// Create votes (2f+1 = 3)
	votes := make([]types.Vote, 3)
	for i := 0; i < 3; i++ {
		vote := types.NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		votes[i] = *vote
	}

	return types.NewCertificate(header, votes)
}

func TestMemoryCertificateStoreSaveAndGet(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	cert := createTestCertificate(t, 0, 10)

	// Save
	err := store.SaveCertificate(cert)
	if err != nil {
		t.Fatalf("SaveCertificate failed: %v", err)
	}

	// Get
	retrieved, err := store.GetCertificate(cert.Digest())
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}

	if !retrieved.Digest().Equal(cert.Digest()) {
		t.Error("Retrieved certificate digest mismatch")
	}

	// Modify retrieved should not affect stored
	retrieved.Votes[0].Validator = 99
	original, _ := store.GetCertificate(cert.Digest())
	if original.Votes[0].Validator == 99 {
		t.Error("Modifying retrieved cert should not affect stored")
	}
}

func TestMemoryCertificateStoreHasCertificate(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	cert := createTestCertificate(t, 0, 10)

	if store.HasCertificate(cert.Digest()) {
		t.Error("Should not have certificate before save")
	}

	_ = store.SaveCertificate(cert)

	if !store.HasCertificate(cert.Digest()) {
		t.Error("Should have certificate after save")
	}
}

func TestMemoryCertificateStoreGetByRound(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	// Create certificates for multiple rounds
	cert1 := createTestCertificate(t, 0, 10)
	cert2 := createTestCertificate(t, 1, 10)
	cert3 := createTestCertificate(t, 0, 11)

	_ = store.SaveCertificate(cert1)
	_ = store.SaveCertificate(cert2)
	_ = store.SaveCertificate(cert3)

	// Get round 10
	certs, err := store.GetCertificatesByRound(10)
	if err != nil {
		t.Fatalf("GetCertificatesByRound failed: %v", err)
	}

	if len(certs) != 2 {
		t.Errorf("Expected 2 certs for round 10, got %d", len(certs))
	}

	// Get round 11
	certs, err = store.GetCertificatesByRound(11)
	if err != nil {
		t.Fatalf("GetCertificatesByRound failed: %v", err)
	}

	if len(certs) != 1 {
		t.Errorf("Expected 1 cert for round 11, got %d", len(certs))
	}

	// Get non-existent round
	certs, err = store.GetCertificatesByRound(99)
	if err != nil {
		t.Fatalf("GetCertificatesByRound failed: %v", err)
	}

	if len(certs) != 0 {
		t.Error("Expected empty slice for non-existent round")
	}
}

func TestMemoryCertificateStoreGetForValidator(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	cert0 := createTestCertificate(t, 0, 10)
	cert1 := createTestCertificate(t, 1, 10)

	_ = store.SaveCertificate(cert0)
	_ = store.SaveCertificate(cert1)

	// Get cert from validator 0
	retrieved, err := store.GetCertificateForValidator(10, 0)
	if err != nil {
		t.Fatalf("GetCertificateForValidator failed: %v", err)
	}

	if retrieved.Author() != 0 {
		t.Errorf("Expected author 0, got %d", retrieved.Author())
	}

	// Get cert from validator 1
	retrieved, err = store.GetCertificateForValidator(10, 1)
	if err != nil {
		t.Fatalf("GetCertificateForValidator failed: %v", err)
	}

	if retrieved.Author() != 1 {
		t.Errorf("Expected author 1, got %d", retrieved.Author())
	}

	// Non-existent validator
	_, err = store.GetCertificateForValidator(10, 99)
	if err != types.ErrCertificateNotFound {
		t.Errorf("Expected ErrCertificateNotFound, got: %v", err)
	}
}

func TestMemoryCertificateStoreDeleteBefore(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	cert1 := createTestCertificate(t, 0, 5)
	cert2 := createTestCertificate(t, 0, 10)
	cert3 := createTestCertificate(t, 0, 15)

	_ = store.SaveCertificate(cert1)
	_ = store.SaveCertificate(cert2)
	_ = store.SaveCertificate(cert3)

	if store.Len() != 3 {
		t.Fatalf("Expected 3 certs, got %d", store.Len())
	}

	// Delete before round 10
	err := store.DeleteCertificatesBefore(10)
	if err != nil {
		t.Fatalf("DeleteCertificatesBefore failed: %v", err)
	}

	// Cert from round 5 should be deleted
	if store.HasCertificate(cert1.Digest()) {
		t.Error("Cert from round 5 should be deleted")
	}

	// Certs from round 10 and 15 should remain
	if !store.HasCertificate(cert2.Digest()) {
		t.Error("Cert from round 10 should remain")
	}
	if !store.HasCertificate(cert3.Digest()) {
		t.Error("Cert from round 15 should remain")
	}

	// Validator index should also be cleaned up
	_, err = store.GetCertificateForValidator(5, 0)
	if err != types.ErrCertificateNotFound {
		t.Error("Validator index for round 5 should be deleted")
	}
}

func TestMemoryCertificateStoreIdempotent(t *testing.T) {
	store := NewMemoryCertificateStore()
	defer store.Close()

	cert := createTestCertificate(t, 0, 10)

	// Save twice
	_ = store.SaveCertificate(cert)
	_ = store.SaveCertificate(cert)

	if store.Len() != 1 {
		t.Errorf("SaveCertificate should be idempotent, got %d certs", store.Len())
	}
}

func TestMemoryCertificateStoreClose(t *testing.T) {
	store := NewMemoryCertificateStore()

	cert := createTestCertificate(t, 0, 10)
	_ = store.SaveCertificate(cert)

	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should fail
	err = store.SaveCertificate(cert)
	if err != types.ErrNotRunning {
		t.Errorf("SaveCertificate after close should return ErrNotRunning, got: %v", err)
	}

	_, err = store.GetCertificate(cert.Digest())
	if err != types.ErrNotRunning {
		t.Errorf("GetCertificate after close should return ErrNotRunning, got: %v", err)
	}
}
