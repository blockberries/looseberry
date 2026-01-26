package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blockberries/looseberry/types"
)

func TestLevelDBCertificateStoreSaveAndGet(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cert := createTestCertificate(t, 0, 10)

	// Save
	err = store.SaveCertificate(cert)
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

func TestLevelDBCertificateStoreHasCertificate(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
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

func TestLevelDBCertificateStoreGetByRound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
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

func TestLevelDBCertificateStoreGetForValidator(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
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

func TestLevelDBCertificateStoreDeleteBefore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
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
	err = store.DeleteCertificatesBefore(10)
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

func TestLevelDBCertificateStoreIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cert := createTestCertificate(t, 0, 10)

	// Save twice
	_ = store.SaveCertificate(cert)
	_ = store.SaveCertificate(cert)

	if store.Len() != 1 {
		t.Errorf("SaveCertificate should be idempotent, got %d certs", store.Len())
	}
}

func TestLevelDBCertificateStoreClose(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	cert := createTestCertificate(t, 0, 10)
	_ = store.SaveCertificate(cert)

	err = store.Close()
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

func TestLevelDBCertificateStorePersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "certs")

	// Create store and save a certificate
	store1, err := NewLevelDBCertificateStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	cert := createTestCertificate(t, 0, 10)
	_ = store1.SaveCertificate(cert)
	digest := cert.Digest()

	// Close store
	store1.Close()

	// Reopen store and verify certificate exists
	store2, err := NewLevelDBCertificateStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	if !store2.HasCertificate(digest) {
		t.Error("Certificate should persist after reopen")
	}

	retrieved, err := store2.GetCertificate(digest)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}

	if !retrieved.Digest().Equal(digest) {
		t.Error("Retrieved certificate digest mismatch after reopen")
	}
}

func TestLevelDBCertificateStoreHighestRound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	if store.HighestRound() != 0 {
		t.Error("Initial highest round should be 0")
	}

	_ = store.SaveCertificate(createTestCertificate(t, 0, 5))
	if store.HighestRound() != 5 {
		t.Errorf("Expected highest round 5, got %d", store.HighestRound())
	}

	_ = store.SaveCertificate(createTestCertificate(t, 0, 10))
	if store.HighestRound() != 10 {
		t.Errorf("Expected highest round 10, got %d", store.HighestRound())
	}

	// Adding lower round should not change highest
	_ = store.SaveCertificate(createTestCertificate(t, 0, 3))
	if store.HighestRound() != 10 {
		t.Errorf("Expected highest round still 10, got %d", store.HighestRound())
	}
}

func TestLevelDBCertificateStoreNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLevelDBCertificateStore(filepath.Join(dir, "certs"))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.GetCertificate(types.HashBytes([]byte("nonexistent")))
	if err != types.ErrCertificateNotFound {
		t.Errorf("Expected ErrCertificateNotFound, got: %v", err)
	}
}

func TestLevelDBCertificateStoreOpenFailed(t *testing.T) {
	// Try to open a database in a non-existent directory without write permissions
	if os.Getuid() == 0 {
		t.Skip("Cannot test permission errors as root")
	}

	// Create a read-only directory
	dir := t.TempDir()
	readOnlyDir := filepath.Join(dir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0444); err != nil {
		t.Fatalf("Failed to create read-only directory: %v", err)
	}
	defer func() { _ = os.Chmod(readOnlyDir, 0755) }() // Restore permissions for cleanup

	_, err := NewLevelDBCertificateStore(filepath.Join(readOnlyDir, "db"))
	if err == nil {
		t.Error("Expected error opening database in read-only directory")
	}
}
