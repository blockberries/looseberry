package store

import (
	"path/filepath"
	"testing"

	"github.com/blockberries/looseberry/types"
)

// TestCramberryBatchRoundTrip ensures the cramberry-encoded batch payload
// survives encode→decode with every field intact. Replaces the previous
// gob-based codec; if cramberry ever drops a field this test fails first.
func TestCramberryBatchRoundTrip(t *testing.T) {
	txs := []types.Transaction{
		types.Transaction([]byte("tx-one")),
		types.Transaction([]byte("tx-two")),
		types.Transaction([]byte{0x00, 0x01, 0x02, 0x03}),
	}
	original := types.NewBatch(7, 13, 42, txs)

	data, err := encodeBatch(original)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encodeBatch returned empty buffer")
	}

	decoded, err := decodeBatch(data)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}

	if decoded.WorkerID != original.WorkerID {
		t.Errorf("WorkerID: got %d, want %d", decoded.WorkerID, original.WorkerID)
	}
	if decoded.ValidatorID != original.ValidatorID {
		t.Errorf("ValidatorID: got %d, want %d", decoded.ValidatorID, original.ValidatorID)
	}
	if decoded.Round != original.Round {
		t.Errorf("Round: got %d, want %d", decoded.Round, original.Round)
	}
	if decoded.Timestamp != original.Timestamp {
		t.Errorf("Timestamp: got %d, want %d", decoded.Timestamp, original.Timestamp)
	}
	if !decoded.Digest.Equal(original.Digest) {
		t.Errorf("Digest: got %s, want %s", decoded.Digest, original.Digest)
	}
	if len(decoded.Transactions) != len(original.Transactions) {
		t.Fatalf("Transactions length: got %d, want %d",
			len(decoded.Transactions), len(original.Transactions))
	}
	for i := range original.Transactions {
		if !decoded.Transactions[i].Equal(original.Transactions[i]) {
			t.Errorf("Transactions[%d]: got %x, want %x", i, decoded.Transactions[i], original.Transactions[i])
		}
	}

	// Re-encoding must yield identical bytes (determinism).
	data2, err := encodeBatch(decoded)
	if err != nil {
		t.Fatalf("re-encodeBatch: %v", err)
	}
	if string(data) != string(data2) {
		t.Errorf("encode is non-deterministic: %d bytes vs %d bytes", len(data), len(data2))
	}
}

// TestCramberryCertificateRoundTrip ensures certificates survive encode→decode.
func TestCramberryCertificateRoundTrip(t *testing.T) {
	cert := createTestCertificate(t, 2, 99)

	data, err := encodeCertificate(cert)
	if err != nil {
		t.Fatalf("encodeCertificate: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encodeCertificate returned empty buffer")
	}

	decoded, err := decodeCertificate(data)
	if err != nil {
		t.Fatalf("decodeCertificate: %v", err)
	}

	if decoded.Author() != cert.Author() {
		t.Errorf("Author: got %d, want %d", decoded.Author(), cert.Author())
	}
	if decoded.Round() != cert.Round() {
		t.Errorf("Round: got %d, want %d", decoded.Round(), cert.Round())
	}
	if !decoded.Digest().Equal(cert.Digest()) {
		t.Errorf("Digest: got %s, want %s", decoded.Digest(), cert.Digest())
	}
	if len(decoded.Votes) != len(cert.Votes) {
		t.Fatalf("Votes length: got %d, want %d", len(decoded.Votes), len(cert.Votes))
	}
	for i := range cert.Votes {
		if decoded.Votes[i].Validator != cert.Votes[i].Validator {
			t.Errorf("Votes[%d].Validator mismatch", i)
		}
		if !decoded.Votes[i].HeaderDigest.Equal(cert.Votes[i].HeaderDigest) {
			t.Errorf("Votes[%d].HeaderDigest mismatch", i)
		}
		if !decoded.Votes[i].Signature.Equal(cert.Votes[i].Signature) {
			t.Errorf("Votes[%d].Signature mismatch", i)
		}
	}

	// Determinism
	data2, err := encodeCertificate(decoded)
	if err != nil {
		t.Fatalf("re-encodeCertificate: %v", err)
	}
	if string(data) != string(data2) {
		t.Errorf("encode is non-deterministic: %d bytes vs %d bytes", len(data), len(data2))
	}
}

// TestCramberryLevelDBBatchRoundTrip exercises the codec end-to-end through
// the LevelDB store: persist a batch, reopen the store, read it back.
func TestCramberryLevelDBBatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "batches")

	original := types.NewBatch(1, 2, 3, []types.Transaction{
		types.Transaction([]byte("alpha")),
		types.Transaction([]byte("beta")),
	})

	{
		s, err := NewLevelDBBatchStore(dbPath)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		if err := s.SaveBatch(original); err != nil {
			t.Fatalf("SaveBatch: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	{
		s, err := NewLevelDBBatchStore(dbPath)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer s.Close()
		got, err := s.GetBatch(original.Digest)
		if err != nil {
			t.Fatalf("GetBatch after reopen: %v", err)
		}
		if !got.Digest.Equal(original.Digest) {
			t.Errorf("Digest mismatch after reopen")
		}
		if len(got.Transactions) != 2 {
			t.Errorf("Transactions length: got %d, want 2", len(got.Transactions))
		}
	}
}

// TestCramberryLevelDBCertificateRoundTrip exercises the cert codec end-to-end.
func TestCramberryLevelDBCertificateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "certs")
	cert := createTestCertificate(t, 1, 55)

	{
		s, err := NewLevelDBCertificateStore(dbPath)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		if err := s.SaveCertificate(cert); err != nil {
			t.Fatalf("SaveCertificate: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	{
		s, err := NewLevelDBCertificateStore(dbPath)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer s.Close()
		got, err := s.GetCertificate(cert.Digest())
		if err != nil {
			t.Fatalf("GetCertificate after reopen: %v", err)
		}
		if !got.Digest().Equal(cert.Digest()) {
			t.Errorf("Digest mismatch after reopen")
		}
		if got.Round() != cert.Round() {
			t.Errorf("Round: got %d, want %d", got.Round(), cert.Round())
		}
		if len(got.Votes) != len(cert.Votes) {
			t.Errorf("Votes length: got %d, want %d", len(got.Votes), len(cert.Votes))
		}
	}
}
