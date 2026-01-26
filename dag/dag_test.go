package dag

import (
	"testing"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func createTestCertificate(t *testing.T, author uint16, round uint64, parents []types.CertificateRef) *types.Certificate {
	t.Helper()

	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	header := types.NewHeader(author, round, 0, nil, parents)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	vote := types.NewVote(header.Digest, author)
	if err := vote.Sign(signer); err != nil {
		t.Fatalf("Failed to sign vote: %v", err)
	}

	return types.NewCertificate(header, []types.Vote{*vote})
}

func TestDAGAddCertificate(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	cert := createTestCertificate(t, 0, 10, nil)
	err := dag.AddCertificate(cert)
	if err != nil {
		t.Fatalf("AddCertificate failed: %v", err)
	}

	// Verify it's in the DAG
	if !dag.HasCertificate(cert.Digest()) {
		t.Error("Certificate should be in DAG")
	}

	// Verify highest round updated
	if dag.HighestRound() != 10 {
		t.Errorf("Expected highest round 10, got %d", dag.HighestRound())
	}

	// Verify it's in store
	if !certStore.HasCertificate(cert.Digest()) {
		t.Error("Certificate should be in store")
	}
}

func TestDAGAddDuplicateCertificate(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	cert := createTestCertificate(t, 0, 10, nil)
	_ = dag.AddCertificate(cert)

	// Add duplicate
	err := dag.AddCertificate(cert)
	if err != types.ErrDuplicateHeader {
		t.Errorf("Expected ErrDuplicateHeader, got: %v", err)
	}
}

func TestDAGAddNilCertificate(t *testing.T) {
	dag := New(nil, DefaultConfig())

	err := dag.AddCertificate(nil)
	if err != types.ErrInvalidCertificate {
		t.Errorf("Expected ErrInvalidCertificate, got: %v", err)
	}
}

func TestDAGGetCertificate(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	cert := createTestCertificate(t, 0, 10, nil)
	_ = dag.AddCertificate(cert)

	// Get from memory
	retrieved, err := dag.GetCertificate(cert.Digest())
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}

	if !retrieved.Digest().Equal(cert.Digest()) {
		t.Error("Retrieved certificate digest mismatch")
	}
}

func TestDAGGetCertificateNotFound(t *testing.T) {
	dag := New(nil, DefaultConfig())

	unknownDigest := types.HashBytes([]byte("unknown"))
	_, err := dag.GetCertificate(unknownDigest)
	if err != types.ErrCertificateNotFound {
		t.Errorf("Expected ErrCertificateNotFound, got: %v", err)
	}
}

func TestDAGGetCertificatesForRound(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Add 3 certificates for round 10
	for i := uint16(0); i < 3; i++ {
		cert := createTestCertificate(t, i, 10, nil)
		_ = dag.AddCertificate(cert)
	}

	certs := dag.GetCertificatesForRound(10)
	if len(certs) != 3 {
		t.Errorf("Expected 3 certificates, got %d", len(certs))
	}
}

func TestDAGGetCertificatesForRoundEmpty(t *testing.T) {
	dag := New(nil, DefaultConfig())

	certs := dag.GetCertificatesForRound(99)
	if len(certs) != 0 {
		t.Errorf("Expected 0 certificates, got %d", len(certs))
	}
}

func TestDAGGetCertificateForValidator(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	cert := createTestCertificate(t, 5, 10, nil)
	_ = dag.AddCertificate(cert)

	retrieved, ok := dag.GetCertificateForValidator(10, 5)
	if !ok {
		t.Error("Should find certificate for validator")
	}
	if !retrieved.Digest().Equal(cert.Digest()) {
		t.Error("Retrieved certificate mismatch")
	}

	// Try non-existent validator
	_, ok = dag.GetCertificateForValidator(10, 99)
	if ok {
		t.Error("Should not find certificate for non-existent validator")
	}
}

func TestDAGCanAdvanceToRound(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Can always advance to round 0
	if !dag.CanAdvanceToRound(0, 3) {
		t.Error("Should be able to advance to round 0")
	}

	// Cannot advance to round 1 without round 0 certificates
	if dag.CanAdvanceToRound(1, 3) {
		t.Error("Should not advance to round 1 without round 0 certs")
	}

	// Add 3 certificates for round 0 (quorum = 3)
	for i := uint16(0); i < 3; i++ {
		cert := createTestCertificate(t, i, 0, nil)
		_ = dag.AddCertificate(cert)
	}

	// Now can advance to round 1
	if !dag.CanAdvanceToRound(1, 3) {
		t.Error("Should be able to advance to round 1 with quorum")
	}
}

func TestDAGCertificateCount(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	if dag.CertificateCount(10) != 0 {
		t.Error("Empty round should have 0 certificates")
	}

	cert := createTestCertificate(t, 0, 10, nil)
	_ = dag.AddCertificate(cert)

	if dag.CertificateCount(10) != 1 {
		t.Error("Round should have 1 certificate")
	}
}

func TestDAGCommittedRound(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	if dag.CommittedRound() != 0 {
		t.Error("Initial committed round should be 0")
	}

	// Add certificates
	for i := uint16(0); i < 3; i++ {
		cert := createTestCertificate(t, i, 5, nil)
		_ = dag.AddCertificate(cert)
	}

	dag.SetCommittedRound(5)

	if dag.CommittedRound() != 5 {
		t.Errorf("Expected committed round 5, got %d", dag.CommittedRound())
	}

	// Verify round is marked as committed
	rd := dag.GetRound(5)
	if rd == nil {
		t.Fatal("Round 5 should exist")
	}
	if !rd.IsCommitted() {
		t.Error("Round 5 should be marked as committed")
	}
}

func TestDAGCausalHistory(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Create chain of certificates
	// Round 0: cert0
	cert0 := createTestCertificate(t, 0, 0, nil)
	_ = dag.AddCertificate(cert0)

	// Round 1: cert1 with parent cert0
	parents1 := []types.CertificateRef{{
		Digest: cert0.Digest(),
		Round:  0,
	}}
	cert1 := createTestCertificate(t, 0, 1, parents1)
	_ = dag.AddCertificate(cert1)

	// Round 2: cert2 with parent cert1
	parents2 := []types.CertificateRef{{
		Digest: cert1.Digest(),
		Round:  1,
	}}
	cert2 := createTestCertificate(t, 0, 2, parents2)
	_ = dag.AddCertificate(cert2)

	// Get causal history of cert2
	history := dag.CausalHistory(cert2)
	if len(history) != 3 {
		t.Errorf("Expected 3 certificates in history, got %d", len(history))
	}

	// Verify all certs are in history
	foundCert0, foundCert1, foundCert2 := false, false, false
	for _, c := range history {
		if c.Digest().Equal(cert0.Digest()) {
			foundCert0 = true
		}
		if c.Digest().Equal(cert1.Digest()) {
			foundCert1 = true
		}
		if c.Digest().Equal(cert2.Digest()) {
			foundCert2 = true
		}
	}

	if !foundCert0 || !foundCert1 || !foundCert2 {
		t.Error("Not all certificates found in history")
	}
}

func TestDAGCausalHistoryNil(t *testing.T) {
	dag := New(nil, DefaultConfig())

	history := dag.CausalHistory(nil)
	if history != nil {
		t.Error("Nil certificate should return nil history")
	}
}

func TestDAGCausalHistoryCaching(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	cert := createTestCertificate(t, 0, 0, nil)
	_ = dag.AddCertificate(cert)

	// First call - builds cache
	history1 := dag.CausalHistory(cert)
	// Second call - uses cache
	history2 := dag.CausalHistory(cert)

	if len(history1) != len(history2) {
		t.Error("Cached history should be same length")
	}
}

func TestDAGGetOrderedCertificates(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Add certificates for multiple rounds
	for round := uint64(0); round < 3; round++ {
		for validator := uint16(0); validator < 3; validator++ {
			cert := createTestCertificate(t, validator, round, nil)
			_ = dag.AddCertificate(cert)
		}
	}

	// Get ordered certificates from round 0 to 2
	ordered := dag.GetOrderedCertificates(0, 2)
	if len(ordered) != 9 {
		t.Errorf("Expected 9 certificates, got %d", len(ordered))
	}

	// Verify ordering: by round, then by validator
	expectedRound := uint64(0)
	expectedValidator := uint16(0)
	for _, cert := range ordered {
		if cert.Round() != expectedRound {
			t.Errorf("Expected round %d, got %d", expectedRound, cert.Round())
		}
		if cert.Author() != expectedValidator {
			t.Errorf("Expected validator %d, got %d", expectedValidator, cert.Author())
		}

		expectedValidator++
		if expectedValidator >= 3 {
			expectedValidator = 0
			expectedRound++
		}
	}
}

func TestDAGGetOrderedCertificatesInvalidRange(t *testing.T) {
	dag := New(nil, DefaultConfig())

	// fromRound > toRound
	certs := dag.GetOrderedCertificates(10, 5)
	if len(certs) != 0 {
		t.Error("Invalid range should return empty slice")
	}
}

func TestDAGPruneRound(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Add certificates
	for round := uint64(0); round < 5; round++ {
		cert := createTestCertificate(t, 0, round, nil)
		_ = dag.AddCertificate(cert)
	}

	if dag.RoundCount() != 5 {
		t.Errorf("Expected 5 rounds, got %d", dag.RoundCount())
	}

	// Prune round 0
	dag.PruneRound(0)

	if dag.RoundCount() != 4 {
		t.Errorf("Expected 4 rounds after prune, got %d", dag.RoundCount())
	}

	// Round 0 should be gone from memory
	rd := dag.GetRound(0)
	if rd != nil {
		t.Error("Round 0 should be pruned from memory")
	}
}

func TestDAGPruneRoundsBefore(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	dag := New(certStore, DefaultConfig())

	// Add certificates
	for round := uint64(0); round < 10; round++ {
		cert := createTestCertificate(t, 0, round, nil)
		_ = dag.AddCertificate(cert)
	}

	// Prune rounds before 5
	dag.PruneRoundsBefore(5)

	if dag.RoundCount() != 5 {
		t.Errorf("Expected 5 rounds after prune, got %d", dag.RoundCount())
	}

	// Rounds 0-4 should be gone
	for round := uint64(0); round < 5; round++ {
		if dag.GetRound(round) != nil {
			t.Errorf("Round %d should be pruned", round)
		}
	}

	// Rounds 5-9 should exist
	for round := uint64(5); round < 10; round++ {
		if dag.GetRound(round) == nil {
			t.Errorf("Round %d should exist", round)
		}
	}
}

func TestDAGLoadRound(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	defer certStore.Close()

	// First DAG - add certificates
	dag1 := New(certStore, DefaultConfig())
	cert := createTestCertificate(t, 0, 10, nil)
	_ = dag1.AddCertificate(cert)

	// Second DAG - load from store
	dag2 := New(certStore, DefaultConfig())
	err := dag2.LoadRound(10)
	if err != nil {
		t.Fatalf("LoadRound failed: %v", err)
	}

	// Verify certificate is loaded
	if dag2.CertificateCount(10) != 1 {
		t.Error("Certificate should be loaded")
	}

	if dag2.HighestRound() != 10 {
		t.Errorf("Expected highest round 10, got %d", dag2.HighestRound())
	}
}

func TestRoundDataBasics(t *testing.T) {
	rd := NewRoundData(10)

	if rd.Round != 10 {
		t.Errorf("Expected round 10, got %d", rd.Round)
	}

	if rd.Count() != 0 {
		t.Error("New round should be empty")
	}

	if rd.IsCommitted() {
		t.Error("New round should not be committed")
	}

	// Add certificate
	cert := createTestCertificate(t, 0, 10, nil)
	rd.AddCertificate(cert)

	if rd.Count() != 1 {
		t.Error("Round should have 1 certificate")
	}

	if !rd.HasCertificate(0) {
		t.Error("Round should have certificate from validator 0")
	}

	// Set committed
	rd.SetCommitted()
	if !rd.IsCommitted() {
		t.Error("Round should be committed")
	}

	// Get all certificates
	certs := rd.GetAllCertificates()
	if len(certs) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(certs))
	}
}

func TestDAGDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxCachedRounds <= 0 {
		t.Error("MaxCachedRounds should be positive")
	}

	if cfg.MaxHistoryDepth <= 0 {
		t.Error("MaxHistoryDepth should be positive")
	}
}

func TestDAGNoStore(t *testing.T) {
	// DAG without persistent store
	dag := New(nil, DefaultConfig())

	cert := createTestCertificate(t, 0, 10, nil)
	err := dag.AddCertificate(cert)
	if err != nil {
		t.Fatalf("AddCertificate failed: %v", err)
	}

	if !dag.HasCertificate(cert.Digest()) {
		t.Error("Certificate should be in memory")
	}

	// GetCertificatesForRound without store
	certs := dag.GetCertificatesForRound(10)
	if len(certs) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(certs))
	}

	// GetCertificateForValidator without store for non-existent
	_, ok := dag.GetCertificateForValidator(99, 0)
	if ok {
		t.Error("Should not find non-existent certificate")
	}
}
