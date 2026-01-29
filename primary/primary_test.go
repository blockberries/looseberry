package primary

import (
	"sync"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func createTestValidatorSet(t *testing.T, count int) (*types.SimpleValidatorSet, []*types.Ed25519Signer) {
	t.Helper()

	signers := make([]*types.Ed25519Signer, count)
	validators := make([]*types.Validator, count)

	for i := range count {
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

	vs := types.NewSimpleValidatorSet(validators, 0)
	return vs, signers
}

func TestPrimaryStartStop(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 1 * time.Second

	p := New(0, signers[0], cfg, certStore, batchStore, vs)

	if p.IsRunning() {
		t.Error("Should not be running initially")
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !p.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := p.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := p.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if p.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := p.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestPrimaryAddBatchDigest(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 1 * time.Second
	p := New(0, signers[0], cfg, certStore, batchStore, vs)

	digest := types.BatchDigest{
		Digest:      types.HashBytes([]byte("batch1")),
		WorkerID:    0,
		ValidatorID: 0,
	}

	p.AddBatchDigest(digest)

	p.digestsMu.Lock()
	count := len(p.batchDigests)
	p.digestsMu.Unlock()

	if count != 1 {
		t.Errorf("Expected 1 batch digest, got %d", count)
	}
}

func TestPrimaryHeaderCreation(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	var createdHeader *types.Header
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 50 * time.Millisecond
	p := New(0, signers[0], cfg, certStore, batchStore, vs)
	p.SetHeaderCallback(func(header *types.Header) {
		mu.Lock()
		createdHeader = header
		mu.Unlock()
	})

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Add batch digest
	digest := types.BatchDigest{
		Digest:      types.HashBytes([]byte("batch1")),
		WorkerID:    0,
		ValidatorID: 0,
	}
	p.AddBatchDigest(digest)

	// Wait for header creation
	time.Sleep(cfg.HeaderTimeout + 50*time.Millisecond)

	mu.Lock()
	header := createdHeader
	mu.Unlock()

	if header == nil {
		t.Fatal("Header should be created")
	}

	if header.Author != 0 {
		t.Errorf("Expected author 0, got %d", header.Author)
	}

	if len(header.BatchRefs) != 1 {
		t.Errorf("Expected 1 batch ref, got %d", len(header.BatchRefs))
	}

	// Header should be tracked for votes
	if !p.voteTracker.HasHeader(header.Digest) {
		t.Error("Header should be tracked in vote tracker")
	}
}

func TestPrimaryHandleVote(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	var createdHeader *types.Header
	var createdCert *types.Certificate
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 50 * time.Millisecond
	p := New(0, signers[0], cfg, certStore, batchStore, vs)
	p.SetHeaderCallback(func(header *types.Header) {
		mu.Lock()
		createdHeader = header
		mu.Unlock()
	})
	p.SetCertificateCallback(func(cert *types.Certificate) {
		mu.Lock()
		createdCert = cert
		mu.Unlock()
	})

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Add batch digest and wait for header
	digest := types.BatchDigest{
		Digest:      types.HashBytes([]byte("batch1")),
		WorkerID:    0,
		ValidatorID: 0,
	}
	p.AddBatchDigest(digest)
	time.Sleep(cfg.HeaderTimeout + 50*time.Millisecond)

	// Get the created header from callback
	mu.Lock()
	header := createdHeader
	mu.Unlock()

	if header == nil {
		t.Fatal("No header created")
	}

	// Send votes (need quorum = 3)
	for i := 1; i < 4; i++ {
		vote := types.NewVote(header.Digest, uint16(i))
		_ = vote.Sign(signers[i])
		p.HandleVote(vote)
	}

	mu.Lock()
	cert := createdCert
	mu.Unlock()

	if cert == nil {
		t.Fatal("Certificate should be formed with quorum votes")
	}

	if len(cert.Votes) < 3 {
		t.Errorf("Certificate should have at least 3 votes, got %d", len(cert.Votes))
	}

	// Certificate should be stored
	if !certStore.HasCertificate(cert.Digest()) {
		t.Error("Certificate should be stored")
	}
}

func TestPrimaryHandleHeader(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	var sentVote *types.Vote
	var mu sync.Mutex

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 1 * time.Second
	p := New(0, signers[0], cfg, certStore, batchStore, vs)
	p.SetVoteCallback(func(vote *types.Vote, to uint16) {
		mu.Lock()
		sentVote = vote
		mu.Unlock()
	})

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create header from another validator
	header := types.NewHeader(1, 0, 0, nil, nil)
	_ = header.Sign(signers[1])

	// Handle header
	err := p.HandleHeader(header)
	if err != nil {
		t.Fatalf("HandleHeader failed: %v", err)
	}

	mu.Lock()
	vote := sentVote
	mu.Unlock()

	if vote == nil {
		t.Fatal("Vote should be sent")
	}

	if !vote.HeaderDigest.Equal(header.Digest) {
		t.Error("Vote should be for the received header")
	}

	if vote.Validator != 0 {
		t.Errorf("Vote validator should be 0, got %d", vote.Validator)
	}
}

func TestPrimaryValidateHeader(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	p := New(0, signers[0], cfg, certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Valid header
	validHeader := types.NewHeader(1, 0, 0, nil, nil)
	_ = validHeader.Sign(signers[1])
	if err := p.validateHeader(validHeader); err != nil {
		t.Errorf("Valid header should pass validation: %v", err)
	}

	// Invalid signature
	badSig, _ := types.GenerateEd25519Signer(99)
	badHeader := types.NewHeader(1, 0, 0, nil, nil)
	_ = badHeader.Sign(badSig)
	if err := p.validateHeader(badHeader); err != types.ErrInvalidSignature {
		t.Errorf("Expected ErrInvalidSignature, got: %v", err)
	}

	// Round too far ahead
	futureHeader := types.NewHeader(1, 100, 0, nil, nil)
	_ = futureHeader.Sign(signers[1])
	if err := p.validateHeader(futureHeader); err != types.ErrRoundMismatch {
		t.Errorf("Expected ErrRoundMismatch, got: %v", err)
	}

	// Wrong epoch
	wrongEpochHeader := types.NewHeader(1, 0, 99, nil, nil)
	_ = wrongEpochHeader.Sign(signers[1])
	if err := p.validateHeader(wrongEpochHeader); err != types.ErrEpochMismatch {
		t.Errorf("Expected ErrEpochMismatch, got: %v", err)
	}
}

func TestPrimaryRoundAndEpoch(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	p := New(0, signers[0], cfg, certStore, batchStore, vs)

	if p.Round() != 0 {
		t.Error("Initial round should be 0")
	}

	if p.Epoch() != 0 {
		t.Error("Initial epoch should be 0")
	}

	p.SetRound(10)
	if p.Round() != 10 {
		t.Errorf("Expected round 10, got %d", p.Round())
	}

	p.SetEpoch(5)
	if p.Epoch() != 5 {
		t.Errorf("Expected epoch 5, got %d", p.Epoch())
	}
}

func TestPrimaryPendingVotes(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 100 * time.Millisecond
	p := New(0, signers[0], cfg, certStore, batchStore, vs)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = p.Stop() }()

	// Create a header digest that doesn't exist yet
	futureDigest := types.HashBytes([]byte("future_header"))

	// Send votes for the non-existent header
	for i := 1; i < 4; i++ {
		vote := types.NewVote(futureDigest, uint16(i))
		_ = vote.Sign(signers[i])
		p.HandleVote(vote)
	}

	// Votes should be buffered
	p.pendingMu.Lock()
	entry := p.pendingVotes[futureDigest]
	pendingCount := 0
	if entry != nil {
		pendingCount = len(entry.votes)
	}
	p.pendingMu.Unlock()

	if pendingCount != 3 {
		t.Errorf("Expected 3 pending votes, got %d", pendingCount)
	}
}

func TestPrimaryDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.HeaderTimeout <= 0 {
		t.Error("HeaderTimeout should be positive")
	}

	if cfg.MaxBatchesPerHeader <= 0 {
		t.Error("MaxBatchesPerHeader should be positive")
	}

	if cfg.VoteTimeout <= 0 {
		t.Error("VoteTimeout should be positive")
	}

	if cfg.MaxRoundGap <= 0 {
		t.Error("MaxRoundGap should be positive")
	}
}
