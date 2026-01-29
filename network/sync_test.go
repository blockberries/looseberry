package network

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/dag"
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

// createTestCertificateWithQuorum creates a certificate with proper quorum for testing.
func createTestCertificateWithQuorum(t *testing.T, author uint16, round uint64, signers []*types.Ed25519Signer, quorum int) *types.Certificate {
	t.Helper()

	header := types.NewHeader(author, round, 0, nil, nil)
	if err := header.Sign(signers[author]); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	// Create votes from enough signers to meet quorum
	votes := make([]types.Vote, quorum)
	for i := range quorum {
		vote := types.NewVote(header.Digest, uint16(i))
		if err := vote.Sign(signers[i]); err != nil {
			t.Fatalf("Failed to sign vote: %v", err)
		}
		votes[i] = *vote
	}

	return types.NewCertificate(header, votes)
}

func createTestCertificateWithBatches(t *testing.T, author uint16, round uint64, batchDigests []types.Hash) *types.Certificate {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	batchRefs := make([]types.BatchDigest, len(batchDigests))
	for i, d := range batchDigests {
		batchRefs[i] = types.BatchDigest{Digest: d}
	}

	header := types.NewHeader(author, round, 0, batchRefs, nil)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	vote := types.NewVote(header.Digest, author)
	if err := vote.Sign(signer); err != nil {
		t.Fatalf("Failed to sign vote: %v", err)
	}

	return types.NewCertificate(header, []types.Vote{*vote})
}

func TestSyncManagerStartStop(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network := NewMockNetwork(0, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	sm := NewSyncManager(d, batchStore, network, vs, DefaultSyncConfig())

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !sm.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := sm.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := sm.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if sm.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := sm.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestSyncManagerRequestSync(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network1 := NewMockNetwork(0, DefaultConfig())
	network2 := NewMockNetwork(1, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	network1.Connect(network2)

	sm := NewSyncManager(d, batchStore, network1, vs, DefaultSyncConfig())
	_ = network1.Start()
	_ = network2.Start()
	defer func() { _ = network1.Stop() }()
	defer func() { _ = network2.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// Request sync
	err := sm.RequestSync(1, 10, 20)
	if err != nil {
		t.Fatalf("RequestSync failed: %v", err)
	}

	// Check pending request
	if sm.GetPendingRequestCount() != 1 {
		t.Errorf("Expected 1 pending request, got %d", sm.GetPendingRequestCount())
	}

	// Check network2 received request
	select {
	case req := <-network2.SyncRequests():
		if req.FromRound != 10 || req.ToRound != 20 {
			t.Error("Sync request mismatch")
		}
		if req.Requester != 0 {
			t.Errorf("Expected requester 0, got %d", req.Requester)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync request")
	}
}

func TestSyncManagerHandleSyncRequest(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network1 := NewMockNetwork(0, DefaultConfig())
	network2 := NewMockNetwork(1, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	network1.Connect(network2)

	// Add certificates to DAG
	for round := uint64(0); round < 5; round++ {
		cert := createTestCertificate(t, 0, round)
		_ = d.AddCertificate(cert)
	}

	sm := NewSyncManager(d, batchStore, network1, vs, DefaultSyncConfig())
	_ = network1.Start()
	_ = network2.Start()
	defer func() { _ = network1.Stop() }()
	defer func() { _ = network2.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// Handle sync request
	req := &SyncRequest{
		FromRound: 0,
		ToRound:   4,
		Requester: 1,
	}

	err := sm.HandleSyncRequest(req)
	if err != nil {
		t.Fatalf("HandleSyncRequest failed: %v", err)
	}

	// Check network2 received response
	select {
	case msg := <-network2.SyncResponses():
		if len(msg.Response.Certificates) != 5 {
			t.Errorf("Expected 5 certificates, got %d", len(msg.Response.Certificates))
		}
		if msg.Response.FromRound != 0 || msg.Response.ToRound != 4 {
			t.Error("Sync response round mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync response")
	}
}

func TestSyncManagerHandleSyncRequestWithBatches(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network1 := NewMockNetwork(0, DefaultConfig())
	network2 := NewMockNetwork(1, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	network1.Connect(network2)

	// Create and store batches
	batch := createTestBatch(t, 0, 0, 10)
	_ = batchStore.SaveBatch(batch)

	// Add certificate referencing batch
	cert := createTestCertificateWithBatches(t, 0, 10, []types.Hash{batch.Digest})
	_ = d.AddCertificate(cert)

	sm := NewSyncManager(d, batchStore, network1, vs, DefaultSyncConfig())
	_ = network1.Start()
	_ = network2.Start()
	defer func() { _ = network1.Stop() }()
	defer func() { _ = network2.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	req := &SyncRequest{
		FromRound: 10,
		ToRound:   10,
		Requester: 1,
	}

	err := sm.HandleSyncRequest(req)
	if err != nil {
		t.Fatalf("HandleSyncRequest failed: %v", err)
	}

	select {
	case msg := <-network2.SyncResponses():
		if len(msg.Response.Certificates) != 1 {
			t.Errorf("Expected 1 certificate, got %d", len(msg.Response.Certificates))
		}
		if len(msg.Response.Batches) != 1 {
			t.Errorf("Expected 1 batch, got %d", len(msg.Response.Batches))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync response")
	}
}

func TestSyncManagerHandleSyncResponse(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network := NewMockNetwork(0, DefaultConfig())
	vs, signers := createTestValidatorSet(t, 4)

	sm := NewSyncManager(d, batchStore, network, vs, DefaultSyncConfig())
	_ = network.Start()
	defer func() { _ = network.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// Create sync response with properly signed certificate
	cert := createTestCertificateWithQuorum(t, 0, 10, signers, vs.Quorum())
	batch := createTestBatch(t, 0, 0, 10)
	resp := &SyncResponse{
		Certificates: []*types.Certificate{cert},
		Batches:      []*types.Batch{batch},
		FromRound:    10,
		ToRound:      10,
	}

	// Track completion
	var completedFrom, completedTo uint64
	sm.SetSyncCompleteCallback(func(from, to uint64) {
		completedFrom = from
		completedTo = to
	})

	err := sm.HandleSyncResponse(resp, 1)
	if err != nil {
		t.Fatalf("HandleSyncResponse failed: %v", err)
	}

	// Verify certificate was added
	if !d.HasCertificate(cert.Digest()) {
		t.Error("Certificate should be in DAG")
	}

	// Verify batch was stored
	if !batchStore.HasBatch(batch.Digest) {
		t.Error("Batch should be in store")
	}

	// Verify callback was called
	if completedFrom != 10 || completedTo != 10 {
		t.Errorf("Expected callback with (10, 10), got (%d, %d)", completedFrom, completedTo)
	}
}

func TestSyncManagerHandleSyncRequestLatest(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network1 := NewMockNetwork(0, DefaultConfig())
	network2 := NewMockNetwork(1, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	network1.Connect(network2)

	// Add certificates
	for round := uint64(0); round < 10; round++ {
		cert := createTestCertificate(t, 0, round)
		_ = d.AddCertificate(cert)
	}

	sm := NewSyncManager(d, batchStore, network1, vs, DefaultSyncConfig())
	_ = network1.Start()
	_ = network2.Start()
	defer func() { _ = network1.Stop() }()
	defer func() { _ = network2.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// Request with toRound = 0 (latest)
	req := &SyncRequest{
		FromRound: 5,
		ToRound:   0,
		Requester: 1,
	}

	err := sm.HandleSyncRequest(req)
	if err != nil {
		t.Fatalf("HandleSyncRequest failed: %v", err)
	}

	select {
	case msg := <-network2.SyncResponses():
		// Should get rounds 5-9 (5 certs)
		if len(msg.Response.Certificates) != 5 {
			t.Errorf("Expected 5 certificates, got %d", len(msg.Response.Certificates))
		}
		if msg.Response.ToRound != 9 {
			t.Errorf("Expected toRound 9, got %d", msg.Response.ToRound)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync response")
	}
}

func TestSyncManagerCatchUp(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network1 := NewMockNetwork(0, DefaultConfig())
	network2 := NewMockNetwork(1, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	network1.Connect(network2)

	sm := NewSyncManager(d, batchStore, network1, vs, DefaultSyncConfig())
	_ = network1.Start()
	_ = network2.Start()
	defer func() { _ = network1.Stop() }()
	defer func() { _ = network2.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// Call CatchUp
	err := sm.CatchUp(10)
	if err != nil {
		t.Fatalf("CatchUp failed: %v", err)
	}

	// Should have sent sync request
	select {
	case req := <-network2.SyncRequests():
		if req.FromRound != 1 {
			t.Errorf("Expected fromRound 1, got %d", req.FromRound)
		}
		if req.ToRound != 10 {
			t.Errorf("Expected toRound 10, got %d", req.ToRound)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync request")
	}
}

func TestSyncManagerCatchUpAlreadyCaughtUp(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network := NewMockNetwork(0, DefaultConfig())
	vs, _ := createTestValidatorSet(t, 4)

	// Add certificates so we're already at round 10
	for round := uint64(0); round <= 10; round++ {
		cert := createTestCertificate(t, 0, round)
		_ = d.AddCertificate(cert)
	}

	sm := NewSyncManager(d, batchStore, network, vs, DefaultSyncConfig())
	_ = network.Start()
	defer func() { _ = network.Stop() }()

	if err := sm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = sm.Stop() }()

	// CatchUp should return immediately
	err := sm.CatchUp(10)
	if err != nil {
		t.Fatalf("CatchUp should succeed when already caught up: %v", err)
	}
}

func TestSyncManagerUpdateValidatorSet(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	d := dag.New(certStore, dag.DefaultConfig())
	network := NewMockNetwork(0, DefaultConfig())
	vs1, _ := createTestValidatorSet(t, 4)

	sm := NewSyncManager(d, batchStore, network, vs1, DefaultSyncConfig())

	// Create new validator set
	vs2, _ := createTestValidatorSet(t, 5)

	sm.UpdateValidatorSet(vs2)

	// The new validator set should be used
	// (We can't directly check this without exposing internal state,
	// but the method should not panic)
}

func TestDefaultSyncConfig(t *testing.T) {
	cfg := DefaultSyncConfig()

	if cfg.SyncInterval <= 0 {
		t.Error("SyncInterval should be positive")
	}

	if cfg.SyncThreshold <= 0 {
		t.Error("SyncThreshold should be positive")
	}

	if cfg.SyncBatchSize <= 0 {
		t.Error("SyncBatchSize should be positive")
	}

	if cfg.SyncTimeout <= 0 {
		t.Error("SyncTimeout should be positive")
	}
}
