package looseberry

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
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

func createTestConfig(signer types.Signer, validatorIndex uint16) *Config {
	cfg := DefaultConfig()
	cfg.Signer = signer
	cfg.ValidatorIndex = validatorIndex
	cfg.Storage.InMemory = true
	return cfg
}

func TestNewLooseberry(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if lb == nil {
		t.Fatal("Looseberry should not be nil")
	}

	if lb.cfg != cfg {
		t.Error("Config should be stored")
	}

	// Should not be running
	if lb.IsRunning() {
		t.Error("Should not be running initially")
	}

	_ = vs // Used for validator set
}

func TestNewLooseberryInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ValidatorIndex = 0
	cfg.Signer = nil // Missing signer
	cfg.Worker.MinWorkers = 0

	_, err := New(cfg)
	if err == nil {
		t.Error("Expected error for invalid config")
	}
}

func TestLooseberrySetters(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Set validator set
	lb.SetValidatorSet(vs)

	// Set network
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Set stores
	batchStore := store.NewMemoryBatchStore()
	certStore := store.NewMemoryCertificateStore()
	txIndex := store.NewMemoryTxIndex()
	lb.SetStores(batchStore, certStore, txIndex)
}

func TestLooseberryStartStop(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Start
	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !lb.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := lb.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	// Stop
	if err := lb.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if lb.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := lb.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestLooseberryStartMissingValidatorSet(t *testing.T) {
	_, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Don't set validator set
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Start should fail
	if err := lb.Start(); err == nil {
		t.Error("Expected error for missing validator set")
	}
}

func TestLooseberryStartMissingNetwork(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	// Don't set network

	// Start should fail
	if err := lb.Start(); err == nil {
		t.Error("Expected error for missing network")
	}
}

func TestLooseberryAddTx(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// AddTx before start should fail
	err = lb.AddTx([]byte("test tx"))
	if err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}

	// Start
	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// AddTx should succeed
	err = lb.AddTx([]byte("test tx 1"))
	if err != nil {
		t.Errorf("AddTx failed: %v", err)
	}

	// Check metrics
	m := lb.Metrics()
	if m.TotalTxAdded != 1 {
		t.Errorf("Expected TotalTxAdded=1, got %d", m.TotalTxAdded)
	}
}

func TestLooseberryAddTxWithValidator(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	// Set a tx validator that rejects certain transactions
	cfg.TxValidator = func(tx []byte) error {
		if string(tx) == "invalid" {
			return types.ErrTxValidationFailed
		}
		return nil
	}

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

	// Valid tx should succeed
	if err := lb.AddTx([]byte("valid tx")); err != nil {
		t.Errorf("AddTx with valid tx failed: %v", err)
	}

	// Invalid tx should fail
	if err := lb.AddTx([]byte("invalid")); err == nil {
		t.Error("Expected error for invalid tx")
	}
}

func TestLooseberryHasTx(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// HasTx before start should return false
	if lb.HasTx([]byte{1, 2, 3}) {
		t.Error("HasTx should return false when not running")
	}

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// HasTx for non-existent tx
	if lb.HasTx([]byte{1, 2, 3}) {
		t.Error("HasTx should return false for non-existent tx")
	}
}

func TestLooseberrySize(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Size before start should be 0
	if lb.Size() != 0 {
		t.Error("Size should be 0 when not running")
	}

	if lb.SizeBytes() != 0 {
		t.Error("SizeBytes should be 0 when not running")
	}

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Add some transactions
	for i := 0; i < 5; i++ {
		_ = lb.AddTx([]byte{byte(i)})
	}

	// Check size
	if lb.Size() < 5 {
		t.Errorf("Expected Size >= 5, got %d", lb.Size())
	}

	if lb.SizeBytes() < 5 {
		t.Errorf("Expected SizeBytes >= 5, got %d", lb.SizeBytes())
	}
}

func TestLooseberryCurrentRound(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// CurrentRound before start should be 0
	if lb.CurrentRound() != 0 {
		t.Error("CurrentRound should be 0 when not running")
	}

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// CurrentRound after start should be a valid value
	round := lb.CurrentRound()
	// Just check it doesn't panic - uint64 is always >= 0
	_ = round
}

func TestLooseberryMetrics(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Metrics before start
	m := lb.Metrics()
	if m == nil {
		t.Fatal("Metrics should not be nil")
	}
	if m.TotalTxAdded != 0 {
		t.Errorf("Expected TotalTxAdded=0, got %d", m.TotalTxAdded)
	}

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// Add some transactions
	for i := 0; i < 3; i++ {
		_ = lb.AddTx([]byte{byte(i)})
	}

	m = lb.Metrics()
	if m.TotalTxAdded != 3 {
		t.Errorf("Expected TotalTxAdded=3, got %d", m.TotalTxAdded)
	}
	if m.WorkerCount < 1 {
		t.Errorf("Expected WorkerCount >= 1, got %d", m.WorkerCount)
	}
}

func TestLooseberryFlush(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

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

	// Add some transactions
	for i := 0; i < 5; i++ {
		_ = lb.AddTx([]byte{byte(i)})
	}

	// Flush should not panic
	lb.Flush()
}

func TestLooseberryNotifyCommitted(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

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

	// NotifyCommitted should not panic
	lb.NotifyCommitted(10)

	m := lb.Metrics()
	if m.CommittedRound != 10 {
		t.Errorf("Expected CommittedRound=10, got %d", m.CommittedRound)
	}
}

func TestLooseberryUpdateValidatorSet(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

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

	// Create new validator set
	newVs, _ := createTestValidatorSet(t, 5)

	// UpdateValidatorSet should not panic
	lb.UpdateValidatorSet(newVs)
}

func TestLooseberryReapCertifiedBatches(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// ReapCertifiedBatches before start
	batches := lb.ReapCertifiedBatches(1024 * 1024)
	if len(batches) != 0 {
		t.Error("Expected empty batches when not running")
	}

	if err := lb.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lb.Stop() }()

	// ReapCertifiedBatches with no committed batches
	batches = lb.ReapCertifiedBatches(1024 * 1024)
	if len(batches) != 0 {
		t.Errorf("Expected 0 batches, got %d", len(batches))
	}
}

func TestLooseberryRestartable(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	lb.SetValidatorSet(vs)
	net := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	// Start, stop, start again
	if err := lb.Start(); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}

	if err := lb.Stop(); err != nil {
		t.Fatalf("First Stop failed: %v", err)
	}

	// Need to reset network
	net = network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(net)

	if err := lb.Start(); err != nil {
		t.Fatalf("Second Start failed: %v", err)
	}

	if err := lb.Stop(); err != nil {
		t.Fatalf("Second Stop failed: %v", err)
	}
}

func TestLooseberryMessageHandlers(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

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

	// Inject a batch message - should not panic
	batch := &types.Batch{
		WorkerID:     0,
		ValidatorID:  1,
		Round:        1,
		Transactions: []types.Transaction{[]byte("tx1")},
		Timestamp:    time.Now().UnixNano(),
	}
	batch.Digest = batch.ComputeDigest()

	net.InjectBatchMessage(&network.BatchMessage{
		Batch: batch,
		From:  1,
	})

	// Give time for message to be processed
	time.Sleep(50 * time.Millisecond)

	// Inject a header message
	header := types.NewHeader(1, 1, 0, nil, nil)
	_ = header.Sign(signers[1])

	net.InjectHeaderMessage(&network.HeaderMessage{
		Header: header,
		From:   1,
	})

	time.Sleep(50 * time.Millisecond)

	// Inject a vote message
	vote := types.NewVote(header.Digest, 1)
	_ = vote.Sign(signers[1])

	net.InjectVoteMessage(&network.VoteMessage{
		Vote: vote,
		From: 1,
	})

	time.Sleep(50 * time.Millisecond)
}

func TestCertifiedBatch(t *testing.T) {
	batch := &types.Batch{
		WorkerID:     0,
		ValidatorID:  0,
		Round:        1,
		Transactions: []types.Transaction{[]byte("tx1")},
	}
	batch.Digest = batch.ComputeDigest()

	cb := CertifiedBatch{
		Batch:       batch,
		Certificate: nil,
	}

	if cb.Batch != batch {
		t.Error("Batch should be stored")
	}
}

func TestLooseberryInterfaceCompliance(t *testing.T) {
	_, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)

	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Verify interface compliance
	var _ DAGMempool = lb
}
