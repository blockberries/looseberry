package network

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/types"
)

func createTestBatch(t *testing.T, workerID, validatorID uint16, round uint64) *types.Batch {
	t.Helper()
	txs := []types.Transaction{
		types.Transaction([]byte("tx1")),
		types.Transaction([]byte("tx2")),
	}
	return types.NewBatch(workerID, validatorID, round, txs)
}

func createTestHeader(t *testing.T, author uint16, round uint64) *types.Header {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	header := types.NewHeader(author, round, 0, nil, nil)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	return header
}

func createTestCertificate(t *testing.T, author uint16, round uint64) *types.Certificate {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	header := types.NewHeader(author, round, 0, nil, nil)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	vote := types.NewVote(header.Digest, author)
	if err := vote.Sign(signer); err != nil {
		t.Fatalf("Failed to sign vote: %v", err)
	}

	return types.NewCertificate(header, []types.Vote{*vote})
}

func createTestVote(t *testing.T, validator uint16, headerDigest types.Hash) *types.Vote {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(validator)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	vote := types.NewVote(headerDigest, validator)
	if err := vote.Sign(signer); err != nil {
		t.Fatalf("Failed to sign vote: %v", err)
	}

	return vote
}

func TestMockNetworkStartStop(t *testing.T) {
	m := NewMockNetwork(0, DefaultConfig())

	if err := m.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Double start should fail
	if err := m.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := m.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Double stop should fail
	if err := m.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestMockNetworkConnect(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)

	if m1.PeerCount() != 1 {
		t.Errorf("Expected 1 peer, got %d", m1.PeerCount())
	}

	if m2.PeerCount() != 1 {
		t.Errorf("Expected 1 peer, got %d", m2.PeerCount())
	}

	m1.Disconnect(1)

	if m1.PeerCount() != 0 {
		t.Errorf("Expected 0 peers, got %d", m1.PeerCount())
	}

	if m2.PeerCount() != 0 {
		t.Errorf("Expected 0 peers, got %d", m2.PeerCount())
	}
}

func TestMockNetworkBroadcastBatch(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())
	m3 := NewMockNetwork(2, DefaultConfig())

	m1.Connect(m2)
	m1.Connect(m3)

	_ = m1.Start()
	_ = m2.Start()
	_ = m3.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()
	defer func() { _ = m3.Stop() }()

	batch := createTestBatch(t, 0, 0, 10)
	err := m1.BroadcastBatch(batch)
	if err != nil {
		t.Fatalf("BroadcastBatch failed: %v", err)
	}

	// Check both peers received
	select {
	case msg := <-m2.BatchMessages():
		if !msg.Batch.Digest.Equal(batch.Digest) {
			t.Error("Batch digest mismatch")
		}
		if msg.From != 0 {
			t.Errorf("Expected from 0, got %d", msg.From)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("m2 did not receive batch")
	}

	select {
	case msg := <-m3.BatchMessages():
		if !msg.Batch.Digest.Equal(batch.Digest) {
			t.Error("Batch digest mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("m3 did not receive batch")
	}

	// Check stats
	stats := m1.Stats()
	if stats.BatchesBroadcast != 1 {
		t.Errorf("Expected 1 batch broadcast, got %d", stats.BatchesBroadcast)
	}
}

func TestMockNetworkBroadcastHeader(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	header := createTestHeader(t, 0, 10)
	err := m1.BroadcastHeader(header)
	if err != nil {
		t.Fatalf("BroadcastHeader failed: %v", err)
	}

	select {
	case msg := <-m2.HeaderMessages():
		if !msg.Header.Digest.Equal(header.Digest) {
			t.Error("Header digest mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive header")
	}

	stats := m1.Stats()
	if stats.HeadersBroadcast != 1 {
		t.Errorf("Expected 1 header broadcast, got %d", stats.HeadersBroadcast)
	}
}

func TestMockNetworkBroadcastCertificate(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	cert := createTestCertificate(t, 0, 10)
	err := m1.BroadcastCertificate(cert)
	if err != nil {
		t.Fatalf("BroadcastCertificate failed: %v", err)
	}

	select {
	case msg := <-m2.CertificateMessages():
		if !msg.Certificate.Digest().Equal(cert.Digest()) {
			t.Error("Certificate digest mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive certificate")
	}

	stats := m1.Stats()
	if stats.CertificatesBroadcast != 1 {
		t.Errorf("Expected 1 certificate broadcast, got %d", stats.CertificatesBroadcast)
	}
}

func TestMockNetworkSendVote(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	headerDigest := types.HashBytes([]byte("test_header"))
	vote := createTestVote(t, 0, headerDigest)

	err := m1.SendVote(1, vote)
	if err != nil {
		t.Fatalf("SendVote failed: %v", err)
	}

	select {
	case msg := <-m2.VoteMessages():
		if !msg.Vote.HeaderDigest.Equal(headerDigest) {
			t.Error("Vote header digest mismatch")
		}
		if msg.From != 0 {
			t.Errorf("Expected from 0, got %d", msg.From)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive vote")
	}

	stats := m1.Stats()
	if stats.VotesSent != 1 {
		t.Errorf("Expected 1 vote sent, got %d", stats.VotesSent)
	}
}

func TestMockNetworkSendVoteUnknownPeer(t *testing.T) {
	m := NewMockNetwork(0, DefaultConfig())
	_ = m.Start()
	defer func() { _ = m.Stop() }()

	headerDigest := types.HashBytes([]byte("test_header"))
	vote := createTestVote(t, 0, headerDigest)

	err := m.SendVote(99, vote)
	if err != types.ErrValidatorNotFound {
		t.Errorf("Expected ErrValidatorNotFound, got: %v", err)
	}
}

func TestMockNetworkSendBatchAck(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	batchDigest := types.HashBytes([]byte("test_batch"))
	ack := &BatchAckMessage{
		BatchDigest: batchDigest,
		Validator:   0,
	}

	err := m1.SendBatchAck(1, ack)
	if err != nil {
		t.Fatalf("SendBatchAck failed: %v", err)
	}

	select {
	case msg := <-m2.BatchAckMessages():
		if !msg.BatchDigest.Equal(batchDigest) {
			t.Error("Batch digest mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive batch ack")
	}

	stats := m1.Stats()
	if stats.BatchAcksSent != 1 {
		t.Errorf("Expected 1 batch ack sent, got %d", stats.BatchAcksSent)
	}
}

func TestMockNetworkSendSyncRequest(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	req := &SyncRequest{
		FromRound: 10,
		ToRound:   20,
		Requester: 0,
	}

	err := m1.SendSyncRequest(1, req)
	if err != nil {
		t.Fatalf("SendSyncRequest failed: %v", err)
	}

	select {
	case msg := <-m2.SyncRequests():
		if msg.FromRound != 10 || msg.ToRound != 20 {
			t.Error("Sync request mismatch")
		}
		if msg.Requester != 0 {
			t.Errorf("Expected requester 0, got %d", msg.Requester)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync request")
	}

	stats := m1.Stats()
	if stats.SyncRequestsSent != 1 {
		t.Errorf("Expected 1 sync request sent, got %d", stats.SyncRequestsSent)
	}
}

func TestMockNetworkSendSyncResponse(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	cert := createTestCertificate(t, 0, 10)
	resp := &SyncResponse{
		Certificates: []*types.Certificate{cert},
		FromRound:    10,
		ToRound:      10,
	}

	err := m1.SendSyncResponse(1, resp)
	if err != nil {
		t.Fatalf("SendSyncResponse failed: %v", err)
	}

	select {
	case msg := <-m2.SyncResponses():
		if len(msg.Response.Certificates) != 1 {
			t.Error("Expected 1 certificate")
		}
		if msg.Response.FromRound != 10 || msg.Response.ToRound != 10 {
			t.Error("Sync response round mismatch")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive sync response")
	}

	stats := m1.Stats()
	if stats.SyncResponsesSent != 1 {
		t.Errorf("Expected 1 sync response sent, got %d", stats.SyncResponsesSent)
	}
}

func TestMockNetworkNotRunning(t *testing.T) {
	m := NewMockNetwork(0, DefaultConfig())
	// Don't start

	batch := createTestBatch(t, 0, 0, 10)
	err := m.BroadcastBatch(batch)
	if err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}

	header := createTestHeader(t, 0, 10)
	err = m.BroadcastHeader(header)
	if err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestMockNetworkInjectMessages(t *testing.T) {
	m := NewMockNetwork(0, DefaultConfig())

	// Inject batch message
	batch := createTestBatch(t, 0, 0, 10)
	m.InjectBatchMessage(&BatchMessage{Batch: batch, From: 1})

	select {
	case msg := <-m.BatchMessages():
		if msg.From != 1 {
			t.Errorf("Expected from 1, got %d", msg.From)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive injected batch")
	}

	// Inject header message
	header := createTestHeader(t, 1, 10)
	m.InjectHeaderMessage(&HeaderMessage{Header: header, From: 1})

	select {
	case msg := <-m.HeaderMessages():
		if msg.From != 1 {
			t.Errorf("Expected from 1, got %d", msg.From)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive injected header")
	}

	// Inject vote message
	vote := createTestVote(t, 1, header.Digest)
	m.InjectVoteMessage(&VoteMessage{Vote: vote, From: 1})

	select {
	case msg := <-m.VoteMessages():
		if msg.From != 1 {
			t.Errorf("Expected from 1, got %d", msg.From)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive injected vote")
	}
}

func TestMockNetworkResetStats(t *testing.T) {
	m1 := NewMockNetwork(0, DefaultConfig())
	m2 := NewMockNetwork(1, DefaultConfig())

	m1.Connect(m2)
	_ = m1.Start()
	_ = m2.Start()
	defer func() { _ = m1.Stop() }()
	defer func() { _ = m2.Stop() }()

	batch := createTestBatch(t, 0, 0, 10)
	_ = m1.BroadcastBatch(batch)

	stats := m1.Stats()
	if stats.BatchesBroadcast != 1 {
		t.Error("Expected 1 batch broadcast")
	}

	m1.ResetStats()

	stats = m1.Stats()
	if stats.BatchesBroadcast != 0 {
		t.Error("Stats should be reset")
	}
}

func TestMockNetworkValidatorID(t *testing.T) {
	m := NewMockNetwork(5, DefaultConfig())

	if m.ValidatorID() != 5 {
		t.Errorf("Expected validator ID 5, got %d", m.ValidatorID())
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BufferSize <= 0 {
		t.Error("BufferSize should be positive")
	}

	if cfg.SyncBatchSize <= 0 {
		t.Error("SyncBatchSize should be positive")
	}

	if cfg.SyncTimeout <= 0 {
		t.Error("SyncTimeout should be positive")
	}
}
