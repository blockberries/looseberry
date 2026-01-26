package looseberry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// TestNode represents a single node in the test network.
type TestNode struct {
	Index       uint16
	Looseberry  *Looseberry
	Network     *network.MockNetwork
	Signer      *types.Ed25519Signer
	BatchStore  store.BatchStore
	CertStore   store.CertificateStore
	TxIndex     store.TxIndex
}

// TestNetwork is a test harness for multi-node integration testing.
type TestNetwork struct {
	nodes        []*TestNode
	validatorSet *types.SimpleValidatorSet
	signers      []*types.Ed25519Signer
	running      bool
	mu           sync.RWMutex
}

// NewTestNetwork creates a new test network with n nodes.
func NewTestNetwork(t *testing.T, n int) *TestNetwork {
	t.Helper()

	if n < 1 {
		t.Fatalf("TestNetwork requires at least 1 node, got %d", n)
	}

	// Create signers and validators
	signers := make([]*types.Ed25519Signer, n)
	validators := make([]*types.Validator, n)

	for i := range n {
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

	tn := &TestNetwork{
		nodes:        make([]*TestNode, n),
		validatorSet: vs,
		signers:      signers,
	}

	// Create nodes
	for i := range n {
		node, err := tn.createNode(t, uint16(i))
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}
		tn.nodes[i] = node
	}

	// Connect all nodes to each other
	for i := range n {
		for j := i + 1; j < n; j++ {
			tn.nodes[i].Network.Connect(tn.nodes[j].Network)
		}
	}

	return tn
}

// createNode creates a single test node.
func (tn *TestNetwork) createNode(t *testing.T, index uint16) (*TestNode, error) {
	t.Helper()

	signer := tn.signers[index]

	cfg := DefaultConfig()
	cfg.Signer = signer
	cfg.ValidatorIndex = index
	cfg.Storage.InMemory = true

	lb, err := New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create Looseberry: %w", err)
	}

	mockNet := network.NewMockNetwork(index, network.DefaultConfig())
	batchStore := store.NewMemoryBatchStore()
	certStore := store.NewMemoryCertificateStore()
	txIndex := store.NewMemoryTxIndex()

	lb.SetValidatorSet(tn.validatorSet)
	lb.SetNetwork(mockNet)
	lb.SetStores(batchStore, certStore, txIndex)

	return &TestNode{
		Index:      index,
		Looseberry: lb,
		Network:    mockNet,
		Signer:     signer,
		BatchStore: batchStore,
		CertStore:  certStore,
		TxIndex:    txIndex,
	}, nil
}

// Start starts all nodes in the network.
func (tn *TestNetwork) Start() error {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	if tn.running {
		return fmt.Errorf("network already running")
	}

	for i, node := range tn.nodes {
		if err := node.Looseberry.Start(); err != nil {
			// Stop already started nodes
			for j := 0; j < i; j++ {
				_ = tn.nodes[j].Looseberry.Stop()
			}
			return fmt.Errorf("start node %d: %w", i, err)
		}
	}

	tn.running = true
	return nil
}

// Stop stops all nodes in the network.
func (tn *TestNetwork) Stop() error {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	if !tn.running {
		return nil
	}

	var errs []error
	for i, node := range tn.nodes {
		if err := node.Looseberry.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop node %d: %w", i, err))
		}
	}

	tn.running = false

	if len(errs) > 0 {
		return fmt.Errorf("errors stopping nodes: %v", errs)
	}
	return nil
}

// Node returns the node at the given index.
func (tn *TestNetwork) Node(index int) *TestNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	if index < 0 || index >= len(tn.nodes) {
		return nil
	}
	return tn.nodes[index]
}

// NodeCount returns the number of nodes in the network.
func (tn *TestNetwork) NodeCount() int {
	tn.mu.RLock()
	defer tn.mu.RUnlock()
	return len(tn.nodes)
}

// ValidatorSet returns the validator set.
func (tn *TestNetwork) ValidatorSet() *types.SimpleValidatorSet {
	return tn.validatorSet
}

// SubmitTx submits a transaction to all nodes.
func (tn *TestNetwork) SubmitTx(tx []byte) error {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	if !tn.running {
		return fmt.Errorf("network not running")
	}

	for i, node := range tn.nodes {
		if err := node.Looseberry.AddTx(tx); err != nil {
			return fmt.Errorf("submit to node %d: %w", i, err)
		}
	}
	return nil
}

// SubmitTxToNode submits a transaction to a specific node.
func (tn *TestNetwork) SubmitTxToNode(nodeIndex int, tx []byte) error {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	if !tn.running {
		return fmt.Errorf("network not running")
	}

	if nodeIndex < 0 || nodeIndex >= len(tn.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	return tn.nodes[nodeIndex].Looseberry.AddTx(tx)
}

// WaitForRound waits until all nodes reach at least the specified round.
func (tn *TestNetwork) WaitForRound(round uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allReached := true
		for _, node := range tn.nodes {
			if node.Looseberry.CurrentRound() < round {
				allReached = false
				break
			}
		}
		if allReached {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for round %d", round)
}

// WaitForBatches waits until the specified number of batches are created.
func (tn *TestNetwork) WaitForBatches(count int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		total := 0
		for _, node := range tn.nodes {
			m := node.Looseberry.Metrics()
			total += int(m.TotalBatches)
		}
		if total >= count {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for %d batches", count)
}

// GetMetrics returns metrics from all nodes.
func (tn *TestNetwork) GetMetrics() []*Metrics {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	metrics := make([]*Metrics, len(tn.nodes))
	for i, node := range tn.nodes {
		metrics[i] = node.Looseberry.Metrics()
	}
	return metrics
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestIntegrationNetworkCreation(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	defer func() { _ = tn.Stop() }()

	if tn.NodeCount() != 4 {
		t.Errorf("Expected 4 nodes, got %d", tn.NodeCount())
	}

	// Verify all nodes are connected
	for i := 0; i < 4; i++ {
		node := tn.Node(i)
		if node == nil {
			t.Fatalf("Node %d is nil", i)
		}
		// Each node should be connected to 3 other nodes
		if node.Network.PeerCount() != 3 {
			t.Errorf("Node %d has %d peers, expected 3", i, node.Network.PeerCount())
		}
	}
}

func TestIntegrationNetworkStartStop(t *testing.T) {
	tn := NewTestNetwork(t, 4)

	// Start
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify all nodes are running
	for i := 0; i < 4; i++ {
		if !tn.Node(i).Looseberry.IsRunning() {
			t.Errorf("Node %d is not running", i)
		}
	}

	// Double start should fail
	if err := tn.Start(); err == nil {
		t.Error("Double start should fail")
	}

	// Stop
	if err := tn.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify all nodes are stopped
	for i := 0; i < 4; i++ {
		if tn.Node(i).Looseberry.IsRunning() {
			t.Errorf("Node %d is still running", i)
		}
	}
}

func TestIntegrationSubmitTransactions(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = tn.Stop() }()

	// Submit transactions to each node
	for i := 0; i < 10; i++ {
		tx := []byte(fmt.Sprintf("tx-%d", i))
		if err := tn.SubmitTxToNode(i%4, tx); err != nil {
			t.Fatalf("SubmitTxToNode failed: %v", err)
		}
	}

	// Verify transactions are in the mempools
	time.Sleep(50 * time.Millisecond)

	totalPending := 0
	for i := 0; i < 4; i++ {
		totalPending += tn.Node(i).Looseberry.Size()
	}

	if totalPending < 10 {
		t.Errorf("Expected at least 10 pending transactions, got %d", totalPending)
	}
}

func TestIntegrationGetMetrics(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = tn.Stop() }()

	// Submit some transactions
	for i := 0; i < 5; i++ {
		tx := []byte(fmt.Sprintf("tx-%d", i))
		_ = tn.SubmitTxToNode(0, tx)
	}

	time.Sleep(50 * time.Millisecond)

	// Get metrics
	metrics := tn.GetMetrics()
	if len(metrics) != 4 {
		t.Fatalf("Expected 4 metrics, got %d", len(metrics))
	}

	// Node 0 should have transactions added
	if metrics[0].TotalTxAdded < 5 {
		t.Errorf("Expected TotalTxAdded >= 5 for node 0, got %d", metrics[0].TotalTxAdded)
	}
}

func TestIntegrationNodeInvalidIndex(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	defer func() { _ = tn.Stop() }()

	if tn.Node(-1) != nil {
		t.Error("Node(-1) should return nil")
	}
	if tn.Node(4) != nil {
		t.Error("Node(4) should return nil")
	}
	if tn.Node(100) != nil {
		t.Error("Node(100) should return nil")
	}
}

func TestIntegrationSubmitBeforeStart(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	defer func() { _ = tn.Stop() }()

	// Try to submit before start
	err := tn.SubmitTx([]byte("test"))
	if err == nil {
		t.Error("SubmitTx before start should fail")
	}
}

func TestIntegrationSubmitToInvalidNode(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	if err := tn.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = tn.Stop() }()

	err := tn.SubmitTxToNode(10, []byte("test"))
	if err == nil {
		t.Error("SubmitTxToNode with invalid index should fail")
	}
}

func TestIntegrationValidatorSet(t *testing.T) {
	tn := NewTestNetwork(t, 4)
	defer func() { _ = tn.Stop() }()

	vs := tn.ValidatorSet()
	if vs == nil {
		t.Fatal("ValidatorSet should not be nil")
	}
	if vs.Count() != 4 {
		t.Errorf("Expected 4 validators, got %d", vs.Count())
	}
	if vs.Quorum() != 3 {
		t.Errorf("Expected quorum of 3, got %d", vs.Quorum())
	}
}
