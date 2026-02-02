# Multi-Node Setup Tutorial

Learn how to set up and run a multi-node Looseberry test network.

## What You'll Build

A 4-node Looseberry network that:
- Runs multiple validators
- Forms certificates through voting
- Synchronizes certificates across nodes
- Demonstrates BFT properties (tolerates f=1 Byzantine validators)

## Prerequisites

- Completed the [Quickstart Tutorial](QUICKSTART.md)
- Understanding of BFT consensus basics
- 15 minutes of your time

## Understanding Multi-Node Setup

### BFT Requirements

For Byzantine fault tolerance:
- **n = 3f + 1** validators (n=total, f=Byzantine)
- **Quorum = 2f + 1** votes needed for certificates

Examples:
- 4 validators → tolerates 1 Byzantine (f=1, quorum=3)
- 7 validators → tolerates 2 Byzantine (f=2, quorum=5)
- 10 validators → tolerates 3 Byzantine (f=3, quorum=7)

### Network Architecture

```
┌──────────────┐     ┌──────────────┐
│ Validator 0  │◄───►│ Validator 1  │
└──────┬───────┘     └───────┬──────┘
       │                     │
       └─────────┬───────────┘
                 │
       ┌─────────┴───────────┐
       │                     │
┌──────▼───────┐     ┌───────▼──────┐
│ Validator 2  │◄───►│ Validator 3  │
└──────────────┘     └──────────────┘
```

## Step 1: Create Project

```bash
mkdir looseberry-multinode
cd looseberry-multinode
go mod init multinode
go get github.com/blockberries/looseberry
```

## Step 2: Implement Multi-Node Setup

Create `main.go`:

```go
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/blockberries/looseberry"
	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Node represents a single validator node
type Node struct {
	Index      uint16
	Signer     *types.Ed25519Signer
	Looseberry *looseberry.Looseberry
	Network    *network.MockNetwork
	BatchStore store.BatchStore
	CertStore  store.CertificateStore
	TxIndex    store.TxIndex
}

// TestNetwork manages multiple nodes
type TestNetwork struct {
	nodes        []*Node
	validatorSet *types.SimpleValidatorSet
}

func main() {
	fmt.Println("=== Multi-Node Looseberry Tutorial ===")

	// Create 4-node network
	tn := createNetwork(4)
	defer tn.shutdown()

	// Start all nodes
	fmt.Println("\n1. Starting 4-node network...")
	if err := tn.start(); err != nil {
		log.Fatalf("Failed to start network: %v", err)
	}
	fmt.Println("   ✓ All nodes started")

	// Display node info
	fmt.Println("\n2. Node information:")
	for i, node := range tn.nodes {
		fmt.Printf("   Node %d: Validator %d\n", i, node.Index)
	}

	// Submit transactions to node 0
	fmt.Println("\n3. Submitting transactions to node 0...")
	node0 := tn.nodes[0]
	for i := 0; i < 50; i++ {
		tx := []byte(fmt.Sprintf("Transaction #%d", i+1))
		if err := node0.Looseberry.AddTx(tx); err != nil {
			log.Printf("   ⚠ Failed to add tx %d: %v", i+1, err)
		}
	}
	fmt.Println("   ✓ Submitted 50 transactions")

	// Wait for certificates to form
	fmt.Println("\n4. Waiting for certificate formation (voting)...")
	time.Sleep(3 * time.Second)

	// Check all nodes
	fmt.Println("\n5. Checking node status:")
	for i, node := range tn.nodes {
		metrics := node.Looseberry.Metrics()
		fmt.Printf("   Node %d:\n", i)
		fmt.Printf("     Current round: %d\n", metrics.CurrentRound)
		fmt.Printf("     Certificates: %d\n", metrics.TotalCertificates)
		fmt.Printf("     Batches: %d\n", metrics.TotalBatches)
		fmt.Printf("     Pending: %d\n", metrics.PendingTxCount)
	}

	// Reap from all nodes
	fmt.Println("\n6. Reaping certified batches from all nodes:")
	for i, node := range tn.nodes {
		batches := node.Looseberry.ReapCertifiedBatches(1024 * 1024)
		fmt.Printf("   Node %d: Reaped %d batches\n", i, len(batches))
	}

	// Test consensus commit
	fmt.Println("\n7. Simulating consensus commit...")
	node0Metrics := node0.Looseberry.Metrics()
	commitRound := node0Metrics.CurrentRound
	for i, node := range tn.nodes {
		node.Looseberry.NotifyCommitted(commitRound)
		fmt.Printf("   ✓ Node %d committed round %d\n", i, commitRound)
	}

	// Verify all nodes are in sync
	fmt.Println("\n8. Verifying synchronization:")
	time.Sleep(500 * time.Millisecond)
	allInSync := true
	baseRound := tn.nodes[0].Looseberry.CurrentRound()
	for i, node := range tn.nodes {
		round := node.Looseberry.CurrentRound()
		if round < baseRound-1 || round > baseRound+1 {
			allInSync = false
			fmt.Printf("   ⚠ Node %d out of sync: round=%d\n", i, round)
		}
	}
	if allInSync {
		fmt.Println("   ✓ All nodes synchronized")
	}

	// Test node failure and recovery
	fmt.Println("\n9. Testing node failure and recovery...")
	fmt.Println("   Stopping node 3...")
	tn.nodes[3].Looseberry.Stop()

	// Submit more transactions
	fmt.Println("   Submitting more transactions...")
	for i := 0; i < 30; i++ {
		tx := []byte(fmt.Sprintf("Recovery-tx-%d", i))
		node0.Looseberry.AddTx(tx)
	}

	time.Sleep(2 * time.Second)

	// Restart node 3
	fmt.Println("   Restarting node 3...")
	tn.nodes[3].Looseberry.Start()

	// Wait for sync
	fmt.Println("   Waiting for sync...")
	time.Sleep(3 * time.Second)

	// Verify node 3 caught up
	round3 := tn.nodes[3].Looseberry.CurrentRound()
	round0 := tn.nodes[0].Looseberry.CurrentRound()
	if round3 >= round0-2 {
		fmt.Printf("   ✓ Node 3 caught up (round %d, expected ~%d)\n", round3, round0)
	} else {
		fmt.Printf("   ⚠ Node 3 still behind (round %d, expected ~%d)\n", round3, round0)
	}

	fmt.Println("\n=== Tutorial Complete! ===")
	fmt.Println("\nYou've successfully:")
	fmt.Println("  ✓ Created a 4-node network")
	fmt.Println("  ✓ Formed certificates through voting")
	fmt.Println("  ✓ Verified synchronization across nodes")
	fmt.Println("  ✓ Tested node failure and recovery")
	fmt.Println("\nNext steps:")
	fmt.Println("  - Explore Configuration guide")
	fmt.Println("  - Try Performance Tuning tutorial")
	fmt.Println("  - Read the Integration guide")
}

func createNetwork(n int) *TestNetwork {
	// Generate signers and validators
	signers := make([]*types.Ed25519Signer, n)
	validators := make([]*types.Validator, n)

	for i := 0; i < n; i++ {
		signer, err := types.GenerateEd25519Signer(uint16(i))
		if err != nil {
			log.Fatalf("Failed to generate signer %d: %v", i, err)
		}
		signers[i] = signer
		validators[i] = &types.Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
		}
	}

	validatorSet := types.NewSimpleValidatorSet(validators, 0)

	// Create nodes
	nodes := make([]*Node, n)
	mockNets := make([]*network.MockNetwork, n)

	for i := 0; i < n; i++ {
		cfg := looseberry.DefaultConfig()
		cfg.ValidatorIndex = uint16(i)
		cfg.Signer = signers[i]
		cfg.Storage.InMemory = true

		// Fast configuration for demo
		cfg.Worker.BatchSize = 10
		cfg.Worker.BatchTimeout = 500 * time.Millisecond
		cfg.Primary.HeaderTimeout = 1 * time.Second

		lb, err := looseberry.New(cfg)
		if err != nil {
			log.Fatalf("Failed to create Looseberry %d: %v", i, err)
		}

		lb.SetValidatorSet(validatorSet)

		mockNet := network.NewMockNetwork(uint16(i), network.DefaultConfig())
		lb.SetNetwork(mockNet)

		batchStore := store.NewMemoryBatchStore()
		certStore := store.NewMemoryCertificateStore()
		txIndex := store.NewMemoryTxIndex()
		lb.SetStores(batchStore, certStore, txIndex)

		nodes[i] = &Node{
			Index:      uint16(i),
			Signer:     signers[i],
			Looseberry: lb,
			Network:    mockNet,
			BatchStore: batchStore,
			CertStore:  certStore,
			TxIndex:    txIndex,
		}
		mockNets[i] = mockNet
	}

	// Connect all networks (full mesh)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			mockNets[i].Connect(mockNets[j])
		}
	}

	return &TestNetwork{
		nodes:        nodes,
		validatorSet: validatorSet,
	}
}

func (tn *TestNetwork) start() error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(tn.nodes))

	for _, node := range tn.nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			if err := n.Looseberry.Start(); err != nil {
				errCh <- fmt.Errorf("node %d: %w", n.Index, err)
			}
		}(node)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}

	return nil
}

func (tn *TestNetwork) shutdown() {
	for _, node := range tn.nodes {
		node.Looseberry.Stop()
	}
}
```

## Step 3: Run It!

```bash
go mod tidy
go run main.go
```

## Understanding the Output

### Certificate Formation

With 4 validators (n=4, f=1, quorum=3):
1. Node 0 creates header
2. Nodes 1, 2, 3 vote on header
3. Node 0 collects 3 votes (quorum)
4. Certificate formed and broadcast

### Synchronization

All nodes should converge to the same round:
```
Node 0: round=5, certificates=20
Node 1: round=5, certificates=20
Node 2: round=5, certificates=20
Node 3: round=5, certificates=20
```

### Node Recovery

When node 3 restarts:
1. Detects it's behind (sync threshold)
2. Requests missing certificates
3. Receives certificates + batches
4. Catches up to network

## Experiments

### Test with More Nodes

```go
tn := createNetwork(7)  // 7 validators, f=2
```

### Simulate Byzantine Behavior

```go
// Stop 1 node (network should continue)
tn.nodes[3].Looseberry.Stop()
// Submit more transactions - should still work

// Stop 2 nodes (network should halt with n=4, f=1)
tn.nodes[2].Looseberry.Stop()
// No new certificates should form
```

### Monitor Metrics

```go
ticker := time.NewTicker(2 * time.Second)
for i := 0; i < 10; i++ {
    <-ticker.C
    for j, node := range tn.nodes {
        m := node.Looseberry.Metrics()
        fmt.Printf("Node %d: round=%d, certs=%d\n",
            j, m.CurrentRound, m.TotalCertificates)
    }
    fmt.Println()
}
```

### Test Network Partition

```go
// Disconnect node 3 from others
tn.nodes[0].Network.Disconnect(tn.nodes[3].Network)
tn.nodes[1].Network.Disconnect(tn.nodes[3].Network)
tn.nodes[2].Network.Disconnect(tn.nodes[3].Network)

// Submit transactions
// Node 3 should fall behind

// Reconnect
tn.nodes[0].Network.Connect(tn.nodes[3].Network)
tn.nodes[1].Network.Connect(tn.nodes[3].Network)
tn.nodes[2].Network.Connect(tn.nodes[3].Network)

// Node 3 should sync
```

## Troubleshooting

**Issue**: Certificates not forming

**Check**:
- All nodes started successfully
- Networks are connected (full mesh)
- Validator set is identical on all nodes

**Solution**:
```go
// Verify network connectivity
for i, node := range tn.nodes {
    fmt.Printf("Node %d network ID: %d\n", i, node.Network.ValidatorID())
}

// Check validator set
for i, node := range tn.nodes {
    vs := node.Looseberry.ValidatorSet()
    fmt.Printf("Node %d has %d validators\n", i, vs.Count())
}
```

**Issue**: Nodes out of sync

**Solution**: Increase sync time or reduce thresholds:
```go
cfg.Sync.SyncThreshold = 2  // Sync more aggressively
time.Sleep(5 * time.Second)  // Wait longer
```

## Key Concepts Demonstrated

### 1. Certificate Formation
```
Header (Node 0) → Votes (Nodes 1,2,3) → Certificate (2f+1 votes)
```

### 2. DAG Construction
Each certificate references 2f+1 parent certificates from previous round.

### 3. BFT Safety
With n=4, f=1:
- Needs 3 votes for certificate
- Can tolerate 1 Byzantine validator
- Can tolerate 1 failed validator

### 4. Synchronization
Nodes automatically detect they're behind and sync.

### 5. Recovery
Nodes can restart and catch up from other validators.

## Production Considerations

In production deployments:

1. **Real Network**: Replace MockNetwork with real P2P (glueberry)
2. **Persistent Storage**: Use LevelDB instead of in-memory
3. **Monitoring**: Add metrics collection and alerting
4. **Security**: Secure key storage, TLS connections
5. **Configuration**: Tune for your workload

Example production configuration:
```go
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"

cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
cfg.Worker.BatchSize = 500

cfg.GC.GCDepth = 100
cfg.GC.RecoverTxs = true
```

## Next Steps

Now that you understand multi-node setup:

1. **[Integration Guide](../guides/INTEGRATION.md)**: Integrate with consensus
2. **[Configuration Guide](../guides/CONFIGURATION.md)**: Tune for production
3. **[Performance Tuning](PERFORMANCE_TUNING.md)**: Optimize performance
4. **[Deployment Guide](../guides/DEPLOYMENT.md)**: Deploy to production

## Complete Code

The complete code is available at:
`docs/tutorials/multinode/main.go`

Congratulations! You've successfully set up a multi-node Looseberry network.
