# Testing Guide

Comprehensive guide to testing Looseberry integrations and deployments.

## Table of Contents

- [Testing Strategy](#testing-strategy)
- [Unit Testing](#unit-testing)
- [Integration Testing](#integration-testing)
- [Multi-Node Testing](#multi-node-testing)
- [Performance Testing](#performance-testing)
- [Stress Testing](#stress-testing)
- [Byzantine Fault Testing](#byzantine-fault-testing)
- [Test Utilities](#test-utilities)
- [Continuous Integration](#continuous-integration)

## Testing Strategy

Looseberry testing follows a pyramid approach:

```
          /\
         /BF\     Byzantine Fault Tests (rare)
        /____\
       /Stress\   Stress & Performance Tests (regular)
      /________\
     /Integration\ Integration Tests (frequent)
    /____________\
   /  Unit Tests  \ Unit Tests (continuous)
  /________________\
```

### Test Levels

| Level | Purpose | Frequency | Duration |
|-------|---------|-----------|----------|
| **Unit** | Test individual components | Every commit | Seconds |
| **Integration** | Test component interactions | Every commit | Seconds-Minutes |
| **Multi-Node** | Test distributed behavior | Pre-release | Minutes |
| **Performance** | Measure throughput/latency | Weekly | Minutes-Hours |
| **Stress** | Test under extreme load | Pre-release | Hours |
| **Byzantine** | Test BFT safety | Pre-release | Minutes-Hours |

## Unit Testing

### Running Unit Tests

Run all unit tests:

```bash
go test ./...
```

Run with race detection:

```bash
go test -race ./...
```

Run specific package:

```bash
go test -v ./worker
```

Run specific test:

```bash
go test -v -run TestWorkerAddTx ./worker
```

### Example Unit Test

```go
package worker

import (
    "testing"
    "time"

    "github.com/blockberries/looseberry/store"
    "github.com/blockberries/looseberry/types"
)

func TestWorkerBatching(t *testing.T) {
    // Setup
    batchStore := store.NewMemoryBatchStore()
    txIndex := store.NewMemoryTxIndex()
    defer batchStore.Close()
    defer txIndex.Close()

    cfg := DefaultConfig()
    cfg.BatchSize = 5
    cfg.BatchTimeout = 100 * time.Millisecond

    w := New(0, 0, cfg, batchStore, txIndex, 1)
    if err := w.Start(); err != nil {
        t.Fatalf("Start failed: %v", err)
    }
    defer w.Stop()

    // Subscribe to batches
    batches := w.Batches()

    // Add transactions
    for i := 0; i < 5; i++ {
        tx := types.Transaction([]byte{byte(i)})
        if err := w.AddTx(tx); err != nil {
            t.Fatalf("AddTx failed: %v", err)
        }
    }

    // Wait for batch
    select {
    case batch := <-batches:
        if len(batch.Transactions) != 5 {
            t.Errorf("Expected 5 transactions, got %d", len(batch.Transactions))
        }
    case <-time.After(200 * time.Millisecond):
        t.Fatal("Timeout waiting for batch")
    }
}

func TestWorkerBackpressure(t *testing.T) {
    batchStore := store.NewMemoryBatchStore()
    txIndex := store.NewMemoryTxIndex()
    defer batchStore.Close()
    defer txIndex.Close()

    cfg := DefaultConfig()
    cfg.BatchSize = 1000
    cfg.MaxPendingTxs = 10
    cfg.BatchTimeout = 1 * time.Hour // Don't timeout

    w := New(0, 0, cfg, batchStore, txIndex, 1)
    if err := w.Start(); err != nil {
        t.Fatalf("Start failed: %v", err)
    }
    defer w.Stop()

    // Fill to capacity
    for i := 0; i < 10; i++ {
        tx := types.Transaction([]byte{byte(i)})
        if err := w.AddTx(tx); err != nil {
            t.Fatalf("AddTx %d failed: %v", i, err)
        }
    }

    // Next transaction should fail with backpressure
    tx := types.Transaction([]byte{99})
    err := w.AddTx(tx)
    if err != types.ErrWorkerBackpressure {
        t.Errorf("Expected ErrWorkerBackpressure, got %v", err)
    }
}
```

### Test Helpers

Create reusable test helpers:

```go
// testutil/helpers.go
package testutil

import (
    "testing"

    "github.com/blockberries/looseberry/types"
)

func MustGenerateSigner(t *testing.T, index uint16) *types.Ed25519Signer {
    t.Helper()
    signer, err := types.GenerateEd25519Signer(index)
    if err != nil {
        t.Fatalf("Failed to generate signer: %v", err)
    }
    return signer
}

func CreateValidatorSet(t *testing.T, n int) (*types.SimpleValidatorSet, []*types.Ed25519Signer) {
    t.Helper()

    signers := make([]*types.Ed25519Signer, n)
    validators := make([]*types.Validator, n)

    for i := 0; i < n; i++ {
        signers[i] = MustGenerateSigner(t, uint16(i))
        validators[i] = &types.Validator{
            Index:     uint16(i),
            PublicKey: signers[i].PublicKey(),
        }
    }

    return types.NewSimpleValidatorSet(validators, 0), signers
}
```

## Integration Testing

Integration tests verify component interactions.

### Test Network Setup

Looseberry provides a test network harness:

```go
package looseberry_test

import (
    "testing"
    "time"

    "github.com/blockberries/looseberry"
)

func TestIntegrationBasic(t *testing.T) {
    // Create 4-node test network
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()

    if err := tn.Start(); err != nil {
        t.Fatalf("Start failed: %v", err)
    }

    // Submit transactions to node 0
    node0 := tn.Nodes()[0]
    for i := 0; i < 100; i++ {
        tx := []byte("test-tx-" + string(rune(i)))
        if err := node0.Looseberry.AddTx(tx); err != nil {
            t.Fatalf("AddTx failed: %v", err)
        }
    }

    // Wait for certificate formation
    time.Sleep(2 * time.Second)

    // Verify all nodes have certificates
    for i, node := range tn.Nodes() {
        round := node.Looseberry.CurrentRound()
        if round == 0 {
            t.Errorf("Node %d has not advanced (round=%d)", i, round)
        }
        t.Logf("Node %d: round=%d", i, round)
    }
}
```

### Testing Certificate Formation

```go
func TestCertificateFormation(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    node0 := tn.Nodes()[0]

    // Add transactions
    for i := 0; i < 10; i++ {
        tx := []byte("test-tx")
        node0.Looseberry.AddTx(tx)
    }

    // Wait for certificates
    time.Sleep(1 * time.Second)

    // Verify certificates exist
    metrics := node0.Looseberry.Metrics()
    if metrics.TotalCertificates == 0 {
        t.Fatal("No certificates formed")
    }

    t.Logf("Formed %d certificates", metrics.TotalCertificates)
}
```

### Testing Synchronization

```go
func TestSynchronization(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    // Stop node 3
    node3 := tn.Nodes()[3]
    node3.Looseberry.Stop()

    // Generate traffic on other nodes
    for i := 0; i < 3; i++ {
        for j := 0; j < 100; j++ {
            tx := []byte("test-tx")
            tn.Nodes()[i].Looseberry.AddTx(tx)
        }
    }

    // Wait for rounds to advance
    time.Sleep(2 * time.Second)

    // Restart node 3
    node3.Looseberry.Start()

    // Wait for sync
    time.Sleep(3 * time.Second)

    // Verify node 3 caught up
    round3 := node3.Looseberry.CurrentRound()
    round0 := tn.Nodes()[0].Looseberry.CurrentRound()

    if round3 < round0-5 {
        t.Errorf("Node 3 did not sync: round=%d, expected~%d", round3, round0)
    }
}
```

## Multi-Node Testing

Test distributed behavior across multiple nodes.

### Creating a Test Network

```go
type TestNode struct {
    Index      uint16
    Looseberry *looseberry.Looseberry
    Network    *network.MockNetwork
}

type TestNetwork struct {
    nodes []*TestNode
    vs    *types.SimpleValidatorSet
}

func NewTestNetwork(t *testing.T, n int) *TestNetwork {
    // Create validators
    validators := make([]*types.Validator, n)
    signers := make([]*types.Ed25519Signer, n)

    for i := 0; i < n; i++ {
        signer, _ := types.GenerateEd25519Signer(uint16(i))
        signers[i] = signer
        validators[i] = &types.Validator{
            Index:     uint16(i),
            PublicKey: signer.PublicKey(),
        }
    }

    vs := types.NewSimpleValidatorSet(validators, 0)

    // Create nodes
    nodes := make([]*TestNode, n)
    networks := make([]*network.MockNetwork, n)

    for i := 0; i < n; i++ {
        cfg := looseberry.DefaultConfig()
        cfg.ValidatorIndex = uint16(i)
        cfg.Signer = signers[i]
        cfg.Storage.InMemory = true

        lb, _ := looseberry.New(cfg)
        lb.SetValidatorSet(vs)

        mockNet := network.NewMockNetwork(uint16(i), network.DefaultConfig())
        lb.SetNetwork(mockNet)

        nodes[i] = &TestNode{
            Index:      uint16(i),
            Looseberry: lb,
            Network:    mockNet,
        }
        networks[i] = mockNet
    }

    // Connect networks
    for i := 0; i < n; i++ {
        for j := i + 1; j < n; j++ {
            networks[i].Connect(networks[j])
        }
    }

    return &TestNetwork{
        nodes: nodes,
        vs:    vs,
    }
}

func (tn *TestNetwork) Start() error {
    for _, node := range tn.nodes {
        if err := node.Looseberry.Start(); err != nil {
            return err
        }
    }
    return nil
}

func (tn *TestNetwork) Stop() error {
    for _, node := range tn.nodes {
        node.Looseberry.Stop()
    }
    return nil
}

func (tn *TestNetwork) Nodes() []*TestNode {
    return tn.nodes
}
```

### Testing Consensus Integration

```go
func TestConsensusIntegration(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    // Submit transactions
    for i := 0; i < 100; i++ {
        tx := []byte("test-tx")
        tn.Nodes()[0].Looseberry.AddTx(tx)
    }

    // Wait for certificates
    time.Sleep(1 * time.Second)

    // Simulate consensus reaping
    node := tn.Nodes()[0]
    batches := node.Looseberry.ReapCertifiedBatches(1024 * 1024)

    if len(batches) == 0 {
        t.Fatal("No certified batches available")
    }

    t.Logf("Reaped %d certified batches", len(batches))

    // Simulate consensus commit
    highestRound := uint64(0)
    for _, cb := range batches {
        if cb.Certificate.Header.Round > highestRound {
            highestRound = cb.Certificate.Header.Round
        }
    }

    // Notify all nodes
    for _, n := range tn.Nodes() {
        n.Looseberry.NotifyCommitted(highestRound)
    }

    // Verify GC ran
    time.Sleep(100 * time.Millisecond)
    metrics := node.Looseberry.Metrics()
    if metrics.CommittedRound != highestRound {
        t.Errorf("Expected committed round %d, got %d",
            highestRound, metrics.CommittedRound)
    }
}
```

## Performance Testing

### Benchmarking

Run benchmarks:

```bash
go test -bench=. -benchmem ./...
```

Run specific benchmark:

```bash
go test -bench=BenchmarkLooseberryAddTx -benchmem .
```

### Example Benchmarks

```go
func BenchmarkLooseberryAddTx(b *testing.B) {
    cfg := looseberry.DefaultConfig()
    cfg.Storage.InMemory = true

    signer, _ := types.GenerateEd25519Signer(0)
    cfg.Signer = signer
    cfg.ValidatorIndex = 0

    lb, _ := looseberry.New(cfg)

    validators := []*types.Validator{
        {Index: 0, PublicKey: signer.PublicKey()},
    }
    lb.SetValidatorSet(types.NewSimpleValidatorSet(validators, 0))
    lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))
    lb.Start()
    defer lb.Stop()

    // Pre-generate transactions
    txs := make([][]byte, b.N)
    for i := 0; i < b.N; i++ {
        txs[i] = []byte("benchmark-tx")
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        lb.AddTx(txs[i])
    }
}

func BenchmarkLooseberryAddTxParallel(b *testing.B) {
    // Setup...

    b.RunParallel(func(pb *testing.PB) {
        i := 0
        for pb.Next() {
            tx := []byte("benchmark-tx")
            lb.AddTx(tx)
            i++
        }
    })
}
```

### Throughput Measurement

```go
func TestThroughput(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    node := tn.Nodes()[0]

    txCount := 10000
    start := time.Now()

    for i := 0; i < txCount; i++ {
        tx := []byte("test-tx")
        if err := node.Looseberry.AddTx(tx); err != nil {
            if !types.IsRetryable(err) {
                t.Fatalf("AddTx failed: %v", err)
            }
            time.Sleep(1 * time.Millisecond)
            i--
        }
    }

    duration := time.Since(start)
    throughput := float64(txCount) / duration.Seconds()

    t.Logf("Throughput: %.0f tx/s", throughput)

    if throughput < 1000 {
        t.Errorf("Throughput too low: %.0f tx/s", throughput)
    }
}
```

## Stress Testing

Test under extreme load conditions.

### High-Volume Stress Test

```go
func TestStressHighVolume(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping stress test in short mode")
    }

    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    const (
        txCount    = 100000
        goroutines = 12
    )

    errCh := make(chan error, goroutines)
    var wg sync.WaitGroup

    start := time.Now()

    // Spawn goroutines
    for g := 0; g < goroutines; g++ {
        wg.Add(1)
        go func(gid int) {
            defer wg.Done()

            node := tn.Nodes()[gid%4]
            for i := 0; i < txCount/goroutines; i++ {
                tx := []byte(fmt.Sprintf("tx-%d-%d", gid, i))
                for {
                    err := node.Looseberry.AddTx(tx)
                    if err == nil {
                        break
                    }
                    if !types.IsRetryable(err) {
                        errCh <- err
                        return
                    }
                    time.Sleep(1 * time.Millisecond)
                }
            }
        }(g)
    }

    wg.Wait()
    close(errCh)

    // Check for errors
    for err := range errCh {
        t.Errorf("Error during stress test: %v", err)
    }

    duration := time.Since(start)
    throughput := float64(txCount) / duration.Seconds()

    t.Logf("Stress test completed:")
    t.Logf("  Transactions: %d", txCount)
    t.Logf("  Duration: %v", duration)
    t.Logf("  Throughput: %.0f tx/s", throughput)

    // Verify all nodes are healthy
    for i, node := range tn.Nodes() {
        metrics := node.Looseberry.Metrics()
        t.Logf("Node %d: round=%d, pending=%d",
            i, metrics.CurrentRound, metrics.PendingTxCount)
    }
}
```

### Memory Stability Test

```go
func TestStressMemoryStability(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping stress test in short mode")
    }

    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    initialHeap := m.HeapAlloc

    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    // Run for extended period
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    done := time.After(30 * time.Second)
    txCount := 0

    for {
        select {
        case <-done:
            goto finish
        case <-ticker.C:
            for i := 0; i < 100; i++ {
                tx := []byte("test-tx")
                tn.Nodes()[0].Looseberry.AddTx(tx)
                txCount++
            }
        }
    }

finish:
    runtime.ReadMemStats(&m)
    finalHeap := m.HeapAlloc
    heapGrowth := finalHeap - initialHeap

    t.Logf("Memory stability test:")
    t.Logf("  Transactions: %d", txCount)
    t.Logf("  Initial heap: %d MB", initialHeap/(1024*1024))
    t.Logf("  Final heap: %d MB", finalHeap/(1024*1024))
    t.Logf("  Heap growth: %d MB", heapGrowth/(1024*1024))

    // Heap should not grow unboundedly
    if heapGrowth > 100*1024*1024 { // 100MB
        t.Errorf("Excessive heap growth: %d MB", heapGrowth/(1024*1024))
    }
}
```

## Byzantine Fault Testing

Test BFT safety properties.

### Invalid Signature Test

```go
func TestByzantineInvalidSignature(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    // Create invalid header with wrong signature
    wrongSigner, _ := types.GenerateEd25519Signer(999)

    header := &types.Header{
        Author: 0,
        Round:  1,
        Epoch:  0,
    }
    header.Sign(wrongSigner) // Sign with wrong key

    // Try to process invalid header
    // Should be rejected
    // (Implementation depends on test harness)
}
```

### Equivocation Test

```go
func TestByzantineEquivocation(t *testing.T) {
    tn := NewTestNetwork(t, 4)
    defer tn.Stop()
    tn.Start()

    // Create two conflicting certificates for same round
    // Verify they are detected and rejected
    // (Implementation depends on Byzantine detection logic)
}
```

## Test Utilities

### Mock Network

Use the mock network for testing:

```go
mockNet := network.NewMockNetwork(0, network.DefaultConfig())

// Connect to other mock networks
mockNet.Connect(otherMockNet)

// Inject messages for testing
mockNet.InjectBatch(batch)
```

### In-Memory Storage

Use in-memory storage for fast tests:

```go
batchStore := store.NewMemoryBatchStore()
certStore := store.NewMemoryCertificateStore()
txIndex := store.NewMemoryTxIndex()
```

### Test Validators

Generate test validators:

```go
func createTestValidators(n int) ([]*types.Validator, []*types.Ed25519Signer) {
    validators := make([]*types.Validator, n)
    signers := make([]*types.Ed25519Signer, n)

    for i := 0; i < n; i++ {
        signer, _ := types.GenerateEd25519Signer(uint16(i))
        signers[i] = signer
        validators[i] = &types.Validator{
            Index:     uint16(i),
            PublicKey: signer.PublicKey(),
        }
    }

    return validators, signers
}
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Run tests
        run: make test

      - name: Run race detector
        run: go test -race ./...

      - name: Run benchmarks
        run: go test -bench=. -benchmem ./...
```

### Makefile Targets

```makefile
.PHONY: test
test:
	go test -v -race ./...

.PHONY: test-short
test-short:
	go test -v -short ./...

.PHONY: test-integration
test-integration:
	go test -v -run Integration ./test

.PHONY: bench
bench:
	go test -bench=. -benchmem ./...

.PHONY: stress
stress:
	go test -v -run Stress ./test
```

## Next Steps

- **[Deployment Guide](DEPLOYMENT.md)**: Deploy to production
- **[Monitoring Guide](MONITORING.md)**: Set up observability
- **[Performance Tuning Tutorial](../tutorials/PERFORMANCE_TUNING.md)**: Optimize performance
