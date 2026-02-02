# Getting Started with Looseberry

This guide will help you get started with Looseberry, a high-performance DAG-based mempool for Byzantine fault-tolerant consensus systems.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Understanding Looseberry](#understanding-looseberry)
- [Creating Your First Instance](#creating-your-first-instance)
- [Basic Operations](#basic-operations)
- [Configuration](#configuration)
- [Next Steps](#next-steps)

## Prerequisites

Before you begin, ensure you have the following:

- **Go 1.21 or later**: Looseberry is written in Go and requires a recent version.
- **Basic understanding of BFT consensus**: Familiarity with Byzantine fault tolerance concepts.
- **LevelDB** (optional): For persistent storage in production deployments.

## Installation

Install Looseberry using Go modules:

```bash
go get github.com/blockberries/looseberry
```

Or add it to your project's `go.mod`:

```bash
go mod init your-project
go get github.com/blockberries/looseberry
```

## Understanding Looseberry

Looseberry is a **DAG-based mempool** that separates transaction dissemination from transaction ordering:

- **Transaction dissemination**: Handled by workers and primary nodes that create certificates
- **Transaction ordering**: Delegated to an external consensus layer (e.g., blockberry)

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Transaction** | Opaque byte slice submitted to the mempool |
| **Batch** | Collection of transactions from a single worker |
| **Header** | DAG vertex that references batches and parent certificates |
| **Vote** | Validator's cryptographic endorsement of a header |
| **Certificate** | Header + 2f+1 votes (proves data availability) |
| **DAG** | Directed acyclic graph of certificates |
| **Worker** | Component that collects transactions into batches |
| **Primary** | Component that creates headers and collects votes |

### The Looseberry Workflow

```
1. Client submits transaction via AddTx()
2. Worker collects transactions into batches
3. Primary creates header referencing batch digests
4. Validators vote on the header
5. Primary collects 2f+1 votes to form certificate
6. Certificate is added to DAG and broadcast
7. Consensus orders certificates deterministically
8. Committed batches are reaped via ReapCertifiedBatches()
```

## Creating Your First Instance

### Step 1: Generate Validator Keys

Each validator needs an Ed25519 key pair:

```go
package main

import (
    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

func main() {
    // Generate a signer for validator 0
    signer, err := types.GenerateEd25519Signer(0)
    if err != nil {
        panic(err)
    }
}
```

### Step 2: Create Validator Set

Define the validator set with all participating validators:

```go
validators := []*types.Validator{
    {Index: 0, PublicKey: signer0.PublicKey()},
    {Index: 1, PublicKey: signer1.PublicKey()},
    {Index: 2, PublicKey: signer2.PublicKey()},
    {Index: 3, PublicKey: signer3.PublicKey()},
}

// Create validator set for epoch 0
validatorSet := types.NewSimpleValidatorSet(validators, 0)
```

**Important**: For BFT safety, you need at least **n = 3f + 1** validators where **f** is the maximum number of Byzantine validators. For example:
- 4 validators tolerate 1 Byzantine (f=1)
- 7 validators tolerate 2 Byzantine (f=2)
- 10 validators tolerate 3 Byzantine (f=3)

### Step 3: Configure Looseberry

Create a configuration with sensible defaults:

```go
cfg := looseberry.DefaultConfig()
cfg.Signer = signer
cfg.ValidatorIndex = 0
cfg.Storage.InMemory = true  // Use in-memory storage for testing
```

### Step 4: Create and Start Looseberry

```go
// Create Looseberry instance
lb, err := looseberry.New(cfg)
if err != nil {
    panic(err)
}

// Set validator set
lb.SetValidatorSet(validatorSet)

// Set network (using mock network for testing)
mockNet := network.NewMockNetwork(0, network.DefaultConfig())
lb.SetNetwork(mockNet)

// Start the mempool
if err := lb.Start(); err != nil {
    panic(err)
}
defer lb.Stop()
```

### Complete Example

Here's a complete working example:

```go
package main

import (
    "fmt"
    "time"

    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

func main() {
    // Generate validator keys
    signer, err := types.GenerateEd25519Signer(0)
    if err != nil {
        panic(err)
    }

    // Create validator set (single node for simplicity)
    validators := []*types.Validator{
        {Index: 0, PublicKey: signer.PublicKey()},
    }
    validatorSet := types.NewSimpleValidatorSet(validators, 0)

    // Create configuration
    cfg := looseberry.DefaultConfig()
    cfg.Signer = signer
    cfg.ValidatorIndex = 0
    cfg.Storage.InMemory = true

    // Create Looseberry instance
    lb, err := looseberry.New(cfg)
    if err != nil {
        panic(err)
    }

    // Set validator set and network
    lb.SetValidatorSet(validatorSet)
    lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))

    // Start the mempool
    if err := lb.Start(); err != nil {
        panic(err)
    }
    defer lb.Stop()

    fmt.Println("Looseberry started successfully!")

    // Give it a moment to initialize
    time.Sleep(100 * time.Millisecond)

    fmt.Printf("Current round: %d\n", lb.CurrentRound())
    fmt.Printf("Pending transactions: %d\n", lb.Size())
}
```

## Basic Operations

### Adding Transactions

Submit transactions to the mempool:

```go
// Add a single transaction
tx := []byte("Hello, Looseberry!")
err := lb.AddTx(tx)
if err != nil {
    // Handle error
    switch {
    case types.IsRetryable(err):
        // Backpressure or flow control - retry later
        fmt.Printf("Retryable error: %v\n", err)
    case err == types.ErrTxAlreadyExists:
        // Transaction already in mempool
        fmt.Println("Duplicate transaction")
    default:
        // Other error
        fmt.Printf("Error: %v\n", err)
    }
}
```

### Checking Transaction Status

Check if a transaction is in the mempool:

```go
txHash := types.HashBytes(tx)
if lb.HasTx(txHash[:]) {
    fmt.Println("Transaction is in mempool")
}
```

### Getting Mempool Status

Retrieve current mempool statistics:

```go
// Simple counters
pendingCount := lb.Size()
pendingBytes := lb.SizeBytes()
currentRound := lb.CurrentRound()

fmt.Printf("Pending: %d transactions (%d bytes)\n", pendingCount, pendingBytes)
fmt.Printf("Current round: %d\n", currentRound)

// Detailed metrics
metrics := lb.Metrics()
fmt.Printf("Total added: %d\n", metrics.TotalTxAdded)
fmt.Printf("Total rejected: %d\n", metrics.TotalTxRejected)
fmt.Printf("Worker count: %d\n", metrics.WorkerCount)
fmt.Printf("Worker load: %.2f\n", metrics.WorkerLoad)
```

### Reaping Certified Batches

When consensus is ready to build a block:

```go
// Reap up to 1MB of certified batches
maxBytes := int64(1024 * 1024)
certifiedBatches := lb.ReapCertifiedBatches(maxBytes)

for _, cb := range certifiedBatches {
    fmt.Printf("Certificate from validator %d, round %d\n",
        cb.Certificate.Header.Author,
        cb.Certificate.Header.Round)

    // Extract transactions
    for _, tx := range cb.Batch.Transactions {
        fmt.Printf("  Transaction: %s\n", string(tx))
        // Include in block
    }
}
```

### Notifying Committed Rounds

After consensus commits a block, notify Looseberry:

```go
// Consensus has committed up to round 42
committedRound := uint64(42)
lb.NotifyCommitted(committedRound)

// This triggers garbage collection and allows
// uncommitted transactions to be recovered
```

### Flushing the Mempool

Clear all pending transactions:

```go
lb.Flush()
fmt.Println("Mempool flushed")
```

## Configuration

### Default Configuration

The default configuration is suitable for most use cases:

```go
cfg := looseberry.DefaultConfig()
// Provides reasonable defaults for all settings
```

### Common Configuration Patterns

#### High-Throughput Configuration

For maximum throughput with adequate resources:

```go
cfg := looseberry.DefaultConfig()
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
cfg.Worker.BatchSize = 1000
cfg.Worker.MaxPendingTxs = 50000
cfg.Worker.MaxPendingBytes = 100 * 1024 * 1024 // 100MB
cfg.Primary.MaxBatchesPerHeader = 200
```

#### Low-Resource Configuration

For constrained environments:

```go
cfg := looseberry.DefaultConfig()
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2
cfg.Worker.BatchSize = 100
cfg.Worker.MaxPendingTxs = 1000
cfg.Worker.MaxPendingBytes = 10 * 1024 * 1024 // 10MB
```

#### Production Configuration

For production with persistence:

```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"
cfg.GC.GCDepth = 100
cfg.GC.RecoverTxs = true
```

### Transaction Validation

Add custom transaction validation:

```go
cfg.TxValidator = func(tx []byte) error {
    // Size limit
    if len(tx) > 1024*1024 {
        return fmt.Errorf("transaction too large: %d bytes", len(tx))
    }

    // Must be valid UTF-8
    if !utf8.Valid(tx) {
        return fmt.Errorf("transaction must be valid UTF-8")
    }

    // Custom validation logic
    // ...

    return nil
}
```

## Next Steps

Now that you have a basic understanding of Looseberry, explore these topics:

1. **[Integration Guide](INTEGRATION.md)**: Learn how to integrate Looseberry with consensus systems
2. **[Configuration Guide](CONFIGURATION.md)**: Deep dive into all configuration options
3. **[Testing Guide](TESTING.md)**: Learn how to test your Looseberry integration
4. **[Deployment Guide](DEPLOYMENT.md)**: Deploy Looseberry in production
5. **[Monitoring Guide](MONITORING.md)**: Set up metrics and observability

### Tutorials

- **[Quickstart Tutorial](../tutorials/QUICKSTART.md)**: 5-minute quickstart
- **[Multi-Node Setup](../tutorials/MULTI_NODE.md)**: Set up a test network
- **[Custom Storage](../tutorials/CUSTOM_STORAGE.md)**: Implement custom storage backend
- **[Performance Tuning](../tutorials/PERFORMANCE_TUNING.md)**: Optimize for your workload

### Reference Documentation

- **[Concurrency Patterns](../reference/CONCURRENCY.md)**: Thread-safety and concurrency
- **[Error Handling](../reference/ERROR_HANDLING.md)**: Error handling best practices
- **[Security](../reference/SECURITY.md)**: Security considerations
- **[FAQ](../reference/FAQ.md)**: Frequently asked questions

## Troubleshooting

### Common Issues

**Issue**: `validator set not set` error

```go
// Solution: Set validator set before starting
lb.SetValidatorSet(validatorSet)
```

**Issue**: `network not set` error

```go
// Solution: Set network before starting
lb.SetNetwork(mockNet)
```

**Issue**: Transactions rejected with `ErrWorkerBackpressure`

```go
// Solution: This is a retryable error - retry after a delay
if types.IsRetryable(err) {
    time.Sleep(10 * time.Millisecond)
    err = lb.AddTx(tx)
}
```

For more troubleshooting guidance, see the [Troubleshooting Reference](../reference/TROUBLESHOOTING.md).

## Summary

You've learned how to:

- Install Looseberry
- Create and configure a Looseberry instance
- Perform basic operations (AddTx, ReapCertifiedBatches, etc.)
- Handle common errors
- Configure Looseberry for different scenarios

Continue to the [Integration Guide](INTEGRATION.md) to learn how to integrate Looseberry with your consensus system.
