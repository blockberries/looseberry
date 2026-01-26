# Looseberry

A high-performance DAG-based mempool implementation for Byzantine fault-tolerant consensus systems.

## Overview

Looseberry is a mempool module inspired by the [Narwhal protocol](https://arxiv.org/abs/2105.11827) that separates transaction dissemination from transaction ordering. This separation enables high throughput by allowing validators to disseminate data in parallel while maintaining BFT safety guarantees.

### Key Features

- **DAG-Based Architecture**: Certificates form a directed acyclic graph that captures causal dependencies
- **BFT Safety**: Tolerates up to f Byzantine validators where n = 3f + 1 (quorum = 2f + 1)
- **High Throughput**: Parallel transaction batching with dynamic worker scaling
- **Certificate-Based Data Availability**: Certificates prove that data has been received by a quorum of validators
- **Flow Control**: Prevents unbounded DAG growth with backpressure mechanisms
- **Garbage Collection**: Automatic pruning with uncommitted transaction recovery
- **Pluggable Storage**: In-memory stores for testing, LevelDB for production

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Looseberry                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Worker 0   │    │   Worker 1   │    │   Worker N   │      │
│  │  (batching)  │    │  (batching)  │    │  (batching)  │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │               │
│         └───────────────────┼───────────────────┘               │
│                             ▼                                   │
│                    ┌────────────────┐                           │
│                    │    Primary     │                           │
│                    │ (headers/certs)│                           │
│                    └────────┬───────┘                           │
│                             │                                   │
│         ┌───────────────────┼───────────────────┐               │
│         ▼                   ▼                   ▼               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │     DAG     │    │   Network   │    │     GC      │         │
│  │ (ordering)  │    │   (P2P)     │    │ (cleanup)   │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **Workers** | Collect transactions into batches with deduplication and backpressure |
| **Worker Pool** | Hash-based routing with dynamic scaling (1-8 workers) |
| **Primary** | Creates headers referencing batches, collects votes, forms certificates |
| **DAG** | Maintains certificate graph with causal ordering |
| **Network** | P2P communication for batches, headers, votes, certificates |
| **Sync Manager** | Catches up nodes that fall behind |
| **GC Manager** | Prunes old rounds and recovers uncommitted transactions |
| **Flow Controller** | Prevents runaway round advancement |

### Data Flow

1. **Transaction Submission**: Client submits transaction via `AddTx()`
2. **Batching**: Workers collect transactions into batches (timeout or size-based)
3. **Header Creation**: Primary creates header referencing batch digests
4. **Voting**: Other validators verify and vote on headers
5. **Certificate Formation**: Primary collects 2f+1 votes to form certificate
6. **DAG Integration**: Certificate is added to DAG and broadcast
7. **Ordering**: Consensus layer orders certificates deterministically
8. **Commit**: Committed batches are reaped via `ReapCertifiedBatches()`

## Installation

```bash
go get github.com/blockberries/looseberry
```

### Requirements

- Go 1.25.6 or later
- LevelDB (for persistent storage)

## Quick Start

```go
package main

import (
    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

func main() {
    // Generate validator keys
    signer, _ := types.GenerateEd25519Signer(0)

    validators := []*types.Validator{
        {Index: 0, PublicKey: signer.PublicKey()},
        // ... other validators
    }
    vs := types.NewSimpleValidatorSet(validators, 0)

    // Create configuration
    cfg := looseberry.DefaultConfig()
    cfg.Signer = signer
    cfg.ValidatorIndex = 0
    cfg.Storage.InMemory = true  // Use in-memory storage

    // Create Looseberry instance
    lb, err := looseberry.New(cfg)
    if err != nil {
        panic(err)
    }

    // Set validator set and network
    lb.SetValidatorSet(vs)
    lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))

    // Start the mempool
    if err := lb.Start(); err != nil {
        panic(err)
    }
    defer lb.Stop()

    // Submit transactions
    err = lb.AddTx([]byte("transaction data"))
    if err != nil {
        // Handle error
    }

    // Check mempool status
    size := lb.Size()           // Number of pending transactions
    round := lb.CurrentRound()  // Current DAG round
    metrics := lb.Metrics()     // Detailed metrics
}
```

## API Reference

### DAGMempool Interface

```go
type DAGMempool interface {
    // AddTx adds a transaction to the mempool
    AddTx(tx []byte) error

    // ReapCertifiedBatches returns committed batches up to maxBytes
    ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

    // NotifyCommitted notifies that a round has been committed by consensus
    NotifyCommitted(round uint64)

    // UpdateValidatorSet updates the validator set for a new epoch
    UpdateValidatorSet(validators types.ValidatorSet)

    // HasTx checks if a transaction exists in the mempool
    HasTx(hash []byte) bool

    // Size returns the number of pending transactions
    Size() int

    // SizeBytes returns the total bytes of pending transactions
    SizeBytes() int64

    // Flush clears all pending transactions
    Flush()

    // CurrentRound returns the current DAG round
    CurrentRound() uint64

    // Metrics returns current mempool metrics
    Metrics() *Metrics
}
```

### Metrics

```go
type Metrics struct {
    TotalTxAdded     uint64  // Total transactions added
    TotalTxRejected  uint64  // Total transactions rejected
    TotalBatches     uint64  // Total batches created
    PendingTxCount   int     // Current pending transactions
    PendingTxBytes   int64   // Current pending bytes
    WorkerCount      int     // Active worker count
    DAGHeight        uint64  // Current DAG round
}
```

## Configuration

### Default Configuration

```go
cfg := looseberry.DefaultConfig()
```

### Configuration Options

| Section | Option | Default | Description |
|---------|--------|---------|-------------|
| **Worker** | BatchSize | 500 | Transactions per batch |
| | BatchTimeout | 100ms | Max time before batch creation |
| | MaxPendingTxs | 10,000 | Backpressure: max pending txs |
| | MaxPendingBytes | 50MB | Backpressure: max pending bytes |
| | AckTimeout | 30s | Batch acknowledgment timeout |
| **Pool** | MinWorkers | 1 | Minimum worker count |
| | MaxWorkers | 8 | Maximum worker count |
| | ScaleUpThreshold | 0.8 | Load ratio to trigger scale-up |
| | ScaleDownThreshold | 0.2 | Load ratio to trigger scale-down |
| | ScaleCooldown | 5s | Minimum time between scaling |
| **Primary** | HeaderTimeout | 500ms | Time between header creation |
| | MaxBatchesPerHeader | 100 | Max batches per header |
| | VoteTimeout | 30s | Time to wait for votes |
| | MaxRoundGap | 10 | Max allowed round difference |
| **Sync** | SyncInterval | 10s | Time between sync checks |
| | SyncThreshold | 5 | Round gap to trigger sync |
| | SyncBatchSize | 100 | Certificates per sync request |
| **GC** | GCDepth | 100 | Rounds to keep before pruning |
| | GCInterval | 30s | Time between GC runs |
| | RecoverUncommittedTxs | true | Recover txs from pruned batches |
| **FlowControl** | MaxUncommittedRounds | 100 | Max gap before pausing |
| | MaxPendingBatches | 1000 | Max unacknowledged batches |
| | MaxPendingHeaders | 100 | Max headers without certificate |
| **Storage** | InMemory | false | Use in-memory stores |
| | DataDir | "data" | Directory for LevelDB |

### Transaction Validation

```go
cfg.TxValidator = func(tx types.Transaction) error {
    // Custom validation logic
    if len(tx) > 1024*1024 {
        return errors.New("transaction too large")
    }
    return nil
}
```

## Storage

### In-Memory Storage

Best for testing and development:

```go
cfg.Storage.InMemory = true
```

### LevelDB Storage

For production with persistence:

```go
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"
```

### Store Interfaces

```go
// BatchStore stores transaction batches
type BatchStore interface {
    SaveBatch(batch *types.Batch) error
    GetBatch(digest types.Hash) (*types.Batch, error)
    HasBatch(digest types.Hash) bool
    GetBatchesByRound(round uint64) ([]*types.Batch, error)
    DeleteBatchesBefore(round uint64) error
    Close() error
}

// CertificateStore stores certificates
type CertificateStore interface {
    SaveCertificate(cert *types.Certificate) error
    GetCertificate(digest types.Hash) (*types.Certificate, error)
    HasCertificate(digest types.Hash) bool
    GetCertificatesByRound(round uint64) ([]*types.Certificate, error)
    GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error)
    DeleteCertificatesBefore(round uint64) error
    HighestRound() uint64
    Close() error
}

// TxIndex provides O(1) transaction lookups
type TxIndex interface {
    AddTx(txHash, batchHash types.Hash) error
    GetBatchForTx(txHash types.Hash) (types.Hash, error)
    HasTx(txHash types.Hash) bool
    RemoveTxsForBatch(batchHash types.Hash) error
    AddBatch(batch *types.Batch) error
    Close() error
}
```

## Types

### Core Types

```go
// Hash is a 32-byte SHA-256 digest
type Hash [32]byte

// Transaction is a byte slice wrapper
type Transaction []byte

// Batch contains transactions from a worker
type Batch struct {
    WorkerID     uint8
    ValidatorID  uint16
    Round        uint64
    Transactions []Transaction
    Digest       Hash
    Timestamp    time.Time
}

// Header is a DAG vertex
type Header struct {
    Author     uint16
    Round      uint64
    Epoch      uint64
    BatchRefs  []BatchDigest
    Parents    []CertificateRef
    Digest     Hash
    Signature  Signature
}

// Vote is a validator's endorsement of a header
type Vote struct {
    HeaderDigest Hash
    Validator    uint16
    Signature    Signature
}

// Certificate proves data availability (header + 2f+1 votes)
type Certificate struct {
    Header     Header
    Votes      []Vote
    SignerMask BitSet
}
```

### Validator Set

```go
type ValidatorSet interface {
    Count() int
    GetByIndex(index uint16) (*Validator, error)
    Contains(index uint16) bool
    F() int                                    // Max Byzantine validators
    Quorum() int                               // 2f + 1
    Epoch() uint64
    VerifySignature(pk PublicKey, msg, sig []byte) bool
    Validators() []*Validator
}
```

## Network Protocol

### Message Types

| Message | Direction | Description |
|---------|-----------|-------------|
| BatchMessage | Broadcast | Worker batch with transactions |
| BatchAckMessage | P2P | Acknowledgment of received batch |
| HeaderMessage | Broadcast | Primary header referencing batches |
| VoteMessage | P2P | Vote on a header |
| CertificateMessage | Broadcast | Complete certificate |
| SyncRequest | P2P | Request missing certificates |
| SyncResponse | P2P | Certificates and batches |

### Network Interface

```go
type Network interface {
    Start() error
    Stop() error

    // Broadcast to all validators
    BroadcastBatch(batch *types.Batch) error
    BroadcastHeader(header *types.Header) error
    BroadcastCertificate(cert *types.Certificate) error

    // Send to specific validator
    SendVote(to uint16, vote *types.Vote) error
    SendBatchAck(to uint16, ack *BatchAck) error
    SendSyncRequest(to uint16, req *SyncRequest) error
    SendSyncResponse(to uint16, resp *SyncResponse) error

    // Receive channels
    BatchMessages() <-chan *BatchMessage
    HeaderMessages() <-chan *HeaderMessage
    VoteMessages() <-chan *VoteMessage
    CertificateMessages() <-chan *CertificateMessage
    BatchAckMessages() <-chan *BatchAckMessage
    SyncRequestMessages() <-chan *SyncRequestMessage
    SyncResponseMessages() <-chan *SyncResponseMessage
}
```

## Performance

### Benchmark Results

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Worker AddTx | 12M ops/sec | ~80ns |
| Worker Pool AddTx | 7M ops/sec | ~145ns |
| Worker Pool AddTx (parallel) | 5M ops/sec | ~188ns |
| DAG AddCertificate | 1.4M ops/sec | ~734ns |
| Looseberry AddTx | 7.5M ops/sec | ~132ns |
| Looseberry AddTx (parallel) | 3M ops/sec | ~335ns |

### Stress Test Results

- **Sequential throughput**: 250,000+ transactions/second
- **Concurrent throughput**: 200,000+ transactions/second (12 goroutines)
- **Memory stability**: <35MB heap growth over 380,000 transactions
- **DAG performance**: 2,200+ certificates/second for 4,000 certificates

## Testing

### Run Unit Tests

```bash
make test
```

### Run with Race Detection

```bash
go test -race -v ./...
```

### Run Benchmarks

```bash
go test -bench=. -benchmem ./...
```

### Run Specific Benchmark

```bash
go test -bench=BenchmarkLooseberryAddTx -benchmem .
```

## Error Handling

### Error Categories

```go
// IsRetryable returns true for transient errors
types.IsRetryable(err)

// IsByzantine returns true for Byzantine behavior
types.IsByzantine(err)
```

### Common Errors

| Error | Retryable | Byzantine | Description |
|-------|-----------|-----------|-------------|
| ErrMempoolFull | Yes | No | Backpressure limit reached |
| ErrDuplicateTx | No | No | Transaction already exists |
| ErrTxTooLarge | No | No | Transaction exceeds size limit |
| ErrInvalidSignature | No | Yes | Cryptographic verification failed |
| ErrInvalidRound | No | Maybe | Round outside acceptable range |
| ErrNoQuorum | No | No | Insufficient votes for certificate |
| ErrEquivocation | No | Yes | Validator signed conflicting data |

## Integration

### With Consensus (blockberry)

```go
// Consensus notifies committed rounds
lb.NotifyCommitted(round)

// Consensus reaps certified batches for block building
batches := lb.ReapCertifiedBatches(maxBytes)
for _, batch := range batches {
    for _, tx := range batch.Transactions {
        // Include in block
    }
}
```

### With Networking (glueberry)

Implement the `network.Network` interface to wrap your P2P layer:

```go
type GlueberryNetwork struct {
    node *glueberry.Node
    // ... channels for message routing
}

func (n *GlueberryNetwork) BroadcastBatch(batch *types.Batch) error {
    // Serialize and broadcast via glueberry
}
// ... implement other methods
```

## Project Structure

```
looseberry/
├── looseberry.go       # Main struct and DAGMempool interface
├── config.go           # Configuration types
├── types/              # Core types (Hash, Tx, Batch, Header, Vote, Cert)
│   ├── hash.go
│   ├── transaction.go
│   ├── batch.go
│   ├── header.go
│   ├── vote.go
│   ├── certificate.go
│   ├── validator.go
│   ├── signer.go
│   └── errors.go
├── store/              # Storage implementations
│   ├── store.go        # Interfaces
│   ├── memory_*.go     # In-memory implementations
│   └── leveldb_*.go    # LevelDB implementations
├── worker/             # Transaction batching
│   ├── worker.go
│   ├── pool.go
│   ├── scaler.go
│   └── ack_tracker.go
├── primary/            # Header/certificate creation
│   ├── primary.go
│   └── vote_tracker.go
├── dag/                # Certificate graph
│   └── dag.go
├── gc/                 # Garbage collection
│   ├── gc.go
│   └── flow.go
├── network/            # Network protocol
│   ├── network.go
│   ├── mock.go
│   └── sync.go
├── *_test.go           # Unit tests
├── integration_test.go # Multi-node tests
├── benchmark_test.go   # Performance benchmarks
├── stress_test.go      # High-volume tests
└── byzantine_test.go   # BFT safety tests
```

## Related Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed system architecture
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) - Development phases
- [PROGRESS_REPORT.md](PROGRESS_REPORT.md) - Implementation status

## License

Copyright 2024 Blockberries. All rights reserved.
