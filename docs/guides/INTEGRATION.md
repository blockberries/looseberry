# Integration Guide

This guide covers integrating Looseberry with consensus systems and networking layers.

## Table of Contents

- [Overview](#overview)
- [Integration Architecture](#integration-architecture)
- [Consensus Integration](#consensus-integration)
- [Network Integration](#network-integration)
- [Storage Integration](#storage-integration)
- [Complete Integration Example](#complete-integration-example)
- [Best Practices](#best-practices)

## Overview

Looseberry is designed to integrate seamlessly with BFT consensus systems. It handles transaction dissemination while delegating transaction ordering to the consensus layer.

### Integration Points

| Component | Interface | Purpose |
|-----------|-----------|---------|
| **Consensus** | `DAGMempool` | Submit transactions, reap batches, notify commits |
| **Network** | `network.Network` | P2P communication for batches, headers, votes, certificates |
| **Storage** | `store.*Store` | Persistent storage for batches and certificates |
| **Validation** | `TxValidator` | Custom transaction validation logic |

## Integration Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Consensus Layer                           │
│                   (e.g., blockberry)                        │
│                                                             │
│  - Orders certificates deterministically                    │
│  - Calls ReapCertifiedBatches() for block building         │
│  - Calls NotifyCommitted() after commit                    │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│                     Looseberry                              │
│                   (DAG Mempool)                             │
│                                                             │
│  - Accepts transactions via AddTx()                         │
│  - Creates batches and certificates                         │
│  - Maintains DAG of certificates                            │
│  - Provides certified batches to consensus                  │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Network Layer                             │
│                   (e.g., glueberry)                         │
│                                                             │
│  - P2P communication                                        │
│  - Broadcasts batches, headers, certificates               │
│  - Routes votes and acknowledgments                         │
└─────────────────────────────────────────────────────────────┘
```

## Consensus Integration

### Interface Overview

Consensus systems interact with Looseberry through the `DAGMempool` interface:

```go
type DAGMempool interface {
    // Submit transactions
    AddTx(tx []byte) error

    // Retrieve certified batches for block building
    ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

    // Notify that a round has been committed
    NotifyCommitted(round uint64)

    // Update validator set (epoch change)
    UpdateValidatorSet(validators types.ValidatorSet)

    // Query operations
    HasTx(hash []byte) bool
    Size() int
    SizeBytes() int64
    CurrentRound() uint64
    Metrics() *Metrics

    // Lifecycle
    Flush()
}
```

### Transaction Submission

Applications submit transactions through the consensus layer:

```go
// In your consensus system's RPC handler
func (c *Consensus) BroadcastTx(tx []byte) error {
    // Submit to mempool
    if err := c.mempool.AddTx(tx); err != nil {
        if types.IsRetryable(err) {
            // Backpressure - ask client to retry
            return fmt.Errorf("mempool busy: %w", err)
        }
        return fmt.Errorf("invalid transaction: %w", err)
    }
    return nil
}
```

### Block Building

When consensus is ready to propose a block:

```go
// In your consensus block proposer
func (c *Consensus) proposeBlock() (*Block, error) {
    // Reap certified batches up to max block size
    maxBytes := c.config.MaxBlockSize
    certifiedBatches := c.mempool.ReapCertifiedBatches(maxBytes)

    var transactions [][]byte
    for _, cb := range certifiedBatches {
        // Extract transactions from batches
        for _, tx := range cb.Batch.Transactions {
            transactions = append(transactions, tx)
        }
    }

    // Create block with transactions
    block := &Block{
        Height:       c.height,
        Transactions: transactions,
        // ... other block fields
    }

    return block, nil
}
```

### Commit Notification

After consensus commits a block, notify Looseberry:

```go
// In your consensus commit handler
func (c *Consensus) commitBlock(block *Block) error {
    // Apply block to state machine
    if err := c.applyBlock(block); err != nil {
        return err
    }

    // Notify mempool of committed round
    // This enables garbage collection
    c.mempool.NotifyCommitted(block.Height)

    return nil
}
```

### Certificate Ordering

Consensus must order certificates deterministically. Here's a reference implementation:

```go
// OrderCertificates orders certificates for deterministic execution
func OrderCertificates(certs []*types.Certificate) []*types.Certificate {
    // Sort by round (ascending), then by author (ascending)
    sort.Slice(certs, func(i, j int) bool {
        if certs[i].Header.Round != certs[j].Header.Round {
            return certs[i].Header.Round < certs[j].Header.Round
        }
        return certs[i].Header.Author < certs[j].Header.Author
    })
    return certs
}
```

### Validator Set Updates

When the validator set changes (epoch transition):

```go
// In your consensus epoch transition handler
func (c *Consensus) transitionEpoch(newEpoch uint64, newValidators []*types.Validator) error {
    // Create new validator set
    validatorSet := types.NewSimpleValidatorSet(newValidators, newEpoch)

    // Update Looseberry
    c.mempool.UpdateValidatorSet(validatorSet)

    return nil
}
```

### Complete Consensus Integration

```go
package consensus

import (
    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/types"
)

type Consensus struct {
    mempool       looseberry.DAGMempool
    height        uint64
    validatorSet  types.ValidatorSet
    // ... other consensus state
}

func NewConsensus(mempool looseberry.DAGMempool) *Consensus {
    return &Consensus{
        mempool: mempool,
    }
}

// RPC handler for transaction submission
func (c *Consensus) BroadcastTx(tx []byte) error {
    return c.mempool.AddTx(tx)
}

// Propose a new block
func (c *Consensus) ProposeBlock() (*Block, error) {
    maxBytes := int64(1024 * 1024) // 1MB
    certifiedBatches := c.mempool.ReapCertifiedBatches(maxBytes)

    var txs [][]byte
    for _, cb := range certifiedBatches {
        for _, tx := range cb.Batch.Transactions {
            txs = append(txs, tx)
        }
    }

    return &Block{
        Height:       c.height,
        Transactions: txs,
    }, nil
}

// Commit a block
func (c *Consensus) CommitBlock(block *Block) error {
    // Apply to state machine
    // ...

    // Notify mempool
    c.mempool.NotifyCommitted(block.Height)
    c.height++

    return nil
}
```

## Network Integration

### Network Interface

Implement the `network.Network` interface to integrate with your P2P layer:

```go
type Network interface {
    // Broadcast methods
    BroadcastBatch(batch *types.Batch) error
    BroadcastHeader(header *types.Header) error
    BroadcastCertificate(cert *types.Certificate) error

    // P2P methods
    SendVote(validator uint16, vote *types.Vote) error
    SendBatchAck(validator uint16, ack *BatchAckMessage) error
    SendBatchRequest(validator uint16, req *BatchRequestMessage) error
    SendBatchResponse(validator uint16, resp *BatchResponseMessage) error
    SendSyncRequest(validator uint16, req *SyncRequest) error
    SendSyncResponse(validator uint16, resp *SyncResponse) error

    // Receive channels
    BatchMessages() <-chan *BatchMessage
    BatchAckMessages() <-chan *BatchAckMessage
    BatchRequestMessages() <-chan *BatchRequestMessage
    BatchResponseMessages() <-chan *BatchResponseMessage
    HeaderMessages() <-chan *HeaderMessage
    VoteMessages() <-chan *VoteMessage
    CertificateMessages() <-chan *CertificateMessage
    SyncRequests() <-chan *SyncRequest
    SyncResponses() <-chan *SyncResponseMessage

    ValidatorID() uint16
    Start() error
    Stop() error
}
```

### Glueberry Integration Example

Here's how to wrap glueberry (P2P networking) for Looseberry:

```go
package network

import (
    "encoding/binary"
    "fmt"
    "sync"

    "github.com/blockberries/glueberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

// GlueberryNetwork wraps glueberry for Looseberry
type GlueberryNetwork struct {
    node        *glueberry.Node
    validatorID uint16
    cfg         network.Config

    // Message channels
    batchCh      chan *network.BatchMessage
    batchAckCh   chan *network.BatchAckMessage
    headerCh     chan *network.HeaderMessage
    voteCh       chan *network.VoteMessage
    certCh       chan *network.CertificateMessage
    syncReqCh    chan *network.SyncRequest
    syncRespCh   chan *network.SyncResponseMessage
    // ... other channels

    stopCh chan struct{}
    wg     sync.WaitGroup
}

func NewGlueberryNetwork(node *glueberry.Node, validatorID uint16, cfg network.Config) *GlueberryNetwork {
    return &GlueberryNetwork{
        node:        node,
        validatorID: validatorID,
        cfg:         cfg,
        batchCh:     make(chan *network.BatchMessage, cfg.BufferSize),
        batchAckCh:  make(chan *network.BatchAckMessage, cfg.BufferSize),
        headerCh:    make(chan *network.HeaderMessage, cfg.BufferSize),
        voteCh:      make(chan *network.VoteMessage, cfg.BufferSize),
        certCh:      make(chan *network.CertificateMessage, cfg.BufferSize),
        syncReqCh:   make(chan *network.SyncRequest, cfg.BufferSize),
        syncRespCh:  make(chan *network.SyncResponseMessage, cfg.BufferSize),
        stopCh:      make(chan struct{}),
    }
}

func (n *GlueberryNetwork) Start() error {
    // Subscribe to topics
    topics := []string{
        "looseberry/batch",
        "looseberry/header",
        "looseberry/certificate",
    }

    for _, topic := range topics {
        if err := n.node.Subscribe(topic); err != nil {
            return fmt.Errorf("subscribe to %s: %w", topic, err)
        }
    }

    // Start message routing goroutine
    n.wg.Add(1)
    go n.routeMessages()

    return nil
}

func (n *GlueberryNetwork) Stop() error {
    close(n.stopCh)
    n.wg.Wait()
    return nil
}

// BroadcastBatch broadcasts a batch to all validators
func (n *GlueberryNetwork) BroadcastBatch(batch *types.Batch) error {
    data, err := batch.Marshal()
    if err != nil {
        return err
    }

    return n.node.Publish("looseberry/batch", data)
}

// SendVote sends a vote to a specific validator
func (n *GlueberryNetwork) SendVote(validator uint16, vote *types.Vote) error {
    data, err := vote.Marshal()
    if err != nil {
        return err
    }

    peerID := n.validatorToPeerID(validator)
    return n.node.Send(peerID, "looseberry/vote", data)
}

// routeMessages routes incoming messages to appropriate channels
func (n *GlueberryNetwork) routeMessages() {
    defer n.wg.Done()

    for {
        select {
        case <-n.stopCh:
            return

        case msg := <-n.node.Messages():
            n.handleMessage(msg)
        }
    }
}

func (n *GlueberryNetwork) handleMessage(msg *glueberry.Message) {
    switch msg.Topic {
    case "looseberry/batch":
        batch := &types.Batch{}
        if err := batch.Unmarshal(msg.Data); err != nil {
            return
        }
        n.batchCh <- &network.BatchMessage{
            Batch: batch,
            From:  n.peerIDToValidator(msg.From),
        }

    case "looseberry/header":
        header := &types.Header{}
        if err := header.Unmarshal(msg.Data); err != nil {
            return
        }
        n.headerCh <- &network.HeaderMessage{
            Header: header,
            From:   n.peerIDToValidator(msg.From),
        }

    case "looseberry/vote":
        vote := &types.Vote{}
        if err := vote.Unmarshal(msg.Data); err != nil {
            return
        }
        n.voteCh <- &network.VoteMessage{
            Vote: vote,
            From: n.peerIDToValidator(msg.From),
        }

    case "looseberry/certificate":
        cert := &types.Certificate{}
        if err := cert.Unmarshal(msg.Data); err != nil {
            return
        }
        n.certCh <- &network.CertificateMessage{
            Certificate: cert,
            From:        n.peerIDToValidator(msg.From),
        }
    }
}

// Channel accessors
func (n *GlueberryNetwork) BatchMessages() <-chan *network.BatchMessage {
    return n.batchCh
}

func (n *GlueberryNetwork) VoteMessages() <-chan *network.VoteMessage {
    return n.voteCh
}

func (n *GlueberryNetwork) ValidatorID() uint16 {
    return n.validatorID
}

// Helper methods for peer ID <-> validator ID mapping
func (n *GlueberryNetwork) validatorToPeerID(validator uint16) string {
    // Implementation depends on your node ID scheme
    return fmt.Sprintf("validator-%d", validator)
}

func (n *GlueberryNetwork) peerIDToValidator(peerID string) uint16 {
    // Implementation depends on your node ID scheme
    var id uint16
    fmt.Sscanf(peerID, "validator-%d", &id)
    return id
}
```

### Using the Network Adapter

```go
// Create glueberry node
glueNode := glueberry.NewNode(config)

// Wrap for Looseberry
network := NewGlueberryNetwork(glueNode, validatorIndex, network.DefaultConfig())

// Set in Looseberry
lb.SetNetwork(network)
```

## Storage Integration

### Storage Interfaces

Looseberry requires three storage interfaces:

```go
type BatchStore interface {
    SaveBatch(batch *types.Batch) error
    GetBatch(digest types.Hash) (*types.Batch, error)
    HasBatch(digest types.Hash) bool
    GetBatchesByRound(round uint64) ([]*types.Batch, error)
    DeleteBatchesBefore(round uint64) error
    Close() error
}

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

type TxIndex interface {
    AddTx(txHash, batchHash types.Hash) error
    GetBatchForTx(txHash types.Hash) (types.Hash, error)
    HasTx(txHash types.Hash) bool
    RemoveTxsForBatch(batchHash types.Hash) error
    AddBatch(batch *types.Batch) error
    Close() error
}
```

### Using Built-in Storage

Looseberry provides two storage implementations:

**In-Memory (Testing)**:
```go
batchStore := store.NewMemoryBatchStore()
certStore := store.NewMemoryCertificateStore()
txIndex := store.NewMemoryTxIndex()

lb.SetStores(batchStore, certStore, txIndex)
```

**LevelDB (Production)**:
```go
batchStore, err := store.NewLevelDBBatchStore("/var/lib/looseberry/batches")
if err != nil {
    panic(err)
}

certStore, err := store.NewLevelDBCertificateStore("/var/lib/looseberry/certs")
if err != nil {
    panic(err)
}

txIndex := store.NewMemoryTxIndex() // TxIndex is always in-memory

lb.SetStores(batchStore, certStore, txIndex)
```

### Automatic Storage Initialization

If you don't set stores, Looseberry will initialize them automatically based on configuration:

```go
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry"

lb, _ := looseberry.New(cfg)
lb.Start() // Automatically initializes LevelDB stores
```

## Complete Integration Example

Here's a complete example integrating Looseberry with consensus and networking:

```go
package main

import (
    "fmt"
    "log"

    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

// MyConsensus represents your consensus system
type MyConsensus struct {
    mempool      looseberry.DAGMempool
    network      network.Network
    validatorSet types.ValidatorSet
    height       uint64
}

func NewMyConsensus(validatorIndex uint16, signer types.Signer, validators []*types.Validator) (*MyConsensus, error) {
    // Create validator set
    validatorSet := types.NewSimpleValidatorSet(validators, 0)

    // Configure Looseberry
    cfg := looseberry.DefaultConfig()
    cfg.ValidatorIndex = validatorIndex
    cfg.Signer = signer
    cfg.Storage.InMemory = false
    cfg.Storage.DataDir = "/var/lib/mychain/looseberry"

    // Create Looseberry
    lb, err := looseberry.New(cfg)
    if err != nil {
        return nil, err
    }

    // Create network (using mock for example)
    net := network.NewMockNetwork(validatorIndex, network.DefaultConfig())

    // Set dependencies
    lb.SetValidatorSet(validatorSet)
    lb.SetNetwork(net)

    // Start Looseberry
    if err := lb.Start(); err != nil {
        return nil, err
    }

    return &MyConsensus{
        mempool:      lb,
        network:      net,
        validatorSet: validatorSet,
    }, nil
}

// SubmitTransaction handles client transaction submission
func (c *MyConsensus) SubmitTransaction(tx []byte) error {
    if err := c.mempool.AddTx(tx); err != nil {
        if types.IsRetryable(err) {
            return fmt.Errorf("mempool busy, please retry: %w", err)
        }
        return fmt.Errorf("invalid transaction: %w", err)
    }
    log.Printf("Transaction accepted, mempool size: %d", c.mempool.Size())
    return nil
}

// ProposeBlock creates a new block proposal
func (c *MyConsensus) ProposeBlock() [][]byte {
    // Reap certified batches
    maxBytes := int64(1024 * 1024) // 1MB
    certifiedBatches := c.mempool.ReapCertifiedBatches(maxBytes)

    var transactions [][]byte
    for _, cb := range certifiedBatches {
        for _, tx := range cb.Batch.Transactions {
            transactions = append(transactions, tx)
        }
    }

    log.Printf("Proposed block with %d transactions", len(transactions))
    return transactions
}

// CommitBlock commits a finalized block
func (c *MyConsensus) CommitBlock(height uint64, transactions [][]byte) error {
    // Apply transactions to state machine
    for _, tx := range transactions {
        log.Printf("Applying transaction: %s", string(tx))
        // Apply to state machine...
    }

    // Notify mempool
    c.mempool.NotifyCommitted(height)
    c.height = height

    log.Printf("Block %d committed, mempool size: %d", height, c.mempool.Size())
    return nil
}

func main() {
    // Generate validator keys
    signer, _ := types.GenerateEd25519Signer(0)

    validators := []*types.Validator{
        {Index: 0, PublicKey: signer.PublicKey()},
    }

    // Create consensus
    consensus, err := NewMyConsensus(0, signer, validators)
    if err != nil {
        panic(err)
    }

    // Submit transactions
    consensus.SubmitTransaction([]byte("tx1"))
    consensus.SubmitTransaction([]byte("tx2"))

    // Propose and commit block
    txs := consensus.ProposeBlock()
    consensus.CommitBlock(1, txs)
}
```

## Best Practices

### Transaction Validation

Always validate transactions before accepting them:

```go
cfg.TxValidator = func(tx []byte) error {
    if len(tx) == 0 {
        return fmt.Errorf("empty transaction")
    }
    if len(tx) > 1024*1024 {
        return fmt.Errorf("transaction too large")
    }
    // Custom validation...
    return nil
}
```

### Error Handling

Handle retryable errors appropriately:

```go
func submitWithRetry(mempool looseberry.DAGMempool, tx []byte, maxRetries int) error {
    for i := 0; i < maxRetries; i++ {
        err := mempool.AddTx(tx)
        if err == nil {
            return nil
        }
        if !types.IsRetryable(err) {
            return err
        }
        time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
    }
    return fmt.Errorf("max retries exceeded")
}
```

### Monitoring

Monitor mempool health:

```go
func monitorMempool(mempool looseberry.DAGMempool) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        metrics := mempool.Metrics()
        log.Printf("Mempool: pending=%d, round=%d, workers=%d, load=%.2f",
            metrics.PendingTxCount,
            metrics.CurrentRound,
            metrics.WorkerCount,
            metrics.WorkerLoad)

        if metrics.IsPaused {
            log.Printf("WARNING: Mempool paused, uncommitted gap: %d",
                metrics.UncommittedGap)
        }
    }
}
```

### Graceful Shutdown

Always stop Looseberry gracefully:

```go
func (c *MyConsensus) Shutdown() error {
    log.Println("Shutting down consensus...")

    // Stop accepting new transactions
    // ...

    // Stop Looseberry
    if lb, ok := c.mempool.(*looseberry.Looseberry); ok {
        if err := lb.Stop(); err != nil {
            log.Printf("Error stopping Looseberry: %v", err)
        }
    }

    return nil
}
```

### Validator Set Updates

Handle epoch transitions correctly:

```go
func (c *MyConsensus) UpdateEpoch(newEpoch uint64, newValidators []*types.Validator) {
    validatorSet := types.NewSimpleValidatorSet(newValidators, newEpoch)
    c.mempool.UpdateValidatorSet(validatorSet)
    c.validatorSet = validatorSet
    log.Printf("Updated to epoch %d with %d validators", newEpoch, len(newValidators))
}
```

## Next Steps

- **[Configuration Guide](CONFIGURATION.md)**: Fine-tune Looseberry configuration
- **[Testing Guide](TESTING.md)**: Test your integration
- **[Deployment Guide](DEPLOYMENT.md)**: Deploy to production
- **[Monitoring Guide](MONITORING.md)**: Set up observability
