# Looseberry Architecture

A comprehensive architectural overview of the Looseberry DAG-based mempool implementation.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Principles](#2-architecture-principles)
3. [System Architecture](#3-system-architecture)
4. [Go Package Architecture](#4-go-package-architecture)
5. [Core Components](#5-core-components)
6. [Concurrency Architecture](#6-concurrency-architecture)
7. [Data Flow](#7-data-flow)
8. [API Design](#8-api-design)
9. [Configuration](#9-configuration)
10. [Testing Strategy](#10-testing-strategy)
11. [Performance Considerations](#11-performance-considerations)
12. [Error Handling](#12-error-handling)
13. [Security Architecture](#13-security-architecture)
14. [Storage Architecture](#14-storage-architecture)
15. [Network Protocol](#15-network-protocol)
16. [Deployment Considerations](#16-deployment-considerations)
17. [Design Decisions](#17-design-decisions)
18. [Future Considerations](#18-future-considerations)

---

## 1. Executive Summary

### 1.1 What is Looseberry?

Looseberry is a production-ready, high-performance mempool implementation for Byzantine fault-tolerant (BFT) consensus systems. It implements a DAG-based architecture inspired by the Narwhal protocol, separating transaction dissemination from transaction ordering to achieve high throughput while maintaining BFT safety guarantees.

### 1.2 Why Looseberry Exists

Traditional mempools in blockchain systems couple transaction dissemination with consensus, creating a bottleneck. Looseberry decouples these concerns:

- **Transaction Dissemination**: Handled by workers and primary nodes creating a DAG of certificates
- **Transaction Ordering**: Delegated to an external consensus layer (e.g., blockberry)

This separation enables:
- Parallel transaction batching across multiple workers
- High throughput independent of consensus speed
- Data availability guarantees through certificate formation
- Resilience to network delays and Byzantine faults

### 1.3 Key Features

| Feature | Description |
|---------|-------------|
| **BFT Safety** | Tolerates up to f Byzantine validators (n = 3f + 1) |
| **High Throughput** | 200,000+ transactions/second sustained throughput |
| **Dynamic Scaling** | Auto-scales workers (1-8) based on load |
| **Flow Control** | Prevents unbounded DAG growth with backpressure |
| **Garbage Collection** | Automatic pruning with transaction recovery |
| **Pluggable Storage** | In-memory for testing, LevelDB for production |
| **Certificate-Based DA** | Certificates prove data availability to 2f+1 validators |

### 1.4 System Requirements

- **Go Version**: 1.21 or later
- **Memory**: ~100MB base + ~50MB per 10K pending transactions
- **Storage**: LevelDB for persistent certificate/batch storage
- **Network**: TCP/IP with multiaddr support (via glueberry integration)
- **Validators**: Minimum 4 validators (3f+1 where f=1)

---

## 2. Architecture Principles

### 2.1 Design Philosophy

Looseberry follows these core principles:

1. **Separation of Concerns**: Transaction dissemination is independent of ordering
2. **BFT First**: All operations assume up to f Byzantine validators
3. **Performance**: Lock-free operations where possible, batching, caching
4. **Reliability**: Graceful degradation, transaction recovery, restart capability
5. **Observability**: Comprehensive metrics and structured logging
6. **Testability**: Interface-driven design with mock implementations

### 2.2 Go Idioms Used

| Idiom | Usage | Benefit |
|-------|-------|---------|
| **Interfaces** | Small, focused interfaces | Easy mocking and testing |
| **Atomics** | Lock-free counters and flags | High-performance concurrency |
| **Channels** | Message passing and cancellation | Safe goroutine communication |
| **Mutexes** | Protecting complex state | Thread-safe data structures |
| **Defer** | Resource cleanup | Guaranteed cleanup on exit |
| **Error Wrapping** | Context preservation | Better debugging |
| **Value Semantics** | Fixed-size arrays for hashes | Safe concurrent access |

### 2.3 Concurrency Model

```
Single-threaded components:
  - None (all components are concurrent)

Multi-threaded components:
  - Worker Pool: 1-8 worker goroutines
  - Primary: 1 header creation goroutine
  - GC Manager: 1 GC goroutine
  - Sync Manager: 1 sync + 1 message handler goroutine
  - Looseberry: 1 message processing goroutine

Synchronization:
  - Atomic operations for simple state (rounds, counters)
  - RWMutex for read-heavy data (validator sets)
  - Mutex for complex mutable state (pending votes)
  - Channels for message passing and cancellation
```

---

## 3. System Architecture

### 3.1 High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Looseberry                              │
│                     (Main Coordinator)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Worker Pool                           │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐              │  │
│  │  │ Worker 0 │  │ Worker 1 │  │ Worker N │  (1-8 total) │  │
│  │  │ Batching │  │ Batching │  │ Batching │              │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘              │  │
│  │       │             │             │                     │  │
│  │       └─────────────┼─────────────┘                     │  │
│  │                     │                                   │  │
│  │              ┌──────▼──────┐                            │  │
│  │              │   Scaler    │  (Auto-scaling)            │  │
│  │              └─────────────┘                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                       │                                         │
│                       │ Batches                                 │
│                       ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      Primary                             │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │  │    Header    │  │     Vote     │  │ Certificate  │   │  │
│  │  │   Creation   │  │   Tracking   │  │  Formation   │   │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                       │                                         │
│                       │ Certificates                            │
│                       ▼                                         │
│  ┌────────────────────────────────────────┐                    │
│  │                  DAG                   │                    │
│  │  ┌────────┐  ┌────────┐  ┌────────┐   │                    │
│  │  │Round 0 │→ │Round 1 │→ │Round N │   │                    │
│  │  └────────┘  └────────┘  └────────┘   │                    │
│  └────────────────────────────────────────┘                    │
│                       │                                         │
│         ┌─────────────┼─────────────┐                          │
│         │             │             │                          │
│         ▼             ▼             ▼                          │
│  ┌───────────┐ ┌───────────┐ ┌──────────┐                     │
│  │  Network  │ │    GC     │ │   Flow   │                     │
│  │  (P2P +   │ │ (Cleanup) │ │ Control  │                     │
│  │   Sync)   │ │           │ │          │                     │
│  └───────────┘ └───────────┘ └──────────┘                     │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Storage Layer                         │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │  │  Batch Store │  │  Cert Store  │  │   Tx Index   │   │  │
│  │  │ (LevelDB/Mem)│  │ (LevelDB/Mem)│  │  (Memory)    │   │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Request Lifecycle

```
1. Transaction Submission
   ├─> Client calls AddTx(tx)
   ├─> Flow control check (paused?)
   ├─> TxValidator check (optional)
   └─> Route to worker by hash

2. Worker Batching
   ├─> Worker adds to pending queue
   ├─> Deduplication check (pending + txIndex)
   ├─> Backpressure check (max pending)
   ├─> Trigger batch creation (size/timeout)
   └─> Batch stored + broadcast

3. Header Creation (Primary)
   ├─> Collect batch digests
   ├─> Select parent certificates (2f+1 from round-1)
   ├─> Create and sign header
   └─> Broadcast header

4. Voting (All Validators)
   ├─> Receive header
   ├─> Verify signature + parents
   ├─> Create and send vote to author
   └─> Author collects votes

5. Certificate Formation (Primary)
   ├─> Collect 2f+1 votes
   ├─> Form certificate
   ├─> Add to DAG
   └─> Broadcast certificate

6. Consensus Integration
   ├─> Consensus orders certificates
   ├─> Consensus calls ReapCertifiedBatches()
   ├─> Transactions extracted for block
   └─> Consensus calls NotifyCommitted(round)

7. Garbage Collection
   ├─> GC triggered by NotifyCommitted
   ├─> Extract uncommitted transactions
   ├─> Re-inject recovered transactions
   ├─> Prune old rounds from DAG + storage
   └─> Update flow control state
```

---

## 4. Go Package Architecture

### 4.1 Package Organization

```
github.com/blockberries/looseberry/
├── looseberry.go          # Main DAGMempool implementation
├── config.go              # Configuration structures
├── types/                 # Core types and primitives
│   ├── hash.go           # 32-byte SHA-256 hash
│   ├── transaction.go    # Transaction wrapper
│   ├── batch.go          # Transaction batch
│   ├── header.go         # DAG vertex (header)
│   ├── vote.go           # Validator vote
│   ├── certificate.go    # Certified header (2f+1 votes)
│   ├── validator.go      # Validator set interface
│   ├── signer.go         # Ed25519 signing
│   └── errors.go         # Error definitions
├── worker/               # Transaction batching
│   ├── worker.go         # Single worker
│   ├── pool.go           # Worker pool manager
│   ├── scaler.go         # Auto-scaling logic
│   └── ack_tracker.go    # Batch acknowledgment tracking
├── primary/              # Header/vote/certificate logic
│   ├── primary.go        # Main primary logic
│   ├── vote_tracker.go   # Vote collection
│   └── batch_fetcher.go  # Missing batch resolution
├── dag/                  # Certificate DAG
│   └── dag.go            # DAG data structure + traversal
├── gc/                   # Garbage collection
│   ├── gc.go             # GC manager
│   └── flow.go           # Flow control
├── store/                # Storage interfaces + implementations
│   ├── store.go          # Interface definitions
│   ├── memory_batch.go   # In-memory batch store
│   ├── memory_cert.go    # In-memory cert store
│   ├── memory_txindex.go # In-memory tx index
│   ├── leveldb_batch.go  # LevelDB batch store
│   └── leveldb_cert.go   # LevelDB cert store
├── network/              # Network abstraction
│   ├── network.go        # Network interface + messages
│   ├── mock.go           # Mock network for testing
│   └── sync.go           # Sync manager
└── test/                 # Integration + system tests
    ├── integration_test.go
    ├── benchmark_test.go
    ├── stress_test.go
    └── byzantine_test.go
```

### 4.2 Dependency Graph

```
looseberry (main)
  ├─> types (no dependencies)
  ├─> config (types)
  ├─> worker (types, store)
  ├─> primary (types, store)
  ├─> dag (types, store)
  ├─> gc (types, dag, store)
  ├─> network (types)
  └─> store (types)

Dependency Rules:
  - types: No internal dependencies (foundation)
  - store: Only depends on types
  - worker/primary/dag: Depend on types + store
  - gc: Depends on dag (higher level)
  - network: Interface only, no storage dependencies
  - looseberry: Orchestrates all components
```

### 4.3 Interface Boundaries

| Package | Public Interfaces | Purpose |
|---------|-------------------|---------|
| **looseberry** | DAGMempool | Main API for consensus integration |
| **types** | Signer, ValidatorSet | Cryptography and validator management |
| **store** | BatchStore, CertificateStore, TxIndex | Storage abstraction |
| **network** | Network | Network communication abstraction |
| **worker** | (internal) | Not exposed, used via DAGMempool |
| **primary** | (internal) | Not exposed, orchestrated by looseberry |
| **dag** | (internal) | Not exposed, managed by looseberry |
| **gc** | (internal) | Not exposed, triggered by NotifyCommitted |

---

## 5. Core Components

### 5.1 Types Package

**Purpose**: Foundation types for the entire system.

**Key Types**:
- `Hash [32]byte` - SHA-256 digest (value type for thread-safety)
- `Transaction []byte` - Opaque transaction bytes
- `Batch` - Collection of transactions from a worker
- `Header` - DAG vertex (references batches + parent certificates)
- `Vote` - Validator's vote on a header
- `Certificate` - Header + 2f+1 votes (certified DAG vertex)
- `Validator` - Validator metadata (index, public key)
- `ValidatorSet` - Interface for validator set management

**Concurrency**: All types are designed for safe concurrent access:
- Value types (Hash, Signature) use fixed-size arrays
- Reference types provide Clone() methods
- Digest computation is deterministic and idempotent

**Example**:
```go
// Create a batch
batch := types.NewBatch(workerID, validatorID, round, transactions)

// Compute digest (deterministic)
digest := batch.ComputeDigest()

// Create header
header := types.NewHeader(author, round, epoch, batchRefs, parents)
header.Sign(signer)

// Create certificate
votes := []types.Vote{vote1, vote2, vote3} // 2f+1 votes
cert := types.NewCertificate(header, votes)

// Verify certificate
if err := cert.Verify(validatorSet); err != nil {
    // Handle Byzantine behavior
}
```

### 5.2 Worker Package

**Purpose**: Parallel transaction batching with deduplication and backpressure.

**Architecture**:
```
WorkerPool
  ├─> Workers (1-8 instances)
  │     ├─> Pending queue (mutex-protected)
  │     ├─> Pending set (deduplication)
  │     ├─> Batch loop (timer + trigger channel)
  │     └─> AckTracker (quorum tracking)
  └─> Scaler
        ├─> Load calculation
        ├─> Scale up/down decisions
        └─> Cooldown management
```

**Hash-Based Routing**:
```go
// Route transaction to worker by hash (uses 8 bytes for distribution)
txHash := tx.Hash()
hashValue := binary.BigEndian.Uint64(txHash[:8])
workerIdx := int(hashValue % uint64(len(workers)))
worker := workers[workerIdx]
```

**Batch Creation Triggers**:
1. **Size-based**: `len(pending) >= batchSize`
2. **Byte-based**: `pendingBytes >= maxBatchBytes`
3. **Time-based**: `elapsed >= batchTimeout AND len(pending) > 0`

**Backpressure**:
- Per-worker limits on pending transactions and bytes
- Returns `ErrWorkerBackpressure` when limit reached
- Backpressure is retryable (transient overload)

**Auto-Scaling**:
```go
// Load calculation (uses max of count-based and byte-based)
countLoad := pendingCount / (workerCount * batchSize)
byteLoad := pendingBytes / (workerCount * maxPendingBytes)
load := max(countLoad, byteLoad)

// Scaling decisions
if load > scaleUpThreshold && workerCount < maxWorkers:
    scaleUp()
if load < scaleDownThreshold && workerCount > minWorkers:
    scaleDown()
```

**Acknowledgment Tracking**:
- Workers track batch acknowledgments from other validators
- Requires 2f+1 acknowledgments for quorum
- Batches without quorum within timeout are retransmitted

### 5.3 Primary Package

**Purpose**: Creates headers, collects votes, forms certificates.

**Architecture**:
```
Primary
  ├─> Header Creation Loop
  │     ├─> Collect batch digests
  │     ├─> Select parents (2f+1 from round-1)
  │     ├─> Create and sign header
  │     └─> Broadcast header
  ├─> Vote Tracker
  │     ├─> Pending headers (waiting for votes)
  │     ├─> Vote collection (per header)
  │     ├─> Double-vote detection
  │     └─> Certificate formation
  └─> Batch Fetcher
        ├─> Missing batch detection
        ├─> Batch request/response
        └─> Dependency resolution
```

**Header Creation**:
```go
// Periodic header creation (every HeaderTimeout)
func tryCreateHeader() {
    // Collect batch digests
    batchRefs := collectBatchDigests(maxBatchesPerHeader)

    // Allow empty headers for liveness (if configured)
    if len(batchRefs) == 0 && !allowEmptyHeaders {
        return
    }

    // Select 2f+1 parent certificates from round-1
    parents := selectParents(currentRound - 1, quorum)

    // Don't create empty headers without valid parents (except round 0)
    if len(batchRefs) == 0 && len(parents) == 0 && currentRound > 0 {
        return
    }

    // Create and sign header
    header := types.NewHeader(validatorID, currentRound, epoch, batchRefs, parents)
    header.Sign(signer)

    // Broadcast
    broadcast(header)
}
```

**Parent Selection**:
```go
// Deterministic parent selection ensures all honest validators
// create headers with the same parent structure
func selectParents(round uint64) []types.CertificateRef {
    // Get certificates from previous round
    certs := certStore.GetCertificatesByRound(round)

    // Sort by validator index (deterministic)
    sort.Slice(certs, func(i, j int) bool {
        return certs[i].Author() < certs[j].Author()
    })

    // Take first 2f+1 (quorum)
    if len(certs) > quorum {
        certs = certs[:quorum]
    }

    return convertToRefs(certs)
}
```

**Vote Collection**:
```go
// Vote tracker collects votes and forms certificates
func HandleVote(vote *types.Vote) (*types.Certificate, bool) {
    // Verify signature
    if !vote.Verify(validatorSet) {
        return nil, false // Byzantine
    }

    // Check for double voting
    if existingVote := voteTracker.GetVote(vote.Validator, vote.HeaderDigest) {
        if !existingVote.Signature.Equal(vote.Signature) {
            // Byzantine: validator voted twice with different signatures
            recordDoubleVote(vote.Validator, existingVote, vote)
            return nil, false
        }
        return nil, false // Already counted
    }

    // Add vote
    voteTracker.AddVote(vote)

    // Check quorum
    if voteTracker.VoteCount(vote.HeaderDigest) >= quorum {
        return voteTracker.FormCertificate(vote.HeaderDigest), true
    }

    return nil, false
}
```

**Pending Vote Buffering**:
- Votes may arrive before their header
- Pending votes are buffered with timestamps
- Periodic cleanup removes votes older than 2 * VoteTimeout
- Prevents memory leaks from missing/invalid headers

### 5.4 DAG Package

**Purpose**: Maintains certificate DAG with causal ordering and efficient traversal.

**Data Structure**:
```go
type DAG struct {
    // Round-based indexing
    rounds map[uint64]*RoundData  // Certificates organized by round

    // Hash-based indexing (O(1) lookups)
    certIndex map[types.Hash]*types.Certificate

    // Atomic state
    highestRound   atomic.Uint64
    committedRound atomic.Uint64

    // LRU cache for causal history
    historyCache     map[types.Hash][]*types.Certificate
    historyCacheKeys []types.Hash  // For LRU eviction

    // Separate locks for independent state
    roundsMu       sync.RWMutex
    certIndexMu    sync.RWMutex
    historyCacheMu sync.RWMutex
}
```

**Certificate Addition**:
```go
func (d *DAG) AddCertificate(cert *types.Certificate) error {
    // Validate parent certificates exist
    for _, parentRef := range cert.Header.Parents {
        if !d.HasCertificate(parentRef.Digest) {
            return ErrMissingParents
        }
    }

    // Check for duplicate
    if d.HasCertificateForValidator(cert.Round(), cert.Author()) {
        return ErrDuplicateHeader
    }

    // Add to round data
    d.rounds[cert.Round()].AddCertificate(cert)

    // Add to hash index
    d.certIndex[cert.Digest()] = cert

    // Update highest round
    if cert.Round() > d.highestRound.Load() {
        d.highestRound.Store(cert.Round())
    }

    // Invalidate affected cache entries
    d.invalidateHistoryCacheForCertificate(cert)

    // Persist
    d.certStore.SaveCertificate(cert)

    return nil
}
```

**Causal History Traversal**:
```go
// BFS traversal with bounded depth and LRU caching
func (d *DAG) CausalHistory(cert *types.Certificate) []*types.Certificate {
    // Check cache
    if cached, ok := d.historyCache[cert.Digest()]; ok {
        return cached
    }

    // BFS traversal
    visited := make(map[types.Hash]bool)
    queue := []*types.Certificate{cert}
    result := []*types.Certificate{}
    depth := 0

    for len(queue) > 0 && depth < maxHistoryDepth {
        levelSize := len(queue)
        for i := 0; i < levelSize; i++ {
            current := queue[0]
            queue = queue[1:]
            result = append(result, current)

            // Add parents to queue
            for _, parentRef := range current.Header.Parents {
                if !visited[parentRef.Digest] {
                    visited[parentRef.Digest] = true
                    parent := d.GetCertificate(parentRef.Digest)
                    if parent != nil {
                        queue = append(queue, parent)
                    }
                }
            }
        }
        depth++
    }

    // Cache result with LRU eviction
    d.addToCacheLRU(cert.Digest(), result)

    return result
}
```

**Smart Cache Invalidation**:
```go
// Only invalidate entries that could be affected by new certificate
func (d *DAG) invalidateHistoryCacheForCertificate(newCert *types.Certificate) {
    newRound := newCert.Round()

    // Only invalidate entries for certificates at HIGHER rounds
    // (causal history only goes backwards in the DAG)
    for digest, history := range d.historyCache {
        if len(history) > 0 && history[0].Round() > newRound {
            delete(d.historyCache, digest)
        }
    }
}
```

**Ordered Certificate Retrieval**:
```go
// Returns certificates in deterministic order for consensus
func (d *DAG) GetOrderedCertificates(fromRound, toRound uint64) []*types.Certificate {
    var result []*types.Certificate

    for round := fromRound; round <= toRound; round++ {
        certs := d.GetCertificatesForRound(round)

        // Sort by validator index for determinism
        sort.Slice(certs, func(i, j int) bool {
            return certs[i].Author() < certs[j].Author()
        })

        result = append(result, certs...)
    }

    return result
}
```

### 5.5 GC Package

**Purpose**: Garbage collection and flow control to prevent unbounded resource growth.

**GC Manager**:
```go
type GCManager struct {
    // Committed round tracking
    committedRound atomic.Uint64
    lastGCRound    atomic.Uint64

    // Uncommitted batch index (for fast extraction)
    uncommittedBatches map[types.Hash]uint64  // batch digest -> round

    // Metrics
    totalGCRuns      atomic.Uint64
    totalTxRecovered atomic.Uint64

    // Lifecycle
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
}
```

**GC Process**:
```go
func (gc *GCManager) performGC(beforeRound uint64) error {
    // 1. Recover uncommitted transactions (if enabled)
    if gc.cfg.RecoverUncommittedTxs {
        txs := gc.extractUncommittedTxs(beforeRound)
        if len(txs) > 0 {
            gc.txRecoveryCallback(txs)  // Re-inject into mempool
            gc.totalTxRecovered.Add(uint64(len(txs)))
        }
    }

    // 2. Prune from DAG memory
    gc.dag.PruneRoundsBefore(beforeRound)

    // 3. Delete from persistent storage
    gc.batchStore.DeleteBatchesBefore(beforeRound)
    gc.certStore.DeleteCertificatesBefore(beforeRound)

    // 4. Prune transaction index
    gc.txIndex.PruneOlderThan(beforeRound, gc.batchStore)

    // 5. Update tracking
    gc.lastGCRound.Store(beforeRound - 1)
    gc.totalGCRuns.Add(1)

    return nil
}
```

**Transaction Recovery**:
```go
// Extract transactions from uncommitted batches before pruning
func (gc *GCManager) extractUncommittedTxs(beforeRound uint64) []types.Transaction {
    // Use uncommitted batch index for O(n) extraction
    var uncommittedTxs []types.Transaction

    for digest, round := range gc.uncommittedBatches {
        if round < beforeRound {
            batch := gc.batchStore.GetBatch(digest)
            if batch != nil {
                uncommittedTxs = append(uncommittedTxs, batch.Transactions...)
            }
            delete(gc.uncommittedBatches, digest)
        }
    }

    return uncommittedTxs
}
```

**Flow Controller**:
```go
type FlowController struct {
    // All state is atomic (lock-free)
    currentRound   atomic.Uint64
    committedRound atomic.Uint64
    pendingBatches atomic.Int64
    pendingHeaders atomic.Int64
    paused         atomic.Bool

    // Metrics
    pauseCount  atomic.Uint64
    resumeCount atomic.Uint64
}

// Check flow control conditions
func (fc *FlowController) checkFlowControl() {
    shouldPause := false

    // Check uncommitted round gap
    if fc.UncommittedGap() >= fc.cfg.MaxUncommittedRounds {
        shouldPause = true
    }

    // Check pending batches
    if fc.pendingBatches.Load() >= int64(fc.cfg.MaxPendingBatches) {
        shouldPause = true
    }

    // Update state with atomic swap
    if shouldPause {
        if !fc.paused.Swap(true) {  // Was not paused, now paused
            fc.pauseCount.Add(1)
            if fc.onPause != nil {
                fc.onPause()
            }
        }
    } else {
        if fc.paused.Swap(false) {  // Was paused, now resumed
            fc.resumeCount.Add(1)
            if fc.onResume != nil {
                fc.onResume()
            }
        }
    }
}
```

### 5.6 Store Package

**Purpose**: Pluggable storage abstraction with in-memory and persistent implementations.

**Interfaces**:
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
    AddBatch(batch *types.Batch) error
    GetBatchForTx(txHash types.Hash) (types.Hash, error)
    HasTx(txHash types.Hash) bool
    RemoveTxsForBatch(batchHash types.Hash) error
    PruneOlderThan(round uint64, batchStore BatchStore) (int, error)
    Close() error
}
```

**LevelDB Implementation**:
```
Key encoding:
  Batches:       "b:" + digest.Bytes()
  Certificates:  "c:" + digest.Bytes()
  Round index:   "r:" + round_bytes + validator_bytes
  Metadata:      "meta:highest_round"

Benefits:
  - Persistent across restarts
  - Efficient range queries by round
  - Automatic compression
  - Crash recovery
```

**Memory Implementation**:
```go
// Simple map-based implementation for testing
type MemoryBatchStore struct {
    batches map[types.Hash]*types.Batch
    rounds  map[uint64][]*types.Batch
    mu      sync.RWMutex
}

// Thread-safe operations with RWMutex
func (s *MemoryBatchStore) GetBatch(digest types.Hash) (*types.Batch, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    batch, ok := s.batches[digest]
    if !ok {
        return nil, nil
    }

    return batch.Clone(), nil  // Clone for data isolation
}
```

### 5.7 Network Package

**Purpose**: Network abstraction for P2P communication and synchronization.

**Network Interface**:
```go
type Network interface {
    // Broadcast methods
    BroadcastBatch(batch *types.Batch) error
    BroadcastHeader(header *types.Header) error
    BroadcastCertificate(cert *types.Certificate) error

    // Point-to-point methods
    SendVote(validator uint16, vote *types.Vote) error
    SendBatchAck(validator uint16, ack *BatchAckMessage) error
    SendSyncRequest(validator uint16, req *SyncRequest) error
    SendSyncResponse(validator uint16, resp *SyncResponse) error

    // Receive channels
    BatchMessages() <-chan *BatchMessage
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

**Sync Manager**:
```go
type SyncManager struct {
    dag          *dag.DAG
    batchStore   store.BatchStore
    network      Network
    validatorSet types.ValidatorSet

    // Pending sync requests (tracked for timeout)
    pendingRequests map[uint64]*pendingSyncRequest

    // Lifecycle
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
    wg        sync.WaitGroup  // Tracks both syncLoop and handleMessages
}

// Periodic sync check
func (sm *SyncManager) checkAndSync() {
    highestRound := sm.dag.HighestRound()

    // Check for gaps in rounds
    for round := uint64(0); round <= highestRound; round++ {
        certs := sm.dag.GetCertificatesForRound(round)
        if len(certs) < sm.validatorSet.Quorum() {
            // Missing certificates, trigger sync
            sm.requestSync(round, highestRound)
            return
        }
    }
}

// Handle sync response (with validation)
func (sm *SyncManager) HandleSyncResponse(resp *SyncResponse, from uint16) error {
    // Verify all certificates before storing
    for _, cert := range resp.Certificates {
        if err := cert.Verify(sm.validatorSet); err != nil {
            return err  // Reject entire response if any cert is invalid
        }
    }

    // Store batches
    for _, batch := range resp.Batches {
        sm.batchStore.SaveBatch(batch)
    }

    // Add certificates to DAG
    for _, cert := range resp.Certificates {
        sm.dag.AddCertificate(cert)
    }

    return nil
}
```

---

## 6. Concurrency Architecture

### 6.1 Goroutine Patterns

**Standard Lifecycle Pattern**:
```go
type Component struct {
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
}

func (c *Component) Start() error {
    if c.running.Swap(true) {
        return ErrAlreadyRunning
    }

    // Reset channels for restart capability
    c.stopCh = make(chan struct{})
    c.stoppedCh = make(chan struct{})

    go c.mainLoop()
    return nil
}

func (c *Component) mainLoop() {
    defer close(c.stoppedCh)  // Signal completion

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            c.doWork()
        case <-c.stopCh:
            return  // Clean exit
        }
    }
}

func (c *Component) Stop() error {
    if !c.running.Swap(false) {
        return ErrNotRunning
    }

    close(c.stopCh)     // Signal goroutine to stop
    <-c.stoppedCh       // Wait for completion
    return nil
}
```

### 6.2 Synchronization Primitives

**Lock Granularity**:
```go
// Worker: Separate locks for independent state
type Worker struct {
    round atomic.Uint64        // Lock-free
    epoch atomic.Uint64        // Lock-free

    pending      []Transaction  // Mutex-protected
    pendingSet   map[Hash]bool // Mutex-protected
    pendingBytes int64         // Mutex-protected
    pendingMu    sync.Mutex    // Single mutex for all pending state

    ackTracker *AckTracker     // Has its own locks
}

// DAG: Separate locks for different indices
type DAG struct {
    rounds      map[uint64]*RoundData  // RWMutex
    roundsMu    sync.RWMutex

    certIndex   map[Hash]*Certificate  // Separate RWMutex
    certIndexMu sync.RWMutex

    historyCache     map[Hash][]*Certificate  // Separate RWMutex
    historyCacheMu   sync.RWMutex

    highestRound   atomic.Uint64  // Lock-free
    committedRound atomic.Uint64  // Lock-free
}
```

**Lock Ordering** (to prevent deadlocks):
1. Atomic operations (no locks)
2. RWMutex for reads
3. Mutex for writes
4. Never hold multiple locks simultaneously (except for separate indices)

### 6.3 Channel Usage

**Buffered Channels for Non-Blocking Signals**:
```go
// Trigger channel (buffered to prevent blocking)
triggerCh := make(chan struct{}, 1)

// Non-blocking send
select {
case triggerCh <- struct{}{}:
    // Signal sent
default:
    // Channel already has signal, skip
}
```

**Unbuffered Channels for Synchronization**:
```go
stopCh := make(chan struct{})    // Unbuffered
stoppedCh := make(chan struct{}) // Unbuffered

close(stopCh)      // Signal stop
<-stoppedCh        // Wait for completion
```

**Channel Multiplexing**:
```go
for {
    select {
    case msg := <-network.BatchMessages():
        handleBatch(msg)
    case msg := <-network.HeaderMessages():
        handleHeader(msg)
    case msg := <-network.VoteMessages():
        handleVote(msg)
    case <-stopCh:
        return
    }
}
```

### 6.4 Data Isolation

**Clone at Boundaries**:
```go
// Clone on entry to prevent external modification
func (w *Worker) AddTx(tx Transaction) error {
    w.pendingMu.Lock()
    defer w.pendingMu.Unlock()

    w.pending = append(w.pending, tx.Clone())
    // ...
}

// Clone on exit to prevent external modification
func (d *DAG) GetCertificatesForRound(round uint64) []*Certificate {
    d.roundsMu.RLock()
    rd := d.rounds[round]
    d.roundsMu.RUnlock()

    if rd == nil {
        return nil
    }

    certs := rd.GetAllCertificates()
    cloned := make([]*Certificate, len(certs))
    for i, cert := range certs {
        cloned[i] = cert.Clone()  // Clone for safety
    }
    return cloned
}
```

### 6.5 Race Condition Prevention

**Atomic Read-Modify-Write**:
```go
// Safe state transition
if !running.Swap(false) {  // Atomic swap returns old value
    return ErrNotRunning
}
// Now we know: (1) was running before, (2) is stopped now

// Increment counter
totalTxAdded.Add(1)  // Atomic increment
```

**Mutex Protection Scope**:
```go
func (p *Primary) HandleVote(vote *Vote) (*Certificate, bool) {
    // Validate without lock
    if !vote.Verify(validatorSet) {
        return nil, false
    }

    // Lock only for state modification
    p.voteTracker.mu.Lock()
    defer p.voteTracker.mu.Unlock()

    // Critical section
    p.voteTracker.AddVote(vote)
    if p.voteTracker.VoteCount(vote.HeaderDigest) >= quorum {
        return p.voteTracker.FormCertificate(vote.HeaderDigest), true
    }

    return nil, false
}
```

---

## 7. Data Flow

### 7.1 Complete Transaction Lifecycle

```
┌────────────────────────────────────────────────────────────────┐
│                    Transaction Lifecycle                       │
└────────────────────────────────────────────────────────────────┘

1. SUBMISSION (Client → Looseberry)
   ┌─────────────┐
   │   Client    │
   └──────┬──────┘
          │ AddTx(tx)
          ▼
   ┌─────────────┐
   │ Looseberry  │  1. Check flow control (paused?)
   │  AddTx()    │  2. Validate with TxValidator (optional)
   └──────┬──────┘  3. Route to worker by hash
          │
          ▼

2. BATCHING (Worker)
   ┌─────────────┐
   │   Worker    │  1. Deduplication check
   │   AddTx()   │  2. Backpressure check
   └──────┬──────┘  3. Add to pending queue
          │         4. Trigger batch creation
          │
          ▼ (size/timeout trigger)
   ┌─────────────┐
   │   Worker    │  1. Create batch
   │ CreateBatch │  2. Compute digest
   └──────┬──────┘  3. Store batch
          │         4. Index transactions
          │         5. Broadcast batch
          │
          ├─────────────────────┐
          │                     │
          ▼                     ▼
   ┌─────────────┐       ┌─────────────┐
   │ BatchStore  │       │   Network   │
   │  SaveBatch  │       │  Broadcast  │
   └─────────────┘       └──────┬──────┘
                                │
                                ▼

3. ACKNOWLEDGMENT (Other Validators)
   ┌─────────────────────────────┐
   │  Other Validators (2f+1)    │
   │  1. Receive batch           │
   │  2. Store batch             │
   │  3. Send acknowledgment     │
   └───────────┬─────────────────┘
               │
               ▼
   ┌─────────────┐
   │ AckTracker  │  Track acknowledgments
   │ RecordAck() │  until quorum (2f+1)
   └─────────────┘

4. HEADER CREATION (Primary)
   ┌─────────────┐
   │   Primary   │  1. Collect batch digests
   │ CreateHeader│  2. Select parents (2f+1 from round-1)
   └──────┬──────┘  3. Create and sign header
          │         4. Broadcast header
          │
          ▼
   ┌─────────────┐
   │   Network   │
   │  Broadcast  │
   └──────┬──────┘
          │
          ▼

5. VOTING (Other Validators)
   ┌─────────────────────────────┐
   │  Other Validators (2f+1)    │
   │  1. Receive header          │
   │  2. Verify signature        │
   │  3. Verify parents exist    │
   │  4. Create and send vote    │
   └───────────┬─────────────────┘
               │
               ▼
   ┌─────────────┐
   │ VoteTracker │  1. Collect votes
   │ RecordVote()│  2. Detect double-voting
   └──────┬──────┘  3. Form certificate (2f+1)
          │
          ▼

6. CERTIFICATE FORMATION (Primary)
   ┌─────────────┐
   │   Primary   │  1. Receive 2f+1 votes
   │   FormCert  │  2. Create certificate
   └──────┬──────┘  3. Add to DAG
          │         4. Broadcast certificate
          │
          ▼
   ┌─────────────┐
   │     DAG     │  1. Validate parents
   │  AddCert()  │  2. Check for duplicates
   └──────┬──────┘  3. Store certificate
          │         4. Update highest round
          │
          ├─────────────────────┐
          │                     │
          ▼                     ▼
   ┌─────────────┐       ┌─────────────┐
   │  CertStore  │       │   Network   │
   │  SaveCert   │       │  Broadcast  │
   └─────────────┘       └─────────────┘

7. CONSENSUS ORDERING (External)
   ┌─────────────────────────────┐
   │  Consensus Layer (external) │
   │  1. Order certificates       │
   │  2. Deterministic ordering   │
   └───────────┬─────────────────┘
               │
               ▼

8. TRANSACTION REAPING (Consensus → Looseberry)
   ┌─────────────┐
   │ Looseberry  │  1. ReapCertifiedBatches(maxBytes)
   │    Reap()   │  2. Get ordered certificates
   └──────┬──────┘  3. Extract batches
          │         4. Return transactions
          │
          ▼
   ┌─────────────┐
   │ Consensus   │  Build block with
   │   (Build)   │  ordered transactions
   └──────┬──────┘
          │
          ▼

9. COMMIT NOTIFICATION (Consensus → Looseberry)
   ┌─────────────┐
   │ Looseberry  │  1. NotifyCommitted(round)
   │  Notify()   │  2. Update DAG committed round
   └──────┬──────┘  3. Update flow control
          │         4. Trigger GC
          │
          ▼

10. GARBAGE COLLECTION (GC Manager)
   ┌─────────────┐
   │ GC Manager  │  1. Extract uncommitted txs
   │  PerformGC  │  2. Re-inject recovered txs
   └──────┬──────┘  3. Prune old rounds
          │         4. Delete from storage
          │         5. Update metrics
          │
          ├───────────────────────────┐
          │                           │
          ▼                           ▼
   ┌─────────────┐           ┌─────────────┐
   │     DAG     │           │   Stores    │
   │  PruneOld   │           │  DeleteOld  │
   └─────────────┘           └─────────────┘
```

### 7.2 Message Flow Diagram

```
Validator 0 (Author)              Validator 1, 2, 3 (Voters)
═══════════════════               ═════════════════════════

[Worker creates batch]
       │
       ├─► Batch ────────────────► [Receive batch]
       │                           [Store batch]
       │                           [Send ack]
       │ ◄──────────────── BatchAck
       │
[AckTracker: 2f+1 acks received]
       │
       │
[Primary creates header]
       │
       ├─► Header ──────────────► [Receive header]
       │                          [Verify signature]
       │                          [Verify parents]
       │                          [Create vote]
       │                          [Send vote to author]
       │ ◄────────────────── Vote
       │ ◄────────────────── Vote
       │ ◄────────────────── Vote
       │
[VoteTracker: 2f+1 votes received]
[Primary forms certificate]
       │
       ├─► Certificate ──────────► [Receive certificate]
       │                           [Verify votes]
       │                           [Add to DAG]
       │
[Certificate added to DAG]
[Broadcast to network]
       │
       ├─► Certificate ──────────► [Add to DAG]
                                   [Update highest round]
```

### 7.3 State Transitions

```
Transaction States:
  1. Submitted → Pending (in worker queue)
  2. Pending → Batched (included in batch)
  3. Batched → Certified (batch included in certificate)
  4. Certified → Reaped (returned to consensus)
  5. Reaped → Committed (consensus commits round)
  6. Committed → GC'd (old round pruned)

Batch States:
  1. Created → Stored (saved to BatchStore)
  2. Stored → Broadcast (sent to network)
  3. Broadcast → Acknowledged (2f+1 acks received)
  4. Acknowledged → Referenced (included in header)
  5. Referenced → Certified (header becomes certificate)
  6. Certified → Committed (round committed by consensus)
  7. Committed → GC'd (pruned after GCDepth rounds)

Certificate States:
  1. Votes Collected → Formed (2f+1 votes received)
  2. Formed → Added to DAG (validated and stored)
  3. Added → Broadcast (sent to network)
  4. Broadcast → Ordered (consensus orders certificate)
  5. Ordered → Committed (consensus commits round)
  6. Committed → GC'd (pruned after GCDepth rounds)

Round States:
  1. Active (current round, accepting headers)
  2. Complete (has 2f+1 certificates)
  3. Committed (consensus has committed)
  4. GC'd (pruned after retention period)
```

---

## 8. API Design

### 8.1 DAGMempool Interface

The primary public interface for consensus integration:

```go
type DAGMempool interface {
    // Transaction submission
    AddTx(tx []byte) error

    // Transaction retrieval
    ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

    // Lifecycle notifications
    NotifyCommitted(round uint64)
    UpdateValidatorSet(validators types.ValidatorSet)

    // Query operations
    HasTx(hash []byte) bool
    Size() int
    SizeBytes() int64
    CurrentRound() uint64

    // Management operations
    Flush()
    Metrics() *Metrics
}
```

### 8.2 API Characteristics

**Thread-Safety**: All methods are safe for concurrent use.

**Non-Blocking**: All methods return quickly (no long-running operations).

**Idempotent Operations**:
- `AddTx()` with duplicate transaction returns nil (already have it)
- `NotifyCommitted()` with stale round is a no-op
- `UpdateValidatorSet()` can be called multiple times safely

**Error Semantics**:
- `AddTx()` returns retryable errors for backpressure/flow control
- `AddTx()` returns non-retryable errors for validation failures
- Byzantine behavior errors are logged but not propagated (system continues)

### 8.3 Public vs Internal APIs

**Public (exposed via DAGMempool)**:
- Transaction submission and retrieval
- Lifecycle notifications
- Query operations
- Metrics

**Internal (package-private)**:
- Worker pool management
- Header/vote/certificate creation
- DAG traversal
- Storage operations
- Network message handling

**Benefits of Encapsulation**:
- Simple integration for consensus layer
- Implementation flexibility (can change internals without breaking API)
- Clear separation of concerns
- Easier testing (mock DAGMempool interface)

### 8.4 Callback Design

**Callback Types**:
```go
type BatchCallback func(batch *types.Batch)
type HeaderCallback func(header *types.Header)
type VoteCallback func(vote *types.Vote, to uint16)
type CertificateCallback func(cert *types.Certificate)
type TxRecoveryCallback func(txs []types.Transaction)
```

**Callback Guarantees**:
- Called asynchronously (non-blocking)
- Panics are recovered (logged with stack trace)
- Errors are logged but not propagated
- Order is not guaranteed (callbacks may execute concurrently)

**Example**:
```go
// Set callbacks during initialization
primary.SetCertificateCallback(func(cert *types.Certificate) {
    // Add to DAG
    dag.AddCertificate(cert)

    // Update flow control
    flowController.UpdateCurrentRound(cert.Header.Round)

    // Broadcast to network
    network.BroadcastCertificate(cert)
})
```

---

## 9. Configuration

### 9.1 Configuration Structure

```go
type Config struct {
    // Identity
    ValidatorIndex uint16        // Required
    Signer         types.Signer  // Required

    // Transaction validation
    TxValidator TxValidator  // Optional

    // Component configuration
    Worker      WorkerConfig
    Primary     PrimaryConfig
    Sync        SyncConfig
    Storage     StorageConfig
    GC          GCConfig
    FlowControl FlowControlConfig
}
```

### 9.2 Default Values

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| **Worker** |||
| MinWorkers | 1 | Minimum viable parallelism |
| MaxWorkers | 8 | Balance throughput and overhead |
| BatchSize | 500 | Sweet spot for latency vs throughput |
| BatchTimeout | 100ms | Low latency for transaction finalization |
| MaxBatchBytes | 512KB | Reasonable network message size |
| MaxPendingTxs | 10,000 | Prevent unbounded memory growth |
| MaxPendingBytes | 50MB | Byte-based backpressure |
| ScaleUpThreshold | 0.8 | Scale before saturation |
| ScaleDownThreshold | 0.2 | Allow hysteresis |
| **Primary** |||
| HeaderTimeout | 500ms | Balance liveness and batching |
| MaxBatchesPerHeader | 100 | Limit header size |
| MaxRoundGap | 10 | Prevent far-ahead attacks |
| VoteTimeout | 30s | Account for network delays |
| AllowEmptyHeaders | true | Ensure liveness |
| **Sync** |||
| SyncInterval | 10s | Periodic gap detection |
| SyncThreshold | 5 | Trigger sync before significant lag |
| SyncBatchSize | 100 | Balance bandwidth and latency |
| SyncTimeout | 30s | Account for large responses |
| **GC** |||
| GCDepth | 50 | Retain recent history for queries |
| RecoverTxs | true | Don't lose uncommitted transactions |
| **FlowControl** |||
| MaxUncommittedRounds | 100 | Prevent unbounded DAG growth |

### 9.3 Configuration Validation

All configuration is validated on `New()`:

```go
func (c *Config) Validate() error {
    // Validate all sub-configs
    if err := c.Worker.Validate(); err != nil {
        return fmt.Errorf("worker config: %w", err)
    }
    if err := c.Primary.Validate(); err != nil {
        return fmt.Errorf("primary config: %w", err)
    }
    // ... other validations

    return nil
}
```

**Validation Rules**:
- All timeouts must be positive
- Min values must be >= 1
- Max values must be >= min values
- Thresholds must be in valid ranges (0.0-1.0)
- ScaleUpThreshold > ScaleDownThreshold

### 9.4 Configuration Examples

**Development (in-memory)**:
```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer
cfg.Storage.InMemory = true
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2
```

**Production (persistent)**:
```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry"
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
cfg.GC.GCDepth = 100
```

**High-Throughput**:
```go
cfg := looseberry.DefaultConfig()
cfg.Worker.BatchSize = 1000
cfg.Worker.BatchTimeout = 50 * time.Millisecond
cfg.Worker.MaxWorkers = 16
cfg.Primary.MaxBatchesPerHeader = 200
```

**Low-Latency**:
```go
cfg := looseberry.DefaultConfig()
cfg.Worker.BatchSize = 100
cfg.Worker.BatchTimeout = 10 * time.Millisecond
cfg.Primary.HeaderTimeout = 100 * time.Millisecond
```

---

## 10. Testing Strategy

### 10.1 Test Pyramid

```
         /\
        /  \  Byzantine Tests (4 tests)
       /----\
      /      \  Stress Tests (3 tests)
     /--------\
    /          \  Integration Tests (1 test)
   /------------\
  /              \  Unit Tests (50+ tests)
 /________________\
```

### 10.2 Unit Tests

**Coverage**: 85%+ code coverage across all packages

**Patterns**:
- Table-driven tests for multiple scenarios
- Helper functions for test setup
- Mock implementations for dependencies
- Parallel test execution where safe

**Example**:
```go
func TestWorkerBatchCreation(t *testing.T) {
    tests := []struct {
        name           string
        batchSize      int
        batchTimeout   time.Duration
        txCount        int
        expectBatches  int
    }{
        {
            name:          "size-based trigger",
            batchSize:     100,
            batchTimeout:  1 * time.Second,
            txCount:       250,
            expectBatches: 2,  // 100 + 100 + 50 (pending)
        },
        {
            name:          "timeout-based trigger",
            batchSize:     1000,
            batchTimeout:  10 * time.Millisecond,
            txCount:       50,
            expectBatches: 1,  // Timeout triggers with 50 txs
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

### 10.3 Integration Tests

**Purpose**: Test interaction between components

**Scenario**: Multi-node integration test
```go
func TestEndToEnd(t *testing.T) {
    // 1. Setup 4 nodes
    nodes := make([]*looseberry.Looseberry, 4)
    networks := make([]*network.MockNetwork, 4)

    for i := 0; i < 4; i++ {
        nodes[i] = createNode(t, i)
        networks[i] = createNetwork(t, i)
        connectNetworks(networks)
    }

    // 2. Start all nodes
    for _, node := range nodes {
        node.Start()
    }

    // 3. Submit transactions to node 0
    for i := 0; i < 1000; i++ {
        nodes[0].AddTx([]byte(fmt.Sprintf("tx-%d", i)))
    }

    // 4. Wait for propagation
    time.Sleep(5 * time.Second)

    // 5. Verify all nodes have certificates
    for i, node := range nodes {
        if node.CurrentRound() == 0 {
            t.Errorf("Node %d has no certificates", i)
        }
    }

    // 6. Verify transaction availability
    txHash := types.HashBytes([]byte("tx-0"))
    for i, node := range nodes {
        if !node.HasTx(txHash[:]) {
            t.Errorf("Node %d missing transaction", i)
        }
    }
}
```

### 10.4 Stress Tests

**Purpose**: Test performance and stability under high load

**Tests**:
1. **Sequential Throughput**: Single goroutine submitting transactions
2. **Concurrent Throughput**: Multiple goroutines submitting in parallel
3. **Memory Stability**: Monitor heap growth over sustained load

**Example**:
```go
func TestStressSequentialThroughput(t *testing.T) {
    lb := createLooseberry(t)
    lb.Start()
    defer lb.Stop()

    txCount := 100000
    start := time.Now()

    for i := 0; i < txCount; i++ {
        tx := []byte(fmt.Sprintf("tx-%d", i))
        if err := lb.AddTx(tx); err != nil {
            t.Fatalf("AddTx failed: %v", err)
        }
    }

    elapsed := time.Since(start)
    throughput := float64(txCount) / elapsed.Seconds()

    t.Logf("Sequential throughput: %.0f tx/sec", throughput)

    if throughput < 100000 {
        t.Errorf("Throughput too low: %.0f tx/sec", throughput)
    }
}
```

### 10.5 Byzantine Tests

**Purpose**: Verify BFT safety guarantees

**Scenarios**:
1. **Invalid Signatures**: Validators with incorrect signatures
2. **Equivocation**: Validator creates multiple headers for same round
3. **Missing Parents**: Headers without required parent certificates
4. **Invalid Certificates**: Certificates with insufficient votes

**Example**:
```go
func TestByzantineInvalidSignature(t *testing.T) {
    // Setup network with 4 validators
    nodes, networks := setupNetwork(t, 4)

    // Node 1 is Byzantine - sends invalid signatures
    byzantineNode := nodes[1]
    byzantineNode.SetSigner(invalidSigner)

    // Submit transactions
    for i := 0; i < 100; i++ {
        nodes[0].AddTx([]byte(fmt.Sprintf("tx-%d", i)))
    }

    // Wait for certificate formation attempts
    time.Sleep(5 * time.Second)

    // Verify honest nodes reject Byzantine certificates
    for i, node := range nodes {
        if i == 1 {
            continue  // Skip Byzantine node
        }

        // Should have certificates from honest nodes only
        round := node.CurrentRound()
        if round == 0 {
            t.Errorf("Node %d has no certificates", i)
        }

        // Verify no certificates from Byzantine node
        for r := uint64(0); r <= round; r++ {
            // Check that Byzantine node's certificates are absent
            // (implementation-specific query)
        }
    }
}
```

### 10.6 Benchmark Tests

**Purpose**: Track performance over time

**Benchmarks**:
- Worker AddTx throughput
- Worker Pool AddTx throughput (parallel)
- DAG AddCertificate throughput
- Looseberry AddTx throughput
- ReapCertifiedBatches latency

**Example**:
```go
func BenchmarkLooseberryAddTx(b *testing.B) {
    lb := createLooseberry(b)
    lb.Start()
    defer lb.Stop()

    txs := make([][]byte, b.N)
    for i := 0; i < b.N; i++ {
        txs[i] = []byte(fmt.Sprintf("tx-%d", i))
    }

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        if err := lb.AddTx(txs[i]); err != nil {
            b.Fatalf("AddTx failed: %v", err)
        }
    }

    b.ReportAllocs()
}
```

**Results**:
```
BenchmarkLooseberryAddTx-8       7,500,000    132 ns/op    64 B/op    2 allocs/op
BenchmarkWorkerPoolAddTx-8       7,000,000    145 ns/op    72 B/op    2 allocs/op
BenchmarkDAGAddCertificate-8     1,400,000    734 ns/op   256 B/op    8 allocs/op
```

---

## 11. Performance Considerations

### 11.1 Optimization Patterns

**1. Lock-Free Operations**:
```go
// Atomic counters for metrics (no lock overhead)
totalTxAdded.Add(1)
totalTxRejected.Add(1)

// Atomic state flags
if running.Swap(false) {
    // State transition
}
```

**2. Pre-Allocation**:
```go
// Pre-allocate with known capacity
data := make([]byte, 0, estimatedSize)
certs := make([]*Certificate, 0, expectedCount)
```

**3. Batching**:
```go
// Batch transactions to reduce per-operation overhead
batch := make([]Transaction, 0, batchSize)
for tx := range pending {
    batch = append(batch, tx)
    if len(batch) >= batchSize {
        processBatch(batch)
        batch = batch[:0]  // Reuse slice
    }
}
```

**4. Caching**:
```go
// LRU cache for expensive operations
if cached, ok := historyCache[certDigest]; ok {
    return cached
}

result := expensiveOperation()

// Cache with LRU eviction
if len(historyCacheKeys) >= maxCacheSize {
    evictOldest()
}
historyCache[certDigest] = result
historyCacheKeys = append(historyCacheKeys, certDigest)
```

**5. Separate Locks**:
```go
// Separate locks for independent data structures
type DAG struct {
    rounds      map[uint64]*RoundData
    roundsMu    sync.RWMutex

    certIndex   map[Hash]*Certificate
    certIndexMu sync.RWMutex  // Different lock!
}

// Concurrent access to different indices
func (d *DAG) operation() {
    d.roundsMu.Lock()
    // Work with rounds
    d.roundsMu.Unlock()

    d.certIndexMu.Lock()  // Can lock independently
    // Work with certIndex
    d.certIndexMu.Unlock()
}
```

### 11.2 Memory Management

**Heap Allocation Patterns**:
```
Allocation Sources:
  1. Transaction cloning: ~100 bytes per transaction
  2. Batch creation: ~50KB per batch (500 txs * 100 bytes)
  3. Certificate storage: ~10KB per certificate
  4. DAG round data: ~100KB per round (assuming 10 certificates)
  5. History cache: Bounded by MaxHistoryCacheSize

Total Memory Estimate:
  Base: ~100MB (data structures, goroutines)
  Per 10K pending txs: ~50MB
  Per 100 rounds cached: ~10MB
  Per 1000 history entries: ~5MB
```

**Memory Optimization Strategies**:
1. **Clone on Boundaries**: Only clone when crossing API boundaries
2. **Bounded Caches**: LRU eviction prevents unbounded growth
3. **GC Integration**: Prune old data regularly
4. **Zero-Copy**: Use pointers where safe (internal only)

### 11.3 Throughput Optimization

**Achieved Throughput**:
- Sequential: 250,000+ transactions/second
- Concurrent (12 goroutines): 200,000+ transactions/second
- Certificate formation: 2,200+ certificates/second

**Bottlenecks and Mitigation**:
1. **Worker Lock Contention**: Use multiple workers with hash-based routing
2. **Storage I/O**: Use batch writes, async persistence
3. **Network Bandwidth**: Compress messages, batch broadcasts
4. **Signature Verification**: Parallelize verification across cores

**Scaling Characteristics**:
```
Workers vs Throughput:
  1 worker:  ~50K tx/sec
  2 workers: ~100K tx/sec
  4 workers: ~200K tx/sec
  8 workers: ~250K tx/sec
  16 workers: ~260K tx/sec (diminishing returns)

Latency vs Batch Size:
  Batch size 100:  p50=50ms,  p99=150ms
  Batch size 500:  p50=100ms, p99=300ms
  Batch size 1000: p50=200ms, p99=500ms
```

### 11.4 Latency Optimization

**Latency Sources**:
1. **Batch Timeout**: 100ms default (configurable)
2. **Header Timeout**: 500ms default (configurable)
3. **Vote Collection**: 1-2 RTT (network-dependent)
4. **Certificate Propagation**: 1 RTT

**Total Transaction Latency**:
```
Best case: ~200ms (immediate batch + header + voting + cert)
Typical: ~500ms
Worst case: ~1s (timeout-based batching)
```

**Latency Reduction Strategies**:
1. Reduce batch timeout (trade throughput for latency)
2. Reduce header timeout (more frequent headers)
3. Optimize network stack (reduce RTT)
4. Use larger worker pool (reduce queuing delay)

---

## 12. Error Handling

### 12.1 Error Categories

**Retryable Errors** (transient failures):
```go
var (
    ErrWorkerBackpressure = errors.New("worker back-pressure")
    ErrMempoolFull       = errors.New("mempool is full")
    ErrFlowControlPaused = errors.New("flow control paused")
    ErrSyncTimeout       = errors.New("sync timeout")
)

func IsRetryable(err error) bool {
    switch {
    case errors.Is(err, ErrWorkerBackpressure):
        return true
    case errors.Is(err, ErrMempoolFull):
        return true
    case errors.Is(err, ErrFlowControlPaused):
        return true
    case errors.Is(err, ErrSyncTimeout):
        return true
    default:
        return false
    }
}
```

**Byzantine Errors** (malicious behavior):
```go
var (
    ErrInvalidSignature  = errors.New("invalid signature")
    ErrDuplicateHeader   = errors.New("duplicate header")
    ErrInvalidBatch      = errors.New("invalid batch")
    ErrEquivocation      = errors.New("equivocation detected")
)

func IsByzantine(err error) bool {
    switch {
    case errors.Is(err, ErrInvalidSignature):
        return true
    case errors.Is(err, ErrDuplicateHeader):
        return true
    case errors.Is(err, ErrInvalidBatch):
        return true
    default:
        return false
    }
}
```

**Permanent Errors** (non-retryable):
```go
var (
    ErrTxValidationFailed = errors.New("transaction validation failed")
    ErrNotFound           = errors.New("not found")
    ErrInvalidConfig      = errors.New("invalid configuration")
)
```

### 12.2 Error Propagation

**API Boundary** (errors returned to caller):
```go
func (l *Looseberry) AddTx(tx []byte) error {
    if !l.running.Load() {
        return ErrNotRunning  // Caller can handle
    }

    if l.flowController.IsPaused() {
        return ErrFlowControlPaused  // Retryable
    }

    if err := l.workerPool.AddTx(tx); err != nil {
        return err  // Propagate worker error
    }

    return nil
}
```

**Internal Boundaries** (errors logged, not propagated):
```go
func (l *Looseberry) onBatchCreated(batch *types.Batch) {
    defer recoverCallback("onBatchCreated")  // Recover panics

    // Errors are logged but not propagated
    if err := l.network.BroadcastBatch(batch); err != nil {
        log.Printf("Failed to broadcast batch: %v", err)
        // Continue operation (non-critical)
    }
}
```

**Byzantine Behavior** (logged for slashing):
```go
func (p *Primary) HandleVote(vote *types.Vote) (*types.Certificate, bool) {
    if !vote.Verify(validatorSet) {
        // Log Byzantine behavior
        logByzantine(vote.Validator, "invalid vote signature", vote)
        return nil, false
    }

    // Check for double voting
    if existingVote := getExistingVote(vote); existingVote != nil {
        // Log equivocation evidence
        logEquivocation(vote.Validator, existingVote, vote)
        return nil, false
    }

    // ... continue processing
}
```

### 12.3 Error Handling Patterns

**Retry with Backoff**:
```go
func submitWithRetry(lb DAGMempool, tx []byte) error {
    maxRetries := 3
    backoff := 100 * time.Millisecond

    for attempt := 0; attempt < maxRetries; attempt++ {
        err := lb.AddTx(tx)
        if err == nil {
            return nil
        }

        if !types.IsRetryable(err) {
            return err  // Permanent failure
        }

        // Exponential backoff
        time.Sleep(backoff * (1 << attempt))
    }

    return errors.New("max retries exceeded")
}
```

**Panic Recovery**:
```go
func recoverCallback(callbackName string) {
    if r := recover(); r != nil {
        log.Printf("ERROR: Panic in %s: %v\n%s",
            callbackName, r, debug.Stack())
    }
}

// Usage
func (l *Looseberry) onCertificateFormed(cert *types.Certificate) {
    defer recoverCallback("onCertificateFormed")

    // If panic occurs, it's caught and logged
    // System continues operating
    l.dag.AddCertificate(cert)
}
```

**Error Wrapping**:
```go
func (l *Looseberry) initializeStores() error {
    if l.batchStore == nil {
        bs, err := store.NewLevelDBBatchStore(path)
        if err != nil {
            return fmt.Errorf("create batch store: %w", err)
        }
        l.batchStore = bs
    }
    return nil
}

// Caller can inspect original error
if err := lb.Start(); err != nil {
    if errors.Is(err, leveldb.ErrCorrupted) {
        // Handle specific error
    }
}
```

---

## 13. Security Architecture

### 13.1 Byzantine Fault Tolerance

**Threat Model**:
- Up to f Byzantine validators (n = 3f + 1)
- Byzantine validators can:
  - Send invalid messages
  - Equivocate (double-sign)
  - Collude with other Byzantine validators
  - Delay or drop messages
  - Deviate from protocol arbitrarily

**BFT Guarantees**:
1. **Safety**: Honest validators never form conflicting certificates
2. **Liveness**: Progress continues with 2f+1 honest validators
3. **Accountability**: Byzantine behavior is detected and logged

**Quorum Requirements**:
```
Validators: n = 3f + 1
Byzantine:  f = (n-1)/3
Quorum:     2f + 1

Examples:
  n=4: f=1, quorum=3
  n=7: f=2, quorum=5
  n=10: f=3, quorum=7
```

### 13.2 Cryptographic Primitives

**Ed25519 Signatures**:
- Public key: 32 bytes
- Private key: 64 bytes (32 bytes seed + 32 bytes derived public key)
- Signature: 64 bytes
- Security level: 128 bits

**SHA-256 Hashing**:
- Digest: 32 bytes
- Security level: 128 bits (birthday bound)

**Signature Verification**:
```go
func (cert *Certificate) Verify(validators ValidatorSet) error {
    // Verify header signature
    author := validators.GetByIndex(cert.Header.Author)
    if !cert.Header.Verify(author.PublicKey) {
        return ErrInvalidSignature
    }

    // Verify all vote signatures
    for _, vote := range cert.Votes {
        validator := validators.GetByIndex(vote.Validator)
        if !vote.Verify(validator.PublicKey) {
            return ErrInvalidSignature
        }

        // Verify vote is for this header
        if !vote.HeaderDigest.Equal(cert.Header.Digest) {
            return ErrVoteWrongHeader
        }
    }

    // Verify quorum
    if len(cert.Votes) < validators.Quorum() {
        return ErrInsufficientQuorum
    }

    return nil
}
```

### 13.3 Attack Mitigation

**1. Equivocation (Double-Signing)**:
```
Attack: Byzantine validator creates multiple headers for same round

Detection:
  - DAG rejects duplicate headers from same validator for same round
  - VoteTracker detects double-voting on different headers

Mitigation:
  - Log equivocation evidence for slashing
  - Reject all future messages from equivocator (optional)
```

**2. Invalid Signatures**:
```
Attack: Byzantine validator sends messages with invalid signatures

Detection:
  - All signatures verified before accepting messages
  - Invalid signatures rejected immediately

Mitigation:
  - Log Byzantine behavior
  - Continue operation (BFT guarantees maintained)
```

**3. Missing Parents**:
```
Attack: Byzantine validator creates header without required parent certificates

Detection:
  - DAG validates all parent certificates exist before accepting
  - Missing parents return ErrMissingParents

Mitigation:
  - Request missing parents via sync protocol
  - Reject header if parents cannot be fetched
```

**4. Far-Ahead Attacks**:
```
Attack: Byzantine validator sends headers far ahead of current round

Detection:
  - Primary rejects headers with round > currentRound + MaxRoundGap
  - Prevents resource exhaustion

Mitigation:
  - Configured MaxRoundGap (default: 10 rounds)
  - Log far-ahead attempts as potential Byzantine behavior
```

**5. DoS via Transaction Spam**:
```
Attack: Flood mempool with transactions to exhaust resources

Detection:
  - Backpressure limits per-worker pending transactions
  - Flow control pauses when uncommitted gap too large

Mitigation:
  - MaxPendingTxs limit per worker
  - MaxUncommittedRounds limit system-wide
  - Return ErrWorkerBackpressure / ErrFlowControlPaused
```

### 13.4 Data Availability Guarantees

**Certificate = Proof of Data Availability**:
- Certificate contains 2f+1 votes
- Each vote proves validator has the data (batches + parents)
- Therefore, 2f+1 validators have the data
- At least f+1 honest validators have the data (guaranteed)

**Batch Acknowledgment Protocol**:
```go
// Sender broadcasts batch
network.BroadcastBatch(batch)

// Receivers store batch and send ack
func handleBatch(batch *Batch) {
    batchStore.SaveBatch(batch)
    txIndex.AddBatch(batch)
    network.SendBatchAck(batch.ValidatorID, &BatchAck{
        BatchDigest: batch.Digest,
        Validator:   myValidatorID,
    })
}

// Sender waits for 2f+1 acks
ackTracker.RecordAck(batchDigest, validatorID)
if ackTracker.HasQuorum(batchDigest, quorum) {
    // Data available to 2f+1 validators
    // Safe to include in header
}
```

---

## 14. Storage Architecture

### 14.1 Storage Abstraction

**Design Principles**:
1. **Interface-based**: Easy to swap implementations
2. **Pluggable**: In-memory for testing, persistent for production
3. **Minimal**: Only essential operations in interface
4. **Thread-safe**: All implementations are concurrent-safe

**Storage Interfaces**:
```go
// BatchStore: Transaction batch storage
type BatchStore interface {
    SaveBatch(batch *types.Batch) error
    GetBatch(digest types.Hash) (*types.Batch, error)
    HasBatch(digest types.Hash) bool
    GetBatchesByRound(round uint64) ([]*types.Batch, error)
    DeleteBatchesBefore(round uint64) error
    Close() error
}

// CertificateStore: Certificate storage with round indexing
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

// TxIndex: O(1) transaction lookup
type TxIndex interface {
    AddTx(txHash, batchHash types.Hash) error
    AddBatch(batch *types.Batch) error
    GetBatchForTx(txHash types.Hash) (types.Hash, error)
    HasTx(txHash types.Hash) bool
    RemoveTxsForBatch(batchHash types.Hash) error
    PruneOlderThan(round uint64, batchStore BatchStore) (int, error)
    Close() error
}
```

### 14.2 Memory Storage

**Use Case**: Testing, development, ephemeral nodes

**Implementation**:
```go
type MemoryBatchStore struct {
    batches map[types.Hash]*types.Batch          // digest -> batch
    rounds  map[uint64][]*types.Batch            // round -> batches
    mu      sync.RWMutex
}

type MemoryCertificateStore struct {
    certs         map[types.Hash]*types.Certificate  // digest -> cert
    rounds        map[uint64][]*types.Certificate    // round -> certs
    validators    map[uint64]map[uint16]*types.Certificate  // round -> validator -> cert
    highestRound  uint64
    mu            sync.RWMutex
}

type MemoryTxIndex struct {
    txToBatch map[types.Hash]types.Hash  // tx hash -> batch hash
    batchToTxs map[types.Hash][]types.Hash  // batch hash -> tx hashes
    batchRounds map[types.Hash]uint64  // batch hash -> round
    mu         sync.RWMutex
}
```

**Characteristics**:
- Fast: All operations O(1)
- Simple: No serialization overhead
- Ephemeral: Data lost on restart
- Thread-safe: RWMutex protection
- Cloning: Returns clones to prevent external modification

### 14.3 LevelDB Storage

**Use Case**: Production, persistent nodes, archive nodes

**Key Encoding**:
```
Batches:
  Key:   "b:" + digest (33 bytes)
  Value: Protobuf-encoded Batch

Certificates:
  Key:   "c:" + digest (33 bytes)
  Value: Protobuf-encoded Certificate

Round Index (Certificates):
  Key:   "r:" + round (8 bytes) + validator (2 bytes)
  Value: digest (32 bytes)

Metadata:
  Key:   "meta:highest_round"
  Value: round (8 bytes)
```

**Implementation**:
```go
type LevelDBBatchStore struct {
    db *leveldb.DB
    mu sync.RWMutex  // Protects Close
}

func (s *LevelDBBatchStore) SaveBatch(batch *types.Batch) error {
    key := append([]byte("b:"), batch.Digest[:]...)
    value, err := encodeBatch(batch)
    if err != nil {
        return err
    }
    return s.db.Put(key, value, nil)
}

func (s *LevelDBBatchStore) GetBatchesByRound(round uint64) ([]*types.Batch, error) {
    // Range scan: "r:{round}:" to "r:{round+1}:"
    start := encodeRoundPrefix(round)
    limit := encodeRoundPrefix(round + 1)

    var batches []*types.Batch
    iter := s.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
    defer iter.Release()

    for iter.Next() {
        batchDigest := iter.Value()
        batch, err := s.GetBatch(types.Hash(batchDigest))
        if err == nil && batch != nil {
            batches = append(batches, batch)
        }
    }

    return batches, iter.Error()
}
```

**Characteristics**:
- Persistent: Survives restarts
- Efficient: Compressed storage, range queries
- Reliable: Write-ahead log, crash recovery
- Scalable: Handles large datasets
- Trade-off: Slower than memory (disk I/O)

### 14.4 Storage Lifecycle

**Initialization**:
```go
func (l *Looseberry) initializeStores() error {
    if l.batchStore == nil {
        if l.cfg.Storage.InMemory {
            l.batchStore = store.NewMemoryBatchStore()
        } else {
            bs, err := store.NewLevelDBBatchStore(l.cfg.Storage.DataDir + "/batches")
            if err != nil {
                return fmt.Errorf("create batch store: %w", err)
            }
            l.batchStore = bs
        }
    }
    // ... similar for certStore and txIndex
    return nil
}
```

**Shutdown**:
```go
func (l *Looseberry) Stop() error {
    // ... stop components ...

    // Close stores (releases resources, flushes data)
    if l.batchStore != nil {
        l.batchStore.Close()
    }
    if l.certStore != nil {
        l.certStore.Close()
    }
    if l.txIndex != nil {
        l.txIndex.Close()
    }

    return nil
}
```

**Garbage Collection**:
```go
func (gc *GCManager) performGC(beforeRound uint64) error {
    // Delete from stores
    if err := gc.batchStore.DeleteBatchesBefore(beforeRound); err != nil {
        return err
    }
    if err := gc.certStore.DeleteCertificatesBefore(beforeRound); err != nil {
        return err
    }
    if _, err := gc.txIndex.PruneOlderThan(beforeRound, gc.batchStore); err != nil {
        return err
    }
    return nil
}
```

---

## 15. Network Protocol

### 15.1 Message Types

```go
// Batch dissemination
type BatchMessage struct {
    Batch *types.Batch
    From  uint16  // Sender validator ID
}

type BatchAckMessage struct {
    BatchDigest types.Hash
    Validator   uint16
    Signature   types.Signature  // Optional: signed ack
}

// Header dissemination and voting
type HeaderMessage struct {
    Header *types.Header
    From   uint16
}

type VoteMessage struct {
    Vote *types.Vote
    From uint16
}

// Certificate dissemination
type CertificateMessage struct {
    Certificate *types.Certificate
    From        uint16
}

// Batch request/response (missing batch resolution)
type BatchRequestMessage struct {
    BatchDigest types.Hash
    Requester   uint16
}

type BatchResponseMessage struct {
    Batch *types.Batch
    Found bool
    From  uint16
}

// Synchronization
type SyncRequest struct {
    FromRound uint64  // Lowest round needed
    ToRound   uint64  // Highest round needed (0 = latest)
    Requester uint16
}

type SyncResponse struct {
    Certificates []*types.Certificate
    Batches      []*types.Batch  // Batches referenced by certificates
    FromRound    uint64
    ToRound      uint64
}
```

### 15.2 Communication Patterns

**Broadcast Messages**:
- `Batch`: Worker broadcasts to all validators
- `Header`: Primary broadcasts to all validators
- `Certificate`: Primary broadcasts to all validators

**Point-to-Point Messages**:
- `Vote`: Validator sends to header author
- `BatchAck`: Validator sends to batch author
- `BatchRequest/Response`: Fetch missing batches
- `SyncRequest/Response`: Catch-up protocol

**Message Flow**:
```
Batch Creation:
  Validator 0 → [Batch] → All Validators
  All Validators → [BatchAck] → Validator 0

Header Creation:
  Validator 0 → [Header] → All Validators

Voting:
  Validator 1 → [Vote] → Validator 0 (header author)
  Validator 2 → [Vote] → Validator 0
  Validator 3 → [Vote] → Validator 0

Certificate Formation:
  Validator 0 → [Certificate] → All Validators

Synchronization:
  Validator 3 (behind) → [SyncRequest] → Validator 0 (ahead)
  Validator 0 → [SyncResponse] → Validator 3
```

### 15.3 Network Interface

```go
type Network interface {
    // Broadcast to all validators
    BroadcastBatch(batch *types.Batch) error
    BroadcastHeader(header *types.Header) error
    BroadcastCertificate(cert *types.Certificate) error

    // Send to specific validator
    SendVote(validator uint16, vote *types.Vote) error
    SendBatchAck(validator uint16, ack *BatchAckMessage) error
    SendBatchRequest(validator uint16, req *BatchRequestMessage) error
    SendBatchResponse(validator uint16, resp *BatchResponseMessage) error
    SendSyncRequest(validator uint16, req *SyncRequest) error
    SendSyncResponse(validator uint16, resp *SyncResponse) error

    // Receive channels (non-blocking reads)
    BatchMessages() <-chan *BatchMessage
    BatchAckMessages() <-chan *BatchAckMessage
    HeaderMessages() <-chan *HeaderMessage
    VoteMessages() <-chan *VoteMessage
    CertificateMessages() <-chan *CertificateMessage
    BatchRequestMessages() <-chan *BatchRequestMessage
    BatchResponseMessages() <-chan *BatchResponseMessage
    SyncRequests() <-chan *SyncRequest
    SyncResponses() <-chan *SyncResponseMessage

    ValidatorID() uint16
    Start() error
    Stop() error
}
```

### 15.4 Mock Network

**Purpose**: Testing without real network

**Implementation**:
```go
type MockNetwork struct {
    validatorID uint16

    // Peer connections (map: validator ID -> network)
    peers   map[uint16]*MockNetwork
    peersMu sync.RWMutex

    // Message channels (buffered)
    batchCh       chan *BatchMessage
    batchAckCh    chan *BatchAckMessage
    headerCh      chan *HeaderMessage
    voteCh        chan *VoteMessage
    certCh        chan *CertificateMessage
    syncReqCh     chan *SyncRequest
    syncRespCh    chan *SyncResponseMessage

    // Statistics
    stats   MockNetworkStats
    statsMu sync.Mutex
}

// Connect two mock networks bidirectionally
func (m *MockNetwork) Connect(peer *MockNetwork) {
    m.peersMu.Lock()
    m.peers[peer.validatorID] = peer
    m.peersMu.Unlock()

    peer.peersMu.Lock()
    peer.peers[m.validatorID] = m
    peer.peersMu.Unlock()
}

// Broadcast to all connected peers
func (m *MockNetwork) BroadcastBatch(batch *types.Batch) error {
    m.peersMu.RLock()
    defer m.peersMu.RUnlock()

    msg := &BatchMessage{
        Batch: batch,
        From:  m.validatorID,
    }

    for _, peer := range m.peers {
        select {
        case peer.batchCh <- msg:
            // Delivered
        default:
            // Channel full (simulate network congestion)
        }
    }

    m.updateStats("batches_broadcast", 1)
    return nil
}
```

**Benefits**:
- Deterministic testing (no network timing issues)
- Simulate network partitions (disconnect peers)
- Simulate packet loss (drop messages)
- Measure message counts (statistics)
- Fast (no actual network I/O)

---

## 16. Deployment Considerations

### 16.1 Integration with Consensus

**Consensus Requirements**:
1. Must call `NotifyCommitted(round)` after committing each round
2. Must call `ReapCertifiedBatches(maxBytes)` to build blocks
3. Must call `UpdateValidatorSet(validators)` on epoch changes

**Example Integration**:
```go
type Consensus struct {
    mempool looseberry.DAGMempool
    // ... other consensus state
}

func (c *Consensus) BuildBlock() (*Block, error) {
    // Reap certified batches
    maxBytes := c.cfg.MaxBlockSize
    certBatches := c.mempool.ReapCertifiedBatches(maxBytes)

    // Extract transactions
    var txs [][]byte
    for _, cb := range certBatches {
        for _, tx := range cb.Batch.Transactions {
            txs = append(txs, tx.Bytes())
        }
    }

    // Build block with transactions
    block := NewBlock(c.height, c.lastBlockHash, txs)
    return block, nil
}

func (c *Consensus) CommitBlock(block *Block, round uint64) error {
    // Commit block
    if err := c.stateDB.CommitBlock(block); err != nil {
        return err
    }

    // Notify mempool
    c.mempool.NotifyCommitted(round)

    return nil
}

func (c *Consensus) BeginEpoch(epoch uint64, validators types.ValidatorSet) error {
    // Update validator set in consensus
    c.validatorSet = validators

    // Update validator set in mempool
    c.mempool.UpdateValidatorSet(validators)

    return nil
}
```

### 16.2 Resource Requirements

**Minimum Requirements**:
```
CPU: 2 cores
Memory: 2GB RAM
Storage: 10GB SSD
Network: 10 Mbps
```

**Recommended Requirements**:
```
CPU: 4+ cores (for parallel worker processing)
Memory: 8GB RAM
Storage: 100GB SSD (for historical data)
Network: 100 Mbps (for high throughput)
```

**Resource Usage Estimates**:
```
Memory:
  Base: ~100MB (data structures, goroutines)
  Per 10K pending txs: ~50MB
  Per 100 DAG rounds: ~10MB
  Per 1000 certificates: ~5MB

Storage (per day, 1000 tx/sec):
  Batches: ~500MB (assuming 500 bytes per tx)
  Certificates: ~50MB (assuming 100 certificates per minute)
  Total: ~550MB/day

CPU:
  Baseline: ~10% (idle with periodic tasks)
  Per 1000 tx/sec: ~20% (single core)
  Signature verification: CPU-bound (scales with cores)

Network:
  Per 1000 tx/sec: ~5 Mbps (assuming 500 bytes per tx)
  Certificate overhead: ~1 Mbps (headers, votes, certificates)
  Total: ~6 Mbps for 1000 tx/sec
```

### 16.3 Configuration for Different Environments

**Development (Local Testing)**:
```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = true
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2
cfg.GC.GCDepth = 10
```

**Staging (Pre-Production)**:
```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/opt/looseberry/data"
cfg.Worker.MinWorkers = 2
cfg.Worker.MaxWorkers = 4
cfg.GC.GCDepth = 50
```

**Production (High Availability)**:
```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
cfg.GC.GCDepth = 100
cfg.FlowControl.MaxUncommittedRounds = 100
```

**Archive Node (Historical Data)**:
```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/mnt/storage/looseberry"
cfg.GC.GCDepth = 10000  // Keep much more history
cfg.GC.RecoverTxs = false  // Don't re-inject (read-only)
```

### 16.4 Monitoring and Observability

**Metrics to Monitor**:
```go
metrics := lb.Metrics()

// Transaction metrics
fmt.Printf("Pending: %d txs (%d bytes)\n", metrics.PendingTxCount, metrics.PendingTxBytes)
fmt.Printf("Total: added=%d, rejected=%d\n", metrics.TotalTxAdded, metrics.TotalTxRejected)

// Worker metrics
fmt.Printf("Workers: %d active, load=%.2f\n", metrics.WorkerCount, metrics.WorkerLoad)

// DAG metrics
fmt.Printf("Rounds: current=%d, committed=%d, highest=%d\n",
    metrics.CurrentRound, metrics.CommittedRound, metrics.HighestRound)

// Flow control metrics
if metrics.IsPaused {
    fmt.Printf("PAUSED: uncommitted gap=%d\n", metrics.UncommittedGap)
}
```

**Health Checks**:
```go
func checkHealth(lb looseberry.DAGMempool) error {
    metrics := lb.Metrics()

    // Check if running
    if !lb.IsRunning() {
        return errors.New("looseberry not running")
    }

    // Check if paused
    if metrics.IsPaused {
        return errors.New("flow control paused")
    }

    // Check rejection rate
    rejectionRate := float64(metrics.TotalTxRejected) / float64(metrics.TotalTxAdded)
    if rejectionRate > 0.1 {
        return fmt.Errorf("high rejection rate: %.2f%%", rejectionRate*100)
    }

    // Check uncommitted gap
    if metrics.UncommittedGap > 50 {
        return fmt.Errorf("large uncommitted gap: %d", metrics.UncommittedGap)
    }

    return nil
}
```

**Alerting Conditions**:
1. `IsPaused = true` → Flow control activated (consensus falling behind)
2. `WorkerLoad > 0.9` → Workers overloaded (need more capacity)
3. `UncommittedGap > 50` → Consensus not committing fast enough
4. `TotalTxRejected / TotalTxAdded > 0.1` → High rejection rate
5. `PendingTxCount > MaxPendingTxs * 0.8` → Approaching backpressure limit

---

## 17. Design Decisions

### 17.1 Why DAG-Based Architecture?

**Traditional Mempool**:
```
Cons:
  - Couples transaction dissemination with consensus
  - Consensus bottleneck limits throughput
  - All validators must agree on transaction order before dissemination
  - Network bandwidth wasted on re-sending transactions

Pros:
  - Simpler to implement
  - Easier to reason about
```

**DAG-Based Mempool (Looseberry)**:
```
Pros:
  - Decouples dissemination from ordering
  - Parallel transaction batching (independent of consensus)
  - Certificate-based data availability (guaranteed by 2f+1 validators)
  - Consensus can be optimized separately
  - Higher throughput potential

Cons:
  - More complex implementation
  - Requires certificate formation protocol
  - Larger state to manage (DAG + batches)
```

**Decision**: Use DAG-based architecture for higher throughput and separation of concerns.

### 17.2 Why Worker Pool with Hash-Based Routing?

**Alternatives Considered**:
1. **Single Worker**: Simple but limits parallelism
2. **Random Routing**: Simple but uneven load distribution
3. **Round-Robin**: Better distribution but cache-unfriendly

**Chosen: Hash-Based Routing**:
```
Benefits:
  - Deterministic routing (same tx always goes to same worker)
  - Good load distribution (uniform hash distribution)
  - Cache-friendly (worker sees same tx hashes repeatedly)
  - Deduplication within worker (no cross-worker coordination)

Implementation:
  txHash := tx.Hash()
  hashValue := binary.BigEndian.Uint64(txHash[:8])
  workerIdx := int(hashValue % uint64(len(workers)))
```

**Decision**: Use hash-based routing for deterministic, well-distributed parallelism.

### 17.3 Why Ed25519 Instead of ECDSA?

**Alternatives Considered**:
- ECDSA (secp256k1): Used in Bitcoin, Ethereum
- BLS: Signature aggregation for smaller certificates

**Chosen: Ed25519**:
```
Benefits:
  - Fast signature generation (~52K ops/sec)
  - Fast verification (~16K ops/sec)
  - Small keys (32 bytes public, 64 bytes private)
  - Deterministic (no RNG required)
  - Side-channel resistant
  - Well-tested (NaCl, libsodium)

Trade-offs:
  - No signature aggregation (vs BLS)
  - Certificates larger than BLS (64 bytes per vote)

Decision rationale:
  - Performance and security outweigh certificate size
  - BLS complexity not justified for current use case
  - May add BLS as optional feature in future
```

**Decision**: Use Ed25519 for performance and simplicity.

### 17.4 Why LevelDB Instead of Other Databases?

**Alternatives Considered**:
- BadgerDB: Pure Go, faster writes
- RocksDB: Facebook's fork of LevelDB, more features
- PostgreSQL: Full RDBMS, more query features
- SQLite: Simpler, single-file database

**Chosen: LevelDB**:
```
Benefits:
  - Battle-tested (used in Bitcoin Core, Chrome, etc.)
  - Efficient range queries (needed for round indexing)
  - Automatic compression
  - Write-ahead logging (crash recovery)
  - Embedded (no external daemon)
  - Go bindings well-maintained

Trade-offs:
  - Single-threaded writes (vs BadgerDB)
  - Fewer features than RocksDB
  - No SQL queries (vs PostgreSQL/SQLite)

Decision rationale:
  - Proven reliability in production systems
  - Sufficient performance for current use case
  - Simple deployment (embedded)
  - Can switch to RocksDB if performance becomes issue
```

**Decision**: Use LevelDB for reliability and simplicity.

### 17.5 Why Callback-Based Integration?

**Alternatives Considered**:
1. **Interfaces**: Components implement interfaces for callbacks
2. **Channels**: Components send events via channels
3. **Direct Calls**: Components directly call methods on dependencies

**Chosen: Function Callbacks**:
```
Benefits:
  - Loose coupling (components don't need to know about each other)
  - Flexible (can change callback behavior without modifying component)
  - Testable (easy to inject test callbacks)
  - Simple (no interface boilerplate)

Implementation:
  type CertificateCallback func(cert *types.Certificate)

  primary.SetCertificateCallback(func(cert *types.Certificate) {
      dag.AddCertificate(cert)
      flowController.UpdateCurrentRound(cert.Header.Round)
      network.BroadcastCertificate(cert)
  })

Trade-offs:
  - No compile-time type checking (vs interfaces)
  - Callback panics must be recovered
  - Order not guaranteed (vs channels)
```

**Decision**: Use function callbacks for flexibility and simplicity.

### 17.6 Why Atomic Operations Instead of Mutexes for Counters?

**Alternatives**:
- Mutex-protected counters
- Channel-based counters
- Lock-free algorithms

**Chosen: Atomic Operations**:
```
Benefits:
  - Lock-free (no contention)
  - Fast (~10ns per operation)
  - Wait-free progress guarantee
  - Simple API (Add, Load, Store, Swap)

Implementation:
  var totalTxAdded atomic.Uint64

  // Increment
  totalTxAdded.Add(1)

  // Read
  count := totalTxAdded.Load()

  // Atomic swap (for state transitions)
  if !running.Swap(false) {
      return ErrNotRunning
  }

Trade-offs:
  - Limited to simple types (integers, booleans, pointers)
  - No compound operations (vs mutex)
  - Requires Go 1.19+ for typed atomics
```

**Decision**: Use atomic operations for counters and simple state for best performance.

---

## 18. Future Considerations

### 18.1 Potential Improvements

**1. BLS Signature Aggregation**:
```
Current: Certificate contains 2f+1 separate Ed25519 signatures (64 bytes each)
Improvement: Single aggregated BLS signature (~96 bytes total)

Benefits:
  - Smaller certificates (~90% reduction)
  - Lower bandwidth usage
  - Faster verification (single pairing check)

Trade-offs:
  - Slower signature generation
  - More complex implementation
  - Additional dependency (BLS library)

Timeline: Q2 2025
```

**2. Batch Compression**:
```
Current: Batches stored uncompressed
Improvement: Compress batches with zstd or similar

Benefits:
  - Smaller storage footprint (~50-70% reduction)
  - Lower network bandwidth usage
  - Faster disk I/O (less data to read/write)

Trade-offs:
  - CPU overhead for compression/decompression
  - Increased memory usage during compression

Timeline: Q3 2025
```

**3. Adaptive Worker Scaling**:
```
Current: Manual scaling based on fixed thresholds
Improvement: ML-based adaptive scaling

Benefits:
  - Better resource utilization
  - Automatic tuning for different workloads
  - Predictive scaling (scale before overload)

Trade-offs:
  - More complex implementation
  - Requires training data
  - Potential for unstable scaling

Timeline: Q4 2025
```

**4. Certificate Sharding**:
```
Current: Single DAG for all certificates
Improvement: Shard DAG by validator or round

Benefits:
  - Better parallelism for large validator sets
  - Reduced lock contention
  - Improved cache locality

Trade-offs:
  - More complex DAG management
  - Cross-shard coordination needed
  - May complicate consensus integration

Timeline: 2026
```

**5. Persistent TxIndex**:
```
Current: TxIndex is in-memory only
Improvement: Add LevelDB-backed TxIndex

Benefits:
  - Survives restarts
  - Enables historical queries
  - Reduces memory usage

Trade-offs:
  - Slower lookups
  - More storage required
  - Additional complexity

Timeline: Q2 2025
```

### 18.2 Scalability Paths

**Vertical Scaling** (single node):
```
Current Limits:
  - 8 workers (CPU-bound)
  - ~250K tx/sec
  - ~100MB RAM base

Scaling Options:
  - Increase worker count (16-32 workers)
  - Optimize batch creation (reduce overhead)
  - Use faster storage (NVMe SSD)
  - Increase batch size (trade latency for throughput)

Expected Improvements:
  - 16 workers: ~400K tx/sec
  - 32 workers: ~500K tx/sec (diminishing returns)
```

**Horizontal Scaling** (multiple nodes):
```
Current: N validators, each running Looseberry
Improvement: Each validator runs multiple Looseberry instances

Architecture:
  Validator 0:
    ├─> Looseberry Instance A (workers 0-7)
    ├─> Looseberry Instance B (workers 8-15)
    └─> Aggregator (combines certificates)

Benefits:
  - Linear scaling with instances
  - Better resource utilization
  - Fault isolation

Challenges:
  - Certificate aggregation complexity
  - Network bandwidth scaling
  - Consensus integration changes

Timeline: 2027+
```

### 18.3 Feature Roadmap

**Q1 2025**:
- ✅ Core implementation complete
- ✅ BFT safety verification
- ✅ Comprehensive testing
- ✅ Production readiness

**Q2 2025**:
- BLS signature aggregation
- Persistent TxIndex
- Improved metrics and monitoring
- Performance tuning

**Q3 2025**:
- Batch compression
- Advanced flow control strategies
- Multi-epoch support
- Archive node mode

**Q4 2025**:
- Adaptive worker scaling
- Enhanced sync protocol
- Formal verification of critical paths
- Production deployment at scale

**2026+**:
- Certificate sharding
- Horizontal scaling support
- Cross-chain certificate validation
- Advanced Byzantine detection

---

## Conclusion

Looseberry is a production-ready, high-performance DAG-based mempool designed for Byzantine fault-tolerant consensus systems. Its architecture prioritizes:

1. **Performance**: 200K+ tx/sec throughput with low latency
2. **Reliability**: BFT safety guarantees, graceful degradation, automatic recovery
3. **Scalability**: Dynamic worker scaling, efficient resource management
4. **Maintainability**: Clean architecture, comprehensive testing, clear interfaces

The separation of transaction dissemination from ordering enables high throughput independent of consensus speed, while certificate-based data availability provides strong BFT guarantees.

For integration guidance, see the [API Documentation](docs/API.md).
For release history, see [CHANGELOG.md](CHANGELOG.md).
For contributing, see [CODE_REVIEW.md](CODE_REVIEW.md).

---

**Document Version**: 1.0.0
**Last Updated**: 2026-02-02
**Authors**: Blockberries Team
