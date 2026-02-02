# Looseberry Architecture

Looseberry is a DAG-based mempool module for high-throughput transaction dissemination, inspired by the Narwhal protocol. It integrates with blockberry for consensus and glueberry for networking.

## Overview

### Design Philosophy

Looseberry separates **transaction dissemination** from **transaction ordering**:

1. **Dissemination (Looseberry)**: Reliably broadcasts transactions to all validators, forming certificates of availability
2. **Ordering (Blockberry/Consensus)**: Orders the certified transaction batches into a deterministic sequence

This separation enables:
- **High throughput**: Workers batch and broadcast transactions in parallel
- **Horizontal scaling**: Add workers to increase throughput linearly
- **Consensus independence**: Any consensus protocol can order the certified DAG

### Key Improvements Over Narwhal

| Aspect | Original Narwhal | Looseberry |
|--------|------------------|------------|
| Worker scaling | Fixed worker count | Dynamic scaling based on load |
| Integration | Standalone system | Native blockberry/glueberry integration |
| Storage | RocksDB (Rust) | Uses blockberry's existing stores |
| Networking | Custom TCP | glueberry (libp2p) with encryption |
| Validator set | Static per epoch | Cosmos-SDK staking integration |
| GC strategy | Round-depth based | Consensus-driven with tx recovery |
| Tx ingress | Direct client submission | Passive TransactionsReactor + direct RPC |

## Node Types

Looseberry operates differently depending on whether a node is a validator or a full node.

| Node Type | Looseberry Components | Transaction Handling |
|-----------|----------------------|---------------------|
| **Validator** | Workers + Primary + DAG | Batches txs, participates in DAG protocol |
| **Full Node** | None (DAG read-only) | Uses TransactionsReactor gossip, receives committed batches |

### Validators

Validators run the full Looseberry stack:
- **Workers**: Collect transactions, form batches, broadcast to peer workers
- **Primary**: Create headers, collect votes, form certificates
- **DAG**: Store and order certified vertices

Validators receive transactions through multiple ingress paths (see Transaction Ingress below) and route them to workers for batching.

### Full Nodes

Full nodes do NOT run Looseberry workers or primary. They:
- Use blockberry's existing `TransactionsReactor` to gossip transactions with other full nodes
- Receive committed blocks containing certified batch references
- Can serve client queries about pending transactions

## Architecture Diagram

### System Overview (Validators and Full Nodes)

```
                         ┌─────────────────────────────────────────┐
                         │              Clients                    │
                         │         (Submit transactions)           │
                         └──────────────┬──────────────────────────┘
                                        │ broadcast_tx RPC
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
┌───────────────────────────┐ ┌───────────────────────────┐ ┌─────────────────┐
│       Validator 1         │ │       Validator 2         │ │   Full Node     │
│  ┌─────────────────────┐  │ │  ┌─────────────────────┐  │ │  ┌───────────┐  │
│  │    Looseberry       │  │ │  │    Looseberry       │  │ │  │  Simple   │  │
│  │  Workers + Primary  │◄─┼─┼─▶│  Workers + Primary  │  │ │  │  Mempool  │  │
│  └─────────────────────┘  │ │  └─────────────────────┘  │ │  └───────────┘  │
│  ┌─────────────────────┐  │ │  ┌─────────────────────┐  │ │  ┌───────────┐  │
│  │  TransactionsRx     │  │ │  │  TransactionsRx     │  │ │  │   TxRx    │  │
│  │  (passive mode)     │◄─┼─┼──┼──(passive mode)     │◄─┼─┼──│  (active) │  │
│  └─────────────────────┘  │ │  └─────────────────────┘  │ │  └───────────┘  │
└───────────────────────────┘ └───────────────────────────┘ └─────────────────┘
            │                             │                         │
            └─────────────────────────────┘                         │
                   DAG Protocol (batches, headers, certs)           │
                                                                    │
                         ◄──────────────────────────────────────────┘
                              Tx Gossip (full nodes → validators)
```

### Validator Node Detail

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Application                                     │
│                    (Consensus Engine via Blockberry)                        │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ ReapCertifiedBatches()
                                  │ NotifyCommitted(round)
                                  │ UpdateValidatorSet()
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                              Looseberry                                      │
│                         (DAG-Based Mempool)                                 │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          Public API                                     │ │
│  │  AddTx() | ReapCertifiedBatches() | NotifyCommitted() | GetMetrics()   │ │
│  └────────────────────────────────┬───────────────────────────────────────┘ │
│                                   │                                          │
│  ┌────────────────┐  ┌────────────▼────────────┐  ┌─────────────────────┐   │
│  │    Workers     │  │        Primary          │  │    Certificate      │   │
│  │    (1-N)       │  │                         │  │      Store          │   │
│  │                │  │  ┌─────────────────┐    │  │                     │   │
│  │ ┌────────────┐ │  │  │ Header Builder  │    │  │  Certified DAG      │   │
│  │ │  Worker 1  │─┼──┼─▶│                 │    │  │  vertices indexed   │   │
│  │ └────────────┘ │  │  │ Batch digests + │    │  │  by round           │   │
│  │ ┌────────────┐ │  │  │ Parent certs    │    │  │                     │   │
│  │ │  Worker 2  │─┼──┼─▶│                 │    │  └──────────┬──────────┘   │
│  │ └────────────┘ │  │  └────────┬────────┘    │             │              │
│  │      ...       │  │           │             │             │              │
│  │ ┌────────────┐ │  │  ┌────────▼────────┐    │             │              │
│  │ │  Worker N  │─┼──┼─▶│  Vote Tracker   │────┼─────────────┘              │
│  │ └────────────┘ │  │  │                 │    │                            │
│  └───────┬────────┘  │  │  Collects 2f+1  │    │                            │
│          │           │  │  signatures     │    │                            │
│          │           │  └─────────────────┘    │                            │
│          │           └─────────────────────────┘                            │
│          │                                                                   │
│  ┌───────▼─────────────────────────────────────────────────────────────────┐│
│  │                         Batch Store                                      ││
│  │                                                                          ││
│  │  Pending batches awaiting certification                                  ││
│  │  Certified batches awaiting consensus ordering                           ││
│  └──────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐│
│  │                      Validator Set Manager                               ││
│  │                                                                          ││
│  │  Current validators + voting power                                       ││
│  │  Epoch tracking                                                          ││
│  │  f = (n-1)/3 calculation                                                 ││
│  └──────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                              Glueberry                                       │
│                         (P2P Networking)                                    │
│                                                                              │
│  Streams:                                                                    │
│  ├── looseberry-batches    (Worker ↔ Worker batch streaming)               │
│  ├── looseberry-headers    (Primary ↔ Primary header/vote exchange)        │
│  └── looseberry-sync       (Certificate synchronization)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Transaction Ingress

This section describes how transactions enter validators for inclusion in batches.

### Two-Stage Model

Transaction handling is split into two stages:

1. **Ingress**: How transactions enter a validator node
2. **Dissemination**: How transactions spread among validators

Looseberry replaces Stage 2 (dissemination via batch protocol), but Stage 1 (ingress) uses existing blockberry infrastructure.

### Ingress Paths for Validators

Validators receive transactions through multiple paths, all routed to Looseberry workers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Validator Node                                     │
│                                                                              │
│   ┌─────────────────┐                                                        │
│   │   Client RPC    │──────────────────────┐                                │
│   │  (broadcast_tx) │                      │                                │
│   └─────────────────┘                      │                                │
│                                            ▼                                │
│   ┌─────────────────┐              ┌──────────────┐      ┌───────────────┐  │
│   │ TransactionsRx  │──────────────▶   AddTx()    │─────▶│    Worker     │  │
│   │ (receive-only)  │              │              │      │    Pool       │  │
│   └─────────────────┘              └──────────────┘      └───────┬───────┘  │
│          ▲                                                       │          │
│          │                                                       ▼          │
│   ┌──────┴────────┐                                      ┌───────────────┐  │
│   │  Full Nodes   │                                      │  Batch +      │  │
│   │  (gossip)     │                                      │  Broadcast    │  │
│   └───────────────┘                                      └───────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Path 1: Direct Client RPC**
- Client calls `broadcast_tx` RPC on validator
- Validator routes to `Looseberry.AddTx()`
- Transaction assigned to worker based on hash

**Path 2: TransactionsReactor (Passive Mode)**
- Full nodes gossip transactions to validators
- Validator's TransactionsReactor receives but does NOT re-gossip
- Received transactions routed to `Looseberry.AddTx()`

### TransactionsReactor: Passive Mode

When Looseberry is active on a validator, the TransactionsReactor operates in **receive-only mode**:

```go
// In blockberry/handlers/transactions.go

type TransactionsReactor struct {
    // ... existing fields ...

    // When true, reactor receives txs but does not initiate gossip
    // Validators with Looseberry set this to true
    passiveMode bool

    // Looseberry integration - route received txs here instead of simple mempool
    looseberry *looseberry.Looseberry
}

// gossipLoop only runs if not in passive mode
func (r *TransactionsReactor) gossipLoop() {
    if r.passiveMode {
        return  // Don't initiate gossip requests
    }
    // ... existing gossip logic ...
}

// handleTransactionDataResponse routes to Looseberry when available
func (r *TransactionsReactor) handleTransactionDataResponse(peerID peer.ID, data []byte) error {
    // ... existing validation ...

    for _, txData := range resp.Transactions {
        // ... hash verification ...

        if r.looseberry != nil {
            // Route to Looseberry workers
            if err := r.looseberry.AddTx(txData.Data); err != nil {
                continue
            }
        } else {
            // Fallback to simple mempool (non-validator nodes)
            if err := r.mempool.AddTx(txData.Data); err != nil {
                continue
            }
        }
    }

    return nil
}
```

### Configuration

```go
type TransactionsReactorConfig struct {
    // Existing fields...

    // PassiveMode disables outbound tx gossip
    // Set to true for validators running Looseberry
    PassiveMode bool
}
```

### Full Node Behavior

Full nodes (non-validators) continue using TransactionsReactor normally:
- Receive transactions from clients via RPC
- Gossip transactions to peers (including validators)
- Maintain local mempool for query serving
- Receive committed blocks and update mempool

### Summary Table

| Component | Validators | Full Nodes |
|-----------|-----------|------------|
| Client RPC → AddTx | ✅ Routes to Worker | ✅ Adds to mempool |
| TransactionsReactor receive | ✅ Routes to Worker | ✅ Adds to mempool |
| TransactionsReactor gossip | ❌ Disabled (passive) | ✅ Enabled |
| Looseberry Workers | ✅ Batch & broadcast | ❌ Not running |
| Looseberry Primary | ✅ Headers & certs | ❌ Not running |
| Simple Mempool | ❌ Replaced by Looseberry | ✅ Active |

## Core Components

### 1. Looseberry (Main Coordinator)

The main entry point that orchestrates all components.

```go
type Looseberry struct {
    // Configuration
    cfg          *Config
    validatorSet ValidatorSet

    // Core components
    workers      *WorkerPool
    primary      *Primary
    dag          *DAG
    batchStore   *BatchStore
    certStore    *CertificateStore

    // Transaction validation
    // This wraps the Application's CheckTx method
    // Called before adding transactions to workers for batching
    // MUST be deterministic across all validators
    txValidator  TxValidator

    // Network
    network      Network  // glueberry adapter

    // Lifecycle
    running      atomic.Bool
    stopCh       chan struct{}
}

// TxValidator validates transactions before batching
// This is a wrapper around the Application's CheckTx method
// In Raspberry integration:
//   cfg.TxValidator = app.CheckTx
type TxValidator func(tx []byte) error

// Core interface for blockberry integration
type DAGMempool interface {
    // Transaction submission (calls TxValidator before accepting)
    AddTx(tx []byte) error

    // Pull certified batches for block building
    // Returns batches in deterministic order: by round ASC, then by validator index ASC
    // Only returns batches from rounds > lastCommittedRound
    ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

    // Notify that consensus has committed up to a round
    // Enables garbage collection of older rounds
    NotifyCommitted(round uint64)

    // Update validator set (epoch change)
    UpdateValidatorSet(validators ValidatorSet)

    // Standard mempool methods for compatibility
    HasTx(hash []byte) bool
    Size() int
    SizeBytes() int64
    Flush()

    // Get current DAG round
    CurrentRound() uint64
}
```

### 2. Workers

Workers collect transactions, form batches, and broadcast to peer workers.

```go
type Worker struct {
    id         uint16
    validatorID uint16

    // Pending transactions (deduplicated)
    pending      []Transaction
    pendingSet   map[Hash]bool  // For O(1) deduplication
    pendingMu    sync.Mutex
    pendingBytes int64

    // Limits
    batchSize      int           // Max txs per batch (default: 500)
    batchTimeout   time.Duration // Max wait before creating batch (default: 100ms)
    maxBatchBytes  int64         // Max bytes per batch (default: 512KB)
    maxPendingTxs  int           // Max pending before back-pressure (default: 10000)
    maxPendingBytes int64        // Max pending bytes (default: 50MB)

    // Acknowledgment tracking
    ackTracker   *AckTracker

    // Output
    batchDigests chan<- BatchDigest  // Send to Primary when quorum reached
}

type WorkerPool struct {
    workers      []*Worker
    workerCount  atomic.Int32

    // Dynamic scaling
    minWorkers   int  // Minimum workers (default: 1)
    maxWorkers   int  // Maximum workers (default: 8)
    scalingState *ScalingState

    // Cross-validator worker routing
    // When validators have different worker counts, route to: workerIdx % peerWorkerCount
    peerWorkerCounts map[uint16]int  // validatorID -> worker count
}

// AddTx adds a transaction with deduplication
func (w *Worker) AddTx(tx Transaction) error {
    w.pendingMu.Lock()
    defer w.pendingMu.Unlock()

    txHash := tx.Hash()

    // Deduplication check
    if w.pendingSet[txHash] {
        return nil  // Already have it, not an error
    }

    // Back-pressure check
    if len(w.pending) >= w.maxPendingTxs || w.pendingBytes >= w.maxPendingBytes {
        return ErrWorkerBackpressure
    }

    w.pending = append(w.pending, tx)
    w.pendingSet[txHash] = true
    w.pendingBytes += int64(tx.Size())
    return nil
}
```

**Batch Creation Trigger** (create when ANY condition is met):
```
if len(pending) >= batchSize:
    createBatch()
else if pendingBytes >= maxBatchBytes:
    createBatch()
else if time.Since(lastBatchTime) >= batchTimeout AND len(pending) > 0:
    createBatch()
```

**Worker Scaling Algorithm**:
```
Every scaling_interval (default: 5s):
  pending_ratio = total_pending_txs / (worker_count * batch_size)

  if pending_ratio > scale_up_threshold (default: 0.8) AND worker_count < max_workers:
    add_worker()
    broadcast_worker_count_update()  // Notify peers of new worker count

  if pending_ratio < scale_down_threshold (default: 0.2) AND worker_count > min_workers:
    drain_worker(lowest_load_worker)  // Wait for pending batches to complete
    remove_worker()
    broadcast_worker_count_update()
```

**Worker Count Mismatch Handling**:
```
When sending batch from Worker_i to Validator_j:
  target_worker = i % peer_worker_counts[j]
  send_to(Validator_j, Worker[target_worker])
```

### 3. Primary

The primary creates headers, collects votes, and forms certificates.

```go
type Primary struct {
    validatorID   uint16
    signer        Signer

    // Header creation
    currentRound  atomic.Uint64
    batchDigests  []BatchDigest      // From workers
    parentCerts   []CertificateRef   // From previous round

    // Vote tracking
    voteTracker   *VoteTracker

    // Pending messages (for out-of-order handling)
    pendingVotes  map[Hash][]Vote    // Votes for headers we haven't seen yet
    pendingCerts  map[Hash]*Certificate  // Certs we can't validate yet (missing parents)

    // Thresholds
    headerTimeout time.Duration  // Max wait for batches (default: 500ms)
    maxRoundGap   uint64         // Max rounds ahead we accept (default: 10)

    // Batch availability checker
    batchChecker  BatchAvailabilityChecker

    // Output
    certificates  chan<- Certificate
}

// BatchAvailabilityChecker verifies our workers have the referenced batches
type BatchAvailabilityChecker interface {
    HasBatches(digests []BatchDigest) bool
    RequestMissingBatches(digests []BatchDigest, fromValidator uint16)
}

type Header struct {
    // Identity
    Author      uint16    // Validator index
    Round       uint64    // DAG round number
    Epoch       uint64    // Validator set epoch

    // Content
    BatchRefs   []BatchDigest      // References to transaction batches
    Parents     []CertificateRef   // 2f+1 certs from round-1 (sorted by validator index)

    // Metadata
    Timestamp   int64

    // Computed
    Digest      Hash      // SHA-256(serialized header)
    Signature   Signature // Author's signature
}

type Certificate struct {
    Header     Header
    Votes      []Vote        // 2f+1 votes (sorted by validator index)
    SignerMask BitSet        // Which validators signed
}
```

**Parent Selection Algorithm** (deterministic):
```go
func (p *Primary) selectParents(round uint64) []CertificateRef {
    // Get all certificates from round-1
    certs := p.dag.GetCertificatesForRound(round - 1)

    // Sort by validator index for determinism
    sort.Slice(certs, func(i, j int) bool {
        return certs[i].Header.Author < certs[j].Header.Author
    })

    // Take first 2f+1 (or all if less)
    quorum := p.validatorSet.Quorum()
    if len(certs) > quorum {
        certs = certs[:quorum]
    }

    // Convert to refs
    refs := make([]CertificateRef, len(certs))
    for i, c := range certs {
        refs[i] = CertificateRef{Digest: c.Digest(), Round: c.Header.Round}
    }
    return refs
}
```

**Empty Headers**: When headerTimeout elapses with no batches, create header with empty BatchRefs. This maintains DAG progress for liveness but should be rare under normal load.

### 4. DAG (Directed Acyclic Graph)

The DAG stores certified vertices and maintains causal relationships.

```go
type DAG struct {
    // Round-indexed storage
    rounds     map[uint64]*RoundData
    roundsMu   sync.RWMutex

    // Tracking
    highestRound   atomic.Uint64
    committedRound atomic.Uint64

    // GC
    gcDepth    int  // Rounds to keep after commit (default: 50)
}

type RoundData struct {
    round        uint64
    certificates map[Hash]*Certificate  // By author's cert digest
    committed    bool
}

// Causal ordering - returns all certificates causally before the given one
func (d *DAG) CausalHistory(cert *Certificate) []*Certificate
```

### 5. Batch Store

Stores transaction batches with their certification status.

```go
type BatchStore struct {
    // Pending batches (not yet certified)
    pending     map[Hash]*Batch

    // Certified batches (available for consensus)
    certified   map[Hash]*CertifiedBatch

    // Index for efficient lookup
    txIndex     map[Hash]Hash  // tx hash -> batch hash

    // Metrics
    totalBytes  atomic.Int64
}

type Batch struct {
    WorkerID    uint16
    ValidatorID uint16
    Round       uint64
    Transactions [][]byte
    Digest      Hash
    Timestamp   int64
}

type CertifiedBatch struct {
    Batch       *Batch
    Certificate *Certificate  // The cert that includes this batch
}
```

### 6. Validator Set Manager

Manages the current validator set and epoch transitions.

```go
type ValidatorSet interface {
    // Validator count
    Count() int

    // Get validator by index
    GetByIndex(index uint16) *Validator

    // Check if index is valid
    Contains(index uint16) bool

    // Byzantine fault tolerance threshold: f = (n-1)/3
    F() int

    // Quorum size: 2f+1
    Quorum() int

    // Current epoch
    Epoch() uint64

    // Verify signature from validator
    VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool
}

type Validator struct {
    Index     uint16
    PublicKey PublicKey
    Power     int64      // Voting power (for weighted voting)
    Address   string     // Network address
}
```

## Network Protocol

### Streams

Looseberry requires three new streams on glueberry:

| Stream Name | Protocol ID | Purpose |
|-------------|-------------|---------|
| `looseberry-batches` | `/looseberry/batches/1.0.0` | Worker-to-worker batch dissemination |
| `looseberry-headers` | `/looseberry/headers/1.0.0` | Primary-to-primary headers and votes |
| `looseberry-sync` | `/looseberry/sync/1.0.0` | Certificate synchronization |

### Message Types

```go
// Batch stream messages (Worker ↔ Worker)
const (
    TypeIDBatch        cramberry.TypeID = 200  // Full batch data
    TypeIDBatchAck     cramberry.TypeID = 201  // Acknowledgment
    TypeIDBatchRequest cramberry.TypeID = 202  // Request missing batch
)

// Header stream messages (Primary ↔ Primary)
const (
    TypeIDHeader       cramberry.TypeID = 210  // Header proposal
    TypeIDVote         cramberry.TypeID = 211  // Vote on header
    TypeIDCertificate  cramberry.TypeID = 212  // Formed certificate
)

// Sync stream messages
const (
    TypeIDSyncRequest  cramberry.TypeID = 220  // Request certs by round
    TypeIDSyncResponse cramberry.TypeID = 221  // Batch of certificates
)
```

### Protocol Flow

#### Batch Dissemination (Workers)

```
Worker_i creates batch B:
  1. Worker_i broadcasts B to all Worker_j (j ≠ i) at same worker index
  2. Each Worker_j stores B and sends BatchAck to Worker_i
  3. When Worker_i receives 2f+1 acks:
     - Batch is "available" (certified storage)
     - Worker_i sends BatchDigest to Primary_i
```

#### Header/Certificate Formation (Primaries)

```
Primary_i creates header H:
  1. Wait for:
     - At least one BatchDigest from workers, OR
     - headerTimeout elapsed (create empty header for liveness)
  2. Select 2f+1 parent certificates from round-1 (sorted by validator index)
  3. Sign H and broadcast to all Primary_j

Primary_j receives H from Primary_i:
  1. Validate H:
     a. Valid signature from author
     b. Round within acceptable range: currentRound <= H.Round <= currentRound + maxRoundGap
     c. Has 2f+1 valid parent certs (verify signatures)
     d. First header from this author for this round (no equivocation)
     e. Epoch matches current epoch
  2. Check batch availability:
     - For each BatchRef in H.BatchRefs:
       - If our workers have the batch: continue
       - If missing: request from Primary_i, DEFER voting until received
  3. Once all batches available:
     - Store H
     - Send signed Vote to Primary_i

Primary_i collects votes:
  1. When 2f+1 votes received for H:
     - Sort votes by validator index (deterministic)
     - Form Certificate C = (H, sorted_votes, signer_mask)
     - Broadcast C to all primaries
     - Store C in DAG
     - Advance to round+1 if we have 2f+1 certs for current round

Out-of-order message handling:
  - Vote for unknown header: buffer in pendingVotes (expire after 30s)
  - Certificate with missing parents: buffer in pendingCerts, request parents
  - Header for future round: buffer if within maxRoundGap, else drop
```

## Transaction Lifecycle

```
1. Client submits tx via AddTx()
   │
   ▼
2. Validate tx via TxValidator (Application.CheckTx)
   │ ├── Invalid → return error, tx rejected
   │ └── Valid → continue
   ▼
3. Route to Worker (hash-based: workerIdx = hash(tx) % workerCount)
   │
   ▼
4. Worker deduplicates and queues tx
   │ ├── Duplicate → ignore (not error)
   │ ├── Back-pressure → return ErrWorkerBackpressure
   │ └── OK → add to pending
   ▼
5. Worker batches txs (when ANY limit hit: count, bytes, or timeout)
   │
   ▼
6. Batch broadcast to peer workers (routed by worker index)
   │
   ▼
7. Collect 2f+1 acks → Batch "available"
   │
   ▼
8. BatchDigest sent to Primary
   │
   ▼
9. Primary creates Header with batch refs + 2f+1 parent certs
   │
   ▼
10. Header broadcast to peers
    │
    ▼
11. Peers verify batches available locally, then vote
    │
    ▼
12. Collect 2f+1 votes → Certificate formed → stored in DAG
    │
    ▼
13. Consensus calls ReapCertifiedBatches()
    │ Returns batches ordered by: round ASC, validator index ASC
    ▼
14. Consensus orders and commits block
    │
    ▼
15. NotifyCommitted(round) → GC old rounds, re-inject uncommitted txs
```

### AddTx Implementation

```go
func (l *Looseberry) AddTx(tx []byte) error {
    // Step 1: Validate via application
    if l.txValidator != nil {
        if err := l.txValidator(tx); err != nil {
            return fmt.Errorf("tx validation failed: %w", err)
        }
    }

    // Step 2: Route to worker
    txHash := HashBytes(tx)
    workerIdx := uint16(txHash[0]) % uint16(l.workers.Count())

    // Step 3: Add to worker (handles deduplication and back-pressure)
    return l.workers.Get(workerIdx).AddTx(tx)
}
```

## Round Synchronization

### Round Advancement

A validator advances to round R when it has collected 2f+1 certificates from round R-1:

```go
func (p *Primary) tryAdvanceRound() {
    currentRound := p.currentRound.Load()
    certs := p.dag.GetCertificatesForRound(currentRound)

    if len(certs) >= p.validatorSet.Quorum() {
        p.currentRound.Store(currentRound + 1)
        p.createHeader()  // Start new round immediately
    }
}
```

### Sync Triggers

Synchronization is triggered when:

1. **Round Gap Detected**: Peer's round > our round + syncThreshold (default: 5)
2. **Missing Parents**: Header references certificates we don't have
3. **Periodic Check**: Every syncInterval (default: 10s) compare with peers

### Sync Protocol

```
SyncRequest:
  from_round: uint64    // Lowest round we need
  to_round: uint64      // Highest round we need (or 0 for "latest")

SyncResponse:
  certificates: []Certificate  // Certificates in requested range
  batches: []Batch            // Batches referenced by certificates
```

### Catching Up

When a validator falls behind:

```
1. Detect gap (peer at round 100, we're at round 50)
2. Send SyncRequest{from: 51, to: 100} to multiple peers
3. Receive certificates and batches
4. Validate each certificate:
   - Verify signatures
   - Verify parent references
   - Store in DAG
5. Fast-forward to latest round
6. Resume normal operation
```

### Maximum Round Gap

- **maxRoundGap** (default: 10): Headers more than this many rounds ahead are dropped
- **syncThreshold** (default: 5): Trigger sync when behind by this many rounds
- Validators too far behind must sync before participating

## Startup and Recovery

### Fresh Start

```
1. Load configuration and validator set
2. Initialize empty DAG at round 0
3. Create genesis certificates (empty headers, self-signed by each validator)
4. Start workers and primary
5. Connect to peers
6. Sync if peers are ahead
7. Begin normal operation
```

### Restart Recovery

```
1. Load DAG from persistent storage
2. Determine highest stored round
3. Resume from that round
4. Reconnect to peers
5. Sync any missed rounds
6. Resume normal operation

State to persist:
- All certificates in DAG (within GC depth)
- All batches referenced by those certificates
- Current round number
- Committed round (for GC)
```

### In-Progress Recovery

Operations in progress at crash time are handled as follows:

| State | Recovery Action |
|-------|-----------------|
| Batch broadcast, no acks | Re-broadcast on restart |
| Batch with some acks | Peers will re-ack on reconnect |
| Header sent, collecting votes | Re-broadcast header |
| Votes collected, no cert | Reform certificate from stored votes |

## Epoch Transitions

### Validator Set Changes

When the validator set changes (new epoch):

```
1. Consensus commits block with new validator set
2. Consensus notifies Looseberry: UpdateValidatorSet(newSet)
3. Looseberry:
   a. Complete current round with OLD validator set
   b. Store epoch boundary marker in DAG
   c. Switch to NEW validator set for next round
   d. Recalculate f and quorum
```

### Edge Cases

**Headers at epoch boundary**:
- Header created in epoch N must be voted on by epoch N validators
- Certificate formed with epoch N votes
- After epoch change, headers use epoch N+1

**Votes from removed validators**:
- Votes are only valid if signer is in the header's epoch
- Certificates store epoch, verified against that epoch's validator set

**New validator bootstrap**:
- New validator syncs DAG from existing validators
- Begins participating in next round after sync complete

## Flow Control

### Preventing Unbounded DAG Growth

If Looseberry produces certificates faster than consensus commits:

```go
type FlowController struct {
    maxUncommittedRounds int  // Default: 100

    currentRound    uint64
    committedRound  uint64
}

func (fc *FlowController) CanCreateHeader() bool {
    gap := fc.currentRound - fc.committedRound
    return gap < fc.maxUncommittedRounds
}
```

When limit is reached:
- Primary pauses header creation
- Workers continue batching (up to their limits)
- System waits for consensus to commit
- Resume when gap decreases

## Garbage Collection

### Strategy: Consensus-Driven with TX Recovery

```go
func (l *Looseberry) NotifyCommitted(round uint64) {
    l.dag.SetCommittedRound(round)

    // Determine GC round
    gcRound := round - l.cfg.GCDepth
    if gcRound <= 0 {
        return
    }

    // Extract uncommitted transactions before pruning
    uncommittedTxs := l.extractUncommittedTxs(gcRound)

    // Prune old rounds
    l.dag.PruneBeforeRound(gcRound)
    l.batchStore.PruneBeforeRound(gcRound)
    l.certStore.PruneBeforeRound(gcRound)

    // Re-inject uncommitted transactions
    for _, tx := range uncommittedTxs {
        l.AddTx(tx)  // Will be included in future batches
    }
}
```

### TX Recovery Flow

```
Before pruning round R:
  1. Get all batches from round R
  2. For each batch:
     - Check if batch was in a committed certificate
     - If not, extract all transactions
  3. Prune round R
  4. Re-inject extracted transactions to current workers
```

## Configuration

```go
type Config struct {
    // Identity
    ValidatorIndex uint16
    Signer         Signer

    // Transaction validation
    TxValidator    TxValidator  // Application.CheckTx wrapper

    // Worker configuration
    WorkerConfig WorkerConfig

    // Primary configuration
    PrimaryConfig PrimaryConfig

    // Sync configuration
    SyncConfig SyncConfig

    // Storage
    StorageConfig StorageConfig

    // Network
    NetworkConfig NetworkConfig

    // Garbage collection
    GCConfig GCConfig

    // Flow control
    FlowControlConfig FlowControlConfig
}

type WorkerConfig struct {
    MinWorkers          int           // Default: 1
    MaxWorkers          int           // Default: 8
    BatchSize           int           // Default: 500 transactions
    BatchTimeout        time.Duration // Default: 100ms
    MaxBatchBytes       int64         // Default: 512KB
    MaxPendingTxs       int           // Default: 10000
    MaxPendingBytes     int64         // Default: 50MB
    ScalingInterval     time.Duration // Default: 5s
    ScaleUpThreshold    float64       // Default: 0.8
    ScaleDownThreshold  float64       // Default: 0.2
    DrainTimeout        time.Duration // Default: 30s (for scale-down)
}

type PrimaryConfig struct {
    HeaderTimeout       time.Duration // Default: 500ms
    MaxBatchesPerHeader int           // Default: 100
    MaxRoundGap         uint64        // Default: 10 (max rounds ahead to accept)
    VoteTimeout         time.Duration // Default: 5s (buffer pending votes)
    AllowEmptyHeaders   bool          // Default: true (for liveness)
}

type SyncConfig struct {
    SyncInterval   time.Duration // Default: 10s (periodic sync check)
    SyncThreshold  uint64        // Default: 5 (rounds behind before sync)
    SyncBatchSize  int           // Default: 100 (certs per sync request)
    SyncTimeout    time.Duration // Default: 30s
}

type GCConfig struct {
    GCDepth        int  // Rounds to keep after commit (default: 50)
    RecoverTxs     bool // Re-inject uncommitted txs (default: true)
}

type FlowControlConfig struct {
    MaxUncommittedRounds int  // Default: 100 (pause if exceeded)
}
```

## Integration with Raspberry

Looseberry integrates into the Raspberry blockchain node as the DAG mempool for validators. The integration involves:

### Transaction Validation Flow

```
1. Transaction arrives (RPC or TransactionsReactor)
   ↓
2. Looseberry.AddTx(tx)
   ↓
3. TxValidator(tx)  ← wraps Application.CheckTx()
   ├─ Invalid: reject, return error
   └─ Valid: continue
   ↓
4. Route to Worker based on hash(tx) % workerCount
   ↓
5. Worker batches and broadcasts to peer workers
   ↓
6. Primary creates headers and collects votes
   ↓
7. Certificate formed (2f+1 votes)
   ↓
8. Leaderberry.ReapCertifiedBatches() pulls batches
   ↓
9. Leaderberry executes via Application.ExecuteTx()
```

**Key Point**: Transactions are validated TWICE:
1. **CheckTx** (by Looseberry before batching) - fast validation for mempool admission
2. **ExecuteTx** (by Application after consensus) - full execution with state changes

This two-phase validation ensures:
- Invalid transactions are rejected early (before network broadcast)
- Valid transactions may still fail execution (e.g., insufficient funds discovered during execution)
- Consensus only orders transactions; execution is application's responsibility

### Configuration

```go
// In Raspberry validator startup
app := NewApplication()

looseCfg := looseberry.Config{
    ValidatorIndex: myIndex,

    // IMPORTANT: Wrap application's CheckTx
    TxValidator: func(tx []byte) error {
        ctx := context.Background()
        return app.CheckTx(ctx, tx)
    },

    // ... worker, primary, sync config ...
}

looseMempool, err := looseberry.New(looseCfg, glueNode)
```

## Integration with Blockberry

### Required Changes to Blockberry

#### 1. New Mempool Option

Allow plugging in Looseberry as the mempool implementation:

```go
// In blockberry/node/node.go
func WithLooseberryMempool(lb *looseberry.Looseberry) Option {
    return func(n *Node) {
        n.mempool = lb
        n.dagMempool = lb  // Extended interface
    }
}
```

#### 2. TransactionsReactor Passive Mode

Modify TransactionsReactor to support passive mode for validators:

```go
// In blockberry/handlers/transactions.go

type TransactionsReactor struct {
    // ... existing fields ...

    // Passive mode: receive txs but don't initiate gossip
    passiveMode bool

    // Looseberry integration for validators
    looseberry looseberry.DAGMempool
}

// NewTransactionsReactor with Looseberry support
func NewTransactionsReactorWithLooseberry(
    mempool mempool.Mempool,
    lb looseberry.DAGMempool,  // nil for full nodes
    network *p2p.Network,
    peerManager *p2p.PeerManager,
    requestInterval time.Duration,
    batchSize int32,
) *TransactionsReactor {
    return &TransactionsReactor{
        mempool:         mempool,
        looseberry:      lb,
        passiveMode:     lb != nil,  // Passive if Looseberry is active
        network:         network,
        peerManager:     peerManager,
        requestInterval: requestInterval,
        batchSize:       batchSize,
        // ...
    }
}

// gossipLoop skips gossip in passive mode
func (r *TransactionsReactor) gossipLoop() {
    defer r.wg.Done()

    // Validators with Looseberry don't initiate gossip
    if r.passiveMode {
        return
    }

    ticker := time.NewTicker(r.requestInterval)
    defer ticker.Stop()

    for {
        select {
        case <-r.stop:
            return
        case <-ticker.C:
            r.requestTransactionsFromPeers()
        }
    }
}

// handleTransactionDataResponse routes to Looseberry when available
func (r *TransactionsReactor) handleTransactionDataResponse(peerID peer.ID, data []byte) error {
    var resp schema.TransactionDataResponse
    if err := resp.UnmarshalCramberry(data); err != nil {
        return types.ErrInvalidMessage
    }

    for _, txData := range resp.Transactions {
        if len(txData.Hash) == 0 || len(txData.Data) == 0 {
            continue
        }

        // Verify hash
        computedHash := types.HashTx(txData.Data)
        if string(computedHash) != string(txData.Hash) {
            continue
        }

        // Route to Looseberry (validators) or simple mempool (full nodes)
        if r.looseberry != nil {
            _ = r.looseberry.AddTx(txData.Data)
        } else if r.mempool != nil {
            _ = r.mempool.AddTx(txData.Data)
        }
    }

    return nil
}
```

#### 3. Node Configuration for Validators vs Full Nodes

```go
// In blockberry/node/node.go

func NewNode(cfg *config.Config, opts ...Option) (*Node, error) {
    // ... existing setup ...

    // Apply options to determine if Looseberry is used
    for _, opt := range opts {
        opt(n)
    }

    // Create TransactionsReactor with appropriate mode
    if n.dagMempool != nil {
        // Validator with Looseberry: passive mode
        n.transactionsReactor = handlers.NewTransactionsReactorWithLooseberry(
            nil,  // No simple mempool needed
            n.dagMempool,
            network,
            peerManager,
            5*time.Second,
            100,
        )
    } else {
        // Full node: normal mode with gossip
        n.transactionsReactor = handlers.NewTransactionsReactor(
            n.mempool,
            network,
            peerManager,
            5*time.Second,
            100,
        )
    }

    // ...
}
```

#### 4. Validator Set Notifications

Blockberry must notify Looseberry of validator set changes:

```go
// When epoch changes (in consensus or staking module)
if n.dagMempool != nil {
    n.dagMempool.UpdateValidatorSet(newValidatorSet)
}
```

#### 5. Commit Notifications

Consensus must notify Looseberry when rounds are committed:

```go
// After block commit
if n.dagMempool != nil {
    n.dagMempool.NotifyCommitted(committedRound)
}
```

### Block Building Flow

```go
// In consensus block proposal
func (c *Consensus) ProposeBlock() *Block {
    // Get certified batches from looseberry
    batches := c.looseberry.ReapCertifiedBatches(maxBlockBytes)

    // Build block with batch references
    block := &Block{
        Height:     c.height,
        Batches:    batches,
        // ... other fields
    }

    return block
}

// After block commit
func (c *Consensus) OnBlockCommit(block *Block) {
    // Extract highest round from committed batches
    maxRound := uint64(0)
    for _, batch := range block.Batches {
        if batch.Round > maxRound {
            maxRound = batch.Round
        }
    }

    // Notify looseberry for GC
    c.looseberry.NotifyCommitted(maxRound)
}
```

## Security Considerations

### Byzantine Fault Tolerance

- **Quorum**: 2f+1 where f = (n-1)/3
- **Safety**: A certificate guarantees at least f+1 honest validators have the data
- **Liveness**: With 2f+1 honest validators, protocol progresses

### Attack Mitigations

| Attack | Mitigation |
|--------|------------|
| Equivocation (double-voting) | SignerMask in certificate detects |
| Invalid batches | Hash verification on receive |
| Spam transactions | Application CheckTx validation |
| DoS on workers | Rate limiting per peer |
| Missing data | Pull-based recovery from signers |

### Signature Verification

```go
// Verify header signature
func (p *Primary) verifyHeader(h *Header) error {
    // Check author is valid validator
    if !p.validatorSet.Contains(h.Author) {
        return ErrInvalidAuthor
    }

    // Verify signature
    digest := h.ComputeDigest()
    if !p.validatorSet.VerifySignature(h.Author, digest, h.Signature) {
        return ErrInvalidSignature
    }

    return nil
}

// Verify certificate has valid 2f+1 votes
func (p *Primary) verifyCertificate(c *Certificate) error {
    quorum := p.validatorSet.Quorum()
    validVotes := 0

    for _, vote := range c.Votes {
        if p.validatorSet.VerifySignature(vote.Validator, c.Header.Digest, vote.Signature) {
            validVotes++
        }
    }

    if validVotes < quorum {
        return ErrInsufficientVotes
    }

    return nil
}
```

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Throughput | 100,000+ tx/sec | With 4+ workers |
| Batch latency | <100ms | Time to form batch |
| Certificate latency | <500ms | Time from batch to certificate |
| Memory per round | O(n) | n = validator count |
| Storage | Bounded by GC depth | ~50 rounds retained |

### Scaling Characteristics

```
Throughput ≈ WorkerCount × (BatchSize / BatchTimeout)

Example with defaults:
  4 workers × (500 txs / 100ms) = 20,000 tx/sec per validator

With 10 validators and parallel batching:
  10 × 20,000 = 200,000 tx/sec network throughput
```

## Testing Strategy

### Unit Tests
- Worker batch formation
- Primary header/vote logic
- DAG certificate storage
- GC with tx recovery

### Integration Tests
- Multi-node batch dissemination
- Certificate formation across validators
- Epoch transitions
- Network partition recovery

### Stress Tests
- High transaction load (100k+ tx/sec)
- Worker scaling under load
- GC under continuous operation

### Byzantine Tests
- Equivocating validators
- Missing/delayed messages
- Invalid signatures

## Known Limitations and Future Considerations

### Current Limitations

1. **Certificate Size**: With 100 validators, certificates contain ~67 signatures (~4.3KB). Consider BLS aggregate signatures for networks with many validators.

2. **Single Primary**: Each validator has one Primary. Primary crash requires full node restart. Consider Primary redundancy for high availability.

3. **No Transaction Priority**: All transactions are treated equally. No gas price or priority mechanism. Application can implement via CheckTx rejection.

4. **Fixed Epoch Model**: Validator set changes require epoch boundaries. No hot-swap of individual validators.

5. **No MEV Protection**: Transaction ordering within batches is deterministic but not MEV-resistant. Consider threshold encryption for MEV-sensitive applications.

### Future Enhancements

1. **BLS Aggregate Signatures**: Reduce certificate size from O(n) to O(1) signatures.

2. **Pipelining**: Start round R+1 before R fully completes (Shoal-style optimization).

3. **Adaptive Timeouts**: Adjust batch/header timeouts based on network conditions.

4. **Priority Lanes**: Support transaction priority classes with separate batching.

5. **Partial Synchrony Optimization**: Optimize for common case (synchronous network) while maintaining safety under asynchrony.

### Comparison with Alternatives

| Feature | Looseberry | Narwhal | Mysticeti |
|---------|------------|---------|-----------|
| Certificate model | Explicit 2f+1 sigs | Explicit 2f+1 sigs | Implicit (DAG ancestry) |
| Message delays | 2 (batch + header) | 2 | 1 |
| Worker scaling | Dynamic | Fixed | N/A |
| Storage backend | Pluggable | RocksDB | Custom |
| BFT assumption | n ≥ 3f+1 | n ≥ 3f+1 | n ≥ 3f+1 |

---

## Ecosystem References

Looseberry is part of the Blockberries ecosystem. For complete integration documentation:

- **[../ECOSYSTEM.md](../ECOSYSTEM.md)** - Complete ecosystem architecture and integration guide
- **[../raspberry/ARCHITECTURE.md](../raspberry/ARCHITECTURE.md)** - Blockchain node integrating Looseberry
- **[../blockberry/ARCHITECTURE.md](../blockberry/ARCHITECTURE.md)** - Node framework with DAGMempool interface
- **[../leaderberry/ARCHITECTURE.md](../leaderberry/ARCHITECTURE.md)** - BFT consensus consuming Looseberry batches
- **[../glueberry/ARCHITECTURE.md](../glueberry/ARCHITECTURE.md)** - Encrypted P2P networking for batch dissemination
- **[../punnet-sdk/ARCHITECTURE.md](../punnet-sdk/ARCHITECTURE.md)** - Application module framework
- **[../cramberry/ARCHITECTURE.md](../cramberry/ARCHITECTURE.md)** - Binary serialization
