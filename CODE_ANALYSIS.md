# Looseberry Go Codebase Analysis

## Executive Summary

Looseberry is a sophisticated DAG-based mempool implementation written in Go, featuring a modular, highly concurrent architecture designed for Byzantine fault-tolerant distributed systems. The codebase demonstrates advanced Go patterns including extensive use of atomic operations, careful mutex management, channel-based communication, and interface-driven design.

**Key Architectural Highlights:**
- Lock-free operations using atomic primitives
- Multi-layered concurrency with goroutine pools
- Interface-based abstractions for storage and network
- Comprehensive error categorization (retryable vs Byzantine)
- Flow control and backpressure mechanisms
- Automatic resource scaling and garbage collection

---

## 1. Package-by-Package Deep Dive

### 1.1 `types` Package - Core Data Structures

**Purpose:** Foundational types for the DAG-based consensus protocol.

#### Exported APIs

**Core Types:**
- `Hash` - 32-byte SHA-256 identifier (array-based for value semantics)
- `Transaction` - Raw transaction bytes
- `Batch` - Collection of transactions with metadata
- `Header` - DAG vertex proposal
- `Vote` - Validator vote on a header
- `Certificate` - Header + 2f+1 votes (certified DAG vertex)
- `Signature` - Ed25519 signature
- `PublicKey` - Ed25519 public key

**Interfaces:**
- `Signer` - Cryptographic signing interface
- `ValidatorSet` - Validator set management

#### Data Structure Relationships

```
Certificate
  ├── Header (DAG vertex)
  │     ├── BatchRefs []BatchDigest
  │     ├── Parents []CertificateRef (2f+1 from round-1)
  │     ├── Digest Hash (content hash)
  │     └── Signature (author's signature)
  ├── Votes []Vote (2f+1 votes)
  └── SignerMask BitSet (efficient validator tracking)

Batch
  ├── Transactions []Transaction
  ├── WorkerID, ValidatorID uint16
  ├── Round uint64
  └── Digest Hash
```

#### Concurrency Patterns

**Value Types for Thread-Safety:**
```go
// Array types prevent accidental sharing
type Hash [32]byte
type Signature [64]byte
type PublicKey [32]byte
```

**Immutability via Clone:**
```go
func (b *Batch) Clone() *Batch {
    clone := &Batch{
        WorkerID:     b.WorkerID,
        Transactions: make([]Transaction, len(b.Transactions)),
        // ... copy all fields
    }
    for i, tx := range b.Transactions {
        clone.Transactions[i] = tx.Clone()
    }
    return clone
}
```

#### Error Handling

**Error Categorization:**
```go
// IsRetryable identifies temporary failures
func IsRetryable(err error) bool {
    switch {
    case errors.Is(err, ErrWorkerBackpressure):
        return true  // Transient overload
    case errors.Is(err, ErrFlowControlPaused):
        return true  // Waiting for consensus
    default:
        return false
    }
}

// IsByzantine identifies malicious behavior
func IsByzantine(err error) bool {
    switch {
    case errors.Is(err, ErrInvalidSignature):
        return true  // Cryptographic failure
    case errors.Is(err, ErrDuplicateHeader):
        return true  // Equivocation
    default:
        return false
    }
}
```

**Pattern:** Sentinel errors with `errors.Is()` for error classification.

#### Interface Implementations

**Signer Interface (Ed25519):**
```go
type Ed25519Signer struct {
    privateKey     ed25519.PrivateKey
    publicKey      PublicKey
    validatorIndex uint16
}

func (s *Ed25519Signer) Sign(digest Hash) (Signature, error) {
    sigBytes := ed25519.Sign(s.privateKey, digest[:])
    var sig Signature
    copy(sig[:], sigBytes)
    return sig, nil
}
```

**Pattern:** Concrete implementation of interface with cryptographic primitives.

#### State Management

**BitSet for Efficient Vote Tracking:**
```go
type BitSet []uint64

func (bs BitSet) Set(i int) {
    word := i / 64
    bit := uint(i % 64)
    if word < len(bs) {
        bs[word] |= 1 << bit
    }
}

func (bs BitSet) IsSet(i int) bool {
    word := i / 64
    bit := uint(i % 64)
    if word >= len(bs) {
        return false
    }
    return bs[word]&(1<<bit) != 0
}
```

**Pattern:** Bit manipulation for memory-efficient set operations (supports up to 256 validators with 4 uint64s).

---

### 1.2 `worker` Package - Transaction Batching

**Purpose:** Parallel transaction processing with auto-scaling worker pools.

#### Exported APIs

**Core Types:**
- `Worker` - Single worker batching transactions
- `Pool` - Manager for multiple workers
- `Scaler` - Auto-scaler based on load
- `AckTracker` - Tracks batch acknowledgments for quorum

**Configuration:**
```go
type Config struct {
    BatchSize       int           // Max txs per batch
    BatchBytes      int64         // Max batch size
    BatchTimeout    time.Duration // Max wait before batching
    MaxPendingTxs   int           // Backpressure limit
    MaxPendingBytes int64         // Byte-based backpressure
    AckTimeout      time.Duration // Timeout for acks
}
```

#### Concurrency Patterns

**Worker Structure (Multi-Level Locking):**
```go
type Worker struct {
    id            uint16
    validatorID   uint16
    round         atomic.Uint64  // Lock-free round tracking
    epoch         atomic.Uint64  // Lock-free epoch tracking

    // Pending transactions (mutex-protected)
    pending      []types.Transaction
    pendingSet   map[types.Hash]bool
    pendingBytes int64
    pendingMu    sync.Mutex

    // Storage (thread-safe externally)
    batchStore store.BatchStore
    txIndex    store.TxIndex

    // Tracking (has its own locks)
    ackTracker *AckTracker

    // Lifecycle (atomic boolean)
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
    triggerCh chan struct{} // Buffered channel for immediate batch creation
}
```

**Pattern:** Layered concurrency with atomic operations for hot paths and mutexes for complex state.

**Batch Creation Loop:**
```go
func (w *Worker) batchLoop() {
    defer close(w.stoppedCh)

    ticker := time.NewTicker(w.cfg.BatchTimeout)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            w.tryCreateBatch()
        case <-w.triggerCh:  // Immediate trigger when size limit reached
            w.tryCreateBatch()
        case <-w.stopCh:
            w.tryCreateBatch()  // Final batch before shutdown
            return
        }
    }
}
```

**Pattern:** Timer + trigger channel for both timeout and immediate batch creation.

**Transaction Deduplication:**
```go
func (w *Worker) AddTx(tx types.Transaction) error {
    w.pendingMu.Lock()
    txHash := tx.Hash()

    // Check pending set
    if w.pendingSet[txHash] {
        w.pendingMu.Unlock()
        return nil  // Idempotent - already have it
    }

    // Check tx index (already batched)
    if w.txIndex != nil && w.txIndex.HasTx(txHash) {
        w.pendingMu.Unlock()
        return nil
    }

    // Check backpressure
    if len(w.pending) >= w.cfg.MaxPendingTxs {
        w.pendingMu.Unlock()
        return types.ErrWorkerBackpressure
    }

    // Add to pending
    w.pending = append(w.pending, tx.Clone())
    w.pendingSet[txHash] = true
    w.pendingBytes += int64(tx.Size())

    shouldTrigger := len(w.pending) >= w.cfg.BatchSize ||
        (w.cfg.BatchBytes > 0 && w.pendingBytes >= w.cfg.BatchBytes)

    w.pendingMu.Unlock()

    // Non-blocking trigger
    if shouldTrigger {
        select {
        case w.triggerCh <- struct{}{}:
        default:  // Already has a signal
        }
    }

    return nil
}
```

**Pattern:** Combined hash set + index lookup for deduplication with non-blocking channel signaling.

#### Error Handling

**Storage Failure Recovery:**
```go
func (w *Worker) tryCreateBatch() {
    // ... create batch ...

    // Store batch
    if w.batchStore != nil {
        if err := w.batchStore.SaveBatch(batch); err != nil {
            // Storage failure - requeue to prevent data loss
            w.requeueTransactions(txs)
            return
        }
    }

    // ... continue with success path ...
}

func (w *Worker) requeueTransactions(txs []types.Transaction) {
    w.pendingMu.Lock()
    defer w.pendingMu.Unlock()

    for _, tx := range txs {
        // Re-add with backpressure checks
        if len(w.pending) >= w.cfg.MaxPendingTxs {
            break  // Must drop if at limit
        }
        // ...
    }
}
```

**Pattern:** Graceful degradation on storage failures with transaction preservation.

#### Performance Patterns

**Hash-Based Worker Routing:**
```go
func (p *Pool) AddTx(tx types.Transaction) error {
    p.workersMu.RLock()

    // Use 8 bytes of hash for better distribution
    txHash := tx.Hash()
    hashValue := binary.BigEndian.Uint64(txHash[:8])
    workerIdx := int(hashValue % uint64(len(p.workers)))
    worker := p.workers[workerIdx]

    p.workersMu.RUnlock()
    return worker.AddTx(tx)
}
```

**Pattern:** 64-bit hash distribution prevents hot spots in worker assignment.

**Auto-Scaling:**
```go
type Scaler struct {
    pool *Pool
    cfg  ScalerConfig

    // Scaling state
    lastScaleTime   time.Time
    lastScaleTimeMu sync.Mutex

    // Statistics (lock-free)
    scaleUpCount   atomic.Uint64
    scaleDownCount atomic.Uint64

    // Event history (ring buffer)
    events   []ScaleEvent
    eventsMu sync.Mutex
}

func (s *Scaler) CalculateLoad() float64 {
    workerCount := s.pool.WorkerCount()

    // Count-based load
    pendingCount := s.pool.PendingCount()
    countLoad := float64(pendingCount) / float64(workerCount * batchSize)

    // Byte-based load
    pendingBytes := s.pool.PendingBytes()
    byteLoad := float64(pendingBytes) / float64(workerCount * maxPendingBytes)

    // Return maximum - scale up if EITHER is too high
    return max(countLoad, byteLoad)
}
```

**Pattern:** Multi-dimensional load calculation with cooldown periods to prevent thrashing.

#### Testing Patterns

**Worker Pool Test:**
```go
func TestWorkerPoolScaling(t *testing.T) {
    batchStore := store.NewMemoryBatchStore()
    txIndex := store.NewMemoryTxIndex()
    defer batchStore.Close()
    defer txIndex.Close()

    cfg := PoolConfig{
        MinWorkers: 1,
        MaxWorkers: 4,
        Worker:     DefaultConfig(),
    }

    pool := NewPool(cfg, 0, batchStore, txIndex, 3)
    if err := pool.Start(); err != nil {
        t.Fatalf("Start failed: %v", err)
    }
    defer pool.Stop()

    // Add workers
    if !pool.ScaleUp() {
        t.Error("ScaleUp should succeed")
    }

    if pool.WorkerCount() != 2 {
        t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
    }
}
```

**Pattern:** Table-driven tests with helper functions and proper cleanup using `defer`.

---

### 1.3 `primary` Package - Header Creation & Voting

**Purpose:** DAG vertex creation and certificate formation through voting.

#### Exported APIs

**Core Types:**
- `Primary` - Main coordinator for header/vote/certificate lifecycle
- `VoteTracker` - Tracks votes toward certificate formation
- `BatchFetcher` - Fetches missing batches for headers

**Configuration:**
```go
type Config struct {
    HeaderTimeout       time.Duration // Max wait for header creation
    MaxBatchesPerHeader int           // Limit batch refs per header
    VoteTimeout         time.Duration // Vote collection timeout
    MaxRoundGap         uint64        // Max round difference accepted
    AllowEmptyHeaders   bool          // Enable liveness via empty headers
}
```

#### Concurrency Patterns

**Primary Structure (Complex State Management):**
```go
type Primary struct {
    validatorID uint16
    signer      types.Signer
    cfg         Config

    // Current state (atomic)
    currentRound atomic.Uint64
    epoch        atomic.Uint64

    // Batch digests waiting for next header (mutex)
    batchDigests []types.BatchDigest
    digestsMu    sync.Mutex

    // Vote tracking (has internal locks)
    voteTracker *VoteTracker

    // Pending votes for unknown headers (mutex + timestamp for cleanup)
    pendingVotes   map[types.Hash]*pendingVoteEntry
    pendingMu      sync.Mutex
    maxPendingAge  time.Duration

    // Validator set (RWMutex for read-heavy access)
    validatorSet types.ValidatorSet
    validatorMu  sync.RWMutex

    // Callbacks
    certCallback   CertificateCallback
    headerCallback HeaderCallback
    voteCallback   VoteCallback

    // Lifecycle
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
}
```

**Pattern:** Segregated locks for different state components - atomics for simple counters, mutexes for complex structures, RWMutex for read-heavy data.

**Vote Tracking with Double-Vote Detection:**
```go
type VoteTracker struct {
    pending map[types.Hash]*PendingHeader
    mu      sync.RWMutex

    // Double vote detection
    validatorVotes map[uint16]map[types.Hash]*types.Vote
    doubleVotes    []DoubleVoteEvidence
}

func (vt *VoteTracker) RecordVote(vote *types.Vote, quorum int) (*types.Certificate, bool) {
    vt.mu.Lock()
    defer vt.mu.Unlock()

    pending, exists := vt.pending[vote.HeaderDigest]
    if !exists {
        return nil, false
    }

    // Check for double voting (Byzantine behavior)
    if existingVotes, hasValidator := vt.validatorVotes[vote.Validator]; hasValidator {
        if existingVote, hasHeader := existingVotes[vote.HeaderDigest]; hasHeader {
            if !existingVote.Signature.Equal(vote.Signature) {
                // Record double vote evidence for slashing
                vt.doubleVotes = append(vt.doubleVotes, DoubleVoteEvidence{
                    ValidatorID: vote.Validator,
                    HeaderID:    vote.HeaderDigest,
                    Vote1:       existingVote.Clone(),
                    Vote2:       vote.Clone(),
                    Timestamp:   time.Now(),
                })
            }
            return nil, false  // Already counted
        }
    }

    // Add vote
    pending.Votes[vote.Validator] = vote.Clone()
    vt.validatorVotes[vote.Validator][vote.HeaderDigest] = vote.Clone()

    // Check quorum
    if len(pending.Votes) >= quorum {
        return vt.formCertificateLocked(pending, quorum), true
    }

    return nil, false
}
```

**Pattern:** Comprehensive Byzantine fault detection with evidence collection for accountability.

**Buffered Vote Handling:**
```go
func (p *Primary) HandleVote(vote *types.Vote) (*types.Certificate, bool) {
    // Validate vote signature
    validator := p.validatorSet.GetByIndex(vote.Validator)
    if !vote.Verify(validator.PublicKey) {
        return nil, false
    }

    // Check if we have the header
    if !p.voteTracker.HasHeader(vote.HeaderDigest) {
        // Buffer vote for later
        p.pendingMu.Lock()
        entry := p.pendingVotes[vote.HeaderDigest]
        if entry == nil {
            entry = &pendingVoteEntry{
                votes:     make([]types.Vote, 0),
                createdAt: time.Now(),
            }
            p.pendingVotes[vote.HeaderDigest] = entry
        }
        entry.votes = append(entry.votes, *vote)
        p.pendingMu.Unlock()
        return nil, false
    }

    // Process vote immediately
    return p.voteTracker.RecordVote(vote, quorum)
}
```

**Pattern:** Out-of-order message handling with timestamp-based cleanup to prevent memory leaks.

#### Interface Implementations

**Header Creation Callback Chain:**
```go
// Callback type definitions
type CertificateCallback func(cert *types.Certificate)
type HeaderCallback func(header *types.Header)
type VoteCallback func(vote *types.Vote, to uint16)

// Header loop with periodic creation
func (p *Primary) headerLoop() {
    ticker := time.NewTicker(p.cfg.HeaderTimeout)
    defer ticker.Stop()

    cleanupTicker := time.NewTicker(p.maxPendingAge)
    defer cleanupTicker.Stop()

    for {
        select {
        case <-ticker.C:
            p.tryCreateHeader()
        case <-cleanupTicker.C:
            p.cleanupPendingVotes()
        case <-p.stopCh:
            return
        }
    }
}
```

**Pattern:** Function-based callbacks for loose coupling with external components.

#### State Management

**Parent Selection (Deterministic Ordering):**
```go
func (p *Primary) selectParents(round uint64) []types.CertificateRef {
    if round == 0 {
        return nil  // Genesis round has no parents
    }

    // Get certificates from previous round
    certs, err := p.certStore.GetCertificatesByRound(round - 1)
    if err != nil || len(certs) == 0 {
        return nil
    }

    // Sort by validator index for determinism
    sort.Slice(certs, func(i, j int) bool {
        return certs[i].Author() < certs[j].Author()
    })

    // Take up to quorum (2f+1)
    quorum := p.validatorSet.Quorum()
    if len(certs) > quorum {
        certs = certs[:quorum]
    }

    // Convert to refs
    refs := make([]types.CertificateRef, len(certs))
    for i, c := range certs {
        refs[i] = c.GetRef()
    }

    return refs
}
```

**Pattern:** Deterministic ordering ensures all honest validators create headers with same parent structure.

**Batch Fetcher (Missing Dependency Resolution):**
```go
type BatchFetcher struct {
    // Pending headers waiting for batches
    pendingHeaders   map[types.Hash]*pendingHeaderEntry
    pendingHeadersMu sync.Mutex

    // Pending batch requests
    pendingRequests   map[types.Hash]*pendingBatchRequest
    pendingRequestsMu sync.Mutex

    // Reverse mapping: batch -> headers that need it
    batchToHeaders   map[types.Hash][]types.Hash
    batchToHeadersMu sync.Mutex
}

func (bf *BatchFetcher) NotifyBatchReceived(batch *types.Batch) {
    // Get headers waiting for this batch
    bf.batchToHeadersMu.Lock()
    headerDigests := bf.batchToHeaders[batch.Digest]
    delete(bf.batchToHeaders, batch.Digest)
    bf.batchToHeadersMu.Unlock()

    // Update each header
    var readyHeaders []*types.Header
    bf.pendingHeadersMu.Lock()
    for _, headerDigest := range headerDigests {
        entry := bf.pendingHeaders[headerDigest]
        delete(entry.missingBatch, batch.Digest)

        // If all batches received, header is ready
        if len(entry.missingBatch) == 0 {
            readyHeaders = append(readyHeaders, entry.header)
            delete(bf.pendingHeaders, headerDigest)
        }
    }
    bf.pendingHeadersMu.Unlock()

    // Notify callbacks
    for _, header := range readyHeaders {
        if bf.headerReadyCallback != nil {
            bf.headerReadyCallback(header)
        }
    }
}
```

**Pattern:** Reverse index for efficient dependency resolution with batch notifications.

---

### 1.4 `dag` Package - DAG Structure & Causal Ordering

**Purpose:** Certificate DAG with efficient traversal and caching.

#### Exported APIs

**Core Type:**
```go
type DAG struct {
    rounds   map[uint64]*RoundData  // Certificates by round
    roundsMu sync.RWMutex

    // O(1) certificate lookup by hash
    certIndex   map[types.Hash]*types.Certificate
    certIndexMu sync.RWMutex

    highestRound   atomic.Uint64
    committedRound atomic.Uint64

    certStore store.CertificateStore
    cfg       Config

    // LRU cache for causal history results
    historyCache     map[types.Hash][]*types.Certificate
    historyCacheKeys []types.Hash  // For LRU eviction
    historyCacheMu   sync.RWMutex
}
```

#### Concurrency Patterns

**Multi-Index Data Structure:**
```go
func (d *DAG) AddCertificate(cert *types.Certificate) error {
    d.roundsMu.Lock()
    defer d.roundsMu.Unlock()

    round := cert.Round()

    // Get or create round data
    rd, exists := d.rounds[round]
    if !exists {
        rd = NewRoundData(round)
        d.rounds[round] = rd
    }

    // Check for duplicate
    if rd.HasCertificate(cert.Author()) {
        return types.ErrDuplicateHeader
    }

    // Validate parent certificates exist
    if round > 0 {
        for _, parentRef := range cert.Header.Parents {
            if !d.hasCertificateLocked(parentRef.Digest) {
                return types.ErrMissingParents
            }
        }
    }

    // Add to round data
    rd.AddCertificate(cert)

    // Add to hash index (separate lock)
    d.certIndexMu.Lock()
    d.certIndex[cert.Digest()] = cert
    d.certIndexMu.Unlock()

    // Update highest round (lock-free)
    if round > d.highestRound.Load() {
        d.highestRound.Store(round)
    }

    // Invalidate affected cache entries
    d.invalidateHistoryCacheForCertificate(cert)

    return nil
}
```

**Pattern:** Separate locks for different indices to minimize contention.

#### Performance Patterns

**O(1) Certificate Lookup:**
```go
func (d *DAG) GetCertificate(digest types.Hash) (*types.Certificate, error) {
    // First check hash index (O(1) lookup)
    d.certIndexMu.RLock()
    if cert, ok := d.certIndex[digest]; ok {
        d.certIndexMu.RUnlock()
        return cert.Clone(), nil
    }
    d.certIndexMu.RUnlock()

    // Fall back to persistent storage
    if d.certStore != nil {
        return d.certStore.GetCertificate(digest)
    }

    return nil, types.ErrCertificateNotFound
}
```

**Pattern:** In-memory hash index for hot path with storage fallback.

**Smart Cache Invalidation:**
```go
func (d *DAG) invalidateHistoryCacheForCertificate(newCert *types.Certificate) {
    newRound := newCert.Round()

    d.historyCacheMu.Lock()
    defer d.historyCacheMu.Unlock()

    // Only invalidate entries for certificates at HIGHER rounds
    // (causal history only goes backwards)
    var keysToRemove []types.Hash
    for digest, history := range d.historyCache {
        if len(history) > 0 && history[0].Round() > newRound {
            keysToRemove = append(keysToRemove, digest)
        }
    }

    // Remove affected entries
    for _, digest := range keysToRemove {
        delete(d.historyCache, digest)
    }

    // Update LRU tracking
    if len(keysToRemove) > 0 {
        removedSet := make(map[types.Hash]bool)
        for _, digest := range keysToRemove {
            removedSet[digest] = true
        }

        newKeys := make([]types.Hash, 0, len(d.historyCacheKeys)-len(keysToRemove))
        for _, key := range d.historyCacheKeys {
            if !removedSet[key] {
                newKeys = append(newKeys, key)
            }
        }
        d.historyCacheKeys = newKeys
    }
}
```

**Pattern:** Selective cache invalidation based on causality constraints - more efficient than clearing entire cache.

**Causal History with LRU Cache:**
```go
func (d *DAG) CausalHistory(cert *types.Certificate) []*types.Certificate {
    digest := cert.Digest()

    // Check cache
    d.historyCacheMu.RLock()
    if cached, ok := d.historyCache[digest]; ok {
        d.historyCacheMu.RUnlock()
        return cached
    }
    d.historyCacheMu.RUnlock()

    // BFS traversal
    visited := make(map[types.Hash]bool)
    queue := []*types.Certificate{cert}
    var result []*types.Certificate

    visited[digest] = true
    depth := 0

    for len(queue) > 0 && depth < d.cfg.MaxHistoryDepth {
        levelSize := len(queue)
        for i := 0; i < levelSize; i++ {
            current := queue[0]
            queue = queue[1:]
            result = append(result, current)

            // Add parents to queue
            for _, parentRef := range current.Header.Parents {
                if visited[parentRef.Digest] {
                    continue
                }
                visited[parentRef.Digest] = true

                parent, err := d.GetCertificate(parentRef.Digest)
                if err == nil && parent != nil {
                    queue = append(queue, parent)
                }
            }
        }
        depth++
    }

    // Cache result with LRU eviction
    d.historyCacheMu.Lock()
    if len(d.historyCacheKeys) >= d.cfg.MaxHistoryCacheSize {
        // Evict oldest 10%
        evictCount := d.cfg.MaxHistoryCacheSize / 10
        for i := 0; i < evictCount; i++ {
            oldKey := d.historyCacheKeys[0]
            d.historyCacheKeys = d.historyCacheKeys[1:]
            delete(d.historyCache, oldKey)
        }
    }
    d.historyCache[digest] = result
    d.historyCacheKeys = append(d.historyCacheKeys, digest)
    d.historyCacheMu.Unlock()

    return result
}
```

**Pattern:** BFS traversal with bounded depth and LRU caching for expensive graph operations.

#### State Management

**Round-Based Organization:**
```go
type RoundData struct {
    Round        uint64
    Certificates map[uint16]*types.Certificate  // By validator index
    committed    bool
}

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

**Pattern:** Round-based indexing with deterministic ordering for consensus.

---

### 1.5 `gc` Package - Garbage Collection & Flow Control

**Purpose:** Resource management through garbage collection and backpressure.

#### Exported APIs

**Core Types:**
- `GCManager` - Garbage collects old rounds
- `FlowController` - Prevents unbounded resource growth

**Configuration:**
```go
type Config struct {
    GCDepth               uint64        // Rounds to keep
    GCInterval            time.Duration // GC frequency
    RecoverUncommittedTxs bool          // Enable tx recovery
}

type FlowConfig struct {
    MaxUncommittedRounds uint64  // Max gap: current - committed
    MaxPendingBatches    int     // Batch limit per worker
    MaxPendingHeaders    int     // Header vote tracking limit
}
```

#### Concurrency Patterns

**GC Manager Structure:**
```go
type GCManager struct {
    dag        *dag.DAG
    batchStore store.BatchStore
    certStore  store.CertificateStore
    txIndex    store.TxIndex
    cfg        Config

    // Committed round tracking (atomic)
    committedRound atomic.Uint64
    lastGCRound    atomic.Uint64

    // Uncommitted batch index for fast extraction
    uncommittedBatches   map[types.Hash]uint64
    uncommittedBatchesMu sync.Mutex

    // Transaction recovery callback
    txRecoveryCallback TxRecoveryCallback

    // Metrics (atomic)
    totalGCRuns      atomic.Uint64
    totalGCFailures  atomic.Uint64
    lastGCDuration   atomic.Int64
    totalTxRecovered atomic.Uint64

    // Lifecycle
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
    mu        sync.Mutex  // Protects GC operations
}
```

**Pattern:** Atomic metrics with mutex-protected GC operations.

**Uncommitted Transaction Recovery:**
```go
func (gc *GCManager) extractUncommittedTxs(beforeRound uint64) []types.Transaction {
    var uncommittedTxs []types.Transaction
    var batchesToRemove []types.Hash

    // Use uncommitted batch index for O(n) extraction
    gc.uncommittedBatchesMu.Lock()
    for digest, round := range gc.uncommittedBatches {
        if round < beforeRound {
            batch, err := gc.batchStore.GetBatch(digest)
            if err == nil && batch != nil {
                uncommittedTxs = append(uncommittedTxs, batch.Transactions...)
            }
            batchesToRemove = append(batchesToRemove, digest)
        }
    }

    // Clean up processed entries
    for _, digest := range batchesToRemove {
        delete(gc.uncommittedBatches, digest)
    }
    gc.uncommittedBatchesMu.Unlock()

    // Fallback to scanning if index is empty (e.g., after restart)
    if len(uncommittedTxs) == 0 && len(batchesToRemove) == 0 {
        uncommittedTxs = gc.extractUncommittedTxsFallback(beforeRound)
    }

    return uncommittedTxs
}
```

**Pattern:** Index-based extraction with fallback to full scan for resilience.

**Flow Controller (Lock-Free):**
```go
type FlowController struct {
    cfg FlowConfig

    // All state is atomic
    currentRound   atomic.Uint64
    committedRound atomic.Uint64
    pendingBatches atomic.Int64
    pendingHeaders atomic.Int64
    paused         atomic.Bool

    // Callbacks
    onPause  func()
    onResume func()

    // Metrics
    pauseCount  atomic.Uint64
    resumeCount atomic.Uint64
}

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

    // Check pending headers
    if fc.pendingHeaders.Load() >= int64(fc.cfg.MaxPendingHeaders) {
        shouldPause = true
    }

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

**Pattern:** Completely lock-free flow control using atomic operations with callback notifications.

#### Error Handling

**GC Operation with Retry:**
```go
func (gc *GCManager) performGC(beforeRound uint64) error {
    // Recover uncommitted transactions first
    if gc.cfg.RecoverUncommittedTxs && gc.txRecoveryCallback != nil {
        txs := gc.extractUncommittedTxs(beforeRound)
        if len(txs) > 0 {
            gc.totalTxRecovered.Add(uint64(len(txs)))
            gc.logger.Info("Recovered uncommitted transactions",
                "count", len(txs),
                "before_round", beforeRound,
            )
            gc.txRecoveryCallback(txs)
        }
    }

    // Prune from DAG memory
    gc.dag.PruneRoundsBefore(beforeRound)

    // Delete from persistent storage
    if gc.batchStore != nil {
        if err := gc.batchStore.DeleteBatchesBefore(beforeRound); err != nil {
            return err
        }
    }

    if gc.certStore != nil {
        if err := gc.certStore.DeleteCertificatesBefore(beforeRound); err != nil {
            return err
        }
    }

    // Prune TxIndex
    if gc.txIndex != nil {
        if _, err := gc.txIndex.PruneOlderThan(beforeRound, gc.batchStore); err != nil {
            return err
        }
    }

    // Update last GC'd round
    if beforeRound > 0 {
        gc.lastGCRound.Store(beforeRound - 1)
    }

    return nil
}
```

**Pattern:** Transaction recovery before deletion to prevent data loss.

---

### 1.6 `store` Package - Storage Abstraction

**Purpose:** Interface-based storage with in-memory and persistent implementations.

#### Exported APIs

**Interfaces:**
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

**Pattern:** Minimal interfaces for testability and flexibility.

#### Dependency Injection

**Storage Initialization in Looseberry:**
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

    if l.certStore == nil {
        if l.cfg.Storage.InMemory {
            l.certStore = store.NewMemoryCertificateStore()
        } else {
            cs, err := store.NewLevelDBCertificateStore(l.cfg.Storage.DataDir + "/certs")
            if err != nil {
                return fmt.Errorf("create cert store: %w", err)
            }
            l.certStore = cs
        }
    }

    if l.txIndex == nil {
        l.txIndex = store.NewMemoryTxIndex()
    }

    return nil
}
```

**Pattern:** Lazy initialization with configuration-based selection between implementations.

---

### 1.7 `network` Package - Network Abstraction

**Purpose:** Interface for network communication with mock support.

#### Exported APIs

**Interface:**
```go
type Network interface {
    // Broadcast
    BroadcastBatch(batch *types.Batch) error
    BroadcastHeader(header *types.Header) error
    BroadcastCertificate(cert *types.Certificate) error

    // Point-to-point
    SendVote(validator uint16, vote *types.Vote) error
    SendBatchAck(validator uint16, ack *BatchAckMessage) error
    SendBatchRequest(validator uint16, req *BatchRequestMessage) error
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

**Pattern:** Channel-based message delivery for decoupling.

**Sync Manager:**
```go
type SyncManager struct {
    dag            *dag.DAG
    batchStore     store.BatchStore
    network        Network
    validatorSet   types.ValidatorSet
    validatorSetMu sync.RWMutex
    cfg            SyncConfig

    // Pending sync requests
    pendingRequests   map[uint64]*pendingSyncRequest
    pendingRequestsMu sync.Mutex

    // Lifecycle
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
}

func (sm *SyncManager) checkAndSync() {
    highestRound := sm.dag.HighestRound()

    // Find missing rounds
    for round := uint64(0); round <= highestRound; round++ {
        certs := sm.dag.GetCertificatesForRound(round)
        quorum := sm.validatorSet.Quorum()

        if len(certs) < quorum {
            // Missing certificates, trigger sync
            sm.requestSync(round, highestRound)
            break
        }
    }
}
```

**Pattern:** Periodic sync with gap detection.

---

### 1.8 `looseberry` Package - Main Orchestration

**Purpose:** Top-level coordinator integrating all components.

#### Concurrency Patterns

**Message Processing Loop:**
```go
func (l *Looseberry) messageLoop() {
    defer l.wg.Done()

    for {
        select {
        case <-l.stopCh:
            return

        case msg := <-l.network.BatchMessages():
            l.handleBatchMessage(msg)

        case msg := <-l.network.HeaderMessages():
            l.handleHeaderMessage(msg)

        case msg := <-l.network.VoteMessages():
            l.handleVoteMessage(msg)

        case msg := <-l.network.CertificateMessages():
            l.handleCertificateMessage(msg)

        case msg := <-l.network.BatchAckMessages():
            l.handleBatchAckMessage(msg)

        case req := <-l.network.SyncRequests():
            l.handleSyncRequest(req)

        case msg := <-l.network.SyncResponses():
            l.handleSyncResponse(msg)
        }
    }
}
```

**Pattern:** Select-based multiplexing over multiple channels with graceful shutdown.

**Callback Chain:**
```go
// Worker creates batch
func (l *Looseberry) onBatchCreated(batch *types.Batch) {
    defer recoverCallback("onBatchCreated")

    l.totalBatches.Add(1)

    // Add to primary for header creation
    l.primaryNode.AddBatchDigest(batch.GetDigest())

    // Broadcast to network
    l.network.BroadcastBatch(batch)
}

// Primary creates header
func (l *Looseberry) onHeaderCreated(header *types.Header) {
    defer recoverCallback("onHeaderCreated")

    // Broadcast to network
    l.network.BroadcastHeader(header)
}

// Primary forms certificate
func (l *Looseberry) onCertificateFormed(cert *types.Certificate) {
    defer recoverCallback("onCertificateFormed")

    // Add to DAG
    l.dag.AddCertificate(cert)

    // Update flow control
    l.flowController.UpdateCurrentRound(cert.Header.Round)

    // Broadcast to network
    l.network.BroadcastCertificate(cert)
}

func recoverCallback(callbackName string) {
    if r := recover(); r != nil {
        log.Printf("ERROR: Panic in %s: %v\nStack: %s",
            callbackName, r, debug.Stack())
    }
}
```

**Pattern:** Panic recovery in callbacks to prevent cascade failures.

---

## 2. Go Architecture Patterns

### 2.1 Interface-Based Design

**Abstraction Hierarchy:**
```
DAGMempool (interface)
    └── Looseberry (concrete implementation)
          ├── Network (interface)
          │     └── MockNetwork (test impl)
          ├── BatchStore (interface)
          │     ├── MemoryBatchStore
          │     └── LevelDBBatchStore
          ├── CertificateStore (interface)
          │     ├── MemoryCertificateStore
          │     └── LevelDBCertificateStore
          └── TxIndex (interface)
                └── MemoryTxIndex
```

**Benefits:**
- Easy mocking for tests
- Pluggable storage backends
- Clear boundaries between components

### 2.2 Struct Composition and Embedding

**No Embedding Used:**
The codebase deliberately avoids struct embedding, preferring explicit composition:

```go
type Looseberry struct {
    workerPool     *worker.Pool      // Explicit field
    primaryNode    *primary.Primary  // Explicit field
    dag            *dag.DAG          // Explicit field
    gcManager      *gc.GCManager     // Explicit field
    // ... no embedded types
}
```

**Rationale:** Avoids method shadowing and makes dependencies explicit.

### 2.3 Dependency Injection Patterns

**Constructor Injection:**
```go
func New(
    validatorID uint16,
    signer types.Signer,
    cfg Config,
    certStore store.CertificateStore,
    batchStore store.BatchStore,
    validatorSet types.ValidatorSet,
) *Primary {
    return &Primary{
        validatorID:  validatorID,
        signer:       signer,
        cfg:          cfg,
        certStore:    certStore,
        batchStore:   batchStore,
        validatorSet: validatorSet,
        // ...
    }
}
```

**Setter Injection:**
```go
func (l *Looseberry) SetNetwork(net network.Network) {
    l.mu.Lock()
    defer l.mu.Unlock()
    l.network = net
}

func (l *Looseberry) SetStores(
    batchStore store.BatchStore,
    certStore store.CertificateStore,
    txIndex store.TxIndex,
) {
    l.mu.Lock()
    defer l.mu.Unlock()
    l.batchStore = batchStore
    l.certStore = certStore
    l.txIndex = txIndex
}
```

**Pattern:** Constructor for required deps, setters for optional/test deps.

### 2.4 Factory Patterns

**Default Configuration Factories:**
```go
func DefaultConfig() Config {
    return Config{
        BatchSize:       1000,
        BatchBytes:      512 * 1024,
        BatchTimeout:    100 * time.Millisecond,
        MaxPendingTxs:   10000,
        MaxPendingBytes: 50 * 1024 * 1024,
        AckTimeout:      30 * time.Second,
    }
}
```

**Signer Generation:**
```go
func GenerateEd25519Signer(validatorIndex uint16) (*Ed25519Signer, error) {
    _, privateKey, err := ed25519.GenerateKey(nil)
    if err != nil {
        return nil, fmt.Errorf("generating key: %w", err)
    }
    return NewEd25519Signer(privateKey, validatorIndex)
}
```

**Pattern:** Factories for complex object creation with sensible defaults.

### 2.5 Callback/Observer Patterns

**Function-Based Callbacks:**
```go
type BatchCallback func(batch *types.Batch)
type CertificateCallback func(cert *types.Certificate)
type TxRecoveryCallback func(txs []types.Transaction)

// Setting callbacks
func (p *Primary) SetCertificateCallback(cb CertificateCallback) {
    p.certCallback = cb
}

// Invoking callbacks
if p.certCallback != nil {
    p.certCallback(cert)
}
```

**Pattern:** Optional callbacks for loose coupling with nil checks.

### 2.6 Context Usage for Cancellation

**Pattern Observed:** The codebase does **not** use `context.Context`. Instead, it uses:

1. **Channel-based cancellation:**
   ```go
   stopCh    chan struct{}
   stoppedCh chan struct{}
   ```

2. **Atomic boolean flags:**
   ```go
   running atomic.Bool
   ```

**Shutdown Pattern:**
```go
func (w *Worker) Stop() error {
    if !w.running.Swap(false) {
        return types.ErrNotRunning
    }

    close(w.stopCh)      // Signal goroutines to stop
    <-w.stoppedCh        // Wait for goroutine completion

    w.ackTracker.Close()
    return nil
}
```

**Rationale:** Simpler than context for long-lived goroutines with explicit lifecycle.

---

## 3. Concurrency Analysis

### 3.1 Goroutine Lifecycle Management

**Standard Pattern:**
```go
type Component struct {
    running   atomic.Bool
    stopCh    chan struct{}
    stoppedCh chan struct{}
}

func (c *Component) Start() error {
    if c.running.Swap(true) {
        return types.ErrAlreadyRunning
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
            // Periodic work
        case <-c.stopCh:
            return  // Clean exit
        }
    }
}

func (c *Component) Stop() error {
    if !c.running.Swap(false) {
        return types.ErrNotRunning
    }

    close(c.stopCh)
    <-c.stoppedCh  // Wait for goroutine
    return nil
}
```

**Key Features:**
- Atomic flag prevents double-start/stop
- Channel reset enables restart
- `defer close(stoppedCh)` ensures cleanup
- Blocking wait on `stoppedCh` ensures graceful shutdown

### 3.2 Channel Usage Patterns

**Buffered Channels for Non-Blocking Signals:**
```go
triggerCh chan struct{}  // Buffered to prevent blocking

// Non-blocking send
select {
case w.triggerCh <- struct{}{}:
    // Signal sent
default:
    // Channel already has signal, skip
}
```

**Unbuffered Channels for Synchronization:**
```go
stopCh    chan struct{}   // Unbuffered
stoppedCh chan struct{}   // Unbuffered

close(stopCh)     // Signal stop
<-stoppedCh       // Wait for completion
```

**Channel Multiplexing:**
```go
for {
    select {
    case msg := <-ch1:
        handle1(msg)
    case msg := <-ch2:
        handle2(msg)
    case <-stopCh:
        return
    }
}
```

### 3.3 Synchronization Primitives

**Mutex Patterns:**

1. **Simple Mutex for State:**
   ```go
   mu sync.Mutex

   mu.Lock()
   defer mu.Unlock()
   // Critical section
   ```

2. **RWMutex for Read-Heavy Access:**
   ```go
   mu sync.RWMutex

   // Multiple readers
   mu.RLock()
   defer mu.RUnlock()
   // Read operations

   // Single writer
   mu.Lock()
   defer mu.Unlock()
   // Write operations
   ```

3. **Multiple Independent Mutexes:**
   ```go
   type DAG struct {
       rounds   map[uint64]*RoundData
       roundsMu sync.RWMutex

       certIndex   map[types.Hash]*types.Certificate
       certIndexMu sync.RWMutex

       historyCache     map[types.Hash][]*types.Certificate
       historyCacheMu   sync.RWMutex
   }
   ```

   **Benefit:** Reduces lock contention by protecting independent state separately.

**Atomic Operations:**
```go
type Worker struct {
    round   atomic.Uint64
    epoch   atomic.Uint64
    running atomic.Bool
}

// Lock-free reads/writes
currentRound := w.round.Load()
w.round.Store(newRound)

// Atomic swap
if !w.running.Swap(false) {
    return types.ErrNotRunning
}
```

**WaitGroup for Goroutine Coordination:**
```go
var wg sync.WaitGroup

wg.Add(1)
go func() {
    defer wg.Done()
    // Work
}()

wg.Wait()  // Wait for all goroutines
```

### 3.4 Race Condition Prevention

**Data Isolation:**
```go
func (w *Worker) AddTx(tx types.Transaction) error {
    // Clone on entry to prevent external modification
    w.pending = append(w.pending, tx.Clone())
    // ...
}

func (w *Worker) GetPending() []types.Transaction {
    // Clone on exit to prevent external modification
    txs := make([]types.Transaction, len(w.pending))
    for i, tx := range w.pending {
        txs[i] = tx.Clone()
    }
    return txs
}
```

**Pattern:** Clone data at API boundaries to prevent shared mutable state.

**Read-Modify-Write Protection:**
```go
func (fc *FlowController) checkFlowControl() {
    // Atomic read-modify-write
    if fc.paused.Swap(true) {
        // Was paused before
        return
    }
    // Was not paused, now is - trigger callback
    fc.pauseCount.Add(1)
    if fc.onPause != nil {
        fc.onPause()
    }
}
```

**Pattern:** Use atomic Swap for thread-safe state transitions.

### 3.5 Graceful Shutdown Patterns

**Component Shutdown Order:**
```go
func (l *Looseberry) Stop() error {
    if !l.running.Swap(false) {
        return types.ErrNotRunning
    }

    l.mu.Lock()
    close(l.stopCh)
    l.mu.Unlock()

    // Wait for message loop
    l.wg.Wait()

    // Stop network (without lock to allow callbacks to complete)
    if l.network != nil {
        _ = l.network.Stop()
    }

    // Stop components in reverse order
    l.stopComponents()

    // Now acquire lock to close stores
    l.mu.Lock()
    defer l.mu.Unlock()

    // Close stores
    if l.batchStore != nil {
        l.batchStore.Close()
    }
    // ...

    return nil
}
```

**Pattern:**
1. Stop accepting new work (close channels)
2. Wait for in-flight work (WaitGroup)
3. Stop components in reverse dependency order
4. Close resources (stores, connections)

### 3.6 Thread-Safety Guarantees

**Documentation Pattern:**
```go
// Worker batches transactions and manages their lifecycle.
// Thread-safe with mutex protection.
type Worker struct {
    // ...
}

// VoteTracker tracks pending headers and their votes.
// Thread-safe with RWMutex.
type VoteTracker struct {
    // ...
}
```

**Guarantee Levels:**
1. **Lock-free:** Pure atomic operations (FlowController)
2. **Mutex-protected:** Mutable state with locks (Worker)
3. **Immutable:** Value types, clone on access (Hash, Signature)

---

## 4. Error Handling Analysis

### 4.1 Error Types and Categorization

**Sentinel Errors:**
```go
var (
    ErrTxAlreadyExists    = errors.New("transaction already exists")
    ErrWorkerBackpressure = errors.New("worker back-pressure: too many pending transactions")
    ErrInvalidSignature   = errors.New("invalid signature")
    ErrFlowControlPaused  = errors.New("flow control: DAG production paused, waiting for consensus")
)
```

**Error Classification Functions:**
```go
// IsRetryable returns true if the error is temporary.
func IsRetryable(err error) bool {
    switch {
    case errors.Is(err, ErrWorkerBackpressure):
        return true
    case errors.Is(err, ErrMempoolFull):
        return true
    case errors.Is(err, ErrFlowControlPaused):
        return true
    default:
        return false
    }
}

// IsByzantine returns true if the error indicates Byzantine behavior.
func IsByzantine(err error) bool {
    switch {
    case errors.Is(err, ErrInvalidSignature):
        return true
    case errors.Is(err, ErrDuplicateHeader):
        return true
    default:
        return false
    }
}
```

**Categories:**
1. **Transient/Retryable:** Backpressure, timeouts, flow control
2. **Byzantine/Malicious:** Invalid signatures, duplicates, equivocation
3. **Permanent:** Not found, validation failed

### 4.2 Error Wrapping and Unwrapping

**Pattern:**
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
```

**Benefits:**
- Preserves original error for `errors.Is()` and `errors.As()`
- Adds context at each layer
- Enables error inspection at any level

### 4.3 Error Propagation Strategies

**Immediate Return:**
```go
func (w *Worker) AddTx(tx types.Transaction) error {
    if !w.running.Load() {
        return types.ErrNotRunning
    }

    if w.txValidator != nil {
        if err := w.txValidator(tx); err != nil {
            return types.ErrTxValidationFailed
        }
    }

    // Check backpressure
    if len(w.pending) >= w.cfg.MaxPendingTxs {
        return types.ErrWorkerBackpressure
    }

    return nil
}
```

**Callback Error Handling:**
```go
func (l *Looseberry) onBatchCreated(batch *types.Batch) {
    defer recoverCallback("onBatchCreated")

    // Errors logged but not propagated
    if l.network != nil {
        _ = l.network.BroadcastBatch(batch)
    }
}

func recoverCallback(callbackName string) {
    if r := recover(); r != nil {
        log.Printf("ERROR: Panic in %s: %v\n%s",
            callbackName, r, debug.Stack())
    }
}
```

**Pattern:** Callbacks absorb errors to prevent cascade failures.

**Logging Patterns:**
```go
type Logger interface {
    Error(msg string, keysAndValues ...interface{})
    Info(msg string, keysAndValues ...interface{})
    Debug(msg string, keysAndValues ...interface{})
}

// Usage
gc.logger.Error("Garbage collection failed",
    "round", gcRound,
    "error", err,
    "committed_round", gc.committedRound.Load(),
)
```

**Pattern:** Structured logging with key-value pairs for observability.

### 4.4 Sentinel Errors

**Definition and Usage:**
```go
// Define once
var ErrWorkerBackpressure = errors.New("worker back-pressure: too many pending transactions")

// Check with errors.Is
if err := w.AddTx(tx); errors.Is(err, types.ErrWorkerBackpressure) {
    // Handle backpressure
}

// Works even with wrapping
if err := someFunc(); errors.Is(err, types.ErrWorkerBackpressure) {
    // Still works after fmt.Errorf("wrapper: %w", err)
}
```

---

## 5. Performance Patterns

### 5.1 Memory Allocation Optimization

**Pre-Allocation:**
```go
func (b *Batch) ComputeDigest() Hash {
    // Pre-allocate with capacity
    size := 20 + len(b.Transactions)*HashSize
    data := make([]byte, 0, size)

    // Append operations don't reallocate
    data = append(data, buf[:2]...)
    data = append(data, buf...)
    // ...

    return HashBytes(data)
}
```

**Buffer Reuse:**
```go
func (h *Header) ComputeDigest() Hash {
    data := make([]byte, 0, estimatedSize)
    buf := make([]byte, 8)  // Reused for all fields

    binary.BigEndian.PutUint16(buf[:2], h.Author)
    data = append(data, buf[:2]...)

    binary.BigEndian.PutUint64(buf, h.Round)
    data = append(data, buf...)

    // ... reuse buf for all fields
}
```

**Sync.Pool (Not Used):**
The codebase does not use `sync.Pool`. This is likely because:
- Objects are small and short-lived
- Garbage collector is efficient for this workload
- Simplicity preferred over marginal gains

### 5.2 Caching Strategies

**LRU Cache with Manual Eviction:**
```go
type DAG struct {
    historyCache     map[types.Hash][]*types.Certificate
    historyCacheKeys []types.Hash  // Insertion order for LRU
    cfg              Config
}

func (d *DAG) addToCacheLRU(key types.Hash, value []*types.Certificate) {
    d.historyCacheMu.Lock()
    defer d.historyCacheMu.Unlock()

    // Evict oldest 10% if full
    if len(d.historyCacheKeys) >= d.cfg.MaxHistoryCacheSize {
        evictCount := d.cfg.MaxHistoryCacheSize / 10
        for i := 0; i < evictCount; i++ {
            oldKey := d.historyCacheKeys[0]
            d.historyCacheKeys = d.historyCacheKeys[1:]
            delete(d.historyCache, oldKey)
        }
    }

    d.historyCache[key] = value
    d.historyCacheKeys = append(d.historyCacheKeys, key)
}
```

**Pattern:** Batch eviction (10% at a time) reduces overhead.

**Selective Cache Invalidation:**
```go
func (d *DAG) invalidateHistoryCacheForCertificate(newCert *types.Certificate) {
    newRound := newCert.Round()

    // Only invalidate entries for higher rounds (causality constraint)
    for digest, history := range d.historyCache {
        if len(history) > 0 && history[0].Round() > newRound {
            delete(d.historyCache, digest)
        }
    }
}
```

**Pattern:** Targeted invalidation preserves valid cache entries.

### 5.3 Buffering Patterns

**Channel Buffering:**
```go
// Buffered for non-blocking sends
triggerCh: make(chan struct{}, 1)

// Message channels with backpressure
batchMsgCh: make(chan *BatchMessage, cfg.BufferSize)
```

**Pattern:** Buffer size determines maximum in-flight messages.

**Batch Processing:**
```go
func (w *Worker) tryCreateBatch() {
    // Take up to BatchSize transactions
    count := min(len(w.pending), w.cfg.BatchSize)

    // Also respect byte limit
    if w.cfg.BatchBytes > 0 {
        var batchBytes int64
        for i := 0; i < count; i++ {
            txBytes := int64(w.pending[i].Size())
            if batchBytes+txBytes > w.cfg.BatchBytes && i > 0 {
                count = i
                break
            }
            batchBytes += txBytes
        }
    }

    txs := make([]types.Transaction, count)
    copy(txs, w.pending[:count])
    w.pending = w.pending[count:]
    // ...
}
```

**Pattern:** Batching reduces per-operation overhead.

### 5.4 Lock Contention Mitigation

**Separate Locks for Independent State:**
```go
type DAG struct {
    rounds      map[uint64]*RoundData
    roundsMu    sync.RWMutex

    certIndex   map[types.Hash]*types.Certificate
    certIndexMu sync.RWMutex  // Separate lock!
}

func (d *DAG) AddCertificate(cert *types.Certificate) error {
    d.roundsMu.Lock()
    // Work with rounds
    d.roundsMu.Unlock()

    d.certIndexMu.Lock()  // Different lock
    d.certIndex[cert.Digest()] = cert
    d.certIndexMu.Unlock()

    return nil
}
```

**RWMutex for Read-Heavy Workloads:**
```go
type Primary struct {
    validatorSet types.ValidatorSet
    validatorMu  sync.RWMutex  // RWMutex for many readers
}

// Many concurrent readers
func (p *Primary) HandleVote(vote *types.Vote) {
    p.validatorMu.RLock()
    validator := p.validatorSet.GetByIndex(vote.Validator)
    quorum := p.validatorSet.Quorum()
    p.validatorMu.RUnlock()
    // ...
}

// Rare writer
func (p *Primary) UpdateValidatorSet(vs types.ValidatorSet) {
    p.validatorMu.Lock()
    p.validatorSet = vs
    p.validatorMu.Unlock()
}
```

**Atomic Operations for Hot Paths:**
```go
type Worker struct {
    round   atomic.Uint64  // No lock needed!
    epoch   atomic.Uint64
    running atomic.Bool
}

// Lock-free reads
currentRound := w.round.Load()

// Lock-free writes
w.round.Store(newRound)
```

**Pattern:** Choose right primitive for access pattern - atomics for simple values, RWMutex for reads >> writes, Mutex for balanced.

---

## 6. Testing Patterns

### 6.1 Table-Driven Tests

**Pattern:**
```go
func TestHashFromHex(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        wantErr bool
    }{
        {
            name:    "valid hash",
            input:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            wantErr: false,
        },
        {
            name:    "invalid length",
            input:   "0123456789",
            wantErr: true,
        },
        {
            name:    "invalid hex",
            input:   "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            hash, err := types.HashFromHex(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("expected error=%v, got %v", tt.wantErr, err)
            }
            if !tt.wantErr && hash.IsEmpty() {
                t.Error("expected non-empty hash")
            }
        })
    }
}
```

### 6.2 Mock Implementations

**Mock Network:**
```go
type MockNetwork struct {
    validatorID uint16

    // Message channels
    batchCh   chan *BatchMessage
    headerCh  chan *HeaderMessage
    voteCh    chan *VoteMessage
    certCh    chan *CertificateMessage

    // Sent messages (for verification)
    sentBatches       []*types.Batch
    sentHeaders       []*types.Header
    sentVotes         []*types.Vote
    sentCertificates  []*types.Certificate

    mu sync.Mutex
}

func (m *MockNetwork) BroadcastBatch(batch *types.Batch) error {
    m.mu.Lock()
    m.sentBatches = append(m.sentBatches, batch)
    m.mu.Unlock()

    // Simulate receiving on other validators
    m.batchCh <- &BatchMessage{
        Batch: batch,
        From:  m.validatorID,
    }
    return nil
}
```

**Pattern:** Mock captures both inputs (for verification) and produces outputs (via channels).

### 6.3 Test Helper Functions

**Common Pattern:**
```go
func createTestValidatorSet(t *testing.T, count int) (*types.SimpleValidatorSet, []*types.Ed25519Signer) {
    t.Helper()  // Marks as helper for better error reporting

    signers := make([]*types.Ed25519Signer, count)
    validators := make([]*types.Validator, count)

    for i := 0; i < count; i++ {
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

    return types.NewSimpleValidatorSet(validators, 0), signers
}
```

**Pattern:** Helper functions reduce boilerplate and improve readability.

### 6.4 Integration Test Structure

**Example:**
```go
func TestEndToEnd(t *testing.T) {
    // Setup
    validators, signers := createTestValidators(t, 4)
    networks := createMockNetworks(t, 4)
    nodes := make([]*looseberry.Looseberry, 4)

    for i := 0; i < 4; i++ {
        cfg := looseberry.DefaultConfig()
        cfg.ValidatorIndex = uint16(i)
        cfg.Signer = signers[i]

        node, err := looseberry.New(cfg)
        if err != nil {
            t.Fatalf("Failed to create node %d: %v", i, err)
        }
        node.SetNetwork(networks[i])
        node.SetValidatorSet(validators)
        nodes[i] = node
    }

    // Start all nodes
    for i, node := range nodes {
        if err := node.Start(); err != nil {
            t.Fatalf("Failed to start node %d: %v", i, err)
        }
        defer node.Stop()
    }

    // Test scenario
    tx := []byte("test transaction")
    if err := nodes[0].AddTx(tx); err != nil {
        t.Fatalf("AddTx failed: %v", err)
    }

    // Wait for consensus
    time.Sleep(5 * time.Second)

    // Verify all nodes have the transaction
    for i, node := range nodes {
        if !node.HasTx(types.HashBytes(tx)) {
            t.Errorf("Node %d does not have transaction", i)
        }
    }
}
```

**Pattern:** Full system test with multiple nodes and network simulation.

---

## 7. Key Insights and Best Practices

### 7.1 Concurrency Best Practices

1. **Atomic for Simple Counters:** Use `atomic.Uint64`, `atomic.Bool` for flags and counters
2. **Mutex for Complex State:** Protect complex structures with `sync.Mutex`
3. **RWMutex for Read-Heavy:** Use `sync.RWMutex` when reads >> writes
4. **Separate Locks:** Use independent locks for unrelated state to reduce contention
5. **Clone at Boundaries:** Clone data entering/leaving components to prevent sharing
6. **Channel-Based Cancellation:** Use `chan struct{}` for shutdown signals
7. **Graceful Shutdown:** Wait for goroutines with channels, not context

### 7.2 Error Handling Best Practices

1. **Sentinel Errors:** Define errors as package-level variables
2. **Error Classification:** Provide `IsRetryable()` and `IsByzantine()` functions
3. **Error Wrapping:** Use `fmt.Errorf("context: %w", err)` to preserve errors
4. **Callback Recovery:** Use `defer recover()` in callbacks to prevent cascades
5. **Structured Logging:** Log errors with key-value pairs for observability

### 7.3 Performance Best Practices

1. **Pre-Allocate:** Use `make([]T, 0, capacity)` when size is known
2. **Reuse Buffers:** Reuse temporary buffers within functions
3. **Batch Operations:** Process multiple items together to reduce overhead
4. **Cache Expensive Ops:** Cache results of expensive operations (with LRU eviction)
5. **Smart Invalidation:** Invalidate cache selectively, not wholesale
6. **Hash Indexing:** Use hash maps for O(1) lookups in hot paths

### 7.4 Architecture Best Practices

1. **Interface Segregation:** Define minimal interfaces for testability
2. **Dependency Injection:** Use constructor injection for required dependencies
3. **Callback-Based Decoupling:** Use function callbacks for loose coupling
4. **Component Lifecycle:** Standardize Start/Stop patterns across components
5. **Panic Recovery:** Recover panics in callbacks and background goroutines
6. **Metrics with Atomics:** Track metrics with atomic counters for thread-safety

---

## 8. Summary

The Looseberry codebase demonstrates **production-grade Go programming** with:

- **Sophisticated concurrency** using atomics, mutexes, and channels
- **Clear architectural boundaries** through interfaces and dependency injection
- **Byzantine fault tolerance** with comprehensive error categorization
- **Performance optimization** via caching, batching, and lock-free operations
- **Graceful degradation** with backpressure and flow control
- **Comprehensive testing** with mocks, helpers, and table-driven tests

The design balances **complexity with maintainability** through:
- Consistent patterns across components
- Clear ownership of concurrent state
- Separation of concerns via interfaces
- Observable behavior through structured logging
- Resilience through panic recovery and error handling

This codebase serves as an excellent reference for building **high-performance, fault-tolerant distributed systems in Go**.
