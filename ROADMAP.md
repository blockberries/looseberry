# Looseberry Roadmap

This document outlines future improvements for Looseberry based on a comprehensive codebase review. Items are organized by priority and include specific implementation guidance.

## Executive Summary

Looseberry is a functional DAG-based mempool implementation with solid foundations. However, several areas require attention before production deployment:

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| Safety/Correctness | 8 | 12 | 6 | 4 |
| Performance | 2 | 5 | 8 | 3 |
| Features | 3 | 7 | 10 | 5 |
| Operations | 1 | 4 | 6 | 3 |

**Production Readiness Assessment: 65%**

---

## Phase 1: Critical Issues

These must be resolved before any production deployment.

### 1.1 Data Loss Prevention

#### Worker Batch Creation Triggers
**Location:** `worker/worker.go:277-330`
**Issue:** Batches are only created on timeout, not when size/byte limits are reached.
**Impact:** Under high load, transactions accumulate indefinitely until timeout.

```
Current:  Create batch ONLY on BatchTimeout tick
Required: Create batch when ANY condition is met:
          - len(pending) >= BatchSize
          - pendingBytes >= MaxBatchBytes
          - BatchTimeout elapsed
```

**Implementation:**
1. Add channel signal for immediate batch creation
2. Check limits in `AddTx()` after successful add
3. Signal batch creation when threshold crossed

---

#### Worker Storage Failure Handling
**Location:** `worker/worker.go:312-314`
**Issue:** If `SaveBatch()` fails, transactions are lost silently.
**Impact:** Data loss under storage pressure.

```go
// Current (BROKEN)
if err := w.batchStore.SaveBatch(batch); err != nil {
    return  // Transactions LOST
}

// Required
if err := w.batchStore.SaveBatch(batch); err != nil {
    log.Error("batch save failed, re-queueing transactions", "err", err)
    w.requeue(batch.Transactions)
    return
}
```

---

#### Pool Scale-Down Transaction Loss
**Location:** `worker/pool.go:294-300`
**Issue:** Removing a worker doesn't drain pending transactions properly.
**Impact:** Transactions lost during scale-down operations.

**Implementation:**
1. Add `Drain()` method to Worker
2. Call `Drain()` before `Stop()` in scale-down
3. Re-inject drained transactions to remaining workers

---

#### TxIndex Never Pruned
**Location:** `gc/gc.go` (missing call to `txIndex.RemoveTxsForBatch`)
**Issue:** Transaction index grows unbounded as it's never cleaned during GC.
**Impact:** Memory leak proportional to transaction history.

**Implementation:**
Add to `performGC()`:
```go
// After extracting uncommitted txs, before deleting batches
for _, batch := range batchesToDelete {
    if err := gc.txIndex.RemoveTxsForBatch(batch.Digest); err != nil {
        log.Warn("failed to remove tx index entries", "err", err)
    }
}
```

---

### 1.2 Safety Violations

#### No Vote Timeout Enforcement
**Location:** `primary/primary.go`
**Issue:** VoteTracker has timeout methods but Primary never calls them.
**Impact:** Orphaned votes accumulate indefinitely; memory leak.

**Implementation:**
1. Add periodic cleanup in Primary's header loop
2. Call `voteTracker.RemoveTimedOut()` every VoteTimeout/2
3. Log timed-out headers for debugging

---

#### Double Voting Not Detected
**Location:** `primary/vote_tracker.go`
**Issue:** A validator can vote for multiple conflicting headers in the same round.
**Impact:** Byzantine behavior goes undetected.

**Implementation:**
1. Add `votedHeaders map[uint16]map[uint64]types.Hash` (validator → round → headerDigest)
2. Check before recording vote
3. Return `ErrEquivocation` if validator already voted for different header

---

#### No Parent Validation in DAG
**Location:** `dag/dag.go:AddCertificate`
**Issue:** Parents aren't validated to exist or be from correct round.
**Impact:** Malformed DAG could be constructed.

**Implementation:**
```go
func (d *DAG) AddCertificate(cert *types.Certificate) error {
    // Validate parents exist and are from round-1
    for _, parent := range cert.Header.Parents {
        if parent.Round != cert.Header.Round-1 {
            return ErrInvalidParentRound
        }
        if !d.HasCertificate(parent.Digest) {
            return ErrMissingParent
        }
    }
    // ... existing logic
}
```

---

#### Silent Error Handling in GC Loop
**Location:** `gc/gc.go:219`
**Issue:** `performGC()` errors are silently discarded in the background loop.
**Impact:** GC failures go unnoticed; storage bloat.

**Implementation:**
```go
// Current
_ = gc.performGC(gcRound)

// Required
if err := gc.performGC(gcRound); err != nil {
    log.Error("GC failed", "round", gcRound, "err", err)
    // Optionally: metrics.GCFailures.Inc()
}
```

---

### 1.3 Missing Persistence

#### No LevelDB TxIndex Implementation
**Location:** `store/` (missing file)
**Issue:** Only in-memory TxIndex exists; transaction lookups lost on restart.
**Impact:** Cannot resume transaction state after crash.

**Implementation:**
1. Create `store/leveldb_txindex.go`
2. Key schema: `T:{txHash}` → `{batchHash}`
3. Reverse index: `TB:{batchHash}:{txHash}` → empty (for batch removal)
4. Implement all `TxIndex` interface methods

---

## Phase 2: High Priority Improvements

### 2.1 Correctness Improvements

#### Incomplete Flow Control
**Location:** `gc/flow.go`
**Issue:** `MaxPendingBatches` and `MaxPendingHeaders` are defined but never enforced.
**Impact:** Unbounded memory growth in pending queues.

**Implementation:**
1. Add batch/header counting to FlowController
2. Add `CanCreateBatch()` and `CanCreateHeader()` methods
3. Integrate checks in Worker and Primary

---

#### Hash-Based Routing Weakness
**Location:** `worker/pool.go:158`
**Issue:** Uses only first byte of hash for worker routing.
**Impact:** Severe load imbalance with certain transaction patterns.

```go
// Current (weak)
workerIdx := int(txHash[0]) % len(p.workers)

// Better (uses more entropy)
workerIdx := binary.BigEndian.Uint64(txHash[:8]) % uint64(len(p.workers))
```

---

#### Scaler Load Calculation Incomplete
**Location:** `worker/scaler.go:134-148`
**Issue:** Only considers transaction count, not byte capacity.
**Impact:** Wrong scaling decisions under byte-heavy workloads.

**Implementation:**
```go
func (s *Scaler) calculateLoad() float64 {
    txLoad := float64(pending) / float64(workers * batchSize)
    byteLoad := float64(pendingBytes) / float64(workers * maxPendingBytes)
    return max(txLoad, byteLoad)  // Use whichever is higher
}
```

---

#### No Hysteresis in Scaling
**Location:** `worker/scaler.go:201-205`
**Issue:** Binary threshold causes oscillation near boundaries.
**Impact:** Scaling thrashing under fluctuating load.

**Implementation:**
- Scale up when load > 0.8
- Scale down only when load < 0.3 (not 0.2)
- Creates stability band between 0.3-0.8

---

#### Missing Batch Availability Verification
**Location:** `primary/primary.go:HandleHeader`
**Issue:** Silently skips voting on headers with missing batches instead of requesting them.
**Impact:** Certificate starvation if batches are incomplete.

**Implementation:**
1. Add `BatchRequestMessage` handling
2. Request missing batches from header author
3. Buffer header until batches received
4. Timeout and reject if batches not received

---

### 2.2 Performance Optimizations

#### O(n) Certificate Lookup in DAG
**Location:** `dag/dag.go:156-165`
**Issue:** `GetCertificate()` scans all rounds to find by digest.
**Impact:** O(rounds × validators) lookup; consensus bottleneck.

**Implementation:**
1. Add `digestToRound map[types.Hash]uint64`
2. Update on `AddCertificate()`
3. Direct lookup: O(1) instead of O(n)

---

#### Unbounded History Cache
**Location:** `dag/dag.go` (historyCache)
**Issue:** Causal history cache grows without limit.
**Impact:** Memory exhaustion at scale.

**Implementation:**
1. Add LRU eviction policy
2. Limit to `MaxCachedRounds * validators` entries
3. Evict oldest entries when limit reached

---

#### Aggressive History Cache Invalidation
**Location:** `dag/dag.go:388-393`
**Issue:** Entire cache cleared on any prune operation.
**Impact:** Wasted BFS computation; repeated cache rebuilding.

**Implementation:**
- Only invalidate cache entries for pruned rounds
- Track round → digests mapping for selective invalidation

---

#### Inefficient Transaction Extraction in GC
**Location:** `gc/gc.go:172-204`
**Issue:** O(rounds × batches × transactions) complexity per GC.
**Impact:** GC latency under high transaction volume.

**Implementation:**
1. Process rounds incrementally (not all at once)
2. Use iterator pattern instead of full collection
3. Stream transactions to callback

---

### 2.3 Reliability Improvements

#### AckTracker Cleanup Timing
**Location:** `worker/ack_tracker.go:199`
**Issue:** Cleanup runs every timeout/2; can leave batches orphaned for full timeout period.
**Impact:** Delayed cleanup; memory accumulation.

**Implementation:**
- Run cleanup every timeout/3
- Or use expiration heap for precise timing

---

#### Sync Request Retry Logic
**Location:** `network/sync.go:CatchUp`
**Issue:** Single attempt per peer; no retry on failure.
**Impact:** Sync failures leave node behind.

**Implementation:**
1. Retry failed requests with exponential backoff
2. Try multiple peers in parallel
3. Track peer responsiveness for ranking

---

#### Callback Panic Recovery
**Location:** `worker/worker.go:328`, `primary/primary.go`
**Issue:** Callback panics crash the worker/primary goroutine.
**Impact:** Component failure without recovery.

**Implementation:**
```go
func (w *Worker) invokeCallback(batch *types.Batch) {
    defer func() {
        if r := recover(); r != nil {
            log.Error("batch callback panic", "err", r)
        }
    }()
    if w.batchCallback != nil {
        w.batchCallback(batch)
    }
}
```

---

## Phase 3: Medium Priority Enhancements

### 3.1 Feature Completeness

#### Binary Serialization for Types
**Location:** `types/*.go`
**Issue:** No `MarshalBinary`/`UnmarshalBinary` for complex types.
**Impact:** Cannot serialize efficiently for network/storage.

**Implementation:**
1. Add binary marshaling to Batch, Header, Certificate, Vote
2. Use length-prefixed encoding
3. Consider Protocol Buffers for cross-language compatibility

---

#### JSON Marshaling for API
**Location:** `types/*.go`
**Issue:** No JSON support for complex types.
**Impact:** Difficult to build REST APIs or logging.

**Implementation:**
- Add `MarshalJSON`/`UnmarshalJSON` to all public types
- Use hex encoding for Hash, Signature, PublicKey

---

#### Equality Methods for Types
**Location:** `types/*.go`
**Issue:** Missing `Equal()` on Batch, Header, Certificate, Vote.
**Impact:** Testing and comparison difficult.

**Implementation:**
- Add `Equal(other *Type) bool` to each type
- Compare all fields including computed digests

---

#### Weighted Voting Support
**Location:** `types/validator.go`, `types/certificate.go`
**Issue:** `Validator.Power` field exists but is never used.
**Impact:** Cannot support stake-weighted consensus.

**Implementation:**
1. Update `ValidatorSet.Quorum()` to return weighted quorum
2. Update `Certificate.HasQuorum()` to sum vote weights
3. Ensure backwards compatibility with equal-weight mode

---

#### View Change / Leader Rotation
**Location:** `primary/primary.go`
**Issue:** No mechanism to detect or replace stalled validators.
**Impact:** Single validator failure can halt progress.

**Implementation:**
1. Add header timeout detection (no header in N rounds)
2. Implement round-robin leader rotation
3. Add view change messages for Byzantine fault tolerance

---

### 3.2 Operational Improvements

#### Metrics and Observability
**Location:** All packages
**Issue:** Limited metrics; no Prometheus integration.
**Impact:** Blind operation in production.

**Implementation:**
1. Add Prometheus metrics exporter
2. Key metrics:
   - `looseberry_transactions_total{status}` (added, rejected, committed)
   - `looseberry_batches_total`
   - `looseberry_certificates_total`
   - `looseberry_round_current`
   - `looseberry_pending_transactions`
   - `looseberry_worker_count`
   - `looseberry_gc_duration_seconds`
   - `looseberry_sync_operations_total`

---

#### Structured Logging
**Location:** All packages
**Issue:** No logging throughout codebase.
**Impact:** Cannot debug issues in production.

**Implementation:**
1. Add `log` interface or use standard library
2. Log at key points: component start/stop, errors, state changes
3. Use structured fields for queryability

---

#### Health Checks
**Location:** `looseberry.go`
**Issue:** No health check endpoint.
**Impact:** Cannot integrate with orchestrators (K8s, etc.).

**Implementation:**
```go
type HealthStatus struct {
    Healthy     bool
    Components  map[string]bool
    LastError   error
    Timestamp   time.Time
}

func (l *Looseberry) Health() *HealthStatus
```

---

#### Database Compaction
**Location:** `store/leveldb_*.go`
**Issue:** No compaction after GC; space not reclaimed.
**Impact:** Storage bloat over time.

**Implementation:**
1. Add `Compact()` method to store interfaces
2. Call after significant deletions
3. Run in background to avoid blocking

---

### 3.3 Testing Improvements

#### Failure Injection Tests
**Issue:** No tests for storage failures, network partitions, Byzantine behavior.
**Impact:** Unknown behavior under failure conditions.

**Implementation:**
1. Add fault-injecting mock stores
2. Test transaction recovery on storage failure
3. Test sync behavior on network partition
4. Test Byzantine validator detection

---

#### Long-Running Stability Tests
**Issue:** No extended operation tests.
**Impact:** Memory leaks and resource exhaustion undetected.

**Implementation:**
1. Add 24-hour soak test
2. Monitor memory, goroutine count, file descriptors
3. Assert no degradation over time

---

#### Performance Regression Tests
**Issue:** No automated performance benchmarks in CI.
**Impact:** Performance regressions undetected.

**Implementation:**
1. Add benchmark baselines to CI
2. Fail on >10% regression
3. Track historical performance

---

## Phase 4: Future Features

### 4.1 Advanced Consensus

#### Commit Rules Implementation
**Location:** `dag/dag.go`
**Issue:** No formal commit rules; rounds marked committed externally.
**Impact:** No provable finality guarantees.

**Implementation:**
1. Define commit rule: Round R committed when:
   - 2f+1 certificates exist at round R
   - 2f+1 certificates at round R+1 reference round R
2. Implement `IsCommitted(round)` with proof generation
3. Add merkle proofs for light client verification

---

#### Equivocation Detection and Slashing
**Location:** `primary/primary.go`, `types/certificate.go`
**Issue:** Equivocating validators not detected or penalized.
**Impact:** Byzantine behavior goes unpunished.

**Implementation:**
1. Track (validator, round) → header mappings
2. Detect conflicting headers from same validator
3. Generate equivocation proof
4. Expose callback for slashing integration

---

#### Topological DAG Ordering
**Location:** `dag/dag.go`
**Issue:** Ordering is by round then validator; ignores causality.
**Impact:** Suboptimal fairness and throughput.

**Implementation:**
1. Implement Kahn's algorithm for topological sort
2. Break ties by validator index (determinism)
3. Cache sorted results per round range

---

### 4.2 Network Layer

#### Real P2P Transport
**Location:** `network/` (new files)
**Issue:** Only mock network exists.
**Impact:** Cannot run actual multi-node cluster.

**Implementation:**
1. Add gRPC transport layer
2. Implement connection pooling
3. Add TLS/mTLS encryption
4. Integrate with glueberry (libp2p)

---

#### Peer Authentication
**Location:** `network/network.go`
**Issue:** No cryptographic verification of peer identity.
**Impact:** Any node can impersonate any validator.

**Implementation:**
1. Add TLS client certificates
2. Verify validator public key matches connection
3. Reject messages from unknown validators

---

#### Bandwidth Management
**Location:** `network/`
**Issue:** No rate limiting or bandwidth shaping.
**Impact:** Network flooding possible.

**Implementation:**
1. Add per-peer rate limiting
2. Implement message prioritization (votes > batches)
3. Add backpressure signaling

---

### 4.3 Advanced Mempool Features

#### Transaction Priority Queue
**Location:** `worker/worker.go`
**Issue:** All transactions treated equally.
**Impact:** Cannot implement gas price ordering.

**Implementation:**
1. Add priority field to Transaction or use callback
2. Use heap-based pending queue
3. Create batches from highest-priority transactions

---

#### Transaction Replacement (RBF)
**Location:** `worker/worker.go`, `types/transaction.go`
**Issue:** No nonce tracking; can't replace pending transactions.
**Impact:** Users stuck with old transactions.

**Implementation:**
1. Add sender extraction callback
2. Track pending by (sender, nonce)
3. Allow replacement with higher priority

---

#### Per-Sender Rate Limiting
**Location:** `worker/worker.go`
**Issue:** No per-sender transaction limits.
**Impact:** Single sender can flood mempool.

**Implementation:**
1. Add sender extraction callback
2. Track pending count per sender
3. Reject when sender exceeds limit

---

### 4.4 Storage Enhancements

#### Snapshot and Restore
**Location:** `store/`
**Issue:** No backup/restore capability.
**Impact:** No disaster recovery mechanism.

**Implementation:**
1. Add `Snapshot(path)` to store interfaces
2. Implement consistent snapshot across all stores
3. Add `Restore(path)` for recovery

---

#### Database Repair
**Location:** `store/leveldb_*.go`
**Issue:** No recovery from corruption.
**Impact:** Corruption requires manual intervention.

**Implementation:**
1. Add `Repair()` method using LevelDB's RepairFile
2. Detect corruption on startup
3. Attempt automatic repair

---

#### Storage Statistics
**Location:** `store/`
**Issue:** No visibility into storage usage.
**Impact:** Cannot monitor storage health.

**Implementation:**
```go
type StoreStats struct {
    TotalKeys     int64
    TotalBytes    int64
    CacheHitRate  float64
    WriteLatency  time.Duration
    ReadLatency   time.Duration
}

func (s *Store) Stats() *StoreStats
```

---

## Implementation Priority Matrix

| Item | Safety | Performance | Complexity | Priority Score |
|------|--------|-------------|------------|----------------|
| Worker batch triggers | Critical | High | Low | **P0** |
| Worker storage failure | Critical | - | Low | **P0** |
| Pool scale-down loss | Critical | - | Medium | **P0** |
| TxIndex pruning | Critical | High | Low | **P0** |
| Vote timeout enforcement | Critical | Medium | Low | **P0** |
| Double voting detection | Critical | - | Medium | **P0** |
| LevelDB TxIndex | Critical | - | Medium | **P0** |
| GC error logging | High | - | Low | **P1** |
| Parent validation | High | - | Low | **P1** |
| Flow control complete | High | Medium | Medium | **P1** |
| Hash routing fix | - | High | Low | **P1** |
| Scaler load calc | - | High | Low | **P1** |
| Certificate lookup | - | Critical | Medium | **P1** |
| History cache bound | - | High | Medium | **P1** |
| Metrics integration | - | - | Medium | **P2** |
| Logging | - | - | Medium | **P2** |
| Binary serialization | - | Medium | Medium | **P2** |
| Real P2P transport | - | - | High | **P3** |
| Priority queues | - | - | High | **P3** |
| Commit rules | High | - | High | **P3** |

---

## Testing Requirements

Each improvement should include:

1. **Unit Tests**
   - Normal operation
   - Edge cases
   - Error conditions
   - Concurrency (with `-race`)

2. **Integration Tests**
   - Multi-component interaction
   - Failure recovery
   - State consistency

3. **Benchmark Tests**
   - Performance baselines
   - Regression detection
   - Scalability limits

4. **Documentation**
   - Code comments
   - API documentation
   - Architecture updates

---

## Version Milestones

### v0.2.0 - Stability Release
- All Phase 1 critical issues resolved
- LevelDB TxIndex implementation
- Basic metrics and logging
- Comprehensive test coverage

### v0.3.0 - Performance Release
- Phase 2 optimizations complete
- Certificate lookup optimization
- Cache management improvements
- Benchmark suite established

### v0.4.0 - Feature Release
- Phase 3 features complete
- Binary/JSON serialization
- Weighted voting support
- Health checks and observability

### v1.0.0 - Production Release
- All phases complete
- Real P2P transport
- Commit rules with proofs
- Full documentation
- Security audit passed

---

## Contributing

When implementing roadmap items:

1. Create an issue referencing the roadmap item
2. Discuss design in the issue before implementation
3. Follow the testing requirements above
4. Update this roadmap when complete
5. Update PROGRESS_REPORT.md with implementation details

---

*Last updated: Based on comprehensive codebase review*
*Total identified items: 52 improvements across 4 phases*
