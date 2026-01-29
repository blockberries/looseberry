# Looseberry Prerelease Implementation Plan

This document provides a comprehensive, prioritized implementation plan for preparing Looseberry for production release. Looseberry is currently the least mature module in the Blockberries ecosystem at **65% production readiness** and requires significant work before deployment.

**Current Status**: 65% feature complete, **NOT production-ready**
**Target**: v1.0.0 stable release with full safety guarantees

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Critical Blockers (Must Fix)](#critical-blockers-must-fix)
3. [Phase 1: Data Loss Prevention](#phase-1-data-loss-prevention)
4. [Phase 2: Safety Violations](#phase-2-safety-violations)
5. [Phase 3: Performance Critical](#phase-3-performance-critical)
6. [Phase 4: Reliability Improvements](#phase-4-reliability-improvements)
7. [Phase 5: Observability](#phase-5-observability)
8. [Phase 6: Integration & Features](#phase-6-integration--features)
9. [Testing Requirements](#testing-requirements)
10. [Implementation Schedule](#implementation-schedule)
11. [Release Checklist](#release-checklist)

---

## Executive Summary

Looseberry is a DAG-based mempool implementation designed for high-throughput BFT consensus systems. While functionally complete, it has **8 critical (P0) issues** that can cause data loss, consensus violations, or system instability.

### Production Readiness Assessment

| Category | Status | Blocking Issues |
|----------|--------|-----------------|
| Data Integrity | **CRITICAL** | 4 issues can cause transaction loss |
| BFT Safety | **CRITICAL** | 3 issues violate consensus assumptions |
| Memory Stability | **HIGH** | 2 memory leak issues |
| Performance | Medium | O(n) lookups, inefficient GC |
| Observability | Low | No metrics or logging |
| Integration | Medium | Mock network only |

### Risk Matrix

| Issue | Likelihood | Impact | Overall Risk |
|-------|------------|--------|--------------|
| Data Loss (P0.1-3) | HIGH | CRITICAL | **CRITICAL** |
| Memory Leak (P0.4,6) | HIGH | HIGH | **CRITICAL** |
| Consensus Break (P0.5,7) | MEDIUM | CRITICAL | **CRITICAL** |
| Monitoring Blind Spot (P0.8) | HIGH | MEDIUM | **HIGH** |

### Dependencies

- **Cramberry**: For binary serialization of types
- **Glueberry**: For real P2P transport (future)
- **Blockberry**: Primary consumer, integrates via adapter

---

## Critical Blockers (Must Fix)

**These 8 issues MUST be resolved before ANY production deployment.**

### Issue Dependency Graph

```
P0.1 (Batch Timeout) ←─ blocks ─→ P0.3 (Scale-Down) ←─ depends on ─→ P0.2 (Storage)
                                                                          │
P0.4 (TxIndex) ─────── needed for ──────────────────────────────────────┤
                                                                          │
                       GC pipeline ◄─────────────────────────────────────┴── P0.8 (GC Errors)

P0.5 (Double-Vote) ───┐
P0.6 (Vote Cleanup) ──┼── blocks ── P0.7 (Parent Validation)
P0.7 (Parent Valid) ──┘              (DAG integrity)
```

### Recommended Fix Order

1. **Week 1**: P0.1, P0.2, P0.4, P0.8 (Data path fixes)
2. **Week 2**: P0.3, P0.5, P0.6, P0.7 (Safety fixes)
3. **Week 3**: Testing, integration, soak tests

---

## Phase 1: Data Loss Prevention

Focus: Fix all paths where transactions can be silently lost.

### P0.1: Worker Batch Creation - Size/Byte Triggers

**Location**: `worker/worker.go:277-330`
**Risk**: CRITICAL
**Effort**: 4 hours

#### Problem

Batches are ONLY created on timeout, not when size or byte limits are reached. Under high load, transactions accumulate indefinitely until timeout fires.

```go
// CURRENT (BROKEN) - Only creates on timeout
case <-batchTicker.C:
    w.createBatch()
```

#### Solution

Add immediate batch creation when thresholds are crossed:

```go
// worker/worker.go

// Add channel for immediate batch trigger
type Worker struct {
    // ... existing fields
    batchTrigger chan struct{}
}

func NewWorker(cfg WorkerConfig) *Worker {
    w := &Worker{
        // ... existing
        batchTrigger: make(chan struct{}, 1),
    }
    return w
}

// Update AddTx to check thresholds
func (w *Worker) AddTx(tx *types.Transaction) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    // ... existing validation

    w.pending = append(w.pending, tx)
    w.pendingBytes += len(tx.Data)

    // Check if batch should be created immediately
    if w.shouldCreateBatch() {
        select {
        case w.batchTrigger <- struct{}{}:
        default:
            // Already triggered
        }
    }

    return nil
}

func (w *Worker) shouldCreateBatch() bool {
    return len(w.pending) >= w.cfg.BatchSize ||
           w.pendingBytes >= w.cfg.MaxBatchBytes
}

// Update run loop
func (w *Worker) run() {
    batchTicker := time.NewTicker(w.cfg.BatchTimeout)
    defer batchTicker.Stop()

    for {
        select {
        case <-w.stopCh:
            return
        case <-batchTicker.C:
            w.createBatch()
        case <-w.batchTrigger:
            w.createBatch()
        }
    }
}
```

#### Tests Required

```go
func TestWorker_BatchCreatedOnSizeLimit(t *testing.T) {
    // Configure worker with BatchSize=10
    // Add 10 transactions
    // Verify batch created immediately (before timeout)
}

func TestWorker_BatchCreatedOnByteLimit(t *testing.T) {
    // Configure worker with MaxBatchBytes=1000
    // Add transactions totaling 1000+ bytes
    // Verify batch created immediately
}

func TestWorker_BatchCreatedOnTimeout(t *testing.T) {
    // Add fewer transactions than limits
    // Verify batch created after timeout
}
```

---

### P0.2: Worker Storage Failure - Transaction Re-queue

**Location**: `worker/worker.go:310-315`
**Risk**: CRITICAL
**Effort**: 3 hours

#### Problem

If `SaveBatch()` fails, transactions are silently lost:

```go
// CURRENT (BROKEN)
if err := w.batchStore.SaveBatch(batch); err != nil {
    return  // Transactions LOST - never re-queued!
}
```

#### Solution

Re-queue transactions on storage failure:

```go
// worker/worker.go

func (w *Worker) createBatch() {
    w.mu.Lock()
    if len(w.pending) == 0 {
        w.mu.Unlock()
        return
    }

    batch := &types.Batch{
        Transactions: w.pending,
        Author:       w.cfg.ValidatorID,
        Round:        w.currentRound,
        Timestamp:    time.Now().UnixNano(),
    }
    batch.ComputeDigest()

    // Clear pending before unlock
    pendingCopy := w.pending
    w.pending = make([]*types.Transaction, 0, w.cfg.BatchSize)
    w.pendingBytes = 0
    w.mu.Unlock()

    // Attempt to save batch
    if err := w.batchStore.SaveBatch(batch); err != nil {
        w.logger.Error("batch save failed, re-queueing transactions",
            "batch_digest", batch.Digest,
            "tx_count", len(batch.Transactions),
            "error", err,
        )
        w.metrics.BatchSaveFailures.Inc()

        // Re-queue transactions
        w.requeue(pendingCopy)
        return
    }

    // Success path
    w.metrics.BatchesCreated.Inc()
    if w.batchCallback != nil {
        w.invokeBatchCallback(batch)
    }
}

func (w *Worker) requeue(txs []*types.Transaction) {
    w.mu.Lock()
    defer w.mu.Unlock()

    // Prepend to pending (maintain order)
    w.pending = append(txs, w.pending...)
    for _, tx := range txs {
        w.pendingBytes += len(tx.Data)
    }

    w.metrics.TransactionsRequeued.Add(float64(len(txs)))
}
```

#### Tests Required

```go
func TestWorker_RequeuesOnStorageFailure(t *testing.T) {
    // Use mock store that fails SaveBatch
    // Add transactions, trigger batch creation
    // Verify transactions are re-queued to pending
    // Verify batch callback NOT called
}

func TestWorker_RequeuePreservesOrder(t *testing.T) {
    // Add txs [1,2,3], fail save
    // Add txs [4,5,6]
    // Verify pending order is [1,2,3,4,5,6]
}
```

---

### P0.3: Pool Scale-Down - Transaction Drain

**Location**: `worker/pool.go:280-301`
**Risk**: HIGH
**Effort**: 6 hours

#### Problem

When scaling down, worker removal doesn't drain pending transactions:

```go
// CURRENT (BROKEN)
func (p *Pool) scaleDown(count int) {
    for i := 0; i < count; i++ {
        worker := p.workers[len(p.workers)-1]
        worker.Stop()  // Transactions in worker.pending are LOST!
        p.workers = p.workers[:len(p.workers)-1]
    }
}
```

#### Solution

Add Drain() method and use it before stopping:

```go
// worker/worker.go

// Drain returns all pending transactions and stops accepting new ones.
func (w *Worker) Drain() []*types.Transaction {
    w.mu.Lock()
    defer w.mu.Unlock()

    // Mark as draining - AddTx will reject
    w.draining = true

    // Return copy of pending
    pending := make([]*types.Transaction, len(w.pending))
    copy(pending, w.pending)
    w.pending = nil
    w.pendingBytes = 0

    return pending
}

// Update AddTx to check draining state
func (w *Worker) AddTx(tx *types.Transaction) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if w.draining {
        return ErrWorkerDraining
    }
    // ... rest of implementation
}

// worker/pool.go

func (p *Pool) scaleDown(count int) {
    p.mu.Lock()
    defer p.mu.Unlock()

    var drainedTxs []*types.Transaction

    for i := 0; i < count && len(p.workers) > p.cfg.MinWorkers; i++ {
        worker := p.workers[len(p.workers)-1]

        // Drain pending transactions
        drained := worker.Drain()
        drainedTxs = append(drainedTxs, drained...)

        // Now safe to stop
        worker.Stop()
        p.workers = p.workers[:len(p.workers)-1]
    }

    // Re-inject drained transactions to remaining workers
    if len(drainedTxs) > 0 {
        p.reinject(drainedTxs)
    }
}

func (p *Pool) reinject(txs []*types.Transaction) {
    for _, tx := range txs {
        // Use routing to find target worker
        worker := p.routeTransaction(tx)
        if err := worker.AddTx(tx); err != nil {
            p.logger.Warn("failed to reinject transaction",
                "tx_hash", tx.Hash,
                "error", err,
            )
        }
    }
}
```

#### Tests Required

```go
func TestPool_ScaleDownDrainsTransactions(t *testing.T) {
    // Create pool with 4 workers
    // Add transactions to each worker
    // Scale down to 2 workers
    // Verify NO transactions lost
    // Verify transactions redistributed to remaining workers
}

func TestPool_DrainedWorkerRejectsNewTxs(t *testing.T) {
    // Drain a worker
    // Attempt to add transaction
    // Verify ErrWorkerDraining returned
}
```

---

### P0.4: TxIndex Memory Leak - GC Pruning

**Location**: `gc/gc.go:144-169`
**Risk**: CRITICAL
**Effort**: 2 hours

#### Problem

TxIndex is NEVER pruned during GC, causing unbounded memory growth:

```go
// CURRENT - Missing TxIndex cleanup in performGC()
// performGC deletes batches but never calls txIndex.RemoveTxsForBatch()
```

At 250k tx/sec, this will cause OOM within 7 days.

#### Solution

Add TxIndex cleanup to GC:

```go
// gc/gc.go

func (gc *GarbageCollector) performGC(round uint64) error {
    // Get batches to delete
    batchesToDelete, err := gc.getBatchesToDelete(round)
    if err != nil {
        return fmt.Errorf("get batches to delete: %w", err)
    }

    // Extract uncommitted transactions (existing logic)
    uncommitted := gc.extractUncommitted(batchesToDelete)

    // NEW: Clean up TxIndex for batches being deleted
    for _, batch := range batchesToDelete {
        if err := gc.txIndex.RemoveTxsForBatch(batch.Digest); err != nil {
            gc.logger.Warn("failed to remove tx index entries",
                "batch_digest", batch.Digest,
                "error", err,
            )
            // Continue - don't fail entire GC for index cleanup failure
        }
    }

    // Delete batches from storage (existing logic)
    for _, batch := range batchesToDelete {
        if err := gc.batchStore.DeleteBatch(batch.Digest); err != nil {
            return fmt.Errorf("delete batch %x: %w", batch.Digest, err)
        }
    }

    gc.metrics.BatchesDeleted.Add(float64(len(batchesToDelete)))
    gc.metrics.TxIndexEntriesRemoved.Add(float64(gc.countTxsInBatches(batchesToDelete)))

    return nil
}
```

#### Tests Required

```go
func TestGC_PrunesTxIndex(t *testing.T) {
    // Add batches with transactions
    // Verify TxIndex contains entries
    // Run GC
    // Verify TxIndex entries removed for deleted batches
}

func TestGC_TxIndexSizeStabilizes(t *testing.T) {
    // Run load test for N rounds
    // Verify TxIndex size doesn't grow unbounded
    // Verify memory usage stable
}
```

---

## Phase 2: Safety Violations

Focus: Fix all paths that can violate BFT safety guarantees.

### P0.5: Double Voting Detection

**Location**: `primary/vote_tracker.go`
**Risk**: CRITICAL
**Effort**: 5 hours

#### Problem

A validator can vote for multiple conflicting headers in the same round without detection:

```go
// CURRENT - No check for existing vote
func (vt *VoteTracker) RecordVote(vote *types.Vote) error {
    vt.mu.Lock()
    defer vt.mu.Unlock()

    header := vt.pendingHeaders[vote.HeaderDigest]
    header.votes[vote.Validator] = vote  // Overwrites any existing vote!
    return nil
}
```

#### Solution

Detect and reject equivocation:

```go
// primary/vote_tracker.go

type VoteTracker struct {
    // ... existing fields

    // Track which headers each validator has voted for per round
    // validator -> round -> headerDigest
    validatorVotes map[uint16]map[uint64]types.Hash
}

func NewVoteTracker(cfg VoteTrackerConfig) *VoteTracker {
    return &VoteTracker{
        // ... existing
        validatorVotes: make(map[uint16]map[uint64]types.Hash),
    }
}

func (vt *VoteTracker) RecordVote(vote *types.Vote) error {
    vt.mu.Lock()
    defer vt.mu.Unlock()

    // Get pending header
    header, ok := vt.pendingHeaders[vote.HeaderDigest]
    if !ok {
        return ErrHeaderNotFound
    }

    // Check for equivocation
    if err := vt.checkEquivocation(vote, header.Round); err != nil {
        return err
    }

    // Record vote
    header.votes[vote.Validator] = vote

    // Track this validator's vote for this round
    vt.recordValidatorVote(vote.Validator, header.Round, vote.HeaderDigest)

    return nil
}

func (vt *VoteTracker) checkEquivocation(vote *types.Vote, round uint64) error {
    roundVotes, ok := vt.validatorVotes[vote.Validator]
    if !ok {
        return nil // First vote from this validator
    }

    existingHeader, ok := roundVotes[round]
    if !ok {
        return nil // First vote in this round
    }

    if existingHeader != vote.HeaderDigest {
        // EQUIVOCATION DETECTED!
        return &EquivocationError{
            Validator:      vote.Validator,
            Round:          round,
            ExistingHeader: existingHeader,
            NewHeader:      vote.HeaderDigest,
        }
    }

    // Same header - duplicate vote (allowed, idempotent)
    return nil
}

func (vt *VoteTracker) recordValidatorVote(validator uint16, round uint64, header types.Hash) {
    if vt.validatorVotes[validator] == nil {
        vt.validatorVotes[validator] = make(map[uint64]types.Hash)
    }
    vt.validatorVotes[validator][round] = header
}

// EquivocationError represents a detected double-voting violation.
type EquivocationError struct {
    Validator      uint16
    Round          uint64
    ExistingHeader types.Hash
    NewHeader      types.Hash
}

func (e *EquivocationError) Error() string {
    return fmt.Sprintf("equivocation detected: validator %d voted for both %x and %x in round %d",
        e.Validator, e.ExistingHeader, e.NewHeader, e.Round)
}
```

#### Equivocation Callback

Allow application to handle equivocation (for slashing):

```go
type VoteTrackerConfig struct {
    // ... existing
    OnEquivocation func(ev *EquivocationError)
}

func (vt *VoteTracker) checkEquivocation(vote *types.Vote, round uint64) error {
    // ... detection logic

    if existingHeader != vote.HeaderDigest {
        err := &EquivocationError{...}

        // Notify application
        if vt.cfg.OnEquivocation != nil {
            vt.cfg.OnEquivocation(err)
        }

        vt.metrics.EquivocationsDetected.Inc()
        return err
    }
    return nil
}
```

#### Tests Required

```go
func TestVoteTracker_DetectsDoubleVote(t *testing.T) {
    // Create two different headers for same round
    // Vote for first header
    // Attempt to vote for second header
    // Verify EquivocationError returned
}

func TestVoteTracker_AllowsDuplicateVote(t *testing.T) {
    // Vote for header
    // Vote for same header again
    // Verify no error (idempotent)
}

func TestVoteTracker_EquivocationCallback(t *testing.T) {
    // Configure callback
    // Trigger equivocation
    // Verify callback invoked with correct details
}
```

---

### P0.6: Vote Timeout Enforcement

**Location**: `primary/primary.go`
**Risk**: HIGH
**Effort**: 3 hours

#### Problem

VoteTracker has timeout methods but Primary never calls them, causing orphaned votes to accumulate:

```go
// VoteTracker has RemoveTimedOut() but it's never called!
// Result: Unbounded memory growth
```

#### Solution

Add periodic cleanup in Primary's header loop:

```go
// primary/primary.go

func (p *Primary) run() {
    headerTicker := time.NewTicker(p.cfg.HeaderInterval)
    cleanupTicker := time.NewTicker(p.cfg.VoteTimeout / 2) // Cleanup at half timeout
    defer headerTicker.Stop()
    defer cleanupTicker.Stop()

    for {
        select {
        case <-p.stopCh:
            return

        case <-headerTicker.C:
            p.maybeCreateHeader()

        case <-cleanupTicker.C:
            p.cleanupTimedOutVotes()

        case header := <-p.incomingHeaders:
            p.handleHeader(header)

        case vote := <-p.incomingVotes:
            p.handleVote(vote)
        }
    }
}

func (p *Primary) cleanupTimedOutVotes() {
    removed := p.voteTracker.RemoveTimedOut(p.cfg.VoteTimeout)

    if removed > 0 {
        p.logger.Debug("cleaned up timed out votes",
            "removed_headers", removed,
        )
        p.metrics.TimedOutHeadersRemoved.Add(float64(removed))
    }
}

// primary/vote_tracker.go

// RemoveTimedOut removes headers that have been pending longer than timeout.
// Returns the number of headers removed.
func (vt *VoteTracker) RemoveTimedOut(timeout time.Duration) int {
    vt.mu.Lock()
    defer vt.mu.Unlock()

    cutoff := time.Now().Add(-timeout)
    removed := 0

    for digest, header := range vt.pendingHeaders {
        if header.receivedAt.Before(cutoff) {
            delete(vt.pendingHeaders, digest)
            removed++
        }
    }

    // Also clean up validatorVotes for old rounds
    vt.pruneOldRoundVotes()

    return removed
}

func (vt *VoteTracker) pruneOldRoundVotes() {
    // Keep votes only for rounds still in pendingHeaders
    activeRounds := make(map[uint64]bool)
    for _, header := range vt.pendingHeaders {
        activeRounds[header.Round] = true
    }

    for validator, roundVotes := range vt.validatorVotes {
        for round := range roundVotes {
            if !activeRounds[round] {
                delete(roundVotes, round)
            }
        }
        if len(roundVotes) == 0 {
            delete(vt.validatorVotes, validator)
        }
    }
}
```

#### Tests Required

```go
func TestPrimary_CleansUpTimedOutVotes(t *testing.T) {
    // Add header
    // Wait for timeout
    // Verify header removed from vote tracker
}

func TestVoteTracker_RemoveTimedOutReturnsCount(t *testing.T) {
    // Add multiple headers at different times
    // Call RemoveTimedOut
    // Verify correct count returned
    // Verify only old headers removed
}

func TestVoteTracker_PrunesValidatorVotesForOldRounds(t *testing.T) {
    // Record votes for various rounds
    // Remove headers for some rounds
    // Verify validator votes for those rounds also removed
}
```

---

### P0.7: DAG Parent Validation

**Location**: `dag/dag.go:110-151`
**Risk**: CRITICAL
**Effort**: 3 hours

#### Problem

Parents aren't validated to exist or be from the correct round:

```go
// CURRENT - No parent validation!
func (d *DAG) AddCertificate(cert *types.Certificate) error {
    // Just adds to storage without checking parents
}
```

A Byzantine validator could create a malformed DAG.

#### Solution

Validate parents before adding certificate:

```go
// dag/dag.go

// ErrInvalidParentRound indicates a parent is from the wrong round.
var ErrInvalidParentRound = errors.New("parent certificate is from invalid round")

// ErrMissingParent indicates a required parent certificate is not in the DAG.
var ErrMissingParent = errors.New("parent certificate not found in DAG")

// ErrParentFromFuture indicates a parent claims to be from a future round.
var ErrParentFromFuture = errors.New("parent certificate is from future round")

func (d *DAG) AddCertificate(cert *types.Certificate) error {
    d.mu.Lock()
    defer d.mu.Unlock()

    // Validate certificate round
    if cert.Header.Round == 0 {
        // Genesis round - no parents required
        return d.addCertificateUnsafe(cert)
    }

    // Validate all parents
    if err := d.validateParents(cert); err != nil {
        return err
    }

    return d.addCertificateUnsafe(cert)
}

func (d *DAG) validateParents(cert *types.Certificate) error {
    expectedParentRound := cert.Header.Round - 1

    for _, parent := range cert.Header.Parents {
        // Check parent round
        if parent.Round != expectedParentRound {
            return fmt.Errorf("%w: parent %x has round %d, expected %d",
                ErrInvalidParentRound, parent.Digest, parent.Round, expectedParentRound)
        }

        // Check parent exists in DAG
        if !d.hasCertificate(parent.Digest) {
            return fmt.Errorf("%w: parent %x", ErrMissingParent, parent.Digest)
        }
    }

    // Optionally: Check minimum parent count for BFT
    if len(cert.Header.Parents) < d.cfg.MinParents {
        return fmt.Errorf("insufficient parents: got %d, need %d",
            len(cert.Header.Parents), d.cfg.MinParents)
    }

    return nil
}

func (d *DAG) hasCertificate(digest types.Hash) bool {
    // Check in rounds map
    for _, certs := range d.rounds {
        for _, cert := range certs {
            if cert.Digest == digest {
                return true
            }
        }
    }
    return false
}
```

#### Tests Required

```go
func TestDAG_RejectsInvalidParentRound(t *testing.T) {
    // Add certificate at round 5
    // Create certificate at round 7 with parent from round 5
    // Verify ErrInvalidParentRound returned
}

func TestDAG_RejectsMissingParent(t *testing.T) {
    // Create certificate with parent digest that doesn't exist
    // Verify ErrMissingParent returned
}

func TestDAG_AcceptsValidParents(t *testing.T) {
    // Add certificates at rounds 1, 2
    // Create certificate at round 3 with valid parents from round 2
    // Verify success
}

func TestDAG_GenesisRoundNoParentsRequired(t *testing.T) {
    // Add certificate at round 0 with no parents
    // Verify success
}
```

---

### P0.8: GC Error Logging

**Location**: `gc/gc.go:213-221`
**Risk**: HIGH
**Effort**: 1 hour

#### Problem

GC errors are silently discarded:

```go
// CURRENT
_ = gc.performGC(gcRound)  // Errors SILENTLY IGNORED!
```

#### Solution

Log errors and expose metrics:

```go
// gc/gc.go

func (gc *GarbageCollector) run() {
    gcTicker := time.NewTicker(gc.cfg.GCInterval)
    defer gcTicker.Stop()

    for {
        select {
        case <-gc.stopCh:
            return
        case <-gcTicker.C:
            gcRound := gc.calculateGCRound()
            if gcRound > gc.lastGCRound {
                if err := gc.performGC(gcRound); err != nil {
                    gc.logger.Error("garbage collection failed",
                        "round", gcRound,
                        "error", err,
                    )
                    gc.metrics.GCFailures.Inc()

                    // Don't update lastGCRound - will retry next interval
                    continue
                }

                gc.lastGCRound = gcRound
                gc.logger.Info("garbage collection completed",
                    "round", gcRound,
                )
                gc.metrics.GCRoundsCompleted.Inc()
            }
        }
    }
}
```

#### Tests Required

```go
func TestGC_LogsAndMetricsOnFailure(t *testing.T) {
    // Use mock store that fails
    // Trigger GC
    // Verify error logged
    // Verify GCFailures metric incremented
    // Verify GC retried on next interval
}
```

---

## Phase 3: Performance Critical

Focus: Fix O(n) operations that become bottlenecks at scale.

### P1-1: O(1) Certificate Lookup

**Location**: `dag/dag.go:156-165`
**Risk**: HIGH (performance)
**Effort**: 4 hours

#### Problem

`GetCertificate()` scans all rounds:

```go
// CURRENT - O(rounds × validators)
func (d *DAG) GetCertificate(digest types.Hash) *types.Certificate {
    for _, certs := range d.rounds {
        for _, cert := range certs {
            if cert.Digest == digest {
                return cert
            }
        }
    }
    return nil
}
```

#### Solution

Add digest-to-round index:

```go
// dag/dag.go

type DAG struct {
    // ... existing fields

    // digestToRound maps certificate digest to its round for O(1) lookup
    digestToRound map[types.Hash]uint64
}

func NewDAG(cfg DAGConfig) *DAG {
    return &DAG{
        // ... existing
        digestToRound: make(map[types.Hash]uint64),
    }
}

func (d *DAG) addCertificateUnsafe(cert *types.Certificate) error {
    // Add to rounds
    round := cert.Header.Round
    d.rounds[round] = append(d.rounds[round], cert)

    // Add to digest index
    d.digestToRound[cert.Digest] = round

    return nil
}

// GetCertificate returns the certificate with the given digest, or nil if not found.
// O(1) time complexity.
func (d *DAG) GetCertificate(digest types.Hash) *types.Certificate {
    d.mu.RLock()
    defer d.mu.RUnlock()

    round, ok := d.digestToRound[digest]
    if !ok {
        return nil
    }

    for _, cert := range d.rounds[round] {
        if cert.Digest == digest {
            return cert
        }
    }

    return nil // Shouldn't happen if indices are consistent
}

// Update prune to clean up index
func (d *DAG) pruneBefore(round uint64) {
    d.mu.Lock()
    defer d.mu.Unlock()

    for r := range d.rounds {
        if r < round {
            // Remove from digest index
            for _, cert := range d.rounds[r] {
                delete(d.digestToRound, cert.Digest)
            }
            delete(d.rounds, r)
        }
    }

    // Invalidate affected cache entries
    d.invalidateCacheBefore(round)
}
```

#### Tests Required

```go
func BenchmarkDAG_GetCertificate(b *testing.B) {
    // Compare old O(n) vs new O(1) implementation
}

func TestDAG_DigestIndexConsistency(t *testing.T) {
    // Add certificates
    // Verify GetCertificate works
    // Prune
    // Verify index updated correctly
}
```

---

### P1-2: Bounded History Cache

**Location**: `dag/dag.go` (historyCache)
**Risk**: HIGH (memory)
**Effort**: 3 hours

#### Problem

Causal history cache grows unbounded.

#### Solution

Add LRU eviction:

```go
// dag/history_cache.go

type HistoryCache struct {
    mu       sync.RWMutex
    entries  map[types.Hash]*historyCacheEntry
    order    []types.Hash  // LRU order
    maxSize  int
}

type historyCacheEntry struct {
    history []types.Hash
    round   uint64
}

func NewHistoryCache(maxSize int) *HistoryCache {
    return &HistoryCache{
        entries: make(map[types.Hash]*historyCacheEntry),
        order:   make([]types.Hash, 0, maxSize),
        maxSize: maxSize,
    }
}

func (c *HistoryCache) Get(digest types.Hash) ([]types.Hash, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    entry, ok := c.entries[digest]
    if !ok {
        return nil, false
    }
    return entry.history, true
}

func (c *HistoryCache) Put(digest types.Hash, history []types.Hash, round uint64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Evict if at capacity
    for len(c.entries) >= c.maxSize {
        oldest := c.order[0]
        delete(c.entries, oldest)
        c.order = c.order[1:]
    }

    c.entries[digest] = &historyCacheEntry{
        history: history,
        round:   round,
    }
    c.order = append(c.order, digest)
}

// InvalidateBefore removes entries for rounds before the given round.
func (c *HistoryCache) InvalidateBefore(round uint64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    newOrder := make([]types.Hash, 0, len(c.order))
    for _, digest := range c.order {
        entry := c.entries[digest]
        if entry.round >= round {
            newOrder = append(newOrder, digest)
        } else {
            delete(c.entries, digest)
        }
    }
    c.order = newOrder
}
```

---

### P1-3: Efficient GC Transaction Extraction

**Location**: `gc/gc.go:172-204`
**Risk**: MEDIUM
**Effort**: 4 hours

#### Problem

O(rounds × batches × transactions) per GC cycle.

#### Solution

Incremental processing with iterators:

```go
// gc/gc.go

func (gc *GarbageCollector) performGC(round uint64) error {
    // Process rounds incrementally
    for r := gc.lastGCRound + 1; r <= round; r++ {
        if err := gc.processRound(r); err != nil {
            return fmt.Errorf("process round %d: %w", r, err)
        }
    }
    return nil
}

func (gc *GarbageCollector) processRound(round uint64) error {
    // Stream batches instead of loading all into memory
    return gc.batchStore.IterateBatches(round, func(batch *types.Batch) error {
        // Check if committed
        if gc.isCommitted(batch.Digest) {
            // Clean up TxIndex
            if err := gc.txIndex.RemoveTxsForBatch(batch.Digest); err != nil {
                gc.logger.Warn("failed to remove tx index", "error", err)
            }
            // Delete batch
            return gc.batchStore.DeleteBatch(batch.Digest)
        }

        // Extract uncommitted transactions via callback
        for _, tx := range batch.Transactions {
            if gc.uncommittedCallback != nil {
                gc.uncommittedCallback(tx)
            }
        }

        return nil
    })
}
```

---

## Phase 4: Reliability Improvements

### P2-1: Complete Flow Control

**Location**: `gc/flow.go`
**Effort**: 4 hours

Enforce `MaxPendingBatches` and `MaxPendingHeaders`:

```go
// gc/flow.go

type FlowController struct {
    mu sync.Mutex

    pendingBatches  int
    pendingHeaders  int
    maxPendingBatches int
    maxPendingHeaders int

    // Blocking channels
    batchAllowed  chan struct{}
    headerAllowed chan struct{}
}

func (fc *FlowController) CanCreateBatch() bool {
    fc.mu.Lock()
    defer fc.mu.Unlock()
    return fc.pendingBatches < fc.maxPendingBatches
}

func (fc *FlowController) WaitForBatchSlot(ctx context.Context) error {
    if fc.CanCreateBatch() {
        return nil
    }

    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-fc.batchAllowed:
        return nil
    }
}

func (fc *FlowController) BatchCreated() {
    fc.mu.Lock()
    fc.pendingBatches++
    fc.mu.Unlock()
}

func (fc *FlowController) BatchCommitted() {
    fc.mu.Lock()
    fc.pendingBatches--
    if fc.pendingBatches < fc.maxPendingBatches {
        select {
        case fc.batchAllowed <- struct{}{}:
        default:
        }
    }
    fc.mu.Unlock()
}
```

---

### P2-2: Hash-Based Routing Fix

**Location**: `worker/pool.go:158`
**Effort**: 1 hour

Use more entropy:

```go
// Before (weak - only 256 buckets)
workerIdx := int(txHash[0]) % len(p.workers)

// After (strong - full 64-bit entropy)
func (p *Pool) routeTransaction(tx *types.Transaction) *Worker {
    // Use first 8 bytes of hash for routing
    h := binary.BigEndian.Uint64(tx.Hash[:8])
    idx := int(h % uint64(len(p.workers)))
    return p.workers[idx]
}
```

---

### P2-3: Scaler Load Calculation

**Location**: `worker/scaler.go:134-148`
**Effort**: 2 hours

Include byte capacity:

```go
func (s *Scaler) calculateLoad() float64 {
    s.mu.RLock()
    defer s.mu.RUnlock()

    var totalPending, totalBytes int
    for _, w := range s.workers {
        pending, bytes := w.GetPendingStats()
        totalPending += pending
        totalBytes += bytes
    }

    maxPending := len(s.workers) * s.cfg.BatchSize
    maxBytes := len(s.workers) * s.cfg.MaxBatchBytes

    txLoad := float64(totalPending) / float64(maxPending)
    byteLoad := float64(totalBytes) / float64(maxBytes)

    // Use the higher of the two loads
    return math.Max(txLoad, byteLoad)
}
```

---

### P2-4: Scaling Hysteresis

**Location**: `worker/scaler.go:201-205`
**Effort**: 1 hour

Prevent oscillation:

```go
const (
    scaleUpThreshold   = 0.8
    scaleDownThreshold = 0.3  // Was 0.2 - creates dead band
)

func (s *Scaler) evaluate() ScalingDecision {
    load := s.calculateLoad()

    if load > scaleUpThreshold {
        return ScaleUp
    }
    if load < scaleDownThreshold {
        return ScaleDown
    }
    return NoChange  // In dead band - maintain current scale
}
```

---

### P2-5: Callback Panic Recovery

**Location**: `worker/worker.go:328`, `primary/primary.go`
**Effort**: 2 hours

```go
func (w *Worker) invokeBatchCallback(batch *types.Batch) {
    defer func() {
        if r := recover(); r != nil {
            w.logger.Error("batch callback panic recovered",
                "batch", batch.Digest,
                "panic", r,
                "stack", string(debug.Stack()),
            )
            w.metrics.CallbackPanics.Inc()
        }
    }()

    if w.batchCallback != nil {
        w.batchCallback(batch)
    }
}
```

---

## Phase 5: Observability

### P3-1: Prometheus Metrics

**Effort**: 4-5 days

```go
// metrics/metrics.go

type Metrics struct {
    // Transaction metrics
    TransactionsReceived  prometheus.Counter
    TransactionsRejected  *prometheus.CounterVec // labels: reason
    TransactionsCommitted prometheus.Counter
    TransactionsRequeued  prometheus.Counter

    // Batch metrics
    BatchesCreated     prometheus.Counter
    BatchesSaveFailures prometheus.Counter
    BatchesDeleted     prometheus.Counter

    // Certificate metrics
    CertificatesCreated prometheus.Counter
    CertificatesReceived prometheus.Counter
    CertificatesRejected *prometheus.CounterVec // labels: reason

    // Round/consensus metrics
    CurrentRound     prometheus.Gauge
    CommittedRound   prometheus.Gauge
    PendingHeaders   prometheus.Gauge
    PendingVotes     prometheus.Gauge

    // Worker metrics
    WorkerCount      prometheus.Gauge
    PendingTxPerWorker *prometheus.GaugeVec // labels: worker_id

    // GC metrics
    GCRoundsCompleted prometheus.Counter
    GCFailures        prometheus.Counter
    GCDuration        prometheus.Histogram

    // Safety metrics
    EquivocationsDetected prometheus.Counter
    ParentValidationFailures prometheus.Counter
    DoubleVotesDetected prometheus.Counter
}
```

---

### P3-2: Structured Logging

**Effort**: 3 days

Add logging interface and strategic log points throughout all components.

---

### P3-3: Health Checks

**Effort**: 2 days

```go
type HealthStatus struct {
    Healthy    bool
    Components map[string]ComponentHealth
}

type ComponentHealth struct {
    Healthy   bool
    LastCheck time.Time
    Details   map[string]interface{}
}

func (l *Looseberry) Health() *HealthStatus {
    return &HealthStatus{
        Healthy: l.isHealthy(),
        Components: map[string]ComponentHealth{
            "workers":   l.pool.Health(),
            "primary":   l.primary.Health(),
            "dag":       l.dag.Health(),
            "gc":        l.gc.Health(),
            "batchStore": l.batchStore.Health(),
        },
    }
}
```

---

## Phase 6: Integration & Features

### P4-1: LevelDB TxIndex Implementation

**Effort**: 1 day

```go
// store/leveldb_txindex.go

type LevelDBTxIndex struct {
    db *leveldb.DB
}

func NewLevelDBTxIndex(path string) (*LevelDBTxIndex, error) {
    db, err := leveldb.OpenFile(path, nil)
    if err != nil {
        return nil, err
    }
    return &LevelDBTxIndex{db: db}, nil
}

// Key schemas:
// T:{txHash} -> {batchHash}           (tx to batch mapping)
// TB:{batchHash}:{txHash} -> empty    (batch to tx reverse index)

func (idx *LevelDBTxIndex) Add(txHash, batchHash types.Hash) error {
    batch := new(leveldb.Batch)

    // Forward index
    batch.Put(idx.txKey(txHash), batchHash[:])

    // Reverse index
    batch.Put(idx.batchTxKey(batchHash, txHash), nil)

    return idx.db.Write(batch, nil)
}

func (idx *LevelDBTxIndex) GetBatch(txHash types.Hash) (types.Hash, error) {
    val, err := idx.db.Get(idx.txKey(txHash), nil)
    if err == leveldb.ErrNotFound {
        return types.Hash{}, ErrTxNotFound
    }
    if err != nil {
        return types.Hash{}, err
    }

    var hash types.Hash
    copy(hash[:], val)
    return hash, nil
}

func (idx *LevelDBTxIndex) RemoveTxsForBatch(batchHash types.Hash) error {
    // Iterate reverse index to find all txs for batch
    prefix := idx.batchTxPrefix(batchHash)
    iter := idx.db.NewIterator(util.BytesPrefix(prefix), nil)
    defer iter.Release()

    batch := new(leveldb.Batch)
    for iter.Next() {
        txHash := idx.extractTxHash(iter.Key())

        // Delete forward index
        batch.Delete(idx.txKey(txHash))

        // Delete reverse index
        batch.Delete(iter.Key())
    }

    return idx.db.Write(batch, nil)
}
```

---

### P4-2: Binary Serialization

**Effort**: 3-4 days

Add Cramberry-based serialization to all types:

```go
// types/batch.go

func (b *Batch) MarshalBinary() ([]byte, error) {
    return cramberry.Marshal(b)
}

func (b *Batch) UnmarshalBinary(data []byte) error {
    return cramberry.Unmarshal(data, b)
}
```

---

## Testing Requirements

### Regression Test Checklist

- [ ] Unit tests for all 8 P0 issues
- [ ] Concurrent tests with `-race` flag pass
- [ ] 24-hour soak test at 250k tx/sec
- [ ] Memory usage stable after GC cycles
- [ ] Byzantine equivocation detected
- [ ] Storage failure recovery works
- [ ] Metrics and logging comprehensive
- [ ] Production readiness assessment >= 90%

### Test Categories

#### Safety Tests
```go
func TestByzantine_DoubleVotingDetected(t *testing.T)
func TestByzantine_InvalidParentRejected(t *testing.T)
func TestByzantine_EquivocationCallbackFired(t *testing.T)
```

#### Data Integrity Tests
```go
func TestDataIntegrity_NoTxLossOnStorageFailure(t *testing.T)
func TestDataIntegrity_NoTxLossOnScaleDown(t *testing.T)
func TestDataIntegrity_TxIndexPrunedCorrectly(t *testing.T)
```

#### Stress Tests
```go
func TestStress_250kTxPerSecond(t *testing.T)
func TestStress_MemoryStableOver24Hours(t *testing.T)
func TestStress_GCKeepsPaceWithLoad(t *testing.T)
```

---

## Implementation Schedule

| Phase | Duration | Items | Dependencies |
|-------|----------|-------|--------------|
| Phase 1 | Week 1 | P0.1, P0.2, P0.3, P0.4 | None |
| Phase 2 | Week 2 | P0.5, P0.6, P0.7, P0.8 | Phase 1 |
| Phase 3 | Week 3 | P1-1, P1-2, P1-3 | Phase 2 |
| Phase 4 | Week 4 | P2-1 through P2-5 | Phase 3 |
| Phase 5 | Week 5 | Metrics, logging, health | Phase 4 |
| Phase 6 | Week 6 | LevelDB TxIndex, serialization | Phase 5 |
| Testing | Week 7-8 | Full regression, soak tests | All phases |
| Hardening | Week 9-10 | Bug fixes, security review | Testing |

**Total Estimated Time**: 10 weeks to production readiness

---

## Release Checklist

### Pre-Release (Must Pass)

- [ ] All 8 P0 issues resolved and tested
- [ ] No data loss in any failure scenario
- [ ] Byzantine validators detected correctly
- [ ] Memory stable under 24-hour load test
- [ ] TxIndex properly pruned by GC
- [ ] Flow control prevents unbounded growth
- [ ] All unit tests pass with `-race`
- [ ] Integration tests pass
- [ ] Performance benchmarks meet targets (250k tx/sec)
- [ ] Metrics and logging operational
- [ ] Security review complete

### Release

1. [ ] Create release branch `release/v1.0.0`
2. [ ] Run full test suite
3. [ ] Run 24-hour soak test
4. [ ] Update version constants
5. [ ] Generate CHANGELOG
6. [ ] Tag release
7. [ ] Create GitHub release

---

*Last Updated: January 2026*
*Production Readiness: 65% → Target 100%*
