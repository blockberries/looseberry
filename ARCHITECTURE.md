# Looseberry — Architecture

## Goals

1. **Decouple dissemination from ordering**. Transactions get to all
   validators (via batches + certificates) before consensus orders them
   (via leaderberry).
2. **Parallel batching**. N workers, hash-routed, run independently.
3. **Causal DAG of certificates** rather than a single chain — many
   batches per round, multiple proposers per round.
4. **Fail-fast on Byzantine equivocation** at the vote-set level.

The design follows Narwhal/Bullshark with a few simplifications:
no consensus protocol embedded (leaderberry handles that); no signature
aggregation (every cert has individual signatures).

## Core types

```go
type Hash [32]byte                    // value type, safe for concurrent reads
type Transaction struct { Data []byte; Hash Hash }
type Batch struct {
    Transactions []Transaction
    Digest       Hash       // SHA256(...) of contents
    Round        uint64
    Author       uint16     // validator index
}
type Header struct {
    Round            uint64
    Author           uint16
    BatchRefs        []Hash      // digests of included batches
    ParentCertRefs   []Hash      // 2f+1 certs from round-1
    Timestamp        int64
    Digest           Hash
    Signature        []byte
}
type Certificate struct {
    Header
    Votes      []Signature   // one per voter
    SignerMask BitSet        // 256-bit mask for fast HasVoteFrom
}
```

## Tx pipeline

```
[client] Looseberry.AddTx(tx)
  → flowController.IsPaused()? → ErrFlowControlPaused
  → workerPool.AddTx(tx)
    → worker = workers[hash(tx) % len(workers)]
    → worker.AddTx(tx)
      → txValidator(tx)?      → reject
      → pendingSet[hash]?     → dedup
      → txIndex.HasTx(hash)?  → cross-worker dedup
      → backpressure?         → ErrWorkerBackpressure
      → pending = append(pending, tx)
      → if len(pending) >= BatchSize || bytes >= BatchBytes → trigger

[worker] batchLoop
  → on tick / trigger / stop
    → tryCreateBatch() → batchStore.SaveBatch → txIndex.AddBatch
    → ackTracker.TrackBatch
    → batchCallback(batch)  → Looseberry.onBatchCreated
                             → primary.AddBatchDigest(batch.Digest)
                             → network.BroadcastBatch(batch)
```

## Certificate formation

```
[primary] tryCreateHeader (ticker, 500 ms)
  → drain queued batchDigests (cap MaxBatchesPerHeader=100)
  → parents = certStore.GetCertificatesByRound(round-1) sorted, capped to quorum
  → if round > 0 && empty → skip
  → header = NewHeader(round, author, batchDigests, parents, sign)
  → voteTracker.TrackHeader(header)
  → self-vote → if threshold==1 → form certificate immediately
  → primary.HandleVote(self.Vote)
  → process pending votes (received before header was known)
  → headerCallback(header) → network.BroadcastHeader
```

## DAG advancement

```
[primary] HandleVote(vote)
  → validatorSet.GetByIndex
  → ed25519.Verify
  → voteTracker.RecordVote
    → if duplicate → noop
    → if conflicting → record DoubleVoteEvidence
    → if 2f+1 reached → formCertificateLocked → certStore.SaveCertificate → DAG.AddCertificate

[primary] tryAdvanceRound
  → certStore.GetCertificatesByRound(currentRound)
  → if 2f+1 → currentRound++ → tryCreateHeader (new round)
```

## DAG

`dag.DAG` keeps in-memory rounds plus a hash index:

- One certificate per `(round, author)` enforced via
  `RoundData.Certificates[author]`. Second insert → `ErrDuplicateHeader`.
- Parent existence checked under `roundsMu` against in-memory cache,
  falling through to `certStore`.
- LRU `causalHistoryCache` for `MaxHistoryDepth=1000` BFS queries.
- `SetCommittedRound` marks committed; pruning happens via `gcManager`.

## GC + tx recovery

`GCManager.NotifyCommitted(round)`:

1. Update committed pointer.
2. Compute `gcRound = committed - GCDepth` (default 50).
3. `extractUncommittedTxs(gcRound)` — find batches in rounds < gcRound
   that are NOT in any committed certificate. Recover their txs by
   calling the recovery callback (re-injects into worker pool).
4. `dag.PruneRoundsBefore(gcRound)`.
5. `batchStore.DeleteBatchesBefore(gcRound)`.
6. `certStore.DeleteCertificatesBefore(gcRound)`.
7. `txIndex.PruneOlderThan(gcRound, batchStore)`.

The fast path uses a `gc.uncommittedBatches` map indexed at batch
creation; this index is currently never populated, so the fallback scan
runs. PLAN B3-fixes this.

## Flow control

`gc.FlowController` gates `AddTx` when the uncommitted gap exceeds
`MaxUncommittedRounds=100`. Returns `ErrFlowControlPaused`, retryable.
The pending-batches and pending-headers counters are defined but never
updated; only the round-gap signal is active.

## ReapCertifiedBatches

Called by raspberry's BlockExecutor:

1. Determine `fromRound`: `committedRound + 1` if anything has committed,
   else `0`.
2. Pull `dag.GetOrderedCertificates(fromRound..highestRound)`.
3. For each cert, for each `BatchRef`: `batchStore.GetBatch(digest)`.
4. Stop when `totalBytes + batchSize > maxBytes` (always include the
   first oversized batch).

Returns `[]CertifiedBatch{Batch, Certificate}` — the same digest may
appear in multiple certificates and is **not deduplicated** (PLAN T2-5).

## Network

`network.Network` is an interface. Methods (abridged):

```go
BroadcastBatch(batch *types.Batch) error
SendBatchAck(to uint16, ack *BatchAckMessage) error
BroadcastHeader(header *types.Header) error
BroadcastVote(vote *types.Vote) error
BroadcastCertificate(cert *types.Certificate) error
SendBatchRequest(to uint16, req *BatchRequest) error
SendBatchResponse(to uint16, resp *BatchResponse) error
SendSyncRequest(to uint16, req *SyncRequest) error
SendSyncResponse(to uint16, resp *SyncResponse) error
BatchMessages() <-chan IncomingBatch
HeaderMessages() <-chan IncomingHeader
VoteMessages() <-chan IncomingVote
CertificateMessages() <-chan IncomingCertificate
BatchAckMessages() <-chan IncomingBatchAck
SyncRequestMessages() <-chan *SyncRequest
SyncResponseMessages() <-chan *SyncResponse
```

`MockNetwork` (in-tree) connects N peers via Go channels.

`SyncManager` (`network/sync.go`):

- `RequestSync(target)` — send SyncRequest to a peer.
- `HandleSyncRequest` — assemble response from DAG + batch store.
- `HandleSyncResponse` — verify certs, store them.
- `checkAndSync` (auto-driver) — **placeholder**, see PLAN T1-5.

## Stores

- `MemoryBatchStore`, `LevelDBBatchStore` (gob-encoded values + atomic
  write batches + per-round indices).
- `MemoryCertificateStore`, `LevelDBCertificateStore`.
- `MemoryTxIndex` only; restart loses dedup state until re-sync.

## Worker scaling

`worker.Scaler` (`worker/scaler.go`):

- Ticker on `ScalingInterval`.
- `Pool.CalculateLoad()` returns `max(countLoad, byteLoad)` relative to
  current worker count.
- `load > 0.8` and not in cooldown → `Pool.ScaleUp`.
- `load < 0.2` and not in cooldown → `Pool.ScaleDown`.
- Cooldown 5 s.

Scale-down: drain dropped worker's pending, redistribute by new modulus.
Tx hash → worker mapping is **not stable** across scaling events. Current
deduplication (txIndex.HasTx + per-worker pendingSet) catches re-adds.

## Layout

```
looseberry/
├── looseberry.go          Orchestrator: AddTx, ReapCertifiedBatches, NotifyCommitted, message loop
├── config.go              Config tree + Validate() per section
├── types/
│   ├── hash.go            32-byte value type
│   ├── transaction.go
│   ├── batch.go           Digest computation
│   ├── header.go
│   ├── vote.go
│   ├── certificate.go     With BitSet for HasVoteFrom
│   ├── validator.go       ValidatorSet, quorum math
│   ├── signer.go          Ed25519 signer abstraction
│   └── bitset.go          256-bit BitSet
├── worker/
│   ├── worker.go          Pending queue + batch loop
│   ├── pool.go            Hash-routed worker pool
│   ├── scaler.go          Load-based scaling
│   └── ack_tracker.go     Per-batch ack quorum tracking
├── primary/
│   ├── primary.go         Header creation, vote handling
│   ├── vote_tracker.go    VoteSet with double-vote evidence
│   └── batch_fetcher.go   Pull missing batches (NOT YET INSTANTIATED)
├── dag/
│   └── dag.go             Causal graph
├── gc/
│   ├── gc.go              GCManager
│   └── flow.go            FlowController
├── network/
│   ├── network.go         Network interface
│   ├── mock.go            MockNetwork (tests)
│   ├── messages.go        BatchAckMessage, BatchRequest/Response, SyncRequest/Response
│   └── sync.go            SyncManager (CatchUp partial)
├── store/
│   ├── batch_store.go         + memory_batches.go, leveldb_batches.go
│   ├── certificate_store.go   + memory_certs.go, leveldb_certs.go
│   └── tx_index.go            (memory only)
└── test/                  Multi-node integration harness
```
