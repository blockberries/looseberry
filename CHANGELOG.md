# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-05-21

### Added

- `types.TxAdmission{Priority int64, Sender string}` returned by
  `TxValidator` to drive priority-fee aware admission.
- `worker.priorityQueue` — `container/heap` backing `Worker.pending`,
  ordered `(Priority DESC, pendingSeq ASC)` for max-priority-first
  inclusion with FIFO tiebreak.
- `Looseberry.RequestPeerCatchUp` and `Looseberry.CatchUpPeerCerts`
  for two-sided recovery when a new peer registers.
- `syncIfStuck` helper in the rebroadcast loop fires a from-round-0
  sync request if the primary stays at round 0 past a 2s grace.
- `LevelDBTxIndex` persisting dedup state across restarts (B3
  finalization).
- `SyncManager.checkAndSync` periodic catchup driver replacing the
  prior placeholder.
- Tests `TestWorkerPriorityOrdering` and
  `TestWorkerPriorityFIFOTiebreak`.

### Changed

- **BREAKING**: `TxValidator` signature is now
  `func([]byte) (types.TxAdmission, error)` (previously
  `func([]byte) error`). Direct consumers must update; apps that
  don't care about priority return the zero `TxAdmission` for FIFO.
- `Worker.pending` is a heap, not a slice. `AddTx` pushes;
  `tryCreateBatch` pops top-N respecting `BatchSize` / `BatchBytes`.
- `DrainPending` returns entries in unspecified order; only used for
  scale-down rerouting, not inclusion.
- `BatchAcks` are now signed
  `Ed25519(privKey, SHA256(digest || idx || round))` and verified
  before `RecordAck` (B3 finalization).
- `ReapCertifiedBatches` dedupes by digest within a single call.
- Stores use cramberry instead of gob (`store/leveldb_*.go`).
- `SyncManager.HandleSyncResponse` routes received certs through the
  certificate callback (so the primary sees them and can advance) and
  continues past per-cert verify failures rather than aborting.
- `checkAndSync` and `CatchUp` use `fromRound=0` when `localRound=0`.

### Performance

- `LevelDBTxIndex.AddBatch` per-tx `Get` loop removed (was ~14% of
  total CPU at 6.8K TPS sustained — 1000 Gets per 1000-tx batch).
  Replaced with a single batch-level Get on `TR:{batchHash}`: if
  already indexed the call is a no-op, else all keys are written in
  one atomic batch. Cross-batch duplicate txs have their `txToBatch`
  mapping written by the later batch; `HasTx` returns true either way
  and `PruneOlderThan` iterates `batchRounds` not `txToBatch`.
- Measured: 30k-v0-only +12%, 200k-v0-only stable +45% over previous
  best (the bimodal collapse is gone).

### Fixed

- PLAN §E7c v3-stuck race in raspberry-orchestrated multi-validator
  startup. A late-joining primary could park at round 0 because peers
  had advanced and stopped re-broadcasting their round-0 certs. Fix
  is two-sided: push-side re-broadcast of the local DAG on peer
  registration (`CatchUpPeerCerts`); pull-side from-round-0 sync
  request on the same trigger (`RequestPeerCatchUp`).
- Sync layer didn't route synced certificates to the certificate
  store, so the primary couldn't advance even when peers replied
  with the full DAG. `HandleSyncResponse` now uses the same
  certificate callback the network loop uses.
- Sync requests skipped round 0 when `localRound=0`, so a fresh node
  could never bootstrap from a from-round-0 response. Fixed in
  `checkAndSync` and `CatchUp`.

### Reverted

- Pebble migration experiment. `cockroachdb/pebble` regressed every
  sweep scenario by 3-95% (200k-v0-only collapsed -95%). Pebble's
  design (group-commit, concurrent writers, larger DBs) doesn't fit
  looseberry's profile (small DBs, few writer goroutines). Reverted
  to leveldb.

## [1.0.0] - 2026-01-29

### Added

- **Core Types & Interfaces**
  - Transaction, Batch, Header, Certificate, Vote types
  - Ed25519 cryptographic signing and verification
  - Hash utilities and type-safe wrappers

- **Storage Layer**
  - BatchStore interface with memory and LevelDB implementations
  - CertificateStore interface with memory and LevelDB implementations
  - TxIndex for O(1) transaction lookup

- **Worker Implementation**
  - Worker pool with configurable batch creation triggers
  - Transaction deduplication and back-pressure handling
  - Acknowledgment tracking for batch availability
  - Dynamic worker scaling based on load

- **Primary Layer**
  - Header creation with batch references and parent certificates
  - Vote collection and certificate formation (2f+1 quorum)
  - Empty headers for liveness (configurable)
  - Parent selection algorithm (sorted by validator index)

- **DAG (Directed Acyclic Graph)**
  - Round-indexed certificate storage
  - Causal history traversal (BFS)
  - Support for certificate pruning

- **Network Protocol**
  - Network interface for validator communication
  - Sync manager for certificate synchronization
  - Message types: Batch, Header, Vote, Certificate, SyncRequest, SyncResponse

- **Garbage Collection**
  - Consensus-driven GC based on committed rounds
  - Transaction recovery from uncommitted batches
  - Configurable GC depth

- **Flow Control**
  - Prevention of unbounded DAG growth
  - MaxUncommittedRounds threshold
  - Automatic pause/resume of header creation

- **DAGMempool Interface**
  - AddTx with TxValidator integration
  - ReapCertifiedBatches with deterministic ordering
  - NotifyCommitted for GC triggering
  - UpdateValidatorSet for epoch changes

- **Testing**
  - Comprehensive unit tests for all packages
  - Race detection enabled in test suite
  - Benchmark suite for performance testing

### Security

- Certificate signature verification on sync responses
- Improved hash-based worker routing (8 bytes instead of 1)
- Protected ValidatorSet access with RWMutex
- Nil checks on all public API entry points
- Bounded pendingVotes map with cleanup

### Fixed

- SyncManager goroutine leak on shutdown
- Components unable to restart after Stop()
- Race condition in SyncManager.UpdateValidatorSet
- Race condition in Primary.UpdateValidatorSet
- GC inefficiency starting from round 0
- Data isolation issues in DAG certificate retrieval
- Data isolation in AckTracker.GetPending

### Documentation

- Comprehensive ARCHITECTURE.md with protocol specification
- README with usage examples and API documentation
- CODE_REVIEW.md tracking all identified and fixed issues
- PROGRESS_REPORT.md with implementation history
