# Looseberry Progress Report

This document tracks the implementation progress of Looseberry, a DAG-based mempool module.

---

## Phase 1: Core Types & Interfaces

**Status:** Completed

### Summary

Phase 1 establishes the foundational types and interfaces for Looseberry. All core types have been implemented with comprehensive unit tests.

### Files Created

| File | Description |
|------|-------------|
| `go.mod` | Module definition with dependencies on cramberry, glueberry, blockberry |
| `Makefile` | Build targets: build, test, lint, integration-test |
| `config.go` | Configuration types and validation |
| `looseberry.go` | Main Looseberry struct and DAGMempool interface |
| `types/hash.go` | 32-byte SHA-256 hash type |
| `types/hash_test.go` | Hash unit tests |
| `types/transaction.go` | Transaction type ([]byte wrapper) |
| `types/transaction_test.go` | Transaction unit tests |
| `types/batch.go` | Batch type with digest computation |
| `types/batch_test.go` | Batch unit tests |
| `types/signature.go` | Ed25519 Signature and PublicKey types |
| `types/header.go` | DAG vertex header with signature |
| `types/header_test.go` | Header unit tests |
| `types/vote.go` | Vote on header |
| `types/vote_test.go` | Vote unit tests |
| `types/certificate.go` | Certificate (header + 2f+1 votes) with BitSet |
| `types/certificate_test.go` | Certificate and BitSet unit tests |
| `types/validator.go` | Validator struct and ValidatorSet interface |
| `types/validator_test.go` | ValidatorSet unit tests |
| `types/signer.go` | Signer interface and Ed25519 implementation |
| `types/signer_test.go` | Signer unit tests |
| `types/errors.go` | Error definitions with IsRetryable/IsByzantine helpers |
| `types/errors_test.go` | Error unit tests |
| `config_test.go` | Configuration validation tests |

### Key Functionality Implemented

1. **Hash Type** (`types/hash.go`)
   - 32-byte SHA-256 hash with `HashBytes()`, `HashConcat()` functions
   - JSON/text marshaling support
   - `HashFromBytes()`, `HashFromHex()` constructors

2. **Transaction Type** (`types/transaction.go`)
   - Simple `[]byte` wrapper with `Hash()`, `Size()`, `Clone()` methods
   - Format-agnostic (validation delegated to application)

3. **Batch Type** (`types/batch.go`)
   - Contains WorkerID, ValidatorID, Round, Transactions, Digest, Timestamp
   - Deterministic digest computation
   - BatchDigest reference type for headers

4. **Header Type** (`types/header.go`)
   - DAG vertex with Author, Round, Epoch, BatchRefs, Parents
   - Sign/Verify methods for Ed25519 signatures
   - CertificateRef for parent references

5. **Vote Type** (`types/vote.go`)
   - Vote on a header (HeaderDigest, Validator, Signature)
   - Sign/Verify methods

6. **Certificate Type** (`types/certificate.go`)
   - Header + 2f+1 Votes + SignerMask
   - `HasQuorum()` check for BFT (n ≥ 3f+1, quorum = 2f+1)
   - Full verification via `Verify(ValidatorSet)`
   - BitSet implementation for signer tracking

7. **ValidatorSet Interface** (`types/validator.go`)
   - `Count()`, `GetByIndex()`, `Contains()`, `F()`, `Quorum()`, `Epoch()`
   - `VerifySignature()` for signature verification
   - `SimpleValidatorSet` implementation for testing

8. **Signer Interface** (`types/signer.go`)
   - `Sign()`, `PublicKey()`, `ValidatorIndex()`
   - `Ed25519Signer` implementation
   - `GenerateEd25519Signer()` for testing

9. **Error Definitions** (`types/errors.go`)
   - 30+ error types covering all subsystems
   - `IsRetryable()` for transient errors
   - `IsByzantine()` for Byzantine behavior detection

10. **Configuration** (`config.go`)
    - WorkerConfig, PrimaryConfig, SyncConfig, GCConfig, FlowControlConfig
    - All configs have defaults and validation
    - TxValidator function type for CheckTx integration

11. **DAGMempool Interface** (`looseberry.go`)
    - `AddTx()`, `ReapCertifiedBatches()`, `NotifyCommitted()`
    - `UpdateValidatorSet()`, `HasTx()`, `Size()`, `SizeBytes()`
    - Stub implementation ready for Phase 2+

### Test Coverage

All types have comprehensive unit tests covering:
- Normal operation
- Edge cases (empty inputs, nil values)
- Error conditions
- Clone operations (ensuring deep copy)
- Cryptographic operations (sign/verify)
- Determinism (digest stability)

### Design Decisions

1. **Ed25519 Signatures**: Chose Ed25519 for its speed and security. Signature and PublicKey are fixed-size arrays for efficiency.

2. **BFT Calculation**: f = (n-1)/3, quorum = 2f+1. This matches standard BFT assumptions.

3. **Deterministic Digests**: Header and Batch digests are computed deterministically by sorting variable-length fields before hashing.

4. **Separated Types Package**: Core types in `types/` package to avoid circular dependencies with storage/network packages.

5. **TxValidator as Function Type**: Allows flexible integration with Application.CheckTx without hard dependency on blockberry.

---

## Phase 2: Storage Layer

**Status:** Completed

### Summary

Phase 2 implements the persistent and in-memory storage layer for batches, certificates, and transaction indexing. Both in-memory implementations (for testing) and LevelDB implementations (for production) are provided.

### Files Created

| File | Description |
|------|-------------|
| `store/store.go` | Storage interface definitions (BatchStore, CertificateStore, TxIndex) |
| `store/memory_batch.go` | In-memory BatchStore implementation |
| `store/memory_batch_test.go` | MemoryBatchStore unit tests |
| `store/memory_cert.go` | In-memory CertificateStore implementation |
| `store/memory_cert_test.go` | MemoryCertificateStore unit tests |
| `store/memory_txindex.go` | In-memory TxIndex implementation |
| `store/memory_txindex_test.go` | MemoryTxIndex unit tests |
| `store/leveldb_batch.go` | LevelDB BatchStore implementation |
| `store/leveldb_batch_test.go` | LevelDBBatchStore unit tests |
| `store/leveldb_cert.go` | LevelDB CertificateStore implementation |
| `store/leveldb_cert_test.go` | LevelDBCertificateStore unit tests |

### Key Functionality Implemented

1. **BatchStore Interface** (`store/store.go`)
   - `SaveBatch()`, `GetBatch()`, `HasBatch()`
   - `GetBatchesByRound()` for round-based queries
   - `DeleteBatchesBefore()` for garbage collection
   - `Close()` for cleanup

2. **CertificateStore Interface** (`store/store.go`)
   - `SaveCertificate()`, `GetCertificate()`, `HasCertificate()`
   - `GetCertificatesByRound()` for round-based queries
   - `GetCertificateForValidator()` for validator-specific lookups
   - `DeleteCertificatesBefore()` for garbage collection

3. **TxIndex Interface** (`store/store.go`)
   - `AddTx()`, `GetBatchForTx()`, `HasTx()` for O(1) tx lookup
   - `RemoveTxsForBatch()` for batch removal
   - `AddBatch()` helper for batch indexing

4. **In-Memory Implementations**
   - Thread-safe with RWMutex
   - Round-indexed for efficient queries
   - Suitable for testing
   - Deep copy on read to prevent mutations

5. **LevelDB Implementations**
   - Key schemas:
     - Batch: `B:{digest}` → batch data, `BR:{round}:{digest}` → index
     - Certificate: `C:{digest}` → cert data, `CR:{round}:{validator}` → digest
   - Atomic writes using LevelDB batches
   - Highest round tracking for metadata
   - Persistence verification tests

### Test Coverage

43 storage tests covering:
- Save and retrieve operations
- Has/existence checks
- Round-based queries
- Validator-specific queries
- Delete operations
- Idempotent operations
- Close behavior
- Persistence (LevelDB)
- Highest round tracking

### Design Decisions

1. **Interface-Based Design**: All stores implement interfaces for testability and flexibility.

2. **Gob Encoding**: Used encoding/gob for LevelDB serialization. Simple, handles Go types well, and sufficient for internal storage.

3. **Key Schema**: LevelDB keys use prefixes for namespace separation:
   - `B:` for batches, `BR:` for batch round index
   - `C:` for certificates, `CR:` for cert round/validator index
   - `BM:`, `CM:` for metadata

4. **Clone on Read**: All Get operations return clones to prevent callers from mutating stored data.

5. **Atomic Writes**: LevelDB uses write batches for atomic operations (data + indices).

6. **Bidirectional TxIndex**: Maintains both txHash→batchHash and batchHash→[]txHash mappings for efficient lookups and removals.

---

## Phase 3: Worker Implementation

**Status:** Completed

### Summary

Phase 3 implements the worker layer that handles transaction batching, deduplication, backpressure, and batch acknowledgment tracking.

### Files Created

| File | Description |
|------|-------------|
| `worker/worker.go` | Worker implementation with tx batching |
| `worker/worker_test.go` | Worker unit tests |
| `worker/ack_tracker.go` | Acknowledgment tracker for pending batches |
| `worker/ack_tracker_test.go` | AckTracker unit tests |
| `worker/pool.go` | Worker pool with hash-based tx routing |
| `worker/pool_test.go` | Pool unit tests |

### Key Functionality Implemented

1. **Worker Core** (`worker/worker.go`)
   - Transaction deduplication using hash set
   - Backpressure with configurable limits:
     - MaxPendingTxs (default: 10,000)
     - MaxPendingBytes (default: 50MB)
   - Automatic batch creation on timeout or size
   - TxValidator integration for CheckTx
   - Round and epoch tracking

2. **Batch Creation**
   - Trigger on BatchTimeout (default: 100ms) or BatchSize (default: 1000)
   - Automatic storage in BatchStore
   - Transaction indexing in TxIndex
   - BatchCallback notification for broadcasting

3. **Acknowledgment Tracker** (`worker/ack_tracker.go`)
   - Track pending batches awaiting acks
   - Record acks with quorum detection
   - Automatic timeout cleanup
   - Thread-safe with RWMutex

4. **Worker Pool** (`worker/pool.go`)
   - Hash-based transaction routing to workers
   - Dynamic scaling (ScaleUp/ScaleDown)
   - MinWorkers/MaxWorkers configuration
   - Shared state propagation (round, epoch, quorum)
   - Unified batch callback

### Test Coverage

39 worker tests covering:
- Transaction add with deduplication
- Backpressure (count and bytes limits)
- Batch creation on timeout and size
- Transaction validation
- Start/stop lifecycle
- Ack tracking and quorum detection
- Pool routing and scaling
- Concurrent operations with race detection

### Design Decisions

1. **Hash-Based Routing**: Transactions are routed to workers based on tx hash for deterministic distribution and natural deduplication.

2. **Configurable Limits**: Backpressure uses both count and byte limits to handle varying transaction sizes.

3. **Flush on Stop**: Workers create a final batch with pending transactions during shutdown.

4. **Separate AckTracker**: Decoupled from worker for testability and potential reuse.

5. **Pool Scaling**: Manual scale up/down methods for external control; auto-scaling deferred to Phase 8.

---

## Phase 4: Primary Implementation

**Status:** Completed

### Summary

Phase 4 implements the primary layer that handles header creation, vote collection, certificate formation, and header validation. The primary coordinates the DAG-based consensus by creating headers that reference batch digests and collecting votes to form certificates.

### Files Created

| File | Description |
|------|-------------|
| `primary/vote_tracker.go` | Vote tracker for pending headers and certificate formation |
| `primary/vote_tracker_test.go` | VoteTracker unit tests |
| `primary/primary.go` | Primary implementation with header loop and handlers |
| `primary/primary_test.go` | Primary unit tests |

### Key Functionality Implemented

1. **VoteTracker** (`primary/vote_tracker.go`)
   - Track pending headers waiting for votes
   - Record votes from validators (deduplicated)
   - Automatic certificate formation when quorum reached
   - Timeout detection for stale headers
   - Thread-safe with RWMutex

2. **Primary Core** (`primary/primary.go`)
   - Header creation loop with configurable timeout
   - Batch digest collection for headers
   - Parent certificate selection (quorum from previous round)
   - Round and epoch tracking
   - Start/Stop lifecycle management

3. **Vote Handling**
   - Vote signature verification
   - Pending vote buffering for unknown headers
   - Certificate storage on quorum
   - Callback notifications

4. **Header Handling**
   - Header signature verification
   - Round range validation (within MaxRoundGap)
   - Epoch validation
   - Parent certificate verification
   - Batch availability checking
   - Vote creation and sending

5. **Certificate Handling**
   - Certificate verification via ValidatorSet
   - Certificate storage
   - Round advancement on quorum certificates

6. **Configuration**
   - HeaderTimeout (default: 500ms)
   - MaxBatchesPerHeader (default: 100)
   - VoteTimeout (default: 30s)
   - MaxRoundGap (default: 10)

### Test Coverage

18 primary tests covering:
- Start/stop lifecycle
- Batch digest addition
- Header creation on timeout
- Vote handling and certificate formation
- Header validation (signature, round, epoch, parents)
- Pending vote buffering
- Round and epoch management
- Vote tracker operations (track, record, duplicate, timeout)
- Configuration defaults

### Design Decisions

1. **Periodic Header Creation**: Headers are created on a timer rather than event-driven. This simplifies the logic and ensures regular DAG progress.

2. **Pending Votes Buffer**: Votes can arrive before their headers (network reordering). These are buffered and processed when the header arrives.

3. **Callbacks for Extensibility**: HeaderCallback, VoteCallback, and CertificateCallback allow the network layer to handle distribution.

4. **Parent Selection**: Takes up to quorum certificates from the previous round, sorted by validator index for determinism.

5. **Batch Availability**: Headers with missing batches are not voted on. Full implementation would request missing batches.

6. **Separate VoteTracker**: Decoupled from Primary for testability and clarity. Handles the bookkeeping of votes → certificate formation.

---

## Phase 5: DAG Implementation

**Status:** Completed

### Summary

Phase 5 implements the certificate DAG (Directed Acyclic Graph) that maintains causal ordering of certificates across rounds. The DAG tracks certificates by round and validator, supports traversal of causal history, and provides deterministic certificate ordering.

### Files Created

| File | Description |
|------|-------------|
| `dag/dag.go` | DAG implementation with round management and causal history |
| `dag/dag_test.go` | DAG unit tests |

### Interface Update

| File | Change |
|------|--------|
| `store/store.go` | Added `HighestRound()` to CertificateStore interface |
| `store/memory_cert.go` | Implemented `HighestRound()` method |

### Key Functionality Implemented

1. **DAG Core** (`dag/dag.go`)
   - In-memory round-indexed certificate storage
   - Persistent storage fallback via CertificateStore
   - Highest round and committed round tracking
   - Thread-safe with RWMutex

2. **RoundData Structure**
   - Per-round certificate storage by validator index
   - Committed status tracking
   - Certificate count and existence queries

3. **Certificate Management**
   - `AddCertificate()`: Add with duplicate detection
   - `GetCertificate()`: Retrieve by digest
   - `HasCertificate()`: Existence check
   - `GetCertificatesForRound()`: All certs for a round
   - `GetCertificateForValidator()`: Specific validator's cert

4. **Round Advancement**
   - `CanAdvanceToRound()`: Check if quorum exists in previous round
   - `SetCommittedRound()`: Mark rounds as committed
   - Automatic highest round tracking

5. **Causal History**
   - `CausalHistory()`: BFS traversal of parent certificates
   - Result caching for performance
   - Configurable max depth to prevent unbounded traversal

6. **Certificate Ordering**
   - `GetOrderedCertificates()`: Deterministic ordering by round then validator
   - Used for block building and replay

7. **Memory Management**
   - `PruneRound()`: Remove single round from memory
   - `PruneRoundsBefore()`: Bulk prune old rounds
   - `LoadRound()`: Load from persistent storage
   - `LoadRoundsFrom()`: Bulk load from round

8. **Configuration**
   - MaxCachedRounds (default: 100)
   - MaxHistoryDepth (default: 1000)

### Test Coverage

22 DAG tests covering:
- Certificate addition (normal, duplicate, nil)
- Certificate retrieval (by digest, round, validator)
- Round advancement checks
- Committed round management
- Causal history traversal and caching
- Ordered certificate retrieval
- Memory pruning (single round, bulk)
- Loading from persistent storage
- Operation without persistent store

### Design Decisions

1. **Dual Storage**: Memory cache for fast access, persistent store for durability. GetCertificate falls back to store if not in memory.

2. **Round-Indexed Storage**: Primary index is by round for efficient round-based queries and GC.

3. **Causal History Caching**: BFS traversal results are cached to avoid repeated computation for the same certificate.

4. **Deterministic Ordering**: Within rounds, certificates are ordered by validator index for reproducible block building.

5. **Lazy Loading**: Rounds can be loaded from persistent storage on demand rather than all at startup.

6. **Committed Round Tracking**: Separate tracking allows GC decisions and flow control based on consensus progress.

---

## Phase 6: Network Protocol

**Status:** Completed

### Summary

Phase 6 implements the network protocol layer for communication between validators. This includes message types, network interface definitions, a mock network for testing, and a synchronization manager for catching up nodes that fall behind.

### Files Created

| File | Description |
|------|-------------|
| `network/network.go` | Network interface and message type definitions |
| `network/mock.go` | Mock network implementation for testing |
| `network/mock_test.go` | Mock network unit tests |
| `network/sync.go` | Sync manager for node synchronization |
| `network/sync_test.go` | Sync manager unit tests |

### Interface Update

| File | Change |
|------|--------|
| `types/validator.go` | Added `Validators()` to ValidatorSet interface |

### Key Functionality Implemented

1. **Message Types** (`network/network.go`)
   - BatchMessage, BatchAckMessage, BatchRequestMessage
   - HeaderMessage, VoteMessage, CertificateMessage
   - SyncRequest, SyncResponse

2. **Network Interface**
   - BroadcastBatch/Header/Certificate for all validators
   - SendVote/BatchAck/SyncRequest/SyncResponse for specific validators
   - Receive channels for each message type
   - Start/Stop lifecycle

3. **MockNetwork** (`network/mock.go`)
   - Full Network interface implementation
   - Peer connection/disconnection for multi-node testing
   - Statistics tracking (messages broadcast, sent)
   - Message injection for test scenarios
   - Thread-safe with RWMutex

4. **SyncManager** (`network/sync.go`)
   - RequestSync for requesting certificates from peers
   - HandleSyncRequest for serving sync requests
   - HandleSyncResponse for processing received certificates
   - CatchUp for synchronizing to target round
   - Automatic batch collection (certificates + referenced batches)
   - Pending request tracking with timeout cleanup
   - Periodic sync checks and cleanup loops

5. **Configuration**
   - Network: BufferSize (1000), SyncBatchSize (100), SyncTimeout (30s)
   - Sync: SyncInterval (10s), SyncThreshold (5), SyncBatchSize (100)

### Test Coverage

25 network tests covering:
- Start/stop lifecycle
- Peer connection/disconnection
- Batch/header/certificate broadcasting
- Vote and batch ack sending
- Sync request/response handling
- Unknown peer handling
- Message injection for testing
- Statistics tracking and reset
- Sync manager operations
- Catch-up scenarios

### Design Decisions

1. **Channel-Based Communication**: Messages are delivered via channels, decoupling network handling from processing.

2. **Mock Network for Testing**: Full implementation allows multi-node testing without real P2P infrastructure.

3. **Bidirectional Connection**: Connect() creates bidirectional peer links, matching real network behavior.

4. **Sync Includes Batches**: Sync responses include referenced batches to ensure receivers have complete data.

5. **Pending Request Tracking**: Prevents duplicate requests and enables timeout-based cleanup.

6. **Validators() Interface Addition**: Added to support iterating over validators for peer selection during sync.

### Note on External Integration

The Network interface is designed for integration with glueberry (P2P networking). The MockNetwork serves as both a testing tool and a reference implementation. Production integration would implement the Network interface wrapping glueberry's node API.

---

## Phase 7: Garbage Collection & Flow Control

**Status:** Completed

### Summary

Phase 7 implements garbage collection for old rounds and flow control to prevent unbounded DAG growth. The GC manager prunes old data while recovering uncommitted transactions, and the flow controller pauses header creation when the system falls too far behind.

### Files Created

| File | Description |
|------|-------------|
| `gc/gc.go` | GC manager for round pruning and transaction recovery |
| `gc/gc_test.go` | GC manager unit tests |
| `gc/flow.go` | Flow controller for preventing runaway round advancement |
| `gc/flow_test.go` | Flow controller unit tests |

### Key Functionality Implemented

1. **GCManager** (`gc/gc.go`)
   - NotifyCommitted for triggering GC on consensus commit
   - Automatic pruning of rounds older than GCDepth
   - Transaction recovery from uncommitted batches
   - Background GC loop with configurable interval
   - ForceGC for immediate garbage collection
   - Prunes from DAG memory and persistent storage

2. **Transaction Recovery**
   - Extracts transactions from batches not in committed certificates
   - Callback mechanism for re-injecting recovered transactions
   - Prevents transaction loss during round pruning

3. **FlowController** (`gc/flow.go`)
   - Tracks gap between current and committed rounds
   - CanCreateHeader/CanAdvanceRound for flow control checks
   - Automatic pause when gap exceeds MaxUncommittedRounds
   - Automatic resume when consensus catches up
   - Pause/resume callbacks for system notifications
   - Metrics for monitoring (pause count, resume count, gap)

4. **Configuration**
   - GC: GCDepth (100), GCInterval (30s), RecoverUncommittedTxs (true)
   - Flow: MaxUncommittedRounds (100), MaxPendingBatches (1000), MaxPendingHeaders (100)

### Test Coverage

18 GC tests covering:
- Start/stop lifecycle
- NotifyCommitted triggering GC
- Round pruning from DAG and storage
- Transaction recovery from uncommitted batches
- Background GC loop
- ForceGC immediate operation
- Flow control pause/resume
- Uncommitted gap calculations
- Metrics tracking
- Multiple pause-resume cycles

### Design Decisions

1. **Consensus-Driven GC**: GC is triggered by consensus commits, not time-based, ensuring safety.

2. **Transaction Recovery**: Uncommitted transactions are recovered before pruning to prevent loss.

3. **Callback-Based Recovery**: TxRecoveryCallback allows flexible handling (re-batching, logging, etc.).

4. **Atomic Flow Control**: Uses atomic operations for thread-safe round tracking without locks.

5. **Pause/Resume Callbacks**: System can react to flow control state changes (logging, metrics, etc.).

6. **Configurable Depth**: GCDepth allows tuning retention vs memory usage trade-off.

---

## Phase 8: Dynamic Worker Scaling

**Status:** Completed

### Summary

Phase 8 implements automatic worker pool scaling based on load metrics. The scaler monitors the ratio of pending transactions to worker capacity and scales up when overloaded or down when underutilized, with cooldown periods to prevent thrashing.

### Files Created

| File | Description |
|------|-------------|
| `worker/scaler.go` | Auto-scaler with load-based scaling and metrics |
| `worker/scaler_test.go` | Scaler unit tests |

### Key Functionality Implemented

1. **Load Calculation** (`worker/scaler.go`)
   - Load = pending transactions / (worker count × batch size)
   - Tracks current capacity vs demand
   - Returns 0 when no workers or no capacity

2. **Automatic Scaling**
   - Scale up when load > ScaleUpThreshold (default: 0.8)
   - Scale down when load < ScaleDownThreshold (default: 0.2)
   - Background scaling loop at configurable interval
   - Respects pool min/max worker limits

3. **Cooldown Protection**
   - ScaleCooldown prevents rapid scaling (default: 5s)
   - Prevents oscillation during load fluctuations
   - Last scale time tracked per operation

4. **Scale Event History**
   - Ring buffer of recent scaling events
   - Each event records: time, old/new workers, load, direction
   - Configurable max events (default: 100)

5. **Metrics Collection**
   - CurrentWorkers, CurrentLoad
   - ScaleUpCount, ScaleDownCount
   - LastScaleTime
   - RecentEvents for history

6. **Callbacks**
   - SetScaleUpCallback for scale-up notifications
   - SetScaleDownCallback for scale-down notifications
   - Thread-safe callback registration and invocation

7. **Manual Scaling**
   - ForceScaleUp for testing/override
   - ForceScaleDown for testing/override
   - Both update metrics and invoke callbacks

8. **Configuration**
   - ScaleUpThreshold (default: 0.8 = 80% capacity)
   - ScaleDownThreshold (default: 0.2 = 20% capacity)
   - ScalingInterval (default: 1s)
   - ScaleCooldown (default: 5s)

### Test Coverage

11 scaler tests covering:
- Start/stop lifecycle
- Load calculation with various scenarios
- Automatic scale-up on high load
- Automatic scale-down on low load
- Cooldown enforcement
- Force scale up/down operations
- Metrics collection
- Event history tracking
- Callback invocation
- Configuration defaults validation

### Design Decisions

1. **Capacity-Based Load**: Load is calculated as demand / capacity, providing a normalized metric regardless of worker count.

2. **Asymmetric Thresholds**: Scale-up threshold (0.8) is higher than scale-down threshold (0.2) to create a stability band and prevent oscillation.

3. **Cooldown per Operation**: Each scaling operation (up or down) updates the cooldown timer, preventing rapid successive operations.

4. **Thread-Safe Callbacks**: Callbacks are protected by RWMutex to allow safe registration while scaler is running.

5. **Ring Buffer Events**: Bounded event history prevents unbounded memory growth while preserving recent history.

6. **Delegation to Pool**: Scaler delegates actual scaling to Pool.ScaleUp/ScaleDown, maintaining separation of concerns.

---

*Next Phase: Phase 9 - Blockberry Integration*
