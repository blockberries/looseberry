# Looseberry Implementation Plan

This document outlines the phased implementation plan for Looseberry, a DAG-based mempool module.

## Implementation Phases

| Phase | Description | Dependencies |
|-------|-------------|--------------|
| 1 | Core Types & Interfaces | None |
| 2 | Storage Layer | Phase 1 |
| 3 | Worker Implementation (with dedup/backpressure) | Phase 1, 2 |
| 4 | Primary Implementation (with out-of-order handling) | Phase 1, 2, 3 |
| 5 | DAG Implementation | Phase 1, 2 |
| 6 | Network Protocol (with sync) | Phase 1-5 |
| 7 | Garbage Collection & Flow Control | Phase 1-5 |
| 8 | Dynamic Worker Scaling | Phase 3 |
| 9 | Blockberry Integration (with startup/recovery, epochs) | Phase 1-7 |
| 10 | Testing & Benchmarks | Phase 1-9 |

---

## Phase 1: Core Types & Interfaces

**Goal**: Define all core types, interfaces, and error definitions.

### Task 1.1: Project Setup
- [ ] Initialize go.mod with `github.com/blockberries/looseberry`
- [ ] Add dependencies: cramberry, glueberry (local), blockberry (local)
- [ ] Create directory structure:
  ```
  looseberry/
  ├── config.go
  ├── errors.go
  ├── looseberry.go
  ├── types/
  │   ├── types.go
  │   ├── hash.go
  │   ├── batch.go
  │   ├── header.go
  │   ├── certificate.go
  │   └── validator.go
  ├── worker/
  ├── primary/
  ├── dag/
  ├── store/
  ├── network/
  └── schema/
  ```
- [ ] Create Makefile with targets: build, test, lint, integration-test

### Task 1.2: Hash Type
- [ ] Define `Hash` type (32-byte SHA-256)
- [ ] Implement `Bytes()`, `String()`, `Equal()` methods
- [ ] Implement `HashBytes(data []byte) Hash` function
- [ ] Implement `HashConcat(left, right Hash) Hash` for Merkle trees
- [ ] Write unit tests

### Task 1.3: Transaction Type
- [ ] Define `Transaction` as `[]byte` alias
- [ ] Implement `Hash()` method
- [ ] Implement `Size()` method
- [ ] Write unit tests

### Task 1.4: Batch Type
- [ ] Define `Batch` struct:
  ```go
  type Batch struct {
      WorkerID     uint16
      ValidatorID  uint16
      Round        uint64
      Transactions []Transaction
      Digest       Hash
      Timestamp    int64
  }
  ```
- [ ] Implement `ComputeDigest()` method
- [ ] Implement `Size()` method (total bytes)
- [ ] Implement cramberry marshaling
- [ ] Write unit tests

### Task 1.5: Header Type
- [ ] Define `Header` struct:
  ```go
  type Header struct {
      Author    uint16
      Round     uint64
      Epoch     uint64
      BatchRefs []BatchDigest
      Parents   []CertificateRef
      Timestamp int64
      Digest    Hash
      Signature Signature
  }
  ```
- [ ] Implement `ComputeDigest()` method (excludes Signature)
- [ ] Implement `Sign(signer Signer)` method
- [ ] Implement cramberry marshaling
- [ ] Write unit tests

### Task 1.6: Vote Type
- [ ] Define `Vote` struct:
  ```go
  type Vote struct {
      HeaderDigest Hash
      Validator    uint16
      Signature    Signature
  }
  ```
- [ ] Implement cramberry marshaling
- [ ] Write unit tests

### Task 1.7: Certificate Type
- [ ] Define `Certificate` struct:
  ```go
  type Certificate struct {
      Header     Header
      Votes      []Vote
      SignerMask BitSet
  }
  ```
- [ ] Implement `Digest()` method (returns Header.Digest)
- [ ] Implement `HasQuorum(validatorSet ValidatorSet)` method
- [ ] Implement cramberry marshaling
- [ ] Write unit tests

### Task 1.8: ValidatorSet Interface
- [ ] Define `ValidatorSet` interface:
  ```go
  type ValidatorSet interface {
      Count() int
      GetByIndex(index uint16) *Validator
      Contains(index uint16) bool
      F() int                    // (n-1)/3
      Quorum() int               // 2f+1
      Epoch() uint64
      VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool
  }
  ```
- [ ] Define `Validator` struct
- [ ] Implement default `ValidatorSetImpl` for testing
- [ ] Write unit tests

### Task 1.9: Signer Interface
- [ ] Define `Signer` interface:
  ```go
  type Signer interface {
      Sign(digest Hash) (Signature, error)
      PublicKey() PublicKey
      ValidatorIndex() uint16
  }
  ```
- [ ] Implement Ed25519 signer
- [ ] Write unit tests

### Task 1.10: Error Definitions
- [ ] Define error types in `errors.go`:
  - `ErrTxAlreadyExists`
  - `ErrMempoolFull`
  - `ErrInvalidBatch`
  - `ErrInvalidHeader`
  - `ErrInvalidCertificate`
  - `ErrInvalidSignature`
  - `ErrInvalidVote`
  - `ErrRoundMismatch`
  - `ErrMissingParents`
  - `ErrDuplicateHeader`
  - `ErrInsufficientQuorum`
  - `ErrValidatorNotFound`
  - `ErrBatchNotFound`
  - `ErrCertificateNotFound`
  - `ErrWorkerBackpressure`
  - `ErrTxValidationFailed`
  - `ErrFlowControlPaused`
  - `ErrEpochMismatch`
- [ ] Write unit tests for error matching

### Task 1.12: TxValidator Type
- [ ] Define `TxValidator` function type for CheckTx integration:
  ```go
  // TxValidator validates transactions before batching.
  // Wraps Application.CheckTx for pre-batch validation.
  type TxValidator func(tx []byte) error
  ```
- [ ] Add TxValidator field to Config struct
- [ ] Write unit tests with mock validator

### Task 1.11: Configuration
- [ ] Define `Config` struct with all configuration options
- [ ] Define `WorkerConfig`, `PrimaryConfig`, `GCConfig` structs
- [ ] Implement `DefaultConfig()` with sensible defaults
- [ ] Implement configuration validation
- [ ] Write unit tests

---

## Phase 2: Storage Layer

**Goal**: Implement persistent storage for batches, headers, and certificates.

### Task 2.1: Storage Interfaces
- [ ] Define `BatchStore` interface:
  ```go
  type BatchStore interface {
      SaveBatch(batch *Batch) error
      GetBatch(digest Hash) (*Batch, error)
      HasBatch(digest Hash) bool
      GetBatchesByRound(round uint64) ([]*Batch, error)
      DeleteBatchesBefore(round uint64) error
      Close() error
  }
  ```
- [ ] Define `CertificateStore` interface:
  ```go
  type CertificateStore interface {
      SaveCertificate(cert *Certificate) error
      GetCertificate(digest Hash) (*Certificate, error)
      HasCertificate(digest Hash) bool
      GetCertificatesByRound(round uint64) ([]*Certificate, error)
      GetCertificatesForValidator(round uint64, validator uint16) (*Certificate, error)
      DeleteCertificatesBefore(round uint64) error
      Close() error
  }
  ```

### Task 2.2: In-Memory Batch Store
- [ ] Implement `MemoryBatchStore` for testing
- [ ] Thread-safe with RWMutex
- [ ] Round-indexed for efficient queries
- [ ] Write unit tests

### Task 2.3: In-Memory Certificate Store
- [ ] Implement `MemoryCertificateStore` for testing
- [ ] Thread-safe with RWMutex
- [ ] Round-indexed storage
- [ ] Write unit tests

### Task 2.4: LevelDB Batch Store
- [ ] Implement `LevelDBBatchStore` using blockberry's LevelDB patterns
- [ ] Key schema: `B:{digest}` -> batch data
- [ ] Index: `BR:{round}:{digest}` -> empty (for round queries)
- [ ] Metadata: `BM:highest_round` -> round number
- [ ] Write unit tests

### Task 2.5: LevelDB Certificate Store
- [ ] Implement `LevelDBCertificateStore`
- [ ] Key schema: `C:{digest}` -> certificate data
- [ ] Index: `CR:{round}:{validator}` -> digest
- [ ] Metadata: `CM:highest_round` -> round number
- [ ] Write unit tests

### Task 2.6: Transaction Index
- [ ] Implement `TxIndex` for fast tx lookup:
  ```go
  type TxIndex interface {
      AddTx(txHash, batchHash Hash) error
      GetBatchForTx(txHash Hash) (Hash, error)
      HasTx(txHash Hash) bool
      RemoveTxsForBatch(batchHash Hash) error
  }
  ```
- [ ] In-memory implementation
- [ ] Write unit tests

---

## Phase 3: Worker Implementation

**Goal**: Implement transaction batching and dissemination.

### Task 3.1: Worker Core
- [ ] Implement `Worker` struct:
  ```go
  type Worker struct {
      id            uint16
      validatorID   uint16
      cfg           *WorkerConfig

      pending        []Transaction
      pendingSet     map[Hash]bool  // O(1) deduplication
      pendingMu      sync.Mutex
      pendingBytes   int64

      // Limits (for back-pressure)
      maxPendingTxs   int    // Default: 10000
      maxPendingBytes int64  // Default: 50MB

      ackTracker    *AckTracker
      batchOutput   chan<- *BatchDigest

      network       Network
      validatorSet  ValidatorSet

      running       atomic.Bool
      stopCh        chan struct{}
  }
  ```
- [ ] Implement `AddTx(tx Transaction) error` with deduplication and back-pressure:
  ```go
  func (w *Worker) AddTx(tx Transaction) error {
      w.pendingMu.Lock()
      defer w.pendingMu.Unlock()

      txHash := tx.Hash()

      // Deduplication check
      if w.pendingSet[txHash] {
          return nil  // Already have it
      }

      // Back-pressure check
      if len(w.pending) >= w.maxPendingTxs ||
         w.pendingBytes >= w.maxPendingBytes {
          return ErrWorkerBackpressure
      }

      w.pending = append(w.pending, tx)
      w.pendingSet[txHash] = true
      w.pendingBytes += int64(tx.Size())
      return nil
  }
  ```
- [ ] Implement batch creation loop
- [ ] Implement graceful shutdown
- [ ] Write unit tests for deduplication
- [ ] Write unit tests for back-pressure
- [ ] Write unit tests for normal flow

### Task 3.2: Batch Creation
- [ ] Implement batch creation logic:
  - Trigger on size threshold OR timeout
  - Compute batch digest
  - Store batch locally
- [ ] Implement `createBatch() *Batch`
- [ ] Write unit tests

### Task 3.3: Acknowledgment Tracker
- [ ] Implement `AckTracker`:
  ```go
  type AckTracker struct {
      pending  map[Hash]*PendingBatch
      mu       sync.RWMutex
  }

  type PendingBatch struct {
      Batch     *Batch
      Acks      map[uint16]bool  // validator -> acked
      CreatedAt time.Time
  }
  ```
- [ ] Implement `TrackBatch(batch *Batch)`
- [ ] Implement `RecordAck(batchDigest Hash, validator uint16) bool` (returns true if quorum reached)
- [ ] Implement timeout cleanup
- [ ] Write unit tests

### Task 3.4: Worker Pool
- [ ] Implement `WorkerPool`:
  ```go
  type WorkerPool struct {
      workers     []*Worker
      workersMu   sync.RWMutex
      workerCount atomic.Int32

      cfg         *WorkerConfig
      validatorID uint16

      batchOutput chan *BatchDigest

      running     atomic.Bool
  }
  ```
- [ ] Implement `AddTx(tx Transaction) error` with hash-based routing
- [ ] Implement `Start()` / `Stop()`
- [ ] Write unit tests

### Task 3.5: Worker Message Handlers
- [ ] Implement handlers for:
  - `BatchMessage`: Store batch, send ack
  - `BatchAckMessage`: Record ack in tracker
  - `BatchRequestMessage`: Send requested batch
- [ ] Write unit tests

---

## Phase 4: Primary Implementation

**Goal**: Implement header creation, voting, and certificate formation.

### Task 4.1: Primary Core
- [ ] Implement `Primary` struct:
  ```go
  type Primary struct {
      validatorID   uint16
      signer        Signer
      cfg           *PrimaryConfig

      currentRound  atomic.Uint64
      roundMu       sync.Mutex

      batchDigests  []BatchDigest
      digestsMu     sync.Mutex

      voteTracker   *VoteTracker

      // Out-of-order message handling
      pendingVotes  map[Hash][]Vote       // Votes for unknown headers
      pendingCerts  map[Hash]*Certificate // Certs with missing parents
      pendingMu     sync.Mutex
      voteExpiry    time.Duration         // Default: 30s

      // Batch availability checking
      batchChecker  BatchAvailabilityChecker

      // Flow control
      flowCtrl      *FlowController

      dag           *DAG
      network       Network
      validatorSet  ValidatorSet

      certOutput    chan<- *Certificate

      running       atomic.Bool
  }
  ```
- [ ] Implement round advancement logic
- [ ] Implement `tryAdvanceRound()`:
  ```go
  func (p *Primary) tryAdvanceRound() {
      currentRound := p.currentRound.Load()
      certs := p.dag.GetCertificatesForRound(currentRound)

      if len(certs) >= p.validatorSet.Quorum() {
          p.currentRound.Store(currentRound + 1)
          p.createHeader()
      }
  }
  ```
- [ ] Write unit tests

### Task 4.2: Header Builder
- [ ] Implement `HeaderBuilder`:
  ```go
  type HeaderBuilder struct {
      author      uint16
      round       uint64
      epoch       uint64
      batchRefs   []BatchDigest
      parents     []CertificateRef
  }
  ```
- [ ] Implement `Build(signer Signer) (*Header, error)`
- [ ] Implement deterministic parent certificate selection:
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
- [ ] Write unit tests for parent selection determinism
- [ ] Write unit tests for header building

### Task 4.2.1: Batch Availability Checker
- [ ] Define `BatchAvailabilityChecker` interface:
  ```go
  type BatchAvailabilityChecker interface {
      // HasBatches returns true if all referenced batches are stored locally
      HasBatches(digests []BatchDigest) bool
      // RequestMissingBatches requests missing batches from the validator
      RequestMissingBatches(digests []BatchDigest, fromValidator uint16)
  }
  ```
- [ ] Implement `DefaultBatchAvailabilityChecker`:
  - Check batch store for each digest
  - Track outstanding requests to avoid duplicates
  - Timeout for requests (default: 5s)
- [ ] Write unit tests

### Task 4.2.2: Out-of-Order Message Handling
- [ ] Implement pending vote buffer:
  ```go
  func (p *Primary) handleVote(vote *Vote) error {
      // If we don't have the header yet, buffer the vote
      if !p.voteTracker.HasHeader(vote.HeaderDigest) {
          p.pendingMu.Lock()
          p.pendingVotes[vote.HeaderDigest] = append(
              p.pendingVotes[vote.HeaderDigest], *vote)
          p.pendingMu.Unlock()
          return nil
      }
      // ... normal processing
  }
  ```
- [ ] Implement pending certificate buffer for certs with missing parents
- [ ] Implement vote expiry cleanup (background goroutine)
- [ ] Implement `processPendingVotes(headerDigest Hash)` called when header arrives
- [ ] Implement `processPendingCerts()` called when parent certs arrive
- [ ] Write unit tests for out-of-order vote handling
- [ ] Write unit tests for out-of-order certificate handling
- [ ] Write unit tests for expiry cleanup

### Task 4.3: Vote Tracker
- [ ] Implement `VoteTracker`:
  ```go
  type VoteTracker struct {
      pending    map[Hash]*PendingHeader
      mu         sync.RWMutex
  }

  type PendingHeader struct {
      Header     *Header
      Votes      map[uint16]*Vote
      CreatedAt  time.Time
  }
  ```
- [ ] Implement `TrackHeader(header *Header)`
- [ ] Implement `RecordVote(vote *Vote) (*Certificate, bool)` (returns cert if quorum)
- [ ] Write unit tests

### Task 4.4: Header Validation
- [ ] Implement header validation:
  - Valid author signature
  - Round within acceptable range: `currentRound <= H.Round <= currentRound + maxRoundGap`
  - Has 2f+1 valid parent certificates (verify signatures)
  - First header from this author for this round (no equivocation)
  - Epoch matches current epoch
- [ ] Implement batch availability check before voting:
  ```go
  func (p *Primary) handleHeader(header *Header) error {
      // Validate header...

      // Check batch availability BEFORE voting
      if !p.batchChecker.HasBatches(header.BatchRefs) {
          // Request missing batches, defer voting
          p.batchChecker.RequestMissingBatches(header.BatchRefs, header.Author)
          p.deferVoting(header)  // Store header, vote when batches arrive
          return nil
      }

      // All batches available, send vote
      return p.sendVote(header)
  }
  ```
- [ ] Implement deferred voting mechanism
- [ ] Write unit tests for validation
- [ ] Write unit tests for deferred voting

### Task 4.5: Certificate Formation
- [ ] Implement certificate creation from votes
- [ ] Implement `SignerMask` BitSet for efficient signer tracking
- [ ] Write unit tests

### Task 4.6: Primary Message Handlers
- [ ] Implement handlers for:
  - `HeaderMessage`: Validate, store, send vote
  - `VoteMessage`: Record vote, form certificate if quorum
  - `CertificateMessage`: Validate and store
- [ ] Write unit tests

---

## Phase 5: DAG Implementation

**Goal**: Implement the certificate DAG with causal ordering.

### Task 5.1: DAG Core
- [ ] Implement `DAG` struct:
  ```go
  type DAG struct {
      rounds         map[uint64]*RoundData
      roundsMu       sync.RWMutex

      highestRound   atomic.Uint64
      committedRound atomic.Uint64

      certStore      CertificateStore
      cfg            *DAGConfig
  }
  ```
- [ ] Implement `AddCertificate(cert *Certificate) error`
- [ ] Implement `GetCertificate(digest Hash) (*Certificate, error)`
- [ ] Write unit tests

### Task 5.2: Round Management
- [ ] Implement `RoundData`:
  ```go
  type RoundData struct {
      round        uint64
      certificates map[uint16]*Certificate  // By validator
      committed    bool
  }
  ```
- [ ] Implement `GetRound(round uint64) *RoundData`
- [ ] Implement `GetCertificatesForRound(round uint64) []*Certificate`
- [ ] Write unit tests

### Task 5.3: Round Advancement
- [ ] Implement round advancement logic:
  - Check if 2f+1 certificates exist for current round
  - If yes, advance to next round
- [ ] Implement `CanAdvanceToRound(round uint64) bool`
- [ ] Write unit tests

### Task 5.4: Causal History
- [ ] Implement `CausalHistory(cert *Certificate) []*Certificate`
- [ ] BFS traversal of parent certificates
- [ ] Implement caching for performance
- [ ] Write unit tests

### Task 5.5: Certificate Ordering
- [ ] Implement `GetOrderedCertificates(fromRound, toRound uint64) []*Certificate`
- [ ] Deterministic ordering within rounds (by validator index)
- [ ] Write unit tests

---

## Phase 6: Network Protocol

**Goal**: Implement glueberry integration and message handling.

### Task 6.1: Network Interface
- [ ] Define `Network` interface:
  ```go
  type Network interface {
      // Broadcast to all validators
      BroadcastBatch(batch *Batch) error
      BroadcastHeader(header *Header) error
      BroadcastCertificate(cert *Certificate) error

      // Send to specific validator
      SendVote(validator uint16, vote *Vote) error
      SendBatchAck(validator uint16, ack *BatchAck) error
      SendBatchRequest(validator uint16, req *BatchRequest) error

      // Receive channels
      BatchMessages() <-chan *BatchMessage
      HeaderMessages() <-chan *HeaderMessage
      VoteMessages() <-chan *VoteMessage
      CertificateMessages() <-chan *CertificateMessage
  }
  ```

### Task 6.2: Cramberry Schema
- [ ] Create `schema/looseberry.cram` with message definitions:
  ```
  message Batch {
      worker_id: u16
      validator_id: u16
      round: u64
      transactions: []bytes
      digest: bytes
      timestamp: i64
  }

  message BatchAck {
      batch_digest: bytes
      validator: u16
      signature: bytes
  }

  message Header {
      author: u16
      round: u64
      epoch: u64
      batch_refs: []BatchDigest
      parents: []CertificateRef
      timestamp: i64
      digest: bytes
      signature: bytes
  }

  // ... etc
  ```
- [ ] Generate Go code from schema
- [ ] Write unit tests for marshaling

### Task 6.3: Glueberry Adapter
- [ ] Implement `GlueberryNetwork` adapter:
  ```go
  type GlueberryNetwork struct {
      glueNode     *glueberry.Node
      validatorSet ValidatorSet

      batchCh      chan *BatchMessage
      headerCh     chan *HeaderMessage
      voteCh       chan *VoteMessage
      certCh       chan *CertificateMessage
  }
  ```
- [ ] Map validators to peer IDs
- [ ] Implement all Network interface methods
- [ ] Write unit tests

### Task 6.4: Stream Handlers
- [ ] Implement batch stream handler (looseberry-batches)
- [ ] Implement header stream handler (looseberry-headers)
- [ ] Implement sync stream handler (looseberry-sync)
- [ ] Register handlers with glueberry
- [ ] Write unit tests

### Task 6.5: Sync Protocol
- [ ] Define sync message types:
  ```go
  type SyncRequest struct {
      FromRound uint64  // Lowest round needed
      ToRound   uint64  // Highest round needed (0 = latest)
  }

  type SyncResponse struct {
      Certificates []*Certificate
      Batches      []*Batch  // Batches referenced by certificates
  }
  ```
- [ ] Implement `SyncManager`:
  ```go
  type SyncManager struct {
      dag         *DAG
      batchStore  BatchStore
      network     Network

      syncInterval   time.Duration  // Default: 10s
      syncThreshold  uint64         // Default: 5 rounds behind
      syncBatchSize  int            // Default: 100 certs per request
      syncTimeout    time.Duration  // Default: 30s
  }
  ```
- [ ] Implement sync triggers:
  - Round gap detection: peer's round > our round + syncThreshold
  - Missing parents: header references certs we don't have
  - Periodic check: every syncInterval compare with peers
- [ ] Implement `RequestSync(fromRound, toRound uint64) error`
- [ ] Implement `HandleSyncRequest(req *SyncRequest) *SyncResponse`
- [ ] Implement catch-up logic:
  ```go
  func (s *SyncManager) CatchUp(targetRound uint64) error {
      currentRound := s.dag.HighestRound()
      for currentRound < targetRound {
          // Request in batches
          toRound := min(currentRound+uint64(s.syncBatchSize), targetRound)
          resp, err := s.requestFromPeers(currentRound+1, toRound)
          if err != nil {
              return err
          }

          // Validate and store each certificate
          for _, cert := range resp.Certificates {
              if err := s.validateAndStore(cert); err != nil {
                  return err
              }
          }

          currentRound = s.dag.HighestRound()
      }
      return nil
  }
  ```
- [ ] Write unit tests for sync request/response
- [ ] Write unit tests for catch-up logic
- [ ] Write integration tests for multi-node sync

---

## Phase 7: Garbage Collection

**Goal**: Implement consensus-driven GC with transaction recovery.

### Task 7.1: GC Manager
- [ ] Implement `GCManager`:
  ```go
  type GCManager struct {
      dag        *DAG
      batchStore BatchStore
      certStore  CertificateStore
      txIndex    TxIndex

      workerPool *WorkerPool  // For tx re-injection

      cfg        *GCConfig
  }
  ```
- [ ] Implement `NotifyCommitted(round uint64) error`
- [ ] Write unit tests

### Task 7.2: Transaction Recovery
- [ ] Implement `extractUncommittedTxs(round uint64) []Transaction`:
  - Get all batches for round
  - Filter out batches in committed certificates
  - Extract transactions from uncommitted batches
- [ ] Write unit tests

### Task 7.3: Round Pruning
- [ ] Implement `pruneRound(round uint64) error`:
  - Delete batches for round
  - Delete certificates for round
  - Update tx index
- [ ] Implement batch pruning in stores
- [ ] Write unit tests

### Task 7.4: GC Scheduling
- [ ] Implement GC scheduling:
  - Trigger on `NotifyCommitted`
  - Calculate GC round = committed - gcDepth
  - Run GC in background goroutine
- [ ] Write unit tests

### Task 7.5: Flow Control
- [ ] Implement `FlowController`:
  ```go
  type FlowController struct {
      maxUncommittedRounds int  // Default: 100

      currentRound   uint64
      committedRound uint64
      mu             sync.RWMutex
  }

  func (fc *FlowController) CanCreateHeader() bool {
      fc.mu.RLock()
      defer fc.mu.RUnlock()
      gap := fc.currentRound - fc.committedRound
      return gap < uint64(fc.maxUncommittedRounds)
  }

  func (fc *FlowController) UpdateCurrentRound(round uint64)
  func (fc *FlowController) UpdateCommittedRound(round uint64)
  ```
- [ ] Integrate flow control with Primary:
  - Check `CanCreateHeader()` before creating new header
  - If limit reached, pause header creation
  - Resume when gap decreases after commits
- [ ] Add flow control metrics for monitoring
- [ ] Write unit tests for flow control limits
- [ ] Write integration tests for flow control under load

---

## Phase 8: Dynamic Worker Scaling

**Goal**: Implement automatic worker scaling based on load.

### Task 8.1: Scaling State
- [ ] Implement `ScalingState`:
  ```go
  type ScalingState struct {
      lastScaleTime   time.Time
      currentLoad     float64
      scaleHistory    []ScaleEvent
  }
  ```
- [ ] Track scaling metrics
- [ ] Write unit tests

### Task 8.2: Load Metrics
- [ ] Implement load calculation:
  ```go
  func (wp *WorkerPool) calculateLoad() float64 {
      totalPending := 0
      for _, w := range wp.workers {
          totalPending += w.PendingCount()
      }
      capacity := wp.workerCount * wp.cfg.BatchSize
      return float64(totalPending) / float64(capacity)
  }
  ```
- [ ] Write unit tests

### Task 8.3: Scale Up Logic
- [ ] Implement `scaleUp()`:
  - Create new worker
  - Register with network
  - Add to pool
- [ ] Implement gradual scale-up (one worker at a time)
- [ ] Write unit tests

### Task 8.4: Scale Down Logic
- [ ] Implement `scaleDown()`:
  - Select worker to remove (lowest load)
  - Drain pending transactions to other workers
  - Stop and remove worker
- [ ] Implement graceful drain
- [ ] Write unit tests

### Task 8.5: Scaling Loop
- [ ] Implement scaling loop:
  ```go
  func (wp *WorkerPool) scalingLoop() {
      ticker := time.NewTicker(wp.cfg.ScalingInterval)
      for {
          select {
          case <-ticker.C:
              load := wp.calculateLoad()
              if load > wp.cfg.ScaleUpThreshold && wp.workerCount < wp.cfg.MaxWorkers {
                  wp.scaleUp()
              } else if load < wp.cfg.ScaleDownThreshold && wp.workerCount > wp.cfg.MinWorkers {
                  wp.scaleDown()
              }
          case <-wp.stopCh:
              return
          }
      }
  }
  ```
- [ ] Write unit tests

---

## Phase 9: Blockberry Integration

**Goal**: Integrate Looseberry as a mempool implementation for blockberry.

### Task 9.1: Mempool Interface Implementation
- [ ] Implement blockberry's `Mempool` interface in Looseberry:
  ```go
  func (l *Looseberry) AddTx(tx []byte) error
  func (l *Looseberry) RemoveTxs(hashes [][]byte)
  func (l *Looseberry) ReapTxs(maxBytes int64) [][]byte  // Returns txs from certified batches
  func (l *Looseberry) HasTx(hash []byte) bool
  func (l *Looseberry) GetTx(hash []byte) ([]byte, error)
  func (l *Looseberry) Size() int
  func (l *Looseberry) SizeBytes() int64
  func (l *Looseberry) Flush()
  func (l *Looseberry) TxHashes() [][]byte
  ```
- [ ] Write unit tests

### Task 9.2: Extended Interface
- [ ] Implement Looseberry-specific interface:
  ```go
  type DAGMempool interface {
      mempool.Mempool

      // Get certified batches for block building
      ReapCertifiedBatches(maxBytes int64) []CertifiedBatch

      // Notify committed round for GC
      NotifyCommitted(round uint64)

      // Update validator set
      UpdateValidatorSet(validators ValidatorSet)

      // Get current round
      CurrentRound() uint64

      // Get metrics
      Metrics() *Metrics
  }
  ```
- [ ] Write unit tests

### Task 9.3: Blockberry Node Option
- [ ] Add `WithLooseberryMempool` option to blockberry:
  ```go
  // In blockberry/node/node.go
  func WithLooseberryMempool(lb looseberry.DAGMempool) Option {
      return func(n *Node) {
          n.mempool = lb
          n.dagMempool = lb
      }
  }
  ```
- [ ] Write integration tests

### Task 9.4: Validator Set Bridge
- [ ] Implement `BlockberryValidatorSet` adapter:
  - Wraps blockberry's validator info
  - Implements looseberry's `ValidatorSet` interface
- [ ] Handle epoch transitions
- [ ] Write unit tests

### Task 9.5: Glueberry Stream Registration
- [ ] Modify blockberry node to register looseberry streams:
  ```go
  // Additional streams for looseberry
  const (
      StreamLooseberryBatches  = "looseberry-batches"
      StreamLooseberryHeaders  = "looseberry-headers"
      StreamLooseberrySync     = "looseberry-sync"
  )
  ```
- [ ] Register stream handlers on node start
- [ ] Write integration tests

### Task 9.6: TransactionsReactor Passive Mode

Modify blockberry's TransactionsReactor to support passive mode for validators.

- [ ] Add `passiveMode` field to `TransactionsReactor`:
  ```go
  type TransactionsReactor struct {
      // ... existing fields ...

      // When true, reactor receives txs but does not initiate gossip
      passiveMode bool

      // Looseberry integration for validators
      looseberry looseberry.DAGMempool
  }
  ```
- [ ] Add `NewTransactionsReactorWithLooseberry` constructor:
  ```go
  func NewTransactionsReactorWithLooseberry(
      lb looseberry.DAGMempool,
      network *p2p.Network,
      peerManager *p2p.PeerManager,
      requestInterval time.Duration,
      batchSize int32,
  ) *TransactionsReactor
  ```
- [ ] Modify `gossipLoop()` to skip gossip in passive mode:
  ```go
  func (r *TransactionsReactor) gossipLoop() {
      if r.passiveMode {
          return  // Don't initiate gossip requests
      }
      // ... existing logic ...
  }
  ```
- [ ] Modify `handleTransactionDataResponse()` to route to Looseberry:
  ```go
  if r.looseberry != nil {
      _ = r.looseberry.AddTx(txData.Data)
  } else {
      _ = r.mempool.AddTx(txData.Data)
  }
  ```
- [ ] Update blockberry Node to create appropriate reactor based on configuration
- [ ] Write unit tests for passive mode
- [ ] Write integration tests for validator tx ingress

### Task 9.7: Full Node Compatibility
- [ ] Ensure full nodes continue using TransactionsReactor normally
- [ ] Verify full nodes can gossip to validators in passive mode
- [ ] Test tx flow: Client → Full Node → Validator → Looseberry Worker
- [ ] Write integration tests for full node → validator tx flow

### Task 9.8: Startup and Recovery
- [ ] Implement fresh start procedure:
  ```go
  func (l *Looseberry) FreshStart() error {
      // 1. Load configuration and validator set
      // 2. Initialize empty DAG at round 0
      // 3. Create genesis certificates (empty headers, self-signed)
      // 4. Start workers and primary
      // 5. Connect to peers
      // 6. Sync if peers are ahead
      // 7. Begin normal operation
  }
  ```
- [ ] Implement restart recovery:
  ```go
  func (l *Looseberry) RecoverFromStorage() error {
      // 1. Load DAG from persistent storage
      // 2. Determine highest stored round
      // 3. Resume from that round
      // 4. Reconnect to peers
      // 5. Sync any missed rounds
      // 6. Resume normal operation
  }
  ```
- [ ] Define persistent state requirements:
  - All certificates in DAG (within GC depth)
  - All batches referenced by those certificates
  - Current round number
  - Committed round (for GC)
- [ ] Implement state persistence in stores:
  - `certStore.SaveState()` / `certStore.LoadState()`
  - `batchStore.SaveState()` / `batchStore.LoadState()`
- [ ] Implement in-progress operation recovery:
  | State | Recovery Action |
  |-------|-----------------|
  | Batch broadcast, no acks | Re-broadcast on restart |
  | Batch with some acks | Peers re-ack on reconnect |
  | Header sent, collecting votes | Re-broadcast header |
  | Votes collected, no cert | Reform cert from stored votes |
- [ ] Write unit tests for fresh start
- [ ] Write unit tests for restart recovery
- [ ] Write integration tests for crash recovery scenarios

### Task 9.9: Epoch Transitions
- [ ] Implement `UpdateValidatorSet()` method:
  ```go
  func (l *Looseberry) UpdateValidatorSet(newSet ValidatorSet) error {
      l.mu.Lock()
      defer l.mu.Unlock()

      // 1. Complete current round with OLD validator set
      l.primary.CompleteCurrentRound()

      // 2. Store epoch boundary marker in DAG
      l.dag.MarkEpochBoundary(l.primary.CurrentRound(), newSet.Epoch())

      // 3. Switch to NEW validator set
      l.validatorSet = newSet
      l.primary.UpdateValidatorSet(newSet)
      l.workers.UpdateValidatorSet(newSet)

      // 4. Recalculate f and quorum
      // (handled by ValidatorSet interface)

      return nil
  }
  ```
- [ ] Handle headers at epoch boundary:
  - Headers created in epoch N must be voted on by epoch N validators
  - Certificate stores epoch, verified against that epoch's validator set
- [ ] Handle votes from removed validators:
  - Votes only valid if signer is in the header's epoch
  - Reject votes from validators not in epoch
- [ ] Implement new validator bootstrap:
  - New validator syncs DAG from existing validators
  - Begins participating after sync complete
- [ ] Write unit tests for epoch transitions
- [ ] Write unit tests for cross-epoch vote handling
- [ ] Write integration tests for validator set changes

---

## Phase 10: Testing & Benchmarks

**Goal**: Comprehensive testing and performance validation.

### Task 10.1: Unit Test Coverage
- [ ] Ensure >80% code coverage for all packages
- [ ] Test all error paths
- [ ] Test edge cases (empty batches, single validator, etc.)

### Task 10.2: Integration Test Harness
- [ ] Create `TestNetwork` for multi-node testing:
  ```go
  type TestNetwork struct {
      nodes     []*TestNode
      validators ValidatorSet
  }

  func NewTestNetwork(n int) *TestNetwork
  func (tn *TestNetwork) Start() error
  func (tn *TestNetwork) Stop() error
  func (tn *TestNetwork) SubmitTx(tx Transaction) error
  func (tn *TestNetwork) WaitForCertificate(round uint64) error
  ```
- [ ] Write integration tests

### Task 10.3: Multi-Node Tests
- [ ] Test: 4 nodes, batch formation
- [ ] Test: 4 nodes, certificate formation
- [ ] Test: 4 nodes, 1 Byzantine (doesn't vote)
- [ ] Test: 10 nodes, high load
- [ ] Test: Network partition recovery

### Task 10.4: Benchmarks
- [ ] Benchmark: Single worker throughput
- [ ] Benchmark: Multi-worker throughput
- [ ] Benchmark: Certificate formation latency
- [ ] Benchmark: DAG causal history traversal
- [ ] Benchmark: GC with tx recovery

### Task 10.5: Stress Tests
- [ ] Stress test: 100k tx/sec sustained
- [ ] Stress test: Worker scaling under load
- [ ] Stress test: Memory usage under load
- [ ] Stress test: GC under continuous operation

### Task 10.6: Byzantine Tests
- [ ] Test: Equivocating validator (double-voting)
- [ ] Test: Invalid signatures
- [ ] Test: Missing parent certificates
- [ ] Test: Validator churn (epoch change)

### Task 10.7: Documentation
- [ ] API documentation (godoc comments)
- [ ] Update README.md with usage examples
- [ ] Add configuration guide
- [ ] Add troubleshooting guide

---

## Estimated Task Breakdown

| Phase | Tasks | Estimated Effort |
|-------|-------|------------------|
| Phase 1: Core Types | 12 tasks | Foundation |
| Phase 2: Storage | 6 tasks | Medium |
| Phase 3: Workers | 5 tasks | Medium |
| Phase 4: Primary | 9 tasks | Large |
| Phase 5: DAG | 5 tasks | Medium |
| Phase 6: Network | 5 tasks | Large |
| Phase 7: GC + Flow Control | 5 tasks | Medium |
| Phase 8: Scaling | 5 tasks | Medium |
| Phase 9: Integration | 9 tasks | Large |
| Phase 10: Testing | 7 tasks | Large |

**Total: 68 tasks**

---

## Success Criteria

### Functional Requirements
- [ ] Transactions submitted via `AddTx` are batched and certified
- [ ] Transactions validated via TxValidator (CheckTx) before batching
- [ ] Certificates form within 500ms under normal conditions
- [ ] `ReapCertifiedBatches` returns only certified data
- [ ] GC correctly prunes old rounds while recovering uncommitted txs
- [ ] Dynamic scaling adjusts worker count based on load
- [ ] Flow control prevents unbounded DAG growth
- [ ] Sync protocol catches up nodes that fall behind
- [ ] Epoch transitions handled correctly

### Performance Requirements
- [ ] Sustained throughput of 100,000 tx/sec with 4 workers
- [ ] Batch formation latency < 100ms
- [ ] Certificate formation latency < 500ms
- [ ] Memory bounded by GC depth (not unbounded growth)
- [ ] Sync catches up at > 1000 certs/sec

### Reliability Requirements
- [ ] Tolerates f Byzantine validators (n = 3f + 1)
- [ ] Recovers from network partitions
- [ ] Handles validator set changes (epoch transitions) gracefully
- [ ] No transaction loss during GC (re-injection works)
- [ ] Recovers correctly from crash/restart
- [ ] Worker deduplication prevents double-batching
- [ ] Back-pressure prevents memory exhaustion
- [ ] Out-of-order messages handled correctly

---

## Dependencies

### External Dependencies
- `github.com/blockberries/cramberry` - Message serialization
- `github.com/blockberries/glueberry` - P2P networking
- `github.com/blockberries/blockberry` - Consensus integration
- `github.com/syndtr/goleveldb` - Persistent storage

### Internal Dependencies (Phase Order)
```
Phase 1 (Types) ─────────────────────────────────────────────────────┐
    │                                                                │
    ▼                                                                │
Phase 2 (Storage) ───────────────────────────────────────────────────┤
    │                                                                │
    ├──────────────────┬─────────────────┐                           │
    ▼                  ▼                 ▼                           │
Phase 3 (Workers)   Phase 4 (Primary)  Phase 5 (DAG)                 │
    │                  │                 │                           │
    └──────────────────┴─────────────────┘                           │
                       │                                             │
                       ▼                                             │
               Phase 6 (Network) ◄───────────────────────────────────┤
                       │                                             │
           ┌───────────┴───────────┐                                 │
           ▼                       ▼                                 │
    Phase 7 (GC)           Phase 8 (Scaling)                         │
           │                       │                                 │
           └───────────┬───────────┘                                 │
                       ▼                                             │
               Phase 9 (Integration) ◄───────────────────────────────┘
                       │
                       ▼
               Phase 10 (Testing)
```
