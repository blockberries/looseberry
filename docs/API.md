# Looseberry API Documentation

Complete API reference for the Looseberry DAG-based mempool library.

## Table of Contents

- [Main Interface](#main-interface)
  - [DAGMempool](#dagmempool-interface)
  - [Looseberry](#looseberry-implementation)
- [Configuration](#configuration)
  - [Config](#config)
  - [WorkerConfig](#workerconfig)
  - [PrimaryConfig](#primaryconfig)
  - [SyncConfig](#syncconfig)
  - [StorageConfig](#storageconfig)
  - [GCConfig](#gcconfig)
  - [FlowControlConfig](#flowcontrolconfig)
- [Core Types](#core-types)
  - [Hash](#hash)
  - [Transaction](#transaction)
  - [Batch](#batch)
  - [Header](#header)
  - [Vote](#vote)
  - [Certificate](#certificate)
- [Validator Types](#validator-types)
  - [Validator](#validator)
  - [ValidatorSet](#validatorset-interface)
  - [SimpleValidatorSet](#simplevalidatorset)
- [Cryptographic Types](#cryptographic-types)
  - [Signature](#signature)
  - [PublicKey](#publickey)
  - [Signer](#signer-interface)
  - [Ed25519Signer](#ed25519signer)
- [Storage Interfaces](#storage-interfaces)
  - [BatchStore](#batchstore-interface)
  - [CertificateStore](#certificatestore-interface)
  - [TxIndex](#txindex-interface)
- [Network Interface](#network-interface)
  - [Network](#network-interface-1)
  - [Message Types](#message-types)
  - [MockNetwork](#mocknetwork)
- [Metrics](#metrics)
- [Error Types](#error-types)

---

## Main Interface

### DAGMempool Interface

The primary interface for interacting with Looseberry.

```go
type DAGMempool interface {
    AddTx(tx []byte) error
    ReapCertifiedBatches(maxBytes int64) []CertifiedBatch
    NotifyCommitted(round uint64)
    UpdateValidatorSet(validators types.ValidatorSet)
    HasTx(hash []byte) bool
    Size() int
    SizeBytes() int64
    Flush()
    CurrentRound() uint64
    Metrics() *Metrics
}
```

#### Methods

##### AddTx

```go
AddTx(tx []byte) error
```

Adds a transaction to the mempool for batching and dissemination.

**Parameters:**
- `tx []byte` - Raw transaction bytes. The format is opaque to Looseberry.

**Returns:**
- `error` - Returns nil on success, or one of:
  - `ErrTxValidationFailed` - Transaction failed validation via TxValidator
  - `ErrWorkerBackpressure` - Worker is overloaded (retryable)
  - `ErrFlowControlPaused` - DAG production paused waiting for consensus (retryable)
  - `ErrNotRunning` - Looseberry is not running

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
tx := []byte("transaction data")
if err := lb.AddTx(tx); err != nil {
    if types.IsRetryable(err) {
        // Retry later
    } else {
        // Transaction rejected
    }
}
```

##### ReapCertifiedBatches

```go
ReapCertifiedBatches(maxBytes int64) []CertifiedBatch
```

Returns certified batches for block building by the consensus layer.

**Parameters:**
- `maxBytes int64` - Maximum total size of batches to return in bytes.

**Returns:**
- `[]CertifiedBatch` - Batches in deterministic order: by round ASC, then by validator index ASC. Only includes batches from rounds greater than the last committed round.

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
// Reap up to 1MB of certified batches
batches := lb.ReapCertifiedBatches(1024 * 1024)
for _, cb := range batches {
    fmt.Printf("Round %d, Validator %d: %d txs\n",
        cb.Certificate.Header.Round,
        cb.Certificate.Header.Author,
        len(cb.Batch.Transactions))
}
```

##### NotifyCommitted

```go
NotifyCommitted(round uint64)
```

Notifies the mempool that consensus has committed up to the specified round. This enables garbage collection of older rounds.

**Parameters:**
- `round uint64` - The highest committed round.

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
// Notify that consensus committed round 100
lb.NotifyCommitted(100)
```

##### UpdateValidatorSet

```go
UpdateValidatorSet(validators types.ValidatorSet)
```

Updates the validator set for a new epoch. This is called when the validator set changes.

**Parameters:**
- `validators types.ValidatorSet` - The new validator set.

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
newValidators := types.NewSimpleValidatorSet(validatorList, newEpoch)
lb.UpdateValidatorSet(newValidators)
```

##### HasTx

```go
HasTx(hash []byte) bool
```

Checks if a transaction exists in the mempool (pending or committed).

**Parameters:**
- `hash []byte` - Transaction hash (32 bytes).

**Returns:**
- `bool` - True if the transaction exists.

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
tx := []byte("transaction data")
txHash := types.HashBytes(tx)
if lb.HasTx(txHash[:]) {
    fmt.Println("Transaction already in mempool")
}
```

##### Size

```go
Size() int
```

Returns the number of pending transactions in the mempool.

**Returns:**
- `int` - Number of pending transactions.

**Thread Safety:** Safe for concurrent use.

##### SizeBytes

```go
SizeBytes() int64
```

Returns the total size of pending transactions in bytes.

**Returns:**
- `int64` - Total bytes of pending transactions.

**Thread Safety:** Safe for concurrent use.

##### Flush

```go
Flush()
```

Removes all pending transactions from the mempool. This does not affect already-committed batches.

**Thread Safety:** Safe for concurrent use.

**Example:**
```go
// Clear all pending transactions
lb.Flush()
```

##### CurrentRound

```go
CurrentRound() uint64
```

Returns the current DAG round number.

**Returns:**
- `uint64` - Current round.

**Thread Safety:** Safe for concurrent use.

##### Metrics

```go
Metrics() *Metrics
```

Returns current mempool metrics for monitoring and observability.

**Returns:**
- `*Metrics` - Current metrics snapshot.

**Thread Safety:** Safe for concurrent use.

---

### Looseberry Implementation

The main implementation of the DAGMempool interface.

#### Constructor

```go
func New(cfg *Config) (*Looseberry, error)
```

Creates a new Looseberry instance.

**Parameters:**
- `cfg *Config` - Configuration. Use `DefaultConfig()` as a starting point.

**Returns:**
- `*Looseberry` - New instance.
- `error` - Returns error if configuration is invalid.

**Example:**
```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

lb, err := looseberry.New(cfg)
if err != nil {
    panic(err)
}
```

#### Lifecycle Methods

##### SetValidatorSet

```go
func (l *Looseberry) SetValidatorSet(validators types.ValidatorSet)
```

Sets the initial validator set. Must be called before `Start()`.

**Thread Safety:** Not safe during Start/Stop.

##### SetNetwork

```go
func (l *Looseberry) SetNetwork(net network.Network)
```

Sets the network implementation. Must be called before `Start()`.

**Thread Safety:** Not safe during Start/Stop.

##### SetStores

```go
func (l *Looseberry) SetStores(batchStore store.BatchStore, certStore store.CertificateStore, txIndex store.TxIndex)
```

Sets custom storage implementations. Optional - if not called, stores are created based on `cfg.Storage`.

**Thread Safety:** Not safe during Start/Stop.

##### Start

```go
func (l *Looseberry) Start() error
```

Starts the Looseberry instance and all internal components.

**Returns:**
- `error` - Returns error if:
  - Already running (ErrAlreadyRunning)
  - Validator set not set
  - Network not set
  - Component initialization fails

**Thread Safety:** Not safe for concurrent Start/Stop calls.

##### Stop

```go
func (l *Looseberry) Stop() error
```

Stops the Looseberry instance and closes all resources.

**Returns:**
- `error` - Returns ErrNotRunning if not running.

**Thread Safety:** Not safe for concurrent Start/Stop calls.

##### IsRunning

```go
func (l *Looseberry) IsRunning() bool
```

Returns true if Looseberry is currently running.

**Thread Safety:** Safe for concurrent use.

---

## Configuration

### Config

Main configuration structure for Looseberry.

```go
type Config struct {
    ValidatorIndex  uint16
    Signer          types.Signer
    TxValidator     TxValidator
    Worker          WorkerConfig
    Primary         PrimaryConfig
    Sync            SyncConfig
    Storage         StorageConfig
    GC              GCConfig
    FlowControl     FlowControlConfig
}
```

#### Fields

- `ValidatorIndex uint16` - **Required.** This validator's index in the validator set.
- `Signer types.Signer` - **Required.** Signer for creating signatures.
- `TxValidator TxValidator` - Optional. Function to validate transactions before batching.
- `Worker WorkerConfig` - Worker pool configuration.
- `Primary PrimaryConfig` - Primary node configuration.
- `Sync SyncConfig` - Sync manager configuration.
- `Storage StorageConfig` - Storage configuration.
- `GC GCConfig` - Garbage collection configuration.
- `FlowControl FlowControlConfig` - Flow control configuration.

#### Constructor

```go
func DefaultConfig() *Config
```

Returns a Config with sensible defaults.

#### Validation

```go
func (c *Config) Validate() error
```

Validates the configuration. Called automatically by `New()`.

#### TxValidator

```go
type TxValidator func(tx []byte) error
```

Function type for validating transactions before they are batched.

**Parameters:**
- `tx []byte` - Raw transaction bytes.

**Returns:**
- `error` - Return nil if valid, or an error describing why invalid.

**Example:**
```go
cfg.TxValidator = func(tx []byte) error {
    if len(tx) == 0 {
        return errors.New("empty transaction")
    }
    if len(tx) > 1024*1024 {
        return errors.New("transaction too large")
    }
    // Custom validation logic
    return nil
}
```

---

### WorkerConfig

Configuration for worker pool and worker behavior.

```go
type WorkerConfig struct {
    MinWorkers         int
    MaxWorkers         int
    BatchSize          int
    BatchTimeout       time.Duration
    MaxBatchBytes      int64
    MaxPendingTxs      int
    MaxPendingBytes    int64
    ScalingInterval    time.Duration
    ScaleUpThreshold   float64
    ScaleDownThreshold float64
    DrainTimeout       time.Duration
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `MinWorkers` | 1 | Minimum number of workers |
| `MaxWorkers` | 8 | Maximum number of workers |
| `BatchSize` | 500 | Maximum transactions per batch |
| `BatchTimeout` | 100ms | Maximum time before creating a batch |
| `MaxBatchBytes` | 512KB | Maximum batch size in bytes |
| `MaxPendingTxs` | 10,000 | Backpressure: max pending transactions per worker |
| `MaxPendingBytes` | 50MB | Backpressure: max pending bytes per worker |
| `ScalingInterval` | 5s | How often to check for scaling opportunities |
| `ScaleUpThreshold` | 0.8 | Load ratio to trigger scale up (0.0-1.0) |
| `ScaleDownThreshold` | 0.2 | Load ratio to trigger scale down (0.0-1.0) |
| `DrainTimeout` | 30s | Maximum time to wait for worker drain on scale down |

#### Constructor

```go
func DefaultWorkerConfig() WorkerConfig
```

#### Validation

```go
func (c *WorkerConfig) Validate() error
```

**Validation Rules:**
- `MinWorkers` must be at least 1
- `MaxWorkers` must be >= `MinWorkers`
- `BatchSize` must be at least 1
- `BatchTimeout` must be positive
- `MaxBatchBytes` must be at least 1
- `MaxPendingTxs` must be at least 1
- `MaxPendingBytes` must be at least 1
- `ScaleUpThreshold` must be > `ScaleDownThreshold`

---

### PrimaryConfig

Configuration for primary node (header and certificate creation).

```go
type PrimaryConfig struct {
    HeaderTimeout       time.Duration
    MaxBatchesPerHeader int
    MaxRoundGap         uint64
    VoteTimeout         time.Duration
    AllowEmptyHeaders   bool
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `HeaderTimeout` | 500ms | Maximum time to wait for batches before creating a header |
| `MaxBatchesPerHeader` | 100 | Maximum batch references per header |
| `MaxRoundGap` | 10 | Maximum rounds ahead to accept headers from |
| `VoteTimeout` | 30s | How long to buffer votes for unknown headers |
| `AllowEmptyHeaders` | true | Allow creating headers with no batches (for liveness) |

#### Constructor

```go
func DefaultPrimaryConfig() PrimaryConfig
```

#### Validation

```go
func (c *PrimaryConfig) Validate() error
```

**Validation Rules:**
- `HeaderTimeout` must be positive
- `MaxBatchesPerHeader` must be at least 1
- `MaxRoundGap` must be at least 1
- `VoteTimeout` must be positive

---

### SyncConfig

Configuration for the sync manager.

```go
type SyncConfig struct {
    SyncInterval  time.Duration
    SyncThreshold uint64
    SyncBatchSize int
    SyncTimeout   time.Duration
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `SyncInterval` | 10s | How often to check if sync is needed |
| `SyncThreshold` | 5 | Rounds behind before triggering sync |
| `SyncBatchSize` | 100 | Number of certificates per sync request |
| `SyncTimeout` | 30s | Timeout for sync requests |

#### Constructor

```go
func DefaultSyncConfig() SyncConfig
```

#### Validation

```go
func (c *SyncConfig) Validate() error
```

**Validation Rules:**
- `SyncInterval` must be positive
- `SyncThreshold` must be at least 1
- `SyncBatchSize` must be at least 1
- `SyncTimeout` must be positive

---

### StorageConfig

Configuration for storage backends.

```go
type StorageConfig struct {
    DataDir  string
    InMemory bool
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `DataDir` | "data/looseberry" | Directory for LevelDB persistent storage |
| `InMemory` | false | Use in-memory storage (for testing) |

#### Constructor

```go
func DefaultStorageConfig() StorageConfig
```

**Example:**
```go
// Production configuration
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry"

// Testing configuration
cfg.Storage.InMemory = true
```

---

### GCConfig

Configuration for garbage collection.

```go
type GCConfig struct {
    GCDepth    int
    RecoverTxs bool
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `GCDepth` | 50 | Number of rounds to keep after commit |
| `RecoverTxs` | true | Re-inject uncommitted transactions during GC |

#### Constructor

```go
func DefaultGCConfig() GCConfig
```

#### Validation

```go
func (c *GCConfig) Validate() error
```

**Validation Rules:**
- `GCDepth` must be at least 1

**Note on Transaction Recovery:**
When `RecoverTxs` is true, the GC manager will extract transactions from batches that are being pruned (if they haven't been committed) and re-submit them to the mempool. This ensures that transactions aren't lost if a validator fails or a batch isn't committed.

---

### FlowControlConfig

Configuration for flow control (prevents unbounded DAG growth).

```go
type FlowControlConfig struct {
    MaxUncommittedRounds int
}
```

#### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `MaxUncommittedRounds` | 100 | Maximum gap between current and committed rounds before pausing |

#### Constructor

```go
func DefaultFlowControlConfig() FlowControlConfig
```

#### Validation

```go
func (c *FlowControlConfig) Validate() error
```

**Validation Rules:**
- `MaxUncommittedRounds` must be at least 1

**Flow Control Behavior:**
When the gap between the highest DAG round and the committed round exceeds `MaxUncommittedRounds`, the flow controller pauses DAG production. New transactions are rejected with `ErrFlowControlPaused`. Production resumes when consensus catches up.

---

## Core Types

### Hash

32-byte SHA-256 hash used throughout Looseberry.

```go
type Hash [HashSize]byte  // HashSize = 32
```

#### Methods

```go
func (h Hash) Bytes() []byte
```
Returns the hash as a byte slice.

```go
func (h Hash) String() string
```
Returns the hex-encoded string representation.

```go
func (h Hash) Equal(other Hash) bool
```
Returns true if hashes are equal.

```go
func (h Hash) IsEmpty() bool
```
Returns true if hash is the zero value.

```go
func (h Hash) MarshalText() ([]byte, error)
```
Implements encoding.TextMarshaler.

```go
func (h *Hash) UnmarshalText(text []byte) error
```
Implements encoding.TextUnmarshaler.

#### Functions

```go
func HashBytes(data []byte) Hash
```
Computes the SHA-256 hash of data.

```go
func HashConcat(left, right Hash) Hash
```
Computes the hash of concatenating left and right. Useful for Merkle trees.

```go
func HashFromBytes(b []byte) (Hash, error)
```
Creates a Hash from a byte slice. Returns error if not exactly 32 bytes.

```go
func HashFromHex(s string) (Hash, error)
```
Creates a Hash from a hex-encoded string.

#### Example

```go
// Hash some data
data := []byte("transaction data")
hash := types.HashBytes(data)

// Convert to hex string
fmt.Println(hash.String())

// Check equality
other := types.HashBytes(data)
fmt.Println(hash.Equal(other))  // true

// Parse from hex
parsed, err := types.HashFromHex(hash.String())
```

---

### Transaction

Raw transaction wrapper. The format is opaque to Looseberry.

```go
type Transaction []byte
```

#### Methods

```go
func (tx Transaction) Hash() Hash
```
Returns the SHA-256 hash of the transaction.

```go
func (tx Transaction) Size() int
```
Returns the size in bytes.

```go
func (tx Transaction) Bytes() []byte
```
Returns the raw transaction bytes.

```go
func (tx Transaction) IsEmpty() bool
```
Returns true if the transaction is empty.

```go
func (tx Transaction) Clone() Transaction
```
Returns a copy of the transaction.

```go
func (tx Transaction) Equal(other Transaction) bool
```
Returns true if transactions are equal.

#### Example

```go
tx := types.Transaction([]byte("transaction data"))

// Get hash
hash := tx.Hash()

// Get size
size := tx.Size()

// Check if same
other := types.Transaction([]byte("transaction data"))
fmt.Println(tx.Equal(other))  // true
```

---

### Batch

Collection of transactions from a single worker.

```go
type Batch struct {
    WorkerID     uint16
    ValidatorID  uint16
    Round        uint64
    Transactions []Transaction
    Digest       Hash
    Timestamp    int64  // Unix nanoseconds
}
```

#### Constructor

```go
func NewBatch(workerID, validatorID uint16, round uint64, txs []Transaction) *Batch
```

Creates a new batch. The digest is computed automatically.

#### Methods

```go
func (b *Batch) ComputeDigest() Hash
```
Computes the batch digest: `SHA256(workerID || validatorID || round || timestamp || tx1_hash || tx2_hash || ...)`.

```go
func (b *Batch) Size() int64
```
Returns the total size of all transactions in bytes.

```go
func (b *Batch) TxCount() int
```
Returns the number of transactions.

```go
func (b *Batch) IsEmpty() bool
```
Returns true if the batch has no transactions.

```go
func (b *Batch) GetDigest() BatchDigest
```
Returns a BatchDigest reference to this batch.

```go
func (b *Batch) Verify() bool
```
Verifies that the batch digest is valid.

```go
func (b *Batch) Clone() *Batch
```
Returns a deep copy of the batch.

#### BatchDigest

Reference to a batch by its digest.

```go
type BatchDigest struct {
    Digest      Hash
    WorkerID    uint16
    ValidatorID uint16
}
```

#### Example

```go
txs := []types.Transaction{
    []byte("tx1"),
    []byte("tx2"),
    []byte("tx3"),
}

batch := types.NewBatch(0, 0, 1, txs)
fmt.Printf("Batch digest: %s\n", batch.Digest.String())
fmt.Printf("Batch size: %d bytes\n", batch.Size())
fmt.Printf("Transaction count: %d\n", batch.TxCount())

// Verify digest
if batch.Verify() {
    fmt.Println("Batch digest is valid")
}
```

---

### Header

DAG vertex proposal from a primary node.

```go
type Header struct {
    Author    uint16
    Round     uint64
    Epoch     uint64
    BatchRefs []BatchDigest
    Parents   []CertificateRef  // 2f+1 certificates from round-1
    Timestamp int64             // Unix nanoseconds
    Digest    Hash
    Signature Signature
}
```

#### Constructor

```go
func NewHeader(author uint16, round, epoch uint64, batchRefs []BatchDigest, parents []CertificateRef) *Header
```

Creates a new unsigned header. The digest is computed automatically.

#### Methods

```go
func (h *Header) ComputeDigest() Hash
```
Computes the header digest: `SHA256(author || round || epoch || timestamp || batch_refs || parent_refs)`. The signature is excluded from the digest.

```go
func (h *Header) Sign(signer Signer) error
```
Signs the header with the given signer.

```go
func (h *Header) Verify(pk PublicKey) bool
```
Verifies the header's signature.

```go
func (h *Header) IsSigned() bool
```
Returns true if the header has been signed.

```go
func (h *Header) ParentCount() int
```
Returns the number of parent certificates.

```go
func (h *Header) BatchCount() int
```
Returns the number of batch references.

```go
func (h *Header) IsEmpty() bool
```
Returns true if this header has no batch references. Empty headers are allowed for liveness.

```go
func (h *Header) Clone() *Header
```
Returns a deep copy of the header.

#### CertificateRef

Reference to a certificate by digest and round.

```go
type CertificateRef struct {
    Digest Hash
    Round  uint64
}
```

#### Example

```go
batchRefs := []types.BatchDigest{
    {Digest: batchHash1, WorkerID: 0, ValidatorID: 0},
    {Digest: batchHash2, WorkerID: 1, ValidatorID: 0},
}

parents := []types.CertificateRef{
    {Digest: cert1Hash, Round: 9},
    {Digest: cert2Hash, Round: 9},
    {Digest: cert3Hash, Round: 9},
}

header := types.NewHeader(0, 10, 0, batchRefs, parents)
if err := header.Sign(signer); err != nil {
    panic(err)
}

fmt.Printf("Header digest: %s\n", header.Digest.String())
fmt.Printf("Batch count: %d\n", header.BatchCount())
fmt.Printf("Parent count: %d\n", header.ParentCount())
```

---

### Vote

Validator's vote on a header.

```go
type Vote struct {
    HeaderDigest Hash
    Validator    uint16
    Signature    Signature
}
```

#### Constructor

```go
func NewVote(headerDigest Hash, validator uint16) *Vote
```

Creates a new unsigned vote.

#### Methods

```go
func (v *Vote) Sign(signer Signer) error
```
Signs the vote with the given signer.

```go
func (v *Vote) Verify(pk PublicKey) bool
```
Verifies the vote's signature.

```go
func (v *Vote) IsSigned() bool
```
Returns true if the vote has been signed.

```go
func (v *Vote) Clone() *Vote
```
Returns a copy of the vote.

#### Example

```go
vote := types.NewVote(headerDigest, 0)
if err := vote.Sign(signer); err != nil {
    panic(err)
}

// Verify the vote
if vote.Verify(signer.PublicKey()) {
    fmt.Println("Vote signature is valid")
}
```

---

### Certificate

Certified DAG vertex (header with 2f+1 votes).

```go
type Certificate struct {
    Header     Header
    Votes      []Vote     // Sorted by validator index
    SignerMask BitSet     // Bitset of which validators signed
}
```

#### Constructor

```go
func NewCertificate(header *Header, votes []Vote) *Certificate
```

Creates a new certificate. Votes are automatically sorted by validator index and a signer mask is built.

#### Methods

```go
func (c *Certificate) Digest() Hash
```
Returns the certificate's digest (same as the header's digest).

```go
func (c *Certificate) Round() uint64
```
Returns the certificate's round.

```go
func (c *Certificate) Author() uint16
```
Returns the certificate author's validator index.

```go
func (c *Certificate) Epoch() uint64
```
Returns the certificate's epoch.

```go
func (c *Certificate) VoteCount() int
```
Returns the number of votes.

```go
func (c *Certificate) HasQuorum(validatorCount int) bool
```
Checks if the certificate has enough votes (2f+1 where f = (n-1)/3).

```go
func (c *Certificate) HasVoteFrom(validatorIndex uint16) bool
```
Returns true if the certificate contains a vote from the given validator.

```go
func (c *Certificate) GetRef() CertificateRef
```
Returns a CertificateRef for this certificate.

```go
func (c *Certificate) Verify(validators ValidatorSet) error
```
Verifies all votes in the certificate using the provided validator set.

**Returns:**
- `ErrInsufficientQuorum` - Not enough votes
- `ErrValidatorNotFound` - Unknown validator
- `ErrInvalidSignature` - Invalid signature
- `ErrInvalidVote` - Vote doesn't match header

```go
func (c *Certificate) Clone() *Certificate
```
Returns a deep copy of the certificate.

#### BitSet

Simple bit set implementation used in certificates.

```go
type BitSet []uint64
```

**Methods:**
```go
func NewBitSet(n int) BitSet
func (bs BitSet) Set(i int)
func (bs BitSet) Clear(i int)
func (bs BitSet) IsSet(i int) bool
func (bs BitSet) Count() int
func (bs BitSet) Clone() BitSet
```

#### Example

```go
// Create certificate
votes := []types.Vote{vote1, vote2, vote3}  // 2f+1 votes
cert := types.NewCertificate(header, votes)

// Verify certificate
if err := cert.Verify(validatorSet); err != nil {
    panic(err)
}

// Check quorum
if cert.HasQuorum(validatorSet.Count()) {
    fmt.Println("Certificate has quorum")
}

// Check specific validator
if cert.HasVoteFrom(0) {
    fmt.Println("Validator 0 signed this certificate")
}
```

---

## Validator Types

### Validator

Represents a validator in the network.

```go
type Validator struct {
    Index     uint16
    PublicKey PublicKey
    Power     int64
    Address   string  // Multiaddr format
}
```

#### Fields

- `Index uint16` - Validator's unique index (0-based)
- `PublicKey PublicKey` - Ed25519 public key
- `Power int64` - Voting power (for weighted voting, currently unused)
- `Address string` - Network address in multiaddr format

---

### ValidatorSet Interface

Represents the set of validators for an epoch.

```go
type ValidatorSet interface {
    Count() int
    GetByIndex(index uint16) *Validator
    Contains(index uint16) bool
    F() int
    Quorum() int
    Epoch() uint64
    VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool
    Validators() []*Validator
}
```

#### Methods

##### Count

```go
Count() int
```
Returns the number of validators.

##### GetByIndex

```go
GetByIndex(index uint16) *Validator
```
Returns the validator at the given index, or nil if not found.

##### Contains

```go
Contains(index uint16) bool
```
Returns true if the index is a valid validator.

##### F

```go
F() int
```
Returns the Byzantine fault tolerance threshold: `f = (n-1)/3`.

##### Quorum

```go
Quorum() int
```
Returns the quorum size: `2f+1`.

##### Epoch

```go
Epoch() uint64
```
Returns the current epoch number.

##### VerifySignature

```go
VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool
```
Verifies a signature from the validator at the given index.

##### Validators

```go
Validators() []*Validator
```
Returns all validators in the set.

---

### SimpleValidatorSet

Basic implementation of ValidatorSet.

```go
type SimpleValidatorSet struct {
    // private fields
}
```

#### Constructor

```go
func NewSimpleValidatorSet(validators []*Validator, epoch uint64) *SimpleValidatorSet
```

Creates a new validator set.

**Parameters:**
- `validators []*Validator` - List of validators
- `epoch uint64` - Epoch number

#### Example

```go
validators := []*types.Validator{
    {
        Index:     0,
        PublicKey: signer0.PublicKey(),
        Power:     1,
        Address:   "/ip4/127.0.0.1/tcp/9000",
    },
    {
        Index:     1,
        PublicKey: signer1.PublicKey(),
        Power:     1,
        Address:   "/ip4/127.0.0.1/tcp/9001",
    },
    {
        Index:     2,
        PublicKey: signer2.PublicKey(),
        Power:     1,
        Address:   "/ip4/127.0.0.1/tcp/9002",
    },
    {
        Index:     3,
        PublicKey: signer3.PublicKey(),
        Power:     1,
        Address:   "/ip4/127.0.0.1/tcp/9003",
    },
}

vs := types.NewSimpleValidatorSet(validators, 0)

fmt.Printf("Validator count: %d\n", vs.Count())
fmt.Printf("Byzantine threshold (f): %d\n", vs.F())
fmt.Printf("Quorum (2f+1): %d\n", vs.Quorum())
```

---

## Cryptographic Types

### Signature

Ed25519 signature (64 bytes).

```go
type Signature [SignatureSize]byte  // SignatureSize = 64
```

#### Methods

```go
func (s Signature) Bytes() []byte
```
Returns the signature as a byte slice.

```go
func (s Signature) String() string
```
Returns the hex-encoded string representation.

```go
func (s Signature) IsEmpty() bool
```
Returns true if the signature is the zero value.

```go
func (s Signature) Equal(other Signature) bool
```
Returns true if two signatures are equal.

#### Functions

```go
func SignatureFromBytes(b []byte) (Signature, error)
```
Creates a Signature from a byte slice. Returns error if not exactly 64 bytes.

---

### PublicKey

Ed25519 public key (32 bytes).

```go
type PublicKey [PublicKeySize]byte  // PublicKeySize = 32
```

#### Methods

```go
func (pk PublicKey) Bytes() []byte
```
Returns the public key as a byte slice.

```go
func (pk PublicKey) String() string
```
Returns the hex-encoded string representation.

```go
func (pk PublicKey) IsEmpty() bool
```
Returns true if the public key is the zero value.

```go
func (pk PublicKey) Verify(digest Hash, sig Signature) bool
```
Verifies a signature against a message digest.

```go
func (pk PublicKey) ToEd25519() ed25519.PublicKey
```
Converts to standard library ed25519.PublicKey.

#### Functions

```go
func PublicKeyFromBytes(b []byte) (PublicKey, error)
```
Creates a PublicKey from a byte slice. Returns error if not exactly 32 bytes.

---

### Signer Interface

Interface for signing digests.

```go
type Signer interface {
    Sign(digest Hash) (Signature, error)
    PublicKey() PublicKey
    ValidatorIndex() uint16
}
```

#### Methods

##### Sign

```go
Sign(digest Hash) (Signature, error)
```
Signs the given digest and returns the signature.

##### PublicKey

```go
PublicKey() PublicKey
```
Returns the signer's public key.

##### ValidatorIndex

```go
ValidatorIndex() uint16
```
Returns the signer's validator index.

---

### Ed25519Signer

Ed25519 implementation of Signer.

```go
type Ed25519Signer struct {
    // private fields
}
```

#### Constructor

```go
func NewEd25519Signer(privateKey ed25519.PrivateKey, validatorIndex uint16) (*Ed25519Signer, error)
```

Creates a new Ed25519 signer.

**Parameters:**
- `privateKey ed25519.PrivateKey` - Ed25519 private key (64 bytes)
- `validatorIndex uint16` - Validator index

**Returns:**
- Error if private key size is invalid

#### Key Generation

```go
func GenerateEd25519Signer(validatorIndex uint16) (*Ed25519Signer, error)
```

Generates a new Ed25519 signer with a random key.

#### Example

```go
// Generate a new signer
signer, err := types.GenerateEd25519Signer(0)
if err != nil {
    panic(err)
}

// Sign a digest
digest := types.HashBytes([]byte("message"))
sig, err := signer.Sign(digest)
if err != nil {
    panic(err)
}

// Verify signature
pk := signer.PublicKey()
if pk.Verify(digest, sig) {
    fmt.Println("Signature is valid")
}

// Use existing key
import "crypto/ed25519"
pub, priv, _ := ed25519.GenerateKey(nil)
signer2, err := types.NewEd25519Signer(priv, 0)
```

---

## Storage Interfaces

### BatchStore Interface

Storage for transaction batches.

```go
type BatchStore interface {
    SaveBatch(batch *types.Batch) error
    GetBatch(digest types.Hash) (*types.Batch, error)
    HasBatch(digest types.Hash) bool
    GetBatchesByRound(round uint64) ([]*types.Batch, error)
    DeleteBatchesBefore(round uint64) error
    Close() error
}
```

#### Methods

##### SaveBatch

```go
SaveBatch(batch *types.Batch) error
```
Stores a batch.

##### GetBatch

```go
GetBatch(digest types.Hash) (*types.Batch, error)
```
Retrieves a batch by digest. Returns nil if not found.

##### HasBatch

```go
HasBatch(digest types.Hash) bool
```
Returns true if the batch exists.

##### GetBatchesByRound

```go
GetBatchesByRound(round uint64) ([]*types.Batch, error)
```
Retrieves all batches for a specific round.

##### DeleteBatchesBefore

```go
DeleteBatchesBefore(round uint64) error
```
Deletes all batches before the given round (for garbage collection).

##### Close

```go
Close() error
```
Closes the store and releases resources.

#### Implementations

**Memory Implementation:**
```go
func NewMemoryBatchStore() *MemoryBatchStore
```
In-memory storage for testing.

**LevelDB Implementation:**
```go
func NewLevelDBBatchStore(dataDir string) (*LevelDBBatchStore, error)
```
Persistent storage using LevelDB.

#### Example

```go
// In-memory store
store := store.NewMemoryBatchStore()
defer store.Close()

// Save a batch
err := store.SaveBatch(batch)
if err != nil {
    panic(err)
}

// Retrieve by digest
retrieved, err := store.GetBatch(batch.Digest)
if err != nil {
    panic(err)
}

// Check existence
if store.HasBatch(batch.Digest) {
    fmt.Println("Batch exists")
}

// Get all batches for round 10
batches, err := store.GetBatchesByRound(10)

// Clean up old batches
err = store.DeleteBatchesBefore(50)
```

---

### CertificateStore Interface

Storage for certificates.

```go
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
```

#### Methods

##### SaveCertificate

```go
SaveCertificate(cert *types.Certificate) error
```
Stores a certificate.

##### GetCertificate

```go
GetCertificate(digest types.Hash) (*types.Certificate, error)
```
Retrieves a certificate by digest. Returns nil if not found.

##### HasCertificate

```go
HasCertificate(digest types.Hash) bool
```
Returns true if the certificate exists.

##### GetCertificatesByRound

```go
GetCertificatesByRound(round uint64) ([]*types.Certificate, error)
```
Retrieves all certificates for a specific round.

##### GetCertificateForValidator

```go
GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error)
```
Retrieves the certificate from a specific validator for a round.

##### DeleteCertificatesBefore

```go
DeleteCertificatesBefore(round uint64) error
```
Deletes all certificates before the given round (for garbage collection).

##### HighestRound

```go
HighestRound() uint64
```
Returns the highest round stored.

##### Close

```go
Close() error
```
Closes the store and releases resources.

#### Implementations

**Memory Implementation:**
```go
func NewMemoryCertificateStore() *MemoryCertificateStore
```
In-memory storage for testing.

**LevelDB Implementation:**
```go
func NewLevelDBCertificateStore(dataDir string) (*LevelDBCertificateStore, error)
```
Persistent storage using LevelDB.

#### Example

```go
// LevelDB store
store, err := store.NewLevelDBCertificateStore("/var/lib/looseberry/certs")
if err != nil {
    panic(err)
}
defer store.Close()

// Save a certificate
err = store.SaveCertificate(cert)
if err != nil {
    panic(err)
}

// Get all certificates for round 10
certs, err := store.GetCertificatesByRound(10)

// Get specific validator's certificate
validatorCert, err := store.GetCertificateForValidator(10, 0)

// Get highest round
highest := store.HighestRound()
fmt.Printf("Highest round: %d\n", highest)

// Clean up
err = store.DeleteCertificatesBefore(50)
```

---

### TxIndex Interface

Fast O(1) transaction lookup index.

```go
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

#### Methods

##### AddTx

```go
AddTx(txHash, batchHash types.Hash) error
```
Adds a transaction hash to batch hash mapping.

##### AddBatch

```go
AddBatch(batch *types.Batch) error
```
Indexes all transactions in a batch.

##### GetBatchForTx

```go
GetBatchForTx(txHash types.Hash) (types.Hash, error)
```
Returns the batch hash containing the transaction.

##### HasTx

```go
HasTx(txHash types.Hash) bool
```
Returns true if the transaction exists.

##### RemoveTxsForBatch

```go
RemoveTxsForBatch(batchHash types.Hash) error
```
Removes all transaction mappings for a batch.

##### PruneOlderThan

```go
PruneOlderThan(round uint64, batchStore BatchStore) (int, error)
```
Removes all transaction mappings for batches older than the given round. Returns the number of batches pruned.

##### Close

```go
Close() error
```
Closes the index and releases resources.

#### Implementation

**Memory Implementation:**
```go
func NewMemoryTxIndex() *MemoryTxIndex
```
In-memory index (currently the only implementation).

#### Example

```go
index := store.NewMemoryTxIndex()
defer index.Close()

// Index a batch
err := index.AddBatch(batch)
if err != nil {
    panic(err)
}

// Check if transaction exists
txHash := types.HashBytes(tx)
if index.HasTx(txHash) {
    // Get batch containing this transaction
    batchHash, err := index.GetBatchForTx(txHash)
    if err == nil {
        fmt.Printf("Transaction in batch: %s\n", batchHash.String())
    }
}

// Prune old transactions
pruned, err := index.PruneOlderThan(50, batchStore)
fmt.Printf("Pruned %d batches from index\n", pruned)
```

---

## Network Interface

### Network Interface

Interface for network communication between validators.

```go
type Network interface {
    // Broadcast methods
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

    // Lifecycle
    ValidatorID() uint16
    Start() error
    Stop() error
}
```

#### Broadcast Methods

##### BroadcastBatch

```go
BroadcastBatch(batch *types.Batch) error
```
Broadcasts a batch to all validators.

##### BroadcastHeader

```go
BroadcastHeader(header *types.Header) error
```
Broadcasts a header to all validators.

##### BroadcastCertificate

```go
BroadcastCertificate(cert *types.Certificate) error
```
Broadcasts a certificate to all validators.

#### Send Methods

##### SendVote

```go
SendVote(validator uint16, vote *types.Vote) error
```
Sends a vote to a specific validator (the header's author).

##### SendBatchAck

```go
SendBatchAck(validator uint16, ack *BatchAckMessage) error
```
Sends a batch acknowledgment to a specific validator.

##### SendBatchRequest

```go
SendBatchRequest(validator uint16, req *BatchRequestMessage) error
```
Sends a batch request to a specific validator.

##### SendBatchResponse

```go
SendBatchResponse(validator uint16, resp *BatchResponseMessage) error
```
Sends a batch response to a specific validator.

##### SendSyncRequest

```go
SendSyncRequest(validator uint16, req *SyncRequest) error
```
Sends a sync request to a specific validator.

##### SendSyncResponse

```go
SendSyncResponse(validator uint16, resp *SyncResponse) error
```
Sends a sync response to a specific validator.

#### Receive Channels

All receive methods return read-only channels that deliver incoming messages. The channels should be buffered with a reasonable size (e.g., 1000).

---

### Message Types

#### BatchMessage

```go
type BatchMessage struct {
    Batch *types.Batch
    From  uint16  // Validator who sent it
}
```

Represents a received batch.

#### BatchAckMessage

```go
type BatchAckMessage struct {
    BatchDigest types.Hash
    Validator   uint16
    Signature   types.Signature
}
```

Represents a batch acknowledgment.

#### BatchRequestMessage

```go
type BatchRequestMessage struct {
    BatchDigest types.Hash
    Requester   uint16
}
```

Represents a request for a batch.

#### BatchResponseMessage

```go
type BatchResponseMessage struct {
    Batch *types.Batch
    Found bool   // True if batch was found
    From  uint16 // Validator who responded
}
```

Represents a response to a batch request.

#### HeaderMessage

```go
type HeaderMessage struct {
    Header *types.Header
    From   uint16
}
```

Represents a received header.

#### VoteMessage

```go
type VoteMessage struct {
    Vote *types.Vote
    From uint16
}
```

Represents a received vote.

#### CertificateMessage

```go
type CertificateMessage struct {
    Certificate *types.Certificate
    From        uint16
}
```

Represents a received certificate.

#### SyncRequest

```go
type SyncRequest struct {
    FromRound uint64 // Lowest round needed
    ToRound   uint64 // Highest round needed (0 = latest)
    Requester uint16
}
```

Represents a request to sync certificates.

#### SyncResponse

```go
type SyncResponse struct {
    Certificates []*types.Certificate
    Batches      []*types.Batch  // Batches referenced by certificates
    FromRound    uint64
    ToRound      uint64
}
```

Represents a response to a sync request.

#### SyncRequestMessage

```go
type SyncRequestMessage struct {
    Request *SyncRequest
    From    uint16
}
```

Wraps a sync request with sender info.

#### SyncResponseMessage

```go
type SyncResponseMessage struct {
    Response *SyncResponse
    From     uint16
}
```

Wraps a sync response with sender info.

---

### MockNetwork

Mock network implementation for testing.

```go
type MockNetwork struct {
    // private fields
}
```

#### Constructor

```go
func NewMockNetwork(validatorID uint16, cfg Config) *MockNetwork
```

Creates a new mock network.

**Parameters:**
- `validatorID uint16` - This validator's ID
- `cfg Config` - Network configuration

#### Testing Methods

```go
func (m *MockNetwork) Connect(peer *MockNetwork)
```
Connects this network to a peer network (bidirectional).

```go
func (m *MockNetwork) Disconnect(validatorID uint16)
```
Disconnects from a peer.

```go
func (m *MockNetwork) PeerCount() int
```
Returns the number of connected peers.

```go
func (m *MockNetwork) Stats() MockNetworkStats
```
Returns network statistics.

```go
func (m *MockNetwork) ResetStats()
```
Resets network statistics.

```go
func (m *MockNetwork) InjectBatchMessage(msg *BatchMessage)
func (m *MockNetwork) InjectHeaderMessage(msg *HeaderMessage)
func (m *MockNetwork) InjectVoteMessage(msg *VoteMessage)
func (m *MockNetwork) InjectCertificateMessage(msg *CertificateMessage)
func (m *MockNetwork) InjectSyncRequest(req *SyncRequest)
func (m *MockNetwork) InjectSyncResponse(resp *SyncResponseMessage)
```
Injects messages for testing.

#### MockNetworkStats

```go
type MockNetworkStats struct {
    BatchesBroadcast      int
    HeadersBroadcast      int
    CertificatesBroadcast int
    VotesSent             int
    BatchAcksSent         int
    BatchRequestsSent     int
    BatchResponsesSent    int
    SyncRequestsSent      int
    SyncResponsesSent     int
}
```

#### Example

```go
// Create network for validator 0
net0 := network.NewMockNetwork(0, network.DefaultConfig())
net1 := network.NewMockNetwork(1, network.DefaultConfig())

// Connect networks
net0.Connect(net1)

// Start networks
net0.Start()
net1.Start()

// Broadcast a batch
batch := types.NewBatch(0, 0, 1, txs)
net0.BroadcastBatch(batch)

// Receive on peer
msg := <-net1.BatchMessages()
fmt.Printf("Received batch from validator %d\n", msg.From)

// Check statistics
stats := net0.Stats()
fmt.Printf("Batches broadcast: %d\n", stats.BatchesBroadcast)
```

---

## Metrics

Metrics structure for monitoring Looseberry performance and health.

```go
type Metrics struct {
    // Transaction metrics
    PendingTxCount   int
    PendingTxBytes   int64
    TotalTxAdded     uint64
    TotalTxRejected  uint64
    TotalTxCommitted uint64

    // Batch metrics
    TotalBatches          uint64
    TotalBatchesCertified uint64

    // Round metrics
    CurrentRound   uint64
    CommittedRound uint64
    HighestRound   uint64

    // Worker metrics
    WorkerCount int
    WorkerLoad  float64  // 0.0 to 1.0

    // Flow control metrics
    IsPaused       bool
    PauseCount     uint64
    ResumeCount    uint64
    UncommittedGap uint64

    // Certificate metrics
    TotalCertificates uint64
}
```

### Field Descriptions

#### Transaction Metrics

- `PendingTxCount int` - Current number of pending transactions across all workers.
- `PendingTxBytes int64` - Current total bytes of pending transactions.
- `TotalTxAdded uint64` - Cumulative count of transactions added since start.
- `TotalTxRejected uint64` - Cumulative count of transactions rejected.
- `TotalTxCommitted uint64` - Cumulative count of transactions committed (requires external tracking).

#### Batch Metrics

- `TotalBatches uint64` - Cumulative count of batches created.
- `TotalBatchesCertified uint64` - Cumulative count of batches that received certificates.

#### Round Metrics

- `CurrentRound uint64` - Current DAG round being produced.
- `CommittedRound uint64` - Highest round committed by consensus.
- `HighestRound uint64` - Highest round in the DAG.

#### Worker Metrics

- `WorkerCount int` - Current number of active workers.
- `WorkerLoad float64` - Current worker pool load (0.0 = idle, 1.0 = saturated). Used for auto-scaling decisions.

#### Flow Control Metrics

- `IsPaused bool` - True if DAG production is paused due to flow control.
- `PauseCount uint64` - Cumulative count of flow control pauses.
- `ResumeCount uint64` - Cumulative count of flow control resumes.
- `UncommittedGap uint64` - Current gap between highest and committed rounds.

#### Certificate Metrics

- `TotalCertificates uint64` - Cumulative count of certificates formed.

### Interpretation Guide

#### Health Indicators

**Healthy State:**
- `PendingTxCount` is low and stable
- `WorkerLoad` is between 0.2 and 0.8
- `IsPaused` is false
- `UncommittedGap` is below `MaxUncommittedRounds`
- `TotalTxRejected / TotalTxAdded` ratio is low

**Warning Signs:**
- `PendingTxCount` growing without bound → increase workers or check consensus
- `WorkerLoad` consistently at 1.0 → increase MaxWorkers
- `IsPaused` is true → consensus is falling behind
- `UncommittedGap` approaching `MaxUncommittedRounds` → consensus needs to commit faster
- High `TotalTxRejected / TotalTxAdded` ratio → check TxValidator or backpressure config

#### Throughput Estimation

```go
// Batch rate (batches/second)
batchRate := (currentBatches - previousBatches) / timeDelta

// Transaction rate (transactions/second)
txRate := (currentTxAdded - previousTxAdded) / timeDelta

// Average batch size
avgBatchSize := TotalTxAdded / TotalBatches
```

#### Scaling Indicators

```go
// Should scale up workers
if metrics.WorkerLoad > 0.8 && metrics.WorkerCount < maxWorkers {
    // System is under load
}

// Can scale down workers
if metrics.WorkerLoad < 0.2 && metrics.WorkerCount > minWorkers {
    // System is underutilized
}
```

### Example

```go
metrics := lb.Metrics()

fmt.Printf("Pending transactions: %d (%d bytes)\n",
    metrics.PendingTxCount, metrics.PendingTxBytes)
fmt.Printf("Total added: %d, rejected: %d\n",
    metrics.TotalTxAdded, metrics.TotalTxRejected)
fmt.Printf("Workers: %d, Load: %.2f\n",
    metrics.WorkerCount, metrics.WorkerLoad)
fmt.Printf("DAG rounds - Current: %d, Committed: %d, Highest: %d\n",
    metrics.CurrentRound, metrics.CommittedRound, metrics.HighestRound)
fmt.Printf("Flow control - Paused: %v, Gap: %d\n",
    metrics.IsPaused, metrics.UncommittedGap)

// Alert on high rejection rate
rejectionRate := float64(metrics.TotalTxRejected) / float64(metrics.TotalTxAdded)
if rejectionRate > 0.1 {
    fmt.Printf("WARNING: High rejection rate: %.2f%%\n", rejectionRate*100)
}

// Alert on flow control
if metrics.IsPaused {
    fmt.Printf("WARNING: DAG production paused (uncommitted gap: %d)\n",
        metrics.UncommittedGap)
}
```

---

## Error Types

Looseberry defines comprehensive error types for different failure scenarios.

### Transaction Errors

```go
var (
    ErrTxAlreadyExists    = errors.New("transaction already exists")
    ErrTxValidationFailed = errors.New("transaction validation failed")
)
```

- `ErrTxAlreadyExists` - Transaction hash is already in the mempool (pending or committed).
- `ErrTxValidationFailed` - Transaction failed validation via TxValidator. **Not retryable.**

### Mempool Errors

```go
var (
    ErrMempoolFull        = errors.New("mempool is full")
    ErrWorkerBackpressure = errors.New("worker back-pressure: too many pending transactions")
)
```

- `ErrMempoolFull` - Mempool has reached capacity. **Retryable.**
- `ErrWorkerBackpressure` - Worker has too many pending transactions or bytes. **Retryable.**

### Batch Errors

```go
var (
    ErrInvalidBatch        = errors.New("invalid batch")
    ErrBatchNotFound       = errors.New("batch not found")
    ErrBatchTooLarge       = errors.New("batch exceeds maximum size")
    ErrEmptyBatch          = errors.New("batch has no transactions")
    ErrBatchDigestMismatch = errors.New("batch digest does not match computed digest")
)
```

- `ErrInvalidBatch` - Batch structure or content is invalid. **Byzantine.**
- `ErrBatchNotFound` - Requested batch not found in storage.
- `ErrBatchTooLarge` - Batch exceeds MaxBatchBytes limit.
- `ErrEmptyBatch` - Batch contains no transactions.
- `ErrBatchDigestMismatch` - Batch digest doesn't match computed value. **Byzantine.**

### Header Errors

```go
var (
    ErrInvalidHeader      = errors.New("invalid header")
    ErrDuplicateHeader    = errors.New("duplicate header from same author for same round")
    ErrMissingParents     = errors.New("header is missing required parent certificates")
    ErrInvalidParentRound = errors.New("parent certificate is from wrong round")
    ErrHeaderTooFarAhead  = errors.New("header round is too far ahead of current round")
)
```

- `ErrInvalidHeader` - Header structure or content is invalid. **Byzantine.**
- `ErrDuplicateHeader` - Validator produced multiple headers for same round. **Byzantine (equivocation).**
- `ErrMissingParents` - Header doesn't reference required 2f+1 parent certificates.
- `ErrInvalidParentRound` - Parent certificates are not from round-1.
- `ErrHeaderTooFarAhead` - Header round exceeds MaxRoundGap ahead of current.

### Vote Errors

```go
var (
    ErrInvalidVote     = errors.New("invalid vote")
    ErrDuplicateVote   = errors.New("duplicate vote from same validator")
    ErrVoteWrongHeader = errors.New("vote is for different header")
)
```

- `ErrInvalidVote` - Vote structure or content is invalid. **Byzantine.**
- `ErrDuplicateVote` - Validator voted twice on same header. **Byzantine.**
- `ErrVoteWrongHeader` - Vote digest doesn't match header digest.

### Certificate Errors

```go
var (
    ErrInvalidCertificate  = errors.New("invalid certificate")
    ErrCertificateNotFound = errors.New("certificate not found")
    ErrInsufficientQuorum  = errors.New("insufficient quorum: need 2f+1 votes")
)
```

- `ErrInvalidCertificate` - Certificate structure or signatures are invalid.
- `ErrCertificateNotFound` - Requested certificate not found in storage.
- `ErrInsufficientQuorum` - Certificate has fewer than 2f+1 votes.

### Signature Errors

```go
var (
    ErrInvalidSignature = errors.New("invalid signature")
)
```

- `ErrInvalidSignature` - Cryptographic signature verification failed. **Byzantine.**

### Validator Errors

```go
var (
    ErrValidatorNotFound = errors.New("validator not found")
    ErrInvalidValidator  = errors.New("invalid validator index")
    ErrEpochMismatch     = errors.New("epoch mismatch")
)
```

- `ErrValidatorNotFound` - Validator index not in current validator set.
- `ErrInvalidValidator` - Validator index is out of bounds.
- `ErrEpochMismatch` - Message is from different epoch than expected.

### Round Errors

```go
var (
    ErrRoundMismatch    = errors.New("round mismatch")
    ErrRoundTooOld      = errors.New("round is too old")
    ErrRoundNotAdvanced = errors.New("cannot advance to round without sufficient certificates")
)
```

- `ErrRoundMismatch` - Message round doesn't match expected round.
- `ErrRoundTooOld` - Message round is older than committed round.
- `ErrRoundNotAdvanced` - Cannot advance to next round without 2f+1 certificates.

### Flow Control Errors

```go
var (
    ErrFlowControlPaused = errors.New("flow control: DAG production paused, waiting for consensus")
)
```

- `ErrFlowControlPaused` - DAG production is paused due to large uncommitted gap. **Retryable.**

### Sync Errors

```go
var (
    ErrSyncRequired = errors.New("sync required: node is too far behind")
    ErrSyncTimeout  = errors.New("sync timeout")
    ErrSyncFailed   = errors.New("sync failed")
)
```

- `ErrSyncRequired` - Node needs to sync before continuing.
- `ErrSyncTimeout` - Sync request timed out. **Retryable.**
- `ErrSyncFailed` - Sync operation failed.

### Lifecycle Errors

```go
var (
    ErrNotRunning     = errors.New("looseberry is not running")
    ErrAlreadyRunning = errors.New("looseberry is already running")
)
```

- `ErrNotRunning` - Operation requires Looseberry to be running.
- `ErrAlreadyRunning` - Looseberry is already started.

### Error Classification

#### IsRetryable

```go
func IsRetryable(err error) bool
```

Returns true if the error is temporary and the operation can be retried.

**Retryable Errors:**
- `ErrWorkerBackpressure`
- `ErrMempoolFull`
- `ErrFlowControlPaused`
- `ErrSyncTimeout`

**Example:**
```go
err := lb.AddTx(tx)
if err != nil {
    if types.IsRetryable(err) {
        // Wait and retry
        time.Sleep(100 * time.Millisecond)
        err = lb.AddTx(tx)
    } else {
        // Permanent failure
        return err
    }
}
```

#### IsByzantine

```go
func IsByzantine(err error) bool
```

Returns true if the error indicates Byzantine (malicious or faulty) behavior.

**Byzantine Errors:**
- `ErrInvalidSignature`
- `ErrDuplicateHeader`
- `ErrDuplicateVote`
- `ErrInvalidBatch`
- `ErrInvalidHeader`
- `ErrInvalidVote`

**Example:**
```go
err := processHeader(header)
if err != nil {
    if types.IsByzantine(err) {
        // Log Byzantine behavior for slashing/reputation
        logByzantineBehavior(header.Author, err)
        // Potentially slash validator
    }
}
```

### Error Handling Patterns

#### Transaction Submission

```go
func submitTx(lb looseberry.DAGMempool, tx []byte) error {
    for retries := 0; retries < 3; retries++ {
        err := lb.AddTx(tx)
        if err == nil {
            return nil
        }

        if types.IsRetryable(err) {
            // Exponential backoff
            time.Sleep(time.Duration(100*(1<<retries)) * time.Millisecond)
            continue
        }

        // Non-retryable error
        return fmt.Errorf("transaction rejected: %w", err)
    }

    return fmt.Errorf("transaction failed after retries")
}
```

#### Header Processing

```go
func processHeader(header *types.Header, validators types.ValidatorSet) error {
    // Verify signature
    author := validators.GetByIndex(header.Author)
    if author == nil {
        return types.ErrValidatorNotFound
    }

    if !header.Verify(author.PublicKey) {
        // Byzantine behavior - invalid signature
        return types.ErrInvalidSignature
    }

    // Verify parent count
    if header.ParentCount() < validators.Quorum() {
        return types.ErrMissingParents
    }

    // Additional validation...
    return nil
}
```

#### Certificate Validation

```go
func validateCertificate(cert *types.Certificate, validators types.ValidatorSet) error {
    // Verify certificate
    if err := cert.Verify(validators); err != nil {
        if types.IsByzantine(err) {
            // Log Byzantine evidence
            logByzantine(cert.Author(), "invalid certificate", err)
        }
        return err
    }

    // Check quorum
    if !cert.HasQuorum(validators.Count()) {
        return types.ErrInsufficientQuorum
    }

    return nil
}
```

---

## Complete Usage Example

Here's a complete example demonstrating the full Looseberry API:

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/network"
    "github.com/blockberries/looseberry/types"
)

func main() {
    // 1. Generate validator keys
    signers := make([]*types.Ed25519Signer, 4)
    for i := 0; i < 4; i++ {
        signer, err := types.GenerateEd25519Signer(uint16(i))
        if err != nil {
            log.Fatalf("Failed to generate signer: %v", err)
        }
        signers[i] = signer
    }

    // 2. Create validator set
    validators := []*types.Validator{
        {Index: 0, PublicKey: signers[0].PublicKey(), Power: 1, Address: "/ip4/127.0.0.1/tcp/9000"},
        {Index: 1, PublicKey: signers[1].PublicKey(), Power: 1, Address: "/ip4/127.0.0.1/tcp/9001"},
        {Index: 2, PublicKey: signers[2].PublicKey(), Power: 1, Address: "/ip4/127.0.0.1/tcp/9002"},
        {Index: 3, PublicKey: signers[3].PublicKey(), Power: 1, Address: "/ip4/127.0.0.1/tcp/9003"},
    }
    vs := types.NewSimpleValidatorSet(validators, 0)

    // 3. Create configuration
    cfg := looseberry.DefaultConfig()
    cfg.ValidatorIndex = 0
    cfg.Signer = signers[0]
    cfg.Storage.InMemory = true

    // 4. Add transaction validator
    cfg.TxValidator = func(tx []byte) error {
        if len(tx) == 0 {
            return fmt.Errorf("empty transaction")
        }
        if len(tx) > 1024*1024 {
            return fmt.Errorf("transaction too large")
        }
        return nil
    }

    // 5. Create Looseberry instance
    lb, err := looseberry.New(cfg)
    if err != nil {
        log.Fatalf("Failed to create Looseberry: %v", err)
    }

    // 6. Set validator set and network
    lb.SetValidatorSet(vs)

    netCfg := network.DefaultConfig()
    mockNet := network.NewMockNetwork(0, netCfg)
    lb.SetNetwork(mockNet)

    // 7. Start Looseberry
    if err := lb.Start(); err != nil {
        log.Fatalf("Failed to start Looseberry: %v", err)
    }
    defer lb.Stop()

    fmt.Println("Looseberry started successfully")

    // 8. Submit transactions
    for i := 0; i < 100; i++ {
        tx := []byte(fmt.Sprintf("transaction-%d", i))
        err := lb.AddTx(tx)
        if err != nil {
            if types.IsRetryable(err) {
                // Retry after short delay
                time.Sleep(10 * time.Millisecond)
                err = lb.AddTx(tx)
            }
            if err != nil {
                log.Printf("Failed to add transaction: %v", err)
            }
        }
    }

    // 9. Monitor metrics
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for i := 0; i < 10; i++ {
        <-ticker.C

        metrics := lb.Metrics()
        fmt.Printf("\n=== Metrics at T+%ds ===\n", i+1)
        fmt.Printf("Pending: %d txs (%d bytes)\n", metrics.PendingTxCount, metrics.PendingTxBytes)
        fmt.Printf("Total: added=%d, rejected=%d\n", metrics.TotalTxAdded, metrics.TotalTxRejected)
        fmt.Printf("Batches: %d total, %d certified\n", metrics.TotalBatches, metrics.TotalBatchesCertified)
        fmt.Printf("Workers: %d active, load=%.2f\n", metrics.WorkerCount, metrics.WorkerLoad)
        fmt.Printf("Rounds: current=%d, committed=%d, highest=%d\n",
            metrics.CurrentRound, metrics.CommittedRound, metrics.HighestRound)

        if metrics.IsPaused {
            fmt.Printf("⚠️  PAUSED: uncommitted gap=%d\n", metrics.UncommittedGap)
        }
    }

    // 10. Check transaction existence
    tx := []byte("transaction-0")
    txHash := types.HashBytes(tx)
    if lb.HasTx(txHash[:]) {
        fmt.Println("\n✓ Transaction found in mempool")
    }

    // 11. Simulate consensus committing rounds
    for round := uint64(1); round <= 5; round++ {
        lb.NotifyCommitted(round)
        fmt.Printf("Committed round %d\n", round)
        time.Sleep(500 * time.Millisecond)
    }

    // 12. Reap certified batches
    batches := lb.ReapCertifiedBatches(1024 * 1024)
    fmt.Printf("\nReaped %d certified batches\n", len(batches))
    for _, cb := range batches {
        fmt.Printf("  Round %d, Validator %d: %d txs\n",
            cb.Certificate.Header.Round,
            cb.Certificate.Header.Author,
            len(cb.Batch.Transactions))
    }

    // 13. Flush mempool
    lb.Flush()
    fmt.Println("\nMempool flushed")

    fmt.Printf("Final size: %d txs\n", lb.Size())
}
```

---

## See Also

- [README.md](../README.md) - Overview and quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed system architecture
- [CHANGELOG.md](CHANGELOG.md) - Release notes and version history
