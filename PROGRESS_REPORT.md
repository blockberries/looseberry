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

*Next Phase: Phase 3 - Worker Implementation*
