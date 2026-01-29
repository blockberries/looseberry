# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
