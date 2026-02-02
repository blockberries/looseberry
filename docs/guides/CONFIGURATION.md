# Configuration Guide

Comprehensive guide to configuring Looseberry for different environments and workloads.

## Table of Contents

- [Configuration Overview](#configuration-overview)
- [Worker Configuration](#worker-configuration)
- [Primary Configuration](#primary-configuration)
- [Sync Configuration](#sync-configuration)
- [Storage Configuration](#storage-configuration)
- [Garbage Collection Configuration](#garbage-collection-configuration)
- [Flow Control Configuration](#flow-control-configuration)
- [Configuration Profiles](#configuration-profiles)
- [Performance Tuning](#performance-tuning)

## Configuration Overview

Looseberry uses a hierarchical configuration structure:

```go
type Config struct {
    ValidatorIndex uint16        // This validator's index
    Signer         types.Signer  // Cryptographic signer
    TxValidator    TxValidator   // Optional transaction validator

    Worker      WorkerConfig      // Worker pool configuration
    Primary     PrimaryConfig     // Primary node configuration
    Sync        SyncConfig        // Synchronization configuration
    Storage     StorageConfig     // Storage backend configuration
    GC          GCConfig          // Garbage collection configuration
    FlowControl FlowControlConfig // Flow control configuration
}
```

### Creating a Configuration

**Default configuration** (recommended starting point):

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer
```

**Custom configuration**:

```go
cfg := &looseberry.Config{
    ValidatorIndex: 0,
    Signer:         signer,
    Worker:         looseberry.DefaultWorkerConfig(),
    Primary:        looseberry.DefaultPrimaryConfig(),
    Sync:           looseberry.DefaultSyncConfig(),
    Storage:        looseberry.DefaultStorageConfig(),
    GC:             looseberry.DefaultGCConfig(),
    FlowControl:    looseberry.DefaultFlowControlConfig(),
}
```

## Worker Configuration

Workers collect transactions into batches for efficient dissemination.

```go
type WorkerConfig struct {
    MinWorkers         int           // Minimum number of workers
    MaxWorkers         int           // Maximum number of workers
    BatchSize          int           // Transactions per batch
    BatchTimeout       time.Duration // Max time before creating batch
    MaxBatchBytes      int64         // Max batch size in bytes
    MaxPendingTxs      int           // Max pending transactions per worker
    MaxPendingBytes    int64         // Max pending bytes per worker
    ScalingInterval    time.Duration // How often to check for scaling
    ScaleUpThreshold   float64       // Load ratio to scale up
    ScaleDownThreshold float64       // Load ratio to scale down
    DrainTimeout       time.Duration // Max time to drain worker
}
```

### Default Values

```go
cfg.Worker = looseberry.WorkerConfig{
    MinWorkers:         1,
    MaxWorkers:         8,
    BatchSize:          500,
    BatchTimeout:       100 * time.Millisecond,
    MaxBatchBytes:      512 * 1024,        // 512KB
    MaxPendingTxs:      10000,
    MaxPendingBytes:    50 * 1024 * 1024,  // 50MB
    ScalingInterval:    5 * time.Second,
    ScaleUpThreshold:   0.8,               // 80% load
    ScaleDownThreshold: 0.2,               // 20% load
    DrainTimeout:       30 * time.Second,
}
```

### Worker Pool Sizing

**MinWorkers**: Starting number of workers
- **Low traffic**: 1-2 workers
- **Medium traffic**: 2-4 workers
- **High traffic**: 4-8 workers

```go
// Low traffic
cfg.Worker.MinWorkers = 1

// High traffic
cfg.Worker.MinWorkers = 4
```

**MaxWorkers**: Maximum number of workers
- More workers = higher throughput but more overhead
- 8 workers is usually sufficient for most workloads

```go
cfg.Worker.MaxWorkers = 8 // Recommended maximum
```

### Batch Configuration

**BatchSize**: Number of transactions per batch

Small batches (100-200):
- Lower latency
- More batches and headers
- Higher network overhead

Large batches (500-1000):
- Higher latency
- Fewer batches and headers
- Better network efficiency

```go
// Low latency (small batches)
cfg.Worker.BatchSize = 200

// High throughput (large batches)
cfg.Worker.BatchSize = 1000
```

**BatchTimeout**: Maximum time before creating a batch

```go
// Low latency (fast batching)
cfg.Worker.BatchTimeout = 50 * time.Millisecond

// Balanced (default)
cfg.Worker.BatchTimeout = 100 * time.Millisecond

// High throughput (wait for full batches)
cfg.Worker.BatchTimeout = 500 * time.Millisecond
```

**MaxBatchBytes**: Maximum batch size in bytes

```go
// Small batches
cfg.Worker.MaxBatchBytes = 256 * 1024 // 256KB

// Default
cfg.Worker.MaxBatchBytes = 512 * 1024 // 512KB

// Large batches
cfg.Worker.MaxBatchBytes = 1024 * 1024 // 1MB
```

### Backpressure Configuration

**MaxPendingTxs**: Maximum pending transactions per worker

```go
// Low memory
cfg.Worker.MaxPendingTxs = 1000

// Default
cfg.Worker.MaxPendingTxs = 10000

// High memory
cfg.Worker.MaxPendingTxs = 50000
```

**MaxPendingBytes**: Maximum pending bytes per worker

```go
// Low memory (~10MB per worker)
cfg.Worker.MaxPendingBytes = 10 * 1024 * 1024

// Default (~50MB per worker)
cfg.Worker.MaxPendingBytes = 50 * 1024 * 1024

// High memory (~200MB per worker)
cfg.Worker.MaxPendingBytes = 200 * 1024 * 1024
```

### Auto-Scaling Configuration

**ScaleUpThreshold**: Load ratio to trigger scale up (0.0-1.0)

```go
// Aggressive scaling (scale up earlier)
cfg.Worker.ScaleUpThreshold = 0.6

// Conservative scaling (scale up later)
cfg.Worker.ScaleUpThreshold = 0.9
```

**ScaleDownThreshold**: Load ratio to trigger scale down (0.0-1.0)

```go
// Aggressive scaling (scale down earlier)
cfg.Worker.ScaleDownThreshold = 0.3

// Conservative scaling (scale down later)
cfg.Worker.ScaleDownThreshold = 0.1
```

**ScalingInterval**: How often to check for scaling

```go
// Frequent checks
cfg.Worker.ScalingInterval = 2 * time.Second

// Infrequent checks
cfg.Worker.ScalingInterval = 10 * time.Second
```

## Primary Configuration

The primary node creates headers, collects votes, and forms certificates.

```go
type PrimaryConfig struct {
    HeaderTimeout       time.Duration // Time between headers
    MaxBatchesPerHeader int           // Max batch refs per header
    MaxRoundGap         uint64        // Max rounds ahead to accept
    VoteTimeout         time.Duration // Vote buffer timeout
    AllowEmptyHeaders   bool          // Allow headers with no batches
}
```

### Default Values

```go
cfg.Primary = looseberry.PrimaryConfig{
    HeaderTimeout:       500 * time.Millisecond,
    MaxBatchesPerHeader: 100,
    MaxRoundGap:         10,
    VoteTimeout:         30 * time.Second,
    AllowEmptyHeaders:   true,
}
```

### Header Creation Rate

**HeaderTimeout**: Time between header creation attempts

```go
// Fast progression (low latency)
cfg.Primary.HeaderTimeout = 250 * time.Millisecond

// Balanced (default)
cfg.Primary.HeaderTimeout = 500 * time.Millisecond

// Conservative (high throughput per header)
cfg.Primary.HeaderTimeout = 1 * time.Second
```

### Header Size

**MaxBatchesPerHeader**: Maximum batch references per header

```go
// Small headers
cfg.Primary.MaxBatchesPerHeader = 50

// Default
cfg.Primary.MaxBatchesPerHeader = 100

// Large headers
cfg.Primary.MaxBatchesPerHeader = 200
```

### Round Gap Protection

**MaxRoundGap**: Maximum rounds ahead to accept headers from other validators

```go
// Strict (prevent runaway rounds)
cfg.Primary.MaxRoundGap = 5

// Balanced (default)
cfg.Primary.MaxRoundGap = 10

// Permissive (handle network partitions)
cfg.Primary.MaxRoundGap = 20
```

### Empty Headers

**AllowEmptyHeaders**: Allow headers with no batches (for liveness)

```go
// Always create headers (better liveness)
cfg.Primary.AllowEmptyHeaders = true

// Only create headers with batches (better efficiency)
cfg.Primary.AllowEmptyHeaders = false
```

## Sync Configuration

Synchronization catches up nodes that fall behind.

```go
type SyncConfig struct {
    SyncInterval  time.Duration // How often to check for sync
    SyncThreshold uint64        // Rounds behind to trigger sync
    SyncBatchSize int           // Certificates per sync request
    SyncTimeout   time.Duration // Timeout for sync requests
}
```

### Default Values

```go
cfg.Sync = looseberry.SyncConfig{
    SyncInterval:  10 * time.Second,
    SyncThreshold: 5,
    SyncBatchSize: 100,
    SyncTimeout:   30 * time.Second,
}
```

### Sync Trigger

**SyncThreshold**: Rounds behind to trigger synchronization

```go
// Aggressive (sync quickly)
cfg.Sync.SyncThreshold = 3

// Default
cfg.Sync.SyncThreshold = 5

// Conservative (avoid unnecessary syncs)
cfg.Sync.SyncThreshold = 10
```

### Sync Performance

**SyncBatchSize**: Number of certificates per sync request

```go
// Small requests (slower but less memory)
cfg.Sync.SyncBatchSize = 50

// Default
cfg.Sync.SyncBatchSize = 100

// Large requests (faster but more memory)
cfg.Sync.SyncBatchSize = 200
```

## Storage Configuration

Storage determines where batches and certificates are persisted.

```go
type StorageConfig struct {
    DataDir  string // Directory for persistent storage
    InMemory bool   // Use in-memory storage (testing only)
}
```

### Default Values

```go
cfg.Storage = looseberry.StorageConfig{
    DataDir:  "data/looseberry",
    InMemory: false,
}
```

### Testing Configuration

```go
cfg.Storage.InMemory = true
```

### Production Configuration

```go
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"
```

Ensure the data directory:
- Has sufficient disk space (grows with traffic)
- Is on fast storage (SSD recommended)
- Is backed up regularly

## Garbage Collection Configuration

Garbage collection prunes old rounds and optionally recovers uncommitted transactions.

```go
type GCConfig struct {
    GCDepth    int  // Rounds to keep after commit
    RecoverTxs bool // Re-inject uncommitted transactions
}
```

### Default Values

```go
cfg.GC = looseberry.GCConfig{
    GCDepth:    50,
    RecoverTxs: true,
}
```

### GC Depth

**GCDepth**: Number of rounds to keep after commit

```go
// Aggressive GC (low memory)
cfg.GC.GCDepth = 20

// Default
cfg.GC.GCDepth = 50

// Conservative (high memory, useful for debugging)
cfg.GC.GCDepth = 200
```

Trade-offs:
- **Small depth**: Lower memory/storage but can't serve old data
- **Large depth**: Higher memory/storage but better for sync and debugging

### Transaction Recovery

**RecoverTxs**: Re-inject uncommitted transactions during GC

```go
// Recover uncommitted transactions (recommended)
cfg.GC.RecoverTxs = true

// Don't recover (transactions may be lost)
cfg.GC.RecoverTxs = false
```

## Flow Control Configuration

Flow control prevents unbounded DAG growth.

```go
type FlowControlConfig struct {
    MaxUncommittedRounds int // Max rounds ahead of commit
}
```

### Default Values

```go
cfg.FlowControl = looseberry.FlowControlConfig{
    MaxUncommittedRounds: 100,
}
```

### Uncommitted Round Limit

**MaxUncommittedRounds**: Maximum rounds ahead of last commit

```go
// Tight coupling (low memory)
cfg.FlowControl.MaxUncommittedRounds = 50

// Default
cfg.FlowControl.MaxUncommittedRounds = 100

// Loose coupling (handle slow consensus)
cfg.FlowControl.MaxUncommittedRounds = 200
```

When the limit is reached:
- `AddTx()` returns `ErrFlowControlPaused`
- Workers stop creating new batches
- Primary stops creating new headers
- System waits for consensus to catch up

## Configuration Profiles

### Development Profile

Optimized for local development and testing:

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

// Fast progression, low resource usage
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2
cfg.Worker.BatchSize = 100
cfg.Worker.BatchTimeout = 50 * time.Millisecond
cfg.Worker.MaxPendingTxs = 1000

cfg.Primary.HeaderTimeout = 250 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 50

cfg.Storage.InMemory = true

cfg.GC.GCDepth = 20
```

### Testing Profile

Optimized for integration tests:

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

// Predictable behavior
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 1
cfg.Worker.BatchSize = 10
cfg.Worker.BatchTimeout = 100 * time.Millisecond

cfg.Primary.HeaderTimeout = 200 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 10

cfg.Storage.InMemory = true

cfg.GC.GCDepth = 10
```

### Production Profile

Optimized for production deployment:

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

// High throughput, reliable
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
cfg.Worker.BatchSize = 500
cfg.Worker.BatchTimeout = 100 * time.Millisecond
cfg.Worker.MaxBatchBytes = 512 * 1024
cfg.Worker.MaxPendingTxs = 50000
cfg.Worker.MaxPendingBytes = 200 * 1024 * 1024

cfg.Primary.HeaderTimeout = 500 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 100
cfg.Primary.AllowEmptyHeaders = true

cfg.Sync.SyncThreshold = 5
cfg.Sync.SyncBatchSize = 100

cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/var/lib/looseberry/data"

cfg.GC.GCDepth = 100
cfg.GC.RecoverTxs = true

cfg.FlowControl.MaxUncommittedRounds = 100
```

### High-Throughput Profile

Optimized for maximum transaction throughput:

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

// Maximum throughput
cfg.Worker.MinWorkers = 8
cfg.Worker.MaxWorkers = 8
cfg.Worker.BatchSize = 1000
cfg.Worker.BatchTimeout = 200 * time.Millisecond
cfg.Worker.MaxBatchBytes = 1024 * 1024
cfg.Worker.MaxPendingTxs = 100000
cfg.Worker.MaxPendingBytes = 500 * 1024 * 1024
cfg.Worker.ScaleUpThreshold = 0.9
cfg.Worker.ScaleDownThreshold = 0.1

cfg.Primary.HeaderTimeout = 1 * time.Second
cfg.Primary.MaxBatchesPerHeader = 200

cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/mnt/fast-ssd/looseberry/data"

cfg.GC.GCDepth = 50
cfg.GC.RecoverTxs = true

cfg.FlowControl.MaxUncommittedRounds = 200
```

### Low-Latency Profile

Optimized for minimum transaction latency:

```go
cfg := looseberry.DefaultConfig()
cfg.ValidatorIndex = 0
cfg.Signer = signer

// Minimum latency
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 4
cfg.Worker.BatchSize = 100
cfg.Worker.BatchTimeout = 25 * time.Millisecond
cfg.Worker.MaxBatchBytes = 256 * 1024

cfg.Primary.HeaderTimeout = 100 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 50

cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/mnt/nvme/looseberry/data"

cfg.GC.GCDepth = 100
```

## Performance Tuning

### Memory Usage

Estimate memory requirements:

```
Memory ≈ Workers × MaxPendingBytes + GCDepth × (Batches per round) × MaxBatchBytes
```

Example:
```
Workers: 8
MaxPendingBytes: 50MB
GCDepth: 100
Batches per round: ~32 (4 validators × 8 workers)
MaxBatchBytes: 512KB

Memory ≈ 8 × 50MB + 100 × 32 × 512KB
       ≈ 400MB + 1.6GB
       ≈ 2GB
```

### Throughput Optimization

**Increase throughput**:
1. Increase `BatchSize` (more transactions per batch)
2. Increase `MaxWorkers` (more parallelism)
3. Increase `MaxBatchesPerHeader` (more batches per round)
4. Increase `MaxPendingTxs` (more buffer capacity)

```go
cfg.Worker.BatchSize = 1000
cfg.Worker.MaxWorkers = 8
cfg.Primary.MaxBatchesPerHeader = 200
cfg.Worker.MaxPendingTxs = 100000
```

### Latency Optimization

**Reduce latency**:
1. Decrease `BatchTimeout` (faster batching)
2. Decrease `BatchSize` (smaller batches)
3. Decrease `HeaderTimeout` (faster rounds)
4. Use fast storage (NVMe SSD)

```go
cfg.Worker.BatchTimeout = 25 * time.Millisecond
cfg.Worker.BatchSize = 100
cfg.Primary.HeaderTimeout = 100 * time.Millisecond
cfg.Storage.DataDir = "/mnt/nvme/looseberry/data"
```

### Network Optimization

**Reduce network overhead**:
1. Increase `BatchSize` (fewer batches)
2. Increase `MaxBatchesPerHeader` (fewer headers)
3. Increase `BatchTimeout` (fuller batches)

```go
cfg.Worker.BatchSize = 1000
cfg.Primary.MaxBatchesPerHeader = 200
cfg.Worker.BatchTimeout = 200 * time.Millisecond
```

### Disk I/O Optimization

**Reduce disk I/O**:
1. Use SSD storage
2. Increase `GCDepth` (less frequent GC)
3. Use in-memory storage for testing

```go
// Fast storage
cfg.Storage.DataDir = "/mnt/ssd/looseberry/data"

// Less frequent GC
cfg.GC.GCDepth = 200

// Testing only
cfg.Storage.InMemory = true
```

## Validation

All configuration is validated on startup:

```go
cfg := looseberry.DefaultConfig()
// ... configure ...

if err := cfg.Validate(); err != nil {
    log.Fatalf("Invalid configuration: %v", err)
}
```

Common validation errors:
- `min_workers must be at least 1`
- `max_workers must be >= min_workers`
- `batch_size must be at least 1`
- `scale_up_threshold must be > scale_down_threshold`

## Next Steps

- **[Testing Guide](TESTING.md)**: Test your configuration
- **[Performance Tuning Tutorial](../tutorials/PERFORMANCE_TUNING.md)**: Optimize for your workload
- **[Deployment Guide](DEPLOYMENT.md)**: Deploy to production
- **[Monitoring Guide](MONITORING.md)**: Monitor your deployment
