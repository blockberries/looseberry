# Concurrency Reference

Comprehensive guide to concurrency patterns and thread-safety in Looseberry.

## Overview

Looseberry is designed for high-concurrency environments with careful attention to thread-safety and performance.

## Concurrency Model

### Goroutine Architecture

```
Main Looseberry Goroutine
├── Message Processing Loop
└── Metrics Collection

Worker Pool (1-8 goroutines)
├── Worker 0: Batch Creation Loop
├── Worker 1: Batch Creation Loop
└── Worker N: Batch Creation Loop

Worker Scaler
└── Scaling Decision Loop

Primary
├── Header Creation Loop
├── Vote Processing
└── Certificate Formation

Sync Manager
├── Sync Check Loop
└── Sync Request/Response Handling

GC Manager
└── Garbage Collection Loop

Flow Controller
└── Flow Control Check Loop (integrated)
```

## Thread-Safety Guarantees

### Lock-Free Operations

**Atomic Operations** (no locks):
```go
// Round tracking
currentRound atomic.Uint64
committedRound atomic.Uint64

// Counters
totalTxAdded atomic.Uint64
totalTxRejected atomic.Uint64

// State flags
running atomic.Bool
paused atomic.Bool
```

**Benefits**:
- No lock contention
- Predictable performance
- Safe concurrent access

### Read-Heavy Data (RWMutex)

**Validator Set** (read-heavy):
```go
type validatorSetHolder struct {
    vs types.ValidatorSet
    mu sync.RWMutex
}

func (h *validatorSetHolder) Get() types.ValidatorSet {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return h.vs
}

func (h *validatorSetHolder) Set(vs types.ValidatorSet) {
    h.mu.Lock()
    defer h.mu.Unlock()
    h.vs = vs
}
```

**Benefits**:
- Multiple concurrent readers
- Exclusive writer access
- Efficient for read-heavy workloads

### Write-Heavy Data (Mutex)

**Worker State** (write-heavy):
```go
type Worker struct {
    pending []types.Transaction
    mu      sync.Mutex
}

func (w *Worker) AddTx(tx types.Transaction) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if len(w.pending) >= w.cfg.MaxPendingTxs {
        return types.ErrWorkerBackpressure
    }

    w.pending = append(w.pending, tx)
    return nil
}
```

### Channel-Based Communication

**Message Passing**:
```go
type Worker struct {
    batchesCh chan *types.Batch // Outgoing batches
    triggerCh chan struct{}      // Batch creation trigger
    stopCh    chan struct{}      // Shutdown signal
}

// Producer
func (w *Worker) produceBatch() {
    select {
    case w.batchesCh <- batch:
        // Batch sent
    case <-w.stopCh:
        return
    }
}

// Consumer
func processB atches(worker *Worker) {
    for {
        select {
        case batch := <-worker.batchesCh:
            // Process batch
        case <-worker.stopCh:
            return
        }
    }
}
```

## Concurrency Patterns

### Pattern 1: Single-Writer, Multiple-Readers

**Use Case**: DAG certificate index

```go
type DAG struct {
    certIndex   map[types.Hash]*types.Certificate
    certIndexMu sync.RWMutex
}

// Single writer (AddCertificate)
func (d *DAG) AddCertificate(cert *types.Certificate) error {
    d.certIndexMu.Lock()
    defer d.certIndexMu.Unlock()

    d.certIndex[cert.Digest()] = cert
    return nil
}

// Multiple readers
func (d *DAG) GetCertificate(digest types.Hash) *types.Certificate {
    d.certIndexMu.RLock()
    defer d.certIndexMu.RUnlock()

    return d.certIndex[digest]
}

func (d *DAG) HasCertificate(digest types.Hash) bool {
    d.certIndexMu.RLock()
    defer d.certIndexMu.RUnlock()

    _, ok := d.certIndex[digest]
    return ok
}
```

### Pattern 2: Work Distribution

**Use Case**: Transaction routing to workers

```go
type Pool struct {
    workers []*Worker
}

func (p *Pool) AddTx(tx types.Transaction) error {
    // Hash-based routing (deterministic, no locks)
    txHash := tx.Hash()
    hashValue := binary.BigEndian.Uint64(txHash[:8])
    workerIdx := int(hashValue % uint64(len(p.workers)))

    // Each worker handles its own concurrency
    return p.workers[workerIdx].AddTx(tx)
}
```

### Pattern 3: Fan-Out, Fan-In

**Use Case**: Batch acknowledgment collection

```go
type AckTracker struct {
    pending map[types.Hash]*AckState
    mu      sync.Mutex
}

type AckState struct {
    acks   map[uint16]bool // validator -> ack received
    quorum int
}

// Fan-out: Send to all validators
func (a *AckTracker) StartTracking(digest types.Hash, quorum int) {
    a.mu.Lock()
    defer a.mu.Unlock()

    a.pending[digest] = &AckState{
        acks:   make(map[uint16]bool),
        quorum: quorum,
    }
}

// Fan-in: Collect acks
func (a *AckTracker) RecordAck(digest types.Hash, validator uint16) bool {
    a.mu.Lock()
    defer a.mu.Unlock()

    state, ok := a.pending[digest]
    if !ok {
        return false
    }

    state.acks[validator] = true
    return len(state.acks) >= state.quorum
}
```

### Pattern 4: Graceful Shutdown

**Use Case**: Stopping goroutines cleanly

```go
type Component struct {
    stopCh chan struct{}
    wg     sync.WaitGroup
}

func (c *Component) Start() {
    c.stopCh = make(chan struct{})

    // Start goroutine
    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        ticker := time.NewTicker(1 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                // Do work
            case <-c.stopCh:
                return
            }
        }
    }()
}

func (c *Component) Stop() {
    close(c.stopCh)
    c.wg.Wait() // Wait for goroutine to finish
}
```

## Data Races and Prevention

### Common Race Conditions

**Race Condition 1: Unsynchronized Map Access**

```go
// WRONG: Data race
type Cache struct {
    data map[string]int
}

func (c *Cache) Get(key string) int {
    return c.data[key] // Race!
}

func (c *Cache) Set(key string, value int) {
    c.data[key] = value // Race!
}

// CORRECT: Synchronized access
type Cache struct {
    data map[string]int
    mu   sync.RWMutex
}

func (c *Cache) Get(key string) int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.data[key]
}

func (c *Cache) Set(key string, value int) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.data[key] = value
}
```

**Race Condition 2: Check-Then-Act**

```go
// WRONG: Race between check and act
if !cache.Has(key) {
    cache.Set(key, value) // Race!
}

// CORRECT: Atomic operation
cache.SetIfAbsent(key, value)
```

**Race Condition 3: Non-Atomic Counter Updates**

```go
// WRONG: Non-atomic increment
counter++ // Race!

// CORRECT: Atomic increment
counter.Add(1)
```

### Race Detection

Enable race detector during testing:

```bash
go test -race ./...
```

Example race detection output:
```
WARNING: DATA RACE
Write at 0x00c000120000 by goroutine 7:
  main.(*Worker).AddTx()
      worker.go:45 +0x123

Previous read at 0x00c000120000 by goroutine 8:
  main.(*Worker).Size()
      worker.go:50 +0x456
```

## Performance Considerations

### Lock Contention

**Problem**: High contention on shared locks

```go
// High contention: Single lock for all workers
type Pool struct {
    allPending []types.Transaction
    mu         sync.Mutex // Bottleneck!
}
```

**Solution**: Partition data to reduce contention

```go
// Low contention: Per-worker locks
type Pool struct {
    workers []*Worker // Each has its own lock
}

type Worker struct {
    pending []types.Transaction
    mu      sync.Mutex // Independent lock
}
```

### Lock-Free Alternatives

**Atomic Operations**:
```go
// Instead of mutex for simple counters
var counter atomic.Uint64
counter.Add(1)
```

**Channel-Based Synchronization**:
```go
// Instead of mutex for state coordination
type Worker struct {
    requests  chan Request
    responses chan Response
}
```

### Goroutine Pool

**Problem**: Creating too many goroutines

```go
// WRONG: Unbounded goroutine creation
for _, tx := range txs {
    go processTx(tx) // Can create millions!
}
```

**Solution**: Fixed-size worker pool

```go
// CORRECT: Fixed pool of workers
pool := NewWorkerPool(8) // Fixed 8 workers
for _, tx := range txs {
    pool.Submit(tx) // Bounded parallelism
}
```

## Debugging Concurrency Issues

### Goroutine Leaks

Detect goroutine leaks:

```go
import "runtime"

func detectLeaks() {
    before := runtime.NumGoroutine()
    // ... run operation ...
    after := runtime.NumGoroutine()

    if after > before+10 {
        panic(fmt.Sprintf("Goroutine leak: %d -> %d", before, after))
    }
}
```

### Deadlock Detection

Go runtime detects deadlocks:

```
fatal error: all goroutines are asleep - deadlock!
```

Common causes:
- Circular lock dependencies
- Waiting on channel that's never written to
- Missing mutex unlock

### Profiling Contention

Profile lock contention:

```bash
go test -bench=. -mutexprofile=mutex.out
go tool pprof mutex.out
```

## Best Practices

1. **Minimize Shared State**: Prefer message passing over shared memory
2. **Use Appropriate Synchronization**: Choose right tool (atomic, mutex, channel)
3. **Avoid Nested Locks**: Prevent deadlocks
4. **Test with Race Detector**: Always run `go test -race`
5. **Profile Contention**: Identify bottlenecks
6. **Document Lock Ordering**: Prevent circular dependencies
7. **Use Timeouts**: Prevent indefinite blocking
8. **Graceful Shutdown**: Clean up goroutines properly

## Thread-Safety Checklist

- [ ] All shared mutable state protected
- [ ] Appropriate synchronization primitive used
- [ ] No data races (verified with -race)
- [ ] No deadlock potential
- [ ] Goroutines cleaned up on shutdown
- [ ] Channel sends never block indefinitely
- [ ] Lock contention profiled and acceptable
- [ ] Critical sections minimized

## Next Steps

- **[Error Handling Reference](ERROR_HANDLING.md)**: Error handling patterns
- **[Security Reference](SECURITY.md)**: Security best practices
- **[Troubleshooting Reference](TROUBLESHOOTING.md)**: Debug concurrency issues
