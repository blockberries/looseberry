# Performance Tuning Tutorial

Learn how to optimize Looseberry for your specific workload.

## Overview

This tutorial covers:
- Benchmarking your deployment
- Identifying bottlenecks
- Tuning configuration
- Measuring improvements

## Baseline Benchmarking

### 1. Measure Current Performance

Create a benchmark script:

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

func benchmark(cfg *looseberry.Config, txCount int) {
    // Setup
    signer, _ := types.GenerateEd25519Signer(0)
    cfg.Signer = signer
    cfg.ValidatorIndex = 0

    lb, _ := looseberry.New(cfg)
    validators := []*types.Validator{{Index: 0, PublicKey: signer.PublicKey()}}
    lb.SetValidatorSet(types.NewSimpleValidatorSet(validators, 0))
    lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))
    lb.Start()
    defer lb.Stop()

    // Warm up
    for i := 0; i < 100; i++ {
        lb.AddTx([]byte("warmup"))
    }
    time.Sleep(1 * time.Second)

    // Benchmark
    start := time.Now()
    for i := 0; i < txCount; i++ {
        if err := lb.AddTx([]byte(fmt.Sprintf("tx-%d", i))); err != nil {
            if !types.IsRetryable(err) {
                log.Printf("Error: %v", err)
            }
        }
    }
    duration := time.Since(start)

    // Results
    throughput := float64(txCount) / duration.Seconds()
    avgLatency := duration / time.Duration(txCount)

    fmt.Printf("\nBenchmark Results:\n")
    fmt.Printf("  Transactions: %d\n", txCount)
    fmt.Printf("  Duration: %v\n", duration)
    fmt.Printf("  Throughput: %.0f tx/s\n", throughput)
    fmt.Printf("  Avg Latency: %v\n", avgLatency)

    // Metrics
    metrics := lb.Metrics()
    fmt.Printf("\nMempool Metrics:\n")
    fmt.Printf("  Pending: %d\n", metrics.PendingTxCount)
    fmt.Printf("  Workers: %d\n", metrics.WorkerCount)
    fmt.Printf("  Load: %.2f\n", metrics.WorkerLoad)
}

func main() {
    fmt.Println("=== Looseberry Performance Tuning ===")

    // Test default configuration
    fmt.Println("\n1. Default Configuration:")
    cfg := looseberry.DefaultConfig()
    cfg.Storage.InMemory = true
    benchmark(cfg, 10000)
}
```

### 2. Profile Your Application

Enable profiling:

```go
import (
    _ "net/http/pprof"
    "net/http"
)

func main() {
    go func() {
        http.ListenAndServe("localhost:6060", nil)
    }()

    // ... rest of code
}
```

Collect profiles:

```bash
# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Memory profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

## Optimization Strategies

### Strategy 1: Optimize for Throughput

For maximum transactions per second:

```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = true

// Increase workers
cfg.Worker.MinWorkers = 8
cfg.Worker.MaxWorkers = 8

// Larger batches
cfg.Worker.BatchSize = 1000
cfg.Worker.MaxBatchBytes = 1024 * 1024 // 1MB

// More buffer capacity
cfg.Worker.MaxPendingTxs = 100000
cfg.Worker.MaxPendingBytes = 500 * 1024 * 1024 // 500MB

// More batches per header
cfg.Primary.MaxBatchesPerHeader = 200
cfg.Primary.HeaderTimeout = 1 * time.Second

// Benchmark
benchmark(cfg, 100000)
```

Expected improvement: **2-3x throughput**

### Strategy 2: Optimize for Latency

For minimum transaction latency:

```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = true

// Fast batching
cfg.Worker.BatchSize = 100
cfg.Worker.BatchTimeout = 25 * time.Millisecond

// Fast header creation
cfg.Primary.HeaderTimeout = 100 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 50

// Multiple workers for parallelism
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 4

// Benchmark
benchmark(cfg, 10000)
```

Expected improvement: **50% lower latency**

### Strategy 3: Optimize for Memory

For constrained memory environments:

```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = true

// Fewer workers
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2

// Smaller buffers
cfg.Worker.MaxPendingTxs = 1000
cfg.Worker.MaxPendingBytes = 10 * 1024 * 1024 // 10MB

// Aggressive GC
cfg.GC.GCDepth = 20

// Benchmark
benchmark(cfg, 10000)
```

Expected: **80% lower memory usage**

### Strategy 4: Balance Trade-offs

For production balance:

```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = false
cfg.Storage.DataDir = "/mnt/ssd/looseberry/data"

// Moderate workers
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8

// Balanced batching
cfg.Worker.BatchSize = 500
cfg.Worker.BatchTimeout = 100 * time.Millisecond

// Moderate buffers
cfg.Worker.MaxPendingTxs = 50000
cfg.Worker.MaxPendingBytes = 100 * 1024 * 1024 // 100MB

// Moderate GC
cfg.GC.GCDepth = 100

// Benchmark
benchmark(cfg, 50000)
```

## Identifying Bottlenecks

### CPU Bottleneck

**Symptoms**:
- High CPU usage
- Worker load consistently high
- Throughput plateaus

**Solutions**:
1. Increase worker count
2. Reduce batch size (less hashing)
3. Optimize transaction validation

```go
// Before
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 4

// After
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 8
```

### Memory Bottleneck

**Symptoms**:
- High memory usage
- Frequent backpressure errors
- GC pauses

**Solutions**:
1. Reduce pending transaction limits
2. Reduce GC depth
3. Smaller batches

```go
// Before
cfg.Worker.MaxPendingTxs = 100000
cfg.GC.GCDepth = 200

// After
cfg.Worker.MaxPendingTxs = 10000
cfg.GC.GCDepth = 50
```

### Disk I/O Bottleneck

**Symptoms**:
- High disk I/O wait
- Slow certificate formation
- Storage latency

**Solutions**:
1. Use SSD/NVMe storage
2. Increase GC depth (fewer GC runs)
3. Batch writes

```go
// Use fast storage
cfg.Storage.DataDir = "/mnt/nvme/looseberry/data"

// Less frequent GC
cfg.GC.GCDepth = 200
```

### Network Bottleneck

**Symptoms**:
- High network latency
- Slow certificate propagation
- Sync issues

**Solutions**:
1. Larger batches (fewer messages)
2. Compress messages
3. Optimize network layer

```go
// Larger batches
cfg.Worker.BatchSize = 1000
cfg.Primary.MaxBatchesPerHeader = 200
```

## Measuring Improvements

### Before and After Comparison

```go
func compareConfigs() {
    configs := map[string]*looseberry.Config{
        "Default": looseberry.DefaultConfig(),
        "High Throughput": createHighThroughputConfig(),
        "Low Latency": createLowLatencyConfig(),
    }

    for name, cfg := range configs {
        fmt.Printf("\n%s Configuration:\n", name)
        cfg.Storage.InMemory = true
        benchmark(cfg, 10000)
    }
}
```

### Continuous Monitoring

```go
func monitor(lb *looseberry.Looseberry, duration time.Duration) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    done := time.After(duration)
    var lastTxAdded uint64

    for {
        select {
        case <-done:
            return
        case <-ticker.C:
            metrics := lb.Metrics()
            throughput := float64(metrics.TotalTxAdded-lastTxAdded) / 5.0
            lastTxAdded = metrics.TotalTxAdded

            fmt.Printf("Round: %d, Pending: %d, Throughput: %.0f tx/s, Workers: %d, Load: %.2f\n",
                metrics.CurrentRound,
                metrics.PendingTxCount,
                throughput,
                metrics.WorkerCount,
                metrics.WorkerLoad)
        }
    }
}
```

## Production Tuning Checklist

- [ ] Baseline benchmark completed
- [ ] Bottlenecks identified
- [ ] Configuration tuned for workload
- [ ] Performance improvements measured
- [ ] Memory usage acceptable
- [ ] Storage performance adequate
- [ ] Network latency acceptable
- [ ] Monitoring configured
- [ ] Load testing performed
- [ ] Production rollout plan ready

## Configuration Templates

### High-Traffic Public Chain

```go
cfg.Worker.MinWorkers = 8
cfg.Worker.MaxWorkers = 8
cfg.Worker.BatchSize = 1000
cfg.Worker.MaxPendingTxs = 100000
cfg.Worker.MaxPendingBytes = 500 * 1024 * 1024
cfg.Primary.HeaderTimeout = 1 * time.Second
cfg.Primary.MaxBatchesPerHeader = 200
cfg.Storage.DataDir = "/mnt/nvme/looseberry/data"
cfg.GC.GCDepth = 100
```

### Low-Latency Payments

```go
cfg.Worker.MinWorkers = 4
cfg.Worker.MaxWorkers = 4
cfg.Worker.BatchSize = 100
cfg.Worker.BatchTimeout = 25 * time.Millisecond
cfg.Primary.HeaderTimeout = 100 * time.Millisecond
cfg.Primary.MaxBatchesPerHeader = 50
cfg.Storage.DataDir = "/mnt/nvme/looseberry/data"
```

### Resource-Constrained Testnet

```go
cfg.Worker.MinWorkers = 1
cfg.Worker.MaxWorkers = 2
cfg.Worker.BatchSize = 200
cfg.Worker.MaxPendingTxs = 5000
cfg.Worker.MaxPendingBytes = 10 * 1024 * 1024
cfg.GC.GCDepth = 20
```

## Next Steps

- **[Deployment Guide](../guides/DEPLOYMENT.md)**: Deploy optimized configuration
- **[Monitoring Guide](../guides/MONITORING.md)**: Monitor performance
- **[Testing Guide](../guides/TESTING.md)**: Benchmark and stress test
