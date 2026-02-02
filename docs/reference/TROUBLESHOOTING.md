# Troubleshooting Reference

Comprehensive troubleshooting guide for Looseberry.

## Common Issues

### Issue: Looseberry Won't Start

**Symptoms**:
- `Start()` returns error
- Process exits immediately
- "validator set not set" error

**Diagnosis**:
```bash
# Check logs
journalctl -u looseberry -n 100

# Verify configuration
/usr/local/bin/looseberry validate-config /etc/looseberry/config.toml

# Check file permissions
ls -la /var/lib/looseberry/data
```

**Solutions**:

1. **Missing validator set**:
```go
// WRONG: Forgot to set validator set
lb.Start()

// CORRECT: Set validator set first
lb.SetValidatorSet(validatorSet)
lb.Start()
```

2. **Missing network**:
```go
// WRONG: Forgot to set network
lb.Start()

// CORRECT: Set network first
lb.SetNetwork(network)
lb.Start()
```

3. **Invalid configuration**:
```go
cfg := looseberry.DefaultConfig()
if err := cfg.Validate(); err != nil {
    log.Fatalf("Invalid config: %v", err)
}
```

4. **Permission denied**:
```bash
# Fix data directory permissions
sudo chown -R validator:validator /var/lib/looseberry
sudo chmod 700 /var/lib/looseberry/data
```

### Issue: Transactions Rejected with Backpressure

**Symptoms**:
- `AddTx()` returns `ErrWorkerBackpressure`
- High pending transaction count
- Worker load consistently high

**Diagnosis**:
```go
metrics := lb.Metrics()
fmt.Printf("Pending: %d/%d\n", metrics.PendingTxCount, cfg.Worker.MaxPendingTxs)
fmt.Printf("Workers: %d, Load: %.2f\n", metrics.WorkerCount, metrics.WorkerLoad)
```

**Solutions**:

1. **Increase pending limits**:
```go
cfg.Worker.MaxPendingTxs = 50000  // Increase from 10000
cfg.Worker.MaxPendingBytes = 200 * 1024 * 1024  // Increase from 50MB
```

2. **Increase worker count**:
```go
cfg.Worker.MinWorkers = 4  // More parallelism
cfg.Worker.MaxWorkers = 8
```

3. **Retry with backoff**:
```go
func addWithRetry(lb looseberry.DAGMempool, tx []byte) error {
    for i := 0; i < 5; i++ {
        err := lb.AddTx(tx)
        if err == nil {
            return nil
        }
        if !types.IsRetryable(err) {
            return err
        }
        time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
    }
    return fmt.Errorf("max retries exceeded")
}
```

### Issue: Round Not Advancing

**Symptoms**:
- `CurrentRound()` stuck at same value
- No new certificates forming
- Network appears idle

**Diagnosis**:
```bash
# Check metrics
curl http://localhost:9090/metrics | grep looseberry_current_round

# Check all nodes
for node in node0 node1 node2 node3; do
    ssh $node "curl -s http://localhost:9090/metrics | grep current_round"
done
```

**Solutions**:

1. **Network connectivity issue**:
```bash
# Test connectivity between validators
ping <validator-ip>
telnet <validator-ip> 26656

# Check firewall
sudo ufw status
```

2. **Insufficient validators**:
```go
// Need at least n = 3f + 1 validators running
// For f=1, need 4 validators
// Check validator count
metrics := lb.Metrics()
log.Printf("Validators: %d", validatorSet.Count())
```

3. **Certificate formation failure**:
```go
// Check for vote collection issues
// Enable debug logging
log.SetLevel("debug")

// Look for:
// - "Received vote" messages
// - "Certificate formed" messages
// - Byzantine behavior warnings
```

4. **Consensus not calling NotifyCommitted**:
```go
// Verify consensus integration
lb.NotifyCommitted(committedRound)

// Check metrics
fmt.Printf("Uncommitted gap: %d\n", metrics.UncommittedGap)
```

### Issue: High Memory Usage

**Symptoms**:
- Process using excessive RAM
- OOM (Out of Memory) errors
- System swap usage high

**Diagnosis**:
```bash
# Check process memory
ps aux | grep looseberry

# Check Go memory stats
curl http://localhost:6060/debug/pprof/heap > heap.profile
go tool pprof heap.profile
```

**Solutions**:

1. **Reduce pending transaction limits**:
```go
cfg.Worker.MaxPendingTxs = 5000  // Reduce from 10000
cfg.Worker.MaxPendingBytes = 20 * 1024 * 1024  // 20MB
```

2. **Reduce GC depth**:
```go
cfg.GC.GCDepth = 50  // Keep fewer rounds in memory
```

3. **Use persistent storage**:
```go
cfg.Storage.InMemory = false  // Don't keep everything in RAM
cfg.Storage.DataDir = "/var/lib/looseberry/data"
```

4. **Check for goroutine leaks**:
```bash
# Get goroutine profile
curl http://localhost:6060/debug/pprof/goroutine > goroutine.profile
go tool pprof goroutine.profile

# In pprof:
> top 10  # Show top goroutine sources
```

### Issue: Flow Control Paused

**Symptoms**:
- `AddTx()` returns `ErrFlowControlPaused`
- `IsPaused` metric is true
- Large uncommitted gap

**Diagnosis**:
```go
metrics := lb.Metrics()
fmt.Printf("Paused: %v\n", metrics.IsPaused)
fmt.Printf("Uncommitted gap: %d\n", metrics.UncommittedGap)
fmt.Printf("Current: %d, Committed: %d\n",
    metrics.CurrentRound, metrics.CommittedRound)
```

**Solutions**:

1. **Consensus is slow - investigate consensus**:
```go
// Check if consensus is committing
// Ensure NotifyCommitted() is being called
log.Printf("Last committed round: %d", metrics.CommittedRound)
```

2. **Increase flow control limit**:
```go
cfg.FlowControl.MaxUncommittedRounds = 200  // Allow larger gap
```

3. **Wait for consensus to catch up**:
```go
// Retry after delay
time.Sleep(100 * time.Millisecond)
err = lb.AddTx(tx)
```

### Issue: Node Falling Behind (Sync Issues)

**Symptoms**:
- Round significantly lower than other nodes
- "sync required" messages in logs
- High sync activity

**Diagnosis**:
```bash
# Compare rounds across nodes
for node in node0 node1 node2 node3; do
    ssh $node "curl -s http://localhost:9090/metrics | grep current_round"
done
```

**Solutions**:

1. **Network latency**:
```bash
# Check network latency
ping -c 10 <validator-ip>
iperf3 -c <validator-ip>
```

2. **Storage performance**:
```bash
# Test disk I/O
dd if=/dev/zero of=/var/lib/looseberry/test bs=1M count=1000 oflag=direct
```

3. **Adjust sync thresholds**:
```go
cfg.Sync.SyncThreshold = 3  // Trigger sync earlier
cfg.Sync.SyncBatchSize = 200  // Larger sync batches
```

4. **Restart node to force full sync**:
```bash
sudo systemctl restart looseberry
```

### Issue: Disk Space Full

**Symptoms**:
- "no space left on device" errors
- Storage writes failing
- Process crashing

**Diagnosis**:
```bash
# Check disk usage
df -h /var/lib/looseberry

# Check which files are large
du -sh /var/lib/looseberry/*

# Check for log rotation
ls -lh /var/log/looseberry/
```

**Solutions**:

1. **Enable garbage collection**:
```go
cfg.GC.GCDepth = 50  // Keep only 50 rounds
lb.NotifyCommitted(currentRound)  // Trigger GC
```

2. **Manually clean old data**:
```bash
# Stop Looseberry first
sudo systemctl stop looseberry

# Clean old LevelDB data (CAREFUL!)
# Only if you understand the implications
rm -rf /var/lib/looseberry/data/batches/*
rm -rf /var/lib/looseberry/data/certificates/*

# Restart
sudo systemctl start looseberry
```

3. **Add more storage**:
```bash
# Mount additional storage
sudo mount /dev/sdb1 /var/lib/looseberry

# Update configuration
cfg.Storage.DataDir = "/var/lib/looseberry/data"
```

### Issue: Performance Degradation

**Symptoms**:
- Throughput declining over time
- Latency increasing
- CPU or I/O saturation

**Diagnosis**:
```bash
# Profile CPU
curl http://localhost:6060/debug/pprof/profile?seconds=30 > cpu.profile
go tool pprof cpu.profile

# Check system resources
top
iostat -x 1
```

**Solutions**:

1. **Optimize configuration** (see [Performance Tuning](../tutorials/PERFORMANCE_TUNING.md))

2. **Check for resource constraints**:
```bash
# CPU
top

# Memory
free -h

# Disk I/O
iostat -x 1

# Network
iftop
```

3. **Profile and optimize hot paths**:
```bash
# Get CPU profile
curl http://localhost:6060/debug/pprof/profile?seconds=30 > cpu.profile
go tool pprof cpu.profile

# In pprof:
> top 10  # Show top CPU consumers
> list <function>  # Show function source with samples
```

## Debug Mode

Enable debug logging:

```go
import "log"

func init() {
    log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// Or with structured logging
logger, _ := zap.NewDevelopment()
```

Enable verbose metrics:

```go
func monitorDetailed(lb *looseberry.Looseberry) {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        metrics := lb.Metrics()
        log.Printf(`
Looseberry Metrics:
  Pending: %d transactions (%d bytes)
  Round: %d (committed: %d, highest: %d)
  Workers: %d (load: %.2f)
  Batches: %d (certified: %d)
  Certificates: %d
  Flow Control: paused=%v, gap=%d
  Totals: added=%d, rejected=%d, committed=%d
`,
            metrics.PendingTxCount, metrics.PendingTxBytes,
            metrics.CurrentRound, metrics.CommittedRound, metrics.HighestRound,
            metrics.WorkerCount, metrics.WorkerLoad,
            metrics.TotalBatches, metrics.TotalBatchesCertified,
            metrics.TotalCertificates,
            metrics.IsPaused, metrics.UncommittedGap,
            metrics.TotalTxAdded, metrics.TotalTxRejected, metrics.TotalTxCommitted)
    }
}
```

## Diagnostic Tools

### Health Check Script

```bash
#!/bin/bash
# health-check.sh

echo "=== Looseberry Health Check ==="

# Check process
if pgrep looseberry > /dev/null; then
    echo "✓ Process running"
else
    echo "✗ Process not running"
    exit 1
fi

# Check metrics endpoint
if curl -s http://localhost:9090/metrics > /dev/null; then
    echo "✓ Metrics endpoint responding"
else
    echo "✗ Metrics endpoint not responding"
    exit 1
fi

# Check round progression
ROUND=$(curl -s http://localhost:9090/metrics | grep looseberry_current_round | awk '{print $2}')
sleep 10
ROUND2=$(curl -s http://localhost:9090/metrics | grep looseberry_current_round | awk '{print $2}')

if [ "$ROUND2" -gt "$ROUND" ]; then
    echo "✓ Round advancing ($ROUND -> $ROUND2)"
else
    echo "⚠ Round not advancing (stuck at $ROUND)"
fi

# Check pause state
PAUSED=$(curl -s http://localhost:9090/metrics | grep looseberry_is_paused | awk '{print $2}')
if [ "$PAUSED" -eq "0" ]; then
    echo "✓ Not paused"
else
    echo "⚠ Flow control paused"
fi

echo "=== Health Check Complete ==="
```

### Log Analysis

```bash
# Find errors
grep ERROR /var/log/looseberry/looseberry.log

# Find Byzantine behavior
grep Byzantine /var/log/looseberry/looseberry.log

# Find backpressure events
grep backpressure /var/log/looseberry/looseberry.log

# Count error types
grep ERROR /var/log/looseberry/looseberry.log | awk '{print $5}' | sort | uniq -c
```

## Getting Help

When reporting issues, include:

1. **Version information**:
```bash
looseberry version
go version
```

2. **Configuration**:
```bash
cat /etc/looseberry/config.toml
```

3. **Metrics**:
```bash
curl http://localhost:9090/metrics
```

4. **Recent logs**:
```bash
journalctl -u looseberry -n 1000
```

5. **System information**:
```bash
uname -a
free -h
df -h
```

## Next Steps

- **[FAQ](FAQ.md)**: Frequently asked questions
- **[Performance Tuning](../tutorials/PERFORMANCE_TUNING.md)**: Optimize performance
- **[Monitoring Guide](../guides/MONITORING.md)**: Set up monitoring
- **[Deployment Guide](../guides/DEPLOYMENT.md)**: Production deployment tips
