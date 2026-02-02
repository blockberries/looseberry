# Frequently Asked Questions (FAQ)

Common questions and answers about Looseberry.

## General Questions

### What is Looseberry?

Looseberry is a high-performance DAG-based mempool implementation for Byzantine fault-tolerant consensus systems. It separates transaction dissemination from transaction ordering, enabling high throughput while maintaining BFT safety guarantees.

### How does Looseberry differ from traditional mempools?

Traditional mempools couple transaction dissemination with consensus, creating a bottleneck. Looseberry decouples these concerns:
- **Dissemination**: Workers batch transactions, primary nodes create certificates
- **Ordering**: External consensus layer orders certificates deterministically

This separation allows parallel transaction batching independent of consensus speed.

### What is a DAG?

A Directed Acyclic Graph (DAG) is a data structure where certificates form vertices with edges representing causal dependencies. Each certificate references parent certificates from the previous round, creating an immutable history.

### What is a certificate?

A certificate is a header plus 2f+1 votes from validators, proving that:
1. A quorum of validators received the data
2. The data is available for consensus ordering
3. The header is valid and properly signed

## Architecture Questions

### How many validators do I need?

Minimum requirements:
- **n = 3f + 1** validators (where f is max Byzantine validators)
- **4 validators** minimum for production (tolerates f=1 Byzantine)
- **7 validators** recommended (tolerates f=2 Byzantine)

### How many workers should I run?

Recommended configuration:
- **Light load**: 1-2 workers
- **Medium load**: 2-4 workers
- **Heavy load**: 4-8 workers
- **Maximum**: 8 workers (diminishing returns beyond this)

Workers auto-scale based on load between MinWorkers and MaxWorkers.

### What is a quorum?

A quorum is the minimum number of validators needed for certificate formation:
- **Formula**: Quorum = 2f + 1
- **With 4 validators** (f=1): Quorum = 3
- **With 7 validators** (f=2): Quorum = 5

This ensures Byzantine fault tolerance.

### How do rounds work?

Rounds are logical time periods in the DAG:
- **Round 0**: Genesis (no parents)
- **Round N**: References 2f+1 certificates from round N-1
- **Round advancement**: Automatic when sufficient certificates exist

## Configuration Questions

### What batch size should I use?

Depends on your priorities:
- **Low latency**: 100-200 transactions (faster batching)
- **Balanced**: 500 transactions (default)
- **High throughput**: 1000+ transactions (maximize efficiency)

### How do I choose BatchTimeout?

Trade-off between latency and batch fullness:
- **Low latency**: 25-50ms (create batches quickly)
- **Balanced**: 100ms (default)
- **High throughput**: 200-500ms (wait for full batches)

### Should I use in-memory or persistent storage?

- **In-memory**: Testing and development only
- **LevelDB**: Production deployments (data survives restarts)

### What GC depth should I use?

Depends on memory and sync requirements:
- **Low memory**: GCDepth = 20-50
- **Balanced**: GCDepth = 100 (default)
- **High memory/debugging**: GCDepth = 200+

Larger depth means more rounds kept in storage but better sync support.

## Performance Questions

### What throughput can I expect?

Benchmark results (single node, in-memory):
- **Sequential**: 250,000+ tx/s
- **Parallel (12 goroutines)**: 200,000+ tx/s

Production throughput depends on:
- Network latency between validators
- Storage performance
- Configuration (batch size, workers, etc.)

### How can I improve throughput?

1. Increase batch size: `cfg.Worker.BatchSize = 1000`
2. Increase workers: `cfg.Worker.MaxWorkers = 8`
3. Use fast storage: SSD/NVMe
4. Tune network buffers
5. See [Performance Tuning Tutorial](../tutorials/PERFORMANCE_TUNING.md)

### How can I reduce latency?

1. Decrease batch timeout: `cfg.Worker.BatchTimeout = 25ms`
2. Smaller batches: `cfg.Worker.BatchSize = 100`
3. Faster headers: `cfg.Primary.HeaderTimeout = 100ms`
4. Use NVMe storage

### Why am I seeing backpressure errors?

`ErrWorkerBackpressure` means workers are overloaded:
- **Cause**: Too many pending transactions
- **Solution**: Increase `MaxPendingTxs` or add more workers
- **Alternative**: Retry with exponential backoff

## Integration Questions

### How do I integrate with consensus?

Three main integration points:

1. **Transaction submission**:
```go
consensus.ReceiveTx(tx) -> mempool.AddTx(tx)
```

2. **Block building**:
```go
batches := mempool.ReapCertifiedBatches(maxBytes)
// Extract transactions for block
```

3. **Commit notification**:
```go
consensus.Commit(block) -> mempool.NotifyCommitted(round)
```

See [Integration Guide](../guides/INTEGRATION.md) for details.

### How do I integrate with networking?

Implement the `network.Network` interface to wrap your P2P layer (e.g., glueberry). See [Integration Guide](../guides/INTEGRATION.md) for examples.

### Can I use custom storage backends?

Yes! Implement the storage interfaces:
- `BatchStore`
- `CertificateStore`
- `TxIndex`

See [Custom Storage Tutorial](../tutorials/CUSTOM_STORAGE.md).

### How do I handle validator set changes?

Call `UpdateValidatorSet()` during epoch transitions:
```go
newValidators := consensus.GetValidatorsForEpoch(newEpoch)
validatorSet := types.NewSimpleValidatorSet(newValidators, newEpoch)
mempool.UpdateValidatorSet(validatorSet)
```

## Error Handling Questions

### What does "validator set not set" mean?

You forgot to call `SetValidatorSet()` before `Start()`:
```go
lb.SetValidatorSet(validatorSet)  // Required
lb.Start()
```

### What does "network not set" mean?

You forgot to call `SetNetwork()` before `Start()`:
```go
lb.SetNetwork(network)  // Required
lb.Start()
```

### How do I handle retryable errors?

Use the `IsRetryable()` helper:
```go
err := lb.AddTx(tx)
if types.IsRetryable(err) {
    time.Sleep(10 * time.Millisecond)
    err = lb.AddTx(tx)  // Retry
}
```

### What should I do with Byzantine errors?

Byzantine errors indicate malicious behavior:
```go
if types.IsByzantine(err) {
    // Log the evidence
    log.Warn("Byzantine behavior", "error", err)
    // Report to consensus for slashing
    consensus.ReportByzantine(evidence)
}
```

## Operational Questions

### How do I monitor Looseberry?

1. **Metrics**: Expose Prometheus metrics
2. **Logging**: Structured logging with appropriate levels
3. **Alerting**: Set up alerts for key metrics
4. **Dashboards**: Create Grafana dashboards

See [Monitoring Guide](../guides/MONITORING.md).

### How do I back up Looseberry data?

1. Stop Looseberry (or accept inconsistent backup)
2. Backup `/var/lib/looseberry/data`
3. Restart Looseberry

For production, use volume snapshots or automated backup tools.

### How do I upgrade Looseberry?

1. **Patch versions** (v1.0.x): No coordination needed
2. **Minor versions** (v1.x.0): Coordinate upgrade window
3. **Major versions** (vx.0.0): Network-wide coordination required

Use rolling upgrades for multi-node deployments.

### Can I run multiple Looseberry instances per validator?

No. Each validator should run exactly one Looseberry instance. Running multiple instances will cause issues with:
- Duplicate headers
- Signature conflicts
- Byzantine detection

### How do I test my integration?

1. **Unit tests**: Test individual components
2. **Integration tests**: Test with mock network
3. **Multi-node tests**: Test with real network
4. **Stress tests**: High-volume testing
5. **Byzantine tests**: Test fault tolerance

See [Testing Guide](../guides/TESTING.md).

## Security Questions

### How are keys managed?

Generate Ed25519 keys:
```go
signer, err := types.GenerateEd25519Signer(validatorIndex)
```

For production:
- Store keys in Vault or HSM
- Never hardcode keys
- Implement key rotation

See [Security Reference](../reference/SECURITY.md).

### How does Looseberry handle Byzantine validators?

Multiple mechanisms:
1. **Quorum requirement**: Need 2f+1 votes for certificates
2. **Signature verification**: All signatures verified
3. **Double-vote detection**: Detects equivocation
4. **Byzantine reporting**: Reports evidence to consensus

### Is communication encrypted?

Communication is not encrypted by default. Implement TLS in your network layer for production deployments.

### How do I secure my deployment?

1. Use TLS for network communication
2. Store keys securely (Vault/HSM)
3. Run as dedicated user with minimal permissions
4. Configure firewall rules
5. Enable audit logging

See [Security Reference](../reference/SECURITY.md).

## Troubleshooting Questions

### Why isn't my round advancing?

Common causes:
1. **Network issues**: Check connectivity between validators
2. **Insufficient validators**: Need n ≥ 3f + 1 running
3. **Consensus not committing**: Check consensus integration
4. **Byzantine behavior**: Check logs for warnings

See [Troubleshooting Reference](../reference/TROUBLESHOOTING.md).

### Why is my node falling behind?

Common causes:
1. **Network latency**: High latency to other validators
2. **Storage performance**: Slow disk I/O
3. **Resource constraints**: CPU/memory saturation
4. **Configuration**: Sync thresholds too high

### How do I debug performance issues?

1. Enable profiling: `import _ "net/http/pprof"`
2. Collect CPU profile: `curl http://localhost:6060/debug/pprof/profile?seconds=30`
3. Collect memory profile: `curl http://localhost:6060/debug/pprof/heap`
4. Analyze with `go tool pprof`

See [Performance Tuning Tutorial](../tutorials/PERFORMANCE_TUNING.md).

### Where can I find logs?

Default log locations:
- **Stdout**: If running in foreground
- **Systemd**: `journalctl -u looseberry`
- **File**: `/var/log/looseberry/looseberry.log`

## Development Questions

### Can I contribute to Looseberry?

Yes! Contributions are welcome:
1. Fork the repository
2. Create a feature branch
3. Implement your changes with tests
4. Submit a pull request

### How do I run tests?

```bash
# All tests
make test

# With race detection
go test -race ./...

# Specific package
go test ./worker

# Benchmarks
go test -bench=. ./...
```

### How do I build from source?

```bash
git clone https://github.com/blockberries/looseberry
cd looseberry
go build
```

### What are the dependencies?

Minimal dependencies:
- Go 1.21+
- LevelDB (for persistent storage)
- Standard library

## Advanced Questions

### Can I customize the DAG structure?

The DAG structure is fixed (2f+1 parents from previous round), but you can:
- Customize batch contents
- Implement custom storage
- Extend with additional metadata

### How does transaction recovery work?

During GC, uncommitted transactions are:
1. Extracted from pruned batches
2. Re-injected into worker queues
3. Included in new batches

Enable with: `cfg.GC.RecoverTxs = true`

### Can I use Looseberry without consensus?

No. Looseberry requires an external consensus layer to:
- Order certificates deterministically
- Notify of committed rounds
- Provide validator sets

### How does flow control work?

Flow control pauses transaction acceptance when:
- Uncommitted gap exceeds threshold
- Prevents unbounded DAG growth
- Waits for consensus to catch up

Returns `ErrFlowControlPaused` when paused.

## Where to Learn More

- **Getting Started**: [Getting Started Guide](../guides/GETTING_STARTED.md)
- **Integration**: [Integration Guide](../guides/INTEGRATION.md)
- **Configuration**: [Configuration Guide](../guides/CONFIGURATION.md)
- **Testing**: [Testing Guide](../guides/TESTING.md)
- **Deployment**: [Deployment Guide](../guides/DEPLOYMENT.md)
- **Monitoring**: [Monitoring Guide](../guides/MONITORING.md)
- **Tutorials**: [Quickstart](../tutorials/QUICKSTART.md), [Multi-Node](../tutorials/MULTI_NODE.md)
- **Reference**: [Concurrency](CONCURRENCY.md), [Error Handling](ERROR_HANDLING.md), [Security](SECURITY.md)

## Still Have Questions?

- Open an issue on GitHub
- Join the community chat
- Check the documentation
- Email: support@blockberries.com
