# Monitoring Guide

Comprehensive guide to monitoring and observability for Looseberry.

## Table of Contents

- [Overview](#overview)
- [Metrics Collection](#metrics-collection)
- [Key Metrics](#key-metrics)
- [Alerting](#alerting)
- [Logging](#logging)
- [Dashboards](#dashboards)
- [Performance Monitoring](#performance-monitoring)
- [Troubleshooting with Metrics](#troubleshooting-with-metrics)

## Overview

Effective monitoring is crucial for production deployments. Looseberry provides comprehensive metrics for:

- **Performance**: Throughput, latency, batch sizes
- **Health**: Round progression, certificate formation, sync status
- **Resources**: Memory usage, worker count, pending transactions
- **Flow Control**: Pause state, uncommitted gaps

## Metrics Collection

### Built-in Metrics

Looseberry provides metrics through the `Metrics()` method:

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
    WorkerLoad  float64

    // Flow control metrics
    IsPaused       bool
    PauseCount     uint64
    ResumeCount    uint64
    UncommittedGap uint64

    // Certificate metrics
    TotalCertificates uint64
}
```

### Prometheus Integration

Export metrics to Prometheus:

```go
package monitoring

import (
    "github.com/blockberries/looseberry"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusExporter struct {
    mempool looseberry.DAGMempool

    // Transaction metrics
    pendingTxCount   prometheus.Gauge
    pendingTxBytes   prometheus.Gauge
    totalTxAdded     prometheus.Counter
    totalTxRejected  prometheus.Counter
    totalTxCommitted prometheus.Counter

    // Round metrics
    currentRound   prometheus.Gauge
    committedRound prometheus.Gauge
    highestRound   prometheus.Gauge

    // Worker metrics
    workerCount prometheus.Gauge
    workerLoad  prometheus.Gauge

    // Flow control
    isPaused       prometheus.Gauge
    uncommittedGap prometheus.Gauge
}

func NewPrometheusExporter(mempool looseberry.DAGMempool) *PrometheusExporter {
    return &PrometheusExporter{
        mempool: mempool,
        pendingTxCount: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_pending_tx_count",
            Help: "Number of pending transactions",
        }),
        pendingTxBytes: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_pending_tx_bytes",
            Help: "Total bytes of pending transactions",
        }),
        totalTxAdded: promauto.NewCounter(prometheus.CounterOpts{
            Name: "looseberry_total_tx_added",
            Help: "Total transactions added",
        }),
        totalTxRejected: promauto.NewCounter(prometheus.CounterOpts{
            Name: "looseberry_total_tx_rejected",
            Help: "Total transactions rejected",
        }),
        totalTxCommitted: promauto.NewCounter(prometheus.CounterOpts{
            Name: "looseberry_total_tx_committed",
            Help: "Total transactions committed",
        }),
        currentRound: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_current_round",
            Help: "Current DAG round",
        }),
        committedRound: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_committed_round",
            Help: "Last committed round",
        }),
        highestRound: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_highest_round",
            Help: "Highest known round",
        }),
        workerCount: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_worker_count",
            Help: "Number of active workers",
        }),
        workerLoad: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_worker_load",
            Help: "Worker load ratio (0.0-1.0)",
        }),
        isPaused: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_is_paused",
            Help: "Flow control pause state (1=paused, 0=running)",
        }),
        uncommittedGap: promauto.NewGauge(prometheus.GaugeOpts{
            Name: "looseberry_uncommitted_gap",
            Help: "Gap between current and committed round",
        }),
    }
}

func (e *PrometheusExporter) Update() {
    metrics := e.mempool.Metrics()

    e.pendingTxCount.Set(float64(metrics.PendingTxCount))
    e.pendingTxBytes.Set(float64(metrics.PendingTxBytes))
    e.totalTxAdded.Add(float64(metrics.TotalTxAdded))
    e.totalTxRejected.Add(float64(metrics.TotalTxRejected))
    e.totalTxCommitted.Add(float64(metrics.TotalTxCommitted))

    e.currentRound.Set(float64(metrics.CurrentRound))
    e.committedRound.Set(float64(metrics.CommittedRound))
    e.highestRound.Set(float64(metrics.HighestRound))

    e.workerCount.Set(float64(metrics.WorkerCount))
    e.workerLoad.Set(metrics.WorkerLoad)

    if metrics.IsPaused {
        e.isPaused.Set(1)
    } else {
        e.isPaused.Set(0)
    }
    e.uncommittedGap.Set(float64(metrics.UncommittedGap))
}
```

### Periodic Update

Update metrics periodically:

```go
func startMetricsExporter(mempool looseberry.DAGMempool) {
    exporter := NewPrometheusExporter(mempool)

    ticker := time.NewTicker(10 * time.Second)
    go func() {
        for range ticker.C {
            exporter.Update()
        }
    }()
}
```

### HTTP Endpoint

Expose metrics via HTTP:

```go
import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func startMetricsServer() {
    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(":9090", nil)
}
```

## Key Metrics

### Transaction Metrics

**looseberry_pending_tx_count**
- Description: Number of pending transactions
- Type: Gauge
- Alert: > 100,000 (backpressure building up)

**looseberry_pending_tx_bytes**
- Description: Total bytes of pending transactions
- Type: Gauge
- Alert: > 500MB (memory pressure)

**looseberry_total_tx_added**
- Description: Total transactions added (rate = throughput)
- Type: Counter
- Alert: Rate drops below expected

**looseberry_total_tx_rejected**
- Description: Total transactions rejected
- Type: Counter
- Alert: Rejection rate > 5%

### Round Metrics

**looseberry_current_round**
- Description: Current DAG round
- Type: Gauge
- Alert: Not advancing for > 30 seconds

**looseberry_committed_round**
- Description: Last committed round
- Type: Gauge
- Alert: Not advancing for > 60 seconds

**looseberry_uncommitted_gap**
- Description: Rounds between current and committed
- Type: Gauge
- Alert: > 50 rounds (consensus falling behind)

### Worker Metrics

**looseberry_worker_count**
- Description: Number of active workers
- Type: Gauge
- Normal: 1-8 workers

**looseberry_worker_load**
- Description: Worker load ratio (0.0-1.0)
- Type: Gauge
- Alert: > 0.9 sustained (workers overloaded)

### Flow Control Metrics

**looseberry_is_paused**
- Description: Flow control pause state (1=paused, 0=running)
- Type: Gauge
- Alert: Paused for > 60 seconds

## Alerting

### Prometheus Alert Rules

```yaml
# prometheus-alerts.yml
groups:
  - name: looseberry
    interval: 10s
    rules:
      # High pending transactions
      - alert: LooseberryHighPendingTx
        expr: looseberry_pending_tx_count > 100000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High pending transaction count"
          description: "{{ $labels.instance }} has {{ $value }} pending transactions"

      # High memory usage
      - alert: LooseberryHighMemory
        expr: looseberry_pending_tx_bytes > 500000000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
          description: "{{ $labels.instance }} has {{ $value }} bytes pending"

      # Round not advancing
      - alert: LooseberryRoundStalled
        expr: rate(looseberry_current_round[5m]) == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Round progression stalled"
          description: "{{ $labels.instance }} current round is not advancing"

      # Commit stalled
      - alert: LooseberryCommitStalled
        expr: rate(looseberry_committed_round[5m]) == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Commit progression stalled"
          description: "{{ $labels.instance }} committed round is not advancing"

      # High uncommitted gap
      - alert: LooseberryHighUncommittedGap
        expr: looseberry_uncommitted_gap > 50
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High uncommitted gap"
          description: "{{ $labels.instance }} has {{ $value }} uncommitted rounds"

      # Flow control paused
      - alert: LooseberryFlowControlPaused
        expr: looseberry_is_paused == 1
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Flow control paused"
          description: "{{ $labels.instance }} is paused"

      # High rejection rate
      - alert: LooseberryHighRejectionRate
        expr: rate(looseberry_total_tx_rejected[5m]) / rate(looseberry_total_tx_added[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High transaction rejection rate"
          description: "{{ $labels.instance }} rejection rate is {{ $value }}"

      # Worker overload
      - alert: LooseberryWorkerOverload
        expr: looseberry_worker_load > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Workers overloaded"
          description: "{{ $labels.instance }} worker load is {{ $value }}"
```

### Alert Routing

Configure Alertmanager:

```yaml
# alertmanager.yml
route:
  group_by: ['alertname', 'instance']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'team-ops'

  routes:
    - match:
        severity: critical
      receiver: 'team-ops-critical'

receivers:
  - name: 'team-ops'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/XXX'
        channel: '#looseberry-alerts'

  - name: 'team-ops-critical'
    pagerduty_configs:
      - service_key: 'XXX'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/XXX'
        channel: '#looseberry-critical'
```

## Logging

### Structured Logging

Use structured logging for better searchability:

```go
import (
    "go.uber.org/zap"
)

func setupLogging() (*zap.Logger, error) {
    config := zap.NewProductionConfig()
    config.OutputPaths = []string{
        "stdout",
        "/var/log/looseberry/looseberry.log",
    }

    logger, err := config.Build()
    if err != nil {
        return nil, err
    }

    return logger, nil
}

func logMetrics(logger *zap.Logger, metrics *looseberry.Metrics) {
    logger.Info("mempool_metrics",
        zap.Int("pending_tx_count", metrics.PendingTxCount),
        zap.Int64("pending_tx_bytes", metrics.PendingTxBytes),
        zap.Uint64("current_round", metrics.CurrentRound),
        zap.Uint64("committed_round", metrics.CommittedRound),
        zap.Int("worker_count", metrics.WorkerCount),
        zap.Float64("worker_load", metrics.WorkerLoad),
        zap.Bool("is_paused", metrics.IsPaused),
    )
}
```

### Log Rotation

Configure log rotation:

```bash
# /etc/logrotate.d/looseberry
/var/log/looseberry/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 validator validator
    sharedscripts
    postrotate
        systemctl reload looseberry
    endscript
}
```

### Log Levels

Control log verbosity:

```go
const (
    LogLevelDebug = "debug"
    LogLevelInfo  = "info"
    LogLevelWarn  = "warn"
    LogLevelError = "error"
)

func getLogLevel() string {
    return os.Getenv("LOG_LEVEL")
}
```

## Dashboards

### Grafana Dashboard

Example Grafana dashboard JSON:

```json
{
  "dashboard": {
    "title": "Looseberry Monitoring",
    "panels": [
      {
        "title": "Transaction Rate",
        "targets": [
          {
            "expr": "rate(looseberry_total_tx_added[1m])"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Pending Transactions",
        "targets": [
          {
            "expr": "looseberry_pending_tx_count"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Round Progression",
        "targets": [
          {
            "expr": "looseberry_current_round",
            "legendFormat": "Current"
          },
          {
            "expr": "looseberry_committed_round",
            "legendFormat": "Committed"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Worker Count",
        "targets": [
          {
            "expr": "looseberry_worker_count"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Worker Load",
        "targets": [
          {
            "expr": "looseberry_worker_load"
          }
        ],
        "type": "gauge",
        "thresholds": [
          {"value": 0.8, "color": "yellow"},
          {"value": 0.9, "color": "red"}
        ]
      }
    ]
  }
}
```

### Key Dashboard Panels

**Transaction Throughput**:
```promql
rate(looseberry_total_tx_added[1m])
```

**Pending Transaction Trend**:
```promql
looseberry_pending_tx_count
```

**Round Gap**:
```promql
looseberry_current_round - looseberry_committed_round
```

**Rejection Rate**:
```promql
rate(looseberry_total_tx_rejected[5m]) / rate(looseberry_total_tx_added[5m])
```

## Performance Monitoring

### Throughput Analysis

Calculate throughput:

```go
func monitorThroughput(mempool looseberry.DAGMempool) {
    ticker := time.NewTicker(10 * time.Second)
    var lastTotal uint64

    for range ticker.C {
        metrics := mempool.Metrics()
        current := metrics.TotalTxAdded

        throughput := float64(current-lastTotal) / 10.0
        log.Printf("Throughput: %.0f tx/s", throughput)

        lastTotal = current
    }
}
```

### Latency Tracking

Track transaction latency:

```go
func trackLatency(mempool looseberry.DAGMempool) {
    start := time.Now()
    tx := []byte("test-tx")

    if err := mempool.AddTx(tx); err != nil {
        log.Printf("AddTx error: %v", err)
        return
    }

    latency := time.Since(start)
    log.Printf("AddTx latency: %v", latency)
}
```

### Resource Monitoring

Monitor system resources:

```go
import "runtime"

func logSystemMetrics(logger *zap.Logger) {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)

    logger.Info("system_metrics",
        zap.Uint64("alloc_mb", m.Alloc/1024/1024),
        zap.Uint64("total_alloc_mb", m.TotalAlloc/1024/1024),
        zap.Uint64("sys_mb", m.Sys/1024/1024),
        zap.Uint32("num_gc", m.NumGC),
        zap.Int("num_goroutine", runtime.NumGoroutine()),
    )
}
```

## Troubleshooting with Metrics

### High Pending Transactions

**Symptom**: `looseberry_pending_tx_count` is high

**Investigation**:
```promql
# Check if workers are overloaded
looseberry_worker_load

# Check if flow control is paused
looseberry_is_paused

# Check batch creation rate
rate(looseberry_total_batches[5m])
```

**Solutions**:
- Increase worker count
- Increase batch size
- Check if consensus is slow (uncommitted gap)

### Round Not Advancing

**Symptom**: `looseberry_current_round` is not increasing

**Investigation**:
```promql
# Check if certificates are forming
rate(looseberry_total_certificates[5m])

# Check if batches are being created
rate(looseberry_total_batches[5m])

# Check network connectivity (application level)
```

**Solutions**:
- Check network connectivity between validators
- Verify validator set is correct
- Check for Byzantine behavior

### High Rejection Rate

**Symptom**: `looseberry_total_tx_rejected` rate is high

**Investigation**:
```go
metrics := mempool.Metrics()
rejectionRate := float64(metrics.TotalTxRejected) / float64(metrics.TotalTxAdded)
log.Printf("Rejection rate: %.2f%%", rejectionRate*100)
```

**Solutions**:
- Review transaction validation logic
- Check for duplicate transactions
- Investigate client behavior

### Flow Control Paused

**Symptom**: `looseberry_is_paused` is 1

**Investigation**:
```promql
# Check uncommitted gap
looseberry_uncommitted_gap

# Check consensus progress
rate(looseberry_committed_round[5m])
```

**Solutions**:
- Investigate consensus performance
- Check if consensus is receiving certificates
- Verify consensus is calling `NotifyCommitted()`

## Next Steps

- **[Troubleshooting Reference](../reference/TROUBLESHOOTING.md)**: Detailed troubleshooting guide
- **[Performance Tuning](../tutorials/PERFORMANCE_TUNING.md)**: Optimize performance
- **[Deployment Guide](DEPLOYMENT.md)**: Production deployment
