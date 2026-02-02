# Deployment Guide

Production deployment guide for Looseberry.

## Table of Contents

- [Prerequisites](#prerequisites)
- [System Requirements](#system-requirements)
- [Pre-Deployment Checklist](#pre-deployment-checklist)
- [Deployment Architecture](#deployment-architecture)
- [Configuration for Production](#configuration-for-production)
- [Storage Setup](#storage-setup)
- [Network Setup](#network-setup)
- [Security Hardening](#security-hardening)
- [High Availability](#high-availability)
- [Backup and Recovery](#backup-and-recovery)
- [Upgrade Procedures](#upgrade-procedures)
- [Troubleshooting](#troubleshooting)

## Prerequisites

Before deploying Looseberry to production:

1. **Go Environment**: Go 1.21 or later
2. **Storage**: Fast persistent storage (SSD/NVMe recommended)
3. **Network**: Reliable network connectivity between validators
4. **Monitoring**: Metrics collection and alerting infrastructure
5. **Backups**: Automated backup system

## System Requirements

### Minimum Requirements

For a production validator node:

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16-32 GB |
| **Storage** | 100 GB SSD | 500 GB NVMe |
| **Network** | 100 Mbps | 1 Gbps |
| **OS** | Linux (Ubuntu 20.04+) | Linux (Ubuntu 22.04+) |

### Resource Scaling

Scale resources based on expected load:

**Low traffic** (< 1,000 tx/s):
- 4 cores, 8 GB RAM, 100 GB SSD

**Medium traffic** (1,000-10,000 tx/s):
- 8 cores, 16 GB RAM, 250 GB NVMe

**High traffic** (> 10,000 tx/s):
- 16+ cores, 32+ GB RAM, 500 GB+ NVMe

## Pre-Deployment Checklist

Before deploying to production:

- [ ] System requirements met
- [ ] Storage configured and tested
- [ ] Network connectivity verified between validators
- [ ] Validator keys generated and secured
- [ ] Configuration reviewed and tested
- [ ] Monitoring and alerting configured
- [ ] Backup procedures established
- [ ] Disaster recovery plan documented
- [ ] Security audit completed
- [ ] Load testing performed
- [ ] Upgrade procedures documented

## Deployment Architecture

### Single Validator Node

```
┌─────────────────────────────────────────────────┐
│            Production Server                    │
│                                                 │
│  ┌───────────────────────────────────────────┐ │
│  │          Consensus Layer                  │ │
│  │         (e.g., blockberry)                │ │
│  └──────────────┬────────────────────────────┘ │
│                 │                               │
│  ┌──────────────▼────────────────────────────┐ │
│  │          Looseberry                       │ │
│  │                                           │ │
│  │  Storage: /var/lib/looseberry/data       │ │
│  └──────────────┬────────────────────────────┘ │
│                 │                               │
│  ┌──────────────▼────────────────────────────┐ │
│  │        Network Layer                      │ │
│  │       (e.g., glueberry)                   │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Multi-Region Deployment

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Region A   │     │   Region B   │     │   Region C   │
│              │     │              │     │              │
│  Validator 0 │◄───►│  Validator 1 │◄───►│  Validator 2 │
│              │     │              │     │              │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                     Network Mesh
```

## Configuration for Production

### Basic Production Configuration

```go
package main

import (
    "log"
    "os"

    "github.com/blockberries/looseberry"
    "github.com/blockberries/looseberry/types"
)

func main() {
    // Load validator key from secure storage
    signer, err := loadValidatorKey()
    if err != nil {
        log.Fatalf("Failed to load validator key: %v", err)
    }

    // Production configuration
    cfg := looseberry.DefaultConfig()
    cfg.ValidatorIndex = getValidatorIndex()
    cfg.Signer = signer

    // Worker configuration
    cfg.Worker.MinWorkers = 4
    cfg.Worker.MaxWorkers = 8
    cfg.Worker.BatchSize = 500
    cfg.Worker.BatchTimeout = 100 * time.Millisecond
    cfg.Worker.MaxPendingTxs = 50000
    cfg.Worker.MaxPendingBytes = 200 * 1024 * 1024 // 200MB

    // Primary configuration
    cfg.Primary.HeaderTimeout = 500 * time.Millisecond
    cfg.Primary.MaxBatchesPerHeader = 100
    cfg.Primary.AllowEmptyHeaders = true

    // Storage configuration
    cfg.Storage.InMemory = false
    cfg.Storage.DataDir = "/var/lib/looseberry/data"

    // GC configuration
    cfg.GC.GCDepth = 100
    cfg.GC.RecoverTxs = true

    // Flow control
    cfg.FlowControl.MaxUncommittedRounds = 100

    // Transaction validation
    cfg.TxValidator = validateTransaction

    // Create and start Looseberry
    lb, err := looseberry.New(cfg)
    if err != nil {
        log.Fatalf("Failed to create Looseberry: %v", err)
    }

    // Set validator set (load from consensus)
    validators := loadValidatorSet()
    lb.SetValidatorSet(validators)

    // Set network
    network := createProductionNetwork()
    lb.SetNetwork(network)

    // Start
    if err := lb.Start(); err != nil {
        log.Fatalf("Failed to start Looseberry: %v", err)
    }

    log.Println("Looseberry started successfully")

    // Wait for shutdown signal
    waitForShutdown()

    // Graceful shutdown
    if err := lb.Stop(); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }
}
```

### Environment-Based Configuration

```go
func loadConfig() *looseberry.Config {
    cfg := looseberry.DefaultConfig()

    // Load from environment variables
    cfg.ValidatorIndex = uint16(mustGetEnvInt("VALIDATOR_INDEX"))
    cfg.Storage.DataDir = getEnv("DATA_DIR", "/var/lib/looseberry/data")

    cfg.Worker.MinWorkers = getEnvInt("MIN_WORKERS", 4)
    cfg.Worker.MaxWorkers = getEnvInt("MAX_WORKERS", 8)
    cfg.Worker.BatchSize = getEnvInt("BATCH_SIZE", 500)
    cfg.Worker.MaxPendingTxs = getEnvInt("MAX_PENDING_TXS", 50000)

    return cfg
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        if i, err := strconv.Atoi(value); err == nil {
            return i
        }
    }
    return defaultValue
}
```

## Storage Setup

### LevelDB Configuration

Create data directory with appropriate permissions:

```bash
sudo mkdir -p /var/lib/looseberry/data
sudo chown validator:validator /var/lib/looseberry/data
sudo chmod 700 /var/lib/looseberry/data
```

### Storage Location

Use fast storage:

```bash
# Check disk performance
sudo hdparm -Tt /dev/nvme0n1

# Mount with optimal settings for SSD/NVMe
sudo mount -o noatime,nodiratime /dev/nvme0n1p1 /var/lib/looseberry
```

### Disk Space Monitoring

Monitor disk usage:

```bash
# Check current usage
du -sh /var/lib/looseberry/data

# Set up monitoring
df -h /var/lib/looseberry | tail -1 | awk '{print $5}' | sed 's/%//'
```

Alert when disk usage exceeds 80%.

### Storage Maintenance

Regular maintenance:

```bash
# Compact LevelDB (if needed)
# Stop Looseberry first, then use LevelDB tools

# Check storage health
sudo smartctl -a /dev/nvme0n1
```

## Network Setup

### Firewall Configuration

Configure firewall to allow validator communication:

```bash
# Allow incoming connections from other validators
sudo ufw allow from <validator-ip> to any port 26656 proto tcp

# Deny all other incoming connections
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw enable
```

### Network Optimization

Optimize network settings:

```bash
# Increase TCP buffer sizes
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"

# Make permanent
echo "net.core.rmem_max=134217728" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_max=134217728" | sudo tee -a /etc/sysctl.conf
```

### Connection Pooling

For network layer (glueberry integration):

```go
networkCfg := network.Config{
    BufferSize:    1000,
    SyncBatchSize: 100,
    SyncTimeout:   30000, // 30s
}
```

## Security Hardening

### Key Management

Store validator keys securely:

```go
import (
    "github.com/blockberries/looseberry/types"
    "github.com/hashicorp/vault/api"
)

func loadValidatorKey() (types.Signer, error) {
    // Load from Vault or HSM
    client, err := api.NewClient(api.DefaultConfig())
    if err != nil {
        return nil, err
    }

    secret, err := client.Logical().Read("secret/data/validator-key")
    if err != nil {
        return nil, err
    }

    privateKey := secret.Data["private_key"].(string)
    return types.ImportEd25519Signer(privateKey)
}
```

### File Permissions

Secure data directory:

```bash
# Lock down data directory
sudo chmod 700 /var/lib/looseberry
sudo chmod 700 /var/lib/looseberry/data

# Lock down configuration
sudo chmod 600 /etc/looseberry/config.toml
```

### Process Isolation

Run as dedicated user:

```bash
# Create validator user
sudo useradd -r -s /bin/false validator

# Run as validator user
sudo -u validator /usr/local/bin/looseberry
```

### Network Encryption

Use TLS for validator communication:

```go
// In network layer implementation
tlsConfig := &tls.Config{
    MinVersion:   tls.VersionTLS13,
    Certificates: []tls.Certificate{cert},
    ClientAuth:   tls.RequireAndVerifyClientCert,
    ClientCAs:    caCertPool,
}
```

## High Availability

### Health Checks

Implement health check endpoint:

```go
func healthCheck(lb *looseberry.Looseberry) bool {
    metrics := lb.Metrics()

    // Check if running
    if metrics.CurrentRound == 0 {
        return false
    }

    // Check if paused
    if metrics.IsPaused {
        return false
    }

    // Check if behind
    if metrics.UncommittedGap > 50 {
        return false
    }

    return true
}
```

### Graceful Shutdown

Handle shutdown signals:

```go
func waitForShutdown() {
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

    <-sigCh
    log.Println("Shutdown signal received")
}

func gracefulShutdown(lb *looseberry.Looseberry) {
    log.Println("Starting graceful shutdown...")

    // Stop accepting new transactions
    // (implement at application level)

    // Wait for pending operations
    time.Sleep(2 * time.Second)

    // Stop Looseberry
    if err := lb.Stop(); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }

    log.Println("Shutdown complete")
}
```

### Restart Procedures

Safe restart:

```bash
#!/bin/bash
# restart-looseberry.sh

echo "Stopping Looseberry..."
systemctl stop looseberry

echo "Waiting for cleanup..."
sleep 5

echo "Starting Looseberry..."
systemctl start looseberry

echo "Checking status..."
systemctl status looseberry
```

## Backup and Recovery

### Backup Strategy

Automated backup script:

```bash
#!/bin/bash
# backup-looseberry.sh

BACKUP_DIR="/backup/looseberry"
DATA_DIR="/var/lib/looseberry/data"
DATE=$(date +%Y%m%d-%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Stop Looseberry (optional, for consistent backup)
systemctl stop looseberry

# Backup data
tar -czf "$BACKUP_DIR/looseberry-$DATE.tar.gz" "$DATA_DIR"

# Start Looseberry
systemctl start looseberry

# Keep only last 7 days
find "$BACKUP_DIR" -name "looseberry-*.tar.gz" -mtime +7 -delete

echo "Backup completed: looseberry-$DATE.tar.gz"
```

### Recovery Procedure

Restore from backup:

```bash
#!/bin/bash
# restore-looseberry.sh

BACKUP_FILE=$1

if [ -z "$BACKUP_FILE" ]; then
    echo "Usage: $0 <backup-file>"
    exit 1
fi

# Stop Looseberry
systemctl stop looseberry

# Backup current data
mv /var/lib/looseberry/data /var/lib/looseberry/data.old

# Restore from backup
tar -xzf "$BACKUP_FILE" -C /var/lib/looseberry

# Start Looseberry
systemctl start looseberry

echo "Recovery completed"
```

### Disaster Recovery

Document disaster recovery procedures:

1. **Data Loss**: Restore from latest backup
2. **Corruption**: Restore from backup, resync from network
3. **Key Compromise**: Generate new keys, update validator set
4. **Network Partition**: Wait for partition to heal, resync

## Upgrade Procedures

### Rolling Upgrade

For multi-validator networks:

```bash
# Upgrade one validator at a time
for validator in validator0 validator1 validator2 validator3; do
    echo "Upgrading $validator..."

    # SSH to validator
    ssh $validator "systemctl stop looseberry"

    # Deploy new binary
    scp looseberry $validator:/usr/local/bin/

    # Start new version
    ssh $validator "systemctl start looseberry"

    # Wait for validator to catch up
    sleep 60

    # Verify health
    ssh $validator "systemctl status looseberry"
done
```

### Version Compatibility

Maintain version compatibility:

- **Patch versions** (v1.0.x): No coordination needed
- **Minor versions** (v1.x.0): Coordinate upgrade window
- **Major versions** (vx.0.0): Requires network-wide coordination

### Rollback Procedure

If upgrade fails:

```bash
#!/bin/bash
# rollback-looseberry.sh

echo "Rolling back Looseberry..."

# Stop current version
systemctl stop looseberry

# Restore old binary
cp /usr/local/bin/looseberry.backup /usr/local/bin/looseberry

# Restore old data (if needed)
if [ -d /var/lib/looseberry/data.backup ]; then
    rm -rf /var/lib/looseberry/data
    mv /var/lib/looseberry/data.backup /var/lib/looseberry/data
fi

# Start old version
systemctl start looseberry

echo "Rollback complete"
```

## Troubleshooting

### Common Issues

**Issue**: Looseberry not starting

```bash
# Check logs
journalctl -u looseberry -n 100

# Check configuration
/usr/local/bin/looseberry validate-config /etc/looseberry/config.toml

# Check permissions
ls -la /var/lib/looseberry/data
```

**Issue**: High memory usage

```bash
# Check metrics
curl http://localhost:8080/metrics | grep memory

# Adjust configuration
# Reduce: MaxPendingTxs, MaxPendingBytes, GCDepth
```

**Issue**: Disk full

```bash
# Check disk usage
df -h /var/lib/looseberry

# Reduce GC depth
# Implement log rotation
# Add more storage
```

**Issue**: Network connectivity problems

```bash
# Check firewall
sudo ufw status

# Check network connectivity
ping <other-validator-ip>
telnet <other-validator-ip> 26656

# Check network metrics
curl http://localhost:8080/metrics | grep network
```

### Debug Mode

Enable debug logging:

```go
import "log"

func init() {
    log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
```

### Performance Profiling

Enable profiling:

```go
import (
    _ "net/http/pprof"
    "net/http"
)

func main() {
    // Start pprof server
    go func() {
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()

    // ... rest of initialization
}
```

Access profiling:

```bash
# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Memory profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

## Next Steps

- **[Monitoring Guide](MONITORING.md)**: Set up metrics and alerting
- **[Performance Tuning](../tutorials/PERFORMANCE_TUNING.md)**: Optimize for your workload
- **[Troubleshooting Reference](../reference/TROUBLESHOOTING.md)**: Detailed troubleshooting guide
- **[Security Reference](../reference/SECURITY.md)**: Security best practices
