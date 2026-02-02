# Security Reference

Security considerations and best practices for Looseberry deployments.

## Security Model

Looseberry is designed for Byzantine fault-tolerant environments where up to f validators may be malicious (n = 3f + 1).

### Threat Model

| Threat | Mitigation |
|--------|------------|
| **Byzantine Validators** | Quorum-based certificate formation (2f+1 votes) |
| **Invalid Signatures** | Cryptographic verification of all signatures |
| **Double Voting** | Vote tracking and equivocation detection |
| **Network Attacks** | TLS encryption, authentication |
| **DoS Attacks** | Backpressure, flow control, rate limiting |
| **Key Compromise** | Key rotation, HSM support |

## Cryptographic Security

### Key Management

**Key Generation**:
```go
// Generate Ed25519 key pair
signer, err := types.GenerateEd25519Signer(validatorIndex)
```

**Secure Key Storage**:
```go
// DON'T: Store keys in code
const privateKey = "..."  // NEVER DO THIS

// DON'T: Store keys in environment variables
privateKey := os.Getenv("PRIVATE_KEY")  // Insecure

// DO: Use secure key storage
func loadSignerFromVault() (types.Signer, error) {
    client, _ := vault.NewClient(...)
    secret, _ := client.Logical().Read("secret/data/validator-key")
    return types.ImportEd25519Signer(secret.Data["private_key"].(string))
}

// DO: Use HSM for production
func loadSignerFromHSM() (types.Signer, error) {
    // Use hardware security module
    return hsm.LoadSigner(keyID)
}
```

**Key Rotation**:
```go
func rotateValidatorKey(consensus Consensus, newSigner types.Signer) error {
    // 1. Generate new key
    // 2. Submit validator update to consensus
    // 3. Wait for epoch transition
    // 4. Update Looseberry configuration
    // 5. Securely delete old key
}
```

### Signature Verification

All signatures are verified:
```go
// Verify header signature
func verifyHeader(header *types.Header, validatorSet types.ValidatorSet) error {
    validator, err := validatorSet.GetByIndex(header.Author)
    if err != nil {
        return types.ErrValidatorNotFound
    }

    if !header.VerifySignature(validator.PublicKey) {
        return types.ErrInvalidSignature
    }

    return nil
}

// Verify vote signature
func verifyVote(vote *types.Vote, validatorSet types.ValidatorSet) error {
    validator, err := validatorSet.GetByIndex(vote.Validator)
    if err != nil {
        return types.ErrValidatorNotFound
    }

    if !vote.VerifySignature(validator.PublicKey) {
        return types.ErrInvalidSignature
    }

    return nil
}
```

## Byzantine Fault Tolerance

### Quorum Requirements

```go
// For n = 3f + 1 validators
n := validatorSet.Count()
f := validatorSet.F()                    // (n - 1) / 3
quorum := validatorSet.Quorum()          // 2f + 1

// Certificate requires 2f+1 votes
if len(votes) < quorum {
    return types.ErrInsufficientQuorum
}
```

### Equivocation Detection

```go
type VoteTracker struct {
    votes map[uint16]map[types.Hash]*types.Vote // validator -> header -> vote
}

func (vt *VoteTracker) AddVote(vote *types.Vote) error {
    existingVotes := vt.votes[vote.Validator]

    // Check for double voting (same validator, different headers, same round)
    for headerDigest, existing := range existingVotes {
        if headerDigest != vote.HeaderDigest {
            // Byzantine: validator voted for two different headers
            return &ByzantineError{
                Type:      "double-vote",
                Validator: vote.Validator,
                Evidence:  []*types.Vote{existing, vote},
            }
        }
    }

    // Record vote
    if existingVotes == nil {
        existingVotes = make(map[types.Hash]*types.Vote)
        vt.votes[vote.Validator] = existingVotes
    }
    existingVotes[vote.HeaderDigest] = vote

    return nil
}
```

### Byzantine Behavior Reporting

```go
type ByzantineEvidence struct {
    Validator uint16
    Type      string // "double-vote", "invalid-signature", "equivocation"
    Evidence  interface{}
    Timestamp time.Time
}

func reportByzantine(validator uint16, evidence ByzantineEvidence) {
    log.Warn("Byzantine behavior detected",
        "validator", validator,
        "type", evidence.Type,
        "timestamp", evidence.Timestamp)

    // Report to consensus for slashing
    consensus.ReportByzantine(evidence)

    // Optionally blacklist validator temporarily
    blacklist.Add(validator, 1*time.Hour)
}
```

## Network Security

### TLS Configuration

```go
import "crypto/tls"

func createTLSConfig() *tls.Config {
    return &tls.Config{
        MinVersion:   tls.VersionTLS13,
        CipherSuites: []uint16{
            tls.TLS_AES_256_GCM_SHA384,
            tls.TLS_CHACHA20_POLY1305_SHA256,
        },
        CurvePreferences: []tls.CurveID{
            tls.X25519,
            tls.CurveP256,
        },
        PreferServerCipherSuites: true,
    }
}
```

### Mutual TLS

```go
func createMutualTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
    cert, err := tls.LoadX509KeyPair(certFile, keyFile)
    if err != nil {
        return nil, err
    }

    caCert, err := os.ReadFile(caFile)
    if err != nil {
        return nil, err
    }

    caCertPool := x509.NewCertPool()
    caCertPool.AppendCertsFromPEM(caCert)

    return &tls.Config{
        Certificates: []tls.Certificate{cert},
        ClientAuth:   tls.RequireAndVerifyClientCert,
        ClientCAs:    caCertPool,
        MinVersion:   tls.VersionTLS13,
    }, nil
}
```

### Message Authentication

```go
// All messages include sender signature
type AuthenticatedMessage struct {
    Payload   []byte
    Sender    uint16
    Signature types.Signature
}

func (m *AuthenticatedMessage) Verify(validatorSet types.ValidatorSet) error {
    validator, err := validatorSet.GetByIndex(m.Sender)
    if err != nil {
        return err
    }

    msg := m.Payload
    if !validator.PublicKey.Verify(msg, m.Signature[:]) {
        return types.ErrInvalidSignature
    }

    return nil
}
```

## Access Control

### File Permissions

```bash
# Data directory
chmod 700 /var/lib/looseberry
chown validator:validator /var/lib/looseberry

# Configuration files
chmod 600 /etc/looseberry/config.toml
chown validator:validator /etc/looseberry/config.toml

# Log files
chmod 640 /var/log/looseberry/looseberry.log
chown validator:validator /var/log/looseberry/looseberry.log
```

### Process Isolation

```bash
# Run as dedicated user
sudo useradd -r -s /bin/false validator
sudo -u validator /usr/local/bin/looseberry

# Systemd service with security options
[Service]
User=validator
Group=validator
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/looseberry
```

### Network Firewall

```bash
# Only allow validator IPs
ufw allow from 10.0.1.10 to any port 26656 proto tcp
ufw allow from 10.0.1.11 to any port 26656 proto tcp
ufw allow from 10.0.1.12 to any port 26656 proto tcp
ufw deny 26656/tcp

# Rate limiting
ufw limit 26656/tcp
```

## DoS Protection

### Rate Limiting

```go
import "golang.org/x/time/rate"

type RateLimiter struct {
    limiters map[uint16]*rate.Limiter
    mu       sync.Mutex
}

func (rl *RateLimiter) Allow(validator uint16) bool {
    rl.mu.Lock()
    defer rl.mu.Unlock()

    limiter, ok := rl.limiters[validator]
    if !ok {
        // 100 requests per second per validator
        limiter = rate.NewLimiter(100, 200)
        rl.limiters[validator] = limiter
    }

    return limiter.Allow()
}

// Usage
if !rateLimiter.Allow(msg.From) {
    log.Warn("Rate limit exceeded", "validator", msg.From)
    return types.ErrRateLimitExceeded
}
```

### Backpressure

Looseberry includes built-in backpressure:
```go
cfg.Worker.MaxPendingTxs = 10000      // Per-worker limit
cfg.Worker.MaxPendingBytes = 50*1024*1024  // 50MB per worker
```

When limits are reached, `AddTx()` returns `ErrWorkerBackpressure`.

### Flow Control

```go
cfg.FlowControl.MaxUncommittedRounds = 100  // Pause if consensus falls behind
```

When paused, `AddTx()` returns `ErrFlowControlPaused`.

## Input Validation

### Transaction Validation

```go
cfg.TxValidator = func(tx []byte) error {
    // Size validation
    if len(tx) == 0 {
        return fmt.Errorf("empty transaction")
    }
    if len(tx) > 1024*1024 {
        return fmt.Errorf("transaction too large: %d bytes", len(tx))
    }

    // Format validation
    if !isValidFormat(tx) {
        return fmt.Errorf("invalid transaction format")
    }

    // Signature validation (if applicable)
    if !verifyTxSignature(tx) {
        return fmt.Errorf("invalid transaction signature")
    }

    return nil
}
```

### Header Validation

```go
func validateHeader(header *types.Header, validatorSet types.ValidatorSet, currentRound uint64) error {
    // Check author is valid validator
    if !validatorSet.Contains(header.Author) {
        return types.ErrInvalidValidator
    }

    // Check round is reasonable
    if header.Round > currentRound+cfg.Primary.MaxRoundGap {
        return types.ErrHeaderTooFarAhead
    }

    // Check epoch matches
    if header.Epoch != validatorSet.Epoch() {
        return types.ErrEpochMismatch
    }

    // Verify signature
    validator, _ := validatorSet.GetByIndex(header.Author)
    if !header.VerifySignature(validator.PublicKey) {
        return types.ErrInvalidSignature
    }

    return nil
}
```

## Auditing and Monitoring

### Security Event Logging

```go
func logSecurityEvent(event string, details map[string]interface{}) {
    log.Warn("SECURITY EVENT",
        "event", event,
        "details", details,
        "timestamp", time.Now().Unix())

    // Send to SIEM
    sendToSIEM(event, details)
}

// Examples
logSecurityEvent("invalid_signature", map[string]interface{}{
    "validator": validator,
    "header": header.Digest(),
})

logSecurityEvent("double_vote", map[string]interface{}{
    "validator": vote.Validator,
    "round": round,
})

logSecurityEvent("rate_limit_exceeded", map[string]interface{}{
    "validator": validator,
    "count": count,
})
```

### Security Metrics

```go
var (
    byzantineCounter = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "looseberry_byzantine_total",
            Help: "Total Byzantine behaviors detected",
        },
        []string{"type", "validator"},
    )

    signatureFailures = promauto.NewCounter(
        prometheus.CounterOpts{
            Name: "looseberry_signature_failures_total",
            Help: "Total signature verification failures",
        },
    )
)

// Record Byzantine behavior
byzantineCounter.WithLabelValues("double-vote", fmt.Sprint(validator)).Inc()

// Record signature failures
signatureFailures.Inc()
```

## Security Checklist

- [ ] Validator keys stored securely (Vault/HSM)
- [ ] TLS enabled for network communication
- [ ] Mutual TLS for validator authentication
- [ ] All signatures verified
- [ ] Byzantine behavior detection enabled
- [ ] Rate limiting configured
- [ ] Backpressure limits set appropriately
- [ ] Input validation implemented
- [ ] File permissions restricted
- [ ] Process runs as dedicated user
- [ ] Firewall rules configured
- [ ] Security events logged
- [ ] Monitoring and alerting configured
- [ ] Incident response plan documented
- [ ] Regular security audits scheduled

## Incident Response

### Compromised Validator Key

1. **Immediate**: Stop validator
2. **Rotate**: Generate new key
3. **Update**: Submit key update to consensus
4. **Monitor**: Watch for unauthorized activity
5. **Investigate**: Determine how compromise occurred

### Byzantine Behavior Detected

1. **Log**: Record evidence
2. **Report**: Submit to consensus for slashing
3. **Isolate**: Temporarily blacklist validator
4. **Alert**: Notify operators
5. **Analyze**: Investigate root cause

### DoS Attack

1. **Identify**: Determine attack source
2. **Mitigate**: Enable rate limiting
3. **Block**: Firewall malicious IPs
4. **Monitor**: Watch for attack evolution
5. **Report**: Notify network operators

## Best Practices

1. **Defense in Depth**: Multiple security layers
2. **Principle of Least Privilege**: Minimal permissions
3. **Secure by Default**: Safe default configuration
4. **Fail Securely**: Reject on verification failure
5. **Audit Everything**: Log all security events
6. **Monitor Continuously**: Real-time alerting
7. **Test Security**: Regular penetration testing
8. **Update Regularly**: Apply security patches promptly

## Next Steps

- **[Troubleshooting Reference](TROUBLESHOOTING.md)**: Debug security issues
- **[Deployment Guide](../guides/DEPLOYMENT.md)**: Secure deployment
- **[Monitoring Guide](../guides/MONITORING.md)**: Security monitoring
