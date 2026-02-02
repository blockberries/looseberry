# Error Handling Reference

Comprehensive guide to error handling patterns and best practices in Looseberry.

## Error Categories

Looseberry errors are categorized by type and handling strategy:

| Category | Retryable | Byzantine | Examples |
|----------|-----------|-----------|----------|
| **Transient** | Yes | No | Backpressure, flow control |
| **Invalid Input** | No | No | Empty transaction, invalid format |
| **Byzantine** | No | Yes | Invalid signature, double vote |
| **System** | Maybe | No | Storage failure, network timeout |

## Core Errors

### Transaction Errors

```go
// ErrTxAlreadyExists - Transaction already in mempool
var ErrTxAlreadyExists = errors.New("transaction already exists")

// ErrTxValidationFailed - Transaction failed validation
var ErrTxValidationFailed = errors.New("transaction validation failed")
```

**Handling**:
```go
err := lb.AddTx(tx)
switch {
case errors.Is(err, types.ErrTxAlreadyExists):
    // Duplicate - ignore
case errors.Is(err, types.ErrTxValidationFailed):
    // Invalid - reject permanently
default:
    // Other error
}
```

### Backpressure Errors

```go
// ErrWorkerBackpressure - Worker overloaded
var ErrWorkerBackpressure = errors.New("worker back-pressure")

// ErrMempoolFull - Mempool at capacity
var ErrMempoolFull = errors.New("mempool is full")
```

**Handling with Retry**:
```go
func addWithRetry(lb looseberry.DAGMempool, tx []byte, maxRetries int) error {
    for i := 0; i < maxRetries; i++ {
        err := lb.AddTx(tx)
        if err == nil {
            return nil
        }

        if !types.IsRetryable(err) {
            return err // Non-retryable error
        }

        // Exponential backoff
        backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
        time.Sleep(backoff)
    }
    return fmt.Errorf("max retries exceeded")
}
```

### Flow Control Errors

```go
// ErrFlowControlPaused - DAG production paused
var ErrFlowControlPaused = errors.New("flow control: DAG production paused")
```

**Handling**:
```go
err := lb.AddTx(tx)
if errors.Is(err, types.ErrFlowControlPaused) {
    // Wait for consensus to catch up
    time.Sleep(100 * time.Millisecond)
    return addWithRetry(lb, tx, 5)
}
```

### Byzantine Errors

```go
// ErrInvalidSignature - Cryptographic verification failed
var ErrInvalidSignature = errors.New("invalid signature")

// ErrDuplicateVote - Validator voted twice
var ErrDuplicateVote = errors.New("duplicate vote from same validator")
```

**Handling**:
```go
err := validateCertificate(cert)
if types.IsByzantine(err) {
    // Log Byzantine behavior
    log.Warn("Byzantine behavior detected",
        "validator", cert.Header.Author,
        "error", err)

    // Report to consensus
    reportByzantine(cert.Header.Author, err)

    // Reject certificate
    return err
}
```

## Error Checking Utilities

### IsRetryable

Check if error is transient and retryable:

```go
func IsRetryable(err error) bool {
    switch {
    case errors.Is(err, ErrWorkerBackpressure):
        return true
    case errors.Is(err, ErrMempoolFull):
        return true
    case errors.Is(err, ErrFlowControlPaused):
        return true
    case errors.Is(err, ErrSyncTimeout):
        return true
    default:
        return false
    }
}
```

**Usage**:
```go
err := lb.AddTx(tx)
if types.IsRetryable(err) {
    // Retry after delay
    time.Sleep(10 * time.Millisecond)
    err = lb.AddTx(tx)
}
```

### IsByzantine

Check if error indicates Byzantine behavior:

```go
func IsByzantine(err error) bool {
    switch {
    case errors.Is(err, ErrInvalidSignature):
        return true
    case errors.Is(err, ErrDuplicateHeader):
        return true
    case errors.Is(err, ErrDuplicateVote):
        return true
    default:
        return false
    }
}
```

## Error Handling Patterns

### Pattern 1: Immediate Rejection

For invalid input that should never be retried:

```go
func validateTransaction(tx []byte) error {
    if len(tx) == 0 {
        return fmt.Errorf("empty transaction")
    }
    if len(tx) > MaxTxSize {
        return fmt.Errorf("transaction too large: %d > %d", len(tx), MaxTxSize)
    }
    return nil
}

// Usage
if err := validateTransaction(tx); err != nil {
    return err // Don't retry
}
```

### Pattern 2: Retry with Backoff

For transient errors:

```go
func exponentialBackoff(attempt int) time.Duration {
    base := 10 * time.Millisecond
    max := 5 * time.Second
    duration := base * time.Duration(1<<uint(attempt))
    if duration > max {
        duration = max
    }
    return duration
}

func submitWithBackoff(lb looseberry.DAGMempool, tx []byte) error {
    for attempt := 0; attempt < 10; attempt++ {
        err := lb.AddTx(tx)
        if err == nil {
            return nil
        }

        if !types.IsRetryable(err) {
            return err
        }

        time.Sleep(exponentialBackoff(attempt))
    }
    return fmt.Errorf("max retries exceeded")
}
```

### Pattern 3: Circuit Breaker

Prevent cascading failures:

```go
type CircuitBreaker struct {
    failures    int
    maxFailures int
    timeout     time.Duration
    lastFailure time.Time
    mu          sync.Mutex
}

func (cb *CircuitBreaker) Call(fn func() error) error {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    // Check if circuit is open
    if cb.failures >= cb.maxFailures {
        if time.Since(cb.lastFailure) < cb.timeout {
            return fmt.Errorf("circuit breaker open")
        }
        // Try to close circuit
        cb.failures = 0
    }

    // Execute function
    err := fn()
    if err != nil {
        cb.failures++
        cb.lastFailure = time.Now()
        return err
    }

    // Success - reset failures
    cb.failures = 0
    return nil
}
```

### Pattern 4: Error Aggregation

Collect multiple errors:

```go
type ErrorList struct {
    errors []error
    mu     sync.Mutex
}

func (el *ErrorList) Add(err error) {
    if err != nil {
        el.mu.Lock()
        el.errors = append(el.errors, err)
        el.mu.Unlock()
    }
}

func (el *ErrorList) Error() string {
    el.mu.Lock()
    defer el.mu.Unlock()

    if len(el.errors) == 0 {
        return ""
    }

    var b strings.Builder
    b.WriteString(fmt.Sprintf("%d errors occurred:\n", len(el.errors)))
    for i, err := range el.errors {
        b.WriteString(fmt.Sprintf("  %d. %v\n", i+1, err))
    }
    return b.String()
}

// Usage
func processMultipleTxs(lb looseberry.DAGMempool, txs [][]byte) error {
    errList := &ErrorList{}
    var wg sync.WaitGroup

    for _, tx := range txs {
        wg.Add(1)
        go func(t []byte) {
            defer wg.Done()
            if err := lb.AddTx(t); err != nil {
                errList.Add(err)
            }
        }(tx)
    }

    wg.Wait()

    if len(errList.errors) > 0 {
        return errList
    }
    return nil
}
```

## Custom Validation Errors

### Implementing TxValidator

```go
type ValidationError struct {
    Reason string
    Tx     []byte
}

func (e *ValidationError) Error() string {
    return fmt.Sprintf("validation failed: %s", e.Reason)
}

cfg.TxValidator = func(tx []byte) error {
    // Check size
    if len(tx) > 1024*1024 {
        return &ValidationError{
            Reason: "transaction too large",
            Tx:     tx,
        }
    }

    // Check format
    if !isValidFormat(tx) {
        return &ValidationError{
            Reason: "invalid format",
            Tx:     tx,
        }
    }

    return nil
}
```

## Error Logging

### Structured Error Logging

```go
import "go.uber.org/zap"

func logError(logger *zap.Logger, err error, ctx ...interface{}) {
    fields := []zap.Field{
        zap.Error(err),
        zap.Bool("retryable", types.IsRetryable(err)),
        zap.Bool("byzantine", types.IsByzantine(err)),
    }

    if types.IsByzantine(err) {
        logger.Warn("Byzantine error", fields...)
    } else if types.IsRetryable(err) {
        logger.Debug("Retryable error", fields...)
    } else {
        logger.Error("Error", fields...)
    }
}
```

### Error Metrics

```go
var (
    errorCounter = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "looseberry_errors_total",
            Help: "Total errors by type",
        },
        []string{"type", "retryable"},
    )
)

func recordError(err error) {
    errorType := "unknown"
    switch {
    case errors.Is(err, types.ErrWorkerBackpressure):
        errorType = "backpressure"
    case errors.Is(err, types.ErrInvalidSignature):
        errorType = "byzantine"
    // ... other cases
    }

    retryable := "false"
    if types.IsRetryable(err) {
        retryable = "true"
    }

    errorCounter.WithLabelValues(errorType, retryable).Inc()
}
```

## Error Handling in Production

### 1. Always Check Errors

```go
// WRONG
lb.AddTx(tx) // Ignoring error

// CORRECT
if err := lb.AddTx(tx); err != nil {
    log.Printf("Failed to add transaction: %v", err)
    return err
}
```

### 2. Context in Errors

```go
// WRONG
return err

// CORRECT
return fmt.Errorf("failed to add transaction: %w", err)
```

### 3. Don't Panic

```go
// WRONG
if err != nil {
    panic(err)
}

// CORRECT
if err != nil {
    log.Printf("Error: %v", err)
    return err
}
```

### 4. Handle All Error Types

```go
err := lb.AddTx(tx)
switch {
case err == nil:
    // Success
case types.IsRetryable(err):
    // Retry
case types.IsByzantine(err):
    // Report
default:
    // Other error
}
```

## Testing Error Handling

```go
func TestErrorHandling(t *testing.T) {
    // Test retryable error
    t.Run("Retryable", func(t *testing.T) {
        err := types.ErrWorkerBackpressure
        if !types.IsRetryable(err) {
            t.Error("Expected retryable error")
        }
    })

    // Test Byzantine error
    t.Run("Byzantine", func(t *testing.T) {
        err := types.ErrInvalidSignature
        if !types.IsByzantine(err) {
            t.Error("Expected Byzantine error")
        }
    })

    // Test error wrapping
    t.Run("Wrapping", func(t *testing.T) {
        original := types.ErrTxAlreadyExists
        wrapped := fmt.Errorf("context: %w", original)
        if !errors.Is(wrapped, original) {
            t.Error("Error wrapping not preserved")
        }
    })
}
```

## Best Practices

1. **Check All Errors**: Never ignore errors
2. **Add Context**: Wrap errors with context using `%w`
3. **Use Correct Type**: Check error type before handling
4. **Log Appropriately**: Different severity for different errors
5. **Retry Wisely**: Only retry transient errors
6. **Report Byzantine**: Always report Byzantine behavior
7. **Test Error Paths**: Test error handling code
8. **Document Errors**: Document which errors functions return

## Next Steps

- **[Security Reference](SECURITY.md)**: Security error handling
- **[Troubleshooting Reference](TROUBLESHOOTING.md)**: Debug errors
- **[FAQ](FAQ.md)**: Common error questions
