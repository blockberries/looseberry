# Custom Storage Backend Tutorial

Learn how to implement a custom storage backend for Looseberry.

## Overview

Looseberry requires three storage interfaces:
- **BatchStore**: Stores transaction batches
- **CertificateStore**: Stores certificates
- **TxIndex**: Transaction lookup index (usually in-memory)

This tutorial shows how to implement custom storage backends.

## Storage Interfaces

### BatchStore Interface

```go
type BatchStore interface {
    SaveBatch(batch *types.Batch) error
    GetBatch(digest types.Hash) (*types.Batch, error)
    HasBatch(digest types.Hash) bool
    GetBatchesByRound(round uint64) ([]*types.Batch, error)
    DeleteBatchesBefore(round uint64) error
    Close() error
}
```

### CertificateStore Interface

```go
type CertificateStore interface {
    SaveCertificate(cert *types.Certificate) error
    GetCertificate(digest types.Hash) (*types.Certificate, error)
    HasCertificate(digest types.Hash) bool
    GetCertificatesByRound(round uint64) ([]*types.Certificate, error)
    GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error)
    DeleteCertificatesBefore(round uint64) error
    HighestRound() uint64
    Close() error
}
```

### TxIndex Interface

```go
type TxIndex interface {
    AddTx(txHash, batchHash types.Hash) error
    GetBatchForTx(txHash types.Hash) (types.Hash, error)
    HasTx(txHash types.Hash) bool
    RemoveTxsForBatch(batchHash types.Hash) error
    AddBatch(batch *types.Batch) error
    Close() error
}
```

## Example: Redis-Backed Storage

### Redis BatchStore

```go
package storage

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/blockberries/looseberry/types"
    "github.com/redis/go-redis/v9"
)

type RedisBatchStore struct {
    client *redis.Client
    ctx    context.Context
}

func NewRedisBatchStore(addr string) (*RedisBatchStore, error) {
    client := redis.NewClient(&redis.Options{
        Addr: addr,
    })

    ctx := context.Background()
    if err := client.Ping(ctx).Err(); err != nil {
        return nil, fmt.Errorf("redis ping failed: %w", err)
    }

    return &RedisBatchStore{
        client: client,
        ctx:    ctx,
    }, nil
}

func (s *RedisBatchStore) SaveBatch(batch *types.Batch) error {
    data, err := json.Marshal(batch)
    if err != nil {
        return err
    }

    key := fmt.Sprintf("batch:%x", batch.Digest)
    if err := s.client.Set(s.ctx, key, data, 0).Err(); err != nil {
        return err
    }

    // Index by round
    roundKey := fmt.Sprintf("batches:round:%d", batch.Round)
    return s.client.SAdd(s.ctx, roundKey, batch.Digest[:]).Err()
}

func (s *RedisBatchStore) GetBatch(digest types.Hash) (*types.Batch, error) {
    key := fmt.Sprintf("batch:%x", digest)
    data, err := s.client.Get(s.ctx, key).Bytes()
    if err != nil {
        if err == redis.Nil {
            return nil, nil
        }
        return nil, err
    }

    var batch types.Batch
    if err := json.Unmarshal(data, &batch); err != nil {
        return nil, err
    }

    return &batch, nil
}

func (s *RedisBatchStore) HasBatch(digest types.Hash) bool {
    key := fmt.Sprintf("batch:%x", digest)
    exists, _ := s.client.Exists(s.ctx, key).Result()
    return exists > 0
}

func (s *RedisBatchStore) GetBatchesByRound(round uint64) ([]*types.Batch, error) {
    roundKey := fmt.Sprintf("batches:round:%d", round)
    digests, err := s.client.SMembers(s.ctx, roundKey).Result()
    if err != nil {
        return nil, err
    }

    batches := make([]*types.Batch, 0, len(digests))
    for _, digestHex := range digests {
        var digest types.Hash
        if _, err := fmt.Sscanf(digestHex, "%x", &digest); err != nil {
            continue
        }

        batch, err := s.GetBatch(digest)
        if err != nil || batch == nil {
            continue
        }

        batches = append(batches, batch)
    }

    return batches, nil
}

func (s *RedisBatchStore) DeleteBatchesBefore(round uint64) error {
    // Use SCAN to find all batch keys for rounds before target
    var cursor uint64
    for {
        keys, nextCursor, err := s.client.Scan(s.ctx, cursor, "batches:round:*", 100).Result()
        if err != nil {
            return err
        }

        for _, key := range keys {
            var r uint64
            if _, err := fmt.Sscanf(key, "batches:round:%d", &r); err != nil {
                continue
            }

            if r < round {
                // Get all digests for this round
                digests, _ := s.client.SMembers(s.ctx, key).Result()

                // Delete batch keys
                for _, digestHex := range digests {
                    batchKey := fmt.Sprintf("batch:%s", digestHex)
                    s.client.Del(s.ctx, batchKey)
                }

                // Delete round index
                s.client.Del(s.ctx, key)
            }
        }

        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    return nil
}

func (s *RedisBatchStore) Close() error {
    return s.client.Close()
}
```

### Usage

```go
package main

import (
    "log"

    "github.com/blockberries/looseberry"
    "your-project/storage"
)

func main() {
    // Create Redis-backed storage
    batchStore, err := storage.NewRedisBatchStore("localhost:6379")
    if err != nil {
        log.Fatalf("Failed to create Redis batch store: %v", err)
    }
    defer batchStore.Close()

    certStore, err := storage.NewRedisCertificateStore("localhost:6379")
    if err != nil {
        log.Fatalf("Failed to create Redis cert store: %v", err)
    }
    defer certStore.Close()

    // TxIndex is typically in-memory
    txIndex := store.NewMemoryTxIndex()
    defer txIndex.Close()

    // Create Looseberry
    cfg := looseberry.DefaultConfig()
    // ... configure ...

    lb, err := looseberry.New(cfg)
    if err != nil {
        log.Fatalf("Failed to create Looseberry: %v", err)
    }

    // Set custom storage
    lb.SetStores(batchStore, certStore, txIndex)

    // ... rest of initialization
}
```

## Performance Considerations

### Caching

Add caching to improve performance:

```go
type CachedBatchStore struct {
    underlying store.BatchStore
    cache      *lru.Cache
}

func (s *CachedBatchStore) GetBatch(digest types.Hash) (*types.Batch, error) {
    // Check cache first
    if cached, ok := s.cache.Get(digest); ok {
        return cached.(*types.Batch), nil
    }

    // Fallback to underlying store
    batch, err := s.underlying.GetBatch(digest)
    if err != nil {
        return nil, err
    }

    // Cache result
    if batch != nil {
        s.cache.Add(digest, batch)
    }

    return batch, nil
}
```

### Batching Writes

Batch writes for better performance:

```go
type BatchedStore struct {
    underlying store.BatchStore
    pending    []*types.Batch
    mu         sync.Mutex
}

func (s *BatchedStore) SaveBatch(batch *types.Batch) error {
    s.mu.Lock()
    s.pending = append(s.pending, batch)
    shouldFlush := len(s.pending) >= 100
    s.mu.Unlock()

    if shouldFlush {
        return s.Flush()
    }

    return nil
}

func (s *BatchedStore) Flush() error {
    s.mu.Lock()
    batches := s.pending
    s.pending = nil
    s.mu.Unlock()

    for _, batch := range batches {
        if err := s.underlying.SaveBatch(batch); err != nil {
            return err
        }
    }

    return nil
}
```

## Testing Your Implementation

```go
package storage

import (
    "testing"

    "github.com/blockberries/looseberry/types"
)

func TestRedisBatchStore(t *testing.T) {
    store, err := NewRedisBatchStore("localhost:6379")
    if err != nil {
        t.Skipf("Redis not available: %v", err)
    }
    defer store.Close()

    // Test save and retrieve
    batch := types.NewBatch(0, 0, 1, []types.Transaction{
        []byte("tx1"),
        []byte("tx2"),
    })

    if err := store.SaveBatch(batch); err != nil {
        t.Fatalf("SaveBatch failed: %v", err)
    }

    retrieved, err := store.GetBatch(batch.Digest)
    if err != nil {
        t.Fatalf("GetBatch failed: %v", err)
    }

    if retrieved == nil {
        t.Fatal("Batch not found")
    }

    if len(retrieved.Transactions) != 2 {
        t.Errorf("Expected 2 transactions, got %d", len(retrieved.Transactions))
    }
}
```

## Best Practices

1. **Thread Safety**: Ensure all methods are thread-safe
2. **Error Handling**: Return descriptive errors
3. **Resource Cleanup**: Implement Close() properly
4. **Idempotency**: SaveBatch should be idempotent
5. **Performance**: Index by round for efficient GC
6. **Consistency**: Maintain consistency with transactions if supported

## Next Steps

- **[Testing Guide](../guides/TESTING.md)**: Test your storage implementation
- **[Performance Tuning](PERFORMANCE_TUNING.md)**: Optimize storage performance
- **[Deployment Guide](../guides/DEPLOYMENT.md)**: Deploy with custom storage
