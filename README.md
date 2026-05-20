# Looseberry

A DAG-based mempool for Byzantine fault-tolerant consensus engines —
Narwhal-style separation of transaction dissemination from consensus
ordering.

Used by validator nodes in the [Stealth stack](../README.md). Full nodes
use a simple FIFO mempool from blockberry instead.

## Architecture

```
Tx → Worker Pool → Batches → Primary → Headers → Votes → Certificates → DAG
       ↑                       ↑                            ↓
   hash routing          batch digests                 ordered output
   (tx % workers)       accumulated                  for consensus layer
```

- **Workers** ingest transactions, hash-route by `binary.BigEndian.Uint64(hash[:8]) % len(workers)`,
  batch on size / byte / time threshold, and broadcast batches.
- **Primary** collects batch digests into Headers, broadcasts, collects
  votes. With 2f+1 votes a Header becomes a Certificate.
- **DAG** stores Certificates with causal parent links; one cert per
  (round, validator); 2f+1 certs at round R advance to R+1.
- **GC** prunes committed rounds, re-injects uncommitted txs into the
  worker pool, and gates new submissions when the uncommitted gap grows.

## Public API

```go
import "github.com/blockberries/looseberry"

cfg := looseberry.DefaultConfig()
cfg.Validator   = mySigner
cfg.Validators  = validatorSet
cfg.Network     = myNetwork    // implements looseberry/network.Network
cfg.TxValidator = func(tx looseberry.Transaction) error { /* ... */ }

lb, err := looseberry.New(cfg)
err = lb.Start(ctx)
defer lb.Stop()

err = lb.AddTx(tx)                                   // submit
batches := lb.ReapCertifiedBatches(maxBytes)         // for block proposal
lb.NotifyCommitted(round)                            // after block commit
lb.UpdateValidatorSet(newSet)                        // on epoch change

current   := lb.CurrentRound()
highest   := lb.HighestRound()
size      := lb.Size()
```

## What's required from the consumer

- A `network.Network` implementation. Looseberry ships only a
  `MockNetwork` for tests; the production transport (libp2p via
  glueberry) lives in raspberry.
- A `BatchStore` and `CertificateStore` (LevelDB and in-memory variants
  ship in `store/`).
- A `TxValidator` (CheckTx-style sanity check before queueing).
- A validator set with `Signer`s.

## Layout

```
looseberry/
├── looseberry.go          Orchestrator + DAGMempool interface + message loop
├── config.go              Config tree + Validate() per section
├── types/                 Hash, Tx, Batch, Header, Vote, Certificate, Validator, Signer, BitSet
├── worker/                Worker, Pool (hash-routed), Scaler (load-based), AckTracker
├── primary/               Primary (header creation, voting), VoteTracker, BatchFetcher
├── dag/                   Causal graph with parent validation
├── gc/                    GCManager (round pruning + tx recovery), FlowController
├── network/               Network interface, MockNetwork, SyncManager
├── store/                 BatchStore, CertificateStore, TxIndex (memory + leveldb)
└── test/                  Multi-node TestNetwork harness
```

## Status

The library is complete in isolation; integration tests with `MockNetwork`
pass. Several seams are stubbed for production deployment — see
[`/Volumes/Tendermint/stealth/PLAN.md`](../PLAN.md) §2.2 for the list:

- T1-2: Headers include batch digests immediately, with no 2f+1 ack gate.
- T1-3: When a referenced batch is missing, `HandleHeader` skips voting
  silently. The fully-written `BatchFetcher` is never instantiated.
- T1-4: BatchAcks are unsigned.
- T1-5: `network/sync.go::checkAndSync` is a literal placeholder.
- T2-5: `ReapCertifiedBatches` may return duplicate batches.
- T2-6: Stores use `encoding/gob`, not Cramberry. TxIndex is in-memory only.

The advertised "200K+ TPS" headline has no benchmark backing it in this
repo; the closest is single-process AddTx ingest.

## Development

See [`CLAUDE.md`](./CLAUDE.md) for development guidelines.
[`ARCHITECTURE.md`](./ARCHITECTURE.md) for design details.
[`CHANGELOG.md`](./CHANGELOG.md) for release history.

## License

Apache-2.0.
