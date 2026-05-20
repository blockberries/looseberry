package looseberry

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/types"
)

// TestBatchFetcher_InstantiatedAndWired confirms that Looseberry.Start
// constructs a BatchFetcher and exposes it through the primary (B3-1).
func TestBatchFetcher_InstantiatedAndWired(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	lb.SetNetwork(network.NewMockNetwork(0, network.DefaultConfig()))
	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lb.Stop() })

	if lb.batchFetcher == nil {
		t.Fatal("Looseberry.batchFetcher is nil after Start")
	}
	if !lb.batchFetcher.IsRunning() {
		t.Error("BatchFetcher should be running")
	}
}

// TestBatchFetcher_RequestsMissingBatchOverNetwork drives the full flow:
// a missing batch triggers a BatchRequest on the wire, the peer responds
// with the batch, the fetcher writes it to the local store. (B3-1 + B3-3).
func TestBatchFetcher_RequestsMissingBatchOverNetwork(t *testing.T) {
	vs, signers := createTestValidatorSet(t, 4)

	// Local node (validator 0).
	cfg := createTestConfig(signers[0], 0)
	lb, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lb.SetValidatorSet(vs)
	localNet := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(localNet)

	// Peer (validator 1) with an answering MockNetwork. We don't run a full
	// Looseberry on the peer side — we drain BatchRequestMessages manually
	// and reply with the batch.
	peerNet := network.NewMockNetwork(1, network.DefaultConfig())
	localNet.Connect(peerNet)
	if err := peerNet.Start(); err != nil {
		t.Fatalf("peer start: %v", err)
	}
	t.Cleanup(func() { _ = peerNet.Stop() })

	batch := types.NewBatch(1, 1, 0, []types.Transaction{
		types.Transaction([]byte("remote-batch")),
	})
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		for {
			select {
			case req := <-peerNet.BatchRequestMessages():
				if req == nil {
					return
				}
				_ = peerNet.SendBatchResponse(req.Requester, &network.BatchResponseMessage{
					Batch: batch,
					Found: true,
					From:  1,
				})
				return
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	if err := lb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lb.Stop() })

	// Ask the fetcher for a header that references the (missing) batch.
	missingRef := types.BatchDigest{
		Digest:      batch.Digest,
		WorkerID:    1,
		ValidatorID: 1,
	}
	header := types.NewHeader(1, 0, 0, []types.BatchDigest{missingRef}, nil)
	if err := header.Sign(signers[1]); err != nil {
		t.Fatalf("sign header: %v", err)
	}

	if available := lb.batchFetcher.RequestBatchesForHeader(header); available {
		t.Fatal("batch reported available before peer reply")
	}

	// Wait for the fetcher to receive the batch over the wire.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lb.batchStore.HasBatch(batch.Digest) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !lb.batchStore.HasBatch(batch.Digest) {
		t.Fatal("batch never landed in local batch store")
	}
	<-peerDone
}
