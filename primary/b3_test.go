package primary

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// fakeAckQuorum is a hand-rolled AckQuorumChecker. The map is populated by
// the test to mark which digests are "quorum-ready".
type fakeAckQuorum struct {
	mu    sync.Mutex
	ready map[types.Hash]bool
}

func newFakeAckQuorum() *fakeAckQuorum {
	return &fakeAckQuorum{ready: make(map[types.Hash]bool)}
}

func (f *fakeAckQuorum) HasBatchAckQuorum(d types.Hash) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready[d]
}

func (f *fakeAckQuorum) MarkReady(d types.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready[d] = true
}

// TestTryCreateHeader_GatesOnAckQuorum is the load-bearing test for B3-2:
// a digest enqueued via AddBatchDigest must not appear in a created header
// until the AckQuorumChecker marks it ready.
func TestTryCreateHeader_GatesOnAckQuorum(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 50 * time.Millisecond
	cfg.AllowEmptyHeaders = true // round-0 headers always allowed empty

	p := New(0, signers[0], cfg, certStore, batchStore, vs)
	acks := newFakeAckQuorum()
	p.SetAckQuorumChecker(acks)

	var observed []*types.Header
	var observedMu sync.Mutex
	p.SetHeaderCallback(func(h *types.Header) {
		observedMu.Lock()
		observed = append(observed, h.Clone())
		observedMu.Unlock()
	})

	digest := types.BatchDigest{
		Digest:      types.HashBytes([]byte("waiting-on-acks")),
		WorkerID:    0,
		ValidatorID: 0,
	}
	p.AddBatchDigest(digest)

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	// Give the ticker enough time to fire several times without quorum.
	time.Sleep(200 * time.Millisecond)
	observedMu.Lock()
	preHeaders := make([]*types.Header, len(observed))
	copy(preHeaders, observed)
	observedMu.Unlock()

	// Headers MAY be produced (empty, for liveness) but they must NOT
	// reference the digest while quorum hasn't been reached.
	for _, h := range preHeaders {
		for _, r := range h.BatchRefs {
			if r.Digest.Equal(digest.Digest) {
				t.Fatalf("digest included in header before ack quorum was reached")
			}
		}
	}
	// The unready digest must remain queued. We poll briefly to side-step
	// the tiny window inside tryCreateHeader where the queue is temporarily
	// drained for classification.
	queued := -1
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); {
		p.digestsMu.Lock()
		queued = len(p.batchDigests)
		p.digestsMu.Unlock()
		if queued == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if queued != 1 {
		t.Fatalf("digest disappeared from queue while not ready: queued=%d", queued)
	}

	// Mark quorum-ready and wait for the next tick.
	acks.MarkReady(digest.Digest)
	time.Sleep(200 * time.Millisecond)

	observedMu.Lock()
	defer observedMu.Unlock()
	includedAny := false
	for _, h := range observed {
		for _, r := range h.BatchRefs {
			if r.Digest.Equal(digest.Digest) {
				includedAny = true
			}
		}
	}
	if !includedAny {
		t.Errorf("header did not include the ready digest after quorum")
	}
}

// TestHandleHeader_RequestsMissingBatches verifies that a header with a
// missing batch causes BatchFetcher.RequestBatchesForHeader to be invoked
// (B3-3 / T1-3) and the vote is NOT sent until the fetcher signals ready.
func TestHandleHeader_RequestsMissingBatches(t *testing.T) {
	certStore := store.NewMemoryCertificateStore()
	batchStore := store.NewMemoryBatchStore()
	defer certStore.Close()
	defer batchStore.Close()

	vs, signers := createTestValidatorSet(t, 4)

	cfg := DefaultConfig()
	cfg.HeaderTimeout = 1 * time.Second
	p := New(1, signers[1], cfg, certStore, batchStore, vs)

	// Wire a fake fetcher.
	fetcher := &fakeFetcher{}
	p.SetBatchFetcher(fetcher)

	var sent atomic.Int32
	p.SetVoteCallback(func(*types.Vote, uint16) { sent.Add(1) })

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	// Build a header by validator 0 (author) referencing a batch that is
	// NOT in our local store.
	missing := types.BatchDigest{
		Digest:      types.HashBytes([]byte("not-here")),
		WorkerID:    0,
		ValidatorID: 0,
	}
	header := types.NewHeader(0, 0, 0, []types.BatchDigest{missing}, nil)
	if err := header.Sign(signers[0]); err != nil {
		t.Fatalf("sign: %v", err)
	}

	if err := p.HandleHeader(header); err != nil {
		t.Fatalf("HandleHeader: %v", err)
	}

	if fetcher.calls.Load() != 1 {
		t.Errorf("fetcher.RequestBatchesForHeader call count: got %d, want 1",
			fetcher.calls.Load())
	}
	if sent.Load() != 0 {
		t.Errorf("vote sent while batches were missing: %d", sent.Load())
	}

	// Now mark the fetcher "ready" and call HandleHeader again — but this
	// time the batch is also locally present, so the fast path votes.
	fetcher.alwaysReady.Store(true)
	if err := batchStore.SaveBatch(types.NewBatch(0, 0, 0, nil)); err != nil {
		// Not strictly required since the fetcher returns true unconditionally,
		// but having it removes the dependency on a particular batch existing.
		_ = err
	}
	if err := p.HandleHeader(header); err != nil {
		t.Fatalf("second HandleHeader: %v", err)
	}
	if sent.Load() == 0 {
		t.Errorf("vote not sent after fetcher reported ready")
	}
}

// fakeFetcher implements BatchAvailabilityChecker. Returns true once
// alwaysReady is set.
type fakeFetcher struct {
	calls       atomic.Int32
	alwaysReady atomic.Bool
}

func (f *fakeFetcher) RequestBatchesForHeader(_ *types.Header) bool {
	f.calls.Add(1)
	return f.alwaysReady.Load()
}
