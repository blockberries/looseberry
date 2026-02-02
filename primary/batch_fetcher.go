package primary

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// BatchFetcherConfig contains batch fetcher configuration.
type BatchFetcherConfig struct {
	// RequestTimeout is the timeout for batch requests.
	RequestTimeout time.Duration
	// MaxRetries is the maximum number of retries for a batch request.
	MaxRetries int
	// MaxPendingHeaders is the maximum number of headers waiting for batches.
	MaxPendingHeaders int
	// CleanupInterval is the interval for cleaning up timed-out requests.
	CleanupInterval time.Duration
}

// DefaultBatchFetcherConfig returns default batch fetcher configuration.
func DefaultBatchFetcherConfig() BatchFetcherConfig {
	return BatchFetcherConfig{
		RequestTimeout:    10 * time.Second,
		MaxRetries:        3,
		MaxPendingHeaders: 100,
		CleanupInterval:   5 * time.Second,
	}
}

// BatchRequestCallback is called to request a batch from a peer.
type BatchRequestCallback func(validator uint16, digest types.Hash) error

// HeaderReadyCallback is called when a header has all batches available.
type HeaderReadyCallback func(header *types.Header)

// pendingBatchRequest tracks a pending batch request.
type pendingBatchRequest struct {
	digest    types.Hash
	requestAt time.Time
	retries   int
	validator uint16 // Which validator we requested from
}

// pendingHeaderEntry tracks a header waiting for batches.
type pendingHeaderEntry struct {
	header       *types.Header
	missingBatch map[types.Hash]bool // Set of missing batch digests
	createdAt    time.Time
}

// BatchFetcher handles fetching missing batches for headers.
type BatchFetcher struct {
	cfg        BatchFetcherConfig
	batchStore store.BatchStore

	// Pending headers waiting for batches (headerDigest -> entry)
	pendingHeaders   map[types.Hash]*pendingHeaderEntry
	pendingHeadersMu sync.Mutex

	// Pending batch requests (batchDigest -> request info)
	pendingRequests   map[types.Hash]*pendingBatchRequest
	pendingRequestsMu sync.Mutex

	// Batch to headers mapping (which headers need this batch)
	batchToHeaders   map[types.Hash][]types.Hash
	batchToHeadersMu sync.Mutex

	// Callbacks
	requestCallback    BatchRequestCallback
	headerReadyCallback HeaderReadyCallback

	// Validators to request from
	validators   []uint16
	validatorsMu sync.RWMutex

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// Metrics
	batchesRequested atomic.Uint64
	batchesReceived  atomic.Uint64
	requestTimeouts  atomic.Uint64
	headersReady     atomic.Uint64
}

// NewBatchFetcher creates a new batch fetcher.
func NewBatchFetcher(cfg BatchFetcherConfig, batchStore store.BatchStore) *BatchFetcher {
	return &BatchFetcher{
		cfg:             cfg,
		batchStore:      batchStore,
		pendingHeaders:  make(map[types.Hash]*pendingHeaderEntry),
		pendingRequests: make(map[types.Hash]*pendingBatchRequest),
		batchToHeaders:  make(map[types.Hash][]types.Hash),
		stopCh:          make(chan struct{}),
		stoppedCh:       make(chan struct{}),
	}
}

// SetRequestCallback sets the callback for batch requests.
func (bf *BatchFetcher) SetRequestCallback(cb BatchRequestCallback) {
	bf.requestCallback = cb
}

// SetHeaderReadyCallback sets the callback for when headers have all batches.
func (bf *BatchFetcher) SetHeaderReadyCallback(cb HeaderReadyCallback) {
	bf.headerReadyCallback = cb
}

// UpdateValidators updates the list of validators to request batches from.
func (bf *BatchFetcher) UpdateValidators(validators []uint16) {
	bf.validatorsMu.Lock()
	defer bf.validatorsMu.Unlock()
	bf.validators = make([]uint16, len(validators))
	copy(bf.validators, validators)
}

// Start starts the batch fetcher.
func (bf *BatchFetcher) Start() error {
	if bf.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	bf.stopCh = make(chan struct{})
	bf.stoppedCh = make(chan struct{})

	go bf.cleanupLoop()
	return nil
}

// Stop stops the batch fetcher.
func (bf *BatchFetcher) Stop() error {
	if !bf.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(bf.stopCh)
	<-bf.stoppedCh
	return nil
}

// IsRunning returns true if the batch fetcher is running.
func (bf *BatchFetcher) IsRunning() bool {
	return bf.running.Load()
}

// RequestBatchesForHeader checks batch availability for a header and requests missing ones.
// Returns true if all batches are available immediately.
func (bf *BatchFetcher) RequestBatchesForHeader(header *types.Header) bool {
	if header == nil || len(header.BatchRefs) == 0 {
		return true // No batches needed
	}

	// Check which batches are missing
	var missingBatches []types.Hash
	for _, batchRef := range header.BatchRefs {
		if !bf.batchStore.HasBatch(batchRef.Digest) {
			missingBatches = append(missingBatches, batchRef.Digest)
		}
	}

	if len(missingBatches) == 0 {
		return true // All batches available
	}

	// Check if we've hit the pending headers limit
	bf.pendingHeadersMu.Lock()
	if len(bf.pendingHeaders) >= bf.cfg.MaxPendingHeaders {
		bf.pendingHeadersMu.Unlock()
		return false // Too many pending headers, reject
	}

	// Track this header
	headerDigest := header.Digest
	missingSet := make(map[types.Hash]bool)
	for _, digest := range missingBatches {
		missingSet[digest] = true
	}

	bf.pendingHeaders[headerDigest] = &pendingHeaderEntry{
		header:       header.Clone(),
		missingBatch: missingSet,
		createdAt:    time.Now(),
	}
	bf.pendingHeadersMu.Unlock()

	// Track which headers need each batch
	bf.batchToHeadersMu.Lock()
	for _, digest := range missingBatches {
		bf.batchToHeaders[digest] = append(bf.batchToHeaders[digest], headerDigest)
	}
	bf.batchToHeadersMu.Unlock()

	// Request missing batches
	for _, digest := range missingBatches {
		bf.requestBatch(digest, header.Author)
	}

	return false
}

// NotifyBatchReceived notifies the fetcher that a batch was received.
func (bf *BatchFetcher) NotifyBatchReceived(batch *types.Batch) {
	if batch == nil {
		return
	}

	bf.batchesReceived.Add(1)
	batchDigest := batch.Digest

	// Remove from pending requests
	bf.pendingRequestsMu.Lock()
	delete(bf.pendingRequests, batchDigest)
	bf.pendingRequestsMu.Unlock()

	// Get headers waiting for this batch
	bf.batchToHeadersMu.Lock()
	headerDigests := bf.batchToHeaders[batchDigest]
	delete(bf.batchToHeaders, batchDigest)
	bf.batchToHeadersMu.Unlock()

	if len(headerDigests) == 0 {
		return
	}

	// Update each header waiting for this batch
	var readyHeaders []*types.Header
	bf.pendingHeadersMu.Lock()
	for _, headerDigest := range headerDigests {
		entry, exists := bf.pendingHeaders[headerDigest]
		if !exists {
			continue
		}

		// Remove this batch from missing set
		delete(entry.missingBatch, batchDigest)

		// If all batches received, header is ready
		if len(entry.missingBatch) == 0 {
			readyHeaders = append(readyHeaders, entry.header)
			delete(bf.pendingHeaders, headerDigest)
		}
	}
	bf.pendingHeadersMu.Unlock()

	// Notify callbacks for ready headers
	for _, header := range readyHeaders {
		bf.headersReady.Add(1)
		if bf.headerReadyCallback != nil {
			bf.headerReadyCallback(header)
		}
	}
}

// HandleBatchResponse handles a batch response from a peer.
func (bf *BatchFetcher) HandleBatchResponse(batch *types.Batch, found bool, from uint16) {
	if !found || batch == nil {
		// Batch not found at this peer, will retry on timeout
		return
	}

	// Store the batch
	if bf.batchStore != nil {
		_ = bf.batchStore.SaveBatch(batch)
	}

	// Notify batch received
	bf.NotifyBatchReceived(batch)
}

// requestBatch requests a batch from peers.
func (bf *BatchFetcher) requestBatch(digest types.Hash, preferredValidator uint16) {
	bf.pendingRequestsMu.Lock()
	// Check if already requested
	if _, exists := bf.pendingRequests[digest]; exists {
		bf.pendingRequestsMu.Unlock()
		return
	}

	// Track request
	bf.pendingRequests[digest] = &pendingBatchRequest{
		digest:    digest,
		requestAt: time.Now(),
		retries:   0,
		validator: preferredValidator,
	}
	bf.pendingRequestsMu.Unlock()

	bf.batchesRequested.Add(1)

	// Send request
	if bf.requestCallback != nil {
		_ = bf.requestCallback(preferredValidator, digest)
	}
}

// cleanupLoop runs periodic cleanup of timed-out requests.
func (bf *BatchFetcher) cleanupLoop() {
	defer close(bf.stoppedCh)

	ticker := time.NewTicker(bf.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bf.cleanupTimedOutRequests()
			bf.cleanupTimedOutHeaders()
		case <-bf.stopCh:
			return
		}
	}
}

// cleanupTimedOutRequests retries or cleans up timed-out batch requests.
func (bf *BatchFetcher) cleanupTimedOutRequests() {
	cutoff := time.Now().Add(-bf.cfg.RequestTimeout)

	bf.pendingRequestsMu.Lock()
	var toRetry []*pendingBatchRequest
	var toRemove []types.Hash

	for digest, req := range bf.pendingRequests {
		if req.requestAt.Before(cutoff) {
			if req.retries < bf.cfg.MaxRetries {
				toRetry = append(toRetry, req)
			} else {
				toRemove = append(toRemove, digest)
				bf.requestTimeouts.Add(1)
			}
		}
	}

	// Update retry counts
	for _, req := range toRetry {
		req.retries++
		req.requestAt = time.Now()
	}

	// Remove failed requests
	for _, digest := range toRemove {
		delete(bf.pendingRequests, digest)
	}
	bf.pendingRequestsMu.Unlock()

	// Retry requests from different validators
	for _, req := range toRetry {
		bf.retryRequest(req)
	}

	// Clean up headers that had batch requests fail
	if len(toRemove) > 0 {
		bf.cleanupFailedBatchHeaders(toRemove)
	}
}

// retryRequest retries a batch request, potentially from a different validator.
func (bf *BatchFetcher) retryRequest(req *pendingBatchRequest) {
	bf.validatorsMu.RLock()
	validators := bf.validators
	bf.validatorsMu.RUnlock()

	if len(validators) == 0 {
		return
	}

	// Try a different validator
	nextValidator := req.validator
	for _, v := range validators {
		if v != req.validator {
			nextValidator = v
			break
		}
	}

	bf.pendingRequestsMu.Lock()
	if entry, exists := bf.pendingRequests[req.digest]; exists {
		entry.validator = nextValidator
	}
	bf.pendingRequestsMu.Unlock()

	if bf.requestCallback != nil {
		_ = bf.requestCallback(nextValidator, req.digest)
	}
}

// cleanupFailedBatchHeaders removes headers that had batch requests fail.
func (bf *BatchFetcher) cleanupFailedBatchHeaders(failedBatches []types.Hash) {
	bf.batchToHeadersMu.Lock()
	headerDigestsToRemove := make(map[types.Hash]bool)
	for _, batchDigest := range failedBatches {
		for _, headerDigest := range bf.batchToHeaders[batchDigest] {
			headerDigestsToRemove[headerDigest] = true
		}
		delete(bf.batchToHeaders, batchDigest)
	}
	bf.batchToHeadersMu.Unlock()

	bf.pendingHeadersMu.Lock()
	for headerDigest := range headerDigestsToRemove {
		delete(bf.pendingHeaders, headerDigest)
	}
	bf.pendingHeadersMu.Unlock()
}

// cleanupTimedOutHeaders removes headers that have been pending too long.
func (bf *BatchFetcher) cleanupTimedOutHeaders() {
	// Headers timeout at 3x the request timeout
	cutoff := time.Now().Add(-3 * bf.cfg.RequestTimeout)

	bf.pendingHeadersMu.Lock()
	var toRemove []types.Hash
	for digest, entry := range bf.pendingHeaders {
		if entry.createdAt.Before(cutoff) {
			toRemove = append(toRemove, digest)
		}
	}

	for _, digest := range toRemove {
		delete(bf.pendingHeaders, digest)
	}
	bf.pendingHeadersMu.Unlock()
}

// BatchFetcherMetrics contains batch fetcher metrics.
type BatchFetcherMetrics struct {
	PendingHeaders   int
	PendingRequests  int
	BatchesRequested uint64
	BatchesReceived  uint64
	RequestTimeouts  uint64
	HeadersReady     uint64
}

// Metrics returns the current batch fetcher metrics.
func (bf *BatchFetcher) Metrics() BatchFetcherMetrics {
	bf.pendingHeadersMu.Lock()
	pendingHeaders := len(bf.pendingHeaders)
	bf.pendingHeadersMu.Unlock()

	bf.pendingRequestsMu.Lock()
	pendingRequests := len(bf.pendingRequests)
	bf.pendingRequestsMu.Unlock()

	return BatchFetcherMetrics{
		PendingHeaders:   pendingHeaders,
		PendingRequests:  pendingRequests,
		BatchesRequested: bf.batchesRequested.Load(),
		BatchesReceived:  bf.batchesReceived.Load(),
		RequestTimeouts:  bf.requestTimeouts.Load(),
		HeadersReady:     bf.headersReady.Load(),
	}
}

// PendingHeaderCount returns the number of pending headers.
func (bf *BatchFetcher) PendingHeaderCount() int {
	bf.pendingHeadersMu.Lock()
	defer bf.pendingHeadersMu.Unlock()
	return len(bf.pendingHeaders)
}
