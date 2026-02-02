package dag

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

// Config contains DAG configuration.
type Config struct {
	// MaxCachedRounds is the maximum number of rounds to cache in memory.
	MaxCachedRounds int
	// MaxHistoryDepth is the maximum depth for causal history traversal.
	MaxHistoryDepth int
	// MaxHistoryCacheSize is the maximum number of history cache entries (for LRU).
	MaxHistoryCacheSize int
}

// DefaultConfig returns default DAG configuration.
func DefaultConfig() Config {
	return Config{
		MaxCachedRounds:     100,
		MaxHistoryDepth:     1000,
		MaxHistoryCacheSize: 1000,
	}
}

// RoundData holds certificates for a specific round.
type RoundData struct {
	Round        uint64
	Certificates map[uint16]*types.Certificate // By validator index
	committed    bool
}

// NewRoundData creates a new RoundData for the given round.
func NewRoundData(round uint64) *RoundData {
	return &RoundData{
		Round:        round,
		Certificates: make(map[uint16]*types.Certificate),
	}
}

// AddCertificate adds a certificate to the round.
func (rd *RoundData) AddCertificate(cert *types.Certificate) {
	rd.Certificates[cert.Author()] = cert
}

// GetCertificate returns the certificate from the given validator.
func (rd *RoundData) GetCertificate(validator uint16) (*types.Certificate, bool) {
	cert, ok := rd.Certificates[validator]
	return cert, ok
}

// HasCertificate returns true if the round has a certificate from the validator.
func (rd *RoundData) HasCertificate(validator uint16) bool {
	_, ok := rd.Certificates[validator]
	return ok
}

// Count returns the number of certificates in the round.
func (rd *RoundData) Count() int {
	return len(rd.Certificates)
}

// IsCommitted returns true if the round is committed.
func (rd *RoundData) IsCommitted() bool {
	return rd.committed
}

// SetCommitted marks the round as committed.
func (rd *RoundData) SetCommitted() {
	rd.committed = true
}

// GetAllCertificates returns all certificates in the round.
func (rd *RoundData) GetAllCertificates() []*types.Certificate {
	certs := make([]*types.Certificate, 0, len(rd.Certificates))
	for _, cert := range rd.Certificates {
		certs = append(certs, cert)
	}
	return certs
}

// DAG represents the certificate DAG with causal ordering.
type DAG struct {
	rounds   map[uint64]*RoundData
	roundsMu sync.RWMutex

	// Hash-based index for O(1) certificate lookups
	certIndex   map[types.Hash]*types.Certificate
	certIndexMu sync.RWMutex

	highestRound   atomic.Uint64
	committedRound atomic.Uint64

	certStore store.CertificateStore
	cfg       Config

	// Cache for causal history results (LRU bounded)
	historyCache     map[types.Hash][]*types.Certificate
	historyCacheKeys []types.Hash // Track insertion order for LRU eviction
	historyCacheMu   sync.RWMutex
}

// New creates a new DAG.
func New(certStore store.CertificateStore, cfg Config) *DAG {
	return &DAG{
		rounds:           make(map[uint64]*RoundData),
		certIndex:        make(map[types.Hash]*types.Certificate),
		certStore:        certStore,
		cfg:              cfg,
		historyCache:     make(map[types.Hash][]*types.Certificate),
		historyCacheKeys: make([]types.Hash, 0),
	}
}

// AddCertificate adds a certificate to the DAG.
// Returns error if certificate is invalid, already exists, or has missing parents.
func (d *DAG) AddCertificate(cert *types.Certificate) error {
	if cert == nil {
		return types.ErrInvalidCertificate
	}

	d.roundsMu.Lock()
	defer d.roundsMu.Unlock()

	round := cert.Round()
	author := cert.Author()
	digest := cert.Digest()

	// Get or create round data
	rd, exists := d.rounds[round]
	if !exists {
		rd = NewRoundData(round)
		d.rounds[round] = rd
	}

	// Check for duplicate
	if rd.HasCertificate(author) {
		return types.ErrDuplicateHeader
	}

	// Validate parent certificates exist (for rounds > 0)
	if round > 0 {
		for _, parentRef := range cert.Header.Parents {
			if !d.hasCertificateLocked(parentRef.Digest) {
				return types.ErrMissingParents
			}
		}
	}

	// Add certificate to round data
	rd.AddCertificate(cert)

	// Add to hash index for O(1) lookups
	d.certIndexMu.Lock()
	d.certIndex[digest] = cert
	d.certIndexMu.Unlock()

	// Update highest round
	if round > d.highestRound.Load() {
		d.highestRound.Store(round)
	}

	// Invalidate only affected history cache entries (smarter than clearing all)
	d.invalidateHistoryCacheForCertificate(cert)

	// Store in persistent storage
	if d.certStore != nil {
		if err := d.certStore.SaveCertificate(cert); err != nil {
			return err
		}
	}

	return nil
}

// hasCertificateLocked checks if a certificate exists (caller must hold roundsMu lock).
func (d *DAG) hasCertificateLocked(digest types.Hash) bool {
	// Check hash index first (O(1))
	d.certIndexMu.RLock()
	_, ok := d.certIndex[digest]
	d.certIndexMu.RUnlock()
	if ok {
		return true
	}

	// Check persistent storage
	if d.certStore != nil {
		return d.certStore.HasCertificate(digest)
	}

	return false
}

// invalidateHistoryCacheForCertificate invalidates only cache entries that could be
// affected by the addition of a new certificate. This is much more efficient than
// clearing the entire cache.
//
// A cache entry for certificate X is affected if X could potentially have newCert
// in its causal history (X's round > newCert's round). Entries for certificates
// at the same or lower round are unaffected since causal history only goes backwards.
func (d *DAG) invalidateHistoryCacheForCertificate(newCert *types.Certificate) {
	if newCert == nil {
		return
	}

	newRound := newCert.Round()

	d.historyCacheMu.Lock()
	defer d.historyCacheMu.Unlock()

	// Collect keys to remove (certificates at higher rounds than the new one)
	var keysToRemove []types.Hash
	for digest, history := range d.historyCache {
		// If cached history is for a cert at higher round, it could be affected
		if len(history) > 0 && history[0].Round() > newRound {
			keysToRemove = append(keysToRemove, digest)
		}
	}

	// Remove affected entries
	for _, digest := range keysToRemove {
		delete(d.historyCache, digest)
	}

	// Update historyCacheKeys to remove deleted entries
	if len(keysToRemove) > 0 {
		removedSet := make(map[types.Hash]bool)
		for _, digest := range keysToRemove {
			removedSet[digest] = true
		}

		newKeys := make([]types.Hash, 0, len(d.historyCacheKeys)-len(keysToRemove))
		for _, key := range d.historyCacheKeys {
			if !removedSet[key] {
				newKeys = append(newKeys, key)
			}
		}
		d.historyCacheKeys = newKeys
	}
}

// invalidateHistoryCacheLocked clears the entire history cache.
// Called when major DAG structure changes occur (like pruning). Caller must hold roundsMu lock.
func (d *DAG) invalidateHistoryCacheLocked() {
	d.historyCacheMu.Lock()
	d.historyCache = make(map[types.Hash][]*types.Certificate)
	d.historyCacheKeys = make([]types.Hash, 0)
	d.historyCacheMu.Unlock()
}

// GetCertificate returns a certificate by digest.
// Uses O(1) hash index for fast lookups.
func (d *DAG) GetCertificate(digest types.Hash) (*types.Certificate, error) {
	// First check hash index (O(1) lookup)
	d.certIndexMu.RLock()
	if cert, ok := d.certIndex[digest]; ok {
		d.certIndexMu.RUnlock()
		return cert.Clone(), nil
	}
	d.certIndexMu.RUnlock()

	// Fall back to persistent storage
	if d.certStore != nil {
		return d.certStore.GetCertificate(digest)
	}

	return nil, types.ErrCertificateNotFound
}

// HasCertificate returns true if the certificate exists.
// Uses O(1) hash index for fast lookups.
func (d *DAG) HasCertificate(digest types.Hash) bool {
	// First check hash index (O(1) lookup)
	d.certIndexMu.RLock()
	_, ok := d.certIndex[digest]
	d.certIndexMu.RUnlock()
	if ok {
		return true
	}

	// Fall back to persistent storage
	if d.certStore != nil {
		return d.certStore.HasCertificate(digest)
	}

	return false
}

// GetRound returns the round data for a specific round.
func (d *DAG) GetRound(round uint64) *RoundData {
	d.roundsMu.RLock()
	defer d.roundsMu.RUnlock()

	return d.rounds[round]
}

// GetCertificatesForRound returns all certificates for a specific round.
func (d *DAG) GetCertificatesForRound(round uint64) []*types.Certificate {
	d.roundsMu.RLock()
	rd := d.rounds[round]
	d.roundsMu.RUnlock()

	if rd == nil {
		// Try persistent storage
		if d.certStore != nil {
			certs, err := d.certStore.GetCertificatesByRound(round)
			if err == nil {
				return certs
			}
		}
		return nil
	}

	// Clone certificates to prevent external modification of internal state
	certs := rd.GetAllCertificates()
	cloned := make([]*types.Certificate, len(certs))
	for i, cert := range certs {
		cloned[i] = cert.Clone()
	}
	return cloned
}

// GetCertificateForValidator returns the certificate from a specific validator at a round.
func (d *DAG) GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, bool) {
	d.roundsMu.RLock()
	rd := d.rounds[round]
	d.roundsMu.RUnlock()

	if rd == nil {
		// Try persistent storage
		if d.certStore != nil {
			cert, err := d.certStore.GetCertificateForValidator(round, validator)
			if err == nil && cert != nil {
				return cert, true
			}
		}
		return nil, false
	}

	cert, ok := rd.GetCertificate(validator)
	if !ok {
		return nil, false
	}
	// Clone certificate to prevent external modification of internal state
	return cert.Clone(), true
}

// HighestRound returns the highest round in the DAG.
func (d *DAG) HighestRound() uint64 {
	return d.highestRound.Load()
}

// CommittedRound returns the committed round.
func (d *DAG) CommittedRound() uint64 {
	return d.committedRound.Load()
}

// SetCommittedRound sets the committed round.
func (d *DAG) SetCommittedRound(round uint64) {
	d.committedRound.Store(round)

	// Mark rounds as committed
	d.roundsMu.Lock()
	for r, rd := range d.rounds {
		if r <= round {
			rd.SetCommitted()
		}
	}
	d.roundsMu.Unlock()
}

// CanAdvanceToRound checks if we can advance to the specified round.
// This requires having quorum certificates in all rounds up to round-1.
func (d *DAG) CanAdvanceToRound(round uint64, quorum int) bool {
	if round == 0 {
		return true
	}

	d.roundsMu.RLock()
	defer d.roundsMu.RUnlock()

	// Check previous round has quorum
	prevRound := d.rounds[round-1]
	if prevRound == nil {
		return false
	}

	return prevRound.Count() >= quorum
}

// CertificateCount returns the total number of certificates at a round.
func (d *DAG) CertificateCount(round uint64) int {
	d.roundsMu.RLock()
	defer d.roundsMu.RUnlock()

	rd := d.rounds[round]
	if rd == nil {
		return 0
	}

	return rd.Count()
}

// CausalHistory returns all certificates in the causal history of the given certificate.
// Uses BFS traversal through parent references. Results are cached with LRU eviction.
func (d *DAG) CausalHistory(cert *types.Certificate) []*types.Certificate {
	if cert == nil {
		return nil
	}

	digest := cert.Digest()

	// Check cache
	d.historyCacheMu.RLock()
	if cached, ok := d.historyCache[digest]; ok {
		d.historyCacheMu.RUnlock()
		return cached
	}
	d.historyCacheMu.RUnlock()

	// BFS traversal
	visited := make(map[types.Hash]bool)
	queue := []*types.Certificate{cert}
	var result []*types.Certificate

	visited[digest] = true

	depth := 0
	for len(queue) > 0 && depth < d.cfg.MaxHistoryDepth {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[0]
			queue = queue[1:]

			result = append(result, current)

			// Add parents to queue
			for _, parentRef := range current.Header.Parents {
				if visited[parentRef.Digest] {
					continue
				}
				visited[parentRef.Digest] = true

				parentCert, err := d.GetCertificate(parentRef.Digest)
				if err == nil && parentCert != nil {
					queue = append(queue, parentCert)
				}
			}
		}
		depth++
	}

	// Cache the result with LRU eviction
	d.historyCacheMu.Lock()
	// Evict oldest entries if cache is full
	if d.cfg.MaxHistoryCacheSize > 0 && len(d.historyCacheKeys) >= d.cfg.MaxHistoryCacheSize {
		// Remove oldest entries (first 10% of cache)
		evictCount := d.cfg.MaxHistoryCacheSize / 10
		if evictCount < 1 {
			evictCount = 1
		}
		for i := 0; i < evictCount && len(d.historyCacheKeys) > 0; i++ {
			oldKey := d.historyCacheKeys[0]
			d.historyCacheKeys = d.historyCacheKeys[1:]
			delete(d.historyCache, oldKey)
		}
	}
	d.historyCache[digest] = result
	d.historyCacheKeys = append(d.historyCacheKeys, digest)
	d.historyCacheMu.Unlock()

	return result
}

// GetOrderedCertificates returns certificates from fromRound to toRound in deterministic order.
// Within each round, certificates are ordered by validator index.
func (d *DAG) GetOrderedCertificates(fromRound, toRound uint64) []*types.Certificate {
	if fromRound > toRound {
		return nil
	}

	var result []*types.Certificate

	for round := fromRound; round <= toRound; round++ {
		certs := d.GetCertificatesForRound(round)
		if len(certs) == 0 {
			continue
		}

		// Sort by validator index for determinism
		sort.Slice(certs, func(i, j int) bool {
			return certs[i].Author() < certs[j].Author()
		})

		result = append(result, certs...)
	}

	return result
}

// PruneRound removes a round from the in-memory cache.
// Does not delete from persistent storage.
func (d *DAG) PruneRound(round uint64) {
	d.roundsMu.Lock()
	rd := d.rounds[round]
	if rd != nil {
		// Remove from hash index
		d.certIndexMu.Lock()
		for _, cert := range rd.Certificates {
			delete(d.certIndex, cert.Digest())
		}
		d.certIndexMu.Unlock()
	}
	delete(d.rounds, round)
	d.roundsMu.Unlock()

	// Clear related history cache entries
	d.historyCacheMu.Lock()
	d.historyCache = make(map[types.Hash][]*types.Certificate)
	d.historyCacheKeys = make([]types.Hash, 0)
	d.historyCacheMu.Unlock()
}

// PruneRoundsBefore removes all rounds before the specified round from memory.
func (d *DAG) PruneRoundsBefore(round uint64) {
	d.roundsMu.Lock()
	d.certIndexMu.Lock()
	for r, rd := range d.rounds {
		if r < round {
			// Remove certificates from hash index
			for _, cert := range rd.Certificates {
				delete(d.certIndex, cert.Digest())
			}
			delete(d.rounds, r)
		}
	}
	d.certIndexMu.Unlock()
	d.roundsMu.Unlock()

	// Clear history cache
	d.historyCacheMu.Lock()
	d.historyCache = make(map[types.Hash][]*types.Certificate)
	d.historyCacheKeys = make([]types.Hash, 0)
	d.historyCacheMu.Unlock()
}

// RoundCount returns the number of rounds currently in memory.
func (d *DAG) RoundCount() int {
	d.roundsMu.RLock()
	defer d.roundsMu.RUnlock()

	return len(d.rounds)
}

// LoadRound loads a round from persistent storage into memory.
func (d *DAG) LoadRound(round uint64) error {
	if d.certStore == nil {
		return nil
	}

	certs, err := d.certStore.GetCertificatesByRound(round)
	if err != nil {
		return err
	}

	d.roundsMu.Lock()
	defer d.roundsMu.Unlock()

	rd := NewRoundData(round)
	d.certIndexMu.Lock()
	for _, cert := range certs {
		rd.AddCertificate(cert)
		// Add to hash index
		d.certIndex[cert.Digest()] = cert
	}
	d.certIndexMu.Unlock()
	d.rounds[round] = rd

	if round > d.highestRound.Load() {
		d.highestRound.Store(round)
	}

	return nil
}

// LoadRoundsFrom loads all rounds from the specified round from persistent storage.
func (d *DAG) LoadRoundsFrom(fromRound uint64) error {
	if d.certStore == nil {
		return nil
	}

	highestRound := d.certStore.HighestRound()
	for round := fromRound; round <= highestRound; round++ {
		if err := d.LoadRound(round); err != nil {
			return err
		}
	}

	return nil
}
