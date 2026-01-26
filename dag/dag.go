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
}

// DefaultConfig returns default DAG configuration.
func DefaultConfig() Config {
	return Config{
		MaxCachedRounds: 100,
		MaxHistoryDepth: 1000,
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

	highestRound   atomic.Uint64
	committedRound atomic.Uint64

	certStore store.CertificateStore
	cfg       Config

	// Cache for causal history results
	historyCache   map[types.Hash][]*types.Certificate
	historyCacheMu sync.RWMutex
}

// New creates a new DAG.
func New(certStore store.CertificateStore, cfg Config) *DAG {
	return &DAG{
		rounds:       make(map[uint64]*RoundData),
		certStore:    certStore,
		cfg:          cfg,
		historyCache: make(map[types.Hash][]*types.Certificate),
	}
}

// AddCertificate adds a certificate to the DAG.
// Returns error if certificate is invalid or already exists.
func (d *DAG) AddCertificate(cert *types.Certificate) error {
	if cert == nil {
		return types.ErrInvalidCertificate
	}

	d.roundsMu.Lock()
	defer d.roundsMu.Unlock()

	round := cert.Round()
	author := cert.Author()

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

	// Add certificate
	rd.AddCertificate(cert)

	// Update highest round
	if round > d.highestRound.Load() {
		d.highestRound.Store(round)
	}

	// Store in persistent storage
	if d.certStore != nil {
		if err := d.certStore.SaveCertificate(cert); err != nil {
			return err
		}
	}

	return nil
}

// GetCertificate returns a certificate by digest.
func (d *DAG) GetCertificate(digest types.Hash) (*types.Certificate, error) {
	// First check memory cache
	d.roundsMu.RLock()
	for _, rd := range d.rounds {
		for _, cert := range rd.Certificates {
			if cert.Digest().Equal(digest) {
				d.roundsMu.RUnlock()
				return cert.Clone(), nil
			}
		}
	}
	d.roundsMu.RUnlock()

	// Fall back to persistent storage
	if d.certStore != nil {
		return d.certStore.GetCertificate(digest)
	}

	return nil, types.ErrCertificateNotFound
}

// HasCertificate returns true if the certificate exists.
func (d *DAG) HasCertificate(digest types.Hash) bool {
	d.roundsMu.RLock()
	for _, rd := range d.rounds {
		for _, cert := range rd.Certificates {
			if cert.Digest().Equal(digest) {
				d.roundsMu.RUnlock()
				return true
			}
		}
	}
	d.roundsMu.RUnlock()

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

	return rd.GetAllCertificates()
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

	return rd.GetCertificate(validator)
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
// Uses BFS traversal through parent references.
func (d *DAG) CausalHistory(cert *types.Certificate) []*types.Certificate {
	if cert == nil {
		return nil
	}

	// Check cache
	d.historyCacheMu.RLock()
	if cached, ok := d.historyCache[cert.Digest()]; ok {
		d.historyCacheMu.RUnlock()
		return cached
	}
	d.historyCacheMu.RUnlock()

	// BFS traversal
	visited := make(map[types.Hash]bool)
	queue := []*types.Certificate{cert}
	var result []*types.Certificate

	visited[cert.Digest()] = true

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

	// Cache the result
	d.historyCacheMu.Lock()
	d.historyCache[cert.Digest()] = result
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
	delete(d.rounds, round)
	d.roundsMu.Unlock()

	// Clear related history cache entries
	d.historyCacheMu.Lock()
	for digest := range d.historyCache {
		// Clear cache - will be rebuilt on next access
		delete(d.historyCache, digest)
	}
	d.historyCacheMu.Unlock()
}

// PruneRoundsBefore removes all rounds before the specified round from memory.
func (d *DAG) PruneRoundsBefore(round uint64) {
	d.roundsMu.Lock()
	for r := range d.rounds {
		if r < round {
			delete(d.rounds, r)
		}
	}
	d.roundsMu.Unlock()

	// Clear history cache
	d.historyCacheMu.Lock()
	d.historyCache = make(map[types.Hash][]*types.Certificate)
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
	for _, cert := range certs {
		rd.AddCertificate(cert)
	}
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
