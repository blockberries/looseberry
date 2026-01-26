package store

import (
	"sync"

	"github.com/blockberries/looseberry/types"
)

// MemoryCertificateStore is an in-memory implementation of CertificateStore.
// Thread-safe with RWMutex. Suitable for testing.
type MemoryCertificateStore struct {
	certs        map[types.Hash]*types.Certificate
	byRound      map[uint64][]types.Hash               // round -> cert digests
	byValidator  map[uint64]map[uint16]types.Hash      // round -> validator -> cert digest
	mu           sync.RWMutex
	closed       bool
}

// NewMemoryCertificateStore creates a new in-memory certificate store.
func NewMemoryCertificateStore() *MemoryCertificateStore {
	return &MemoryCertificateStore{
		certs:       make(map[types.Hash]*types.Certificate),
		byRound:     make(map[uint64][]types.Hash),
		byValidator: make(map[uint64]map[uint16]types.Hash),
	}
}

// SaveCertificate stores a certificate.
func (s *MemoryCertificateStore) SaveCertificate(cert *types.Certificate) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	digest := cert.Digest()

	// Check if already exists
	if _, exists := s.certs[digest]; exists {
		return nil // Idempotent
	}

	// Store certificate
	s.certs[digest] = cert.Clone()

	// Index by round
	round := cert.Round()
	s.byRound[round] = append(s.byRound[round], digest)

	// Index by validator
	if s.byValidator[round] == nil {
		s.byValidator[round] = make(map[uint16]types.Hash)
	}
	s.byValidator[round][cert.Author()] = digest

	return nil
}

// GetCertificate retrieves a certificate by digest.
func (s *MemoryCertificateStore) GetCertificate(digest types.Hash) (*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	cert, exists := s.certs[digest]
	if !exists {
		return nil, types.ErrCertificateNotFound
	}

	return cert.Clone(), nil
}

// HasCertificate returns true if the certificate exists.
func (s *MemoryCertificateStore) HasCertificate(digest types.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	_, exists := s.certs[digest]
	return exists
}

// GetCertificatesByRound retrieves all certificates for a round.
func (s *MemoryCertificateStore) GetCertificatesByRound(round uint64) ([]*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	digests, exists := s.byRound[round]
	if !exists {
		return nil, nil
	}

	certs := make([]*types.Certificate, 0, len(digests))
	for _, digest := range digests {
		if cert, ok := s.certs[digest]; ok {
			certs = append(certs, cert.Clone())
		}
	}

	return certs, nil
}

// GetCertificateForValidator retrieves the certificate from a validator for a round.
func (s *MemoryCertificateStore) GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	validatorMap, exists := s.byValidator[round]
	if !exists {
		return nil, types.ErrCertificateNotFound
	}

	digest, exists := validatorMap[validator]
	if !exists {
		return nil, types.ErrCertificateNotFound
	}

	cert, exists := s.certs[digest]
	if !exists {
		return nil, types.ErrCertificateNotFound
	}

	return cert.Clone(), nil
}

// DeleteCertificatesBefore deletes all certificates before the given round.
func (s *MemoryCertificateStore) DeleteCertificatesBefore(round uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Find rounds to delete
	var roundsToDelete []uint64
	for r := range s.byRound {
		if r < round {
			roundsToDelete = append(roundsToDelete, r)
		}
	}

	// Delete certificates and indices
	for _, r := range roundsToDelete {
		for _, digest := range s.byRound[r] {
			delete(s.certs, digest)
		}
		delete(s.byRound, r)
		delete(s.byValidator, r)
	}

	return nil
}

// Close closes the store.
func (s *MemoryCertificateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.certs = nil
	s.byRound = nil
	s.byValidator = nil

	return nil
}

// Len returns the number of stored certificates (for testing).
func (s *MemoryCertificateStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.certs)
}

// Verify interface compliance
var _ CertificateStore = (*MemoryCertificateStore)(nil)
