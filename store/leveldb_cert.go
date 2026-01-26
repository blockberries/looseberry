package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/blockberries/looseberry/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Key prefixes for LevelDB certificate store.
var (
	certPrefix           = []byte("C:")  // C:{digest} -> certificate data
	certRoundPrefix      = []byte("CR:") // CR:{round}:{validator} -> digest (index)
	certHighestKey       = []byte("CM:highest_round")
)

// LevelDBCertificateStore is a persistent implementation of CertificateStore using LevelDB.
type LevelDBCertificateStore struct {
	db     *leveldb.DB
	mu     sync.RWMutex
	closed bool
}

// NewLevelDBCertificateStore creates a new LevelDB certificate store.
func NewLevelDBCertificateStore(path string) (*LevelDBCertificateStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	return &LevelDBCertificateStore{
		db: db,
	}, nil
}

// SaveCertificate stores a certificate.
func (s *LevelDBCertificateStore) SaveCertificate(cert *types.Certificate) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	digest := cert.Digest()

	// Check if already exists
	key := makeCertKey(digest)
	has, err := s.db.Has(key, nil)
	if err != nil {
		return fmt.Errorf("failed to check certificate existence: %w", err)
	}
	if has {
		return nil // Idempotent
	}

	// Encode certificate
	data, err := encodeCertificate(cert)
	if err != nil {
		return fmt.Errorf("failed to encode certificate: %w", err)
	}

	// Create write batch for atomicity
	wb := new(leveldb.Batch)

	// Store certificate data
	wb.Put(key, data)

	// Store round:validator index
	indexKey := makeCertRoundValidatorKey(cert.Round(), cert.Author())
	wb.Put(indexKey, digest[:])

	// Update highest round if needed
	currentHighest := s.getHighestRound()
	if cert.Round() > currentHighest {
		roundBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(roundBytes, cert.Round())
		wb.Put(certHighestKey, roundBytes)
	}

	// Write atomically
	if err := s.db.Write(wb, nil); err != nil {
		return fmt.Errorf("failed to write certificate: %w", err)
	}

	return nil
}

// GetCertificate retrieves a certificate by digest.
func (s *LevelDBCertificateStore) GetCertificate(digest types.Hash) (*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	key := makeCertKey(digest)
	data, err := s.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, types.ErrCertificateNotFound
		}
		return nil, fmt.Errorf("failed to get certificate: %w", err)
	}

	cert, err := decodeCertificate(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode certificate: %w", err)
	}

	return cert.Clone(), nil
}

// HasCertificate returns true if the certificate exists.
func (s *LevelDBCertificateStore) HasCertificate(digest types.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	key := makeCertKey(digest)
	has, _ := s.db.Has(key, nil)
	return has
}

// GetCertificatesByRound retrieves all certificates for a round.
func (s *LevelDBCertificateStore) GetCertificatesByRound(round uint64) ([]*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	// Create prefix for round
	prefix := makeCertRoundPrefix(round)

	var certs []*types.Certificate
	iter := s.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		// Value is the digest
		digestBytes := iter.Value()
		if len(digestBytes) != types.HashSize {
			continue
		}

		var digest types.Hash
		copy(digest[:], digestBytes)

		// Get certificate data
		cert, err := s.getCertificateInternal(digest)
		if err != nil {
			continue
		}

		certs = append(certs, cert.Clone())
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iteration error: %w", err)
	}

	return certs, nil
}

// GetCertificateForValidator retrieves the certificate from a validator for a round.
func (s *LevelDBCertificateStore) GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrNotRunning
	}

	indexKey := makeCertRoundValidatorKey(round, validator)
	digestBytes, err := s.db.Get(indexKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, types.ErrCertificateNotFound
		}
		return nil, fmt.Errorf("failed to get certificate index: %w", err)
	}

	if len(digestBytes) != types.HashSize {
		return nil, types.ErrCertificateNotFound
	}

	var digest types.Hash
	copy(digest[:], digestBytes)

	cert, err := s.getCertificateInternal(digest)
	if err != nil {
		return nil, err
	}

	return cert.Clone(), nil
}

// DeleteCertificatesBefore deletes all certificates before the given round.
func (s *LevelDBCertificateStore) DeleteCertificatesBefore(round uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrNotRunning
	}

	// Find all certificates to delete by iterating round indices
	var toDelete []struct {
		certKey  []byte
		indexKey []byte
	}

	iter := s.db.NewIterator(util.BytesPrefix(certRoundPrefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		r := parseRoundFromCertKey(key)

		if r < round {
			digestBytes := iter.Value()
			if len(digestBytes) == types.HashSize {
				var digest types.Hash
				copy(digest[:], digestBytes)
				toDelete = append(toDelete, struct {
					certKey  []byte
					indexKey []byte
				}{
					certKey:  makeCertKey(digest),
					indexKey: append([]byte{}, key...), // Copy key
				})
			}
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iteration error: %w", err)
	}

	// Delete in batch
	if len(toDelete) > 0 {
		wb := new(leveldb.Batch)
		for _, item := range toDelete {
			wb.Delete(item.certKey)
			wb.Delete(item.indexKey)
		}

		if err := s.db.Write(wb, nil); err != nil {
			return fmt.Errorf("failed to delete certificates: %w", err)
		}
	}

	return nil
}

// Close closes the store.
func (s *LevelDBCertificateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.db.Close()
}

// Len returns the number of stored certificates (for testing).
func (s *LevelDBCertificateStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0
	}

	count := 0
	iter := s.db.NewIterator(util.BytesPrefix(certPrefix), nil)
	defer iter.Release()

	for iter.Next() {
		count++
	}

	return count
}

// HighestRound returns the highest round with stored certificates.
func (s *LevelDBCertificateStore) HighestRound() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getHighestRound()
}

// Internal helpers

func (s *LevelDBCertificateStore) getHighestRound() uint64 {
	data, err := s.db.Get(certHighestKey, nil)
	if err != nil {
		return 0
	}
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func (s *LevelDBCertificateStore) getCertificateInternal(digest types.Hash) (*types.Certificate, error) {
	key := makeCertKey(digest)
	data, err := s.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, types.ErrCertificateNotFound
		}
		return nil, err
	}

	return decodeCertificate(data)
}

// Key construction helpers

func makeCertKey(digest types.Hash) []byte {
	key := make([]byte, len(certPrefix)+types.HashSize)
	copy(key, certPrefix)
	copy(key[len(certPrefix):], digest[:])
	return key
}

func makeCertRoundPrefix(round uint64) []byte {
	prefix := make([]byte, len(certRoundPrefix)+8)
	copy(prefix, certRoundPrefix)
	binary.BigEndian.PutUint64(prefix[len(certRoundPrefix):], round)
	return prefix
}

func makeCertRoundValidatorKey(round uint64, validator uint16) []byte {
	key := make([]byte, len(certRoundPrefix)+8+2)
	copy(key, certRoundPrefix)
	binary.BigEndian.PutUint64(key[len(certRoundPrefix):], round)
	binary.BigEndian.PutUint16(key[len(certRoundPrefix)+8:], validator)
	return key
}

func parseRoundFromCertKey(key []byte) uint64 {
	prefixLen := len(certRoundPrefix)
	if len(key) < prefixLen+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[prefixLen : prefixLen+8])
}

// Encoding helpers

func encodeCertificate(cert *types.Certificate) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cert); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCertificate(data []byte) (*types.Certificate, error) {
	var cert types.Certificate
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&cert); err != nil {
		return nil, err
	}
	return &cert, nil
}

// Verify interface compliance
var _ CertificateStore = (*LevelDBCertificateStore)(nil)
