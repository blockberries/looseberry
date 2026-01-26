package store

import (
	"github.com/blockberries/looseberry/types"
)

// BatchStore provides storage for transaction batches.
type BatchStore interface {
	// SaveBatch stores a batch.
	SaveBatch(batch *types.Batch) error

	// GetBatch retrieves a batch by digest.
	GetBatch(digest types.Hash) (*types.Batch, error)

	// HasBatch returns true if the batch exists.
	HasBatch(digest types.Hash) bool

	// GetBatchesByRound retrieves all batches for a round.
	GetBatchesByRound(round uint64) ([]*types.Batch, error)

	// DeleteBatchesBefore deletes all batches before the given round.
	DeleteBatchesBefore(round uint64) error

	// Close closes the store.
	Close() error
}

// CertificateStore provides storage for certificates.
type CertificateStore interface {
	// SaveCertificate stores a certificate.
	SaveCertificate(cert *types.Certificate) error

	// GetCertificate retrieves a certificate by digest.
	GetCertificate(digest types.Hash) (*types.Certificate, error)

	// HasCertificate returns true if the certificate exists.
	HasCertificate(digest types.Hash) bool

	// GetCertificatesByRound retrieves all certificates for a round.
	GetCertificatesByRound(round uint64) ([]*types.Certificate, error)

	// GetCertificateForValidator retrieves the certificate from a validator for a round.
	GetCertificateForValidator(round uint64, validator uint16) (*types.Certificate, error)

	// DeleteCertificatesBefore deletes all certificates before the given round.
	DeleteCertificatesBefore(round uint64) error

	// HighestRound returns the highest round stored.
	HighestRound() uint64

	// Close closes the store.
	Close() error
}

// TxIndex provides fast transaction lookup.
type TxIndex interface {
	// AddTx adds a transaction hash to batch mapping.
	AddTx(txHash, batchHash types.Hash) error

	// AddBatch indexes all transactions in a batch.
	AddBatch(batch *types.Batch) error

	// GetBatchForTx returns the batch hash containing the transaction.
	GetBatchForTx(txHash types.Hash) (types.Hash, error)

	// HasTx returns true if the transaction exists.
	HasTx(txHash types.Hash) bool

	// RemoveTxsForBatch removes all transaction mappings for a batch.
	RemoveTxsForBatch(batchHash types.Hash) error

	// Close closes the index.
	Close() error
}
