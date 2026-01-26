package types

// Transaction represents a raw transaction as a byte slice.
// The transaction format is opaque to Looseberry - validation
// is delegated to the application via TxValidator.
type Transaction []byte

// Hash returns the SHA-256 hash of the transaction.
func (tx Transaction) Hash() Hash {
	return HashBytes(tx)
}

// Size returns the size of the transaction in bytes.
func (tx Transaction) Size() int {
	return len(tx)
}

// Bytes returns the raw transaction bytes.
func (tx Transaction) Bytes() []byte {
	return tx
}

// IsEmpty returns true if the transaction is empty.
func (tx Transaction) IsEmpty() bool {
	return len(tx) == 0
}

// Clone returns a copy of the transaction.
func (tx Transaction) Clone() Transaction {
	if tx == nil {
		return nil
	}
	clone := make(Transaction, len(tx))
	copy(clone, tx)
	return clone
}

// Equal returns true if tx equals other.
func (tx Transaction) Equal(other Transaction) bool {
	if len(tx) != len(other) {
		return false
	}
	for i := range tx {
		if tx[i] != other[i] {
			return false
		}
	}
	return true
}
