package types

import (
	"testing"
)

func TestTransactionHash(t *testing.T) {
	tx := Transaction([]byte("test transaction"))
	hash := tx.Hash()

	// Hash should be deterministic
	if !hash.Equal(tx.Hash()) {
		t.Error("Transaction hash should be deterministic")
	}

	// Different transactions should have different hashes
	tx2 := Transaction([]byte("different transaction"))
	if hash.Equal(tx2.Hash()) {
		t.Error("Different transactions should have different hashes")
	}
}

func TestTransactionSize(t *testing.T) {
	tx := Transaction([]byte("hello"))
	if tx.Size() != 5 {
		t.Errorf("Transaction size mismatch: expected 5, got %d", tx.Size())
	}

	empty := Transaction(nil)
	if empty.Size() != 0 {
		t.Errorf("Empty transaction size should be 0, got %d", empty.Size())
	}
}

func TestTransactionIsEmpty(t *testing.T) {
	empty := Transaction(nil)
	if !empty.IsEmpty() {
		t.Error("Nil transaction should be empty")
	}

	empty2 := Transaction([]byte{})
	if !empty2.IsEmpty() {
		t.Error("Empty slice transaction should be empty")
	}

	nonEmpty := Transaction([]byte("data"))
	if nonEmpty.IsEmpty() {
		t.Error("Non-empty transaction should not be empty")
	}
}

func TestTransactionClone(t *testing.T) {
	original := Transaction([]byte("original"))
	clone := original.Clone()

	if !original.Equal(clone) {
		t.Error("Clone should equal original")
	}

	// Modify clone should not affect original
	clone[0] = 'X'
	if original[0] == 'X' {
		t.Error("Modifying clone should not affect original")
	}

	// Clone of nil should be nil
	var nilTx Transaction
	if nilTx.Clone() != nil {
		t.Error("Clone of nil should be nil")
	}
}

func TestTransactionEqual(t *testing.T) {
	tx1 := Transaction([]byte("same"))
	tx2 := Transaction([]byte("same"))
	tx3 := Transaction([]byte("different"))
	tx4 := Transaction([]byte("sam"))

	if !tx1.Equal(tx2) {
		t.Error("Equal transactions should be equal")
	}

	if tx1.Equal(tx3) {
		t.Error("Different transactions should not be equal")
	}

	if tx1.Equal(tx4) {
		t.Error("Transactions of different lengths should not be equal")
	}
}

func TestTransactionBytes(t *testing.T) {
	data := []byte("test data")
	tx := Transaction(data)

	if string(tx.Bytes()) != string(data) {
		t.Error("Bytes() should return the raw transaction data")
	}
}
