package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// HashSize is the size of a SHA-256 hash in bytes.
const HashSize = 32

// Hash represents a 32-byte SHA-256 hash.
type Hash [HashSize]byte

// EmptyHash is the zero-value hash.
var EmptyHash Hash

// Bytes returns the hash as a byte slice.
func (h Hash) Bytes() []byte {
	return h[:]
}

// String returns the hex-encoded string representation of the hash.
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// Equal returns true if h equals other.
func (h Hash) Equal(other Hash) bool {
	return h == other
}

// IsEmpty returns true if the hash is the zero value.
func (h Hash) IsEmpty() bool {
	return h == EmptyHash
}

// MarshalText implements encoding.TextMarshaler.
func (h Hash) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (h *Hash) UnmarshalText(text []byte) error {
	decoded, err := hex.DecodeString(string(text))
	if err != nil {
		return fmt.Errorf("invalid hash hex: %w", err)
	}
	if len(decoded) != HashSize {
		return fmt.Errorf("invalid hash length: expected %d, got %d", HashSize, len(decoded))
	}
	copy(h[:], decoded)
	return nil
}

// HashBytes computes the SHA-256 hash of the given data.
func HashBytes(data []byte) Hash {
	return sha256.Sum256(data)
}

// HashConcat computes the hash of the concatenation of left and right.
// Useful for building Merkle trees.
func HashConcat(left, right Hash) Hash {
	combined := make([]byte, HashSize*2)
	copy(combined[:HashSize], left[:])
	copy(combined[HashSize:], right[:])
	return sha256.Sum256(combined)
}

// HashFromBytes creates a Hash from a byte slice.
// Returns an error if the slice is not exactly HashSize bytes.
func HashFromBytes(b []byte) (Hash, error) {
	if len(b) != HashSize {
		return EmptyHash, fmt.Errorf("invalid hash length: expected %d, got %d", HashSize, len(b))
	}
	var h Hash
	copy(h[:], b)
	return h, nil
}

// HashFromHex creates a Hash from a hex-encoded string.
func HashFromHex(s string) (Hash, error) {
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return EmptyHash, fmt.Errorf("invalid hash hex: %w", err)
	}
	return HashFromBytes(decoded)
}
