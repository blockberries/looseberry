package types

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
)

// SignatureSize is the size of an Ed25519 signature.
const SignatureSize = ed25519.SignatureSize

// PublicKeySize is the size of an Ed25519 public key.
const PublicKeySize = ed25519.PublicKeySize

// Signature represents an Ed25519 signature.
type Signature [SignatureSize]byte

// EmptySignature is the zero-value signature.
var EmptySignature Signature

// Bytes returns the signature as a byte slice.
func (s Signature) Bytes() []byte {
	return s[:]
}

// String returns the hex-encoded string representation.
func (s Signature) String() string {
	return hex.EncodeToString(s[:])
}

// IsEmpty returns true if the signature is the zero value.
func (s Signature) IsEmpty() bool {
	return s == EmptySignature
}

// Equal returns true if two signatures are equal.
func (s Signature) Equal(other Signature) bool {
	return s == other
}

// SignatureFromBytes creates a Signature from a byte slice.
func SignatureFromBytes(b []byte) (Signature, error) {
	if len(b) != SignatureSize {
		return EmptySignature, fmt.Errorf("invalid signature length: expected %d, got %d", SignatureSize, len(b))
	}
	var sig Signature
	copy(sig[:], b)
	return sig, nil
}

// PublicKey represents an Ed25519 public key.
type PublicKey [PublicKeySize]byte

// EmptyPublicKey is the zero-value public key.
var EmptyPublicKey PublicKey

// Bytes returns the public key as a byte slice.
func (pk PublicKey) Bytes() []byte {
	return pk[:]
}

// String returns the hex-encoded string representation.
func (pk PublicKey) String() string {
	return hex.EncodeToString(pk[:])
}

// IsEmpty returns true if the public key is the zero value.
func (pk PublicKey) IsEmpty() bool {
	return pk == EmptyPublicKey
}

// Verify verifies a signature against a message digest.
func (pk PublicKey) Verify(digest Hash, sig Signature) bool {
	return ed25519.Verify(pk[:], digest[:], sig[:])
}

// PublicKeyFromBytes creates a PublicKey from a byte slice.
func PublicKeyFromBytes(b []byte) (PublicKey, error) {
	if len(b) != PublicKeySize {
		return EmptyPublicKey, fmt.Errorf("invalid public key length: expected %d, got %d", PublicKeySize, len(b))
	}
	var pk PublicKey
	copy(pk[:], b)
	return pk, nil
}

// ToEd25519 converts to ed25519.PublicKey.
func (pk PublicKey) ToEd25519() ed25519.PublicKey {
	return pk[:]
}
