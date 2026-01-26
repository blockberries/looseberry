package types

import (
	"crypto/ed25519"
	"fmt"
)

// Signer is the interface for signing digests.
type Signer interface {
	// Sign signs the given digest and returns the signature.
	Sign(digest Hash) (Signature, error)

	// PublicKey returns the signer's public key.
	PublicKey() PublicKey

	// ValidatorIndex returns the signer's validator index.
	ValidatorIndex() uint16
}

// Ed25519Signer implements Signer using Ed25519.
type Ed25519Signer struct {
	privateKey     ed25519.PrivateKey
	publicKey      PublicKey
	validatorIndex uint16
}

// NewEd25519Signer creates a new Ed25519 signer.
func NewEd25519Signer(privateKey ed25519.PrivateKey, validatorIndex uint16) (*Ed25519Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: expected %d, got %d", ed25519.PrivateKeySize, len(privateKey))
	}

	// Extract public key
	pub := privateKey.Public().(ed25519.PublicKey)
	var publicKey PublicKey
	copy(publicKey[:], pub)

	return &Ed25519Signer{
		privateKey:     privateKey,
		publicKey:      publicKey,
		validatorIndex: validatorIndex,
	}, nil
}

// Sign signs the given digest.
func (s *Ed25519Signer) Sign(digest Hash) (Signature, error) {
	sigBytes := ed25519.Sign(s.privateKey, digest[:])
	var sig Signature
	copy(sig[:], sigBytes)
	return sig, nil
}

// PublicKey returns the signer's public key.
func (s *Ed25519Signer) PublicKey() PublicKey {
	return s.publicKey
}

// ValidatorIndex returns the signer's validator index.
func (s *Ed25519Signer) ValidatorIndex() uint16 {
	return s.validatorIndex
}

// GenerateEd25519Signer generates a new Ed25519 signer with a random key.
func GenerateEd25519Signer(validatorIndex uint16) (*Ed25519Signer, error) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("generating key: %w", err)
	}
	return NewEd25519Signer(privateKey, validatorIndex)
}
