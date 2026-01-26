package types

import (
	"crypto/ed25519"
	"testing"
)

func TestNewEd25519Signer(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	signer, err := NewEd25519Signer(privateKey, 5)
	if err != nil {
		t.Fatalf("NewEd25519Signer failed: %v", err)
	}

	if signer.ValidatorIndex() != 5 {
		t.Errorf("ValidatorIndex mismatch: expected 5, got %d", signer.ValidatorIndex())
	}

	if signer.PublicKey().IsEmpty() {
		t.Error("PublicKey should not be empty")
	}
}

func TestNewEd25519SignerInvalidKey(t *testing.T) {
	// Wrong size key
	_, err := NewEd25519Signer([]byte("too short"), 0)
	if err == nil {
		t.Error("Expected error for invalid key size")
	}
}

func TestEd25519SignerSign(t *testing.T) {
	signer, err := GenerateEd25519Signer(0)
	if err != nil {
		t.Fatalf("GenerateEd25519Signer failed: %v", err)
	}

	digest := HashBytes([]byte("test message"))
	sig, err := signer.Sign(digest)
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	if sig.IsEmpty() {
		t.Error("Signature should not be empty")
	}

	// Verify signature
	if !signer.PublicKey().Verify(digest, sig) {
		t.Error("Signature should verify")
	}

	// Different digest should not verify
	wrongDigest := HashBytes([]byte("wrong message"))
	if signer.PublicKey().Verify(wrongDigest, sig) {
		t.Error("Signature should not verify for wrong digest")
	}
}

func TestGenerateEd25519Signer(t *testing.T) {
	signer1, err := GenerateEd25519Signer(1)
	if err != nil {
		t.Fatalf("GenerateEd25519Signer failed: %v", err)
	}

	signer2, err := GenerateEd25519Signer(2)
	if err != nil {
		t.Fatalf("GenerateEd25519Signer failed: %v", err)
	}

	// Different signers should have different public keys
	if signer1.PublicKey() == signer2.PublicKey() {
		t.Error("Different signers should have different public keys")
	}

	// ValidatorIndex should be set correctly
	if signer1.ValidatorIndex() != 1 {
		t.Errorf("ValidatorIndex mismatch: expected 1, got %d", signer1.ValidatorIndex())
	}
	if signer2.ValidatorIndex() != 2 {
		t.Errorf("ValidatorIndex mismatch: expected 2, got %d", signer2.ValidatorIndex())
	}
}

func TestSignatureFromBytes(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	digest := HashBytes([]byte("test"))
	sig, _ := signer.Sign(digest)

	// Round trip
	recovered, err := SignatureFromBytes(sig.Bytes())
	if err != nil {
		t.Fatalf("SignatureFromBytes failed: %v", err)
	}

	if sig != recovered {
		t.Error("Recovered signature should match original")
	}

	// Invalid length
	_, err = SignatureFromBytes([]byte("too short"))
	if err == nil {
		t.Error("Expected error for invalid signature length")
	}
}

func TestPublicKeyFromBytes(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	pk := signer.PublicKey()

	// Round trip
	recovered, err := PublicKeyFromBytes(pk.Bytes())
	if err != nil {
		t.Fatalf("PublicKeyFromBytes failed: %v", err)
	}

	if pk != recovered {
		t.Error("Recovered public key should match original")
	}

	// Invalid length
	_, err = PublicKeyFromBytes([]byte("too short"))
	if err == nil {
		t.Error("Expected error for invalid public key length")
	}
}

func TestPublicKeyToEd25519(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	pk := signer.PublicKey()

	ed25519PK := pk.ToEd25519()
	if len(ed25519PK) != ed25519.PublicKeySize {
		t.Errorf("Ed25519 public key has wrong size: expected %d, got %d",
			ed25519.PublicKeySize, len(ed25519PK))
	}
}
