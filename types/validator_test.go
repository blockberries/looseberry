package types

import (
	"testing"
)

func TestSimpleValidatorSet(t *testing.T) {
	signers := make([]*Ed25519Signer, 4)
	validators := make([]*Validator, 4)

	for i := 0; i < 4; i++ {
		signer, _ := GenerateEd25519Signer(uint16(i))
		signers[i] = signer
		validators[i] = &Validator{
			Index:     uint16(i),
			PublicKey: signer.PublicKey(),
			Power:     int64(i + 1),
			Address:   "test",
		}
	}

	vs := NewSimpleValidatorSet(validators, 5)

	if vs.Count() != 4 {
		t.Errorf("Count mismatch: expected 4, got %d", vs.Count())
	}

	if vs.Epoch() != 5 {
		t.Errorf("Epoch mismatch: expected 5, got %d", vs.Epoch())
	}

	// f = (4-1)/3 = 1
	if vs.F() != 1 {
		t.Errorf("F mismatch: expected 1, got %d", vs.F())
	}

	// quorum = 2f+1 = 3
	if vs.Quorum() != 3 {
		t.Errorf("Quorum mismatch: expected 3, got %d", vs.Quorum())
	}
}

func TestSimpleValidatorSetGetByIndex(t *testing.T) {
	validators := []*Validator{
		{Index: 0, Power: 1},
		{Index: 1, Power: 2},
		{Index: 2, Power: 3},
	}

	vs := NewSimpleValidatorSet(validators, 1)

	v := vs.GetByIndex(1)
	if v == nil {
		t.Fatal("GetByIndex(1) should return validator")
	}
	if v.Power != 2 {
		t.Errorf("Power mismatch: expected 2, got %d", v.Power)
	}

	// Non-existent index
	v = vs.GetByIndex(99)
	if v != nil {
		t.Error("GetByIndex(99) should return nil")
	}
}

func TestSimpleValidatorSetContains(t *testing.T) {
	validators := []*Validator{
		{Index: 0},
		{Index: 1},
		{Index: 2},
	}

	vs := NewSimpleValidatorSet(validators, 1)

	if !vs.Contains(0) {
		t.Error("Should contain index 0")
	}
	if !vs.Contains(1) {
		t.Error("Should contain index 1")
	}
	if !vs.Contains(2) {
		t.Error("Should contain index 2")
	}
	if vs.Contains(3) {
		t.Error("Should not contain index 3")
	}
}

func TestSimpleValidatorSetVerifySignature(t *testing.T) {
	signer, _ := GenerateEd25519Signer(0)
	validators := []*Validator{
		{Index: 0, PublicKey: signer.PublicKey()},
	}

	vs := NewSimpleValidatorSet(validators, 1)

	digest := HashBytes([]byte("test"))
	sig, _ := signer.Sign(digest)

	if !vs.VerifySignature(0, digest, sig) {
		t.Error("Valid signature should verify")
	}

	// Wrong digest
	wrongDigest := HashBytes([]byte("wrong"))
	if vs.VerifySignature(0, wrongDigest, sig) {
		t.Error("Signature with wrong digest should not verify")
	}

	// Non-existent validator
	if vs.VerifySignature(99, digest, sig) {
		t.Error("Non-existent validator should not verify")
	}
}

func TestSimpleValidatorSetValidators(t *testing.T) {
	validators := []*Validator{
		{Index: 0, Power: 1},
		{Index: 1, Power: 2},
	}

	vs := NewSimpleValidatorSet(validators, 1)
	returned := vs.Validators()

	if len(returned) != 2 {
		t.Errorf("Validators() length mismatch: expected 2, got %d", len(returned))
	}

	// Should be a copy
	returned[0].Power = 99
	if validators[0].Power == 99 {
		t.Error("Validators() should return a copy")
	}
}

func TestValidatorSetFAndQuorum(t *testing.T) {
	testCases := []struct {
		n      int
		f      int
		quorum int
	}{
		{1, 0, 1},  // n=1: f=0, quorum=1
		{2, 0, 1},  // n=2: f=0, quorum=1
		{3, 0, 1},  // n=3: f=0, quorum=1
		{4, 1, 3},  // n=4: f=1, quorum=3
		{5, 1, 3},  // n=5: f=1, quorum=3
		{6, 1, 3},  // n=6: f=1, quorum=3
		{7, 2, 5},  // n=7: f=2, quorum=5
		{10, 3, 7}, // n=10: f=3, quorum=7
	}

	for _, tc := range testCases {
		validators := make([]*Validator, tc.n)
		for i := 0; i < tc.n; i++ {
			validators[i] = &Validator{Index: uint16(i)}
		}

		vs := NewSimpleValidatorSet(validators, 1)

		if vs.F() != tc.f {
			t.Errorf("n=%d: F() expected %d, got %d", tc.n, tc.f, vs.F())
		}
		if vs.Quorum() != tc.quorum {
			t.Errorf("n=%d: Quorum() expected %d, got %d", tc.n, tc.quorum, vs.Quorum())
		}
	}
}
