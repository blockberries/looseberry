package types

import (
	"encoding/json"
	"testing"
)

func TestHashBytes(t *testing.T) {
	data := []byte("hello world")
	hash := HashBytes(data)

	// SHA-256 of "hello world" is well-known
	expected := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if hash.String() != expected {
		t.Errorf("HashBytes mismatch: expected %s, got %s", expected, hash.String())
	}
}

func TestHashEqual(t *testing.T) {
	h1 := HashBytes([]byte("test"))
	h2 := HashBytes([]byte("test"))
	h3 := HashBytes([]byte("other"))

	if !h1.Equal(h2) {
		t.Error("Equal hashes should be equal")
	}

	if h1.Equal(h3) {
		t.Error("Different hashes should not be equal")
	}
}

func TestHashIsEmpty(t *testing.T) {
	var empty Hash
	nonEmpty := HashBytes([]byte("test"))

	if !empty.IsEmpty() {
		t.Error("Zero hash should be empty")
	}

	if nonEmpty.IsEmpty() {
		t.Error("Non-zero hash should not be empty")
	}
}

func TestHashConcat(t *testing.T) {
	h1 := HashBytes([]byte("left"))
	h2 := HashBytes([]byte("right"))

	// HashConcat should be deterministic
	result1 := HashConcat(h1, h2)
	result2 := HashConcat(h1, h2)

	if !result1.Equal(result2) {
		t.Error("HashConcat should be deterministic")
	}

	// Order matters
	result3 := HashConcat(h2, h1)
	if result1.Equal(result3) {
		t.Error("HashConcat should be order-dependent")
	}
}

func TestHashFromBytes(t *testing.T) {
	original := HashBytes([]byte("test"))
	recovered, err := HashFromBytes(original.Bytes())
	if err != nil {
		t.Fatalf("HashFromBytes failed: %v", err)
	}

	if !original.Equal(recovered) {
		t.Error("HashFromBytes should recover original hash")
	}

	// Test invalid length
	_, err = HashFromBytes([]byte("too short"))
	if err == nil {
		t.Error("HashFromBytes should fail for invalid length")
	}
}

func TestHashFromHex(t *testing.T) {
	original := HashBytes([]byte("test"))
	hexStr := original.String()

	recovered, err := HashFromHex(hexStr)
	if err != nil {
		t.Fatalf("HashFromHex failed: %v", err)
	}

	if !original.Equal(recovered) {
		t.Error("HashFromHex should recover original hash")
	}

	// Test invalid hex
	_, err = HashFromHex("not hex")
	if err == nil {
		t.Error("HashFromHex should fail for invalid hex")
	}

	// Test wrong length
	_, err = HashFromHex("abcd")
	if err == nil {
		t.Error("HashFromHex should fail for wrong length")
	}
}

func TestHashMarshalText(t *testing.T) {
	original := HashBytes([]byte("test"))

	text, err := original.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText failed: %v", err)
	}

	var recovered Hash
	err = recovered.UnmarshalText(text)
	if err != nil {
		t.Fatalf("UnmarshalText failed: %v", err)
	}

	if !original.Equal(recovered) {
		t.Error("MarshalText/UnmarshalText round-trip failed")
	}
}

func TestHashJSON(t *testing.T) {
	type testStruct struct {
		Hash Hash `json:"hash"`
	}

	original := testStruct{Hash: HashBytes([]byte("test"))}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("JSON marshal failed: %v", err)
	}

	var recovered testStruct
	err = json.Unmarshal(data, &recovered)
	if err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	if !original.Hash.Equal(recovered.Hash) {
		t.Error("JSON round-trip failed")
	}
}
