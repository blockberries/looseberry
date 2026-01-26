package types

import (
	"encoding/binary"
	"sort"
	"time"
)

// CertificateRef is a reference to a certificate by its digest and round.
type CertificateRef struct {
	Digest Hash
	Round  uint64
}

// Header represents a DAG vertex proposal from a primary.
type Header struct {
	// Author is the validator index that created this header.
	Author uint16

	// Round is the DAG round number.
	Round uint64

	// Epoch is the validator set epoch.
	Epoch uint64

	// BatchRefs are references to transaction batches included in this header.
	BatchRefs []BatchDigest

	// Parents are references to 2f+1 certificates from round-1.
	// Must be sorted by the certificate's author (validator index).
	Parents []CertificateRef

	// Timestamp is when this header was created (Unix nanoseconds).
	Timestamp int64

	// Digest is the computed hash of this header (excludes Signature).
	Digest Hash

	// Signature is the author's signature over the Digest.
	Signature Signature
}

// NewHeader creates a new unsigned header.
func NewHeader(author uint16, round, epoch uint64, batchRefs []BatchDigest, parents []CertificateRef) *Header {
	h := &Header{
		Author:    author,
		Round:     round,
		Epoch:     epoch,
		BatchRefs: batchRefs,
		Parents:   parents,
		Timestamp: time.Now().UnixNano(),
	}
	h.Digest = h.ComputeDigest()
	return h
}

// ComputeDigest computes the header digest (excludes Signature).
// The digest is: SHA256(author || round || epoch || timestamp || batch_refs || parent_refs)
func (h *Header) ComputeDigest() Hash {
	// Estimate size
	size := 2 + 8 + 8 + 8 + // author, round, epoch, timestamp
		len(h.BatchRefs)*(HashSize+2+2) + // batch refs
		len(h.Parents)*(HashSize+8) // parent refs

	data := make([]byte, 0, size)
	buf := make([]byte, 8)

	// Author
	binary.BigEndian.PutUint16(buf[:2], h.Author)
	data = append(data, buf[:2]...)

	// Round
	binary.BigEndian.PutUint64(buf, h.Round)
	data = append(data, buf...)

	// Epoch
	binary.BigEndian.PutUint64(buf, h.Epoch)
	data = append(data, buf...)

	// Timestamp
	binary.BigEndian.PutUint64(buf, uint64(h.Timestamp))
	data = append(data, buf...)

	// Batch refs (sorted by digest for determinism)
	sortedBatchRefs := make([]BatchDigest, len(h.BatchRefs))
	copy(sortedBatchRefs, h.BatchRefs)
	sort.Slice(sortedBatchRefs, func(i, j int) bool {
		return sortedBatchRefs[i].Digest.String() < sortedBatchRefs[j].Digest.String()
	})
	for _, ref := range sortedBatchRefs {
		data = append(data, ref.Digest[:]...)
		binary.BigEndian.PutUint16(buf[:2], ref.WorkerID)
		data = append(data, buf[:2]...)
		binary.BigEndian.PutUint16(buf[:2], ref.ValidatorID)
		data = append(data, buf[:2]...)
	}

	// Parent refs (should already be sorted by validator index)
	for _, ref := range h.Parents {
		data = append(data, ref.Digest[:]...)
		binary.BigEndian.PutUint64(buf, ref.Round)
		data = append(data, buf...)
	}

	return HashBytes(data)
}

// Sign signs the header with the given signer.
func (h *Header) Sign(signer Signer) error {
	sig, err := signer.Sign(h.Digest)
	if err != nil {
		return err
	}
	h.Signature = sig
	return nil
}

// Verify verifies the header's signature using the given public key.
func (h *Header) Verify(pk PublicKey) bool {
	return pk.Verify(h.Digest, h.Signature)
}

// IsSigned returns true if the header has been signed.
func (h *Header) IsSigned() bool {
	return !h.Signature.IsEmpty()
}

// ParentCount returns the number of parent certificates.
func (h *Header) ParentCount() int {
	return len(h.Parents)
}

// BatchCount returns the number of batch references.
func (h *Header) BatchCount() int {
	return len(h.BatchRefs)
}

// IsEmpty returns true if this header has no batch references.
// Empty headers are allowed for liveness (created when headerTimeout expires with no batches).
func (h *Header) IsEmpty() bool {
	return len(h.BatchRefs) == 0
}

// Clone returns a deep copy of the header.
func (h *Header) Clone() *Header {
	clone := &Header{
		Author:    h.Author,
		Round:     h.Round,
		Epoch:     h.Epoch,
		BatchRefs: make([]BatchDigest, len(h.BatchRefs)),
		Parents:   make([]CertificateRef, len(h.Parents)),
		Timestamp: h.Timestamp,
		Digest:    h.Digest,
		Signature: h.Signature,
	}
	copy(clone.BatchRefs, h.BatchRefs)
	copy(clone.Parents, h.Parents)
	return clone
}
