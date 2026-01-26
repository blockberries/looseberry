package types

import (
	"sort"
)

// Certificate represents a certified DAG vertex (header with 2f+1 votes).
type Certificate struct {
	// Header is the certified header.
	Header Header

	// Votes are the 2f+1 votes that form this certificate.
	// Must be sorted by validator index.
	Votes []Vote

	// SignerMask is a bitset indicating which validators signed.
	SignerMask BitSet
}

// NewCertificate creates a new certificate from a header and votes.
// The votes are sorted by validator index for determinism.
func NewCertificate(header *Header, votes []Vote) *Certificate {
	// Sort votes by validator index
	sortedVotes := make([]Vote, len(votes))
	copy(sortedVotes, votes)
	sort.Slice(sortedVotes, func(i, j int) bool {
		return sortedVotes[i].Validator < sortedVotes[j].Validator
	})

	// Build signer mask
	mask := NewBitSet(256) // Support up to 256 validators
	for _, v := range sortedVotes {
		mask.Set(int(v.Validator))
	}

	return &Certificate{
		Header:     *header.Clone(),
		Votes:      sortedVotes,
		SignerMask: mask,
	}
}

// Digest returns the certificate's digest (same as the header's digest).
func (c *Certificate) Digest() Hash {
	return c.Header.Digest
}

// Round returns the certificate's round.
func (c *Certificate) Round() uint64 {
	return c.Header.Round
}

// Author returns the certificate author's validator index.
func (c *Certificate) Author() uint16 {
	return c.Header.Author
}

// Epoch returns the certificate's epoch.
func (c *Certificate) Epoch() uint64 {
	return c.Header.Epoch
}

// VoteCount returns the number of votes in this certificate.
func (c *Certificate) VoteCount() int {
	return len(c.Votes)
}

// HasQuorum checks if the certificate has enough votes for the given validator count.
// Requires 2f+1 votes where f = (n-1)/3.
func (c *Certificate) HasQuorum(validatorCount int) bool {
	f := (validatorCount - 1) / 3
	quorum := 2*f + 1
	return len(c.Votes) >= quorum
}

// HasVoteFrom returns true if the certificate contains a vote from the given validator.
func (c *Certificate) HasVoteFrom(validatorIndex uint16) bool {
	return c.SignerMask.IsSet(int(validatorIndex))
}

// GetRef returns a CertificateRef for this certificate.
func (c *Certificate) GetRef() CertificateRef {
	return CertificateRef{
		Digest: c.Digest(),
		Round:  c.Round(),
	}
}

// Verify verifies all votes in the certificate using the provided validator set.
func (c *Certificate) Verify(validators ValidatorSet) error {
	// Check quorum
	if !c.HasQuorum(validators.Count()) {
		return ErrInsufficientQuorum
	}

	// Verify header signature
	author := validators.GetByIndex(c.Header.Author)
	if author == nil {
		return ErrValidatorNotFound
	}
	if !c.Header.Verify(author.PublicKey) {
		return ErrInvalidSignature
	}

	// Verify each vote
	for _, vote := range c.Votes {
		if !vote.HeaderDigest.Equal(c.Header.Digest) {
			return ErrInvalidVote
		}

		validator := validators.GetByIndex(vote.Validator)
		if validator == nil {
			return ErrValidatorNotFound
		}
		if !vote.Verify(validator.PublicKey) {
			return ErrInvalidSignature
		}
	}

	return nil
}

// Clone returns a deep copy of the certificate.
func (c *Certificate) Clone() *Certificate {
	votes := make([]Vote, len(c.Votes))
	for i, v := range c.Votes {
		votes[i] = *v.Clone()
	}

	return &Certificate{
		Header:     *c.Header.Clone(),
		Votes:      votes,
		SignerMask: c.SignerMask.Clone(),
	}
}

// BitSet is a simple bit set implementation.
type BitSet []uint64

// NewBitSet creates a new bit set that can hold at least n bits.
func NewBitSet(n int) BitSet {
	words := (n + 63) / 64
	return make(BitSet, words)
}

// Set sets the bit at index i.
func (bs BitSet) Set(i int) {
	word := i / 64
	bit := uint(i % 64)
	if word < len(bs) {
		bs[word] |= 1 << bit
	}
}

// Clear clears the bit at index i.
func (bs BitSet) Clear(i int) {
	word := i / 64
	bit := uint(i % 64)
	if word < len(bs) {
		bs[word] &^= 1 << bit
	}
}

// IsSet returns true if the bit at index i is set.
func (bs BitSet) IsSet(i int) bool {
	word := i / 64
	bit := uint(i % 64)
	if word >= len(bs) {
		return false
	}
	return bs[word]&(1<<bit) != 0
}

// Count returns the number of set bits.
func (bs BitSet) Count() int {
	count := 0
	for _, word := range bs {
		count += popCount(word)
	}
	return count
}

// Clone returns a copy of the bit set.
func (bs BitSet) Clone() BitSet {
	clone := make(BitSet, len(bs))
	copy(clone, bs)
	return clone
}

// popCount counts the number of set bits in a uint64.
func popCount(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}
