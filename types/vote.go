package types

// Vote represents a validator's vote on a header.
type Vote struct {
	// HeaderDigest is the hash of the header being voted on.
	HeaderDigest Hash

	// Validator is the index of the voting validator.
	Validator uint16

	// Signature is the validator's signature over the HeaderDigest.
	Signature Signature
}

// NewVote creates a new unsigned vote.
func NewVote(headerDigest Hash, validator uint16) *Vote {
	return &Vote{
		HeaderDigest: headerDigest,
		Validator:    validator,
	}
}

// Sign signs the vote with the given signer.
func (v *Vote) Sign(signer Signer) error {
	sig, err := signer.Sign(v.HeaderDigest)
	if err != nil {
		return err
	}
	v.Signature = sig
	return nil
}

// Verify verifies the vote's signature using the given public key.
func (v *Vote) Verify(pk PublicKey) bool {
	return pk.Verify(v.HeaderDigest, v.Signature)
}

// IsSigned returns true if the vote has been signed.
func (v *Vote) IsSigned() bool {
	return !v.Signature.IsEmpty()
}

// Clone returns a copy of the vote.
func (v *Vote) Clone() *Vote {
	return &Vote{
		HeaderDigest: v.HeaderDigest,
		Validator:    v.Validator,
		Signature:    v.Signature,
	}
}
