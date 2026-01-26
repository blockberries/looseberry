package types

// Validator represents a validator in the network.
type Validator struct {
	// Index is the validator's unique index in the validator set.
	Index uint16

	// PublicKey is the validator's Ed25519 public key.
	PublicKey PublicKey

	// Power is the validator's voting power (for weighted voting).
	Power int64

	// Address is the validator's network address (multiaddr).
	Address string
}

// ValidatorSet represents the set of validators for an epoch.
type ValidatorSet interface {
	// Count returns the number of validators.
	Count() int

	// GetByIndex returns the validator at the given index, or nil if not found.
	GetByIndex(index uint16) *Validator

	// Contains returns true if the index is a valid validator.
	Contains(index uint16) bool

	// F returns the Byzantine fault tolerance threshold: f = (n-1)/3
	F() int

	// Quorum returns the quorum size: 2f+1
	Quorum() int

	// Epoch returns the current epoch number.
	Epoch() uint64

	// VerifySignature verifies a signature from the validator at the given index.
	VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool
}

// SimpleValidatorSet is a basic implementation of ValidatorSet.
type SimpleValidatorSet struct {
	validators []*Validator
	byIndex    map[uint16]*Validator
	epoch      uint64
}

// NewSimpleValidatorSet creates a new SimpleValidatorSet.
func NewSimpleValidatorSet(validators []*Validator, epoch uint64) *SimpleValidatorSet {
	byIndex := make(map[uint16]*Validator, len(validators))
	for _, v := range validators {
		byIndex[v.Index] = v
	}
	return &SimpleValidatorSet{
		validators: validators,
		byIndex:    byIndex,
		epoch:      epoch,
	}
}

// Count returns the number of validators.
func (vs *SimpleValidatorSet) Count() int {
	return len(vs.validators)
}

// GetByIndex returns the validator at the given index.
func (vs *SimpleValidatorSet) GetByIndex(index uint16) *Validator {
	return vs.byIndex[index]
}

// Contains returns true if the index is a valid validator.
func (vs *SimpleValidatorSet) Contains(index uint16) bool {
	_, ok := vs.byIndex[index]
	return ok
}

// F returns the Byzantine fault tolerance threshold.
func (vs *SimpleValidatorSet) F() int {
	return (len(vs.validators) - 1) / 3
}

// Quorum returns the quorum size.
func (vs *SimpleValidatorSet) Quorum() int {
	return 2*vs.F() + 1
}

// Epoch returns the epoch number.
func (vs *SimpleValidatorSet) Epoch() uint64 {
	return vs.epoch
}

// VerifySignature verifies a signature from the given validator.
func (vs *SimpleValidatorSet) VerifySignature(validatorIdx uint16, digest Hash, sig Signature) bool {
	v := vs.GetByIndex(validatorIdx)
	if v == nil {
		return false
	}
	return v.PublicKey.Verify(digest, sig)
}

// Validators returns a deep copy of the validator list.
func (vs *SimpleValidatorSet) Validators() []*Validator {
	result := make([]*Validator, len(vs.validators))
	for i, v := range vs.validators {
		// Create a copy of each validator
		copy := *v
		result[i] = &copy
	}
	return result
}
