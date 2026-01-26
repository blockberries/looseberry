package types

import "errors"

// Core errors for Looseberry.
var (
	// Transaction errors
	ErrTxAlreadyExists    = errors.New("transaction already exists")
	ErrTxValidationFailed = errors.New("transaction validation failed")

	// Mempool errors
	ErrMempoolFull        = errors.New("mempool is full")
	ErrWorkerBackpressure = errors.New("worker back-pressure: too many pending transactions")

	// Batch errors
	ErrInvalidBatch   = errors.New("invalid batch")
	ErrBatchNotFound  = errors.New("batch not found")
	ErrBatchTooLarge  = errors.New("batch exceeds maximum size")
	ErrEmptyBatch     = errors.New("batch has no transactions")
	ErrBatchDigestMismatch = errors.New("batch digest does not match computed digest")

	// Header errors
	ErrInvalidHeader     = errors.New("invalid header")
	ErrDuplicateHeader   = errors.New("duplicate header from same author for same round")
	ErrMissingParents    = errors.New("header is missing required parent certificates")
	ErrInvalidParentRound = errors.New("parent certificate is from wrong round")
	ErrHeaderTooFarAhead = errors.New("header round is too far ahead of current round")

	// Vote errors
	ErrInvalidVote      = errors.New("invalid vote")
	ErrDuplicateVote    = errors.New("duplicate vote from same validator")
	ErrVoteWrongHeader  = errors.New("vote is for different header")

	// Certificate errors
	ErrInvalidCertificate  = errors.New("invalid certificate")
	ErrCertificateNotFound = errors.New("certificate not found")
	ErrInsufficientQuorum  = errors.New("insufficient quorum: need 2f+1 votes")

	// Signature errors
	ErrInvalidSignature = errors.New("invalid signature")

	// Validator errors
	ErrValidatorNotFound = errors.New("validator not found")
	ErrInvalidValidator  = errors.New("invalid validator index")
	ErrEpochMismatch     = errors.New("epoch mismatch")

	// Round errors
	ErrRoundMismatch      = errors.New("round mismatch")
	ErrRoundTooOld        = errors.New("round is too old")
	ErrRoundNotAdvanced   = errors.New("cannot advance to round without sufficient certificates")

	// Flow control errors
	ErrFlowControlPaused = errors.New("flow control: DAG production paused, waiting for consensus")

	// Sync errors
	ErrSyncRequired = errors.New("sync required: node is too far behind")
	ErrSyncTimeout  = errors.New("sync timeout")
	ErrSyncFailed   = errors.New("sync failed")

	// Lifecycle errors
	ErrNotRunning    = errors.New("looseberry is not running")
	ErrAlreadyRunning = errors.New("looseberry is already running")
)

// IsRetryable returns true if the error is temporary and the operation can be retried.
func IsRetryable(err error) bool {
	switch {
	case errors.Is(err, ErrWorkerBackpressure):
		return true
	case errors.Is(err, ErrMempoolFull):
		return true
	case errors.Is(err, ErrFlowControlPaused):
		return true
	case errors.Is(err, ErrSyncTimeout):
		return true
	default:
		return false
	}
}

// IsByzantine returns true if the error indicates Byzantine behavior.
func IsByzantine(err error) bool {
	switch {
	case errors.Is(err, ErrInvalidSignature):
		return true
	case errors.Is(err, ErrDuplicateHeader):
		return true
	case errors.Is(err, ErrDuplicateVote):
		return true
	case errors.Is(err, ErrInvalidBatch):
		return true
	case errors.Is(err, ErrInvalidHeader):
		return true
	case errors.Is(err, ErrInvalidVote):
		return true
	default:
		return false
	}
}
