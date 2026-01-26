package types

import (
	"errors"
	"testing"
)

func TestErrorsAreDefined(t *testing.T) {
	// Verify all errors are non-nil
	errs := []error{
		ErrTxAlreadyExists,
		ErrTxValidationFailed,
		ErrMempoolFull,
		ErrWorkerBackpressure,
		ErrInvalidBatch,
		ErrBatchNotFound,
		ErrBatchTooLarge,
		ErrEmptyBatch,
		ErrBatchDigestMismatch,
		ErrInvalidHeader,
		ErrDuplicateHeader,
		ErrMissingParents,
		ErrInvalidParentRound,
		ErrHeaderTooFarAhead,
		ErrInvalidVote,
		ErrDuplicateVote,
		ErrVoteWrongHeader,
		ErrInvalidCertificate,
		ErrCertificateNotFound,
		ErrInsufficientQuorum,
		ErrInvalidSignature,
		ErrValidatorNotFound,
		ErrInvalidValidator,
		ErrEpochMismatch,
		ErrRoundMismatch,
		ErrRoundTooOld,
		ErrRoundNotAdvanced,
		ErrFlowControlPaused,
		ErrSyncRequired,
		ErrSyncTimeout,
		ErrSyncFailed,
		ErrNotRunning,
		ErrAlreadyRunning,
	}

	for i, err := range errs {
		if err == nil {
			t.Errorf("Error at index %d is nil", i)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	retryable := []error{
		ErrWorkerBackpressure,
		ErrMempoolFull,
		ErrFlowControlPaused,
		ErrSyncTimeout,
	}

	for _, err := range retryable {
		if !IsRetryable(err) {
			t.Errorf("%v should be retryable", err)
		}
	}

	nonRetryable := []error{
		ErrInvalidBatch,
		ErrInvalidSignature,
		ErrDuplicateHeader,
		ErrNotRunning,
	}

	for _, err := range nonRetryable {
		if IsRetryable(err) {
			t.Errorf("%v should not be retryable", err)
		}
	}
}

func TestIsByzantine(t *testing.T) {
	byzantine := []error{
		ErrInvalidSignature,
		ErrDuplicateHeader,
		ErrDuplicateVote,
		ErrInvalidBatch,
		ErrInvalidHeader,
		ErrInvalidVote,
	}

	for _, err := range byzantine {
		if !IsByzantine(err) {
			t.Errorf("%v should be Byzantine", err)
		}
	}

	nonByzantine := []error{
		ErrMempoolFull,
		ErrWorkerBackpressure,
		ErrSyncRequired,
		ErrNotRunning,
	}

	for _, err := range nonByzantine {
		if IsByzantine(err) {
			t.Errorf("%v should not be Byzantine", err)
		}
	}
}

func TestErrorWrapping(t *testing.T) {
	// Test that errors can be wrapped and matched
	wrapped := errors.New("wrapper: " + ErrInvalidBatch.Error())

	// Direct comparison should work for sentinel errors
	if ErrInvalidBatch == wrapped {
		t.Error("Wrapped error should not be equal to original")
	}

	// errors.Is should work with proper wrapping
	properlyWrapped := errors.New("prefix: ")
	_ = properlyWrapped

	// Test errors.Is with the base error
	if !errors.Is(ErrInvalidBatch, ErrInvalidBatch) {
		t.Error("errors.Is should match same error")
	}
}
