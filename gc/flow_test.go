package gc

import (
	"sync/atomic"
	"testing"
)

func TestFlowControllerCanCreateHeader(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	// Initially can create headers
	if !fc.CanCreateHeader() {
		t.Error("Should be able to create headers initially")
	}

	// Set current round to 5, committed 0 - gap is 5, should allow
	fc.UpdateCurrentRound(5)
	if !fc.CanCreateHeader() {
		t.Error("Should be able to create headers with gap 5")
	}

	// Set current round to 15 - gap is 15, should NOT allow
	fc.UpdateCurrentRound(15)
	if fc.CanCreateHeader() {
		t.Error("Should NOT be able to create headers with gap 15")
	}

	// Update committed to 10 - gap is 5, should allow again
	fc.UpdateCommittedRound(10)
	if !fc.CanCreateHeader() {
		t.Error("Should be able to create headers with gap 5")
	}
}

func TestFlowControllerUncommittedGap(t *testing.T) {
	fc := NewFlowController(DefaultFlowConfig())

	// Initial gap is 0
	if fc.UncommittedGap() != 0 {
		t.Errorf("Expected gap 0, got %d", fc.UncommittedGap())
	}

	fc.UpdateCurrentRound(10)
	if fc.UncommittedGap() != 10 {
		t.Errorf("Expected gap 10, got %d", fc.UncommittedGap())
	}

	fc.UpdateCommittedRound(5)
	if fc.UncommittedGap() != 5 {
		t.Errorf("Expected gap 5, got %d", fc.UncommittedGap())
	}

	// Committed > current (edge case)
	fc.UpdateCommittedRound(15)
	if fc.UncommittedGap() != 0 {
		t.Errorf("Expected gap 0 when committed > current, got %d", fc.UncommittedGap())
	}
}

func TestFlowControllerPauseResume(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	var pauseCount atomic.Int32
	var resumeCount atomic.Int32

	fc.SetPauseCallback(func() {
		pauseCount.Add(1)
	})

	fc.SetResumeCallback(func() {
		resumeCount.Add(1)
	})

	// Initially not paused
	if fc.IsPaused() {
		t.Error("Should not be paused initially")
	}

	// Trigger pause by exceeding limit
	fc.UpdateCurrentRound(15)
	if !fc.IsPaused() {
		t.Error("Should be paused with gap 15")
	}

	if pauseCount.Load() != 1 {
		t.Errorf("Expected 1 pause callback, got %d", pauseCount.Load())
	}

	// Update current again while paused - should not trigger another pause
	fc.UpdateCurrentRound(16)
	if pauseCount.Load() != 1 {
		t.Errorf("Should not trigger another pause callback, got %d", pauseCount.Load())
	}

	// Resume by catching up
	fc.UpdateCommittedRound(10)
	if fc.IsPaused() {
		t.Error("Should not be paused after catching up")
	}

	if resumeCount.Load() != 1 {
		t.Errorf("Expected 1 resume callback, got %d", resumeCount.Load())
	}
}

func TestFlowControllerMetrics(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	fc.UpdateCurrentRound(20)
	fc.UpdateCommittedRound(5)

	metrics := fc.Metrics()

	if metrics.CurrentRound != 20 {
		t.Errorf("Expected current round 20, got %d", metrics.CurrentRound)
	}

	if metrics.CommittedRound != 5 {
		t.Errorf("Expected committed round 5, got %d", metrics.CommittedRound)
	}

	if metrics.UncommittedGap != 15 {
		t.Errorf("Expected gap 15, got %d", metrics.UncommittedGap)
	}

	if !metrics.IsPaused {
		t.Error("Expected to be paused")
	}

	if metrics.PauseCount != 1 {
		t.Errorf("Expected pause count 1, got %d", metrics.PauseCount)
	}

	if metrics.MaxUncommittedRounds != 10 {
		t.Errorf("Expected max uncommitted rounds 10, got %d", metrics.MaxUncommittedRounds)
	}
}

func TestFlowControllerReset(t *testing.T) {
	fc := NewFlowController(DefaultFlowConfig())

	fc.UpdateCurrentRound(100)
	fc.UpdateCommittedRound(50)

	fc.Reset()

	if fc.CurrentRound() != 0 {
		t.Errorf("Expected current round 0 after reset, got %d", fc.CurrentRound())
	}

	if fc.CommittedRound() != 0 {
		t.Errorf("Expected committed round 0 after reset, got %d", fc.CommittedRound())
	}

	if fc.IsPaused() {
		t.Error("Should not be paused after reset")
	}
}

func TestFlowControllerCanAdvanceRound(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 5
	fc := NewFlowController(cfg)

	if !fc.CanAdvanceRound() {
		t.Error("Should be able to advance round initially")
	}

	fc.UpdateCurrentRound(10)

	if fc.CanAdvanceRound() {
		t.Error("Should not be able to advance round with large gap")
	}
}

func TestFlowControllerPauseResumeEdgeCases(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	// Exactly at the limit - should pause
	fc.UpdateCurrentRound(10)
	if !fc.IsPaused() {
		t.Error("Should pause at exactly MaxUncommittedRounds")
	}

	// One below the limit - should resume
	fc.UpdateCommittedRound(1)
	if fc.IsPaused() {
		t.Error("Should resume when gap < MaxUncommittedRounds")
	}
}

func TestFlowControllerNoCallbacks(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	// Don't set callbacks - should not panic
	fc.UpdateCurrentRound(15)
	fc.UpdateCommittedRound(10)

	// Test passed if no panic
}

func TestDefaultFlowConfig(t *testing.T) {
	cfg := DefaultFlowConfig()

	if cfg.MaxUncommittedRounds == 0 {
		t.Error("MaxUncommittedRounds should be non-zero")
	}

	if cfg.MaxPendingBatches == 0 {
		t.Error("MaxPendingBatches should be non-zero")
	}

	if cfg.MaxPendingHeaders == 0 {
		t.Error("MaxPendingHeaders should be non-zero")
	}
}

func TestFlowControllerMultiplePauseResumeCycles(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxUncommittedRounds = 10
	fc := NewFlowController(cfg)

	var pauseCount atomic.Int32
	var resumeCount atomic.Int32

	fc.SetPauseCallback(func() {
		pauseCount.Add(1)
	})

	fc.SetResumeCallback(func() {
		resumeCount.Add(1)
	})

	// First pause-resume cycle
	fc.UpdateCurrentRound(15)
	fc.UpdateCommittedRound(10)

	// Second pause-resume cycle
	fc.UpdateCurrentRound(25)
	fc.UpdateCommittedRound(20)

	// Third pause-resume cycle
	fc.UpdateCurrentRound(35)
	fc.UpdateCommittedRound(30)

	if pauseCount.Load() != 3 {
		t.Errorf("Expected 3 pause callbacks, got %d", pauseCount.Load())
	}

	if resumeCount.Load() != 3 {
		t.Errorf("Expected 3 resume callbacks, got %d", resumeCount.Load())
	}

	metrics := fc.Metrics()
	if metrics.PauseCount != 3 {
		t.Errorf("Expected pause count 3, got %d", metrics.PauseCount)
	}

	if metrics.ResumeCount != 3 {
		t.Errorf("Expected resume count 3, got %d", metrics.ResumeCount)
	}
}
