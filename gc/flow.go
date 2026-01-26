package gc

import (
	"sync/atomic"
)

// FlowConfig contains flow control configuration.
type FlowConfig struct {
	// MaxUncommittedRounds is the maximum gap between current and committed rounds.
	MaxUncommittedRounds uint64
	// MaxPendingBatches is the maximum number of pending batches per worker.
	MaxPendingBatches int
	// MaxPendingHeaders is the maximum number of pending headers awaiting votes.
	MaxPendingHeaders int
}

// DefaultFlowConfig returns default flow control configuration.
func DefaultFlowConfig() FlowConfig {
	return FlowConfig{
		MaxUncommittedRounds: 100,
		MaxPendingBatches:    1000,
		MaxPendingHeaders:    100,
	}
}

// FlowController manages flow control to prevent unbounded resource usage.
// It tracks the gap between current and committed rounds, and pauses
// header creation when the system falls too far behind.
type FlowController struct {
	cfg FlowConfig

	currentRound   atomic.Uint64
	committedRound atomic.Uint64

	// Pause state
	paused atomic.Bool

	// Callbacks
	onPause  func()
	onResume func()

	// Metrics
	pauseCount   atomic.Uint64
	resumeCount  atomic.Uint64
}

// NewFlowController creates a new flow controller.
func NewFlowController(cfg FlowConfig) *FlowController {
	return &FlowController{
		cfg: cfg,
	}
}

// SetPauseCallback sets the callback for when flow control pauses.
func (fc *FlowController) SetPauseCallback(cb func()) {
	fc.onPause = cb
}

// SetResumeCallback sets the callback for when flow control resumes.
func (fc *FlowController) SetResumeCallback(cb func()) {
	fc.onResume = cb
}

// UpdateCurrentRound updates the current round.
func (fc *FlowController) UpdateCurrentRound(round uint64) {
	fc.currentRound.Store(round)
	fc.checkFlowControl()
}

// UpdateCommittedRound updates the committed round.
func (fc *FlowController) UpdateCommittedRound(round uint64) {
	fc.committedRound.Store(round)
	fc.checkFlowControl()
}

// CurrentRound returns the current round.
func (fc *FlowController) CurrentRound() uint64 {
	return fc.currentRound.Load()
}

// CommittedRound returns the committed round.
func (fc *FlowController) CommittedRound() uint64 {
	return fc.committedRound.Load()
}

// UncommittedGap returns the gap between current and committed rounds.
func (fc *FlowController) UncommittedGap() uint64 {
	current := fc.currentRound.Load()
	committed := fc.committedRound.Load()
	if current <= committed {
		return 0
	}
	return current - committed
}

// CanCreateHeader returns true if it's safe to create a new header.
// This is the main flow control check that prevents runaway round advancement.
func (fc *FlowController) CanCreateHeader() bool {
	return fc.UncommittedGap() < fc.cfg.MaxUncommittedRounds
}

// CanAdvanceRound returns true if we can advance to the next round.
func (fc *FlowController) CanAdvanceRound() bool {
	return fc.CanCreateHeader()
}

// IsPaused returns true if flow control is paused.
func (fc *FlowController) IsPaused() bool {
	return fc.paused.Load()
}

// checkFlowControl checks if we need to pause or resume.
func (fc *FlowController) checkFlowControl() {
	gap := fc.UncommittedGap()

	if gap >= fc.cfg.MaxUncommittedRounds {
		// Need to pause
		if !fc.paused.Swap(true) {
			// Was not paused, now paused
			fc.pauseCount.Add(1)
			if fc.onPause != nil {
				fc.onPause()
			}
		}
	} else {
		// Can resume
		if fc.paused.Swap(false) {
			// Was paused, now resumed
			fc.resumeCount.Add(1)
			if fc.onResume != nil {
				fc.onResume()
			}
		}
	}
}

// Metrics returns flow control metrics.
func (fc *FlowController) Metrics() FlowMetrics {
	return FlowMetrics{
		CurrentRound:     fc.currentRound.Load(),
		CommittedRound:   fc.committedRound.Load(),
		UncommittedGap:   fc.UncommittedGap(),
		IsPaused:         fc.paused.Load(),
		PauseCount:       fc.pauseCount.Load(),
		ResumeCount:      fc.resumeCount.Load(),
		MaxUncommittedRounds: fc.cfg.MaxUncommittedRounds,
	}
}

// FlowMetrics contains flow control metrics.
type FlowMetrics struct {
	CurrentRound         uint64
	CommittedRound       uint64
	UncommittedGap       uint64
	IsPaused             bool
	PauseCount           uint64
	ResumeCount          uint64
	MaxUncommittedRounds uint64
}

// Reset resets the flow controller state (for testing).
func (fc *FlowController) Reset() {
	fc.currentRound.Store(0)
	fc.committedRound.Store(0)
	fc.paused.Store(false)
	fc.pauseCount.Store(0)
	fc.resumeCount.Store(0)
}
