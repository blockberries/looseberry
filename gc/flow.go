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
// It tracks the gap between current and committed rounds, pending batches,
// and pending headers, pausing operations when the system falls too far behind.
type FlowController struct {
	cfg FlowConfig

	currentRound   atomic.Uint64
	committedRound atomic.Uint64

	// Pending resource tracking
	pendingBatches atomic.Int64
	pendingHeaders atomic.Int64

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
// It enforces MaxUncommittedRounds and MaxPendingHeaders limits.
func (fc *FlowController) CanCreateHeader() bool {
	if fc.UncommittedGap() >= fc.cfg.MaxUncommittedRounds {
		return false
	}
	if fc.cfg.MaxPendingHeaders > 0 && fc.pendingHeaders.Load() >= int64(fc.cfg.MaxPendingHeaders) {
		return false
	}
	return true
}

// CanCreateBatch returns true if it's safe to create a new batch.
// Enforces MaxPendingBatches limit.
func (fc *FlowController) CanCreateBatch() bool {
	if fc.cfg.MaxPendingBatches > 0 && fc.pendingBatches.Load() >= int64(fc.cfg.MaxPendingBatches) {
		return false
	}
	return !fc.IsPaused()
}

// CanAdvanceRound returns true if we can advance to the next round.
func (fc *FlowController) CanAdvanceRound() bool {
	return fc.CanCreateHeader()
}

// AddPendingBatch increments the pending batch counter.
func (fc *FlowController) AddPendingBatch() {
	fc.pendingBatches.Add(1)
	fc.checkFlowControl()
}

// RemovePendingBatch decrements the pending batch counter.
func (fc *FlowController) RemovePendingBatch() {
	val := fc.pendingBatches.Add(-1)
	if val < 0 {
		fc.pendingBatches.Store(0)
	}
	fc.checkFlowControl()
}

// AddPendingHeader increments the pending header counter.
func (fc *FlowController) AddPendingHeader() {
	fc.pendingHeaders.Add(1)
	fc.checkFlowControl()
}

// RemovePendingHeader decrements the pending header counter.
func (fc *FlowController) RemovePendingHeader() {
	val := fc.pendingHeaders.Add(-1)
	if val < 0 {
		fc.pendingHeaders.Store(0)
	}
	fc.checkFlowControl()
}

// PendingBatches returns the current pending batch count.
func (fc *FlowController) PendingBatches() int64 {
	return fc.pendingBatches.Load()
}

// PendingHeaders returns the current pending header count.
func (fc *FlowController) PendingHeaders() int64 {
	return fc.pendingHeaders.Load()
}

// IsPaused returns true if flow control is paused.
func (fc *FlowController) IsPaused() bool {
	return fc.paused.Load()
}

// checkFlowControl checks if we need to pause or resume.
func (fc *FlowController) checkFlowControl() {
	shouldPause := false

	// Check uncommitted round gap
	if fc.UncommittedGap() >= fc.cfg.MaxUncommittedRounds {
		shouldPause = true
	}

	// Check pending batches limit
	if fc.cfg.MaxPendingBatches > 0 && fc.pendingBatches.Load() >= int64(fc.cfg.MaxPendingBatches) {
		shouldPause = true
	}

	// Check pending headers limit
	if fc.cfg.MaxPendingHeaders > 0 && fc.pendingHeaders.Load() >= int64(fc.cfg.MaxPendingHeaders) {
		shouldPause = true
	}

	if shouldPause {
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
		CurrentRound:         fc.currentRound.Load(),
		CommittedRound:       fc.committedRound.Load(),
		UncommittedGap:       fc.UncommittedGap(),
		PendingBatches:       fc.pendingBatches.Load(),
		PendingHeaders:       fc.pendingHeaders.Load(),
		IsPaused:             fc.paused.Load(),
		PauseCount:           fc.pauseCount.Load(),
		ResumeCount:          fc.resumeCount.Load(),
		MaxUncommittedRounds: fc.cfg.MaxUncommittedRounds,
		MaxPendingBatches:    fc.cfg.MaxPendingBatches,
		MaxPendingHeaders:    fc.cfg.MaxPendingHeaders,
	}
}

// FlowMetrics contains flow control metrics.
type FlowMetrics struct {
	CurrentRound         uint64
	CommittedRound       uint64
	UncommittedGap       uint64
	PendingBatches       int64
	PendingHeaders       int64
	IsPaused             bool
	PauseCount           uint64
	ResumeCount          uint64
	MaxUncommittedRounds uint64
	MaxPendingBatches    int
	MaxPendingHeaders    int
}

// Reset resets the flow controller state (for testing).
func (fc *FlowController) Reset() {
	fc.currentRound.Store(0)
	fc.committedRound.Store(0)
	fc.pendingBatches.Store(0)
	fc.pendingHeaders.Store(0)
	fc.paused.Store(false)
	fc.pauseCount.Store(0)
	fc.resumeCount.Store(0)
}
