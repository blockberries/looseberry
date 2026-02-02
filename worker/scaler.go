package worker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/blockberries/looseberry/types"
)

// ScalerConfig contains auto-scaler configuration.
type ScalerConfig struct {
	// ScaleUpThreshold is the load threshold above which to scale up.
	// Load is calculated as pending transactions / (worker count * batch size).
	ScaleUpThreshold float64
	// ScaleDownThreshold is the load threshold below which to scale down.
	ScaleDownThreshold float64
	// ScalingInterval is the interval between scaling checks.
	ScalingInterval time.Duration
	// ScaleCooldown is the minimum time between scaling operations.
	ScaleCooldown time.Duration
}

// DefaultScalerConfig returns default scaler configuration.
func DefaultScalerConfig() ScalerConfig {
	return ScalerConfig{
		ScaleUpThreshold:   0.8,  // 80% capacity
		ScaleDownThreshold: 0.2,  // 20% capacity
		ScalingInterval:    time.Second,
		ScaleCooldown:      5 * time.Second,
	}
}

// ScaleEvent represents a scaling operation.
type ScaleEvent struct {
	Time       time.Time
	OldWorkers int
	NewWorkers int
	Load       float64
	ScaleUp    bool
}

// ScalerMetrics contains scaler metrics.
type ScalerMetrics struct {
	CurrentWorkers  int
	CurrentLoad     float64
	ScaleUpCount    uint64
	ScaleDownCount  uint64
	LastScaleTime   time.Time
	RecentEvents    []ScaleEvent
}

// Scaler manages automatic worker scaling based on load.
type Scaler struct {
	pool *Pool
	cfg  ScalerConfig

	// Scaling state
	lastScaleTime   time.Time
	lastScaleTimeMu sync.Mutex

	// Statistics
	scaleUpCount   atomic.Uint64
	scaleDownCount atomic.Uint64

	// Event history (ring buffer)
	events   []ScaleEvent
	eventsMu sync.Mutex
	maxEvents int

	// Lifecycle
	running   atomic.Bool
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// Callbacks
	callbacksMu sync.RWMutex
	onScaleUp   func(oldWorkers, newWorkers int, load float64)
	onScaleDown func(oldWorkers, newWorkers int, load float64)
}

// NewScaler creates a new auto-scaler for a worker pool.
func NewScaler(pool *Pool, cfg ScalerConfig) *Scaler {
	return &Scaler{
		pool:      pool,
		cfg:       cfg,
		events:    make([]ScaleEvent, 0, 100),
		maxEvents: 100,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// SetScaleUpCallback sets the callback for scale-up events.
func (s *Scaler) SetScaleUpCallback(cb func(oldWorkers, newWorkers int, load float64)) {
	s.callbacksMu.Lock()
	defer s.callbacksMu.Unlock()
	s.onScaleUp = cb
}

// SetScaleDownCallback sets the callback for scale-down events.
func (s *Scaler) SetScaleDownCallback(cb func(oldWorkers, newWorkers int, load float64)) {
	s.callbacksMu.Lock()
	defer s.callbacksMu.Unlock()
	s.onScaleDown = cb
}

// Start starts the auto-scaler.
func (s *Scaler) Start() error {
	if s.running.Swap(true) {
		return types.ErrAlreadyRunning
	}

	// Reset channels for restart capability
	s.stopCh = make(chan struct{})
	s.stoppedCh = make(chan struct{})

	go s.scalingLoop()
	return nil
}

// Stop stops the auto-scaler.
func (s *Scaler) Stop() error {
	if !s.running.Swap(false) {
		return types.ErrNotRunning
	}

	close(s.stopCh)
	<-s.stoppedCh
	return nil
}

// IsRunning returns true if the scaler is running.
func (s *Scaler) IsRunning() bool {
	return s.running.Load()
}

// CalculateLoad calculates the current load factor.
// Load = max(countLoad, byteLoad) where:
//   - countLoad = pending transactions / (worker count * batch size)
//   - byteLoad = pending bytes / (worker count * max pending bytes per worker)
//
// This ensures scaling decisions consider both transaction count and byte capacity.
func (s *Scaler) CalculateLoad() float64 {
	workerCount := s.pool.WorkerCount()
	if workerCount == 0 {
		return 0
	}

	// Calculate count-based load
	pendingCount := s.pool.PendingCount()
	countCapacity := float64(workerCount * s.pool.cfg.Worker.BatchSize)
	var countLoad float64
	if countCapacity > 0 {
		countLoad = float64(pendingCount) / countCapacity
	}

	// Calculate byte-based load
	pendingBytes := s.pool.PendingBytes()
	byteCapacity := float64(workerCount) * float64(s.pool.cfg.Worker.MaxPendingBytes)
	var byteLoad float64
	if byteCapacity > 0 {
		byteLoad = float64(pendingBytes) / byteCapacity
	}

	// Return the maximum of the two - scale up if EITHER is too high
	if countLoad > byteLoad {
		return countLoad
	}
	return byteLoad
}

// CalculateCountLoad calculates the transaction count based load factor.
func (s *Scaler) CalculateCountLoad() float64 {
	workerCount := s.pool.WorkerCount()
	if workerCount == 0 {
		return 0
	}

	pendingCount := s.pool.PendingCount()
	capacity := float64(workerCount * s.pool.cfg.Worker.BatchSize)

	if capacity == 0 {
		return 0
	}

	return float64(pendingCount) / capacity
}

// CalculateByteLoad calculates the byte-based load factor.
func (s *Scaler) CalculateByteLoad() float64 {
	workerCount := s.pool.WorkerCount()
	if workerCount == 0 {
		return 0
	}

	pendingBytes := s.pool.PendingBytes()
	capacity := float64(workerCount) * float64(s.pool.cfg.Worker.MaxPendingBytes)

	if capacity == 0 {
		return 0
	}

	return float64(pendingBytes) / capacity
}

// Metrics returns the current scaler metrics.
func (s *Scaler) Metrics() ScalerMetrics {
	s.eventsMu.Lock()
	eventsCopy := make([]ScaleEvent, len(s.events))
	copy(eventsCopy, s.events)
	s.eventsMu.Unlock()

	s.lastScaleTimeMu.Lock()
	lastScale := s.lastScaleTime
	s.lastScaleTimeMu.Unlock()

	return ScalerMetrics{
		CurrentWorkers:  s.pool.WorkerCount(),
		CurrentLoad:     s.CalculateLoad(),
		ScaleUpCount:    s.scaleUpCount.Load(),
		ScaleDownCount:  s.scaleDownCount.Load(),
		LastScaleTime:   lastScale,
		RecentEvents:    eventsCopy,
	}
}

// scalingLoop is the main scaling loop.
func (s *Scaler) scalingLoop() {
	defer close(s.stoppedCh)

	ticker := time.NewTicker(s.cfg.ScalingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkAndScale()
		case <-s.stopCh:
			return
		}
	}
}

// checkAndScale checks load and scales if needed.
func (s *Scaler) checkAndScale() {
	// Check cooldown
	s.lastScaleTimeMu.Lock()
	if time.Since(s.lastScaleTime) < s.cfg.ScaleCooldown {
		s.lastScaleTimeMu.Unlock()
		return
	}
	s.lastScaleTimeMu.Unlock()

	load := s.CalculateLoad()
	oldWorkers := s.pool.WorkerCount()

	if load > s.cfg.ScaleUpThreshold {
		s.tryScaleUp(oldWorkers, load)
	} else if load < s.cfg.ScaleDownThreshold {
		s.tryScaleDown(oldWorkers, load)
	}
}

// tryScaleUp attempts to scale up.
func (s *Scaler) tryScaleUp(oldWorkers int, load float64) {
	if s.pool.ScaleUp() {
		newWorkers := s.pool.WorkerCount()
		s.recordScaleEvent(oldWorkers, newWorkers, load, true)
		s.scaleUpCount.Add(1)

		s.lastScaleTimeMu.Lock()
		s.lastScaleTime = time.Now()
		s.lastScaleTimeMu.Unlock()

		s.callbacksMu.RLock()
		cb := s.onScaleUp
		s.callbacksMu.RUnlock()
		if cb != nil {
			cb(oldWorkers, newWorkers, load)
		}
	}
}

// tryScaleDown attempts to scale down.
func (s *Scaler) tryScaleDown(oldWorkers int, load float64) {
	if s.pool.ScaleDown() {
		newWorkers := s.pool.WorkerCount()
		s.recordScaleEvent(oldWorkers, newWorkers, load, false)
		s.scaleDownCount.Add(1)

		s.lastScaleTimeMu.Lock()
		s.lastScaleTime = time.Now()
		s.lastScaleTimeMu.Unlock()

		s.callbacksMu.RLock()
		cb := s.onScaleDown
		s.callbacksMu.RUnlock()
		if cb != nil {
			cb(oldWorkers, newWorkers, load)
		}
	}
}

// recordScaleEvent records a scaling event.
func (s *Scaler) recordScaleEvent(oldWorkers, newWorkers int, load float64, scaleUp bool) {
	event := ScaleEvent{
		Time:       time.Now(),
		OldWorkers: oldWorkers,
		NewWorkers: newWorkers,
		Load:       load,
		ScaleUp:    scaleUp,
	}

	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()

	// Ring buffer - remove oldest if at capacity
	if len(s.events) >= s.maxEvents {
		s.events = s.events[1:]
	}
	s.events = append(s.events, event)
}

// ForceScaleUp forces a scale-up operation (for testing).
func (s *Scaler) ForceScaleUp() bool {
	oldWorkers := s.pool.WorkerCount()
	load := s.CalculateLoad()

	if s.pool.ScaleUp() {
		newWorkers := s.pool.WorkerCount()
		s.recordScaleEvent(oldWorkers, newWorkers, load, true)
		s.scaleUpCount.Add(1)

		s.lastScaleTimeMu.Lock()
		s.lastScaleTime = time.Now()
		s.lastScaleTimeMu.Unlock()

		s.callbacksMu.RLock()
		cb := s.onScaleUp
		s.callbacksMu.RUnlock()
		if cb != nil {
			cb(oldWorkers, newWorkers, load)
		}
		return true
	}
	return false
}

// ForceScaleDown forces a scale-down operation (for testing).
func (s *Scaler) ForceScaleDown() bool {
	oldWorkers := s.pool.WorkerCount()
	load := s.CalculateLoad()

	if s.pool.ScaleDown() {
		newWorkers := s.pool.WorkerCount()
		s.recordScaleEvent(oldWorkers, newWorkers, load, false)
		s.scaleDownCount.Add(1)

		s.lastScaleTimeMu.Lock()
		s.lastScaleTime = time.Now()
		s.lastScaleTimeMu.Unlock()

		s.callbacksMu.RLock()
		cb := s.onScaleDown
		s.callbacksMu.RUnlock()
		if cb != nil {
			cb(oldWorkers, newWorkers, load)
		}
		return true
	}
	return false
}

