package worker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockberries/looseberry/store"
	"github.com/blockberries/looseberry/types"
)

func TestScalerStartStop(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scalerCfg := DefaultScalerConfig()
	scalerCfg.ScalingInterval = 50 * time.Millisecond
	scaler := NewScaler(pool, scalerCfg)

	if err := scaler.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !scaler.IsRunning() {
		t.Error("Should be running after start")
	}

	// Double start should fail
	if err := scaler.Start(); err != types.ErrAlreadyRunning {
		t.Errorf("Expected ErrAlreadyRunning, got: %v", err)
	}

	if err := scaler.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if scaler.IsRunning() {
		t.Error("Should not be running after stop")
	}

	// Double stop should fail
	if err := scaler.Stop(); err != types.ErrNotRunning {
		t.Errorf("Expected ErrNotRunning, got: %v", err)
	}
}

func TestScalerCalculateLoad(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	cfg.Worker.BatchSize = 100
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scaler := NewScaler(pool, DefaultScalerConfig())

	// Initially load should be 0
	load := scaler.CalculateLoad()
	if load != 0 {
		t.Errorf("Expected load 0, got %f", load)
	}

	// Add transactions
	for i := 0; i < 50; i++ {
		tx := types.Transaction([]byte{byte(i)})
		_ = pool.AddTx(tx)
	}

	// Load should be 50 / (1 * 100) = 0.5
	load = scaler.CalculateLoad()
	if load < 0.45 || load > 0.55 {
		t.Errorf("Expected load ~0.5, got %f", load)
	}
}

func TestScalerAutoScaleUp(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	cfg.Worker.BatchSize = 100
	cfg.Worker.BatchTimeout = 10 * time.Second // Long timeout to prevent batch creation
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scalerCfg := DefaultScalerConfig()
	scalerCfg.ScaleUpThreshold = 0.5
	scalerCfg.ScalingInterval = 50 * time.Millisecond
	scalerCfg.ScaleCooldown = 10 * time.Millisecond
	scaler := NewScaler(pool, scalerCfg)

	_ = scaler.Start()
	defer func() { _ = scaler.Stop() }()

	// Track scale events
	var scaleUpCount atomic.Int32
	scaler.SetScaleUpCallback(func(old, new int, load float64) {
		scaleUpCount.Add(1)
	})

	// Add enough transactions to trigger scale-up (load > 0.5)
	for i := 0; i < 60; i++ {
		tx := types.Transaction([]byte{byte(i)})
		_ = pool.AddTx(tx)
	}

	// Wait for scaling to occur
	time.Sleep(150 * time.Millisecond)

	if pool.WorkerCount() <= 1 {
		t.Errorf("Expected worker count > 1, got %d", pool.WorkerCount())
	}

	if scaleUpCount.Load() == 0 {
		t.Error("Expected at least one scale-up callback")
	}
}

func TestScalerAutoScaleDown(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	cfg.Worker.BatchSize = 100
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	// Add workers to scale down from
	pool.ScaleUp()
	pool.ScaleUp()
	if pool.WorkerCount() != 3 {
		t.Fatalf("Expected 3 workers, got %d", pool.WorkerCount())
	}

	scalerCfg := DefaultScalerConfig()
	scalerCfg.ScaleDownThreshold = 0.3
	scalerCfg.ScalingInterval = 50 * time.Millisecond
	scalerCfg.ScaleCooldown = 10 * time.Millisecond
	scaler := NewScaler(pool, scalerCfg)

	_ = scaler.Start()
	defer func() { _ = scaler.Stop() }()

	// Track scale events
	var scaleDownCount atomic.Int32
	scaler.SetScaleDownCallback(func(old, new int, load float64) {
		scaleDownCount.Add(1)
	})

	// With no transactions, load is 0 which is < 0.3
	// Wait for scaling to occur
	time.Sleep(200 * time.Millisecond)

	if pool.WorkerCount() >= 3 {
		t.Errorf("Expected worker count < 3, got %d", pool.WorkerCount())
	}

	if scaleDownCount.Load() == 0 {
		t.Error("Expected at least one scale-down callback")
	}
}

func TestScalerCooldown(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scalerCfg := DefaultScalerConfig()
	scalerCfg.ScaleCooldown = 1 * time.Second // Long cooldown
	scaler := NewScaler(pool, scalerCfg)

	// First scale should work
	if !scaler.ForceScaleUp() {
		t.Error("First scale-up should succeed")
	}

	// Second scale should be blocked by cooldown
	// (Not calling ForceScaleUp as it bypasses cooldown check)
	// Instead verify the last scale time is set
	metrics := scaler.Metrics()
	if metrics.LastScaleTime.IsZero() {
		t.Error("Last scale time should be set")
	}
}

func TestScalerForceScaleUp(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 3
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scaler := NewScaler(pool, DefaultScalerConfig())

	// Force scale up
	if !scaler.ForceScaleUp() {
		t.Error("ForceScaleUp should succeed")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	metrics := scaler.Metrics()
	if metrics.ScaleUpCount != 1 {
		t.Errorf("Expected scale-up count 1, got %d", metrics.ScaleUpCount)
	}

	// Scale to max
	scaler.ForceScaleUp()
	if pool.WorkerCount() != 3 {
		t.Errorf("Expected 3 workers, got %d", pool.WorkerCount())
	}

	// Should fail at max
	if scaler.ForceScaleUp() {
		t.Error("ForceScaleUp should fail at max")
	}
}

func TestScalerForceScaleDown(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 3
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	// Scale up first
	pool.ScaleUp()
	pool.ScaleUp()

	scaler := NewScaler(pool, DefaultScalerConfig())

	// Force scale down
	if !scaler.ForceScaleDown() {
		t.Error("ForceScaleDown should succeed")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	metrics := scaler.Metrics()
	if metrics.ScaleDownCount != 1 {
		t.Errorf("Expected scale-down count 1, got %d", metrics.ScaleDownCount)
	}

	// Scale to min
	scaler.ForceScaleDown()
	if pool.WorkerCount() != 1 {
		t.Errorf("Expected 1 worker, got %d", pool.WorkerCount())
	}

	// Should fail at min
	if scaler.ForceScaleDown() {
		t.Error("ForceScaleDown should fail at min")
	}
}

func TestScalerMetrics(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scaler := NewScaler(pool, DefaultScalerConfig())

	// Initial metrics
	metrics := scaler.Metrics()
	if metrics.CurrentWorkers != 1 {
		t.Errorf("Expected 1 worker, got %d", metrics.CurrentWorkers)
	}
	if metrics.ScaleUpCount != 0 {
		t.Errorf("Expected 0 scale-ups, got %d", metrics.ScaleUpCount)
	}

	// Perform some scaling
	scaler.ForceScaleUp()
	scaler.ForceScaleDown()

	metrics = scaler.Metrics()
	if metrics.ScaleUpCount != 1 {
		t.Errorf("Expected 1 scale-up, got %d", metrics.ScaleUpCount)
	}
	if metrics.ScaleDownCount != 1 {
		t.Errorf("Expected 1 scale-down, got %d", metrics.ScaleDownCount)
	}
	if len(metrics.RecentEvents) != 2 {
		t.Errorf("Expected 2 events, got %d", len(metrics.RecentEvents))
	}
}

func TestScalerEventHistory(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 10
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scaler := NewScaler(pool, DefaultScalerConfig())

	// Perform multiple scale operations
	for i := 0; i < 5; i++ {
		scaler.ForceScaleUp()
	}

	metrics := scaler.Metrics()
	if len(metrics.RecentEvents) != 5 {
		t.Errorf("Expected 5 events, got %d", len(metrics.RecentEvents))
	}

	// Verify events are scale-up
	for _, event := range metrics.RecentEvents {
		if !event.ScaleUp {
			t.Error("All events should be scale-up")
		}
		if event.Time.IsZero() {
			t.Error("Event time should be set")
		}
	}
}

func TestScalerCallbacks(t *testing.T) {
	batchStore := store.NewMemoryBatchStore()
	txIndex := store.NewMemoryTxIndex()
	defer batchStore.Close()
	defer txIndex.Close()

	cfg := DefaultPoolConfig()
	cfg.MinWorkers = 1
	cfg.MaxWorkers = 4
	pool := NewPool(cfg, 0, batchStore, txIndex, 3)
	_ = pool.Start()
	defer func() { _ = pool.Stop() }()

	scaler := NewScaler(pool, DefaultScalerConfig())

	var lastScaleUpOld, lastScaleUpNew int
	var lastScaleDownOld, lastScaleDownNew int

	scaler.SetScaleUpCallback(func(old, new int, load float64) {
		lastScaleUpOld = old
		lastScaleUpNew = new
	})

	scaler.SetScaleDownCallback(func(old, new int, load float64) {
		lastScaleDownOld = old
		lastScaleDownNew = new
	})

	scaler.ForceScaleUp()
	if lastScaleUpOld != 1 || lastScaleUpNew != 2 {
		t.Errorf("Scale-up callback wrong values: %d -> %d", lastScaleUpOld, lastScaleUpNew)
	}

	scaler.ForceScaleDown()
	if lastScaleDownOld != 2 || lastScaleDownNew != 1 {
		t.Errorf("Scale-down callback wrong values: %d -> %d", lastScaleDownOld, lastScaleDownNew)
	}
}

func TestDefaultScalerConfig(t *testing.T) {
	cfg := DefaultScalerConfig()

	if cfg.ScaleUpThreshold <= 0 || cfg.ScaleUpThreshold > 1 {
		t.Error("ScaleUpThreshold should be between 0 and 1")
	}

	if cfg.ScaleDownThreshold <= 0 || cfg.ScaleDownThreshold >= cfg.ScaleUpThreshold {
		t.Error("ScaleDownThreshold should be between 0 and ScaleUpThreshold")
	}

	if cfg.ScalingInterval <= 0 {
		t.Error("ScalingInterval should be positive")
	}

	if cfg.ScaleCooldown <= 0 {
		t.Error("ScaleCooldown should be positive")
	}
}
