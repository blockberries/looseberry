package looseberry

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should be valid: %v", err)
	}
}

func TestDefaultWorkerConfig(t *testing.T) {
	cfg := DefaultWorkerConfig()

	if cfg.MinWorkers != 1 {
		t.Errorf("MinWorkers mismatch: expected 1, got %d", cfg.MinWorkers)
	}
	if cfg.MaxWorkers != 8 {
		t.Errorf("MaxWorkers mismatch: expected 8, got %d", cfg.MaxWorkers)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("BatchSize mismatch: expected 500, got %d", cfg.BatchSize)
	}
	if cfg.BatchTimeout != 100*time.Millisecond {
		t.Errorf("BatchTimeout mismatch: expected 100ms, got %v", cfg.BatchTimeout)
	}
	if cfg.MaxBatchBytes != 512*1024 {
		t.Errorf("MaxBatchBytes mismatch: expected 512KB, got %d", cfg.MaxBatchBytes)
	}
	if cfg.MaxPendingTxs != 10000 {
		t.Errorf("MaxPendingTxs mismatch: expected 10000, got %d", cfg.MaxPendingTxs)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default worker config should be valid: %v", err)
	}
}

func TestDefaultPrimaryConfig(t *testing.T) {
	cfg := DefaultPrimaryConfig()

	if cfg.HeaderTimeout != 500*time.Millisecond {
		t.Errorf("HeaderTimeout mismatch: expected 500ms, got %v", cfg.HeaderTimeout)
	}
	if cfg.MaxBatchesPerHeader != 100 {
		t.Errorf("MaxBatchesPerHeader mismatch: expected 100, got %d", cfg.MaxBatchesPerHeader)
	}
	if cfg.MaxRoundGap != 10 {
		t.Errorf("MaxRoundGap mismatch: expected 10, got %d", cfg.MaxRoundGap)
	}
	if !cfg.AllowEmptyHeaders {
		t.Error("AllowEmptyHeaders should be true by default")
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default primary config should be valid: %v", err)
	}
}

func TestDefaultSyncConfig(t *testing.T) {
	cfg := DefaultSyncConfig()

	if cfg.SyncInterval != 10*time.Second {
		t.Errorf("SyncInterval mismatch: expected 10s, got %v", cfg.SyncInterval)
	}
	if cfg.SyncThreshold != 5 {
		t.Errorf("SyncThreshold mismatch: expected 5, got %d", cfg.SyncThreshold)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default sync config should be valid: %v", err)
	}
}

func TestDefaultGCConfig(t *testing.T) {
	cfg := DefaultGCConfig()

	if cfg.GCDepth != 50 {
		t.Errorf("GCDepth mismatch: expected 50, got %d", cfg.GCDepth)
	}
	if !cfg.RecoverTxs {
		t.Error("RecoverTxs should be true by default")
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default GC config should be valid: %v", err)
	}
}

func TestDefaultFlowControlConfig(t *testing.T) {
	cfg := DefaultFlowControlConfig()

	if cfg.MaxUncommittedRounds != 100 {
		t.Errorf("MaxUncommittedRounds mismatch: expected 100, got %d", cfg.MaxUncommittedRounds)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Default flow control config should be valid: %v", err)
	}
}

func TestWorkerConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*WorkerConfig)
		wantErr bool
	}{
		{
			name:    "valid",
			modify:  func(c *WorkerConfig) {},
			wantErr: false,
		},
		{
			name:    "min_workers < 1",
			modify:  func(c *WorkerConfig) { c.MinWorkers = 0 },
			wantErr: true,
		},
		{
			name:    "max_workers < min_workers",
			modify:  func(c *WorkerConfig) { c.MaxWorkers = 0 },
			wantErr: true,
		},
		{
			name:    "batch_size < 1",
			modify:  func(c *WorkerConfig) { c.BatchSize = 0 },
			wantErr: true,
		},
		{
			name:    "batch_timeout <= 0",
			modify:  func(c *WorkerConfig) { c.BatchTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "scale_up <= scale_down",
			modify:  func(c *WorkerConfig) { c.ScaleUpThreshold = 0.1; c.ScaleDownThreshold = 0.5 },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultWorkerConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrimaryConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*PrimaryConfig)
		wantErr bool
	}{
		{
			name:    "valid",
			modify:  func(c *PrimaryConfig) {},
			wantErr: false,
		},
		{
			name:    "header_timeout <= 0",
			modify:  func(c *PrimaryConfig) { c.HeaderTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "max_batches < 1",
			modify:  func(c *PrimaryConfig) { c.MaxBatchesPerHeader = 0 },
			wantErr: true,
		},
		{
			name:    "max_round_gap < 1",
			modify:  func(c *PrimaryConfig) { c.MaxRoundGap = 0 },
			wantErr: true,
		},
		{
			name:    "vote_timeout <= 0",
			modify:  func(c *PrimaryConfig) { c.VoteTimeout = 0 },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultPrimaryConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGCConfigValidation(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.GCDepth = 0

	if err := cfg.Validate(); err == nil {
		t.Error("GC depth of 0 should be invalid")
	}
}

func TestFlowControlConfigValidation(t *testing.T) {
	cfg := DefaultFlowControlConfig()
	cfg.MaxUncommittedRounds = 0

	if err := cfg.Validate(); err == nil {
		t.Error("MaxUncommittedRounds of 0 should be invalid")
	}
}
