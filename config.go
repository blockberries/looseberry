package looseberry

import (
	"fmt"
	"time"

	"github.com/blockberries/looseberry/types"
)

// TxValidator validates transactions before they are added to batches.
// This wraps Application.CheckTx for pre-batch validation.
// Return nil if the transaction is valid, or an error describing why it's invalid.
type TxValidator func(tx []byte) error

// Config contains all configuration options for Looseberry.
type Config struct {
	// Identity
	ValidatorIndex uint16
	Signer         types.Signer

	// Transaction validation
	TxValidator TxValidator

	// Worker configuration
	Worker WorkerConfig

	// Primary configuration
	Primary PrimaryConfig

	// Sync configuration
	Sync SyncConfig

	// Storage configuration
	Storage StorageConfig

	// Garbage collection configuration
	GC GCConfig

	// Flow control configuration
	FlowControl FlowControlConfig
}

// WorkerConfig contains worker-specific configuration.
type WorkerConfig struct {
	// MinWorkers is the minimum number of workers (default: 1).
	MinWorkers int

	// MaxWorkers is the maximum number of workers (default: 8).
	MaxWorkers int

	// BatchSize is the maximum number of transactions per batch (default: 500).
	BatchSize int

	// BatchTimeout is the maximum time to wait before creating a batch (default: 100ms).
	BatchTimeout time.Duration

	// MaxBatchBytes is the maximum batch size in bytes (default: 512KB).
	MaxBatchBytes int64

	// MaxPendingTxs is the maximum pending transactions per worker (default: 10000).
	MaxPendingTxs int

	// MaxPendingBytes is the maximum pending bytes per worker (default: 50MB).
	MaxPendingBytes int64

	// ScalingInterval is how often to check for scaling (default: 5s).
	ScalingInterval time.Duration

	// ScaleUpThreshold is the load ratio to trigger scale up (default: 0.8).
	ScaleUpThreshold float64

	// ScaleDownThreshold is the load ratio to trigger scale down (default: 0.2).
	ScaleDownThreshold float64

	// DrainTimeout is the maximum time to wait for worker drain on scale down (default: 30s).
	DrainTimeout time.Duration
}

// PrimaryConfig contains primary-specific configuration.
type PrimaryConfig struct {
	// HeaderTimeout is the maximum time to wait for batches before creating a header (default: 500ms).
	HeaderTimeout time.Duration

	// MaxBatchesPerHeader is the maximum batch references per header (default: 100).
	MaxBatchesPerHeader int

	// MaxRoundGap is the maximum rounds ahead to accept headers from (default: 10).
	MaxRoundGap uint64

	// VoteTimeout is how long to buffer votes for unknown headers (default: 30s).
	VoteTimeout time.Duration

	// AllowEmptyHeaders allows creating headers with no batches for liveness (default: true).
	AllowEmptyHeaders bool
}

// SyncConfig contains sync-specific configuration.
type SyncConfig struct {
	// SyncInterval is how often to check for sync (default: 10s).
	SyncInterval time.Duration

	// SyncThreshold is rounds behind before triggering sync (default: 5).
	SyncThreshold uint64

	// SyncBatchSize is the number of certificates per sync request (default: 100).
	SyncBatchSize int

	// SyncTimeout is the timeout for sync requests (default: 30s).
	SyncTimeout time.Duration
}

// StorageConfig contains storage-specific configuration.
type StorageConfig struct {
	// DataDir is the directory for persistent storage.
	DataDir string

	// InMemory uses in-memory storage instead of persistent (for testing).
	InMemory bool
}

// GCConfig contains garbage collection configuration.
type GCConfig struct {
	// GCDepth is the number of rounds to keep after commit (default: 50).
	GCDepth int

	// RecoverTxs enables re-injection of uncommitted transactions (default: true).
	RecoverTxs bool
}

// FlowControlConfig contains flow control configuration.
type FlowControlConfig struct {
	// MaxUncommittedRounds is the maximum uncommitted rounds before pausing (default: 100).
	MaxUncommittedRounds int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Worker:      DefaultWorkerConfig(),
		Primary:     DefaultPrimaryConfig(),
		Sync:        DefaultSyncConfig(),
		Storage:     DefaultStorageConfig(),
		GC:          DefaultGCConfig(),
		FlowControl: DefaultFlowControlConfig(),
	}
}

// DefaultWorkerConfig returns WorkerConfig with defaults.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MinWorkers:         1,
		MaxWorkers:         8,
		BatchSize:          500,
		BatchTimeout:       100 * time.Millisecond,
		MaxBatchBytes:      512 * 1024, // 512KB
		MaxPendingTxs:      10000,
		MaxPendingBytes:    50 * 1024 * 1024, // 50MB
		ScalingInterval:    5 * time.Second,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.2,
		DrainTimeout:       30 * time.Second,
	}
}

// DefaultPrimaryConfig returns PrimaryConfig with defaults.
func DefaultPrimaryConfig() PrimaryConfig {
	return PrimaryConfig{
		HeaderTimeout:       500 * time.Millisecond,
		MaxBatchesPerHeader: 100,
		MaxRoundGap:         10,
		VoteTimeout:         30 * time.Second,
		AllowEmptyHeaders:   true,
	}
}

// DefaultSyncConfig returns SyncConfig with defaults.
func DefaultSyncConfig() SyncConfig {
	return SyncConfig{
		SyncInterval:  10 * time.Second,
		SyncThreshold: 5,
		SyncBatchSize: 100,
		SyncTimeout:   30 * time.Second,
	}
}

// DefaultStorageConfig returns StorageConfig with defaults.
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		DataDir:  "data/looseberry",
		InMemory: false,
	}
}

// DefaultGCConfig returns GCConfig with defaults.
func DefaultGCConfig() GCConfig {
	return GCConfig{
		GCDepth:    50,
		RecoverTxs: true,
	}
}

// DefaultFlowControlConfig returns FlowControlConfig with defaults.
func DefaultFlowControlConfig() FlowControlConfig {
	return FlowControlConfig{
		MaxUncommittedRounds: 100,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if err := c.Worker.Validate(); err != nil {
		return fmt.Errorf("worker config: %w", err)
	}
	if err := c.Primary.Validate(); err != nil {
		return fmt.Errorf("primary config: %w", err)
	}
	if err := c.Sync.Validate(); err != nil {
		return fmt.Errorf("sync config: %w", err)
	}
	if err := c.GC.Validate(); err != nil {
		return fmt.Errorf("gc config: %w", err)
	}
	if err := c.FlowControl.Validate(); err != nil {
		return fmt.Errorf("flow control config: %w", err)
	}
	return nil
}

// Validate validates the worker configuration.
func (c *WorkerConfig) Validate() error {
	if c.MinWorkers < 1 {
		return fmt.Errorf("min_workers must be at least 1")
	}
	if c.MaxWorkers < c.MinWorkers {
		return fmt.Errorf("max_workers must be >= min_workers")
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1")
	}
	if c.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be positive")
	}
	if c.MaxBatchBytes < 1 {
		return fmt.Errorf("max_batch_bytes must be at least 1")
	}
	if c.MaxPendingTxs < 1 {
		return fmt.Errorf("max_pending_txs must be at least 1")
	}
	if c.MaxPendingBytes < 1 {
		return fmt.Errorf("max_pending_bytes must be at least 1")
	}
	if c.ScaleUpThreshold <= c.ScaleDownThreshold {
		return fmt.Errorf("scale_up_threshold must be > scale_down_threshold")
	}
	return nil
}

// Validate validates the primary configuration.
func (c *PrimaryConfig) Validate() error {
	if c.HeaderTimeout <= 0 {
		return fmt.Errorf("header_timeout must be positive")
	}
	if c.MaxBatchesPerHeader < 1 {
		return fmt.Errorf("max_batches_per_header must be at least 1")
	}
	if c.MaxRoundGap < 1 {
		return fmt.Errorf("max_round_gap must be at least 1")
	}
	if c.VoteTimeout <= 0 {
		return fmt.Errorf("vote_timeout must be positive")
	}
	return nil
}

// Validate validates the sync configuration.
func (c *SyncConfig) Validate() error {
	if c.SyncInterval <= 0 {
		return fmt.Errorf("sync_interval must be positive")
	}
	if c.SyncThreshold < 1 {
		return fmt.Errorf("sync_threshold must be at least 1")
	}
	if c.SyncBatchSize < 1 {
		return fmt.Errorf("sync_batch_size must be at least 1")
	}
	if c.SyncTimeout <= 0 {
		return fmt.Errorf("sync_timeout must be positive")
	}
	return nil
}

// Validate validates the GC configuration.
func (c *GCConfig) Validate() error {
	if c.GCDepth < 1 {
		return fmt.Errorf("gc_depth must be at least 1")
	}
	return nil
}

// Validate validates the flow control configuration.
func (c *FlowControlConfig) Validate() error {
	if c.MaxUncommittedRounds < 1 {
		return fmt.Errorf("max_uncommitted_rounds must be at least 1")
	}
	return nil
}
