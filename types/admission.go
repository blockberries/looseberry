package types

// TxAdmission carries the application's mempool-ordering hints for an
// accepted transaction. Returned from the TxValidator alongside a nil
// error to indicate the tx is admitted to the mempool.
//
// Both fields are advisory. Priority=0 and an empty Sender mean "no
// opinion" — the worker falls back to FIFO arrival order. Apps that
// implement priority-fee economics derive Priority from the tx's fee
// component; Sender enables same-sender sequencing (e.g., honoring
// nonce order or replace-by-fee).
//
// The type lives in this package rather than `worker/` so the public
// Looseberry config can reference it without pulling in the worker
// internals.
type TxAdmission struct {
	// Priority is a free-form ordering hint; higher = scheduled into
	// a batch sooner. Apps with no fee model can leave this at 0.
	Priority int64
	// Sender identifies the tx's origin. Empty means the worker
	// treats this tx independently of any other.
	Sender string
}
