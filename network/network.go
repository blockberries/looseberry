package network

import (
	"github.com/blockberries/looseberry/types"
)

// BatchMessage represents a received batch.
type BatchMessage struct {
	Batch *types.Batch
	From  uint16 // Validator who sent it
}

// BatchAckMessage represents a batch acknowledgment.
type BatchAckMessage struct {
	BatchDigest types.Hash
	Validator   uint16
	Signature   types.Signature
}

// BatchRequestMessage represents a request for a batch.
type BatchRequestMessage struct {
	BatchDigest types.Hash
	Requester   uint16
}

// HeaderMessage represents a received header.
type HeaderMessage struct {
	Header *types.Header
	From   uint16
}

// VoteMessage represents a received vote.
type VoteMessage struct {
	Vote *types.Vote
	From uint16
}

// CertificateMessage represents a received certificate.
type CertificateMessage struct {
	Certificate *types.Certificate
	From        uint16
}

// SyncRequest represents a request to sync certificates.
type SyncRequest struct {
	FromRound uint64 // Lowest round needed
	ToRound   uint64 // Highest round needed (0 = latest)
	Requester uint16
}

// SyncResponse represents a response to a sync request.
type SyncResponse struct {
	Certificates []*types.Certificate
	Batches      []*types.Batch // Batches referenced by certificates
	FromRound    uint64
	ToRound      uint64
}

// SyncRequestMessage wraps a sync request with sender info.
type SyncRequestMessage struct {
	Request *SyncRequest
	From    uint16
}

// SyncResponseMessage wraps a sync response with sender info.
type SyncResponseMessage struct {
	Response *SyncResponse
	From     uint16
}

// BatchAck represents a batch acknowledgment.
type BatchAck struct {
	BatchDigest types.Hash
	Validator   uint16
	Signature   types.Signature
}

// Network defines the interface for network communication.
type Network interface {
	// Broadcast sends a message to all validators.
	BroadcastBatch(batch *types.Batch) error
	BroadcastHeader(header *types.Header) error
	BroadcastCertificate(cert *types.Certificate) error

	// Send sends a message to a specific validator.
	SendVote(validator uint16, vote *types.Vote) error
	SendBatchAck(validator uint16, ack *BatchAckMessage) error
	SendBatchRequest(validator uint16, req *BatchRequestMessage) error
	SendSyncRequest(validator uint16, req *SyncRequest) error
	SendSyncResponse(validator uint16, resp *SyncResponse) error

	// Receive channels for incoming messages.
	BatchMessages() <-chan *BatchMessage
	BatchAckMessages() <-chan *BatchAckMessage
	BatchRequestMessages() <-chan *BatchRequestMessage
	HeaderMessages() <-chan *HeaderMessage
	VoteMessages() <-chan *VoteMessage
	CertificateMessages() <-chan *CertificateMessage
	SyncRequests() <-chan *SyncRequest
	SyncResponses() <-chan *SyncResponseMessage

	// ValidatorID returns this node's validator ID.
	ValidatorID() uint16

	// Start starts the network.
	Start() error

	// Stop stops the network.
	Stop() error
}

// Config contains network configuration.
type Config struct {
	// BufferSize is the size of message channel buffers.
	BufferSize int
	// SyncBatchSize is the max certificates per sync response.
	SyncBatchSize int
	// SyncTimeout is the timeout for sync requests.
	SyncTimeout int // milliseconds
}

// DefaultConfig returns default network configuration.
func DefaultConfig() Config {
	return Config{
		BufferSize:    1000,
		SyncBatchSize: 100,
		SyncTimeout:   30000, // 30 seconds
	}
}
