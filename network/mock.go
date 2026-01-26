package network

import (
	"sync"
	"sync/atomic"

	"github.com/blockberries/looseberry/types"
)

// MockNetwork is a mock implementation of Network for testing.
// It supports connecting multiple mock networks together for multi-node testing.
type MockNetwork struct {
	validatorID uint16
	cfg         Config

	// Message channels
	batchCh        chan *BatchMessage
	batchAckCh     chan *BatchAckMessage
	batchReqCh     chan *BatchRequestMessage
	headerCh       chan *HeaderMessage
	voteCh         chan *VoteMessage
	certCh         chan *CertificateMessage
	syncReqCh      chan *SyncRequest
	syncRespCh     chan *SyncResponse

	// Connected peers (for testing multi-node scenarios)
	peers   map[uint16]*MockNetwork
	peersMu sync.RWMutex

	// Lifecycle
	running atomic.Bool
	stopCh  chan struct{}

	// Statistics for testing
	stats    MockNetworkStats
	statsMu  sync.Mutex
}

// MockNetworkStats tracks network statistics for testing.
type MockNetworkStats struct {
	BatchesBroadcast      int
	HeadersBroadcast      int
	CertificatesBroadcast int
	VotesSent             int
	BatchAcksSent         int
	SyncRequestsSent      int
	SyncResponsesSent     int
}

// NewMockNetwork creates a new mock network.
func NewMockNetwork(validatorID uint16, cfg Config) *MockNetwork {
	return &MockNetwork{
		validatorID:    validatorID,
		cfg:            cfg,
		batchCh:        make(chan *BatchMessage, cfg.BufferSize),
		batchAckCh:     make(chan *BatchAckMessage, cfg.BufferSize),
		batchReqCh:     make(chan *BatchRequestMessage, cfg.BufferSize),
		headerCh:       make(chan *HeaderMessage, cfg.BufferSize),
		voteCh:         make(chan *VoteMessage, cfg.BufferSize),
		certCh:         make(chan *CertificateMessage, cfg.BufferSize),
		syncReqCh:      make(chan *SyncRequest, cfg.BufferSize),
		syncRespCh:     make(chan *SyncResponse, cfg.BufferSize),
		peers:          make(map[uint16]*MockNetwork),
		stopCh:         make(chan struct{}),
	}
}

// Connect connects this network to a peer network.
func (m *MockNetwork) Connect(peer *MockNetwork) {
	m.peersMu.Lock()
	m.peers[peer.validatorID] = peer
	m.peersMu.Unlock()

	// Bidirectional connection
	peer.peersMu.Lock()
	peer.peers[m.validatorID] = m
	peer.peersMu.Unlock()
}

// Disconnect disconnects from a peer.
func (m *MockNetwork) Disconnect(validatorID uint16) {
	m.peersMu.Lock()
	peer := m.peers[validatorID]
	delete(m.peers, validatorID)
	m.peersMu.Unlock()

	if peer != nil {
		peer.peersMu.Lock()
		delete(peer.peers, m.validatorID)
		peer.peersMu.Unlock()
	}
}

// Start starts the mock network.
func (m *MockNetwork) Start() error {
	if m.running.Swap(true) {
		return types.ErrAlreadyRunning
	}
	return nil
}

// Stop stops the mock network.
func (m *MockNetwork) Stop() error {
	if !m.running.Swap(false) {
		return types.ErrNotRunning
	}
	close(m.stopCh)
	return nil
}

// ValidatorID returns this node's validator ID.
func (m *MockNetwork) ValidatorID() uint16 {
	return m.validatorID
}

// BroadcastBatch broadcasts a batch to all peers.
func (m *MockNetwork) BroadcastBatch(batch *types.Batch) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.BatchesBroadcast++
	m.statsMu.Unlock()

	msg := &BatchMessage{
		Batch: batch.Clone(),
		From:  m.validatorID,
	}

	m.peersMu.RLock()
	for _, peer := range m.peers {
		select {
		case peer.batchCh <- msg:
		default:
			// Channel full, skip
		}
	}
	m.peersMu.RUnlock()

	return nil
}

// BroadcastHeader broadcasts a header to all peers.
func (m *MockNetwork) BroadcastHeader(header *types.Header) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.HeadersBroadcast++
	m.statsMu.Unlock()

	msg := &HeaderMessage{
		Header: header.Clone(),
		From:   m.validatorID,
	}

	m.peersMu.RLock()
	for _, peer := range m.peers {
		select {
		case peer.headerCh <- msg:
		default:
		}
	}
	m.peersMu.RUnlock()

	return nil
}

// BroadcastCertificate broadcasts a certificate to all peers.
func (m *MockNetwork) BroadcastCertificate(cert *types.Certificate) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.CertificatesBroadcast++
	m.statsMu.Unlock()

	msg := &CertificateMessage{
		Certificate: cert.Clone(),
		From:        m.validatorID,
	}

	m.peersMu.RLock()
	for _, peer := range m.peers {
		select {
		case peer.certCh <- msg:
		default:
		}
	}
	m.peersMu.RUnlock()

	return nil
}

// SendVote sends a vote to a specific validator.
func (m *MockNetwork) SendVote(validator uint16, vote *types.Vote) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.VotesSent++
	m.statsMu.Unlock()

	m.peersMu.RLock()
	peer, ok := m.peers[validator]
	m.peersMu.RUnlock()

	if !ok {
		return types.ErrValidatorNotFound
	}

	msg := &VoteMessage{
		Vote: vote.Clone(),
		From: m.validatorID,
	}

	select {
	case peer.voteCh <- msg:
		return nil
	default:
		return types.ErrMempoolFull
	}
}

// SendBatchAck sends a batch acknowledgment to a specific validator.
func (m *MockNetwork) SendBatchAck(validator uint16, ack *BatchAckMessage) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.BatchAcksSent++
	m.statsMu.Unlock()

	m.peersMu.RLock()
	peer, ok := m.peers[validator]
	m.peersMu.RUnlock()

	if !ok {
		return types.ErrValidatorNotFound
	}

	select {
	case peer.batchAckCh <- ack:
		return nil
	default:
		return types.ErrMempoolFull
	}
}

// SendBatchRequest sends a batch request to a specific validator.
func (m *MockNetwork) SendBatchRequest(validator uint16, req *BatchRequestMessage) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.peersMu.RLock()
	peer, ok := m.peers[validator]
	m.peersMu.RUnlock()

	if !ok {
		return types.ErrValidatorNotFound
	}

	select {
	case peer.batchReqCh <- req:
		return nil
	default:
		return types.ErrMempoolFull
	}
}

// SendSyncRequest sends a sync request to a specific validator.
func (m *MockNetwork) SendSyncRequest(validator uint16, req *SyncRequest) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.SyncRequestsSent++
	m.statsMu.Unlock()

	m.peersMu.RLock()
	peer, ok := m.peers[validator]
	m.peersMu.RUnlock()

	if !ok {
		return types.ErrValidatorNotFound
	}

	select {
	case peer.syncReqCh <- req:
		return nil
	default:
		return types.ErrMempoolFull
	}
}

// SendSyncResponse sends a sync response to a specific validator.
func (m *MockNetwork) SendSyncResponse(validator uint16, resp *SyncResponse) error {
	if !m.running.Load() {
		return types.ErrNotRunning
	}

	m.statsMu.Lock()
	m.stats.SyncResponsesSent++
	m.statsMu.Unlock()

	m.peersMu.RLock()
	peer, ok := m.peers[validator]
	m.peersMu.RUnlock()

	if !ok {
		return types.ErrValidatorNotFound
	}

	select {
	case peer.syncRespCh <- resp:
		return nil
	default:
		return types.ErrMempoolFull
	}
}

// BatchMessages returns the channel for incoming batch messages.
func (m *MockNetwork) BatchMessages() <-chan *BatchMessage {
	return m.batchCh
}

// BatchAckMessages returns the channel for incoming batch ack messages.
func (m *MockNetwork) BatchAckMessages() <-chan *BatchAckMessage {
	return m.batchAckCh
}

// BatchRequestMessages returns the channel for incoming batch request messages.
func (m *MockNetwork) BatchRequestMessages() <-chan *BatchRequestMessage {
	return m.batchReqCh
}

// HeaderMessages returns the channel for incoming header messages.
func (m *MockNetwork) HeaderMessages() <-chan *HeaderMessage {
	return m.headerCh
}

// VoteMessages returns the channel for incoming vote messages.
func (m *MockNetwork) VoteMessages() <-chan *VoteMessage {
	return m.voteCh
}

// CertificateMessages returns the channel for incoming certificate messages.
func (m *MockNetwork) CertificateMessages() <-chan *CertificateMessage {
	return m.certCh
}

// SyncRequests returns the channel for incoming sync requests.
func (m *MockNetwork) SyncRequests() <-chan *SyncRequest {
	return m.syncReqCh
}

// SyncResponses returns the channel for incoming sync responses.
func (m *MockNetwork) SyncResponses() <-chan *SyncResponse {
	return m.syncRespCh
}

// Stats returns the network statistics.
func (m *MockNetwork) Stats() MockNetworkStats {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	return m.stats
}

// ResetStats resets the network statistics.
func (m *MockNetwork) ResetStats() {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	m.stats = MockNetworkStats{}
}

// InjectBatchMessage injects a batch message for testing.
func (m *MockNetwork) InjectBatchMessage(msg *BatchMessage) {
	select {
	case m.batchCh <- msg:
	default:
	}
}

// InjectHeaderMessage injects a header message for testing.
func (m *MockNetwork) InjectHeaderMessage(msg *HeaderMessage) {
	select {
	case m.headerCh <- msg:
	default:
	}
}

// InjectVoteMessage injects a vote message for testing.
func (m *MockNetwork) InjectVoteMessage(msg *VoteMessage) {
	select {
	case m.voteCh <- msg:
	default:
	}
}

// InjectCertificateMessage injects a certificate message for testing.
func (m *MockNetwork) InjectCertificateMessage(msg *CertificateMessage) {
	select {
	case m.certCh <- msg:
	default:
	}
}

// InjectSyncRequest injects a sync request for testing.
func (m *MockNetwork) InjectSyncRequest(req *SyncRequest) {
	select {
	case m.syncReqCh <- req:
	default:
	}
}

// InjectSyncResponse injects a sync response for testing.
func (m *MockNetwork) InjectSyncResponse(resp *SyncResponse) {
	select {
	case m.syncRespCh <- resp:
	default:
	}
}

// PeerCount returns the number of connected peers.
func (m *MockNetwork) PeerCount() int {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	return len(m.peers)
}

// Verify interface compliance
var _ Network = (*MockNetwork)(nil)
