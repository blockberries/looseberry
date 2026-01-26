package primary

import (
	"testing"
	"time"

	"github.com/blockberries/looseberry/types"
)

func createTestHeader(t *testing.T, author uint16, round uint64) *types.Header {
	t.Helper()
	signer, err := types.GenerateEd25519Signer(author)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}

	header := types.NewHeader(author, round, 0, nil, nil)
	if err := header.Sign(signer); err != nil {
		t.Fatalf("Failed to sign header: %v", err)
	}

	return header
}

func TestVoteTrackerTrackHeader(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	header := createTestHeader(t, 0, 10)
	vt.TrackHeader(header)

	if !vt.HasHeader(header.Digest) {
		t.Error("Header should be tracked")
	}

	retrieved, ok := vt.GetHeader(header.Digest)
	if !ok {
		t.Error("Should get tracked header")
	}

	if !retrieved.Digest.Equal(header.Digest) {
		t.Error("Retrieved header digest mismatch")
	}
}

func TestVoteTrackerRecordVote(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	// Create signers
	signers := make([]*types.Ed25519Signer, 4)
	for i := range 4 {
		signer, _ := types.GenerateEd25519Signer(uint16(i))
		signers[i] = signer
	}

	header := createTestHeader(t, 0, 10)
	vt.TrackHeader(header)

	quorum := 3 // 2f+1 for n=4

	// Record first vote
	vote1 := types.NewVote(header.Digest, 0)
	_ = vote1.Sign(signers[0])
	cert, formed := vt.RecordVote(vote1, quorum)
	if formed {
		t.Error("Should not form certificate with 1 vote")
	}
	if cert != nil {
		t.Error("Certificate should be nil")
	}

	// Record second vote
	vote2 := types.NewVote(header.Digest, 1)
	_ = vote2.Sign(signers[1])
	_, formed = vt.RecordVote(vote2, quorum)
	if formed {
		t.Error("Should not form certificate with 2 votes")
	}

	// Record third vote (quorum)
	vote3 := types.NewVote(header.Digest, 2)
	_ = vote3.Sign(signers[2])
	cert, formed = vt.RecordVote(vote3, quorum)
	if !formed {
		t.Error("Should form certificate with 3 votes (quorum)")
	}
	if cert == nil {
		t.Fatal("Certificate should not be nil")
	}

	if len(cert.Votes) != 3 {
		t.Errorf("Expected 3 votes in certificate, got %d", len(cert.Votes))
	}
}

func TestVoteTrackerDuplicateVote(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	signer, _ := types.GenerateEd25519Signer(0)
	header := createTestHeader(t, 0, 10)
	vt.TrackHeader(header)

	vote := types.NewVote(header.Digest, 0)
	_ = vote.Sign(signer)

	// Record same vote twice
	vt.RecordVote(vote, 3)
	vt.RecordVote(vote, 3)

	if vt.VoteCount(header.Digest) != 1 {
		t.Errorf("Duplicate vote should not be counted, got %d votes", vt.VoteCount(header.Digest))
	}
}

func TestVoteTrackerRemoveHeader(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	header := createTestHeader(t, 0, 10)
	vt.TrackHeader(header)

	if !vt.HasHeader(header.Digest) {
		t.Error("Header should be tracked")
	}

	vt.RemoveHeader(header.Digest)

	if vt.HasHeader(header.Digest) {
		t.Error("Header should be removed")
	}
}

func TestVoteTrackerTimedOut(t *testing.T) {
	timeout := 5 * time.Second
	vt := NewVoteTracker(timeout)
	defer vt.Close()

	header := createTestHeader(t, 0, 10)

	// Manually add with expired timestamp
	vt.mu.Lock()
	vt.pending[header.Digest] = &PendingHeader{
		Header:    header.Clone(),
		Votes:     make(map[uint16]*types.Vote),
		CreatedAt: time.Now().Add(-6 * time.Second),
	}
	vt.mu.Unlock()

	timedOut := vt.GetTimedOut()
	if len(timedOut) != 1 {
		t.Errorf("Expected 1 timed out header, got %d", len(timedOut))
	}
}

func TestVoteTrackerRemoveTimedOut(t *testing.T) {
	timeout := 5 * time.Second
	vt := NewVoteTracker(timeout)
	defer vt.Close()

	header1 := createTestHeader(t, 0, 10)
	header2 := createTestHeader(t, 1, 10)

	// Add header1 with expired timestamp
	vt.mu.Lock()
	vt.pending[header1.Digest] = &PendingHeader{
		Header:    header1.Clone(),
		Votes:     make(map[uint16]*types.Vote),
		CreatedAt: time.Now().Add(-6 * time.Second),
	}
	vt.mu.Unlock()

	// Add header2 normally
	vt.TrackHeader(header2)

	removed := vt.RemoveTimedOut()
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed header, got %d", len(removed))
	}

	if vt.HasHeader(header1.Digest) {
		t.Error("header1 should be removed")
	}

	if !vt.HasHeader(header2.Digest) {
		t.Error("header2 should still be tracked")
	}
}

func TestVoteTrackerIdempotentTrack(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	header := createTestHeader(t, 0, 10)

	vt.TrackHeader(header)
	vt.TrackHeader(header)

	if vt.GetPendingCount() != 1 {
		t.Error("Should only track header once")
	}
}

func TestVoteTrackerUnknownHeader(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)
	defer vt.Close()

	signer, _ := types.GenerateEd25519Signer(0)
	unknownDigest := types.HashBytes([]byte("unknown"))

	vote := types.NewVote(unknownDigest, 0)
	_ = vote.Sign(signer)

	cert, formed := vt.RecordVote(vote, 3)
	if formed {
		t.Error("Should not form certificate for unknown header")
	}
	if cert != nil {
		t.Error("Certificate should be nil for unknown header")
	}

	if vt.VoteCount(unknownDigest) != 0 {
		t.Error("Unknown header should have 0 votes")
	}
}

func TestVoteTrackerClose(t *testing.T) {
	vt := NewVoteTracker(30 * time.Second)

	header := createTestHeader(t, 0, 10)
	vt.TrackHeader(header)

	vt.Close()

	// Operations after close should be no-ops
	vt.TrackHeader(header)
	if vt.pending != nil {
		t.Error("pending should be nil after close")
	}

	signer, _ := types.GenerateEd25519Signer(0)
	vote := types.NewVote(header.Digest, 0)
	_ = vote.Sign(signer)

	cert, formed := vt.RecordVote(vote, 3)
	if formed {
		t.Error("RecordVote after close should return false")
	}
	if cert != nil {
		t.Error("Certificate should be nil after close")
	}
}
