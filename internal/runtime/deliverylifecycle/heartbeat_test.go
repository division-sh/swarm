package deliverylifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
)

type heartbeatTestStore struct {
	Store

	mu       sync.Mutex
	renewals int
	failAt   int
	renewed  chan int
}

func (s *heartbeatTestStore) RenewClaim(context.Context, Claim) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.renewed != nil {
		select {
		case s.renewed <- s.renewals:
		default:
		}
	}
	if s.failAt > 0 && s.renewals >= s.failAt {
		return Snapshot{}, errors.New("renewal rejected")
	}
	now := time.Now().UTC()
	return Snapshot{UpdatedAt: now, ClaimExpiresAt: now.Add(DefaultLeaseTTL)}, nil
}

func (s *heartbeatTestStore) renewalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewals
}

func TestClaimHeartbeatRenewsBeforeDuringAndAfterExecution(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{renewed: make(chan int, 4)}
	heartbeat, err := startClaimHeartbeat(context.Background(), owner, store, heartbeatTestClaim(), 5*time.Millisecond)
	if err != nil {
		t.Fatalf("start heartbeat: %v", err)
	}
	for store.renewalCount() < 2 {
		select {
		case <-store.renewed:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for periodic renewal")
		}
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop heartbeat: %v", err)
	}
	if got := store.renewalCount(); got < 3 {
		t.Fatalf("renewals = %d, want immediate, periodic, and final renewal", got)
	}
	if err := owner.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("wait for heartbeat ownership: %v", err)
	}
}

func TestClaimHeartbeatDerivesDefaultCadenceFromRenewedLease(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{}
	heartbeat, err := StartClaimHeartbeat(context.Background(), owner, store, heartbeatTestClaim())
	if err != nil {
		t.Fatalf("start default heartbeat: %v", err)
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop default heartbeat: %v", err)
	}
	if got := store.renewalCount(); got != 2 {
		t.Fatalf("renewals = %d, want immediate and final renewal", got)
	}
}

func TestClaimHeartbeatCancelsHandlerWhenRenewalFails(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{failAt: 2}
	heartbeat, err := startClaimHeartbeat(context.Background(), owner, store, heartbeatTestClaim(), time.Millisecond)
	if err != nil {
		t.Fatalf("start heartbeat: %v", err)
	}
	select {
	case <-heartbeat.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled after renewal failure")
	}
	if err := heartbeat.Stop(); err == nil || !strings.Contains(err.Error(), "renew delivery claim during execution") {
		t.Fatalf("stop heartbeat error = %v, want renewal failure", err)
	}
	if err := owner.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("wait for failed heartbeat ownership: %v", err)
	}
}

func TestClaimHeartbeatFailsClosedWhenFinalRenewalIsRejected(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{failAt: 2}
	heartbeat, err := startClaimHeartbeat(context.Background(), owner, store, heartbeatTestClaim(), DefaultLeaseTTL-time.Minute)
	if err != nil {
		t.Fatalf("start heartbeat: %v", err)
	}
	if err := heartbeat.Stop(); err == nil || !strings.Contains(err.Error(), "renew delivery claim after execution") {
		t.Fatalf("stop heartbeat error = %v, want final renewal failure", err)
	}
	if err := owner.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("wait for final-renewal heartbeat ownership: %v", err)
	}
}

func newHeartbeatTestOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "delivery-heartbeat-runtime",
		BundleHash:        "delivery-heartbeat-bundle",
	})
	if err != nil {
		t.Fatalf("new heartbeat work owner: %v", err)
	}
	t.Cleanup(func() {
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire heartbeat work owner: %v", err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Errorf("join heartbeat process owner: %v", err)
		}
	})
	return owner
}

func heartbeatTestClaim() Claim {
	return Claim{
		deliveryID:    uuid.NewString(),
		runID:         uuid.NewString(),
		routeIdentity: "agent\x00agent-a",
		token:         uuid.NewString(),
		version:       1,
		class:         SubscriberAgent,
		subscriberID:  "agent-a",
	}
}
