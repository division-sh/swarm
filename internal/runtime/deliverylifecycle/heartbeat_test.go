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

func TestClaimHeartbeatRenewalHandoffPausesAndResumesGenerationOwner(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{renewed: make(chan int, 8)}
	heartbeat, err := startClaimHeartbeat(context.Background(), owner, store, heartbeatTestClaim(), 2*time.Millisecond)
	if err != nil {
		t.Fatalf("start heartbeat: %v", err)
	}
	if contextual, ok := ClaimHeartbeatFromContext(heartbeat.Context()); !ok || contextual != heartbeat {
		t.Fatal("heartbeat context did not retain the exact renewal owner")
	}
	handoff, err := heartbeat.BeginRenewalHandoff()
	if err != nil {
		t.Fatalf("begin renewal handoff: %v", err)
	}
	drained := false
	for !drained {
		select {
		case <-store.renewed:
		default:
			drained = true
		}
	}
	baseline := store.renewalCount()
	select {
	case renewal := <-store.renewed:
		if renewal > baseline {
			t.Fatalf("generation heartbeat renewed during handoff: renewal=%d baseline=%d", renewal, baseline)
		}
	case <-time.After(20 * time.Millisecond):
	}
	handoff.Finish()
	for store.renewalCount() == baseline {
		select {
		case <-store.renewed:
		case <-time.After(time.Second):
			t.Fatal("generation heartbeat did not resume after handoff")
		}
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop heartbeat: %v", err)
	}
}

func TestClaimHeartbeatCommittedSettlementKeepsHandlerContextUntilPostCommitJoin(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	store := &heartbeatTestStore{}
	claim := heartbeatTestClaim()
	heartbeat, err := startClaimHeartbeat(context.Background(), owner, store, claim, DefaultLeaseTTL-time.Minute)
	if err != nil {
		t.Fatalf("start heartbeat: %v", err)
	}
	contextClaim, ok := ClaimFromContext(heartbeat.Context())
	if !ok || !contextClaim.Same(claim) {
		t.Fatal("heartbeat handler context did not retain the exact claim")
	}
	guard, err := heartbeat.BeginSettlement()
	if err != nil {
		t.Fatalf("begin settlement: %v", err)
	}
	if err := guard.MarkCommitted(); err != nil {
		t.Fatalf("mark committed settlement: %v", err)
	}
	select {
	case <-heartbeat.Context().Done():
		t.Fatal("committed settlement canceled handler before post-commit work returned")
	case <-time.After(20 * time.Millisecond):
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("join committed heartbeat: %v", err)
	}
	select {
	case <-heartbeat.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("joined heartbeat left handler context live")
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

func TestClaimSameIncludesOpaqueFencingToken(t *testing.T) {
	claim := heartbeatTestClaim()
	if !claim.Same(claim) {
		t.Fatal("exact claim did not match itself")
	}
	foreign := claim
	foreign.token = uuid.NewString()
	if claim.Same(foreign) {
		t.Fatal("claim matched a foreign fencing token")
	}
}
