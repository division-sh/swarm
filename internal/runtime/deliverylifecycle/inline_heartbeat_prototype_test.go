package deliverylifecycle

// PROTOTYPE ONLY: isolated inline lifecycle proofs, not production adoption.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type inlinePrototypeStore struct {
	Store
	mu        sync.Mutex
	remaining time.Duration
	admitErr  error
	renewErr  error
	admitted  []Claim
	renewed   []Claim
	renewCtx  []error
	entered   chan struct{}
	release   chan struct{}
}

func (s *inlinePrototypeStore) AdmitInlineClaim(ctx context.Context, claim Claim) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admitted = append(s.admitted, claim)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.remaining, s.admitErr
}

func (s *inlinePrototypeStore) RenewClaim(ctx context.Context, claim Claim) (Snapshot, error) {
	s.mu.Lock()
	s.renewed = append(s.renewed, claim)
	s.renewCtx = append(s.renewCtx, ctx.Err())
	err := s.renewErr
	s.mu.Unlock()
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		<-s.release
	}
	now := time.Now().UTC()
	return Snapshot{UpdatedAt: now, ClaimExpiresAt: now.Add(time.Hour)}, err
}

func (s *inlinePrototypeStore) requireCalls(t *testing.T, claim Claim, admissions, renewals int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.admitted) != admissions || len(s.renewed) != renewals {
		t.Fatalf("admissions/renewals = %d/%d, want %d/%d", len(s.admitted), len(s.renewed), admissions, renewals)
	}
	for _, actual := range append(append([]Claim(nil), s.admitted...), s.renewed...) {
		if !actual.Same(claim) {
			t.Fatal("inline lifecycle changed the exact claim")
		}
	}
	for _, err := range s.renewCtx {
		if err != nil {
			t.Fatalf("accepted renewal received canceled context: %v", err)
		}
	}
}

func inlinePrototypeClaim() Claim {
	claim := heartbeatTestClaim()
	claim.class = SubscriberNode
	claim.subscriberID = "prototype-node"
	claim.routeIdentity = "node\x00prototype-node"
	return claim
}

func inlinePrototypeWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("inline lifecycle did not reach the synchronized boundary")
	}
}

func TestInlinePrototypeHeartbeatNoCompulsoryRenewals(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	claim := inlinePrototypeClaim()
	store := &inlinePrototypeStore{remaining: time.Hour}
	h, err := StartInlineClaimHeartbeat(context.Background(), owner, store, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	store.requireCalls(t, claim, 1, 0)
	actual, ok := ClaimFromContext(h.Context())
	if !ok || !actual.Same(claim) || !h.Owns(claim) {
		t.Fatal("inline heartbeat lost its exact claim")
	}
	guard, err := h.BeginSettlement()
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Abort()
	store.requireCalls(t, claim, 1, 0)
	if err := guard.MarkCommitted(); err != nil {
		t.Fatal(err)
	}
	if h.Context().Err() != nil || owner.ActiveCount() != 1 {
		t.Fatal("commit released or canceled post-commit ownership before join")
	}
	if duplicate, err := h.BeginSettlement(); err == nil || duplicate != nil {
		if duplicate != nil {
			duplicate.Abort()
		}
		t.Fatal("committed heartbeat admitted a second settlement")
	}
	for i := 0; i < 2; i++ {
		if err := h.Stop(); err != nil {
			t.Fatal(err)
		}
	}
	store.requireCalls(t, claim, 1, 0)
	if owner.ActiveCount() != 0 || h.Context().Err() == nil {
		t.Fatal("joined heartbeat retained work or a live execution context")
	}
}

func TestInlinePrototypeHeartbeatAdmissionRefusal(t *testing.T) {
	lost := errors.New("prototype lost admission")
	for _, tc := range []struct {
		name      string
		remaining time.Duration
		err       error
		agent     bool
		canceled  bool
	}{
		{name: "lost_claim", remaining: time.Hour, err: lost},
		{name: "expired"},
		{name: "negative_remaining", remaining: -time.Second},
		{name: "agent_rejected", remaining: time.Hour, agent: true},
		{name: "canceled_before_admission", remaining: time.Hour, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newHeartbeatTestOwner(t)
			claim := inlinePrototypeClaim()
			if tc.agent {
				claim = heartbeatTestClaim()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			store := &inlinePrototypeStore{remaining: tc.remaining, admitErr: tc.err}
			h, err := StartInlineClaimHeartbeat(ctx, owner, store, claim)
			if h != nil {
				_ = h.Stop()
				t.Fatal("refused admission returned a heartbeat")
			}
			if err == nil || (tc.err != nil && !errors.Is(err, tc.err)) {
				t.Fatalf("admission error = %v, want refusal with original cause %v", err, tc.err)
			}
			if tc.canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation cause lost: %v", err)
			}
			admissions := 1
			if tc.agent {
				admissions = 0
			}
			store.requireCalls(t, claim, admissions, 0)
			if owner.ActiveCount() != 0 {
				t.Fatal("refused admission leaked occurrence ownership")
			}
		})
	}
}

func TestInlinePrototypeHeartbeatPeriodicRenewalAndFailure(t *testing.T) {
	lost := errors.New("prototype renewal fenced")
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "long_preparation", true: "lost_periodic_renewal"}[fail], func(t *testing.T) {
			owner := newHeartbeatTestOwner(t)
			claim := inlinePrototypeClaim()
			store := &inlinePrototypeStore{remaining: time.Hour, entered: make(chan struct{}, 8)}
			if fail {
				store.renewErr = lost
			}
			h, err := startClaimHeartbeatMode(context.Background(), owner, store, claim, time.Millisecond, true)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Stop() })
			inlinePrototypeWait(t, store.entered)
			if fail {
				inlinePrototypeWait(t, h.Context().Done())
				if !errors.Is(context.Cause(h.Context()), lost) {
					t.Fatalf("handler cancellation lost renewal cause: %v", context.Cause(h.Context()))
				}
				if guard, err := h.BeginSettlement(); !errors.Is(err, lost) || guard != nil {
					if guard != nil {
						guard.Abort()
					}
					t.Fatalf("failed renewal was rescued by settlement: %v", err)
				}
				if err := h.Stop(); !errors.Is(err, lost) {
					t.Fatalf("stop lost renewal error: %v", err)
				}
				store.requireCalls(t, claim, 1, 1)
			} else {
				guard, err := h.BeginSettlement()
				if err != nil {
					t.Fatal(err)
				}
				defer guard.Abort()
				store.mu.Lock()
				before := len(store.renewed)
				store.mu.Unlock()
				if before < 1 {
					t.Fatal("long preparation did not renew")
				}
				if err := guard.Finish(true); err != nil {
					t.Fatal(err)
				}
				store.requireCalls(t, claim, 1, before)
			}
			if owner.ActiveCount() != 0 {
				t.Fatal("periodic heartbeat was not joined")
			}
		})
	}
}

func TestInlinePrototypeHeartbeatSettlementExcludesRenewalAndAbortResumes(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	claim := inlinePrototypeClaim()
	store := &inlinePrototypeStore{remaining: time.Hour}
	h, err := StartInlineClaimHeartbeat(context.Background(), owner, store, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	guard, err := h.BeginSettlement()
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Abort()
	if h.renewMu.TryLock() {
		h.renewMu.Unlock()
		t.Fatal("settlement did not hold renewal exclusion")
	}
	entered, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(entered)
		h.renew(store, claim, "prototype synchronized renewal")
		close(done)
	}()
	inlinePrototypeWait(t, entered)
	select {
	case <-done:
		t.Fatal("renewal crossed held settlement guard")
	case <-time.After(20 * time.Millisecond):
	}
	store.requireCalls(t, claim, 1, 0)
	guard.Abort()
	guard.Abort()
	inlinePrototypeWait(t, done)
	store.requireCalls(t, claim, 1, 1)
	next, err := h.BeginSettlement()
	if err != nil {
		t.Fatal(err)
	}
	defer next.Abort()
	if err := next.Finish(true); err != nil {
		t.Fatal(err)
	}
	store.requireCalls(t, claim, 1, 1)
}

func TestInlinePrototypeHeartbeatSettlementWaitsForAdmittedRenewal(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	claim := inlinePrototypeClaim()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	store := &inlinePrototypeStore{remaining: time.Hour, entered: make(chan struct{}, 1), release: release}
	h, err := StartInlineClaimHeartbeat(context.Background(), owner, store, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	renewed := make(chan struct{})
	go func() {
		h.renew(store, claim, "prototype admitted renewal")
		close(renewed)
	}()
	inlinePrototypeWait(t, store.entered)
	if h.renewMu.TryLock() {
		h.renewMu.Unlock()
		t.Fatal("admitted renewal did not retain exclusion")
	}
	settled := make(chan error, 1)
	go func() {
		guard, err := h.BeginSettlement()
		if err == nil {
			err = guard.Finish(true)
		}
		settled <- err
	}()
	select {
	case err := <-settled:
		t.Fatalf("settlement crossed an in-flight renewal: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	inlinePrototypeWait(t, renewed)
	select {
	case err := <-settled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("settlement failed to resume after renewal")
	}
	store.requireCalls(t, claim, 1, 1)
	if owner.ActiveCount() != 0 {
		t.Fatal("settlement did not join heartbeat")
	}
}

func TestInlinePrototypeHeartbeatRollbackRetainsUnsettledStopRenewal(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	claim := inlinePrototypeClaim()
	store := &inlinePrototypeStore{remaining: time.Hour}
	h, err := StartInlineClaimHeartbeat(context.Background(), owner, store, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	guard, err := h.BeginSettlement()
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Abort()
	store.requireCalls(t, claim, 1, 0)
	if err := guard.Finish(false); err != nil {
		t.Fatal(err)
	}
	store.requireCalls(t, claim, 1, 1)
	if owner.ActiveCount() != 0 {
		t.Fatal("rolled-back settlement leaked accepted work")
	}
}

func TestInlinePrototypeHeartbeatRetirementRetainsSettlementOwnership(t *testing.T) {
	owner := newHeartbeatTestOwner(t)
	claim := inlinePrototypeClaim()
	store := &inlinePrototypeStore{remaining: time.Hour}
	h, err := StartInlineClaimHeartbeat(context.Background(), owner, store, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	owner.Retire()
	inlinePrototypeWait(t, h.done)
	if owner.ActiveCount() != 1 || h.Context().Err() == nil {
		t.Fatal("retirement failed to cancel execution while retaining accepted settlement")
	}
	guard, err := h.BeginSettlement()
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Abort()
	ctx := guard.Context()
	actual, ok := ClaimFromContext(ctx)
	actualOwner, owned := worklifetime.OccurrenceFromContext(ctx)
	if ctx.Err() != nil || !ok || !actual.Same(claim) || !owned || actualOwner != owner {
		t.Fatal("cleanup lost uncanceled exact claim/occurrence context")
	}
	if lease, err := actualOwner.Begin(ctx); err == nil {
		_ = lease.Done()
		t.Fatal("retired settlement context admitted new work")
	}
	// The guard is exclusion, not durable settlement authority. Native-store
	// tests must independently reject a retired grant or expired claim here.
	if err := guard.Finish(true); err != nil {
		t.Fatal(err)
	}
	store.requireCalls(t, claim, 1, 0)
	if owner.ActiveCount() != 0 {
		t.Fatal("retired settlement did not release accepted work")
	}
}
