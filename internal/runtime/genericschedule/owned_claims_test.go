package genericschedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type ownedClaimsStore struct {
	*lifecycleProofStore
	mu              sync.Mutex
	activations     map[string]Activation
	held            map[Wakeup]bool
	releaseFailures map[Wakeup]int
	released        []Wakeup
	globalReleases  int
	claimRefused    bool
	claimStarted    chan struct{}
	claimContinue   chan struct{}
	prepareStarted  chan struct{}
	prepareContinue chan struct{}
	callbackStop    *Lifecycle
	callbackStopErr error
}

func (s *ownedClaimsStore) LoadGenericScheduleActivation(_ context.Context, id string) (Activation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	activation, found := s.activations[id]
	return activation, found, nil
}

func (s *ownedClaimsStore) ClaimGenericScheduleWakeup(_ context.Context, wakeup Wakeup) (bool, error) {
	if s.claimStarted != nil {
		close(s.claimStarted)
		<-s.claimContinue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimRefused {
		return false, nil
	}
	s.held[wakeup] = true
	return true, nil
}

func (s *ownedClaimsStore) ReleaseGenericScheduleWakeup(_ context.Context, wakeup Wakeup) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.releaseFailures[wakeup] > 0 {
		s.releaseFailures[wakeup]--
		return errors.New("exact owned claim release failed")
	}
	s.released = append(s.released, wakeup)
	delete(s.held, wakeup)
	return nil
}

func (s *ownedClaimsStore) ReleaseGenericScheduleClaims(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.globalReleases++
	clear(s.held)
	return errors.New("lifecycle must not release shared-store claims globally")
}

func (s *ownedClaimsStore) PrepareGenericScheduleOccurrence(ctx context.Context, _ Wakeup) (PreparationCommit, error) {
	if s.callbackStop != nil {
		s.callbackStopErr = s.callbackStop.Stop(ctx)
	}
	if s.prepareStarted != nil {
		close(s.prepareStarted)
		<-s.prepareContinue
	}
	return PreparationCommit{Result: PreparedOccurrence{Outcome: PrepareTerminal}, Acknowledged: true}, nil
}

func newOwnedClaimsFixture(t *testing.T) (*ownedClaimsStore, Activation, Activation) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	a := testGlobalActivation(t, AbsoluteDue(now.Add(time.Hour)), now, now.Add(time.Hour))
	b := testGlobalActivation(t, AbsoluteDue(now.Add(2*time.Hour)), now, now.Add(2*time.Hour))
	return &ownedClaimsStore{
		lifecycleProofStore: &lifecycleProofStore{},
		activations:         map[string]Activation{a.ID: a, b.ID: b},
		held:                make(map[Wakeup]bool), releaseFailures: make(map[Wakeup]int),
	}, a, b
}

func newOwnedClaimsLifecycle(t *testing.T, store *ownedClaimsStore, scheduler Scheduler) *Lifecycle {
	t.Helper()
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
	return lifecycle
}

func requireOwnedClaims(t *testing.T, store *ownedClaimsStore, expected ...Wakeup) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.globalReleases != 0 || len(store.held) != len(expected) {
		t.Fatalf("shared-store claims changed: held=%v global releases=%d", store.held, store.globalReleases)
	}
	for _, wakeup := range expected {
		if !store.held[wakeup] {
			t.Fatalf("sibling claim %v was released", wakeup)
		}
	}
}

func TestLifecycleStopReleasesOnlyItsOwnedClaims(t *testing.T) {
	store, a, b := newOwnedClaimsFixture(t)
	first := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	second := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	for _, item := range []struct {
		lifecycle  *Lifecycle
		activation Activation
	}{{first, a}, {second, b}} {
		if err := item.lifecycle.ReconcileWakeup(t.Context(), item.activation.ID); err != nil {
			t.Fatal(err)
		}
	}
	aWakeup, _ := a.Wakeup()
	bWakeup, _ := b.Wakeup()
	requireOwnedClaims(t, store, aWakeup, bWakeup)
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, bWakeup)
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, bWakeup)
	if err := second.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.released) != 2 || store.released[0] != aWakeup || store.released[1] != bWakeup {
		t.Fatalf("owned release inventory lost exact coordinates: %v", store.released)
	}
}

func TestLifecycleUnclaimedStopPreservesSiblingClaim(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	first := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	second := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	if err := second.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	wakeup, _ := a.Wakeup()
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, wakeup)
}

func TestLifecycleForeignTerminalReadbackCannotReleaseSiblingClaim(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	first := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	second := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	if err := second.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	wakeup, _ := a.Wakeup()
	a.Status, a.CancelCause, a.CancelledAt = StatusCancelled, "operator_cancelled", time.Now().UTC().Truncate(time.Microsecond)
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.activations[a.ID] = a
	store.mu.Unlock()
	if err := first.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	first.handleWakeup(t.Context(), wakeup)
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, wakeup)
	if len(store.released) != 0 {
		t.Fatal("foreign terminal readback acquired native release authority")
	}
}

func TestLifecycleUnownedDueCoordinateCannotReleaseCurrentClaim(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	lifecycle := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	current, _ := a.Wakeup()
	foreign, err := NewWakeup(a.ID, a.CurrentDueAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.releaseWakeup(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, current)
	if len(store.released) != 0 {
		t.Fatal("unowned due coordinate released this activation's current claim")
	}
	if err := lifecycle.releaseWakeup(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.releaseWakeup(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if len(store.released) != 1 {
		t.Fatal("successful exact release retained native release authority")
	}
}

func TestLifecycleNilInventoryIsInitializedOnlyAfterSuccessfulClaim(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	lifecycle := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	lifecycle.ownedClaims = nil
	store.claimRefused = true
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	if lifecycle.ownedClaims != nil {
		t.Fatal("unacquired claim minted cleanup inventory")
	}
	store.claimRefused = false
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	wakeup, _ := a.Wakeup()
	if _, owned := lifecycle.ownedClaims[wakeup]; !owned {
		t.Fatal("successful claim lost exact cleanup inventory")
	}
}

func TestLifecycleFailedOwnedReleaseRemainsRetryable(t *testing.T) {
	store, a, b := newOwnedClaimsFixture(t)
	first := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	second := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	if err := first.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	if err := second.ReconcileWakeup(t.Context(), b.ID); err != nil {
		t.Fatal(err)
	}
	aWakeup, _ := a.Wakeup()
	bWakeup, _ := b.Wakeup()
	store.releaseFailures[aWakeup] = 1
	if err := first.Stop(t.Context()); err == nil {
		t.Fatal("failed release was acknowledged as complete cleanup")
	}
	requireOwnedClaims(t, store, aWakeup, bWakeup)
	first.claimsMu.Lock()
	_, retained := first.ownedClaims[aWakeup]
	first.claimsMu.Unlock()
	if !retained {
		t.Fatal("failed release lost lifecycle cleanup ownership")
	}
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store, bWakeup)
}

func TestLifecycleRegistrationReleaseFailureRetainsOwnedClaim(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	wakeup, _ := a.Wakeup()
	store.releaseFailures[wakeup] = 1
	registrationErr := errors.New("register wakeup failed")
	lifecycle := newOwnedClaimsLifecycle(t, store, &lifecycleProofScheduler{registerErrs: []error{registrationErr}})
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); !errors.Is(err, registrationErr) {
		t.Fatalf("registration failure lost: %v", err)
	}
	requireOwnedClaims(t, store, wakeup)
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
}

func TestLifecycleUnacquiredWakeupDoesNotEnterCleanupInventory(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	store.claimRefused = true
	lifecycle := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
	if len(store.released) != 0 {
		t.Fatal("unacquired wakeup was released as lifecycle-owned")
	}
}

func TestLifecycleReentrantClaimIsReleasedOnce(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	lifecycle := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	for range 2 {
		if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
	if len(store.released) != 1 {
		t.Fatal("reentrant claim acquired duplicate cleanup ownership")
	}
}

func TestLifecycleStopJoinsInFlightClaimBeforeRelease(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	store.claimStarted, store.claimContinue = make(chan struct{}), make(chan struct{})
	lifecycle := newOwnedClaimsLifecycle(t, store, &restoreScheduler{})
	done := make(chan error, 1)
	go func() { done <- lifecycle.ReconcileWakeup(context.Background(), a.ID) }()
	<-store.claimStarted
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lifecycle.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop did not join pending claim admission: %v", err)
	}
	close(store.claimContinue)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wakeup, _ := a.Wakeup()
	requireOwnedClaims(t, store, wakeup)
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
}

func TestLifecycleStopJoinsCallbackBeforeRelease(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	store.prepareStarted, store.prepareContinue = make(chan struct{}), make(chan struct{})
	scheduler := &restoreScheduler{}
	lifecycle := newOwnedClaimsLifecycle(t, store, scheduler)
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	wakeup, _ := a.Wakeup()
	done := make(chan struct{})
	go func() { defer close(done); scheduler.callback(context.Background(), wakeup) }()
	<-store.prepareStarted
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lifecycle.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop did not join current callback: %v", err)
	}
	requireOwnedClaims(t, store, wakeup)
	close(store.prepareContinue)
	<-done
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireOwnedClaims(t, store)
	// A scheduler callback arriving after Stop cannot reacquire work or release a sibling.
	scheduler.callback(context.Background(), wakeup)
	if len(store.released) != 1 {
		t.Fatal("stopped callback repeated exact claim retirement")
	}
}

func TestLifecycleCallbackCannotJoinItsOwnStop(t *testing.T) {
	store, a, _ := newOwnedClaimsFixture(t)
	scheduler := &restoreScheduler{}
	lifecycle := newOwnedClaimsLifecycle(t, store, scheduler)
	if err := lifecycle.ReconcileWakeup(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	store.callbackStop = lifecycle
	wakeup, _ := a.Wakeup()
	scheduler.callback(t.Context(), wakeup)
	if !errors.Is(store.callbackStopErr, errLifecycleCallbackStop) || lifecycle.stop {
		t.Fatalf("callback attempted self-join: err=%v stopped=%t", store.callbackStopErr, lifecycle.stop)
	}
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
