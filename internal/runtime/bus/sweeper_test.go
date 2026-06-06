package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type sweeperTestStore struct {
	events      []events.PersistedReplayEvent
	deliveries  map[string][]string
	scopes      map[string]runtimereplayclaim.CommittedReplayScope
	receipts    map[string]string
	receiptErrs map[string]string
	claimMu     sync.Mutex
	claimed     map[string]bool
	releaseGate chan struct{}
	releasing   chan struct{}
}

type sweeperMissingClaimStore struct {
	events     []events.PersistedReplayEvent
	deliveries map[string][]string
}

func (s *sweeperTestStore) AppendEvent(context.Context, events.Event) error { return nil }
func (s *sweeperTestStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (s *sweeperTestStore) UpsertPipelineReceipt(_ context.Context, eventID, status, errText string) error {
	if s.receipts == nil {
		s.receipts = map[string]string{}
	}
	if s.receiptErrs == nil {
		s.receiptErrs = map[string]string{}
	}
	s.receipts[eventID] = status
	s.receiptErrs[eventID] = errText
	return nil
}
func (s *sweeperTestStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.events...), nil
}
func (s *sweeperTestStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), s.deliveries[eventID]...), nil
}
func (s *sweeperTestStore) LoadCommittedReplayScope(_ context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	scope, ok := s.scopes[eventID]
	if !ok {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	return scope, nil
}
func (s *sweeperTestStore) ClaimPipelineReplay(_ context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if s.claimed == nil {
		s.claimed = map[string]bool{}
	}
	if s.claimed[eventID] {
		return nil, false, nil
	}
	s.claimed[eventID] = true
	return sweeperClaimLease{store: s, eventID: eventID}, true, nil
}

func (s *sweeperMissingClaimStore) AppendEvent(context.Context, events.Event) error { return nil }
func (s *sweeperMissingClaimStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (s *sweeperMissingClaimStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.events...), nil
}
func (s *sweeperMissingClaimStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), s.deliveries[eventID]...), nil
}

type sweeperClaimLease struct {
	store   *sweeperTestStore
	eventID string
}

func (l sweeperClaimLease) Release(context.Context) error {
	if l.store == nil {
		return nil
	}
	if l.store.releasing != nil {
		select {
		case l.store.releasing <- struct{}{}:
		default:
		}
	}
	if l.store.releaseGate != nil {
		<-l.store.releaseGate
	}
	l.store.claimMu.Lock()
	delete(l.store.claimed, l.eventID)
	l.store.claimMu.Unlock()
	return nil
}

func TestSweepUndispatchedUsesPersistedDeliveryRecipients(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-1",
				events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-1")},
		},
		deliveries: map[string][]string{"evt-1": {"agent-a"}},
		scopes:     map[string]runtimereplayclaim.CommittedReplayScope{"evt-1": runtimereplayclaim.CommittedReplayScopeSubscribed},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-a")

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-1"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	evt := requireBusEvent(t, ch, "swept subscribed delivery")
	if evt.ID() != "evt-1" {
		t.Fatalf("delivered event id = %q, want evt-1", evt.ID())
	}
}

func TestSweepUndispatched_UsesAuthoritativeEmptyFanOutWithoutSubscribedFallback(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-2",
				events.EventType("custom.routed"), "", "", []byte(`{"entity_id":"ent-2"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-2")},
		},
		deliveries: map[string][]string{},
		scopes:     map[string]runtimereplayclaim.CommittedReplayScope{"evt-2": runtimereplayclaim.CommittedReplayScopeDirect},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-b", events.EventType("custom.routed"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-2"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	requireNoBusEvent(t, ch, "empty direct fan-out replay")
}

func TestSweepUndispatched_ReplaysSubscribedInternalOnlyUsingReplayScope(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-internal-only",
				events.EventType("custom.internal"), "", "", []byte(`{"entity_id":"ent-internal"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-internal")},
		},
		deliveries: map[string][]string{},
		scopes:     map[string]runtimereplayclaim.CommittedReplayScope{"evt-internal-only": runtimereplayclaim.CommittedReplayScopeSubscribed},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.internal"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-internal-only"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	evt := requireBusEvent(t, internalCh, "internal-only replay delivery")
	if evt.ID() != "evt-internal-only" {
		t.Fatalf("delivered event id = %q, want evt-internal-only", evt.ID())
	}
}

func TestSweepUndispatched_ReplaysSubscribedMixedRecipientsUsingReplayScope(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-mixed",
				events.EventType("custom.mixed"), "", "", []byte(`{"entity_id":"ent-mixed"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-mixed")},
		},
		deliveries: map[string][]string{"evt-mixed": {"agent-a"}},
		scopes:     map[string]runtimereplayclaim.CommittedReplayScope{"evt-mixed": runtimereplayclaim.CommittedReplayScopeSubscribed},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.mixed"))
	agentCh := eb.Subscribe("agent-a", events.EventType("custom.mixed"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	evt := requireBusEvent(t, internalCh, "mixed replay delivery to internal subscriber")
	if evt.ID() != "evt-mixed" {
		t.Fatalf("internal delivered event id = %q, want evt-mixed", evt.ID())
	}
	evt = requireBusEvent(t, agentCh, "mixed replay delivery to persisted agent")
	if evt.ID() != "evt-mixed" {
		t.Fatalf("agent delivered event id = %q, want evt-mixed", evt.ID())
	}
}

func TestSweepUndispatched_DirectScopeDoesNotBroadenToCurrentInternalSubscribers(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-direct-mixed",
				events.EventType("custom.direct"), "", "", []byte(`{"entity_id":"ent-direct"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-direct")},
		},
		deliveries: map[string][]string{"evt-direct-mixed": {"agent-a"}},
		scopes: map[string]runtimereplayclaim.CommittedReplayScope{
			"evt-direct-mixed": runtimereplayclaim.CommittedReplayScopeDirect,
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.direct"))
	agentCh := eb.Subscribe("agent-a", events.EventType("custom.direct"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	evt := requireBusEvent(t, agentCh, "direct replay delivery to persisted agent")
	if evt.ID() != "evt-direct-mixed" {
		t.Fatalf("agent delivered event id = %q, want evt-direct-mixed", evt.ID())
	}
	requireNoBusEvent(t, internalCh, "direct replay delivery to current internal subscriber")
}

func TestSweepUndispatched_DirectEmptyManifestDoesNotBroadenToCurrentInternalSubscribers(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-direct-empty",
				events.EventType("custom.direct.empty"), "", "", []byte(`{"entity_id":"ent-direct-empty"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-direct-empty")},
		},
		deliveries: map[string][]string{},
		scopes: map[string]runtimereplayclaim.CommittedReplayScope{
			"evt-direct-empty": runtimereplayclaim.CommittedReplayScopeDirect,
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.direct.empty"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-direct-empty"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	requireNoBusEvent(t, internalCh, "direct empty manifest delivery to current internal subscriber")
}

func TestSweepUndispatched_SkipsMalformedReplayRowsAndContinues(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{
				Event: events.NewProjectionEvent("evt-bad",
					events.EventType("custom.bad"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),

				ReplayError: "missing canonical run_id",
			},
			{
				Event: (events.NewProjectionEvent("evt-good",
					events.EventType("custom.good"), "", "", []byte(`{"entity_id":"ent-good"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-good"),
			},
		},
		deliveries: map[string][]string{"evt-good": {"agent-good"}},
		scopes:     map[string]runtimereplayclaim.CommittedReplayScope{"evt-good": runtimereplayclaim.CommittedReplayScopeSubscribed},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-good")

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-bad"]; got != "error" {
		t.Fatalf("bad receipt status = %q, want error", got)
	}
	if got := store.receiptErrs["evt-bad"]; got != "missing canonical run_id" {
		t.Fatalf("bad receipt error = %q, want missing canonical run_id", got)
	}
	if got := store.receipts["evt-good"]; got != "processed" {
		t.Fatalf("good receipt status = %q, want processed", got)
	}
	evt := requireBusEvent(t, ch, "good replay delivery after malformed row")
	if evt.ID() != "evt-good" {
		t.Fatalf("delivered event id = %q, want evt-good", evt.ID())
	}
}

func TestSweepUndispatched_TerminallyMarksMissingCommittedReplayScopeAndContinues(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: events.NewProjectionEvent("evt-markerless",
				events.EventType("custom.markerless"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
			},
			{Event: events.NewProjectionEvent("evt-good-after-markerless",
				events.EventType("custom.good"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
			},
		},
		deliveries: map[string][]string{
			"evt-markerless":            {"agent-missing"},
			"evt-good-after-markerless": {"agent-good"},
		},
		scopes: map[string]runtimereplayclaim.CommittedReplayScope{
			"evt-good-after-markerless": runtimereplayclaim.CommittedReplayScopeSubscribed,
		},
	}
	logger := &recordingLoggerHook{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{Logger: logger})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	goodCh := eb.Subscribe("agent-good")
	missingCh := eb.Subscribe("agent-missing")

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-markerless"]; got != "error" {
		t.Fatalf("markerless receipt status = %q, want error", got)
	}
	if got := store.receiptErrs["evt-markerless"]; got != runtimereplayclaim.ErrMissingCommittedReplayScope.Error() {
		t.Fatalf("markerless receipt error = %q, want missing committed replay scope", got)
	}
	if got := store.receipts["evt-good-after-markerless"]; got != "processed" {
		t.Fatalf("good receipt status = %q, want processed", got)
	}
	foundTerminalLog := false
	for _, entry := range logger.entries {
		if entry.Action == "outbox_replay_scope_unavailable" {
			foundTerminalLog = true
		}
		if entry.Action == "outbox_sweep_failed" {
			t.Fatal("SweepUndispatched should not emit the global sweep-failed warning for terminal markerless rows")
		}
	}
	if !foundTerminalLog {
		t.Fatal("expected explicit terminal committed replay-scope warning")
	}
	requireNoBusEvent(t, missingCh, "markerless replay delivery to missing subscriber")
	evt := requireBusEvent(t, goodCh, "good replay delivery after markerless row")
	if evt.ID() != "evt-good-after-markerless" {
		t.Fatalf("delivered event id = %q, want evt-good-after-markerless", evt.ID())
	}
}

func TestSweepUndispatched_ClaimsReplayOwnershipBeforeDispatch(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.NewProjectionEvent("evt-claim",
				events.EventType("custom.claimed"), "", "", []byte(`{"entity_id":"ent-claim"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID("ent-claim")},
		},
		deliveries:  map[string][]string{"evt-claim": {"agent-claim"}},
		scopes:      map[string]runtimereplayclaim.CommittedReplayScope{"evt-claim": runtimereplayclaim.CommittedReplayScopeSubscribed},
		releaseGate: make(chan struct{}),
		releasing:   make(chan struct{}, 1),
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-claim")

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if _, err := eb.SweepUndispatched(context.Background(), time.Hour, 10); err != nil {
			t.Errorf("first SweepUndispatched: %v", err)
		}
	}()

	requireSignalBefore(t, store.releasing, time.Second, "first sweep replay claim release")

	secondCount, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("second SweepUndispatched: %v", err)
	}
	if secondCount != 0 {
		t.Fatalf("second swept count = %d, want 0 while claim is held", secondCount)
	}

	close(store.releaseGate)
	requireSignalBefore(t, firstDone, time.Second, "first sweep completion")

	evt := requireBusEvent(t, ch, "claimed replay delivery")
	if evt.ID() != "evt-claim" {
		t.Fatalf("delivered event id = %q, want evt-claim", evt.ID())
	}
	requireNoBusEvent(t, ch, "duplicate claimed replay delivery")
}

func TestSweepUndispatched_FailsClosedWithoutReplayClaimOwner(t *testing.T) {
	store := &sweeperMissingClaimStore{
		events: []events.PersistedReplayEvent{
			{Event: events.NewProjectionEvent("evt-claim-missing",
				events.EventType("custom.claimed"), "", "", []byte(`{"entity_id":"ent-claim"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
			},
		},
		deliveries: map[string][]string{"evt-claim-missing": {"agent-a"}},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	_, err = eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err == nil {
		t.Fatal("expected SweepUndispatched to fail without replay claim owner")
	}
	if got := err.Error(); got != "store does not support explicit pipeline replay claims" {
		t.Fatalf("SweepUndispatched error = %q, want explicit replay claim owner failure", got)
	}
}

func TestSweepUndispatched_NoopsWithoutPersistedReplayStore(t *testing.T) {
	eb, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 0 {
		t.Fatalf("swept count = %d, want 0", count)
	}
}
