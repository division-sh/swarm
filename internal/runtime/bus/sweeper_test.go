package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
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
			{Event: (events.Event{
				ID:        "evt-1",
				Type:      events.EventType("custom.emitted"),
				Payload:   []byte(`{"entity_id":"ent-1"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-1")},
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
	select {
	case evt := <-ch:
		if evt.ID != "evt-1" {
			t.Fatalf("delivered event id = %q, want evt-1", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected swept delivery")
	}
}

func TestSweepUndispatched_UsesAuthoritativeEmptyFanOutWithoutSubscribedFallback(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-2",
				Type:      events.EventType("custom.routed"),
				Payload:   []byte(`{"entity_id":"ent-2"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-2")},
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
	waitForNoEvent(t, ch)
}

func TestSweepUndispatched_ReplaysSubscribedInternalOnlyUsingReplayScope(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-internal-only",
				Type:      events.EventType("custom.internal"),
				Payload:   []byte(`{"entity_id":"ent-internal"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-internal")},
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
	select {
	case evt := <-internalCh:
		if evt.ID != "evt-internal-only" {
			t.Fatalf("delivered event id = %q, want evt-internal-only", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected internal subscriber to receive replayed event")
	}
}

func TestSweepUndispatched_ReplaysSubscribedMixedRecipientsUsingReplayScope(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-mixed",
				Type:      events.EventType("custom.mixed"),
				Payload:   []byte(`{"entity_id":"ent-mixed"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-mixed")},
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
	select {
	case evt := <-internalCh:
		if evt.ID != "evt-mixed" {
			t.Fatalf("internal delivered event id = %q, want evt-mixed", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected internal subscriber to receive replayed event")
	}
	select {
	case evt := <-agentCh:
		if evt.ID != "evt-mixed" {
			t.Fatalf("agent delivered event id = %q, want evt-mixed", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected persisted agent to receive replayed event")
	}
}

func TestSweepUndispatched_DirectScopeDoesNotBroadenToCurrentInternalSubscribers(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-direct-mixed",
				Type:      events.EventType("custom.direct"),
				Payload:   []byte(`{"entity_id":"ent-direct"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-direct")},
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
	select {
	case evt := <-agentCh:
		if evt.ID != "evt-direct-mixed" {
			t.Fatalf("agent delivered event id = %q, want evt-direct-mixed", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected direct persisted agent recipient to receive replayed event")
	}
	waitForNoEvent(t, internalCh)
}

func TestSweepUndispatched_DirectEmptyManifestDoesNotBroadenToCurrentInternalSubscribers(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-direct-empty",
				Type:      events.EventType("custom.direct.empty"),
				Payload:   []byte(`{"entity_id":"ent-direct-empty"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-direct-empty")},
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
	waitForNoEvent(t, internalCh)
}

func TestSweepUndispatched_SkipsMalformedReplayRowsAndContinues(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{
				Event: events.Event{
					ID:        "evt-bad",
					Type:      events.EventType("custom.bad"),
					CreatedAt: time.Now().UTC(),
				},
				ReplayError: "missing canonical run_id",
			},
			{
				Event: (events.Event{
					ID:        "evt-good",
					Type:      events.EventType("custom.good"),
					Payload:   []byte(`{"entity_id":"ent-good"}`),
					CreatedAt: time.Now().UTC(),
				}).WithEntityID("ent-good"),
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
	select {
	case evt := <-ch:
		if evt.ID != "evt-good" {
			t.Fatalf("delivered event id = %q, want evt-good", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected good swept delivery")
	}
}

func TestSweepUndispatched_TerminallyMarksMissingCommittedReplayScopeAndContinues(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: events.Event{
				ID:        "evt-markerless",
				Type:      events.EventType("custom.markerless"),
				CreatedAt: time.Now().UTC(),
			}},
			{Event: events.Event{
				ID:        "evt-good-after-markerless",
				Type:      events.EventType("custom.good"),
				CreatedAt: time.Now().UTC(),
			}},
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
	waitForNoEvent(t, missingCh)
	select {
	case evt := <-goodCh:
		if evt.ID != "evt-good-after-markerless" {
			t.Fatalf("delivered event id = %q, want evt-good-after-markerless", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected good event after markerless row to be replayed")
	}
}

func TestSweepUndispatched_ClaimsReplayOwnershipBeforeDispatch(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.PersistedReplayEvent{
			{Event: (events.Event{
				ID:        "evt-claim",
				Type:      events.EventType("custom.claimed"),
				Payload:   []byte(`{"entity_id":"ent-claim"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-claim")},
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

	select {
	case <-store.releasing:
	case <-time.After(time.Second):
		t.Fatal("expected first sweep to reach claim release")
	}

	secondCount, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("second SweepUndispatched: %v", err)
	}
	if secondCount != 0 {
		t.Fatalf("second swept count = %d, want 0 while claim is held", secondCount)
	}

	close(store.releaseGate)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first sweep to finish")
	}

	select {
	case evt := <-ch:
		if evt.ID != "evt-claim" {
			t.Fatalf("delivered event id = %q, want evt-claim", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected claimed replay delivery")
	}
	waitForNoEvent(t, ch)
}

func TestSweepUndispatched_FailsClosedWithoutReplayClaimOwner(t *testing.T) {
	store := &sweeperMissingClaimStore{
		events: []events.PersistedReplayEvent{
			{Event: events.Event{
				ID:        "evt-claim-missing",
				Type:      events.EventType("custom.claimed"),
				Payload:   []byte(`{"entity_id":"ent-claim"}`),
				CreatedAt: time.Now().UTC(),
			}},
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
