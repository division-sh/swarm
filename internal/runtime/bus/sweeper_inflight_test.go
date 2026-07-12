package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
)

type inFlightSweepStore struct {
	event  events.PersistedReplayEvent
	claims atomic.Int32
}

func (*inFlightSweepStore) AppendEvent(context.Context, events.Event) error { return nil }

func (*inFlightSweepStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (*inFlightSweepStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *inFlightSweepStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return []events.PersistedReplayEvent{s.event}, nil
}

func (s *inFlightSweepStore) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	s.claims.Add(1)
	return nil, false, nil
}

func TestSweepUndispatchedSkipsForegroundInFlightEvent(t *testing.T) {
	evt := eventtest.RootIngress(
		"evt-in-flight",
		events.EventType("custom.in_flight"),
		"test",
		"",
		[]byte(`{}`),
		0,
		"run-in-flight",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	store := &inFlightSweepStore{event: events.PersistedReplayEvent{Event: evt}}
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.beginEventPublish(evt.ID())
	defer eb.endEventPublish(evt.ID())

	got, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if got != 0 {
		t.Fatalf("SweepUndispatched recovered = %d, want 0 for foreground in-flight event", got)
	}
	if claims := store.claims.Load(); claims != 0 {
		t.Fatalf("pipeline replay claims = %d, want 0 for foreground in-flight event", claims)
	}
}
