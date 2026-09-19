package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func TestEnginePublicationPreparationBindsExactAdmittedSource(t *testing.T) {
	entityID := eventtest.UUID("prepared-source-entity")
	store := &outboxClaimStore{directRecipientTransactionalStore: directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", entityID, "")},
	}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	runtimebustest.Subscribe(t, eb, "reviewer")
	now := time.Now().UTC()
	makeEvent := func(id, run, payload string, at time.Time) events.Event {
		return eventtest.RunCreatingRootIngress(id, "custom.emitted", "", "", []byte(payload), 0, run, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), at)
	}
	id := eventtest.UUID("prepared-source-event")
	original := makeEvent(id, runtimebustest.DefaultRunID, `{"value":1}`, now)
	plans, err := eb.PrepareEnginePublications(context.Background(), []runtimeengine.EmitIntent{{Event: original, Recipients: []string{"reviewer"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := eb.ReleaseEnginePublications(context.Background(), plans); err != nil {
			t.Error(err)
		}
	})
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	plan := plans[0].(runtimebus.EnginePublicationPlan)
	if err := plan.ValidatePreparedFanOutEvent(original); err != nil {
		t.Fatalf("exact source rejected: %v", err)
	}
	for name, hostile := range map[string]events.Event{
		"event":            makeEvent(eventtest.UUID("other-event"), runtimebustest.DefaultRunID, `{"value":1}`, now),
		"run":              makeEvent(id, eventtest.UUID("other-run"), `{"value":1}`, now),
		"business payload": makeEvent(id, runtimebustest.DefaultRunID, `{"value":2}`, now),
		"time":             makeEvent(id, runtimebustest.DefaultRunID, `{"value":1}`, now.Add(time.Second)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := plan.ValidatePreparedFanOutEvent(hostile); err == nil {
				t.Fatal("substituted admitted source accepted")
			}
		})
	}
	if err := (runtimebus.EnginePublicationPlan{}).ValidatePreparedFanOutEvent(original); err == nil {
		t.Fatal("zero planner evidence accepted")
	}
	if len(store.events) != 0 {
		t.Fatal("preparation or hostile validation persisted an event")
	}
}
