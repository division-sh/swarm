package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestWorkflowStageEntryEventReadRequiresCanonicalOwner(t *testing.T) {
	for _, fault := range []string{"none", "missing_reader", "missing_event", "foreign_run", "wrong_event", "read_failure", "invalid_settlement", "blank_identity"} {
		t.Run(fault, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			bus, err := newScopedTestEventBus(store)
			if err != nil {
				t.Fatal(err)
			}
			event := exactSiblingObligationEvent("stage-entry-read")
			seedCommittedNoDeliveryForTest(t, store, event)
			reader, run, id := bus.durable.PreparedEvents, event.RunID(), event.ID()
			canonical, found, err := reader.LoadPreparedPublishEvent(context.Background(), event.ID())
			if err != nil || !found {
				t.Fatal("missing canonical publication fixture")
			}
			readFailure := errors.New("entry publication read failed")
			switch fault {
			case "missing_reader":
				bus.durable.PreparedEvents = nil
			case "missing_event":
				bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(context.Context, string) (PreparedPublishEvent, bool, error) {
					return PreparedPublishEvent{}, false, nil
				})
			case "foreign_run":
				run = "foreign-run"
			case "wrong_event":
				other := exactSiblingObligationEvent("other-stage-entry")
				seedCommittedNoDeliveryForTest(t, store, other)
				bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(ctx context.Context, _ string) (PreparedPublishEvent, bool, error) {
					return reader.LoadPreparedPublishEvent(ctx, other.ID())
				})
			case "read_failure":
				bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(context.Context, string) (PreparedPublishEvent, bool, error) {
					return PreparedPublishEvent{}, false, readFailure
				})
			case "invalid_settlement":
				bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(ctx context.Context, id string) (PreparedPublishEvent, bool, error) {
					row, found, err := reader.LoadPreparedPublishEvent(ctx, id)
					row.Settlement = events.RouteSettlement{}
					return row, found, err
				})
			case "blank_identity":
				id = " "
			}
			beforeEvents, beforeRoutes, beforeSettlements := len(store.events), len(store.routes), len(store.settlements)
			got, err := bus.LoadWorkflowStageEntryEvent(context.Background(), run, id)
			if fault == "none" {
				if err != nil || !reflect.DeepEqual(canonical.Event.Event(), got) {
					t.Fatalf("immutable event read changed facts: %v", err)
				}
			} else if err == nil || got.ID() != "" {
				t.Fatalf("%s returned an event without its exact owner: %s/%v", fault, got.ID(), err)
			}
			if fault == "read_failure" && !errors.Is(err, readFailure) {
				t.Fatal("entry read lost its original failure")
			}
			if len(store.events) != beforeEvents || len(store.routes) != beforeRoutes || len(store.settlements) != beforeSettlements {
				t.Fatal("immutable event read mutated publication state")
			}
		})
	}
}
