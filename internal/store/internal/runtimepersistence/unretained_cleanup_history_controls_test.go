package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func unretainedCleanupHistoryInput(t *testing.T, fork string) ([2]events.Event, events.DeliveryRoute) {
	t.Helper()
	route := testAgentDeliveryRoute(t, fork, "selected-agent", "fixture/selected-agent")
	var history [2]events.Event
	for i, kind := range []events.EventType{"selected.claimed", "selected.settled"} {
		event := eventtest.PersistedProjection(uuid.NewString(), kind, "selected-test", "", []byte(`{}`), 0, fork, "", events.EventEnvelope{}, time.Now().UTC())
		bound, err := bindSemanticEventFixturePayload(event)
		if err != nil {
			t.Fatal(err)
		}
		history[i] = bound
	}
	return history, route
}

func proveRetainedExecutionRefusesPhysicalCleanupHistory(t *testing.T, ctx context.Context, selected any, fork, source, frontier string) {
	t.Helper()
	history, route := unretainedCleanupHistoryInput(t, fork)
	assertUnretainedCleanupHistoryRefusal(t, ctx, selected, fork, source, frontier, history, route, "refuses every retained selected execution")
}

func installAndProveUnretainedCleanupHistory(t *testing.T, ctx context.Context, selected any, fork, source, frontier string) [2]string {
	t.Helper()
	history, route := unretainedCleanupHistoryInput(t, fork)
	proveUnretainedCleanupHistoryInputRefusals(t, ctx, selected, fork, source, frontier, history, route)
	proveUnretainedCleanupHistoryRollback(t, ctx, selected, fork, source, frontier, history, route)
	ids, err := InstallUnretainedSelectedForkCleanupHistoryForTest(ctx, selected, fork, source, frontier, history, route)
	if err != nil {
		t.Fatalf("install physical unretained cleanup history: %v", err)
	}
	if ids[0] == "" || ids[1] == "" || ids[0] == ids[1] {
		t.Fatal("claimed selected-fork delivery has no durable identity")
	}
	before, err := ReadUnretainedSelectedForkCleanupHistoryForTest(ctx, selected, fork, source, frontier)
	if err != nil || before != (UnretainedCleanupHistoryObservation{Deliveries: 2, Claimed: 1, Delivered: 1, Attempts: 2, OpenAttempts: 1, Settlements: 1, RuleSelections: 1}) {
		t.Fatalf("unretained cleanup lost its positive physical history or zero-execution premise: %+v err=%v", before, err)
	}
	assertUnretainedCleanupHistoryRefusal(t, ctx, selected, fork, source, frontier, history, route, "target is not empty")
	return ids
}

func assertUnretainedCleanupHistoryRefusal(t *testing.T, ctx context.Context, selected any, fork, source, frontier string, history [2]events.Event, route events.DeliveryRoute, want string) {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := InstallUnretainedSelectedForkCleanupHistoryForTest(ctx, selected, fork, source, frontier, history, route)
	if err == nil || !strings.Contains(err.Error(), want) || ids != ([2]string{}) {
		t.Fatalf("invalid unretained history missed its exact refusal: ids=%v err=%v want=%s", ids, err, want)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(context.Background(), selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused physical history mutated complete storage: %v", err)
	}
}

func proveUnretainedCleanupHistoryInputRefusals(t *testing.T, ctx context.Context, selected any, fork, source, frontier string, history [2]events.Event, route events.DeliveryRoute) {
	t.Helper()
	for _, name := range []string{"binding_source", "frontier", "foreign_recipient", "foreign_event_run", "occupied_event", "running_target", "canceled"} {
		t.Run("refuse_"+name, func(t *testing.T) {
			badCtx, badFork, badSource, badFrontier, badHistory, badRoute := ctx, fork, source, frontier, history, route
			want := "unretained history exact binding"
			switch name {
			case "binding_source":
				badSource = uuid.NewString()
			case "frontier":
				badFrontier = uuid.NewString()
			case "foreign_recipient":
				badRoute.AgentIdentity.Name.Owner = "foreign-history-owner"
				want = "foreign or expanded history identities"
			case "foreign_event_run":
				badHistory[0] = unretainedHistoryEvent(t, history[0], history[0].ID(), source)
				want = "differs from its fixed original event footprint"
			case "occupied_event":
				badHistory[0] = unretainedHistoryEvent(t, history[0], frontier, fork)
				want = "requires absent event"
			case "running_target":
				badFork, badSource = source, fork
				badRoute.AgentIdentity.RunID = source
				for i, event := range history {
					badHistory[i] = unretainedHistoryEvent(t, event, event.ID(), source)
				}
				want = "materialized pause"
			case "canceled":
				var cancel context.CancelFunc
				badCtx, cancel = context.WithCancel(ctx)
				cancel()
				want = "context canceled"
			}
			assertUnretainedCleanupHistoryRefusal(t, badCtx, selected, badFork, badSource, badFrontier, badHistory, badRoute, want)
		})
	}
}

func unretainedHistoryEvent(t *testing.T, original events.Event, id, run string) events.Event {
	t.Helper()
	event := eventtest.PersistedProjection(id, original.Type(), "selected-test", "", []byte(`{}`), 0, run, "", events.EventEnvelope{}, original.CreatedAt())
	bound, err := bindSemanticEventFixturePayload(event)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func proveUnretainedCleanupHistoryRollback(t *testing.T, ctx context.Context, selected any, fork, source, frontier string, history [2]events.Event, route events.DeliveryRoute) {
	t.Helper()
	if err := SetOutboxAfterFirstEventWriteFaultForTest(ctx, selected, fork, history[0].ID(), history[1].ID(), true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := SetOutboxAfterFirstEventWriteFaultForTest(context.Background(), selected, fork, history[0].ID(), history[1].ID(), false); err != nil {
			t.Error(err)
		}
	})
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := InstallUnretainedSelectedForkCleanupHistoryForTest(ctx, selected, fork, source, frontier, history, route)
	if err == nil || !strings.Contains(err.Error(), "outbox_after_first_event_write") || ids != ([2]string{}) {
		t.Fatalf("physical history missed its real second-write cut: ids=%v err=%v", ids, err)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed history setup left partial state: %v", err)
	}
	if err := SetOutboxAfterFirstEventWriteFaultForTest(ctx, selected, fork, history[0].ID(), history[1].ID(), false); err != nil {
		t.Fatal(err)
	}
}
