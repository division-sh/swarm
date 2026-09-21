package cataloge2e

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
)

func catalogSettledUnitDelivery() operatorread.OperatorEventDelivery {
	finished := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	return operatorread.OperatorEventDelivery{Status: "delivered", Terminal: true, FinishedAt: &finished}
}

func TestCatalogEventSuccessfullySettled(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*operatorread.OperatorEventFull)
		wantSettled bool
		wantErr     bool
	}{
		{name: "delivered", wantSettled: true},
		{name: "publication_only", mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries = nil }},
		{name: "in_progress", mutate: func(e *operatorread.OperatorEventFull) {
			e.Deliveries[0].Status = "in_progress"
			e.Deliveries[0].Terminal = false
		}},
		{name: "not_terminal", mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].Terminal = false }},
		{name: "no_finish", mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].FinishedAt = nil }},
		{name: "zero_finish", mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].FinishedAt = new(time.Time) }},
		{name: "retry_pending", mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].RetryScheduled = true }},
		{name: "second_receiver_pending", mutate: func(e *operatorread.OperatorEventFull) {
			e.Deliveries = append(e.Deliveries, operatorread.OperatorEventDelivery{Status: "pending"})
		}},
		{name: "dead_letter", wantErr: true, mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].Status = "dead_letter" }},
		{name: "terminal_skip", wantErr: true, mutate: func(e *operatorread.OperatorEventFull) { e.Deliveries[0].Status = "skipped" }},
		{name: "failure_envelope", wantErr: true, mutate: func(e *operatorread.OperatorEventFull) {
			e.Deliveries[0].Failure = replayProjectionDelivery(t, "failed").Failure
		}},
		{name: "delivery_dead_letters", wantErr: true, mutate: func(e *operatorread.OperatorEventFull) {
			e.Deliveries[0].DeadLetters = []operatorread.OperatorDeadLetterRecord{{}}
		}},
		{name: "event_dead_letters", wantErr: true, mutate: func(e *operatorread.OperatorEventFull) { e.DeadLetters = []operatorread.OperatorDeadLetterRecord{{}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := operatorread.OperatorEventFull{EventID: "tick", EventName: "timer.tick", Deliveries: []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}}
			if tc.mutate != nil {
				tc.mutate(&event)
			}
			settled, err := catalogEventSuccessfullySettled(event)
			if settled != tc.wantSettled || (err != nil) != tc.wantErr {
				t.Fatalf("settled=%t err=%v; want settled=%t error=%t", settled, err, tc.wantSettled, tc.wantErr)
			}
		})
	}
}

func TestCatalogAutomaticEventCountRequiresCompletedDeliveries(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	root := createdRootEvent(eventtest.UUID("barrier-root"), "timer.tick", "author", "task", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, created)
	makeTick := func(id string) operatorread.OperatorEventFull {
		return replayOperatorEvent(t, eventtest.Child(eventtest.UUID(id), events.EventType("timer.tick"), "timer", "task", json.RawMessage(`{}`), 1, root, events.EventEnvelope{}, created.Add(time.Second)))
	}
	first, second, authored := makeTick("first"), makeTick("second"), makeTick("authored")
	first.Deliveries = []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}
	lister := &catalogReplayPageLister{pages: map[string]operatorread.OperatorEventListResult{
		"":          {Events: []operatorread.OperatorEventFull{replayOperatorEvent(t, root), authored}, NextCursor: "automatic"},
		"automatic": {Events: []operatorread.OperatorEventFull{first, second}},
	}}
	authoredIDs := map[string]struct{}{authored.EventID: {}}
	assertCount := func(want int) {
		t.Helper()
		count, err := countCatalogSettledAutomaticEvents(context.Background(), lister, "timer.tick", 2, authoredIDs)
		if err != nil || count != want {
			t.Fatalf("count=%d err=%v, want %d", count, err, want)
		}
	}
	assertCount(1) // Both ticks are durable, but the second has no delivery yet.
	second.Deliveries = []operatorread.OperatorEventDelivery{{Status: "in_progress"}}
	lister.pages["automatic"] = operatorread.OperatorEventListResult{Events: []operatorread.OperatorEventFull{first, second}}
	assertCount(1)
	second.Deliveries = []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}
	lister.pages["automatic"] = operatorread.OperatorEventListResult{Events: []operatorread.OperatorEventFull{first, second}}
	assertCount(2)
	for _, opts := range lister.options {
		if opts.Filter.RunID != catalogRuntimeRunID || opts.Filter.EventName != "timer.tick" || opts.Limit != 2 {
			t.Fatalf("barrier lost bounded run/event query: %+v", opts)
		}
	}
	second.Deliveries[0].Status = "dead_letter"
	lister.pages["automatic"] = operatorread.OperatorEventListResult{Events: []operatorread.OperatorEventFull{first, second}}
	if _, err := countCatalogSettledAutomaticEvents(context.Background(), lister, "timer.tick", 2, authoredIDs); err == nil {
		t.Fatal("failed tick must reject the barrier, not satisfy it or wait for another tick")
	}
}

func TestCatalogSuccessfulDeliveriesRejectsEquivalentDeadLetters(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	root := createdRootEvent(eventtest.UUID("success-root"), "flow.spawned", "author", "task", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, created)
	event := replayOperatorEvent(t, root)
	event.Deliveries = []operatorread.OperatorEventDelivery{replayProjectionDelivery(t, "failed")}
	transcript := catalogReplayUnitTranscript(t, root)
	source := catalogReplayUnitProjection(t, transcript, event)
	replay := catalogReplayUnitProjection(t, transcript, event)
	if !bytes.Equal(source, replay) {
		t.Fatal("identically failed executions should have equal projections")
	}
	required := map[string]int{"flow.spawned": 1}
	full := map[string]operatorread.OperatorEventFull{event.EventID: event}
	if err := validateCatalogSuccessfulDeliveries(full, required); err == nil {
		t.Fatal("equality cannot discharge success obligation")
	}
	event.Deliveries = nil
	full[event.EventID] = event
	if err := validateCatalogSuccessfulDeliveries(full, required); err == nil {
		t.Fatal("publication alone cannot discharge success obligation")
	}
	event.Deliveries = []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}
	full[event.EventID] = event
	if err := validateCatalogSuccessfulDeliveries(full, required); err != nil {
		t.Fatal(err)
	}
	if err := validateCatalogSuccessfulDeliveries(full, map[string]int{"flow.spawned": 2}); err == nil {
		t.Fatal("missing successful occurrence accepted")
	}
	if err := validateCatalogSuccessfulDeliveries(full, map[string]int{"worker.ready": 1}); err == nil {
		t.Fatal("missing required event accepted")
	}
}

func TestCatalogCreationDeliveriesOnlyExemptsExactConflictingRoot(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, change := range []string{"exact_conflict", "failed_child", "failed_first_root", "missing_conflict", "conflict_delivered", "child_exception"} {
		t.Run(change, func(t *testing.T) {
			root := createdRootEvent(eventtest.UUID("creation-first"), "flow.spawn_requested", "author", "task", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, created)
			first := replayOperatorEvent(t, root)
			first.Deliveries = []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}
			conflict := replayOperatorEvent(t, createdRootEvent(eventtest.UUID("creation-conflict"), "flow.spawn_requested", "author", "task", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, created.Add(time.Second)))
			conflict.Deliveries = []operatorread.OperatorEventDelivery{replayProjectionDelivery(t, "conflict")}
			child := replayOperatorEvent(t, eventtest.Child(eventtest.UUID("creation-child"), events.EventType("flow.spawned"), "worker", "task", json.RawMessage(`{}`), 1, root, events.EventEnvelope{}, created.Add(time.Second)))
			child.Deliveries = []operatorread.OperatorEventDelivery{catalogSettledUnitDelivery()}
			full := map[string]operatorread.OperatorEventFull{first.EventID: first, conflict.EventID: conflict, child.EventID: child}
			exception := conflict.EventID
			switch change {
			case "failed_child":
				child.Deliveries[0] = replayProjectionDelivery(t, "unexpected-child-failure")
			case "failed_first_root":
				first.Deliveries[0] = replayProjectionDelivery(t, "unexpected-first-root-failure")
			case "missing_conflict":
				delete(full, conflict.EventID)
			case "conflict_delivered":
				conflict.Deliveries[0] = catalogSettledUnitDelivery()
			case "child_exception":
				child.EventName = "flow.spawn_requested"
				child.Deliveries[0] = replayProjectionDelivery(t, "child")
				full[child.EventID] = child
				exception = child.EventID
			}
			err := validateCatalogCreationDeliveries(full, map[string]int{"flow.spawn_requested": 1, "flow.spawned": 1}, exception)
			if (err == nil) != (change == "exact_conflict") {
				t.Fatalf("creation success obligation: %v, want success=%t", err, change == "exact_conflict")
			}
		})
	}
}
