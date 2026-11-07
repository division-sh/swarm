package pipeline

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestWorkflowTimerCauseReplayPreservesImmutableFacts(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 1, 0, 0, 0, 123456000, time.UTC)
	initial := WorkflowTimerActivation{
		Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "waiting.timeout", DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
		RunID: runID, EntityID: entityID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-owner", EventType: "timer.elapsed", ExecutionMode: executionmode.Live,
		Payload: []byte(`{"nested":{"b":2,"a":1},"message":"ok"}`), CreatedAt: at, FireAt: at.Add(time.Hour), Status: workflowTimerStatusActive,
	}
	if err := initial.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Run("life_start_and_timer_scope", func(t *testing.T) {
		instance := WorkflowInstance{Life: 2, CreatedAt: at, EnteredStageAt: at.Add(48 * time.Hour)}
		started := at.Add(24 * time.Hour)
		if _, err := instance.LifeStartedAt(); err == nil {
			t.Fatal("restarted life inferred its start from the old birth or current stage")
		}
		if err := retainInstanceLifeStart(&instance, started); err != nil {
			t.Fatal(err)
		}
		if got, err := instance.LifeStartedAt(); err != nil || !got.Equal(started) {
			t.Fatalf("retained life start = %v %v", got, err)
		}
		if err := retainInstanceLifeStart(&instance, started); err != nil {
			t.Fatalf("exact life-start replay refused: %v", err)
		}
		if err := retainInstanceLifeStart(&instance, started.Add(time.Second)); err == nil {
			t.Fatal("initial-entry replay moved the life start")
		}
		bundle := workflowTimerOwnerBundleWithDelay(t, false, "1h")
		selected := semanticview.Wrap(bundle)
		cause := workflowTimerCause{Kind: workflowTimerCauseInitial, Life: 1, EventType: "state:waiting", ToState: "waiting", OccurredAt: started, ExecutionMode: executionmode.Live}
		first, err := workflowTimerActivationForCause(selected, runID, entityID, initial.Route, bundle.Semantics.Timers[0], initial.Ref.Generation, cause, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		cause.Life = 2
		next, err := workflowTimerActivationForCause(selected, runID, entityID, initial.Route, bundle.Semantics.Timers[0], initial.Ref.Generation, cause, time.Hour)
		if err != nil || first.Ref.ActivationID == next.Ref.ActivationID || next.RoutingSource.Life() != 2 || first.RoutingSource.Life() != 1 || !next.FireAt.Equal(first.FireAt) {
			t.Fatalf("timer life changed its clock or reused the previous arm: first=%+v next=%+v err=%v", first, next, err)
		}
		kept, err := workflowTimersForLife(2, []WorkflowTimerActivation{first, next})
		if err != nil || len(kept) != 1 || kept[0].Ref != next.Ref || first.Status != workflowTimerStatusActive {
			t.Fatalf("timer selection crossed life or retired the old arm: %+v %v", kept, err)
		}
		cause.Life = 0
		if _, err := workflowTimerActivationForCause(selected, runID, entityID, initial.Route, bundle.Semantics.Timers[0], initial.Ref.Generation, cause, time.Hour); err == nil {
			t.Fatal("timer constructor repaired missing captured life")
		}
	})
	for _, status := range []string{workflowTimerStatusActive, workflowTimerStatusFired, workflowTimerStatusCancelled} {
		t.Run(status, func(t *testing.T) {
			current := initial
			current.Status = status
			if status == workflowTimerStatusFired {
				current.FiredAt = initial.FireAt.Add(time.Second)
			}
			current.Payload = []byte(`{"message":"ok","nested":{"a":1,"b":2}}`)
			before := current.Canonical()
			if err := current.ValidateCauseReplay(initial); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(current.Canonical(), before) {
				t.Fatal("cause comparison changed the current activation")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*WorkflowTimerActivation)
	}{
		{"activation", func(a *WorkflowTimerActivation) { a.Ref.ActivationID = uuid.NewString() }},
		{"declaration", func(a *WorkflowTimerActivation) { a.Ref.DeclarationKey += ".other" }},
		{"revision", func(a *WorkflowTimerActivation) { a.Ref.DeclarationRevision += ".other" }},
		{"cause", func(a *WorkflowTimerActivation) { a.Ref.Cause = timeridentity.WorkflowTimerActivationCauseEvent }},
		{"run", func(a *WorkflowTimerActivation) { a.RunID = uuid.NewString() }},
		{"route", func(a *WorkflowTimerActivation) { a.Route = flowidentity.StoredRoute(".", "other", "other") }},
		{"source", func(a *WorkflowTimerActivation) {
			a.EntityID = uuid.NewString()
			a.RoutingSource, err = events.NewRootRoutingSource(a.EntityID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"life", func(a *WorkflowTimerActivation) {
			a.RoutingSource, err = a.RoutingSource.WithLife(2)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"owner", func(a *WorkflowTimerActivation) { a.OwnerAgent += ".other" }},
		{"event", func(a *WorkflowTimerActivation) { a.EventType = "timer.other" }},
		{"mode", func(a *WorkflowTimerActivation) { a.ExecutionMode = executionmode.Mock }},
		{"payload", func(a *WorkflowTimerActivation) { a.Payload = []byte(`{"different":true}`) }},
		{"created_at", func(a *WorkflowTimerActivation) { a.CreatedAt = a.CreatedAt.Add(time.Microsecond) }},
		{"due_at", func(a *WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Microsecond) }},
		{"lineage", func(a *WorkflowTimerActivation) {
			a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromEventID, a.ReconstructionOwner = uuid.NewString(), uuid.NewString(), uuid.NewString(), "fork-owner"
			a.ForkedFromPointKind, a.ForkedFromPointRevision, a.SourceArmedAt = forkpoint.Event, 1, a.CreatedAt
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := initial
			test.change(&changed)
			if err := changed.Validate(); err != nil {
				t.Fatalf("negative control must be a valid but different activation: %v", err)
			}
			if err := initial.ValidateCauseReplay(changed); err == nil {
				t.Fatal("changed immutable fact accepted as exact cause replay")
			}
		})
	}
}

func TestWorkflowTimerCauseReplayValidatesBothRecurringCoordinates(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	initial := WorkflowTimerActivation{
		Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "waiting.recurring", DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseEvent},
		RunID: runID, EntityID: entityID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-owner", EventType: "timer.elapsed", ExecutionMode: executionmode.Live,
		CreatedAt: at, FireAt: at.Add(time.Hour), Recurring: true, RecurrenceInterval: time.Hour, Status: workflowTimerStatusActive,
	}
	advanced := initial
	advanced.FireAt = initial.FireAt.Add(2 * time.Hour)
	advanced.FiredAt = advanced.FireAt.Add(-time.Hour)
	for _, status := range []string{workflowTimerStatusActive, workflowTimerStatusCancelled} {
		current := advanced
		current.Status = status
		if err := current.ValidateCauseReplay(initial); err != nil {
			t.Fatalf("legitimate advanced %s replay: %v", status, err)
		}
	}
	if err := initial.ValidateCauseReplay(advanced); err == nil {
		t.Fatal("replay accepted a requested coordinate ahead of the current row")
	}
	for _, test := range []struct {
		name   string
		change func(*WorkflowTimerActivation)
	}{
		{"off_lattice", func(a *WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Microsecond) }},
		{"before_first_due", func(a *WorkflowTimerActivation) { a.FireAt = a.CreatedAt }},
		{"interval", func(a *WorkflowTimerActivation) { a.RecurrenceInterval = 0 }},
		{"invalid_status", func(a *WorkflowTimerActivation) { a.Status = workflowTimerStatusFired }},
		{"fired_before_previous_due", func(a *WorkflowTimerActivation) { a.FiredAt = a.CreatedAt }},
		{"partial_lineage", func(a *WorkflowTimerActivation) { a.SourceTimerID = uuid.NewString() }},
		{"invalid_payload", func(a *WorkflowTimerActivation) { a.Payload = []byte(`[]`) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := advanced
			test.change(&invalid)
			if err := invalid.ValidateCauseReplay(initial); err == nil {
				t.Fatal("invalid persisted activation accepted")
			}
			if err := advanced.ValidateCauseReplay(invalid); err == nil {
				t.Fatal("invalid requested activation accepted")
			}
		})
	}
}
