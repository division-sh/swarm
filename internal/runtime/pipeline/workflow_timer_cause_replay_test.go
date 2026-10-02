package pipeline

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
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
		{"owner", func(a *WorkflowTimerActivation) { a.OwnerAgent += ".other" }},
		{"event", func(a *WorkflowTimerActivation) { a.EventType = "timer.other" }},
		{"mode", func(a *WorkflowTimerActivation) { a.ExecutionMode = executionmode.Mock }},
		{"payload", func(a *WorkflowTimerActivation) { a.Payload = []byte(`{"different":true}`) }},
		{"created_at", func(a *WorkflowTimerActivation) { a.CreatedAt = a.CreatedAt.Add(time.Microsecond) }},
		{"due_at", func(a *WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Microsecond) }},
		{"lineage", func(a *WorkflowTimerActivation) {
			a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromEventID, a.ReconstructionOwner = uuid.NewString(), uuid.NewString(), uuid.NewString(), "fork-owner"
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
