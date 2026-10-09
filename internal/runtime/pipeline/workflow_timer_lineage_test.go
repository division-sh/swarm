package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func inheritedWorkflowTimerForTest(t *testing.T) WorkflowTimerActivation {
	t.Helper()
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	armedAt := time.Date(2026, 10, 9, 0, 0, 0, 123456000, time.UTC)
	return WorkflowTimerActivation{
		Ref: timeridentity.WorkflowTimerActivationRef{
			ActivationID: "33333333-3333-4333-8333-333333333333", DeclarationKey: "waiting.timeout",
			DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
		},
		RunID: runID, EntityID: entityID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-owner", EventType: "timer.elapsed", ExecutionMode: executionmode.Live, Payload: []byte(`{}`),
		CreatedAt: armedAt.Add(3 * time.Hour), FireAt: armedAt.Add(time.Hour), Status: workflowTimerStatusActive,
		SourceTimerID: "44444444-4444-4444-8444-444444444444", ForkedFromRunID: "55555555-5555-4555-8555-555555555555",
		ForkedFromPointKind: forkpoint.RunStart, ForkedFromPointRevision: 7, SourceArmedAt: armedAt, ReconstructionOwner: "selected-cut",
	}
}

func TestWorkflowTimerTypedForkLineageRequiresExactPointAndOriginalArm(t *testing.T) {
	for _, kind := range []forkpoint.Kind{forkpoint.Event, forkpoint.RunStart, forkpoint.DeploymentRevision} {
		t.Run(string(kind), func(t *testing.T) {
			activation := inheritedWorkflowTimerForTest(t)
			activation.ForkedFromPointKind = kind
			if kind == forkpoint.Event {
				activation.ForkedFromEventID = "66666666-6666-4666-8666-666666666666"
			}
			if err := activation.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name   string
				change func(*WorkflowTimerActivation)
			}{
				{"missing_source_timer", func(a *WorkflowTimerActivation) { a.SourceTimerID = "" }},
				{"missing_source_run", func(a *WorkflowTimerActivation) { a.ForkedFromRunID = "" }},
				{"missing_kind", func(a *WorkflowTimerActivation) { a.ForkedFromPointKind = "" }},
				{"unknown_kind", func(a *WorkflowTimerActivation) { a.ForkedFromPointKind = "latest" }},
				{"missing_revision", func(a *WorkflowTimerActivation) { a.ForkedFromPointRevision = 0 }},
				{"negative_revision", func(a *WorkflowTimerActivation) { a.ForkedFromPointRevision = -1 }},
				{"wrong_event_identity", func(a *WorkflowTimerActivation) {
					if kind == forkpoint.Event {
						a.ForkedFromEventID = ""
					} else {
						a.ForkedFromEventID = "66666666-6666-4666-8666-666666666666"
					}
				}},
				{"missing_owner", func(a *WorkflowTimerActivation) { a.ReconstructionOwner = "" }},
				{"missing_original_arm", func(a *WorkflowTimerActivation) { a.SourceArmedAt = time.Time{} }},
				{"self_timer", func(a *WorkflowTimerActivation) { a.SourceTimerID = a.Ref.ActivationID }},
				{"self_run", func(a *WorkflowTimerActivation) { a.ForkedFromRunID = a.RunID }},
				{"arm_after_birth", func(a *WorkflowTimerActivation) { a.SourceArmedAt = a.CreatedAt.Add(time.Microsecond) }},
				{"due_before_arm", func(a *WorkflowTimerActivation) { a.FireAt = a.SourceArmedAt.Add(-time.Microsecond) }},
			} {
				t.Run(test.name, func(t *testing.T) {
					invalid := activation
					test.change(&invalid)
					if err := invalid.Validate(); err == nil {
						t.Fatal("incomplete or contradictory inherited obligation accepted")
					}
				})
			}
		})
	}
}

func TestWorkflowTimerOverdueInheritanceDoesNotRelaxOrdinaryArming(t *testing.T) {
	inherited := inheritedWorkflowTimerForTest(t)
	if err := inherited.Validate(); err != nil {
		t.Fatal(err)
	}
	ordinary := inherited
	ordinary.SourceTimerID, ordinary.ForkedFromRunID, ordinary.ReconstructionOwner = "", "", ""
	ordinary.ForkedFromPointKind, ordinary.ForkedFromPointRevision, ordinary.SourceArmedAt = "", 0, time.Time{}
	if err := ordinary.Validate(); err == nil {
		t.Fatal("ordinary overdue row borrowed inherited arming exception")
	}
	ordinary.FireAt = ordinary.CreatedAt.Add(time.Hour)
	if err := ordinary.Validate(); err != nil {
		t.Fatal(err)
	}
	ordinary.SourceArmedAt = inherited.SourceArmedAt
	if err := ordinary.Validate(); err == nil {
		t.Fatal("detached arm coordinate accepted without exact inherited lineage")
	}
}

func TestWorkflowTimerInheritedRecurrenceRetainsOriginalLatticeAndReplay(t *testing.T) {
	initial := inheritedWorkflowTimerForTest(t)
	initial.Recurring, initial.RecurrenceInterval = true, time.Hour
	current := initial
	current.FireAt = initial.FireAt.Add(time.Hour)
	current.FiredAt = initial.FireAt
	if err := current.ValidateCauseReplay(initial); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*WorkflowTimerActivation){
		func(a *WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Microsecond) },
		func(a *WorkflowTimerActivation) { a.ForkedFromPointRevision++ },
		func(a *WorkflowTimerActivation) { a.ForkedFromPointKind = forkpoint.DeploymentRevision },
		func(a *WorkflowTimerActivation) { a.SourceTimerID = "77777777-7777-4777-8777-777777777777" },
		func(a *WorkflowTimerActivation) { a.ForkedFromRunID = "88888888-8888-4888-8888-888888888888" },
		func(a *WorkflowTimerActivation) { a.SourceArmedAt = a.SourceArmedAt.Add(-time.Hour) },
	} {
		changed := current
		change(&changed)
		if err := changed.ValidateCauseReplay(initial); err == nil {
			t.Fatal("changed immutable lineage or occurrence lattice accepted as exact replay")
		}
	}
}
