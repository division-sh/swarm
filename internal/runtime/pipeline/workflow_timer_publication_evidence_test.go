package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestWorkflowTimerPublishedOccurrenceEvidenceIsExact(t *testing.T) {
	runID := uuid.NewString()
	source, err := events.NewRootRoutingSource(runID)
	if err != nil {
		t.Fatal(err)
	}
	activation := WorkflowTimerActivation{
		Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "timer", DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
		RunID: runID, EntityID: runID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-node", EventType: "timer.check", ExecutionMode: executionmode.Mock, Payload: []byte(`{}`),
		CreatedAt: time.Unix(100, 0).UTC(), FireAt: time.Unix(110, 0).UTC(), FiredAt: time.Unix(111, 0).UTC(), Status: "fired",
	}
	occurrence := activation.Occurrence()
	event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		RunID: runID,
		Facts: events.EventFacts{
			ID: timeridentity.WorkflowTimerOccurrenceEventID(occurrence), Type: "timer.check",
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime.workflow_timer"},
			TaskID:   occurrence.TaskID(), Payload: activation.Payload,
			Envelope: events.EventEnvelope{EntityID: runID, FlowInstance: runID}, RoutingSource: source,
			CreatedAt: activation.FiredAt, ExecutionMode: executionmode.Mock,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := activation.ValidatePublishedOccurrence(event); err != nil || got != occurrence {
		t.Fatalf("exact durable occurrence rejected: %+v err=%v", got, err)
	}
	for _, test := range []struct {
		name   string
		change func(*WorkflowTimerActivation)
	}{
		{"foreign_run", func(a *WorkflowTimerActivation) { a.RunID = uuid.NewString() }},
		{"activation", func(a *WorkflowTimerActivation) { a.Ref.ActivationID = uuid.NewString() }},
		{"declaration", func(a *WorkflowTimerActivation) { a.Ref.DeclarationRevision = "another-revision" }},
		{"event", func(a *WorkflowTimerActivation) { a.EventType = "timer.other" }},
		{"payload", func(a *WorkflowTimerActivation) { a.Payload = []byte(`{"foreign":true}`) }},
		{"execution_mode", func(a *WorkflowTimerActivation) { a.ExecutionMode = executionmode.Live }},
		{"accepted_time", func(a *WorkflowTimerActivation) { a.FiredAt = a.FiredAt.Add(time.Second) }},
		{"due", func(a *WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Second) }},
		{"unfired", func(a *WorkflowTimerActivation) { a.Status, a.FiredAt = "active", time.Time{} }},
		{"invalid", func(a *WorkflowTimerActivation) { a.Ref = timeridentity.WorkflowTimerActivationRef{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := activation
			test.change(&bad)
			if _, err := bad.ValidatePublishedOccurrence(event); err == nil {
				t.Fatal("unrelated/unaccepted timer fact authorized publication lineage")
			}
		})
	}
}

func TestWorkflowTimerPublishedOccurrenceUsesInheritedRecurringArm(t *testing.T) {
	runID := uuid.NewString()
	source, err := events.NewRootRoutingSource(runID)
	if err != nil {
		t.Fatal(err)
	}
	activation := WorkflowTimerActivation{
		Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "timer", DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
		RunID: runID, EntityID: runID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-node", EventType: "timer.check", ExecutionMode: executionmode.Mock, Payload: []byte(`{}`),
		CreatedAt: time.Unix(200, 0).UTC(), SourceArmedAt: time.Unix(100, 0).UTC(), FireAt: time.Unix(130, 0).UTC(),
		FiredAt: time.Unix(201, 0).UTC(), Status: "active", Recurring: true, RecurrenceInterval: 10 * time.Second,
		SourceTimerID: uuid.NewString(), ForkedFromRunID: uuid.NewString(), ForkedFromEventID: uuid.NewString(),
		ForkedFromPointKind: "event", ForkedFromPointRevision: 1, ReconstructionOwner: "run_fork_workflow_timer",
	}
	for _, due := range []int64{90, 110, 111} {
		occurrence := activation.Occurrence()
		occurrence.DueAt = time.Unix(due, 0).UTC()
		event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
			RunID: runID, Facts: events.EventFacts{
				ID: timeridentity.WorkflowTimerOccurrenceEventID(occurrence), Type: "timer.check",
				Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime.workflow_timer"},
				TaskID:   occurrence.TaskID(), Payload: activation.Payload,
				Envelope: events.EventEnvelope{EntityID: runID, FlowInstance: runID}, RoutingSource: source,
				CreatedAt: activation.FiredAt, ExecutionMode: executionmode.Mock,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = activation.ValidatePublishedOccurrence(event)
		if (err == nil) != (due == 110) {
			t.Fatalf("retained recurrence ignored original arm or admitted an unproven coordinate: due=%d err=%v", due, err)
		}
	}
}
