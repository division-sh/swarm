package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type workflowStageEntryEventReader interface {
	LoadWorkflowStageEntryEvent(context.Context, string, string) (events.Event, error)
}

func (l *WorkflowTimerLifecycle) stageEntryOriginEvent(ctx context.Context, entry timeridentity.StageEntryRef) (events.Event, error) {
	var empty events.Event
	if err := entry.Validate(); err != nil {
		return empty, err
	}
	if l.entryEvents == nil || entry.Cause == "construction" {
		return empty, fmt.Errorf("transition entry requires its native immutable publication reader")
	}
	originRun := entry.RunID
	if entry.OriginRunID != "" {
		originRun = entry.OriginRunID
	}
	event, err := l.entryEvents.LoadWorkflowStageEntryEvent(ctx, originRun, entry.EventID)
	if err != nil {
		return empty, err
	}
	if event.ID() != entry.EventID || event.RunID() != originRun {
		return empty, fmt.Errorf("stage entry publication disagrees with its exact origin")
	}
	return event, nil
}

// Publication authenticates a fired occurrence, not its applicability to a later
// visit to the same stage. Reuse the canonical activation derivation against the
// retained entry, without substituting clocks or business-field revisions.
func (l *WorkflowTimerLifecycle) stageTimerEntryCurrent(ctx context.Context, instance WorkflowInstance, activation WorkflowTimerActivation, declaration contracts.WorkflowTimerContract) (bool, error) {
	if instance.CurrentState != declaration.Stage {
		return false, nil
	}
	entry, found, err := workflowlifecycle.LoadStageEntry(instance.Bookkeeping)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("stage timer requires its retained lifecycle entry")
	}
	route := activation.Route
	if err := entry.RequireOwner(activation.RunID, route.ScopeKey, route.InstanceID, route.InstancePath, activation.EntityID, declaration.Stage); err != nil {
		return false, err
	}
	cause := workflowTimerCause{Kind: workflowTimerCauseInitial, EventType: "state:" + entry.Stage, ToState: entry.Stage,
		OccurredAt: instance.CreatedAt, ExecutionMode: activation.ExecutionMode}
	if entry.Cause != "construction" {
		if len(instance.TransitionHistory) == 0 {
			return false, fmt.Errorf("stage timer entry requires its committed transition")
		}
		record := instance.TransitionHistory[len(instance.TransitionHistory)-1]
		if record.TriggerEventID != entry.EventID || record.TransitionID != entry.TransitionID || record.Evidence.ID() != entry.TransitionID || record.To != entry.Stage || record.From != record.Evidence.From() || record.To != record.Evidence.To() {
			return false, fmt.Errorf("stage timer entry disagrees with its committed transition")
		}
		event, err := l.stageEntryOriginEvent(ctx, entry)
		if err != nil {
			return false, err
		}
		if !record.FiredAt.Equal(event.CreatedAt()) {
			return false, fmt.Errorf("stage timer entry disagrees with its immutable transition cause")
		}
		cause = workflowTimerCause{Kind: workflowTimerCauseTransition, EventID: entry.EventID, EventType: string(event.Type()),
			TransitionID: entry.TransitionID, FromState: record.From, ToState: record.To,
			OccurredAt: event.CreatedAt(), ExecutionMode: event.ExecutionMode()}
	}
	interval := workflowTimerDuration(declaration, workflowTimerPolicy(l.source, declaration.OwningFlowID()))
	expected, err := workflowTimerActivationForCause(l.source, activation.RunID, activation.EntityID, route, declaration, activation.Ref.Generation, cause, interval)
	if err != nil {
		return false, err
	}
	return activation.Ref == expected.Ref, nil
}
