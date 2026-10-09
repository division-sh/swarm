package runforkpersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type runForkWorkflowTimerSelectionOwner interface {
	SelectInheritedWorkflowTimer(pipeline.WorkflowTimerActivation) (*pipeline.WorkflowTimerActivation, error)
}

type runForkWorkflowTimerMaterializationOwner interface {
	MaterializeRunForkWorkflowTimerTx(context.Context, *mutationprotocol.Attempt, pipeline.WorkflowTimerActivation, bool) error
	RequireRunForkWorkflowTimerTx(context.Context, *mutationprotocol.Attempt, pipeline.WorkflowTimerActivation, bool) error
}

type runForkWorkflowTimerProjection struct {
	activation pipeline.WorkflowTimerActivation
	removed    bool
}

// The plan is already fixed-cut admitted. This adapter never reads source rows,
// installs wakeups, or grants execution. Returned removals require the caller's
// named dependent-settlement operation in this same materialization attempt.
func materializeRunForkWorkflowTimers(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	plan runfork.RunForkPlan,
	forkRunID string,
	selection runForkWorkflowTimerSelectionOwner,
	owner runForkWorkflowTimerMaterializationOwner,
	bornAt time.Time,
	reuse bool,
) ([]pipeline.WorkflowTimerActivation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(plan.WorkflowTimers) == 0 {
		return nil, nil
	}
	projected, err := prepareRunForkWorkflowTimers(plan, forkRunID, selection, bornAt)
	if err != nil {
		return nil, err
	}
	if len(projected) != 0 && (attempt == nil || owner == nil) {
		return nil, fmt.Errorf("fork workflow timers require the materialization attempt and canonical timer owner")
	}
	var removed []pipeline.WorkflowTimerActivation
	for _, timer := range projected {
		if reuse {
			err = owner.RequireRunForkWorkflowTimerTx(ctx, attempt, timer.activation, timer.removed)
		} else {
			err = owner.MaterializeRunForkWorkflowTimerTx(ctx, attempt, timer.activation, timer.removed)
		}
		if err != nil {
			return nil, fmt.Errorf("materialize fork workflow timer %s: %w", timer.activation.SourceTimerID, err)
		}
		if timer.removed {
			cancelled := timer.activation.Canonical()
			cancelled.Status, cancelled.CancelCause, cancelled.CancelledAt = "cancelled", pipeline.WorkflowTimerCancelCauseRuleRemoved, cancelled.CreatedAt
			if err := cancelled.Validate(); err != nil {
				return nil, err
			}
			removed = append(removed, cancelled)
		}
	}
	return removed, nil
}

func prepareRunForkWorkflowTimers(plan runfork.RunForkPlan, forkRunID string, selection runForkWorkflowTimerSelectionOwner, bornAt time.Time) ([]runForkWorkflowTimerProjection, error) {
	if err := plan.ForkPoint.Validate(); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(plan.WorkflowTimers))
	var out []runForkWorkflowTimerProjection
	for _, record := range plan.WorkflowTimers {
		source, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
		if err != nil {
			return nil, fmt.Errorf("fixed-cut workflow timer: %w", err)
		}
		if source.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("fixed-cut workflow timer belongs to another run")
		}
		if _, duplicate := seen[source.Ref.ActivationID]; duplicate {
			return nil, fmt.Errorf("fixed-cut workflow timer %s appears more than once", source.Ref.ActivationID)
		}
		seen[source.Ref.ActivationID] = struct{}{}
		if source.Status != "active" {
			continue
		}
		if selection == nil {
			return nil, fmt.Errorf("fork workflow timer requires sealed selected declaration admission")
		}
		correspondence, err := runForkWorkflowTimerCorrespondence(plan, forkRunID, source)
		if err != nil {
			return nil, err
		}
		raw, err := encodeRunForkWorkflowTimerProjectionSource(source)
		if err != nil {
			return nil, err
		}
		projected, _, err := projectRunForkWorkflowTimer(raw, plan.SourceRunID, forkRunID, plan.ForkPoint, nil, correspondence, bornAt)
		if err != nil {
			return nil, err
		}
		selected, err := selection.SelectInheritedWorkflowTimer(projected)
		if err != nil {
			return nil, fmt.Errorf("admit selected workflow timer %s: %w", source.Ref.DeclarationKey, err)
		}
		projected, removed, err := projectRunForkWorkflowTimer(raw, plan.SourceRunID, forkRunID, plan.ForkPoint, selected, correspondence, bornAt)
		if err != nil {
			return nil, err
		}
		out = append(out, runForkWorkflowTimerProjection{activation: projected, removed: removed})
	}
	return out, nil
}

func runForkWorkflowTimerCorrespondence(plan runfork.RunForkPlan, forkRunID string, source pipeline.WorkflowTimerActivation) (*loopruntime.ForkCorrespondence, error) {
	var entity *runfork.RunForkEntityState
	for i := range plan.Entities {
		candidate := &plan.Entities[i]
		if candidate.EntityID != source.EntityID {
			continue
		}
		if candidate.MaterializationMetadata == nil {
			return nil, fmt.Errorf("fixed-cut workflow timer owner lacks construction metadata")
		}
		if candidate.MaterializationMetadata.FlowInstance != source.Route.InstancePath {
			continue
		}
		if entity != nil {
			return nil, fmt.Errorf("fixed-cut workflow timer has duplicate constructed owners")
		}
		entity = candidate
	}
	if entity == nil {
		return nil, fmt.Errorf("fixed-cut workflow timer lacks its exact constructed owner")
	}
	if entity.MaterializationMetadata.FlowTemplate != source.Route.ScopeKey {
		return nil, fmt.Errorf("fixed-cut workflow timer declaration disagrees with its constructed owner")
	}
	projection, err := projectRunForkEntityOwnership(plan.SourceRunID, forkRunID, source.EntityID, source.Route.InstancePath)
	if err != nil {
		return nil, err
	}
	_, _, correspondence, err := projectRunForkEntityExecutionState(*entity, plan.SourceRunID, forkRunID, projection)
	return correspondence, err
}

// Box the already-admitted primitive record in the existing historical wire
// codec solely to consume the canonical projection; no new fact is captured.
func encodeRunForkWorkflowTimerProjectionSource(source pipeline.WorkflowTimerActivation) ([]byte, error) {
	routing, err := json.Marshal(source.RoutingSource)
	if err != nil {
		return nil, err
	}
	interval := ""
	if source.Recurring {
		interval = source.RecurrenceInterval.String()
	}
	snapshot := runforkrevision.TimerSnapshot{
		TimerID: source.Ref.ActivationID, TimerName: source.Ref.TaskID(), RunID: source.RunID, EntityID: source.EntityID,
		FlowScopeKey: source.Route.ScopeKey, FlowInstanceID: source.Route.InstanceID, FlowInstance: source.Route.InstancePath,
		FireEvent: source.EventType, FirePayload: source.Payload, RoutingSource: routing, ExecutionMode: string(source.ExecutionMode),
		FireAt: source.FireAt, Recurring: source.Recurring, RecurrenceInterval: interval,
		OwnerAgent: source.OwnerAgent, OwnerKind: "system", TaskType: "workflow_timer", Status: source.Status, CreatedAt: source.CreatedAt,
		SourceTimerID: source.SourceTimerID, ForkedFromRunID: source.ForkedFromRunID, ForkedFromEventID: source.ForkedFromEventID,
		ForkedFromPointKind: source.ForkedFromPointKind, ForkedFromPointRevision: source.ForkedFromPointRevision,
		ReconstructionOwner: source.ReconstructionOwner,
	}
	if !source.SourceArmedAt.IsZero() {
		snapshot.SourceArmedAt = &source.SourceArmedAt
	}
	if !source.FiredAt.IsZero() {
		snapshot.FiredAt = &source.FiredAt
	}
	return json.Marshal(snapshot)
}
