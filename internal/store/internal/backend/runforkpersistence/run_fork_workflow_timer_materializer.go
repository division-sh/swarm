package runforkpersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type runForkWorkflowTimerSelectionOwner = runfork.InheritedWorkflowTimerSelection

type runForkWorkflowTimerMaterializationOwner interface {
	MaterializeRunForkWorkflowTimerTx(context.Context, *mutationprotocol.Attempt, pipeline.WorkflowTimerActivation, bool) error
	ReadRunForkWorkflowTimerInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]pipeline.WorkflowTimerActivation, error)
}

type runForkWorkflowTimerProjection struct {
	activation pipeline.WorkflowTimerActivation
	removed    bool
}

type runForkWorkflowTimerReadbackPhase uint8

const (
	runForkWorkflowTimerAtCut runForkWorkflowTimerReadbackPhase = iota + 1
	runForkWorkflowTimerContinuing
)

// Require-only readback is also used on reuse/recovery. It cannot create a
// missing timer or settle an unrelated dependent obligation.
func requireMaterializedRunForkWorkflowTimers(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	plan runfork.RunForkPlan,
	forkRunID string,
	selection runForkWorkflowTimerSelectionOwner,
	owner runForkWorkflowTimerMaterializationOwner,
	bornAt time.Time,
	admission runfork.RunForkReplayResumeAdmission,
) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkWorkflowTimerInventory(ctx, attempt, plan, forkRunID, selection, owner, bornAt, admission, runForkWorkflowTimerAtCut)
}

func requireContinuingRunForkWorkflowTimers(
	ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan,
	forkRunID string, selection runForkWorkflowTimerSelectionOwner,
	owner runForkWorkflowTimerMaterializationOwner, bornAt time.Time,
	admission runfork.RunForkReplayResumeAdmission,
) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkWorkflowTimerInventory(ctx, attempt, plan, forkRunID, selection, owner, bornAt, admission, runForkWorkflowTimerContinuing)
}

func requireRunForkWorkflowTimerInventory(
	ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan,
	forkRunID string, selection runForkWorkflowTimerSelectionOwner,
	owner runForkWorkflowTimerMaterializationOwner, bornAt time.Time,
	admission runfork.RunForkReplayResumeAdmission, phase runForkWorkflowTimerReadbackPhase,
) (runfork.RunForkReplayResumeAdmission, error) {
	if phase != runForkWorkflowTimerAtCut && phase != runForkWorkflowTimerContinuing {
		return admission, fmt.Errorf("workflow timer readback requires an exact lifecycle phase")
	}
	materializable, err := runForkWorkflowTimerHistoryMaterializable(plan)
	if err != nil {
		return admission, err
	}
	if !materializable && len(plan.WorkflowTimers) != 0 {
		return admission, fmt.Errorf("workflow timer readback requires complete exact source inventory admission")
	}
	if !materializable {
		for _, blocker := range admission.UnsupportedBlockers {
			if blocker.Code == runfork.RunForkBlockerTimerHistoryUnproven {
				return admission, fmt.Errorf("workflow timer readback cannot omit blocked source history")
			}
		}
		for _, fact := range admission.Dispositions {
			if fact.Fact == runfork.RunForkReplayResumeFactTimerHistory {
				return admission, fmt.Errorf("workflow timer readback cannot omit relevant source history")
			}
		}
	}
	if err := requireRunForkWorkflowTimerReadbackFrame(ctx, attempt, plan, forkRunID, bornAt); err != nil {
		return admission, err
	}
	if owner == nil {
		return admission, fmt.Errorf("workflow timer readback requires its canonical timer owner")
	}
	actual, err := owner.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, forkRunID)
	if err != nil {
		return admission, err
	}
	projected, err := prepareRunForkWorkflowTimers(plan, forkRunID, selection, bornAt)
	if err != nil {
		return admission, err
	}
	if err := requireExactRunForkWorkflowTimerInventory(projected, actual, phase); err != nil {
		return admission, err
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return admission, err
	}
	if !materializable {
		return admission, nil
	}
	inventory, err := runForkWorkflowTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers)
	if err != nil {
		return admission, err
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	pending, err := inventory.pendingCertificate()
	if err != nil {
		return admission, err
	}
	if len(projected) != len(inventory.ActiveTimerIDs) {
		return admission, fmt.Errorf("workflow timer readback does not cover the complete active source inventory")
	}
	applied, err := runForkWorkflowTimerAppliedCertificate(pending, forkRunID, bornAt, projected)
	if err != nil {
		return admission, err
	}
	return dischargeMaterializedRunForkWorkflowTimerAdmission(admission, pending, applied, len(projected))
}

func requireExactRunForkWorkflowTimerInventory(expected []runForkWorkflowTimerProjection, actual []pipeline.WorkflowTimerActivation, phase runForkWorkflowTimerReadbackPhase) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("inherited workflow timer inventory differs from the fixed-cut projection")
	}
	byID := make(map[string]pipeline.WorkflowTimerActivation, len(actual))
	for _, timer := range actual {
		if err := timer.Validate(); err != nil {
			return err
		}
		if _, duplicate := byID[timer.Ref.ActivationID]; duplicate {
			return fmt.Errorf("inherited workflow timer inventory repeats an activation")
		}
		byID[timer.Ref.ActivationID] = timer
	}
	for _, projection := range expected {
		timer, found := byID[projection.activation.Ref.ActivationID]
		if !found {
			return fmt.Errorf("expected inherited workflow timer is absent")
		}
		if err := timer.ValidateCauseReplay(projection.activation); err != nil {
			return err
		}
		if projection.removed {
			if timer.Status != "cancelled" || timer.CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || !timer.CancelledAt.Equal(projection.activation.CreatedAt) {
				return fmt.Errorf("inherited workflow timer lost its exact rule-removal disposition")
			}
		} else if phase == runForkWorkflowTimerAtCut && (timer.Status != "active" || !timer.FireAt.Equal(projection.activation.FireAt) || !timer.FiredAt.IsZero() || !timer.CancelledAt.IsZero() || timer.CancelCause != "") {
			return fmt.Errorf("unactivated inherited workflow timer already progressed beyond its materialized cut")
		}
	}
	return nil
}

func requireRunForkWorkflowTimerReadbackFrame(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, forkRunID string, bornAt time.Time) error {
	if attempt == nil {
		return fmt.Errorf("workflow timer readback requires its native attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return err
	}
	source, sourceErr := uuid.Parse(plan.SourceRunID)
	child, childErr := uuid.Parse(forkRunID)
	if sourceErr != nil || childErr != nil || source == uuid.Nil || child == uuid.Nil || source == child ||
		source.String() != plan.SourceRunID || child.String() != forkRunID || correlation.RunIDFromContext(ctx) != forkRunID {
		return fmt.Errorf("workflow timer readback requires the exact distinct source and child run context")
	}
	if bornAt.IsZero() || bornAt != bornAt.UTC().Truncate(time.Microsecond) {
		return fmt.Errorf("workflow timer readback requires canonical recorded child birth")
	}
	for _, record := range plan.WorkflowTimers {
		if bornAt.Before(record.CreatedAt) {
			return fmt.Errorf("workflow timer readback child birth precedes its source record")
		}
	}
	return nil
}

// The plan is already fixed-cut admitted. This adapter never reads source rows,
// installs wakeups, or grants execution. The canonical timer writer settles
// ordinary removal here; join and barrier deadlines have distinct owners.
func materializeRunForkWorkflowTimers(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	plan runfork.RunForkPlan,
	forkRunID string,
	selection runForkWorkflowTimerSelectionOwner,
	owner runForkWorkflowTimerMaterializationOwner,
	bornAt time.Time,
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
		if err := owner.MaterializeRunForkWorkflowTimerTx(ctx, attempt, timer.activation, timer.removed); err != nil {
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
		selectedRecord, err := selection.SelectInheritedWorkflowTimerRecord(projected.PersistenceRecord())
		if err != nil {
			return nil, fmt.Errorf("admit selected workflow timer %s: %w", source.Ref.DeclarationKey, err)
		}
		var selected *pipeline.WorkflowTimerActivation
		if selectedRecord != nil {
			value, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(*selectedRecord)
			if err != nil {
				return nil, err
			}
			selected = &value
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
