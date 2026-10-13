package runforkpersistence

import (
	"fmt"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func runForkConstructionOccurrenceOwed(plan runfork.RunForkPlan, entity runfork.RunForkEntityState) (bool, error) {
	meta := entity.MaterializationMetadata
	if meta == nil {
		return false, fmt.Errorf("construction occurrence requires fixed-cut owner metadata")
	}
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(meta.InitialMaterialization,
		plan.SourceRunID, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
	if err != nil || receipt.CreationEvent == nil {
		return false, err
	}
	ids, admitted := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !admitted {
		return false, fmt.Errorf("construction occurrence requires admitted fixed-cut event membership")
	}
	return !slices.Contains(ids, receipt.CreationEvent.EventID), nil
}

// ProjectRunForkConstructionReceipt projects immutable initialization from the
// full admitted fixed-cut plan, including reply, join and bookkeeping evidence.
// born is the child's original materialization birth, not the inspection time.
// This pure projection grants neither fresh construction nor current execution.
func ProjectRunForkConstructionReceipt(plan runfork.RunForkPlan, forkRunID string, entity runfork.RunForkEntityState, born time.Time) (pipeline.FlowConstructionReceipt, error) {
	meta := entity.MaterializationMetadata
	if meta == nil || meta.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance || born.IsZero() {
		return pipeline.FlowConstructionReceipt{}, fmt.Errorf("fork construction requires an immutable receipt and child birth")
	}
	source, err := pipeline.DecodeStoredFlowConstructionReceipt(meta.InitialMaterialization,
		plan.SourceRunID, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	identity, err := runfork.ProjectConstructionIdentity(plan.SourceRunID, forkRunID, source.Identity)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(forkRunID, identity.Route())
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	input := source.CreatingInput
	if input.EventID != "" {
		input.EventID = deterministicRunForkReplayEventID(forkRunID, input.EventID)
	}
	var occurrence *pipeline.DynamicFlowRuntimeCreationEventPlan
	if source.CreationEvent != nil {
		copy := *source.CreationEvent
		copy.RunID, copy.EventID = forkRunID, deterministicRunForkReplayEventID(forkRunID, copy.EventID)
		if copy.ParentEventID != "" {
			copy.ParentEventID = deterministicRunForkReplayEventID(forkRunID, copy.ParentEventID)
		}
		copy.DeliveryContext, err = projectRunForkConstructionContext(plan, forkRunID, copy.DeliveryContext)
		if err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		occurrence = &copy
	}
	child, err := pipeline.ProjectFlowConstructionReceipt(source, owner, identity, input, occurrence)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	initial := entity
	initial.CurrentState = source.InitialState
	initial.Bookkeeping, initial.Accumulator = source.Persisted.Bookkeeping, source.Persisted.Accumulator
	projection, err := runfork.ProjectEntityOwnership(plan.SourceRunID, forkRunID, entity.EntityID, meta.FlowInstance)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	child.Persisted.Bookkeeping, child.Persisted.Accumulator, _, err = projectRunForkEntityExecutionState(initial, plan.SourceRunID, forkRunID, projection)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, err
	}
	child.OccurredAt = born
	return child, nil
}

func projectRunForkConstructionContext(plan runfork.RunForkPlan, forkRunID string, source events.DeliveryContext) (events.DeliveryContext, error) {
	if err := source.Validate(); err != nil {
		return events.DeliveryContext{}, err
	}
	child := events.DeliveryContext{Joins: append([]events.JoinAdmissionReceipt(nil), source.Joins...)}
	if source.Reply != nil {
		id, err := projectRunForkReplyReference(plan.SourceRunID, forkRunID, plan.ReplyContexts, source.Reply.ID)
		if err != nil {
			return events.DeliveryContext{}, err
		}
		child.Reply = &events.ReplyContextRef{ID: id}
	}
	for index, receipt := range source.Joins {
		if receipt.Disposition == events.JoinAdmissionEarly {
			continue
		}
		var err error
		child.Joins[index].Ref, err = projectConstructionReturnAtCut(plan, forkRunID, receipt.Ref)
		if err != nil {
			return events.DeliveryContext{}, err
		}
	}
	return child, child.Validate()
}

func projectConstructionReturnAtCut(plan runfork.RunForkPlan, forkRunID string, ref timeridentity.JoinRef) (timeridentity.JoinRef, error) {
	var entity *runfork.RunForkEntityState
	for i := range plan.Entities {
		if plan.Entities[i].EntityID == ref.StageEntry().EntityID {
			if entity != nil {
				return timeridentity.JoinRef{}, fmt.Errorf("construction return repeats its source owner")
			}
			entity = &plan.Entities[i]
		}
	}
	if entity == nil || entity.MaterializationMetadata == nil {
		return timeridentity.JoinRef{}, fmt.Errorf("construction return has no fixed-cut source owner")
	}
	projection, err := runfork.ProjectEntityOwnership(plan.SourceRunID, forkRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	_, _, correspondence, err := projectRunForkEntityExecutionState(*entity, plan.SourceRunID, forkRunID, projection)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	return projectRunForkJoinReference(ref, plan.SourceRunID, forkRunID, projection, correspondence)
}

func deterministicRunForkReplyContextID(forkRunID, sourceReplyID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:run-fork-reply:"+forkRunID+":"+sourceReplyID)).String()
}
