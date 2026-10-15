package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	lifecycleowner "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/google/uuid"
)

// Materialization records the child's immutable birth in its initial revision.
// Neither the live header nor the physical receipt being compared chooses it.
type runForkReceiverBirth struct {
	owner     flowidentity.RunScopedFlowInstance
	header    pipeline.WorkflowInstanceRecordedHeader
	createdAt time.Time
}

func readRunForkReceiverBirthTx(ctx context.Context, tx *sql.Tx, forkRunID string, header pipeline.WorkflowInstance) (runForkReceiverBirth, error) {
	owner, err := flowidentity.NewRunScopedFlowInstance(forkRunID,
		flowidentity.StoredRoute(header.WorkflowName, header.InstanceID, header.StorageRef))
	if err != nil {
		return runForkReceiverBirth{}, err
	}
	initial, err := ReadConstructedEntityAtRevisionTx(ctx, tx, owner, header.EntityID, 1)
	if err != nil {
		return runForkReceiverBirth{}, fmt.Errorf("read original fork receiver birth: %w", err)
	}
	recorded, err := pipeline.DecodeWorkflowInstanceRecordedHeader(owner.Route, initial.MaterializationMetadata.FlowConfig)
	if err != nil {
		return runForkReceiverBirth{}, fmt.Errorf("decode original fork receiver birth: %w", err)
	}
	if recorded.WorkflowVersion() == "" || initial.MaterializationMetadata.CreatedAt.IsZero() {
		return runForkReceiverBirth{}, fmt.Errorf("read original fork receiver birth: incomplete construction")
	}
	return runForkReceiverBirth{owner: owner, header: recorded, createdAt: initial.MaterializationMetadata.CreatedAt}, nil
}

func runForkConstructionOccurrenceOwed(plan runfork.RunForkPlan, entity runfork.RunForkEntityState) (bool, error) {
	meta := entity.MaterializationMetadata
	if meta == nil {
		return false, fmt.Errorf("construction occurrence requires fixed-cut owner metadata")
	}
	occurrenceID, err := runforkadmission.FixedConstructionOccurrenceID(plan, entity)
	if err != nil || occurrenceID == "" {
		return false, err
	}
	ids, admitted := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !admitted {
		return false, fmt.Errorf("construction occurrence requires admitted fixed-cut event membership")
	}
	return !slices.Contains(ids, occurrenceID), nil
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
	return projectRunForkConstructionReceipt(plan, forkRunID, entity, born, source)
}

func projectRunForkConstructionReceiptTx(ctx context.Context, tx *sql.Tx, plan runfork.RunForkPlan, forkRunID string, entity runfork.RunForkEntityState, born time.Time) (pipeline.FlowConstructionReceipt, error) {
	active := make(map[string]bool)
	var project func(runfork.RunForkPlan, string, runfork.RunForkEntityState, time.Time) (pipeline.FlowConstructionReceipt, error)
	project = func(cut runfork.RunForkPlan, child string, state runfork.RunForkEntityState, birth time.Time) (pipeline.FlowConstructionReceipt, error) {
		if err := ctx.Err(); err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		if active[cut.SourceRunID] {
			return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical construction projection has cyclic lineage")
		}
		active[cut.SourceRunID] = true
		defer delete(active, cut.SourceRunID)
		meta := state.MaterializationMetadata
		if meta == nil {
			return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical projection requires captured construction")
		}
		route := flowidentity.StoredRoute(meta.FlowTemplate, flowidentity.LogicalInstanceID(meta.FlowInstance), meta.FlowInstance)
		if _, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(runfork.RunForkPlan{SourceRunID: cut.SourceRunID, Entities: []runfork.RunForkEntityState{state}}, route); err != nil || !found {
			return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical projection lacks admitted construction: found=%t: %w", found, err)
		}
		history := meta.HistoricalConstruction
		if history == nil {
			return ProjectRunForkConstructionReceipt(cut, child, state, birth)
		}
		origin, err := lifecycleowner.ReadImmutableRunOriginTx(ctx, tx, cut.SourceRunID)
		if err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		if origin.Kind() != runlifecycle.OriginForkMaterialization || origin.SourceRunID() != history.SourceRunID || origin.ForkRevision() != history.SourceRevision {
			return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical construction projection contradicts immutable source origin")
		}
		snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, history.SourceRunID, history.SourceRevision)
		if err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		parent, err := constructionPlanAtSnapshot(snapshot)
		if err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		var original *runfork.RunForkEntityState
		for i := range parent.Entities {
			if parent.Entities[i].EntityID == history.SourceEntity.EntityID {
				original = &parent.Entities[i]
			}
		}
		if original == nil || !reflect.DeepEqual(*original, history.SourceEntity) {
			return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical construction projection has crossed fixed-cut evidence")
		}
		source, err := project(parent, cut.SourceRunID, *original, meta.CreatedAt)
		if err != nil {
			return pipeline.FlowConstructionReceipt{}, err
		}
		return projectRunForkConstructionReceipt(cut, child, state, birth, source)
	}
	return project(plan, forkRunID, entity, born)
}

func constructionPlanAtSnapshot(snapshot *runForkRevisionSnapshot) (runfork.RunForkPlan, error) {
	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Revision: snapshot.Revision}, Entities: entities}
	for _, reply := range snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	return plan, nil
}

func projectRunForkConstructionReceipt(plan runfork.RunForkPlan, forkRunID string, entity runfork.RunForkEntityState, born time.Time, source pipeline.FlowConstructionReceipt) (pipeline.FlowConstructionReceipt, error) {
	meta := entity.MaterializationMetadata
	if meta == nil || born.IsZero() || source.RunID != plan.SourceRunID {
		return pipeline.FlowConstructionReceipt{}, fmt.Errorf("historical projection requires its original source and birth")
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
