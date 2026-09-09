package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"maps"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func requireOriginalFanOutCarriage(ctx context.Context, source runForkSourceOwnerFunc, plan runfork.RunForkPlan, original semanticview.OriginalLoopCarriage) error {
	if len(plan.FanOutObligations) == 0 {
		return nil
	}
	if source == nil {
		return fmt.Errorf("fork fan-out requires the original run source owner")
	}
	fact, err := source.LoadRunSource(ctx, plan.SourceRunID)
	if err != nil {
		return err
	}
	return original.RequireSource(fact.BundleHash())
}

func projectRunForkFanOutCapsule(ctx context.Context, tx *sql.Tx, forkRunID string, plan runfork.RunForkPlan, obligation runfork.RunForkFanOutObligation, original semanticview.OriginalLoopCarriage) (fanoutobligation.Capsule, *loopruntime.ForkChildReference, error) {
	capsule := obligation.Intent.Request.Capsule
	if err := capsule.Validate(); err != nil {
		return capsule, nil, err
	}
	// The intent is owned by the fixed source run. Trigger lineage is immutable
	// ancestor evidence and may legitimately predate that run after an earlier fork.
	if obligation.Intent.Request.Key.RunID != plan.SourceRunID {
		return capsule, nil, fmt.Errorf("fan-out capsule does not belong to its fixed source run")
	}
	node, err := identity.ParseExecutableNodeKey(capsule.NodeKey)
	if err != nil {
		return capsule, nil, err
	}
	role, declared, err := original.ResolveFanOut(obligation.Intent.Request.PlanRef, node, capsule.HandlerEventKey)
	if err != nil {
		return capsule, nil, err
	}
	for _, field := range []*map[string]any{&capsule.Entity, &capsule.PlatformEntity, &capsule.Computed, &capsule.Accumulated, &capsule.Join, &capsule.Loop, &capsule.StateFields, &capsule.StateBookkeeping} {
		if *field != nil {
			*field, err = cloneForkLoopState(*field)
			if err != nil {
				return fanoutobligation.Capsule{}, nil, err
			}
		}
	}
	capsule.StateGates = maps.Clone(capsule.StateGates)
	if capsule.Receiver != nil {
		receiver := *capsule.Receiver
		capsule.Receiver = &receiver
	}
	var barrierGeneration attemptgeneration.Generation
	if obligation.Barrier != nil {
		if err := obligation.Barrier.Validate(); err != nil {
			return capsule, nil, err
		}
		join, ok := obligation.Barrier.Registration.Handle.JoinRef()
		if !ok || obligation.Barrier.Registration.IntentKey != obligation.Intent.Request.Key {
			return capsule, nil, fmt.Errorf("fan-out barrier does not belong to its fixed source intent")
		}
		barrierGeneration = join.Generation()
		if obligation.Barrier.Registration.EntityID != capsule.EntityID || obligation.Barrier.Registration.Route != capsule.Route {
			return capsule, nil, fmt.Errorf("fan-out barrier contradicts its execution owner")
		}
	}
	projected, err := projectRunForkFanOutExecutionOwnership(plan, forkRunID, capsule)
	if err != nil {
		return capsule, nil, err
	}
	if !declared {
		if len(capsule.Loop) != 0 || barrierGeneration != (attemptgeneration.Generation{}) {
			return capsule, nil, fmt.Errorf("fan-out loop evidence has no original declaration role")
		}
		return projected, nil, nil
	}
	if capsule.ExecutionFlowID != role.FlowID() {
		return capsule, nil, fmt.Errorf("fan-out loop context disagrees with its declaration scope")
	}
	var entity *runfork.RunForkEntityState
	for i := range plan.Entities {
		if plan.Entities[i].EntityID == capsule.EntityID {
			if entity != nil {
				return capsule, nil, fmt.Errorf("duplicate source entity for fan-out loop context")
			}
			entity = &plan.Entities[i]
		}
	}
	if entity == nil || entity.MaterializationMetadata == nil || entity.MaterializationMetadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner {
		return capsule, nil, fmt.Errorf("fan-out loop context lacks fixed source entity ownership")
	}
	metadata := entity.MaterializationMetadata
	if metadata.FlowInstance != capsule.Route.InstancePath {
		return capsule, nil, fmt.Errorf("fan-out loop context contradicts its source entity route")
	}
	projection, err := runfork.ProjectEntityOwnership(plan.SourceRunID, forkRunID, entity.EntityID, metadata.FlowInstance)
	if err != nil {
		return capsule, nil, err
	}
	_, correspondence, err := projectRunForkAttemptGenerationState(entity.Accumulator, forkRunID, projection.Fork.EntityID)
	if err != nil {
		return capsule, nil, err
	}
	source, err := correspondence.AdmitSourceContext(capsule.Loop)
	if err != nil {
		return capsule, nil, err
	}
	generation := source.Generation()
	if generation.FlowID != role.FlowID() || generation.LoopID != role.LoopID() || generation.RevisionField != role.RevisionField() {
		return capsule, nil, fmt.Errorf("fan-out context contradicts its original loop declaration")
	}
	if obligation.Barrier != nil && barrierGeneration != generation {
		return capsule, nil, fmt.Errorf("fan-out barrier and capsule disagree on source generation")
	}
	child, err := correspondence.Bind(source)
	if err != nil {
		return capsule, nil, err
	}
	actual, err := loadRunForkEntityActivations(ctx, tx, forkRunID, projection.Fork.EntityID)
	if err != nil {
		return capsule, nil, err
	}
	if err := correspondence.ValidateChild(child, actual); err != nil {
		return capsule, nil, err
	}
	projected.Loop, err = child.Context()
	if err != nil {
		return capsule, nil, err
	}
	return projected, &child, nil
}

func projectRunForkFanOutExecutionOwnership(plan runfork.RunForkPlan, forkRunID string, capsule fanoutobligation.Capsule) (fanoutobligation.Capsule, error) {
	projected := capsule
	node, err := identity.ParseExecutableNodeKey(capsule.NodeKey)
	if err != nil || node.FlowPath() != capsule.ExecutionFlowID {
		return capsule, fmt.Errorf("fan-out execution scope contradicts its node")
	}
	projected.Route, err = runfork.ProjectExecutionRoute(plan.SourceRunID, forkRunID, capsule.ExecutionFlowID, capsule.Route)
	if err != nil {
		return capsule, err
	}
	projected.ProducerSource, err = runfork.ProjectProducerOwnership(plan.SourceRunID, forkRunID, capsule.ProducerSource)
	if err != nil {
		return capsule, err
	}
	if capsule.EntityID != "" {
		var owner *runfork.RunForkMaterializedEntitySnapshotMetadata
		for _, entity := range plan.Entities {
			if entity.EntityID != capsule.EntityID {
				continue
			}
			if owner != nil || entity.MaterializationMetadata == nil || entity.MaterializationMetadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner {
				return capsule, fmt.Errorf("fan-out execution requires unique fixed-revision entity metadata")
			}
			owner = entity.MaterializationMetadata
		}
		if owner == nil {
			if capsule.Receiver == nil || !capsule.Receiver.Target.MaterializingEntity() {
				return capsule, fmt.Errorf("fan-out existing entity is absent at the fixed revision")
			}
		} else if owner.FlowInstance != capsule.Route.InstancePath {
			return capsule, fmt.Errorf("fan-out execution would rehome its fixed-revision entity")
		}
		entity, err := runfork.ProjectEntityOwnership(plan.SourceRunID, forkRunID, capsule.EntityID, capsule.Route.InstancePath)
		if err != nil {
			return capsule, err
		}
		projected.EntityID = entity.Fork.EntityID
	}
	if capsule.Receiver != nil {
		delivery := *capsule.Receiver
		receiver := delivery.Target.Route()
		if delivery.Node != node || receiver.FlowID != capsule.ExecutionFlowID || receiver.FlowInstance != capsule.Route.InstancePath || receiver.EntityID != capsule.EntityID {
			return capsule, fmt.Errorf("fan-out delivery target contradicts its execution owner")
		}
		receiver.FlowInstance, receiver.EntityID = projected.Route.InstancePath, projected.EntityID
		switch {
		case delivery.Target.ExistingEntity():
			delivery.Target, err = events.NewExistingEntityTarget(receiver)
		case delivery.Target.MaterializingEntity():
			delivery.Target, err = events.NewMaterializingEntityTarget(receiver)
		case delivery.Target.EntitylessReceiver():
			delivery.Target, err = events.NewEntitylessReceiverTarget(receiver)
		default:
			return capsule, fmt.Errorf("fan-out delivery target has no admitted ownership")
		}
		if err != nil {
			return capsule, err
		}
		projected.Receiver = &delivery
	}
	return projected, projected.Validate()
}
