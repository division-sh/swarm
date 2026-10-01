package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func projectRunForkStageEntry(source timeridentity.StageEntryRef, sourceRunID, forkRunID string, projection runfork.EntityProjection) (timeridentity.StageEntryRef, error) {
	want, err := runfork.ProjectEntityOwnership(sourceRunID, forkRunID, projection.Source.EntityID, projection.Source.FlowInstance)
	if err != nil || want != projection {
		return timeridentity.StageEntryRef{}, fmt.Errorf("fork stage entry requires exact entity correspondence")
	}
	route := flowidentity.StoredRoute(source.FlowScope, source.InstanceID, projection.Source.FlowInstance)
	root := projection.Source == (runfork.EntityIdentity{EntityID: sourceRunID, FlowInstance: sourceRunID})
	if root {
		route = flowidentity.StoredRoute(sourceRunID, sourceRunID, sourceRunID)
	}
	if err := source.RequireOwner(sourceRunID, route.ScopeKey, route.InstanceID, route.InstancePath, projection.Source.EntityID, source.Stage); err != nil {
		return timeridentity.StageEntryRef{}, err
	}
	// Snapshot admission supplies source history, not a child execution occurrence.
	child := source
	child.RunID, child.EntityID = forkRunID, projection.Fork.EntityID
	if root {
		child.FlowScope, child.InstanceID, child.InstancePath = forkRunID, forkRunID, forkRunID
	}
	if child.OriginRunID == "" {
		child.OriginRunID = source.RunID
	}
	return child, child.Validate()
}

func projectRunForkJoinReference(source timeridentity.JoinRef, sourceRunID, forkRunID string, projection runfork.EntityProjection, correspondence *loopruntime.ForkCorrespondence) (timeridentity.JoinRef, error) {
	entry, err := projectRunForkStageEntry(source.StageEntry(), sourceRunID, forkRunID, projection)
	if err != nil {
		return timeridentity.JoinRef{}, err
	}
	declarationScope := source.StageEntry().FlowScope
	if projection.Source == (runfork.EntityIdentity{EntityID: sourceRunID, FlowInstance: sourceRunID}) {
		declarationScope = "."
	}
	if source.FlowPath() != declarationScope {
		return timeridentity.JoinRef{}, fmt.Errorf("fork join declaration contradicts its source entry scope")
	}
	generation := source.Generation()
	if generation != (attemptgeneration.Generation{}) {
		admitted, err := correspondence.AdmitSource(generation)
		if err != nil {
			return timeridentity.JoinRef{}, fmt.Errorf("fork join generation: %w", err)
		}
		child, err := correspondence.Bind(admitted)
		if err != nil {
			return timeridentity.JoinRef{}, err
		}
		generation = child.Generation()
	}
	return source.Declaration().BindStageEntry(entry, generation)
}

func projectRunForkEntityExecutionState(entity runfork.RunForkEntityState, sourceRunID, forkRunID string, projection runfork.EntityProjection) (map[string]any, map[string]any, *loopruntime.ForkCorrespondence, error) {
	if entity.EntityID != projection.Source.EntityID {
		return nil, nil, nil, fmt.Errorf("fork execution state contradicts its source entity")
	}
	entry, found, err := workflowlifecycle.LoadStageEntry(entity.Bookkeeping)
	if err != nil {
		return nil, nil, nil, err
	}
	// The lifecycle owner stores a typed ref before JSON persistence. Clone other
	// bookkeeping values without interpreting arbitrary business maps as refs.
	bookkeeping := make(map[string]any, len(entity.Bookkeeping))
	for key, value := range entity.Bookkeeping {
		if key == "stage_entry" {
			continue
		}
		bookkeeping[key], err = cloneForkStateValue(value)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fork bookkeeping %q: %w", key, err)
		}
	}
	if found {
		if entry.Stage != entity.CurrentState {
			return nil, nil, nil, fmt.Errorf("fork current stage contradicts its retained lifecycle entry")
		}
		child, err := projectRunForkStageEntry(entry, sourceRunID, forkRunID, projection)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := workflowlifecycle.StoreStageEntry(bookkeeping, child); err != nil {
			return nil, nil, nil, err
		}
	}
	accumulator, correspondence, err := projectRunForkAttemptGenerationState(entity.Accumulator, forkRunID, projection.Fork.EntityID)
	if err != nil {
		return nil, nil, nil, err
	}
	buckets, err := joinruntime.PersistedBuckets(accumulator)
	if err != nil {
		return nil, nil, nil, err
	}
	joins, err := joinruntime.List(buckets)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(joins) > 0 && !found {
		return nil, nil, nil, fmt.Errorf("fork joins require retained lifecycle entry bookkeeping")
	}
	for _, source := range joins {
		ref, err := projectRunForkJoinReference(source.JoinRef(), sourceRunID, forkRunID, projection, correspondence)
		if err != nil {
			return nil, nil, nil, err
		}
		child, err := source.WithForkReference(ref)
		if err != nil {
			return nil, nil, nil, err
		}
		nodeBucket := buckets["handler_joins:"+source.JoinRef().Node().Key()]
		stored := nodeBucket["handler_joins"].(map[string]any)
		if _, occupied := stored[child.Key()]; occupied {
			return nil, nil, nil, fmt.Errorf("fork join destination is already occupied")
		}
		if err := joinruntime.Store(buckets, child); err != nil {
			return nil, nil, nil, err
		}
		delete(stored, source.Key())
	}
	return bookkeeping, accumulator, correspondence, nil
}
