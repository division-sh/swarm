package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func projectRunForkRemovedArrivalRefs(plan runfork.RunForkPlan, childRunID string, refs []timeridentity.JoinRef) (map[string][]timeridentity.JoinRef, error) {
	byEntity := make(map[string][]timeridentity.JoinRef)
	for _, sourceRef := range refs {
		childRef, err := projectConstructionReturnAtCut(plan, childRunID, sourceRef)
		if err != nil {
			return nil, err
		}
		entityID := sourceRef.StageEntry().EntityID
		byEntity[entityID] = append(byEntity[entityID], childRef)
	}
	return byEntity, nil
}

// Both physical state projections consume the same admitted child references.
// The accumulator is a private child projection, never the source snapshot.
func cancelRunForkRemovedArrivalState(accumulator map[string]any, refs []timeridentity.JoinRef) error {
	if len(refs) == 0 {
		return nil
	}
	buckets, err := joinruntime.PersistedBuckets(accumulator)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		arm, found, err := joinruntime.Load(buckets, ref.Node(), ref.Key())
		if err != nil {
			return err
		}
		if !found || !arm.JoinRef().Equal(ref) {
			return fmt.Errorf("removed arrival has no exact projected dependent arm")
		}
		if err := arm.CancelForRuleRemoval(); err != nil {
			return err
		}
		if err := joinruntime.Store(buckets, arm); err != nil {
			return err
		}
	}
	return nil
}
