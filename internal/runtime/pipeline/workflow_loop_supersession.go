package pipeline

import (
	"fmt"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func supersedePriorLoopGenerationArtifacts(instance *WorkflowInstance, previousBuckets map[string]any, nextCarrier *runtimeengine.StateCarrier) error {
	if instance == nil || nextCarrier == nil {
		return nil
	}
	previous, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, previousBuckets)
	if err != nil {
		return fmt.Errorf("decode prior loop state: %w", err)
	}
	priorLoops, err := loopruntime.List(previous.StateBuckets)
	if err != nil {
		return fmt.Errorf("list prior loop state: %w", err)
	}
	for _, prior := range priorLoops {
		next, found, err := loopruntime.Load(nextCarrier.StateBuckets, prior.FlowID, prior.LoopID)
		if err != nil {
			return err
		}
		if !found || next.Generation().Equal(prior.Generation()) && next.Status == prior.Status {
			continue
		}
		priorGeneration := prior.Generation()
		joins, err := joinruntime.List(nextCarrier.StateBuckets)
		if err != nil {
			return fmt.Errorf("list joins for loop supersession: %w", err)
		}
		for _, activation := range joins {
			if !activation.Generation.Equal(priorGeneration) || !activation.CloseForStageExit() {
				continue
			}
			activation.TimerCancelled = true
			if err := joinruntime.Store(nextCarrier.StateBuckets, activation); err != nil {
				return fmt.Errorf("supersede join %s: %w", activation.Key(), err)
			}
		}
	}
	return nil
}
