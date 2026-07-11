package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func workflowLoopGenerationForStage(source semanticview.Source, instance *WorkflowInstance, stage string) (attemptgeneration.Generation, bool, error) {
	if source == nil || instance == nil {
		return attemptgeneration.Generation{}, false, nil
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return attemptgeneration.Generation{}, false, fmt.Errorf("decode loop state: %w", err)
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return attemptgeneration.Generation{}, false, fmt.Errorf("list loop state: %w", err)
	}
	stage = strings.TrimSpace(stage)
	var found attemptgeneration.Generation
	for _, activation := range activations {
		if activation.Status != loopruntime.StatusOpen || activation.CurrentStage != stage {
			continue
		}
		if !loopPlanOwnsStage(source, activation.FlowID, activation.LoopID, stage) {
			continue
		}
		if found.Valid() {
			return attemptgeneration.Generation{}, false, fmt.Errorf("stage %s is owned by overlapping active loops", stage)
		}
		found = activation.Generation()
	}
	return found, found.Valid(), nil
}

func workflowLoopGenerationFromBuckets(source semanticview.Source, raw map[string]any) (attemptgeneration.Generation, bool, error) {
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, raw)
	if err != nil {
		return attemptgeneration.Generation{}, false, fmt.Errorf("decode loop state: %w", err)
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return attemptgeneration.Generation{}, false, fmt.Errorf("list loop state: %w", err)
	}
	var found attemptgeneration.Generation
	for _, activation := range activations {
		if activation.Status != loopruntime.StatusOpen || !loopPlanOwnsStage(source, activation.FlowID, activation.LoopID, activation.CurrentStage) {
			continue
		}
		if found.Valid() {
			return attemptgeneration.Generation{}, false, fmt.Errorf("state buckets contain overlapping active loop generations")
		}
		found = activation.Generation()
	}
	return found, found.Valid(), nil
}

func workflowLoopGenerationCurrent(instance *WorkflowInstance, generation attemptgeneration.Generation, expectedStage string) (bool, error) {
	generation = generation.Normalize()
	if !generation.Valid() {
		return true, nil
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return false, err
	}
	activation, ok, err := loopruntime.Load(carrier.StateBuckets, generation.FlowID, generation.LoopID)
	if err != nil || !ok {
		return false, err
	}
	return activation.Status == loopruntime.StatusOpen && activation.Generation().Equal(generation) &&
		(strings.TrimSpace(expectedStage) == "" || activation.CurrentStage == strings.TrimSpace(expectedStage)), nil
}

func loopPlanOwnsStage(source semanticview.Source, flowID, loopID, stage string) bool {
	for _, plan := range semanticview.WorkflowLoops(source) {
		if strings.TrimSpace(plan.ID) != strings.TrimSpace(loopID) || !loopFlowIDMatches(source, plan.FlowID, flowID) {
			continue
		}
		for _, candidate := range plan.RegionStages {
			if strings.TrimSpace(candidate) == strings.TrimSpace(stage) {
				return true
			}
		}
	}
	return false
}

func loopFlowIDMatches(source semanticview.Source, declared, actual string) bool {
	declared, actual = strings.TrimSpace(declared), strings.TrimSpace(actual)
	if declared == actual {
		return true
	}
	return declared == "" && actual == strings.TrimSpace(source.WorkflowName())
}
