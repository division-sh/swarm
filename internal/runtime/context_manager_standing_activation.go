package runtime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func standingActivationsForPublication(existing, incoming []StandingActivation, flowPath, bundleHash string) ([]StandingActivation, int, error) {
	var activations []StandingActivation
	for _, activation := range existing {
		if activation.FlowPath != flowPath {
			activations = append(activations, activation)
		}
	}
	added := 0
	for _, activation := range incoming {
		if activation.FlowPath != flowPath {
			return nil, 0, fmt.Errorf("flow activation does not match publication")
		}
		if activation.BundleHash == bundleHash {
			activations = append(activations, activation)
			added++
		}
	}
	return activations, added, nil
}

func validateRuntimeContextStandingActivations(contextDef BundleContext) error {
	if len(contextDef.StandingActivations) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(contextDef.StandingActivations))
	for _, activation := range contextDef.StandingActivations {
		if activation.BundleHash != contextDef.BundleHash() || activation.RunID == "" ||
			(activation.RunState != runlifecycle.StateLive && activation.RunState != runlifecycle.StatePaused) {
			return fmt.Errorf("flow activation requires its source and live or paused run")
		}
		if _, duplicate := seen[activation.FlowPath]; duplicate {
			return fmt.Errorf("flow activation has duplicate flow identity")
		}
		seen[activation.FlowPath] = struct{}{}
		keyless, err := pipeline.StandingConstructionIsKeyless(contextDef.Source, activation.FlowPath)
		if err != nil {
			return err
		}
		if !keyless {
			if activation.Construction != (flowidentity.Instance{}) {
				return fmt.Errorf("keyed standing declaration cannot publish an eager receiver")
			}
			continue
		}
		instance, err := flowidentity.StandingForGeneration(contextDef.Source, activation.FlowPath, activation.RunID)
		if err != nil {
			return fmt.Errorf("standing activation does not match its constructed instance: %w", err)
		}
		if instance != activation.Construction {
			return fmt.Errorf("standing activation does not match its constructed instance")
		}
	}
	return nil
}
