package runtime

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// StandingServiceCoordinate is the execution fact shared by clock and ingress
// bindings. Provider transport fields never select a lifecycle owner.
type StandingServiceCoordinate struct {
	BundleHash          string
	ServiceID           string
	RunID               string
	Generation          int64
	PublicationSequence int64
	FlowPath            string
}

func standingActivationsForPublication(existing, incoming []StandingActivation, serviceID, bundleHash string) ([]StandingActivation, int, error) {
	var activations []StandingActivation
	for _, activation := range existing {
		if activation.ServiceID != serviceID {
			activations = append(activations, activation)
		}
	}
	added := 0
	for _, activation := range incoming {
		if activation.ServiceID != serviceID {
			return nil, 0, fmt.Errorf("standing activation service_id does not match publication")
		}
		if activation.BundleHash == bundleHash {
			activations = append(activations, activation)
			added++
		}
	}
	return activations, added, nil
}

func (c BundleContext) standingServiceCoordinates() ([]StandingServiceCoordinate, error) {
	byService := make(map[string]StandingServiceCoordinate)
	add := func(coordinate StandingServiceCoordinate) error {
		if prior, exists := byService[coordinate.ServiceID]; exists && prior != coordinate {
			return fmt.Errorf("standing service %s has contradictory binding coordinates", coordinate.ServiceID)
		}
		byService[coordinate.ServiceID] = coordinate
		return nil
	}
	for _, target := range c.StandingTargets {
		coordinate := StandingServiceCoordinate{
			BundleHash: target.BundleHash, ServiceID: target.ServiceID, RunID: target.RunID,
			Generation: target.Generation, PublicationSequence: target.PublicationSequence,
			FlowPath: target.FlowPath,
		}
		if err := add(coordinate); err != nil {
			return nil, err
		}
	}
	for _, activation := range c.StandingActivations {
		coordinate := StandingServiceCoordinate{
			BundleHash: activation.BundleHash, ServiceID: activation.ServiceID, RunID: activation.RunID,
			Generation: activation.Generation, PublicationSequence: activation.PublicationSequence,
			FlowPath: activation.FlowPath,
		}
		if err := add(coordinate); err != nil {
			return nil, err
		}
	}
	keys := make([]string, 0, len(byService))
	for key := range byService {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	coordinates := make([]StandingServiceCoordinate, 0, len(keys))
	for _, key := range keys {
		coordinates = append(coordinates, byService[key])
	}
	return coordinates, nil
}

func validateRuntimeContextStandingActivations(contextDef BundleContext) error {
	if len(contextDef.StandingActivations) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(contextDef.StandingActivations))
	for _, activation := range contextDef.StandingActivations {
		if err := activation.RestartDisposition.Validate(); err != nil {
			return err
		}
		disposition := activation.RestartDisposition
		if activation.BundleHash != contextDef.BundleHash() || activation.ServiceID != disposition.ServiceID ||
			activation.RunID != disposition.RunID || activation.Generation != disposition.Generation {
			return fmt.Errorf("standing activation does not match its source and acknowledged generation")
		}
		if _, duplicate := seen[activation.ServiceID]; duplicate {
			return fmt.Errorf("standing activation has duplicate service identity")
		}
		seen[activation.ServiceID] = struct{}{}
		if !disposition.Executable() {
			continue
		}
		keyless, err := pipeline.StandingConstructionIsKeyless(contextDef.Source, activation.FlowPath)
		if err != nil {
			return err
		}
		if activation.ServiceID != flowidentity.StandingServiceID(activation.FlowPath) {
			return fmt.Errorf("standing activation does not match its declaring flow")
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
	_, err := StandingExecutionIdentities(contextDef.StandingTargets, contextDef.StandingActivations)
	if err != nil {
		return err
	}
	_, err = contextDef.standingServiceCoordinates()
	return err
}
