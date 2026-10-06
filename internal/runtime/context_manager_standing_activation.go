package runtime

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
	InstanceID          string
	FlowInstance        string
	EntityID            string
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
			FlowPath: target.FlowPath, InstanceID: target.InstanceID, FlowInstance: target.FlowInstance, EntityID: target.EntityID,
		}
		if err := add(coordinate); err != nil {
			return nil, err
		}
	}
	for _, activation := range c.StandingActivations {
		coordinate := StandingServiceCoordinate{
			BundleHash: activation.BundleHash, ServiceID: activation.ServiceID, RunID: activation.RunID,
			Generation: activation.Generation, PublicationSequence: activation.PublicationSequence,
			FlowPath: activation.FlowPath, InstanceID: activation.InstanceID, FlowInstance: activation.FlowInstance, EntityID: activation.EntityID,
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
		instance, err := flowidentity.StandingForGeneration(contextDef.Source, activation.FlowPath, activation.RunID)
		if err != nil {
			return err
		}
		if activation.ServiceID != flowidentity.StandingServiceID(activation.FlowPath) ||
			instance.InstanceID != activation.InstanceID || instance.InstancePath != activation.FlowInstance || instance.EntityID != activation.EntityID {
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
