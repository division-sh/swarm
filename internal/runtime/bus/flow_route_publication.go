package bus

import (
	"context"
	"errors"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// FlowRoutePublication is the process-local ownership receipt for one exact
// activation attempt. It cannot retire a later publication at the same path.
type FlowRoutePublication struct {
	table     *RouteTable
	identity  runtimeflowidentity.RunScopedFlowInstance
	sequence  uint64
	attemptID string
}

type flowRoutePublicationRecord struct {
	sequence  uint64
	attemptID string
}

func (rt *RouteTable) publishFlowInstanceRouteForAttempt(req FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (FlowRoutePublication, error) {
	if rt == nil {
		return FlowRoutePublication{}, errors.New("route table is required")
	}
	if err := attempt.Validate(); err != nil {
		return FlowRoutePublication{}, err
	}
	req = req.Normalized()
	identity, err := normalizeFlowInstanceRouteIdentity(req.Identity)
	if err != nil {
		return FlowRoutePublication{}, err
	}
	if identity.RunID != attempt.RunID() || identity.Route.InstancePath != attempt.InstancePath() {
		return FlowRoutePublication{}, errors.New("flow route publication differs from activation attempt identity")
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	rt.mu.RLock()
	current, published := rt.publications[identity]
	_, routeExists, ownerErr := rt.matchFlowInstanceRouteOwnerLocked(identity)
	rt.mu.RUnlock()
	if ownerErr != nil {
		return FlowRoutePublication{}, ownerErr
	}
	if published {
		if current.attemptID != attempt.ID() {
			return FlowRoutePublication{}, errors.New("flow route predecessor publication has not retired")
		}
		return FlowRoutePublication{table: rt, identity: identity, sequence: current.sequence, attemptID: attempt.ID()}, nil
	}
	if routeExists {
		return FlowRoutePublication{}, errors.New("flow route exists without an activation publication owner")
	}
	if err := rt.addFlowInstanceRoute(req, nil); err != nil {
		return FlowRoutePublication{}, errors.Join(err, rt.removeFlowInstanceRoute(identity))
	}
	rt.mu.Lock()
	rt.nextPublication++
	current = flowRoutePublicationRecord{sequence: rt.nextPublication, attemptID: attempt.ID()}
	rt.publications[identity] = current
	rt.mu.Unlock()
	return FlowRoutePublication{table: rt, identity: identity, sequence: current.sequence, attemptID: current.attemptID}, nil
}

func (p FlowRoutePublication) Retire() error {
	if p.table == nil || p.sequence == 0 || p.attemptID == "" {
		return errors.New("flow route publication handle is invalid")
	}
	rt := p.table
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	rt.mu.RLock()
	current, exists := rt.publications[p.identity]
	rt.mu.RUnlock()
	if !exists || current.sequence != p.sequence || current.attemptID != p.attemptID {
		return nil
	}
	return rt.removeFlowInstanceRoute(p.identity)
}

func (eb *EventBus) PublishPersistedFlowInstanceRouteForAttempt(req FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (FlowRoutePublication, error) {
	if eb == nil {
		return FlowRoutePublication{}, errors.New("event bus is required")
	}
	if _, err := eb.admitSourceArtifactFact(context.Background()); err != nil {
		return FlowRoutePublication{}, err
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return FlowRoutePublication{}, errors.New("route table is not initialized")
	}
	return table.publishFlowInstanceRouteForAttempt(req, attempt)
}

func (eb *EventBus) RetireFlowInstanceRoutePublication(publication FlowRoutePublication) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if err := publication.Retire(); err != nil {
		return fmt.Errorf("retire flow route publication: %w", err)
	}
	return nil
}
