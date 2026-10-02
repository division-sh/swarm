package bus

import (
	"context"
	"errors"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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

// FlowRoutePublicationHandle grants retirement of one exact local publication.
// Only RouteTable mints production handles; consumers cannot retire by identity.
type FlowRoutePublicationHandle interface {
	Retire() error
}

type flowRoutePublicationRecord struct {
	sequence  uint64
	attemptID string
}

type flowRoutePublicationFence struct {
	identity  runtimeflowidentity.RunScopedFlowInstance
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
	_, fenced := rt.fencedPublications[flowRoutePublicationFence{identity: identity, attemptID: attempt.ID()}]
	_, routeExists, ownerErr := rt.matchFlowInstanceRouteOwnerLocked(identity)
	rt.mu.RUnlock()
	if fenced {
		return FlowRoutePublication{}, errors.New("flow route activation attempt is fenced")
	}
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
	return p.table.retireFlowInstanceRouteForAttempt(p.identity, p.attemptID, p.sequence)
}

func (rt *RouteTable) retireFlowInstanceRouteForAttempt(identity runtimeflowidentity.RunScopedFlowInstance, attemptID string, sequence uint64) error {
	if rt == nil || attemptID == "" {
		return errors.New("flow route activation attempt is required")
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	rt.mu.Lock()
	if rt.fencedPublications == nil {
		rt.fencedPublications = make(map[flowRoutePublicationFence]struct{})
	}
	rt.fencedPublications[flowRoutePublicationFence{identity: identity, attemptID: attemptID}] = struct{}{}
	current, exists := rt.publications[identity]
	rt.mu.Unlock()
	if !exists || current.attemptID != attemptID || (sequence != 0 && current.sequence != sequence) {
		return nil
	}
	return rt.removeFlowInstanceRoute(identity)
}

func (eb *EventBus) RetireFlowInstanceRouteForAttempt(identity runtimeflowidentity.RunScopedFlowInstance, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if err := attempt.Validate(); err != nil {
		return err
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.RunID != attempt.RunID() || identity.Route.InstancePath != attempt.InstancePath() {
		return errors.New("flow retirement identity differs from activation attempt")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	return table.retireFlowInstanceRouteForAttempt(identity, attempt.ID(), 0)
}

func (eb *EventBus) PublishPersistedFlowInstanceRouteForAttempt(ctx context.Context, req FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (FlowRoutePublicationHandle, error) {
	if eb == nil {
		return nil, errors.New("event bus is required")
	}
	admittedCtx, err := eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return nil, err
	}
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(admittedCtx)
	if !ok || source.BundleHash() != attempt.ProcessBinding().BundleHash {
		return nil, errors.New("flow route publication source differs from activation attempt")
	}
	runtimeID, ok := runtimecorrelation.RuntimeInstanceIDFromContext(admittedCtx)
	if !ok || runtimeID != attempt.ProcessBinding().RuntimeInstanceID {
		return nil, errors.New("flow route publication runtime differs from activation attempt")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return nil, errors.New("route table is not initialized")
	}
	return table.publishFlowInstanceRouteForAttempt(req, attempt)
}
