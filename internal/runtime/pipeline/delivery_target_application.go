package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

// DeliveryTargetApplication is the immutable execution projection of one
// admitted durable target. Engines consume this value; they do not reinterpret
// the stamped target or perform broad entity selection.
type DeliveryTargetApplication struct {
	owner      events.DeliveryTargetOwnership
	flowID     string
	entityType string
	route      runtimeflowidentity.Route
	entityID   string
	event      events.Event
	state      WorkflowState
	instance   WorkflowInstance
	presence   WorkflowTargetPersistencePresence
	preview    bool
}

func (a DeliveryTargetApplication) Owner() events.DeliveryTargetOwnership { return a.owner }
func (a DeliveryTargetApplication) FlowID() string                        { return a.flowID }
func (a DeliveryTargetApplication) Route() runtimeflowidentity.Route      { return a.route }
func (a DeliveryTargetApplication) EntityID() string                      { return a.entityID }
func (a DeliveryTargetApplication) Event() events.Event                   { return a.event }
func (a DeliveryTargetApplication) State() WorkflowState {
	return cloneDeliveryTargetApplicationState(a.state)
}

func (a DeliveryTargetApplication) previewOnly() bool { return a.preview }

func (a DeliveryTargetApplication) Validate() error {
	if err := a.owner.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(a.flowID) == "" || !a.route.Valid() || strings.TrimSpace(a.event.ID()) == "" {
		return fmt.Errorf("delivery target application requires exact flow, route, and event")
	}
	if a.route.InstancePath != a.owner.Route().FlowInstance {
		return fmt.Errorf("delivery target application route disagrees with admitted owner")
	}
	if a.owner.EntitylessReceiver() {
		return fmt.Errorf("handler execution requires a constructed lifecycle header")
	}
	if (!a.preview && !a.presence.Constructed()) || (a.preview && a.presence != WorkflowTargetPersistenceAbsent) {
		return fmt.Errorf("delivery target application requires construction evidence, not field-row presence")
	}
	if strings.TrimSpace(a.entityID) == "" || a.entityID != a.owner.Route().EntityID || strings.TrimSpace(a.state.EntityID) != a.entityID {
		return fmt.Errorf("delivery target application entity disagrees with admitted owner")
	}
	if strings.TrimSpace(a.state.Control.EntityType) != a.entityType {
		return fmt.Errorf("delivery target application requires exact canonical entity contract")
	}
	if a.state.Control.FlowPath != a.route.InstancePath || a.state.Control.StorageRef != a.route.InstancePath || a.state.Control.InstanceID != a.route.InstanceID {
		return fmt.Errorf("delivery target application requires exact constructed instance control")
	}
	if a.presence.Constructed() {
		if _, err := requireWorkflowInstanceIdentity(a.route, identity.NormalizeEntityID(a.entityID), a.instance); err != nil {
			return fmt.Errorf("delivery target application persisted state disagrees with admitted owner: %w", err)
		}
		if strings.TrimSpace(a.instance.EntityType) != a.entityType {
			return fmt.Errorf("delivery target application persisted entity contract disagrees with admitted owner")
		}
	}
	return nil
}

type deliveryTargetApplicationContextKey struct{}

func withDeliveryTargetApplication(ctx context.Context, application DeliveryTargetApplication) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, deliveryTargetApplicationContextKey{}, application)
}

func deliveryTargetApplicationFromContext(ctx context.Context) (DeliveryTargetApplication, bool) {
	if ctx == nil {
		return DeliveryTargetApplication{}, false
	}
	application, ok := ctx.Value(deliveryTargetApplicationContextKey{}).(DeliveryTargetApplication)
	return application, ok
}

func (pc *PipelineCoordinator) prepareDeliveryTargetApplication(
	ctx context.Context,
	nodeID string,
	handlerFact DeliveryTargetHandler,
	handler SystemNodeEventHandler,
	evt events.Event,
	owner events.DeliveryTargetOwnership,
	previewState ...WorkflowState,
) (DeliveryTargetApplication, error) {
	if pc == nil {
		return DeliveryTargetApplication{}, fmt.Errorf("delivery target application requires pipeline coordinator")
	}
	source := pc.SemanticSource()
	flowID := handlerFact.ExecutionFlowID(source)
	if targetFlowID := strings.TrimSpace(owner.Route().FlowID); targetFlowID != "" {
		flowID = targetFlowID
	}
	if err := ValidateStampedDeliveryTargetOwnership(source, evt, events.MustNodeDeliveryRecipient(handlerFact.Node()), handlerFact, handler, owner); err != nil {
		return DeliveryTargetApplication{}, err
	}
	if admitted, found := workflowNodeDeliveryRoute(ctx); found && len(admitted.Context.Joins) > 0 {
		if _, _, err := PrepareWorkflowJoinAdmission(source, evt.RunID(), string(evt.Type()), admitted, nil); err != nil {
			return DeliveryTargetApplication{}, err
		}
	}
	// The admitted target names the concrete construction coordinate. Its
	// persisted header validates that coordinate below; a path prefix is not
	// construction evidence for children of keyed parents.
	route := runtimeflowidentity.StoredRoute(runtimeflowidentity.ScopeKey(source, flowID), runtimeflowidentity.LogicalInstanceID(owner.Route().FlowInstance), owner.Route().FlowInstance)
	if !route.Valid() {
		return DeliveryTargetApplication{}, fmt.Errorf("delivery target application requires an exact constructed route")
	}
	executionEvent, err := events.ResolveEnvelope(evt, events.EnvelopeForTargetRoute(evt.NormalizedEnvelope(), owner.Route()))
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("project admitted delivery target onto execution event: %w", err)
	}
	application := DeliveryTargetApplication{
		owner: owner, flowID: flowID, route: route, event: executionEvent,
		state: WorkflowState{Metadata: map[string]any{}}, presence: WorkflowTargetPersistenceAbsent,
	}
	entityType, err := workflowEntityTypeForFlow(source, flowID)
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("resolve delivery target entity contract: %w", err)
	}
	application.entityType = entityType
	application.entityID = owner.Route().EntityID
	if len(previewState) > 0 {
		if len(previewState) != 1 {
			return DeliveryTargetApplication{}, fmt.Errorf("delivery target application accepts at most one exact preview state")
		}
		application.state = cloneDeliveryTargetApplicationState(previewState[0])
		application.preview = true
		if err := application.Validate(); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate delivery target preview state: %w", err)
		}
		return application, nil
	}
	if pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return DeliveryTargetApplication{}, fmt.Errorf("delivery target application requires workflow persistence")
	}
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(evt.RunID(), route)
	if err != nil {
		return DeliveryTargetApplication{}, err
	}
	target, err := pc.workflowStore.LoadTargetPersistence(ctx, flowIdentity, identity.NormalizeEntityID(application.entityID))
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("load exact admitted delivery target persistence: %w", err)
	}
	if err := target.Validate(route, identity.NormalizeEntityID(application.entityID)); err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target persistence: %w", err)
	}
	switch target.Presence {
	case WorkflowTargetPersistenceComplete, WorkflowTargetPersistenceCompleteFieldless:
		instance, err := target.DecodeComplete(route, identity.NormalizeEntityID(application.entityID))
		if err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("decode exact admitted delivery target: %w", err)
		}
		if _, err := requireWorkflowInstanceIdentity(route, identity.NormalizeEntityID(application.entityID), instance); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target: %w", err)
		}
		if err := validateWorkflowEntityType(source, flowID, instance.EntityType); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target entity contract: %w", err)
		}
		owned := workflowInstanceOwnedByFlow(source, instance, flowID, evt.RunID())
		if !owned {
			return DeliveryTargetApplication{}, fmt.Errorf("exact admitted delivery target lifecycle descriptor conflicts with compiled receiver: flow=%q workflow=%q route=%q", flowID, instance.WorkflowName, instance.StorageRef)
		}
		if err := validateAdmittedReceiverAvailability(ctx, source, flowID, evt, instance); err != nil {
			return DeliveryTargetApplication{}, err
		}
		if err := application.applyPersistedInstance(instance, target.Presence); err != nil {
			return DeliveryTargetApplication{}, err
		}
	case WorkflowTargetPersistenceStateOnly, WorkflowTargetPersistenceLifecycleOnly, WorkflowTargetPersistenceAbsent:
		return DeliveryTargetApplication{}, fmt.Errorf("target %q is not constructed: %w", route.InstancePath, runtimeengine.ErrUnconstructedWorkflowTarget)
	default:
		return DeliveryTargetApplication{}, fmt.Errorf("exact admitted delivery target has unknown persistence presence")
	}
	if err := application.Validate(); err != nil {
		return DeliveryTargetApplication{}, err
	}
	return application, nil
}

func (a *DeliveryTargetApplication) applyPersistedInstance(instance WorkflowInstance, presence WorkflowTargetPersistencePresence) error {
	if a == nil {
		return fmt.Errorf("delivery target application is required")
	}
	if !presence.Constructed() {
		return fmt.Errorf("delivery target application requires complete construction evidence")
	}
	a.instance = cloneWorkflowInstanceForEngineMutation(instance)
	a.state = workflowStateForDeliveryTargetInstance(instance)
	a.presence = presence
	return nil
}

// loadCurrentDeliveryTargetState reloads mutable execution state while the
// engine entity lock is held. DeliveryTargetApplication remains the immutable
// identity/policy owner; its pre-lock projection is never mutation authority.
func (pc *PipelineCoordinator) loadCurrentDeliveryTargetState(
	ctx context.Context,
	application DeliveryTargetApplication,
) (WorkflowInstance, WorkflowTargetPersistencePresence, json.RawMessage, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, nil, fmt.Errorf("delivery target state requires workflow persistence")
	}
	if err := application.Validate(); err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, nil, err
	}
	entityID := identity.NormalizeEntityID(application.EntityID())
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(application.Event().RunID(), application.Route())
	if err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, nil, err
	}
	target, err := pc.workflowStore.LoadTargetPersistence(ctx, flowIdentity, entityID)
	if err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, nil, fmt.Errorf("reload exact admitted delivery target persistence: %w", err)
	}
	if err := target.Validate(application.Route(), entityID); err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, nil, fmt.Errorf("validate reloaded admitted delivery target persistence: %w", err)
	}

	var current WorkflowInstance
	switch target.Presence {
	case WorkflowTargetPersistenceComplete, WorkflowTargetPersistenceCompleteFieldless:
		current, err = target.DecodeComplete(application.Route(), entityID)
	case WorkflowTargetPersistenceStateOnly, WorkflowTargetPersistenceLifecycleOnly, WorkflowTargetPersistenceAbsent:
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf("target %q is not constructed: %w", application.Route().InstancePath, runtimeengine.ErrUnconstructedWorkflowTarget)
	default:
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf("exact admitted delivery target has unknown persistence presence")
	}
	if err != nil {
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf("decode reloaded admitted delivery target: %w", err)
	}
	if _, err := requireWorkflowInstanceIdentity(application.Route(), entityID, current); err != nil {
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf("validate reloaded admitted delivery target identity: %w", err)
	}
	if err := validateWorkflowEntityType(pc.SemanticSource(), application.FlowID(), current.EntityType); err != nil {
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf("validate reloaded admitted delivery target entity contract: %w", err)
	}
	if !workflowInstanceOwnedByFlow(pc.SemanticSource(), current, application.FlowID(), application.Event().RunID()) {
		return WorkflowInstance{}, target.Presence, nil, fmt.Errorf(
			"reloaded admitted delivery target conflicts with compiled receiver: flow=%q workflow=%q route=%q status=%q",
			application.FlowID(), current.WorkflowName, current.StorageRef, current.Status,
		)
	}
	if err := validateAdmittedReceiverAvailability(ctx, pc.SemanticSource(), application.FlowID(), application.Event(), current); err != nil {
		return WorkflowInstance{}, target.Presence, nil, err
	}
	return current, target.Presence, append(json.RawMessage(nil), target.Lifecycle.Config...), nil
}

func workflowStateForDeliveryTargetInstance(instance WorkflowInstance) WorkflowState {
	state := WorkflowState{
		EntityID: strings.TrimSpace(instance.EntityID),
		Stage:    NormalizeWorkflowStateID(instance.CurrentState),
		Metadata: cloneStringAnyMap(instance.Fields),
		Control:  workflowInstanceStateControl(instance),
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state
}

func runtimeStateControlForDeliveryTarget(route runtimeflowidentity.Route, entityType string) runtimeengine.StateControl {
	return runtimeengine.StateControl{
		FlowPath: route.InstancePath, StorageRef: route.InstancePath, InstanceID: route.InstanceID,
		EntityType: strings.TrimSpace(entityType),
	}
}

func cloneDeliveryTargetApplicationState(state WorkflowState) WorkflowState {
	state.Metadata = cloneStringAnyMap(state.Metadata)
	return state
}
