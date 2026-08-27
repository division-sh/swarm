package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// DeliveryTargetApplication is the immutable execution projection of one
// admitted durable target. Engines consume this value; they do not reinterpret
// the stamped target or perform broad entity selection.
type DeliveryTargetApplication struct {
	owner      events.DeliveryTargetOwnership
	policy     DeliveryTargetCompatibilityPolicy
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
func (a DeliveryTargetApplication) Policy() DeliveryTargetCompatibilityPolicy {
	return a.policy
}
func (a DeliveryTargetApplication) FlowID() string                   { return a.flowID }
func (a DeliveryTargetApplication) Route() runtimeflowidentity.Route { return a.route }
func (a DeliveryTargetApplication) EntityID() string                 { return a.entityID }
func (a DeliveryTargetApplication) Event() events.Event              { return a.event }
func (a DeliveryTargetApplication) State() WorkflowState {
	return cloneDeliveryTargetApplicationState(a.state)
}

func (a DeliveryTargetApplication) previewOnly() bool { return a.preview }

func (a DeliveryTargetApplication) Validate() error {
	if err := a.owner.Validate(); err != nil {
		return err
	}
	if err := a.policy.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(a.flowID) == "" || !a.route.Valid() || strings.TrimSpace(a.event.ID()) == "" {
		return fmt.Errorf("delivery target application requires exact flow, route, and event")
	}
	if a.route.InstancePath != a.owner.Route().FlowInstance {
		return fmt.Errorf("delivery target application route disagrees with admitted owner")
	}
	if a.owner.EntitylessReceiver() {
		if a.entityID != "" || strings.TrimSpace(a.state.EntityID) != "" {
			return fmt.Errorf("entityless delivery target application carries entity state")
		}
		return nil
	}
	if a.preview && a.presence != WorkflowTargetPersistenceAbsent {
		return fmt.Errorf("delivery target preview cannot carry persisted state authority")
	}
	if !a.presence.Valid() || a.presence == WorkflowTargetPersistenceLifecycleOnly {
		return fmt.Errorf("delivery target application carries invalid persistence presence")
	}
	if strings.TrimSpace(a.entityID) == "" || a.entityID != a.owner.Route().EntityID || strings.TrimSpace(a.state.EntityID) != a.entityID {
		return fmt.Errorf("delivery target application entity disagrees with admitted owner")
	}
	if strings.TrimSpace(a.entityType) == "" || strings.TrimSpace(a.state.Control.EntityType) != a.entityType {
		return fmt.Errorf("delivery target application requires exact canonical entity contract")
	}
	if a.presence.HasState() {
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
	handlerEventType := evt.Type()
	if handlerFact.eventType != "" {
		handlerEventType = handlerFact.eventType
	}
	policy, err := CompileDeliveryTargetCompatibilityPolicy(source, handlerFact.Node(), flowID, handlerEventType, handler)
	if err != nil {
		return DeliveryTargetApplication{}, err
	}
	route, err := workflowInstanceRouteForExecution(source, flowID, owner.Route().FlowInstance)
	if err != nil {
		return DeliveryTargetApplication{}, err
	}
	executionEvent, err := events.ResolveEnvelope(evt, events.EnvelopeForTargetRoute(evt.NormalizedEnvelope(), owner.Route()))
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("project admitted delivery target onto execution event: %w", err)
	}
	application := DeliveryTargetApplication{
		owner: owner, policy: policy, flowID: flowID, route: route, event: executionEvent,
		state: WorkflowState{Metadata: map[string]any{}}, presence: WorkflowTargetPersistenceAbsent,
	}
	if owner.EntitylessReceiver() {
		if err := application.Validate(); err != nil {
			return DeliveryTargetApplication{}, err
		}
		return application, nil
	}
	entityType, err := requireWorkflowEntityType(source, flowID)
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
		if strings.TrimSpace(application.state.EntityID) == "" {
			application.state.EntityID = application.entityID
		}
		if application.state.Control.FlowPath == "" {
			application.state.Control = runtimeStateControlForDeliveryTarget(route, entityType)
		}
		if err := application.Validate(); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate delivery target preview state: %w", err)
		}
		return application, nil
	}
	if pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return DeliveryTargetApplication{}, fmt.Errorf("delivery target application requires workflow persistence")
	}
	target, err := pc.workflowStore.LoadTargetPersistence(ctx, route, identity.NormalizeEntityID(application.entityID))
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("load exact admitted delivery target persistence: %w", err)
	}
	if err := target.Validate(route, identity.NormalizeEntityID(application.entityID)); err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target persistence: %w", err)
	}
	switch target.Presence {
	case WorkflowTargetPersistenceComplete:
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
		unavailable := deliveryTargetWorkflowInstanceUnavailable(source, flowID, instance)
		if !owned || unavailable {
			return DeliveryTargetApplication{}, fmt.Errorf("exact admitted delivery target lifecycle descriptor or status conflicts with compiled receiver: flow=%q workflow=%q route=%q state=%q status=%q owned=%t unavailable=%t", flowID, instance.WorkflowName, instance.StorageRef, instance.CurrentState, instance.Status, owned, unavailable)
		}
		if err := validateDeliveryTargetDeclaredKey(source, flowID, nodeID, handler, executionEvent, policy.Acquisition, instance); err != nil {
			return DeliveryTargetApplication{}, err
		}
		if err := application.applyPersistedInstance(instance, target.Presence); err != nil {
			return DeliveryTargetApplication{}, err
		}
	case WorkflowTargetPersistenceStateOnly:
		instance, err := decodeDeliveryTargetWorkflowEntityState(source, flowID, evt.RunID(), target.State)
		if err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("decode exact admitted delivery target state: %w", err)
		}
		if _, err := requireWorkflowInstanceIdentity(route, identity.NormalizeEntityID(application.entityID), instance); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target state: %w", err)
		}
		if !workflowInstanceOwnedByFlow(source, instance, flowID, evt.RunID()) || deliveryTargetWorkflowInstanceUnavailable(source, flowID, instance) {
			return DeliveryTargetApplication{}, fmt.Errorf("exact admitted delivery target state conflicts with compiled receiver: flow=%q workflow=%q route=%q status=%q", flowID, instance.WorkflowName, instance.StorageRef, instance.Status)
		}
		if err := validateDeliveryTargetDeclaredKey(source, flowID, nodeID, handler, executionEvent, policy.Acquisition, instance); err != nil {
			return DeliveryTargetApplication{}, err
		}
		if err := application.applyPersistedInstance(instance, target.Presence); err != nil {
			return DeliveryTargetApplication{}, err
		}
	case WorkflowTargetPersistenceLifecycleOnly:
		return DeliveryTargetApplication{}, fmt.Errorf("exact admitted delivery target has lifecycle companion without state")
	case WorkflowTargetPersistenceAbsent:
		if owner.ExistingEntity() {
			return DeliveryTargetApplication{}, fmt.Errorf("existing_entity target %q is missing at execution", owner.Route().FlowInstance)
		} else {
			application.state, err = materializingDeliveryTargetState(source, flowID, entityType, handler, executionEvent, owner, policy)
			if err != nil {
				return DeliveryTargetApplication{}, err
			}
		}
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
	if !presence.HasState() {
		return fmt.Errorf("delivery target application persisted state requires state presence")
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
) (WorkflowInstance, WorkflowTargetPersistencePresence, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, fmt.Errorf("delivery target state requires workflow persistence")
	}
	if err := application.Validate(); err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, err
	}
	if application.Owner().EntitylessReceiver() {
		return WorkflowInstance{}, WorkflowTargetPersistenceAbsent, nil
	}
	entityID := identity.NormalizeEntityID(application.EntityID())
	target, err := pc.workflowStore.LoadTargetPersistence(ctx, application.Route(), entityID)
	if err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, fmt.Errorf("reload exact admitted delivery target persistence: %w", err)
	}
	if err := target.Validate(application.Route(), entityID); err != nil {
		return WorkflowInstance{}, WorkflowTargetPersistencePresenceUnknown, fmt.Errorf("validate reloaded admitted delivery target persistence: %w", err)
	}

	var current WorkflowInstance
	switch target.Presence {
	case WorkflowTargetPersistenceComplete:
		current, err = target.DecodeComplete(application.Route(), entityID)
	case WorkflowTargetPersistenceStateOnly:
		current, err = decodeDeliveryTargetWorkflowEntityState(
			pc.SemanticSource(), application.FlowID(), application.Event().RunID(), target.State,
		)
	case WorkflowTargetPersistenceAbsent:
		if application.Owner().ExistingEntity() {
			return WorkflowInstance{}, target.Presence, fmt.Errorf("existing_entity target %q disappeared before execution", application.Route().InstancePath)
		}
		return WorkflowInstance{}, target.Presence, nil
	case WorkflowTargetPersistenceLifecycleOnly:
		return WorkflowInstance{}, target.Presence, fmt.Errorf("exact admitted delivery target has lifecycle companion without state")
	default:
		return WorkflowInstance{}, target.Presence, fmt.Errorf("exact admitted delivery target has unknown persistence presence")
	}
	if err != nil {
		return WorkflowInstance{}, target.Presence, fmt.Errorf("decode reloaded admitted delivery target: %w", err)
	}
	if _, err := requireWorkflowInstanceIdentity(application.Route(), entityID, current); err != nil {
		return WorkflowInstance{}, target.Presence, fmt.Errorf("validate reloaded admitted delivery target identity: %w", err)
	}
	if err := validateWorkflowEntityType(pc.SemanticSource(), application.FlowID(), current.EntityType); err != nil {
		return WorkflowInstance{}, target.Presence, fmt.Errorf("validate reloaded admitted delivery target entity contract: %w", err)
	}
	if !workflowInstanceOwnedByFlow(pc.SemanticSource(), current, application.FlowID(), application.Event().RunID()) ||
		deliveryTargetWorkflowInstanceUnavailable(pc.SemanticSource(), application.FlowID(), current) {
		return WorkflowInstance{}, target.Presence, fmt.Errorf(
			"reloaded admitted delivery target conflicts with compiled receiver: flow=%q workflow=%q route=%q status=%q",
			application.FlowID(), current.WorkflowName, current.StorageRef, current.Status,
		)
	}
	return current, target.Presence, nil
}

func validateDeliveryTargetDeclaredKey(source semanticview.Source, flowID, nodeID string, handler SystemNodeEventHandler, evt events.Event, acquisition DeliveryTargetAcquisition, instance WorkflowInstance) error {
	if !deliveryTargetHandlerUsesDeclaredKey(handler, acquisition) {
		return nil
	}
	var (
		expected map[string]any
		err      error
	)
	if acquisition == DeliveryTargetAcquisitionSelect {
		expected, err = selectEntityExpectedValues(handler.SelectEntity, evt)
	} else {
		expected, err = selectOrCreateEntityExpectedValues(handler.SelectOrCreateEntity, evt)
	}
	if err != nil {
		return fmt.Errorf("%s_invalid: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	if !workflowInstanceOwnedByFlow(source, instance, flowID, evt.RunID()) || deliveryTargetWorkflowInstanceUnavailable(source, flowID, instance) || !selectEntityCandidateMatches(instance, expected) {
		return fmt.Errorf("%s_conflict: stamped target %q does not satisfy the committed declared key", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(instance.StorageRef))
	}
	return nil
}

func materializingDeliveryTargetState(source semanticview.Source, flowID, entityType string, handler SystemNodeEventHandler, evt events.Event, owner events.DeliveryTargetOwnership, policy DeliveryTargetCompatibilityPolicy) (WorkflowState, error) {
	route, err := workflowInstanceRouteForExecution(source, flowID, owner.Route().FlowInstance)
	if err != nil {
		return WorkflowState{}, err
	}
	state := WorkflowState{
		EntityID: owner.Route().EntityID,
		Stage:    NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID)),
		Metadata: workflowMaterializeEntityFields(source, flowID, nil),
		Control:  runtimeStateControlForDeliveryTarget(route, entityType),
	}
	if policy.Acquisition == DeliveryTargetAcquisitionCreate {
		state.Metadata = workflowCreateEntityFields(source, flowID)
	}
	if policy.Acquisition == DeliveryTargetAcquisitionSelectOrCreate && handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		expected, err := selectOrCreateEntityExpectedValues(handler.SelectOrCreateEntity, evt)
		if err != nil {
			return WorkflowState{}, err
		}
		if state.Metadata == nil {
			state.Metadata = map[string]any{}
		}
		for field, value := range expected {
			values.Wrap(state.Metadata).SetPath(paths.Parse(field), value)
		}
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state, nil
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
