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
	owner     events.DeliveryTargetOwnership
	policy    DeliveryTargetCompatibilityPolicy
	flowID    string
	route     runtimeflowidentity.Route
	entityID  string
	event     events.Event
	state     WorkflowState
	instance  WorkflowInstance
	snapshot  runtimeengine.StateSnapshot
	persisted bool
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
	if strings.TrimSpace(a.entityID) == "" || a.entityID != a.owner.Route().EntityID || strings.TrimSpace(a.state.EntityID) != a.entityID {
		return fmt.Errorf("delivery target application entity disagrees with admitted owner")
	}
	if a.persisted {
		if _, err := requireWorkflowInstanceIdentity(a.route, identity.NormalizeEntityID(a.entityID), a.instance); err != nil {
			return fmt.Errorf("delivery target application persisted state disagrees with admitted owner: %w", err)
		}
	}
	return nil
}

func (a DeliveryTargetApplication) persistedInstance() (WorkflowInstance, bool) {
	if !a.persisted {
		return WorkflowInstance{}, false
	}
	return cloneWorkflowInstanceForEngineMutation(a.instance), true
}

func (a DeliveryTargetApplication) persistedSnapshot() (runtimeengine.StateSnapshot, bool, error) {
	if !a.persisted {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(
		a.snapshot.StateCarrier.PersistedFields(),
		a.snapshot.StateCarrier.PersistedBookkeeping(),
		a.snapshot.StateCarrier.Gates,
		a.snapshot.StateCarrier.PersistedStateBuckets(),
	)
	if err != nil {
		return runtimeengine.StateSnapshot{}, false, err
	}
	carrier.Control = a.snapshot.StateCarrier.Control
	return runtimeengine.StateSnapshot{
		EntityID: a.snapshot.EntityID, WorkflowName: a.snapshot.WorkflowName,
		WorkflowVersion: a.snapshot.WorkflowVersion, CurrentState: a.snapshot.CurrentState,
		StateCarrier: carrier, EnteredStateAt: a.snapshot.EnteredStateAt,
	}, true, nil
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
	policy, err := CompileDeliveryTargetCompatibilityPolicy(source, flowID, handlerEventType, handler)
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
		state: WorkflowState{Metadata: map[string]any{}},
	}
	if owner.EntitylessReceiver() {
		if err := application.Validate(); err != nil {
			return DeliveryTargetApplication{}, err
		}
		return application, nil
	}
	application.entityID = owner.Route().EntityID
	if len(previewState) > 0 {
		if len(previewState) != 1 {
			return DeliveryTargetApplication{}, fmt.Errorf("delivery target application accepts at most one exact preview state")
		}
		application.state = cloneDeliveryTargetApplicationState(previewState[0])
		if strings.TrimSpace(application.state.EntityID) == "" {
			application.state.EntityID = application.entityID
		}
		if application.state.Control.FlowPath == "" {
			application.state.Control = runtimeStateControlForDeliveryTarget(route)
		}
		if err := application.Validate(); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate delivery target preview state: %w", err)
		}
		return application, nil
	}
	if pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return DeliveryTargetApplication{}, fmt.Errorf("delivery target application requires workflow persistence")
	}
	instance, exists, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return DeliveryTargetApplication{}, fmt.Errorf("load exact admitted delivery target: %w", err)
	}
	if exists {
		if _, err := requireWorkflowInstanceIdentity(route, identity.NormalizeEntityID(application.entityID), instance); err != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target: %w", err)
		}
		if err := validateDeliveryTargetDeclaredKey(source, flowID, nodeID, handler, executionEvent, policy.Acquisition, instance); err != nil {
			return DeliveryTargetApplication{}, err
		}
		if err := application.applyPersistedInstance(source, flowID, instance); err != nil {
			return DeliveryTargetApplication{}, err
		}
	} else {
		record, stateExists, stateErr := pc.workflowStore.LoadEntityState(ctx, route, identity.NormalizeEntityID(application.entityID))
		if stateErr != nil {
			return DeliveryTargetApplication{}, fmt.Errorf("load exact admitted delivery target state: %w", stateErr)
		}
		if stateExists {
			instance, err = decodeDeliveryTargetWorkflowEntityState(source, flowID, record)
			if err != nil {
				return DeliveryTargetApplication{}, fmt.Errorf("decode exact admitted delivery target state: %w", err)
			}
			if _, err := requireWorkflowInstanceIdentity(route, identity.NormalizeEntityID(application.entityID), instance); err != nil {
				return DeliveryTargetApplication{}, fmt.Errorf("validate exact admitted delivery target state: %w", err)
			}
			if err := validateDeliveryTargetDeclaredKey(source, flowID, nodeID, handler, executionEvent, policy.Acquisition, instance); err != nil {
				return DeliveryTargetApplication{}, err
			}
			if err := application.applyPersistedInstance(source, flowID, instance); err != nil {
				return DeliveryTargetApplication{}, err
			}
		} else if owner.ExistingEntity() {
			return DeliveryTargetApplication{}, fmt.Errorf("existing_entity target %q is missing at execution", owner.Route().FlowInstance)
		} else {
			application.state, err = materializingDeliveryTargetState(source, flowID, handler, executionEvent, owner, policy)
			if err != nil {
				return DeliveryTargetApplication{}, err
			}
		}
	}
	if err := application.Validate(); err != nil {
		return DeliveryTargetApplication{}, err
	}
	return application, nil
}

func (a *DeliveryTargetApplication) applyPersistedInstance(source semanticview.Source, flowID string, instance WorkflowInstance) error {
	if a == nil {
		return fmt.Errorf("delivery target application is required")
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return fmt.Errorf("project exact admitted delivery target state: %w", err)
	}
	carrier.Gates = workflowStateGatesForScope(source, flowID, carrier.Gates)
	a.instance = cloneWorkflowInstanceForEngineMutation(instance)
	a.snapshot = runtimeengine.StateSnapshot{
		EntityID: identity.NormalizeEntityID(instance.EntityID), WorkflowName: strings.TrimSpace(instance.WorkflowName),
		WorkflowVersion: strings.TrimSpace(instance.WorkflowVersion), CurrentState: strings.TrimSpace(instance.CurrentState),
		StateCarrier: carrier, EnteredStateAt: instance.EnteredStageAt,
	}
	a.state = workflowStateForDeliveryTargetInstance(instance)
	a.persisted = true
	return nil
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
	if !workflowInstanceOwnedByFlow(source, instance, flowID) || deliveryTargetWorkflowInstanceTerminal(source, flowID, instance) || !selectEntityCandidateMatches(instance, expected) {
		return fmt.Errorf("%s_conflict: stamped target %q does not satisfy the committed declared key", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(instance.StorageRef))
	}
	return nil
}

func materializingDeliveryTargetState(source semanticview.Source, flowID string, handler SystemNodeEventHandler, evt events.Event, owner events.DeliveryTargetOwnership, policy DeliveryTargetCompatibilityPolicy) (WorkflowState, error) {
	route, err := workflowInstanceRouteForExecution(source, flowID, owner.Route().FlowInstance)
	if err != nil {
		return WorkflowState{}, err
	}
	state := WorkflowState{
		EntityID: owner.Route().EntityID,
		Stage:    NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID)),
		Metadata: workflowMaterializeEntityFields(source, flowID, nil),
		Control:  runtimeStateControlForDeliveryTarget(route),
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

func runtimeStateControlForDeliveryTarget(route runtimeflowidentity.Route) runtimeengine.StateControl {
	return runtimeengine.StateControl{
		FlowPath: route.InstancePath, StorageRef: route.InstancePath, InstanceID: route.InstanceID,
	}
}

func cloneDeliveryTargetApplicationState(state WorkflowState) WorkflowState {
	state.Metadata = cloneStringAnyMap(state.Metadata)
	return state
}
