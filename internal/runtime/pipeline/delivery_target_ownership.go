package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// DeliveryTargetOwnerCandidate is exact selected-run evidence available before
// durable delivery admission. Materializing candidates come from an admitted
// same-plan activation, never from an existing entity row.
type DeliveryTargetOwnerCandidate struct {
	Route         events.RouteIdentity
	Materializing bool
}

// DeliveryTargetHandler is the admitted, non-durable receiver fact consumed by
// both route planning and handler execution. An empty handler declaration is a
// valid entityless handler, so presence is represented separately.
type DeliveryTargetHandler struct {
	node      runtimeidentity.ExecutableNode
	eventType events.EventType
	present   bool
}

func NewDeliveryTargetHandler(node runtimeidentity.ExecutableNode) (DeliveryTargetHandler, error) {
	if !node.Valid() {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler requires exact executable node identity")
	}
	return DeliveryTargetHandler{node: node, present: true}, nil
}

func MustDeliveryTargetHandler(node runtimeidentity.ExecutableNode) DeliveryTargetHandler {
	owner, err := NewDeliveryTargetHandler(node)
	if err != nil {
		panic(err)
	}
	return owner
}

func (h DeliveryTargetHandler) Empty() bool { return !h.present }

func (h DeliveryTargetHandler) Equal(other DeliveryTargetHandler) bool {
	return h.present == other.present && h.node.Equal(other.node) && h.eventType == other.eventType
}

func (h DeliveryTargetHandler) FlowID() string {
	if !h.present {
		return ""
	}
	return h.node.FlowID()
}

func (h DeliveryTargetHandler) NodeID() string {
	if !h.present {
		return ""
	}
	return h.node.NodeID()
}

func (h DeliveryTargetHandler) Node() runtimeidentity.ExecutableNode {
	if !h.present {
		return runtimeidentity.ExecutableNode{}
	}
	return h.node
}

// ExecutionFlowID derives the runtime flow scope without changing the
// declaration coordinate. Root project nodes have an explicitly empty owning
// flow in ExecutableNode and execute in the bundle's root flow.
func (h DeliveryTargetHandler) ExecutionFlowID(source semanticview.Source) string {
	if !h.present {
		return ""
	}
	if flowID := h.node.FlowID(); flowID != "" {
		return flowID
	}
	return semanticview.RootExecutionFlowID(source)
}

func (h DeliveryTargetHandler) ForEvent(eventType events.EventType) DeliveryTargetHandler {
	if !h.present {
		return DeliveryTargetHandler{}
	}
	h.eventType = events.EventType(strings.TrimSpace(string(eventType)))
	return h
}

func (h DeliveryTargetHandler) resolve(source semanticview.Source, eventType events.EventType) (SystemNodeEventHandler, bool) {
	if !h.present {
		return SystemNodeEventHandler{}, false
	}
	if h.eventType != "" {
		eventType = h.eventType
	}
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, h.node, string(eventType))
	return resolved.Handler, resolved.Matched
}

// AdmitDeliveryTargetHandler admits one exact authored declaration owner. The
// concrete event is resolved later so wildcard subscriptions remain bounded by
// the same owner without freezing a pattern as an executable handler.
func AdmitDeliveryTargetHandler(source semanticview.Source, node runtimeidentity.ExecutableNode) (DeliveryTargetHandler, error) {
	if source == nil || !node.Valid() {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler requires source and exact executable node identity")
	}
	owner, err := NewDeliveryTargetHandler(node)
	if err != nil {
		return DeliveryTargetHandler{}, err
	}
	if _, ok := source.ExecutableNode(node); !ok {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler node %s has no exact declaration", node.Key())
	}
	return owner, nil
}

type DeliveryTargetOwnershipRequest struct {
	Context              context.Context
	Source               semanticview.Source
	Event                events.Event
	Recipient            events.DeliveryRecipient
	Blueprint            events.RouteIdentity
	Handler              DeliveryTargetHandler
	Candidates           []DeliveryTargetOwnerCandidate
	WorkflowInstances    WorkflowInstancePersistenceReader
	StructuralOwnerProof runtimepinrouting.StructuralTargetOwnerProof
}

// DeliveryTargetEntityDependency is the closed execution-semantic state
// dependency of one admitted handler. It is deliberately independent from how
// a receiver entity is acquired.
type DeliveryTargetEntityDependency uint8

const (
	DeliveryTargetEntityOptional DeliveryTargetEntityDependency = iota
	DeliveryTargetExistingEntityRequired
	DeliveryTargetEntityMaterializing
)

func (d DeliveryTargetEntityDependency) Valid() bool {
	return d <= DeliveryTargetEntityMaterializing
}

func (d DeliveryTargetEntityDependency) merge(other DeliveryTargetEntityDependency) DeliveryTargetEntityDependency {
	if other > d {
		return other
	}
	return d
}

func (d DeliveryTargetEntityDependency) materializes() bool {
	return d == DeliveryTargetEntityMaterializing
}

// DeliveryTargetAcquisition is the closed receiver acquisition policy. The
// zero value means that exact route evidence, rather than a key lookup, owns
// receiver selection.
type DeliveryTargetAcquisition uint8

const (
	DeliveryTargetAcquisitionNone DeliveryTargetAcquisition = iota
	DeliveryTargetAcquisitionCreate
	DeliveryTargetAcquisitionSelect
	DeliveryTargetAcquisitionSelectOrCreate
)

func (a DeliveryTargetAcquisition) Valid() bool {
	return a <= DeliveryTargetAcquisitionSelectOrCreate
}

func (a DeliveryTargetAcquisition) UsesDeclaredKey() bool {
	return a == DeliveryTargetAcquisitionSelect || a == DeliveryTargetAcquisitionSelectOrCreate
}

// DeliveryTargetCompatibilityPolicy is the canonical receiver contract shared
// by routing admission, boot verification, and stamped execution.
type DeliveryTargetCompatibilityPolicy struct {
	Dependency  DeliveryTargetEntityDependency
	Acquisition DeliveryTargetAcquisition
}

func (p DeliveryTargetCompatibilityPolicy) Validate() error {
	if !p.Dependency.Valid() || !p.Acquisition.Valid() {
		return fmt.Errorf("invalid delivery target compatibility policy")
	}
	return nil
}

// ClassifyDeliveryTargetOwnership is the single receiver-side owner for exact
// handler target classification. It returns one closed durable ownership
// variant and never repairs missing evidence from source-event identity.
func ClassifyDeliveryTargetOwnership(req DeliveryTargetOwnershipRequest) (events.DeliveryTargetOwnership, error) {
	if !req.Recipient.IsNode() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("delivery target ownership classification requires a node recipient")
	}
	if isJoinLifecycleEvent(req.Event.Type()) {
		recipient, target, handler, ok, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(req.Source, req.Event)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		if !ok {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("join lifecycle delivery requires its exact declaration handle")
		}
		if req.Recipient != recipient || (!req.Handler.Empty() && !req.Handler.Node().Equal(handler.Node())) {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("join lifecycle delivery route contradicts its exact declaration handler")
		}
		if blueprint := req.Blueprint.Normalized(); !blueprint.Empty() && blueprint != target {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("join lifecycle delivery route contradicts its exact declaration target")
		}
		req.Handler = handler
		req.Blueprint = target
	}
	blueprint := req.Blueprint.Normalized()
	handler, admitted := req.Handler.resolve(req.Source, req.Event.Type())
	if !admitted {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver %s requires an exact admitted target handler for event %s", req.Recipient.ID(), req.Event.Type())
	}
	flowID := req.Handler.ExecutionFlowID(req.Source)
	handlerEventType := req.Event.Type()
	if req.Handler.eventType != "" {
		handlerEventType = req.Handler.eventType
	}
	policy, err := CompileDeliveryTargetCompatibilityPolicy(req.Source, flowID, handlerEventType, handler)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if deliveryTargetHandlerUsesDeclaredKey(handler, policy.Acquisition) && !req.Event.HasTargetRoute() {
		// Declared-key acquisition owns selection only for an explicitly untargeted
		// event. A targeted event already carries admitted exact receiver evidence;
		// re-resolving its payload key could redirect it to another entity.
		blueprint = events.RouteIdentity{FlowID: flowID}
		acquired, err := acquireDeliveryTargetByDeclaredKey(req.Context, req.WorkflowInstances, req.Source, flowID, req.Recipient.ID(), handler, req.Event, policy.Acquisition)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		blueprint = acquired.Route()
		req.Candidates = append(req.Candidates, DeliveryTargetOwnerCandidate{
			Route: acquired.Route(), Materializing: acquired.MaterializingEntity(),
		})
	}
	blueprint.FlowID = flowID
	blueprint = selectedRunRootTargetBlueprint(req.Source, req.Event, flowID, blueprint)
	if blueprint.FlowInstance == "" {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint requires an exact flow instance")
	}
	existing, materializing, err := matchingDeliveryTargetOwnerCandidates(blueprint, req.Candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if len(existing)+len(materializing) == 0 && !req.StructuralOwnerProof.Empty() {
		if err := req.StructuralOwnerProof.Validate(); err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		if req.StructuralOwnerProof.TargetBlueprint() != blueprint {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("compiled structural target-owner proof does not match receiver blueprint")
		}
		owner := req.StructuralOwnerProof.TargetOwner()
		if owner.MaterializingEntity() {
			present := false
			for _, candidate := range materializing {
				present = present || candidate == owner.Route()
			}
			if !present {
				materializing = append(materializing, owner.Route())
			}
		} else if owner.ExistingEntity() {
			present := false
			for _, candidate := range existing {
				present = present || candidate == owner.Route()
			}
			if !present {
				existing = append(existing, owner.Route())
			}
		}
	}
	if len(existing)+len(materializing) > 1 {
		return events.DeliveryTargetOwnership{}, ambiguousDeliveryTargetOwnerError(blueprint.FlowInstance, existing, materializing)
	}
	if len(materializing) == 1 && len(existing) == 0 {
		planned, err := canonicalHandlerMaterializationTarget(req.Source, flowID, handler, req.Event, blueprint)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		if planned != materializing[0] {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("materializing target evidence disagrees with canonical handler identity: evidence=%#v planned=%#v", materializing[0], planned)
		}
		return events.NewMaterializingEntityTarget(materializing[0])
	}
	if len(existing) == 1 && len(materializing) == 0 {
		if policy.Acquisition == DeliveryTargetAcquisitionCreate {
			planned, err := canonicalHandlerMaterializationTarget(req.Source, flowID, handler, req.Event, blueprint)
			if err != nil {
				return events.DeliveryTargetOwnership{}, err
			}
			if planned != existing[0] {
				return events.DeliveryTargetOwnership{}, fmt.Errorf("existing target evidence disagrees with canonical handler identity: evidence=%#v planned=%#v", existing[0], planned)
			}
		}
		return events.NewExistingEntityTarget(existing[0])
	}
	if policy.Acquisition == DeliveryTargetAcquisitionSelect {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("select_entity target owner is missing for flow %q", flowID)
	}
	if policy.Acquisition == DeliveryTargetAcquisitionCreate || policy.Acquisition == DeliveryTargetAcquisitionSelectOrCreate || policy.Dependency == DeliveryTargetEntityMaterializing {
		planned, err := canonicalHandlerMaterializationTarget(req.Source, flowID, handler, req.Event, blueprint)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		return events.NewMaterializingEntityTarget(planned)
	}
	if policy.Dependency == DeliveryTargetEntityOptional {
		blueprint.EntityID = ""
		return events.NewEntitylessReceiverTarget(blueprint)
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", blueprint.FlowInstance)
}

func selectedRunRootTargetBlueprint(source semanticview.Source, evt events.Event, flowID string, blueprint events.RouteIdentity) events.RouteIdentity {
	if source == nil || strings.TrimSpace(flowID) != strings.TrimSpace(source.WorkflowName()) {
		return blueprint
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		return blueprint
	}
	blueprint.FlowInstance = runID
	return blueprint.Normalized()
}

func ValidateDeliveryTargetOwnership(req DeliveryTargetOwnershipRequest, owner events.DeliveryTargetOwnership) error {
	want, err := ClassifyDeliveryTargetOwnership(req)
	if err != nil {
		return err
	}
	if want != owner {
		return fmt.Errorf("stamped delivery target ownership disagrees with exact handler: stamped=%s %#v expected=%s %#v", owner.Code(), owner.Route(), want.Code(), want.Route())
	}
	return nil
}

// ValidateStampedDeliveryTargetOwnership checks immutable route authority
// against the already-resolved executing handler without consulting mutable
// entity rows or rediscovering declarations from a concrete recipient ID.
func ValidateStampedDeliveryTargetOwnership(source semanticview.Source, evt events.Event, recipient events.DeliveryRecipient, handlerFact DeliveryTargetHandler, handler SystemNodeEventHandler, owner events.DeliveryTargetOwnership) error {
	if !recipient.IsNode() {
		return fmt.Errorf("stamped delivery target ownership requires a node recipient")
	}
	if err := owner.Validate(); err != nil {
		return err
	}
	if handlerFact.Empty() {
		return fmt.Errorf("receiver %s requires an admitted target handler", recipient.ID())
	}
	flowID := handlerFact.ExecutionFlowID(source)
	if routeFlowID := owner.Route().FlowID; routeFlowID != "" && routeFlowID != flowID {
		return fmt.Errorf("stamped delivery target flow %q disagrees with handler flow %q", routeFlowID, flowID)
	}
	handlerEventType := evt.Type()
	if handlerFact.eventType != "" {
		handlerEventType = handlerFact.eventType
	}
	policy, err := CompileDeliveryTargetCompatibilityPolicy(source, flowID, handlerEventType, handler)
	if err != nil {
		return err
	}
	switch {
	case owner.EntitylessReceiver():
		if policy.Dependency != DeliveryTargetEntityOptional || policy.Acquisition != DeliveryTargetAcquisitionNone {
			return fmt.Errorf("entityless_receiver ownership disagrees with entity-scoped handler %s", recipient.ID())
		}
	case owner.MaterializingEntity():
		planned, err := canonicalHandlerMaterializationTarget(source, flowID, handler, evt, owner.Route())
		if err != nil {
			return err
		}
		if planned != owner.Route() {
			return fmt.Errorf("materializing_entity ownership disagrees with canonical future identity: stamped=%#v planned=%#v", owner.Route(), planned)
		}
	case owner.ExistingEntity():
		if policy.Acquisition == DeliveryTargetAcquisitionCreate {
			planned, err := canonicalHandlerMaterializationTarget(source, flowID, handler, evt, owner.Route())
			if err != nil {
				return err
			}
			if planned != owner.Route() {
				return fmt.Errorf("existing_entity ownership disagrees with canonical handler identity: stamped=%#v planned=%#v", owner.Route(), planned)
			}
		}
	default:
		return fmt.Errorf("delivery target ownership kind is unsupported")
	}
	return nil
}

// CompileDeliveryTargetCompatibilityPolicy is the shared verifier/runtime
// policy owner. Handler fields own execution dependency; explicit acquisition
// declarations and exact input resolution own acquisition independently.
func CompileDeliveryTargetCompatibilityPolicy(source semanticview.Source, flowID string, eventType events.EventType, handler SystemNodeEventHandler) (DeliveryTargetCompatibilityPolicy, error) {
	flowID = strings.TrimSpace(flowID)
	policy := DeliveryTargetCompatibilityPolicy{
		Dependency:  handlerExecutionEntityRequirement(source, flowID, handler),
		Acquisition: deliveryTargetHandlerAcquisition(handler),
	}
	endpointAcquisition, err := deliveryTargetEndpointAcquisition(source, flowID, eventType)
	if err != nil {
		return DeliveryTargetCompatibilityPolicy{}, err
	}
	if endpointAcquisition != DeliveryTargetAcquisitionNone {
		if policy.Acquisition != DeliveryTargetAcquisitionNone && policy.Acquisition != endpointAcquisition {
			return DeliveryTargetCompatibilityPolicy{}, fmt.Errorf("handler acquisition %s contradicts input resolution %s", deliveryTargetAcquisitionCode(policy.Acquisition), deliveryTargetAcquisitionCode(endpointAcquisition))
		}
		policy.Acquisition = endpointAcquisition
	}
	if err := policy.Validate(); err != nil {
		return DeliveryTargetCompatibilityPolicy{}, err
	}
	return policy, nil
}

func deliveryTargetHandlerAcquisition(handler SystemNodeEventHandler) DeliveryTargetAcquisition {
	switch {
	case handler.CreateEntity:
		return DeliveryTargetAcquisitionCreate
	case handler.SelectEntity != nil && !handler.SelectEntity.Empty():
		return DeliveryTargetAcquisitionSelect
	case handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty():
		return DeliveryTargetAcquisitionSelectOrCreate
	default:
		return DeliveryTargetAcquisitionNone
	}
}

func deliveryTargetHandlerUsesDeclaredKey(handler SystemNodeEventHandler, acquisition DeliveryTargetAcquisition) bool {
	return (acquisition == DeliveryTargetAcquisitionSelect && handler.SelectEntity != nil && !handler.SelectEntity.Empty()) ||
		(acquisition == DeliveryTargetAcquisitionSelectOrCreate && handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty())
}

func deliveryTargetEndpointAcquisition(source semanticview.Source, flowID string, eventType events.EventType) (DeliveryTargetAcquisition, error) {
	if source == nil || strings.TrimSpace(flowID) == "" || strings.TrimSpace(string(eventType)) == "" {
		return DeliveryTargetAcquisitionNone, nil
	}
	association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, string(eventType))
	if association.Status == semanticview.EndpointAssociationAmbiguous {
		return DeliveryTargetAcquisitionNone, association.Err()
	}
	endpoint, ok := association.Endpoint()
	if !ok {
		return DeliveryTargetAcquisitionNone, nil
	}
	switch endpoint.ResolutionMode {
	case runtimecontracts.FlowInputResolutionModeCreate:
		return DeliveryTargetAcquisitionCreate, nil
	case runtimecontracts.FlowInputResolutionModeSelect:
		return DeliveryTargetAcquisitionSelect, nil
	case runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return DeliveryTargetAcquisitionSelectOrCreate, nil
	default:
		return DeliveryTargetAcquisitionNone, nil
	}
}

func deliveryTargetAcquisitionCode(acquisition DeliveryTargetAcquisition) string {
	switch acquisition {
	case DeliveryTargetAcquisitionCreate:
		return "create_entity"
	case DeliveryTargetAcquisitionSelect:
		return "select_entity"
	case DeliveryTargetAcquisitionSelectOrCreate:
		return "select_or_create_entity"
	default:
		return "none"
	}
}

func acquireDeliveryTargetByDeclaredKey(
	ctx context.Context,
	reader WorkflowInstancePersistenceReader,
	source semanticview.Source,
	flowID, nodeID string,
	handler SystemNodeEventHandler,
	evt events.Event,
	acquisition DeliveryTargetAcquisition,
) (events.DeliveryTargetOwnership, error) {
	if !acquisition.UsesDeclaredKey() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("declared-key acquisition requires select or select-or-create policy")
	}
	if reader == nil {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_unavailable: workflow instance reader is required for node %s flow %s", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID))
	}
	if ctx == nil {
		ctx = context.Background()
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
		return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_invalid: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	selectionOwner, err := AdmitWorkflowEntityStateSelectionOwner(source, flowID)
	if err != nil {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_lookup_failed: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	stateRecords, err := reader.SelectActiveWorkflowEntityStates(
		ctx,
		selectionOwner,
		selectEntityFieldSelectors(expected),
		source.FlowTerminalStages(flowID),
	)
	if err != nil {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_lookup_failed: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	matches := make([]WorkflowInstance, 0, len(stateRecords))
	for _, record := range stateRecords {
		candidate, err := decodeDeliveryTargetWorkflowEntityState(source, flowID, record)
		if err != nil {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_lookup_failed: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
		}
		if !workflowInstanceOwnedByFlow(source, candidate, flowID) || deliveryTargetWorkflowInstanceTerminal(source, flowID, candidate) || !selectEntityCandidateMatches(candidate, expected) {
			continue
		}
		matches = append(matches, candidate)
	}
	switch len(matches) {
	case 0:
		if acquisition == DeliveryTargetAcquisitionSelect {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("select_entity_no_match: node %s flow %s found no entity matching declared key", strings.TrimSpace(nodeID), strings.TrimSpace(flowID))
		}
		return acquireSelectOrCreateMaterializingTarget(ctx, reader, source, flowID, nodeID, evt, expected)
	case 1:
		route, err := deliveryTargetRouteForWorkflowInstance(source, flowID, matches[0])
		if err != nil {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_no_match: node %s flow %s: %w", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
		}
		return events.NewExistingEntityTarget(route)
	default:
		return events.DeliveryTargetOwnership{}, fmt.Errorf("%s_ambiguous: node %s flow %s found %d entities matching declared key", deliveryTargetAcquisitionCode(acquisition), strings.TrimSpace(nodeID), strings.TrimSpace(flowID), len(matches))
	}
}

func acquireSelectOrCreateMaterializingTarget(ctx context.Context, reader WorkflowInstancePersistenceReader, source semanticview.Source, flowID, nodeID string, evt events.Event, expected map[string]any) (events.DeliveryTargetOwnership, error) {
	instanceID, err := selectOrCreateEntityInstanceID(source, flowID, expected)
	if err != nil {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s: %w", strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	identity := deriveFlowInstanceIdentity(source, flowID, instanceID)
	route := events.RouteIdentity{FlowID: strings.TrimSpace(flowID), FlowInstance: identity.InstancePath, EntityID: identity.EntityID}.Normalized()
	existing, ok, err := reader.LoadWorkflowInstance(ctx, identity.Route())
	if err != nil {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_lookup_failed: node %s flow %s: %w", strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
	}
	if !ok {
		record, stateExists, stateErr := reader.LoadWorkflowEntityState(ctx, identity.Route(), runtimeidentity.NormalizeEntityID(identity.EntityID))
		if stateErr != nil {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_lookup_failed: node %s flow %s: %w", strings.TrimSpace(nodeID), strings.TrimSpace(flowID), stateErr)
		}
		if stateExists {
			existing, err = decodeDeliveryTargetWorkflowEntityState(source, flowID, record)
			if err != nil {
				return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_lookup_failed: node %s flow %s: %w", strings.TrimSpace(nodeID), strings.TrimSpace(flowID), err)
			}
			ok = true
		}
	}
	if ok {
		if !workflowInstanceOwnedByFlow(source, existing, flowID) || deliveryTargetWorkflowInstanceTerminal(source, flowID, existing) || !selectEntityCandidateMatches(existing, expected) {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_conflict: node %s flow %s deterministic entity %s exists but does not match declared active key", strings.TrimSpace(nodeID), strings.TrimSpace(flowID), route.EntityID)
		}
		existingRoute, err := deliveryTargetRouteForWorkflowInstance(source, flowID, existing)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		if existingRoute != route {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("select_or_create_entity_conflict: deterministic target %#v disagrees with persisted target %#v", route, existingRoute)
		}
		return events.NewExistingEntityTarget(existingRoute)
	}
	return events.NewMaterializingEntityTarget(route)
}

func decodeDeliveryTargetWorkflowEntityState(source semanticview.Source, flowID string, record WorkflowEntityStatePersistenceRecord) (WorkflowInstance, error) {
	route, err := workflowInstanceRouteForExecution(source, flowID, record.FlowInstance)
	if err != nil {
		return WorkflowInstance{}, fmt.Errorf("decode declared-key entity state route: %w", err)
	}
	workflowVersion := ""
	if source != nil {
		workflowVersion = source.WorkflowVersion()
	}
	mode := workflowPersistedFlowMode(source, flowID)
	if mode == "" {
		return WorkflowInstance{}, fmt.Errorf("decode declared-key entity state: flow %s has unsupported persistence mode", strings.TrimSpace(flowID))
	}
	return DecodeWorkflowEntityStatePersistenceRecord(record, route, strings.TrimSpace(flowID), workflowVersion, mode)
}

func deliveryTargetRouteForWorkflowInstance(source semanticview.Source, flowID string, instance WorkflowInstance) (events.RouteIdentity, error) {
	storedRoute, err := workflowInstanceRouteForPersisted(source, instance)
	if err != nil {
		return events.RouteIdentity{}, err
	}
	entityID, err := workflowInstancePersistedEntityID(instance)
	if err != nil {
		return events.RouteIdentity{}, err
	}
	route := events.RouteIdentity{FlowID: strings.TrimSpace(flowID), FlowInstance: storedRoute.InstancePath, EntityID: entityID.String()}.Normalized()
	if route.FlowInstance == "" || route.EntityID == "" {
		return events.RouteIdentity{}, fmt.Errorf("persisted workflow instance is missing exact delivery target identity")
	}
	return route, nil
}

func deliveryTargetWorkflowInstanceTerminal(source semanticview.Source, flowID string, instance WorkflowInstance) bool {
	if strings.TrimSpace(instance.Status) == "terminated" || !instance.TerminatedAt.IsZero() {
		return true
	}
	for _, terminal := range source.FlowTerminalStages(flowID) {
		if strings.EqualFold(strings.TrimSpace(terminal), strings.TrimSpace(instance.CurrentState)) {
			return true
		}
	}
	return false
}

func matchingDeliveryTargetOwnerCandidates(blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate) ([]events.RouteIdentity, []events.RouteIdentity, error) {
	blueprint = blueprint.Normalized()
	exact := make([]DeliveryTargetOwnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		route := candidate.Route.Normalized()
		if route.FlowInstance == "" || route.EntityID == "" {
			return nil, nil, fmt.Errorf("receiver target owner candidate requires exact flow instance and entity identity: %#v", route)
		}
		if route.FlowInstance != blueprint.FlowInstance {
			continue
		}
		if blueprint.EntityID != "" && route.EntityID != blueprint.EntityID {
			return nil, nil, fmt.Errorf("receiver target owner candidate entity %q disagrees with receiver entity %q for instance %q", route.EntityID, blueprint.EntityID, blueprint.FlowInstance)
		}
		candidate.Route = route
		exact = append(exact, candidate)
	}
	candidates = exact
	existingSet := map[events.RouteIdentity]struct{}{}
	materializingSet := map[events.RouteIdentity]struct{}{}
	for _, candidate := range candidates {
		route := candidate.Route.Normalized()
		route.FlowID = blueprint.FlowID
		if candidate.Materializing {
			materializingSet[route] = struct{}{}
		} else {
			existingSet[route] = struct{}{}
		}
	}
	existing := sortedTargetOwnerRoutes(existingSet)
	materializing := sortedTargetOwnerRoutes(materializingSet)
	if len(existing) > 0 && len(materializing) > 0 {
		return nil, nil, fmt.Errorf("receiver target has contradictory existing and materializing ownership evidence for flow instance %q", blueprint.FlowInstance)
	}
	return existing, materializing, nil
}

func sortedTargetOwnerRoutes(set map[events.RouteIdentity]struct{}) []events.RouteIdentity {
	out := make([]events.RouteIdentity, 0, len(set))
	for route := range set {
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowInstance != out[j].FlowInstance {
			return out[i].FlowInstance < out[j].FlowInstance
		}
		return out[i].EntityID < out[j].EntityID
	})
	return out
}

func ambiguousDeliveryTargetOwnerError(flowInstance string, groups ...[]events.RouteIdentity) error {
	candidates := []string{}
	for _, group := range groups {
		for _, route := range group {
			candidates = append(candidates, route.FlowInstance+"/"+route.EntityID)
		}
	}
	sort.Strings(candidates)
	return fmt.Errorf("receiver target owner is ambiguous for flow instance %q; candidates: %s", flowInstance, strings.Join(candidates, ", "))
}

type handlerEntityFieldClassifier func(semanticview.Source, string, SystemNodeEventHandler) DeliveryTargetEntityDependency

// Every executable handler field has one explicit execution-semantic
// disposition. The result owns both routing admission and engine preparation.
var systemNodeEventHandlerEntityClassifiers = map[string]handlerEntityFieldClassifier{
	"Action": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return actionEntityRequirement(handler.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return activityEntityRequirement(handler.Activity)
	},
	"CreateEntity":         noHandlerEntityRequirement,
	"SelectEntity":         noHandlerEntityRequirement,
	"SelectOrCreateEntity": noHandlerEntityRequirement,
	"Description":          noHandlerEntityRequirement,
	"EvidenceTarget":       noHandlerEntityRequirement,
	"Emit": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return materializingWhen(emitSpecReferencesEntity(handler.Emit))
	},
	"OnSuccess": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(emitSpecReferencesEntity(handler.OnSuccess.Emit))
	},
	"Guard": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return guardEntityRequirement(handler.Guard)
	},
	"AdvancesTo": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return materializingWhen(strings.TrimSpace(handler.AdvancesTo) != "")
	},
	"SetsGate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return materializingWhen(gateSpecName(handler.SetsGate) != "")
	},
	"ClearGates": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return materializingWhen(len(handler.ClearGates) != 0)
	},
	"DataAccumulation": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		if workflowDataWritesEntityFields(handler.DataAccumulation, workflowEntitySchemaFields(source, flowID)) {
			return DeliveryTargetEntityMaterializing
		}
		return existingWhen(dataAccumulationReferencesEntity(handler.DataAccumulation))
	},
	"Condition": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(expressionReferencesEntity(handler.Condition))
	},
	"Logic": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(expressionReferencesEntity(handler.Logic))
	},
	"Loop": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(handler.Loop != nil)
	},
	"OnComplete": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return completionRulesEntityRequirement(source, flowID, handler.OnComplete)
	},
	"Rules": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return selectableRulesEntityRequirement(source, flowID, handler.Rules)
	},
	"Accumulate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(handler.Accumulate != nil)
	},
	"Join": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(handler.Join != nil)
	},
	"Compute": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		if computeStoresEntityField(handler.Compute, workflowEntitySchemaFields(source, flowID)) {
			return DeliveryTargetEntityMaterializing
		}
		return existingWhen(computeReferencesEntity(handler.Compute))
	},
	"Query": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(queryReferencesEntity(handler.Query))
	},
	"FanOut": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return fanOutEntityRequirement(handler.FanOut)
	},
	"GroupBy": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(groupByReferencesEntity(handler.GroupBy))
	},
	"Filter": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(filterReferencesEntity(handler.Filter))
	},
	"Reduce": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(reduceReferencesEntity(handler.Reduce))
	},
	"Count": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(countReferencesEntity(handler.Count))
	},
	"Clear": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
		return existingWhen(handler.Clear != nil && len(handler.Clear.Targets) != 0)
	},
}

type handlerRuleEntityFieldClassifier func(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency

var handlerRuleEntryEntityClassifiers = map[string]handlerRuleEntityFieldClassifier{
	"ID":          noHandlerRuleEntityRequirement,
	"Description": noHandlerRuleEntityRequirement,
	"Condition": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return existingWhen(expressionReferencesEntity(rule.Condition))
	},
	"PolicyRow": noHandlerRuleEntityRequirement,
	"AdvancesTo": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return materializingWhen(strings.TrimSpace(rule.AdvancesTo) != "")
	},
	"Emit": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return materializingWhen(emitSpecReferencesEntity(rule.Emit))
	},
	"Action": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return actionEntityRequirement(rule.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return activityEntityRequirement(rule.Activity)
	},
	"DataAccumulation": func(source semanticview.Source, flowID string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		if workflowDataWritesEntityFields(rule.DataAccumulation, workflowEntitySchemaFields(source, flowID)) {
			return DeliveryTargetEntityMaterializing
		}
		return existingWhen(dataAccumulationReferencesEntity(rule.DataAccumulation))
	},
	"Compute": func(source semanticview.Source, flowID string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		if computeStoresEntityField(rule.Compute, workflowEntitySchemaFields(source, flowID)) {
			return DeliveryTargetEntityMaterializing
		}
		return existingWhen(computeReferencesEntity(rule.Compute))
	},
	"FanOut": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
		return fanOutEntityRequirement(rule.FanOut)
	},
}

func handlerExecutionEntityRequirement(source semanticview.Source, flowID string, handler SystemNodeEventHandler) DeliveryTargetEntityDependency {
	requirement := DeliveryTargetEntityOptional
	for _, classify := range systemNodeEventHandlerEntityClassifiers {
		requirement = requirement.merge(classify(source, flowID, handler))
	}
	return requirement
}

func noHandlerEntityRequirement(semanticview.Source, string, SystemNodeEventHandler) DeliveryTargetEntityDependency {
	return DeliveryTargetEntityOptional
}

func noHandlerRuleEntityRequirement(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
	return DeliveryTargetEntityOptional
}

func selectableRulesEntityRequirement(source semanticview.Source, flowID string, rules []runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
	return rulesEntityRequirement(source, flowID, rules, true)
}

func completionRulesEntityRequirement(source semanticview.Source, flowID string, rules []runtimecontracts.HandlerRuleEntry) DeliveryTargetEntityDependency {
	return rulesEntityRequirement(source, flowID, rules, false)
}

func rulesEntityRequirement(source semanticview.Source, flowID string, rules []runtimecontracts.HandlerRuleEntry, effectsSelectable bool) DeliveryTargetEntityDependency {
	requirement := DeliveryTargetEntityOptional
	for _, rule := range rules {
		for field, classify := range handlerRuleEntryEntityClassifiers {
			if !effectsSelectable && (field == "Action" || field == "Activity") {
				continue
			}
			requirement = requirement.merge(classify(source, flowID, rule))
		}
	}
	return requirement
}

func existingWhen(required bool) DeliveryTargetEntityDependency {
	if required {
		return DeliveryTargetExistingEntityRequired
	}
	return DeliveryTargetEntityOptional
}

func materializingWhen(required bool) DeliveryTargetEntityDependency {
	if required {
		return DeliveryTargetEntityMaterializing
	}
	return DeliveryTargetEntityOptional
}

func actionEntityRequirement(action runtimecontracts.ActionSpec) DeliveryTargetEntityDependency {
	if actionMaterializesEntity(action) {
		return DeliveryTargetEntityMaterializing
	}
	return existingWhen(actionReferencesEntity(action))
}

func activityEntityRequirement(activity runtimecontracts.ActivitySpec) DeliveryTargetEntityDependency {
	return existingWhen((activity.Approval != nil && strings.TrimSpace(activity.Approval.Decision) != "") || expressionValueMapReferencesEntity(activity.Input))
}

func guardEntityRequirement(guard *runtimecontracts.GuardSpec) DeliveryTargetEntityDependency {
	if guard == nil {
		return DeliveryTargetEntityOptional
	}
	failure, err := guard.FailureSpec()
	return existingWhen((err == nil && failure.Action == runtimecontracts.GuardFailureActionKill) || guardReferencesEntity(guard))
}

func fanOutEntityRequirement(spec *runtimecontracts.FanOutSpec) DeliveryTargetEntityDependency {
	if spec == nil {
		return DeliveryTargetEntityOptional
	}
	if emitSpecReferencesEntity(spec.Emit) {
		return DeliveryTargetEntityMaterializing
	}
	return existingWhen(fanOutReferencesEntity(spec))
}

func guardReferencesEntity(guard *runtimecontracts.GuardSpec) bool {
	if guard == nil {
		return false
	}
	for _, check := range guard.EffectiveChecks() {
		if expressionReferencesEntity(check.Check) {
			return true
		}
	}
	return emitSpecReferencesEntity(guard.OnFailSpec.Escalation)
}

func queryReferencesEntity(query *runtimecontracts.QuerySpec) bool {
	if query == nil {
		return false
	}
	if typedPathReferencesEntity(query.Source, query.SourcePath) ||
		typedPathReferencesEntity(query.StoreAs, query.StorePath) ||
		typedPathReferencesEntity(query.Entities, query.EntitiesPath) ||
		expressionReferencesEntity(query.Filter) ||
		typedPathReferencesEntity(query.GroupBy, query.GroupByPath) {
		return true
	}
	return false
}

func actionReferencesEntity(action runtimecontracts.ActionSpec) bool {
	if typedPathReferencesEntity(action.InstanceIDFrom, action.InstanceIDPath) {
		return true
	}
	if action.ConfigFrom != nil {
		for _, binding := range action.ConfigFrom.Entries {
			if typedPathReferencesEntity(binding.Ref, binding.RefPath) {
				return true
			}
		}
		for _, ref := range action.ConfigFrom.Bindings {
			if pathReferencesEntity(ref) {
				return true
			}
		}
	}
	if mailboxReferencesEntity(action.Mailbox) || artifactRepoReferencesEntity(action.ArtifactRepo) {
		return true
	}
	return false
}

func mailboxReferencesEntity(mailbox *runtimecontracts.MailboxWriteSpec) bool {
	if mailbox == nil {
		return false
	}
	return expressionValueReferencesEntity(mailbox.ItemType) ||
		expressionValueReferencesEntity(mailbox.Severity) ||
		expressionValueReferencesEntity(mailbox.Summary) ||
		expressionValueReferencesEntity(mailbox.EntityID) ||
		expressionValueReferencesEntity(mailbox.FlowInstance) ||
		expressionValueMapReferencesEntity(mailbox.Payload)
}

func artifactRepoReferencesEntity(spec *runtimecontracts.ArtifactRepoSpec) bool {
	if spec == nil {
		return false
	}
	if expressionValueReferencesEntity(spec.RepoID) ||
		expressionValueReferencesEntity(spec.Namespace) ||
		expressionValueReferencesEntity(spec.PartitionKey) ||
		expressionValueReferencesEntity(spec.DisplaySlug) ||
		expressionValueReferencesEntity(spec.RequestID) ||
		expressionValueReferencesEntity(spec.Author) ||
		expressionValueMapReferencesEntity(spec.Provenance) ||
		expressionValueMapReferencesEntity(spec.SuccessPayload) ||
		expressionValueMapReferencesEntity(spec.FailurePayload) {
		return true
	}
	for _, file := range spec.Files {
		if expressionValueReferencesEntity(file.Path) || expressionValueReferencesEntity(file.Content) {
			return true
		}
	}
	return false
}

func dataAccumulationReferencesEntity(spec runtimecontracts.WorkflowDataAccumulation) bool {
	for _, write := range spec.Writes {
		if typedPathReferencesEntity(write.Source(), write.SourcePath) ||
			typedPathReferencesEntity(write.Target(), write.TargetPath) ||
			expressionValueReferencesEntity(write.Value) ||
			expressionValueReferencesEntity(write.Key) ||
			expressionValueReferencesEntity(write.Index) {
			return true
		}
	}
	return false
}

func emitSpecReferencesEntity(spec runtimecontracts.EmitSpec) bool {
	return pathReferencesEntity(spec.From) || expressionValueMapReferencesEntity(spec.Fields)
}

func fanOutReferencesEntity(spec *runtimecontracts.FanOutSpec) bool {
	if spec == nil {
		return false
	}
	return typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		pathReferencesEntity(spec.Identity) ||
		emitSpecReferencesEntity(spec.Emit)
}

func groupByReferencesEntity(spec *runtimecontracts.GroupBySpec) bool {
	if spec == nil {
		return false
	}
	return typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		typedPathReferencesEntity(spec.Key, spec.KeyPath) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func filterReferencesEntity(spec *runtimecontracts.FilterSpec) bool {
	if spec == nil {
		return false
	}
	return collectionSourceReferencesEntity(spec.Source, spec.SourcePath, spec.ItemsFrom, spec.ItemsPath) ||
		expressionReferencesEntity(spec.Condition) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func reduceReferencesEntity(spec *runtimecontracts.ReduceSpec) bool {
	if spec == nil {
		return false
	}
	return collectionSourceReferencesEntity(spec.Source, spec.SourcePath, spec.ItemsFrom, spec.ItemsPath) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func countReferencesEntity(spec *runtimecontracts.CountSpec) bool {
	if spec == nil {
		return false
	}
	return collectionSourceReferencesEntity(spec.Source, spec.SourcePath, spec.ItemsFrom, spec.ItemsPath) ||
		expressionReferencesEntity(spec.Condition) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func collectionSourceReferencesEntity(source string, sourcePath paths.Path, itemsFrom string, itemsPath paths.Path) bool {
	if strings.TrimSpace(itemsFrom) != "" {
		return typedPathReferencesEntity(itemsFrom, itemsPath)
	}
	return typedPathReferencesEntity(source, sourcePath)
}

func computeReferencesEntity(spec *runtimecontracts.ComputeSpec) bool {
	if spec == nil {
		return false
	}
	if pathReferencesEntity(spec.StoreAs) {
		return true
	}
	if spec.Lookup != nil {
		for index, raw := range spec.Lookup.On {
			var path paths.Path
			if index < len(spec.Lookup.OnPaths) {
				path = spec.Lookup.OnPaths[index]
			}
			if typedPathReferencesEntity(raw, path) {
				return true
			}
		}
	}
	for _, inputPaths := range []map[string]paths.Path{computeValidationInputPaths(spec), computeModuleInputPaths(spec)} {
		for _, path := range inputPaths {
			if path.Root == paths.RootEntity {
				return true
			}
		}
	}
	return false
}

func computeValidationInputPaths(spec *runtimecontracts.ComputeSpec) map[string]paths.Path {
	if spec.Validation == nil {
		return nil
	}
	return spec.Validation.InputPaths
}

func computeModuleInputPaths(spec *runtimecontracts.ComputeSpec) map[string]paths.Path {
	if spec.Module == nil {
		return nil
	}
	return spec.Module.InputPaths
}

func joinReferencesEntity(source semanticview.Source, flowID string, spec *runtimecontracts.JoinSpec) bool {
	if spec == nil {
		return false
	}
	if typedPathReferencesEntity(spec.Members.From, spec.Members.FromPath) ||
		typedPathReferencesEntity(spec.Members.By, spec.Members.ByPath) ||
		typedPathReferencesEntity(spec.Output, spec.OutputPath) ||
		expressionReferencesEntity(spec.CompleteWhen) {
		return true
	}
	if spec.Window != nil && (typedPathReferencesEntity(spec.Window.From, spec.Window.FromPath) || typedPathReferencesEntity(spec.Window.By, spec.Window.ByPath)) {
		return true
	}
	return completionRulesEntityRequirement(source, flowID, []runtimecontracts.HandlerRuleEntry{spec.OnComplete, spec.Timeout.Outcome}) != DeliveryTargetEntityOptional
}

func expressionValueMapReferencesEntity(values map[string]runtimecontracts.ExpressionValue) bool {
	for _, value := range values {
		if expressionValueReferencesEntity(value) {
			return true
		}
	}
	return false
}

func expressionValueReferencesEntity(value runtimecontracts.ExpressionValue) bool {
	return typedPathReferencesEntity(value.Ref, value.RefPath) || expressionReferencesEntity(value.CEL)
}

func typedPathReferencesEntity(raw string, path paths.Path) bool {
	if path.Root == paths.RootEntity {
		return true
	}
	return pathReferencesEntity(raw)
}

func expressionReferencesEntity(expression string) bool {
	expression = strings.TrimSpace(expression)
	if workflowexpr.ExpressionReferencesEntity(expression) {
		return true
	}
	for _, ref := range workflowexpr.PlatformEntityReferences(expression) {
		head, _, _ := strings.Cut(strings.TrimSpace(ref), ".")
		switch head {
		case "id", "current_state", "gates":
			return true
		}
	}
	return false
}

func pathReferencesEntity(path string) bool {
	path = strings.TrimSpace(path)
	return path == "entity" || strings.HasPrefix(path, "entity.")
}

func stampedDeliveryTargetOwnership(ctx context.Context) (events.DeliveryTargetOwnership, bool) {
	route, ok := runtimedelivery.RouteFromContext(ctx)
	if !ok || !route.Recipient.IsNode() || route.Target.Empty() {
		return events.DeliveryTargetOwnership{}, false
	}
	return route.Target, true
}
