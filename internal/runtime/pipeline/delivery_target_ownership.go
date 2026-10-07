package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// DeliveryTargetOwnerCandidate is exact selected-run evidence available before
// durable delivery admission. Materializing candidates come from an admitted
// same-plan activation, never from an existing entity row.
type DeliveryTargetOwnerCandidate struct {
	Route         events.RouteIdentity
	Materializing bool
	Availability  DeliveryTargetAvailability
}

// DeliveryTargetAvailability is immutable state/companion evidence, not a
// receiver-selection policy. Missing companion rows are admitted by the reader
// as active state-only ownership; an existing inactive companion is not absent.
type DeliveryTargetAvailability struct {
	stage    string
	inactive bool
}

// TerminalReceiverError is a proven receiver-state refusal, not an inactive
// lifecycle or storage failure. Wrapping must preserve this typed cause.
type TerminalReceiverError struct {
	FlowID string
	Stage  string
}

func (e *TerminalReceiverError) Error() string {
	return fmt.Sprintf("receiver target owner is unavailable: terminal state %q in flow %q", e.Stage, e.FlowID)
}

func NewDeliveryTargetAvailability(stage, status string, terminated bool) DeliveryTargetAvailability {
	return DeliveryTargetAvailability{stage: strings.TrimSpace(stage), inactive: terminated || !strings.EqualFold(strings.TrimSpace(status), "active")}
}

func (a DeliveryTargetAvailability) Validate(source semanticview.Source, flowID string) error {
	if a.inactive {
		return fmt.Errorf("receiver target owner is unavailable: lifecycle is not active")
	}
	if a.stage != "" {
		graph, ok := semanticview.WorkflowStageTopology(source, flowID)
		if !ok || graph.FlowID != flowID {
			return fmt.Errorf("receiver flow %q has no selected compiled stage topology", flowID)
		}
		stage, err := graph.ResolveStoredStage(a.stage)
		if err != nil {
			return fmt.Errorf("receiver stage admission: %w", err)
		}
		if stage.IsFinal() {
			return &TerminalReceiverError{FlowID: flowID, Stage: stage.ID()}
		}
	}
	return nil
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
	return h.node.FlowPath()
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

// EventOverride returns the admitted handler-local event without localizing or
// inferring a replacement from a publication envelope.
func (h DeliveryTargetHandler) EventOverride() (events.EventType, bool) {
	return h.eventType, h.present && h.eventType != ""
}

// ExecutionFlowID derives the runtime flow scope without changing the
// declaration coordinate. Root project nodes have an explicitly empty owning
// flow in ExecutableNode and execute in the bundle's root flow.
func (h DeliveryTargetHandler) ExecutionFlowID(source semanticview.Source) string {
	if !h.present {
		return ""
	}
	if flowID := h.node.FlowPath(); flowID != "" {
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
	Source     semanticview.Source
	Event      events.Event
	Recipient  events.DeliveryRecipient
	Blueprint  events.RouteIdentity
	Handler    DeliveryTargetHandler
	Candidates []DeliveryTargetOwnerCandidate
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
	if err := ValidateExecutionHandlerDeclaration(req.Source, req.Handler.Node(), handler); err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	var err error
	if strings.TrimSpace(flowID) == strings.TrimSpace(semanticview.RootExecutionFlowID(req.Source)) {
		blueprint, err = selectedRunRootTargetBlueprint(req.Source, req.Event, blueprint, req.Event.HasTargetRoute())
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
	} else {
		blueprint.FlowID = flowID
		blueprint = blueprint.Normalized()
	}
	if blueprint.FlowInstance == "" {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint requires an exact flow instance")
	}
	existing, materializing, err := matchingDeliveryTargetOwnerCandidates(req.Source, blueprint, req.Candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if len(existing)+len(materializing) > 1 {
		return events.DeliveryTargetOwnership{}, ambiguousDeliveryTargetOwnerError(blueprint.FlowInstance, existing, materializing)
	}
	if len(materializing) == 1 && len(existing) == 0 {
		if materializing[0].EntityID != FlowInstanceEntityID(materializing[0].FlowInstance) {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("constructor target evidence disagrees with canonical instance identity")
		}
		return events.NewMaterializingEntityTarget(materializing[0])
	}
	if len(existing) == 1 && len(materializing) == 0 {
		return events.NewExistingEntityTarget(existing[0])
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q; construct it before handler delivery", blueprint.FlowInstance)
}

func selectedRunRootTargetBlueprint(source semanticview.Source, evt events.Event, blueprint events.RouteIdentity, exactTarget bool) (events.RouteIdentity, error) {
	coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID())
	if err != nil {
		return events.RouteIdentity{}, err
	}
	blueprint = blueprint.Normalized()
	if exactTarget && blueprint.FlowID != "" && blueprint.FlowID != coordinate.FlowID() {
		return events.RouteIdentity{}, fmt.Errorf("root target flow %q disagrees with authored root flow %q", blueprint.FlowID, coordinate.FlowID())
	}
	if exactTarget && blueprint.FlowInstance != "" && blueprint.FlowInstance != coordinate.RunID() {
		return events.RouteIdentity{}, fmt.Errorf("root target run %q disagrees with current run %q", blueprint.FlowInstance, coordinate.RunID())
	}
	blueprint.FlowID = coordinate.FlowID()
	blueprint.FlowInstance = coordinate.RunID()
	return blueprint.Normalized(), nil
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
	if isJoinLifecycleEvent(evt.Type()) {
		declaredRecipient, target, declaredHandler, found, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, evt)
		if err != nil {
			return err
		}
		if !found || recipient != declaredRecipient || !handlerFact.Node().Equal(declaredHandler.Node()) || handlerFact.eventType != declaredHandler.eventType || owner.Route() != target {
			return fmt.Errorf("stamped join lifecycle ownership contradicts its exact declaration occurrence")
		}
	}
	flowID := handlerFact.ExecutionFlowID(source)
	route := owner.Route()
	if flowID == strings.TrimSpace(semanticview.RootExecutionFlowID(source)) {
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID())
		if err != nil {
			return err
		}
		if !coordinate.Matches(route.FlowID, route.FlowInstance) {
			return fmt.Errorf("stamped root target (%q, %q) disagrees with current root coordinate (%q, %q)", route.FlowID, route.FlowInstance, coordinate.FlowID(), coordinate.RunID())
		}
	} else if routeFlowID := route.FlowID; routeFlowID != "" && routeFlowID != flowID {
		return fmt.Errorf("stamped delivery target flow %q disagrees with handler flow %q", routeFlowID, flowID)
	}
	if err := ValidateExecutionHandlerDeclaration(source, handlerFact.Node(), handler); err != nil {
		return err
	}
	if owner.EntitylessReceiver() {
		return fmt.Errorf("entityless_receiver cannot authorize handler execution; every flow requires a constructed lifecycle header")
	}
	if !owner.ExistingEntity() && !owner.MaterializingEntity() {
		return fmt.Errorf("delivery target ownership kind is unsupported")
	}
	if owner.MaterializingEntity() && route.EntityID != FlowInstanceEntityID(route.FlowInstance) {
		return fmt.Errorf("materializing_entity ownership disagrees with canonical instance identity")
	}
	return nil
}

// ValidateExecutionHandlerDeclaration does not infer construction from effects
// or field references. Every handler executes against an already constructed flow.
func ValidateExecutionHandlerDeclaration(source semanticview.Source, node runtimeidentity.ExecutableNode, handler SystemNodeEventHandler) error {
	if source == nil || !node.Valid() {
		return fmt.Errorf("handler execution requires admitted source and exact executable node identity")
	}
	if _, found := source.ExecutableNode(node); !found {
		return fmt.Errorf("handler execution requires its exact admitted declaration")
	}
	if handler.CreateEntity {
		return fmt.Errorf("handler create_entity is retired; use the canonical flow constructor")
	}
	return nil
}

func deliveryTargetWorkflowInstanceUnavailable(source semanticview.Source, flowID string, instance WorkflowInstance) bool {
	return NewDeliveryTargetAvailability(instance.CurrentState, instance.Status, !instance.TerminatedAt.IsZero()).Validate(source, flowID) != nil
}

func matchingDeliveryTargetOwnerCandidates(source semanticview.Source, blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate) ([]events.RouteIdentity, []events.RouteIdentity, error) {
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
		if !candidate.Materializing {
			if err := candidate.Availability.Validate(source, blueprint.FlowID); err != nil {
				return nil, nil, err
			}
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

func stampedDeliveryTargetOwnership(ctx context.Context) (events.DeliveryTargetOwnership, bool) {
	route, ok := runtimedelivery.RouteFromContext(ctx)
	if !ok || !route.Recipient.IsNode() || route.Target.Empty() {
		return events.DeliveryTargetOwnership{}, false
	}
	return route.Target, true
}
