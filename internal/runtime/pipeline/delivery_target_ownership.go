package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// DeliveryTargetOwnerCandidate is exact selected-run evidence available before
// durable delivery admission. Materializing candidates come from an admitted
// same-plan activation, never from an existing entity row.
type DeliveryTargetOwnerCandidate struct {
	Route         events.RouteIdentity
	Materializing bool
	Availability  DeliveryTargetAvailability
	Life          int64
}

// DeliveryTargetAvailability is immutable state/companion evidence, not a
// receiver-selection policy. Missing companion rows are admitted by the reader
// as active state-only ownership; an existing inactive companion is not absent.
type DeliveryTargetAvailability struct {
	stage      string
	inactive   bool
	terminated bool
}

// NoHandlerError describes ordinary receiver eligibility, not resource lifetime.
type NoHandlerError struct {
	FlowID        string
	FlowInstance  string
	Stage         string
	HandledEvents []string
}

func (e *NoHandlerError) Error() string {
	return e.Unwrap().Error()
}

func (e *NoHandlerError) Unwrap() error {
	return failures.New(failures.ClassUnexpectedArrival, "no_route", "pipeline", "receiver_admission", map[string]any{
		"flow_id": e.FlowID, "flow_instance": e.FlowInstance, "stage": e.Stage, "handled_events": append([]string{}, e.HandledEvents...),
	})
}

// TerminatedReceiverError proves occupied, non-executable construction.
// Unknown lifecycle spellings remain independent admission errors.
type TerminatedReceiverError struct {
	FlowID string
}

func (e *TerminatedReceiverError) Error() string {
	return fmt.Sprintf("receiver target owner is unavailable: lifecycle is terminated in flow %q", e.FlowID)
}

func NewDeliveryTargetAvailability(stage, status string, terminated bool) DeliveryTargetAvailability {
	return DeliveryTargetAvailability{
		stage: strings.TrimSpace(stage), inactive: terminated || !strings.EqualFold(strings.TrimSpace(status), "active"),
		terminated: status == "terminated" || status == "active" && terminated,
	}
}

func (a DeliveryTargetAvailability) Validate(source semanticview.Source, flowID string) error {
	final, err := a.validateOwner(source, flowID)
	if err != nil {
		return err
	}
	if final {
		return &NoHandlerError{FlowID: flowID, Stage: a.stage, HandledEvents: []string{}}
	}
	return nil
}

func (a DeliveryTargetAvailability) validateOwner(source semanticview.Source, flowID string) (bool, error) {
	if a.inactive {
		if a.terminated {
			return false, &TerminatedReceiverError{FlowID: flowID}
		}
		return false, fmt.Errorf("receiver target owner is unavailable: lifecycle is not active")
	}
	if a.stage != "" {
		graph, ok := semanticview.WorkflowStageTopology(source, flowID)
		if !ok || graph.FlowID != flowID {
			return false, fmt.Errorf("receiver flow %q has no selected compiled stage topology", flowID)
		}
		stage, err := graph.ResolveStoredStage(a.stage)
		if err != nil {
			return false, fmt.Errorf("receiver stage admission: %w", err)
		}
		return stage.IsFinal(), nil
	}
	return false, nil
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
	Source    semanticview.Source
	Event     events.Event
	Recipient events.DeliveryRecipient
	Blueprint events.RouteIdentity
	Handler   DeliveryTargetHandler
}

// DeliveryTargetLookup retains the admitted request while its exact instance
// evidence is read. It is neither a durable route nor execution authority.
type DeliveryTargetLookup struct {
	request    DeliveryTargetOwnershipRequest
	coordinate events.RouteIdentity
	present    bool
}

func (l DeliveryTargetLookup) Coordinate() events.RouteIdentity { return l.coordinate }

// ReplyOriginTargetAdmission is fixed by the Bus constructor. Its zero value
// requires native ownership; only the explicit ephemeral constructor opts out.
type ReplyOriginTargetAdmission struct {
	ephemeral bool
}

func NewEphemeralReplyOriginTargetAdmission() ReplyOriginTargetAdmission {
	return ReplyOriginTargetAdmission{ephemeral: true}
}

type ReplyOriginTargetLookup struct {
	origin    events.RouteIdentity
	ephemeral bool
	present   bool
}

func (l ReplyOriginTargetLookup) Coordinate() events.RouteIdentity { return l.origin }

func (a ReplyOriginTargetAdmission) Admit(origin events.RouteIdentity, prospective PreparedWorkflowPublicationState) (ReplyOriginTargetLookup, error) {
	origin = origin.Normalized()
	if err := prospective.ValidateTarget(origin); err != nil {
		return ReplyOriginTargetLookup{}, err
	}
	if origin.Empty() && !a.ephemeral {
		return ReplyOriginTargetLookup{}, fmt.Errorf("receiver target blueprint is required")
	}
	return ReplyOriginTargetLookup{origin: origin, ephemeral: a.ephemeral, present: true}, nil
}

func ClassifyReplyOriginTargetOwnership(source semanticview.Source, lookup ReplyOriginTargetLookup, candidates []DeliveryTargetOwnerCandidate) (events.DeliveryTargetOwnership, error) {
	if !lookup.present {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("reply origin classification requires its admitted lookup")
	}
	if lookup.origin.Empty() {
		return events.DeliveryTargetOwnership{}, nil
	}
	// An explicit reply entity narrows the inventory. Node classification below
	// deliberately retains its contradictory-entity refusal instead.
	matching := make([]DeliveryTargetOwnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if lookup.origin.FlowInstance != "" && candidate.Route.FlowInstance != lookup.origin.FlowInstance || lookup.origin.EntityID != "" && candidate.Route.EntityID != lookup.origin.EntityID {
			continue
		}
		matching = append(matching, candidate)
	}
	owner, found, err := selectDeliveryTargetOwner(source, lookup.origin, matching, lookup)
	if err != nil || found {
		return owner, err
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", lookup.origin.FlowInstance)
}

// AdmitDeliveryTargetLookup resolves the receiving declaration once, before any
// instance lookup. Classification consumes this fact, not a caller-built route.
func AdmitDeliveryTargetLookup(req DeliveryTargetOwnershipRequest) (DeliveryTargetLookup, error) {
	lookup, err := admitDeliveryTargetCoordinate(req)
	if err != nil {
		return DeliveryTargetLookup{}, err
	}
	req = lookup.request
	handler, admitted := req.Handler.resolve(req.Source, req.Event.Type())
	if !admitted {
		return DeliveryTargetLookup{}, fmt.Errorf("receiver %s requires an exact admitted target handler for event %s", req.Recipient.ID(), req.Event.Type())
	}
	if err := ValidateExecutionHandlerDeclaration(req.Source, req.Handler.Node(), handler); err != nil {
		return DeliveryTargetLookup{}, err
	}
	return lookup, nil
}

// Coordinate admission is shared with receipt; only planning resolves a handler.
func admitDeliveryTargetCoordinate(req DeliveryTargetOwnershipRequest) (DeliveryTargetLookup, error) {
	if !req.Recipient.IsNode() {
		return DeliveryTargetLookup{}, fmt.Errorf("node target lookup requires its admitted handler")
	}
	if isJoinLifecycleEvent(req.Event.Type()) {
		recipient, target, handler, ok, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(req.Source, req.Event)
		if err != nil {
			return DeliveryTargetLookup{}, err
		}
		if !ok {
			return DeliveryTargetLookup{}, fmt.Errorf("join lifecycle delivery requires its exact declaration handle")
		}
		if req.Recipient != recipient || (!req.Handler.Empty() && !req.Handler.Node().Equal(handler.Node())) {
			return DeliveryTargetLookup{}, fmt.Errorf("join lifecycle delivery route contradicts its exact declaration handler")
		}
		if blueprint := req.Blueprint.Normalized(); !blueprint.Empty() && blueprint != target {
			return DeliveryTargetLookup{}, fmt.Errorf("join lifecycle delivery route contradicts its exact declaration target")
		}
		req.Handler = handler
		req.Blueprint = target
	}
	if req.Handler.Empty() {
		return DeliveryTargetLookup{}, fmt.Errorf("receiver %s requires an exact admitted target handler for event %s", req.Recipient.ID(), req.Event.Type())
	}
	node, _ := req.Recipient.Node()
	if !node.Equal(req.Handler.Node()) {
		return DeliveryTargetLookup{}, fmt.Errorf("node target lookup disagrees with its handler recipient")
	}
	flowID := req.Handler.ExecutionFlowID(req.Source)
	blueprint := req.Blueprint.Normalized()
	if blueprint.FlowID != "" && blueprint.FlowID != flowID {
		return DeliveryTargetLookup{}, fmt.Errorf("target lookup declaration %q disagrees with handler declaration %q", blueprint.FlowID, flowID)
	}
	if flowID == semanticview.RootExecutionFlowID(req.Source) {
		var err error
		blueprint, err = selectedRunRootTargetBlueprint(req.Source, req.Event, blueprint)
		if err != nil {
			return DeliveryTargetLookup{}, err
		}
	} else {
		if blueprint.FlowInstance == "" {
			return DeliveryTargetLookup{}, fmt.Errorf("non-root target lookup requires an exact instance coordinate")
		}
		blueprint.FlowID = flowID
	}
	return DeliveryTargetLookup{request: req, coordinate: blueprint, present: true}, nil
}

// ClassifyDeliveryTargetOwnership consumes fresh candidates for the already
// admitted request without resolving its handler or receiving coordinate again.
func ClassifyDeliveryTargetOwnership(lookup DeliveryTargetLookup, candidates []DeliveryTargetOwnerCandidate) (events.DeliveryTargetOwnership, error) {
	if !lookup.present {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("delivery target classification requires its admitted lookup")
	}
	blueprint := lookup.coordinate
	owner, found, err := SelectDeliveryTargetOwner(lookup.request.Source, blueprint, candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if !found {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q; construct it before handler delivery", blueprint.FlowInstance)
	}
	if owner.MaterializingEntity() && owner.Route().EntityID != FlowInstanceEntityID(owner.Route().FlowInstance) {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("constructor target evidence disagrees with canonical instance identity")
	}
	return owner, nil
}

// SelectDeliveryTargetOwner decides only the supplied candidate evidence, not
// handler or reply admission. Only found=false with no error means absence.
func SelectDeliveryTargetOwner(source semanticview.Source, blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate) (events.DeliveryTargetOwnership, bool, error) {
	return selectDeliveryTargetOwner(source, blueprint, candidates, ReplyOriginTargetLookup{})
}

func selectDeliveryTargetOwner(source semanticview.Source, blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate, reply ReplyOriginTargetLookup) (events.DeliveryTargetOwnership, bool, error) {
	blueprint = blueprint.Normalized()
	existing, materializing, err := matchingDeliveryTargetOwnerCandidates(source, blueprint, candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, false, err
	}
	if len(existing)+len(materializing) > 1 {
		return events.DeliveryTargetOwnership{}, false, ambiguousDeliveryTargetOwnerError(blueprint.FlowInstance, existing, materializing)
	}
	if len(materializing) == 1 && len(existing) == 0 {
		owner, err := events.NewMaterializingEntityTarget(materializing[0])
		if err != nil {
			return events.DeliveryTargetOwnership{}, false, err
		}
		return captureDeliveryTargetLife(owner, candidates)
	}
	var selected events.RouteIdentity
	native := len(existing) == 1
	switch {
	case native:
		selected = existing[0]
	case reply.present && reply.ephemeral:
		selected = reply.origin
		if selected.EntityID == "" {
			owner, err := events.NewEntitylessReceiverTarget(selected)
			return owner, err == nil, err
		}
	default:
		return events.DeliveryTargetOwnership{}, false, nil
	}
	owner, err := events.NewExistingEntityTarget(selected)
	if err != nil {
		return events.DeliveryTargetOwnership{}, false, err
	}
	if native {
		return captureDeliveryTargetLife(owner, candidates)
	}
	return owner, true, nil
}

func captureDeliveryTargetLife(owner events.DeliveryTargetOwnership, candidates []DeliveryTargetOwnerCandidate) (events.DeliveryTargetOwnership, bool, error) {
	var life int64
	for _, candidate := range candidates {
		if candidate.Route.Normalized().FlowInstance != owner.Route().FlowInstance {
			continue
		}
		if candidate.Life <= 0 || life != 0 && life != candidate.Life {
			return events.DeliveryTargetOwnership{}, false, fmt.Errorf("receiver target has missing or contradictory life evidence")
		}
		life = candidate.Life
	}
	owner, err := owner.WithLife(life)
	return owner, err == nil, err
}

func selectedRunRootTargetBlueprint(source semanticview.Source, evt events.Event, blueprint events.RouteIdentity) (events.RouteIdentity, error) {
	coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID())
	if err != nil {
		return events.RouteIdentity{}, err
	}
	blueprint = blueprint.Normalized()
	if blueprint.FlowID != "" && blueprint.FlowID != coordinate.FlowID() {
		return events.RouteIdentity{}, fmt.Errorf("root target flow %q disagrees with authored root flow %q", blueprint.FlowID, coordinate.FlowID())
	}
	if blueprint.FlowInstance != "" && blueprint.FlowInstance != coordinate.RunID() {
		return events.RouteIdentity{}, fmt.Errorf("root target run %q disagrees with current run %q", blueprint.FlowInstance, coordinate.RunID())
	}
	blueprint.FlowID = coordinate.FlowID()
	blueprint.FlowInstance = coordinate.RunID()
	return blueprint.Normalized(), nil
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
				if refusal, ok := err.(*NoHandlerError); ok {
					refusal.FlowInstance = route.FlowInstance
				}
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
