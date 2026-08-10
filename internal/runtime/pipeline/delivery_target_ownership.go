package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
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
	flowID    string
	nodeID    string
	eventType events.EventType
	present   bool
}

func NewDeliveryTargetHandler(flowID, nodeID string) (DeliveryTargetHandler, error) {
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	if flowID == "" || nodeID == "" {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler requires exact flow and node owners")
	}
	return DeliveryTargetHandler{flowID: flowID, nodeID: nodeID, present: true}, nil
}

func MustDeliveryTargetHandler(flowID, nodeID string) DeliveryTargetHandler {
	owner, err := NewDeliveryTargetHandler(flowID, nodeID)
	if err != nil {
		panic(err)
	}
	return owner
}

func (h DeliveryTargetHandler) Empty() bool { return !h.present }

func (h DeliveryTargetHandler) Equal(other DeliveryTargetHandler) bool {
	return h.present == other.present && h.flowID == other.flowID && h.nodeID == other.nodeID && h.eventType == other.eventType
}

func (h DeliveryTargetHandler) FlowID() string {
	if !h.present {
		return ""
	}
	return h.flowID
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
	resolved := semanticview.ResolveFlowNodeSubscriptionHandler(source, h.flowID, h.nodeID, string(eventType))
	return resolved.Handler, resolved.Matched
}

// AdmitDeliveryTargetHandler admits one exact authored declaration owner. The
// concrete event is resolved later so wildcard subscriptions remain bounded by
// the same owner without freezing a pattern as an executable handler.
func AdmitDeliveryTargetHandler(source semanticview.Source, flowID, nodeID string) (DeliveryTargetHandler, error) {
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	if source == nil || flowID == "" || nodeID == "" {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler requires source, flow, and node owners")
	}
	owner, err := NewDeliveryTargetHandler(flowID, nodeID)
	if err != nil {
		return DeliveryTargetHandler{}, err
	}
	if _, _, ok := semanticview.ResolveFlowNodeDeclaration(source, flowID, nodeID); !ok {
		return DeliveryTargetHandler{}, fmt.Errorf("delivery target handler node %s has no declaration in flow %q", nodeID, flowID)
	}
	return owner, nil
}

type DeliveryTargetOwnershipRequest struct {
	Source               semanticview.Source
	Event                events.Event
	Recipient            events.DeliveryRecipient
	Blueprint            events.RouteIdentity
	Handler              DeliveryTargetHandler
	Candidates           []DeliveryTargetOwnerCandidate
	StructuralOwner      events.DeliveryTargetOwnership
	AllowStructuralOwner bool
}

// ClassifyDeliveryTargetOwnership is the single receiver-side owner for exact
// handler target classification. It returns one closed durable ownership
// variant and never repairs missing evidence from source-event identity.
func ClassifyDeliveryTargetOwnership(req DeliveryTargetOwnershipRequest) (events.DeliveryTargetOwnership, error) {
	if !req.Recipient.IsNode() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("delivery target ownership classification requires a node recipient")
	}
	blueprint := req.Blueprint.Normalized()
	if blueprint.FlowInstance == "" {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint requires an exact flow instance")
	}
	handler, admitted := req.Handler.resolve(req.Source, req.Event.Type())
	if !admitted {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver %s requires an exact admitted target handler for event %s", req.Recipient.ID(), req.Event.Type())
	}
	flowID := req.Handler.FlowID()
	existing, materializing, err := matchingDeliveryTargetOwnerCandidates(blueprint, req.Candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if len(existing) == 0 && len(materializing) == 0 && req.AllowStructuralOwner && !req.StructuralOwner.Empty() {
		structural := req.StructuralOwner.Route()
		if structural.EntityID != "" {
			ownerRoute := blueprint
			ownerRoute.EntityID = structural.EntityID
			if req.StructuralOwner.MaterializingEntity() {
				materializing = append(materializing, ownerRoute)
			} else if req.StructuralOwner.ExistingEntity() {
				existing = append(existing, ownerRoute)
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
		return events.NewMaterializingEntityTarget(planned)
	}
	if len(existing) == 1 && len(materializing) == 0 {
		return events.NewExistingEntityTarget(existing[0])
	}
	if blueprint.EntityID != "" && len(req.Candidates) == 0 {
		return events.NewExistingEntityTarget(blueprint)
	}
	if !handlerUsesEntity(req.Source, flowID, handler) {
		blueprint.EntityID = ""
		return events.NewEntitylessReceiverTarget(blueprint)
	}
	if handlerCanMaterializeEntity(req.Source, flowID, handler) {
		planned, err := canonicalHandlerMaterializationTarget(req.Source, flowID, handler, req.Event, blueprint)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		return events.NewMaterializingEntityTarget(planned)
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", blueprint.FlowInstance)
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
	flowID := handlerFact.FlowID()
	if routeFlowID := owner.Route().FlowID; routeFlowID != "" && routeFlowID != flowID {
		return fmt.Errorf("stamped delivery target flow %q disagrees with handler flow %q", routeFlowID, flowID)
	}
	usesEntity := handlerUsesEntity(source, flowID, handler)
	switch {
	case owner.EntitylessReceiver():
		if usesEntity {
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
	default:
		return fmt.Errorf("delivery target ownership kind is unsupported")
	}
	return nil
}

func matchingDeliveryTargetOwnerCandidates(blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate) ([]events.RouteIdentity, []events.RouteIdentity, error) {
	existingSet := map[events.RouteIdentity]struct{}{}
	materializingSet := map[events.RouteIdentity]struct{}{}
	for _, candidate := range candidates {
		route := candidate.Route.Normalized()
		if route.FlowInstance != blueprint.FlowInstance || route.EntityID == "" {
			continue
		}
		if blueprint.EntityID != "" && route.EntityID != blueprint.EntityID {
			continue
		}
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

func handlerCanMaterializeEntity(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
	if handler.CreateEntity || (handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()) {
		return true
	}
	return flowIsStateless(source, flowID) && handlerMaterializesEntity(source, flowID, handler)
}

func flowIsStateless(source semanticview.Source, flowID string) bool {
	return source != nil && strings.TrimSpace(source.FlowInitialStage(flowID)) == "" && len(source.FlowStates(flowID)) == 0
}

func handlerUsesEntity(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
	if handler.CreateEntity ||
		handler.SelectEntity != nil && !handler.SelectEntity.Empty() ||
		handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() ||
		handlerMaterializesEntity(source, flowID, handler) {
		return true
	}
	if expressionReferencesEntity(handler.Condition) || expressionReferencesEntity(handler.Logic) || guardReferencesEntity(handler.Guard) {
		return true
	}
	for _, rule := range append(append([]runtimecontracts.HandlerRuleEntry(nil), handler.Rules...), handler.OnComplete...) {
		if expressionReferencesEntity(rule.Condition) {
			return true
		}
	}
	if handler.Filter != nil && (pathReferencesEntity(handler.Filter.Source) || pathReferencesEntity(handler.Filter.ItemsFrom) || expressionReferencesEntity(handler.Filter.Condition)) {
		return true
	}
	if handler.Count != nil && (pathReferencesEntity(handler.Count.Source) || pathReferencesEntity(handler.Count.ItemsFrom) || expressionReferencesEntity(handler.Count.Condition)) {
		return true
	}
	if handler.Reduce != nil && (pathReferencesEntity(handler.Reduce.Source) || pathReferencesEntity(handler.Reduce.ItemsFrom)) {
		return true
	}
	if handler.Query != nil && queryReferencesEntity(handler.Query) {
		return true
	}
	if handler.Clear != nil {
		for _, target := range handler.Clear.Targets {
			if pathReferencesEntity(target) {
				return true
			}
		}
	}
	return false
}

func guardReferencesEntity(guard *runtimecontracts.GuardSpec) bool {
	if guard == nil {
		return false
	}
	if expressionReferencesEntity(guard.Check) {
		return true
	}
	for _, check := range guard.Checks {
		if expressionReferencesEntity(check.Check) {
			return true
		}
	}
	return false
}

func queryReferencesEntity(query *runtimecontracts.QuerySpec) bool {
	if query == nil {
		return false
	}
	if pathReferencesEntity(query.Source) || pathReferencesEntity(query.Entities) || expressionReferencesEntity(query.Filter) || pathReferencesEntity(query.GroupBy) {
		return true
	}
	for index := range query.Queries {
		if queryReferencesEntity(&query.Queries[index]) {
			return true
		}
	}
	return false
}

func expressionReferencesEntity(expression string) bool {
	return workflowexpr.ExpressionReferencesEntity(strings.TrimSpace(expression))
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

func prepareStampedSelectOrCreateState(source semanticview.Source, flowID string, handler SystemNodeEventHandler, evt events.Event, owner events.DeliveryTargetOwnership, state *WorkflowState) error {
	if state == nil || !owner.MaterializingEntity() || handler.SelectOrCreateEntity == nil || handler.SelectOrCreateEntity.Empty() {
		return nil
	}
	expected, err := selectOrCreateEntityExpectedValues(handler.SelectOrCreateEntity, evt)
	if err != nil {
		return err
	}
	route := owner.Route()
	instanceID := runtimeflowidentity.LogicalInstanceID(route.FlowInstance)
	instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
	if instance.InstancePath != route.FlowInstance || instance.EntityID != route.EntityID {
		return fmt.Errorf("stamped select_or_create_entity target disagrees with canonical instance")
	}
	state.EntityID = route.EntityID
	state.Stage = NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID))
	state.Metadata = workflowCreateEntityMetadata(source, flowID, instance)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	for field, value := range expected {
		values.Wrap(state.Metadata).SetPath(paths.Parse(field), value)
	}
	return nil
}
