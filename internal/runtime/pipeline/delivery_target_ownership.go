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
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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

func (h DeliveryTargetHandler) NodeID() string {
	if !h.present {
		return ""
	}
	return h.nodeID
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
	StructuralOwnerProof runtimepinrouting.StructuralTargetOwnerProof
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
		if req.Recipient != recipient || (!req.Handler.Empty() &&
			(req.Handler.FlowID() != handler.FlowID() || req.Handler.NodeID() != handler.NodeID())) {
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
	flowID := req.Handler.FlowID()
	blueprint.FlowID = flowID
	blueprint = selectedRunRootTargetBlueprint(req.Source, req.Event, flowID, blueprint)
	if blueprint.FlowInstance == "" {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint requires an exact flow instance")
	}
	requirement, err := deliveryTargetEntityRequirement(req.Source, req.Handler, req.Event.Type(), handler)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	existing, materializing, err := matchingDeliveryTargetOwnerCandidates(blueprint, req.Candidates)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if requirement == handlerEntitylessSafe && len(existing)+len(materializing) > 0 {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("entityless-safe handler has selected entity ownership evidence for flow instance %q", blueprint.FlowInstance)
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
		return events.NewExistingEntityTarget(existing[0])
	}
	if requirement == handlerEntitylessSafe {
		blueprint.EntityID = ""
		return events.NewEntitylessReceiverTarget(blueprint)
	}
	if requirement == handlerMaterializingEntity {
		planned, err := canonicalHandlerMaterializationTarget(req.Source, flowID, handler, req.Event, blueprint)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		return events.NewMaterializingEntityTarget(planned)
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
	flowID := handlerFact.FlowID()
	if routeFlowID := owner.Route().FlowID; routeFlowID != "" && routeFlowID != flowID {
		return fmt.Errorf("stamped delivery target flow %q disagrees with handler flow %q", routeFlowID, flowID)
	}
	requirement, err := deliveryTargetEntityRequirement(source, handlerFact, evt.Type(), handler)
	if err != nil {
		return err
	}
	switch {
	case owner.EntitylessReceiver():
		if requirement != handlerEntitylessSafe {
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

func deliveryTargetHandlerAcquisition(source semanticview.Source, handler DeliveryTargetHandler, eventType events.EventType) (usesEntity, materializes bool, err error) {
	if source == nil || handler.Empty() {
		return false, false, nil
	}
	if handler.eventType != "" {
		eventType = handler.eventType
	}
	association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(handler.FlowID(), string(eventType))
	if association.Status == semanticview.EndpointAssociationAmbiguous {
		return false, false, association.Err()
	}
	endpoint, ok := association.Endpoint()
	if !ok || endpoint.ResolutionMode == runtimecontracts.FlowInputResolutionModeNone {
		return false, false, nil
	}
	switch endpoint.ResolutionMode {
	case runtimecontracts.FlowInputResolutionModeCreate, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return true, true, nil
	default:
		return true, false, nil
	}
}

func deliveryTargetEntityRequirement(source semanticview.Source, handlerFact DeliveryTargetHandler, eventType events.EventType, handler SystemNodeEventHandler) (handlerEntityRequirement, error) {
	requirement := handlerExecutionEntityRequirement(source, handlerFact.FlowID(), handler)
	usesEntity, materializes, err := deliveryTargetHandlerAcquisition(source, handlerFact, eventType)
	if err != nil {
		return handlerEntitylessSafe, err
	}
	if materializes {
		return requirement.merge(handlerMaterializingEntity), nil
	}
	if usesEntity {
		return requirement.merge(handlerExistingEntityRequired), nil
	}
	return requirement, nil
}

func matchingDeliveryTargetOwnerCandidates(blueprint events.RouteIdentity, candidates []DeliveryTargetOwnerCandidate) ([]events.RouteIdentity, []events.RouteIdentity, error) {
	exact := make([]DeliveryTargetOwnerCandidate, 0, len(candidates))
	sameFlow := make([]DeliveryTargetOwnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		route := candidate.Route.Normalized()
		if route.FlowInstance == blueprint.FlowInstance {
			exact = append(exact, candidate)
			continue
		}
		if route.FlowID != "" && route.FlowID == blueprint.FlowID {
			sameFlow = append(sameFlow, candidate)
		}
	}
	if len(exact) > 0 {
		candidates = exact
	} else {
		candidates = sameFlow
	}
	existingSet := map[events.RouteIdentity]struct{}{}
	materializingSet := map[events.RouteIdentity]struct{}{}
	for _, candidate := range candidates {
		route := candidate.Route.Normalized()
		if route.EntityID == "" {
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

type handlerEntityRequirement uint8

const (
	handlerEntitylessSafe handlerEntityRequirement = iota
	handlerExistingEntityRequired
	handlerMaterializingEntity
)

func (r handlerEntityRequirement) merge(other handlerEntityRequirement) handlerEntityRequirement {
	if other > r {
		return other
	}
	return r
}

func (r handlerEntityRequirement) materializes() bool { return r == handlerMaterializingEntity }

type handlerEntityFieldClassifier func(semanticview.Source, string, SystemNodeEventHandler) handlerEntityRequirement

// Every executable handler field has one explicit execution-semantic
// disposition. The result owns both routing admission and engine preparation.
var systemNodeEventHandlerEntityClassifiers = map[string]handlerEntityFieldClassifier{
	"Action": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return actionEntityRequirement(handler.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return activityEntityRequirement(handler.Activity)
	},
	"CreateEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(handler.CreateEntity)
	},
	"SelectEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(handler.SelectEntity != nil && !handler.SelectEntity.Empty())
	},
	"SelectOrCreateEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty())
	},
	"Description":    noHandlerEntityRequirement,
	"EvidenceTarget": noHandlerEntityRequirement,
	"Emit": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(emitSpecReferencesEntity(handler.Emit))
	},
	"OnSuccess": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(emitSpecReferencesEntity(handler.OnSuccess.Emit))
	},
	"Guard": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return guardEntityRequirement(handler.Guard)
	},
	"AdvancesTo": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(strings.TrimSpace(handler.AdvancesTo) != "")
	},
	"SetsGate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(gateSpecName(handler.SetsGate) != "")
	},
	"ClearGates": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return materializingWhen(len(handler.ClearGates) != 0)
	},
	"DataAccumulation": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) handlerEntityRequirement {
		if workflowDataWritesEntityFields(handler.DataAccumulation, workflowEntitySchemaFields(source, flowID)) {
			return handlerMaterializingEntity
		}
		return existingWhen(dataAccumulationReferencesEntity(handler.DataAccumulation))
	},
	"Condition": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(expressionReferencesEntity(handler.Condition))
	},
	"Logic": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(expressionReferencesEntity(handler.Logic))
	},
	"Loop": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(handler.Loop != nil)
	},
	"OnComplete": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return rulesEntityRequirement(source, flowID, handler.OnComplete)
	},
	"Rules": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return rulesEntityRequirement(source, flowID, handler.Rules)
	},
	"Accumulate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(handler.Accumulate != nil)
	},
	"Join": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(handler.Join != nil)
	},
	"Compute": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) handlerEntityRequirement {
		if computeStoresEntityField(handler.Compute, workflowEntitySchemaFields(source, flowID)) {
			return handlerMaterializingEntity
		}
		return existingWhen(computeReferencesEntity(handler.Compute))
	},
	"Query": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(queryReferencesEntity(handler.Query))
	},
	"FanOut": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return fanOutEntityRequirement(handler.FanOut)
	},
	"GroupBy": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(groupByReferencesEntity(handler.GroupBy))
	},
	"Filter": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(filterReferencesEntity(handler.Filter))
	},
	"Reduce": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(reduceReferencesEntity(handler.Reduce))
	},
	"Count": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(countReferencesEntity(handler.Count))
	},
	"Clear": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) handlerEntityRequirement {
		return existingWhen(handler.Clear != nil && len(handler.Clear.Targets) != 0)
	},
}

type handlerRuleEntityFieldClassifier func(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) handlerEntityRequirement

var handlerRuleEntryEntityClassifiers = map[string]handlerRuleEntityFieldClassifier{
	"ID":          noHandlerRuleEntityRequirement,
	"Description": noHandlerRuleEntityRequirement,
	"Condition": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return existingWhen(expressionReferencesEntity(rule.Condition))
	},
	"PolicyRow": noHandlerRuleEntityRequirement,
	"AdvancesTo": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return materializingWhen(strings.TrimSpace(rule.AdvancesTo) != "")
	},
	"Emit": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return materializingWhen(emitSpecReferencesEntity(rule.Emit))
	},
	"Action": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return actionEntityRequirement(rule.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return activityEntityRequirement(rule.Activity)
	},
	"DataAccumulation": func(source semanticview.Source, flowID string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		if workflowDataWritesEntityFields(rule.DataAccumulation, workflowEntitySchemaFields(source, flowID)) {
			return handlerMaterializingEntity
		}
		return existingWhen(dataAccumulationReferencesEntity(rule.DataAccumulation))
	},
	"Compute": func(source semanticview.Source, flowID string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		if computeStoresEntityField(rule.Compute, workflowEntitySchemaFields(source, flowID)) {
			return handlerMaterializingEntity
		}
		return existingWhen(computeReferencesEntity(rule.Compute))
	},
	"FanOut": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
		return fanOutEntityRequirement(rule.FanOut)
	},
}

func handlerExecutionEntityRequirement(source semanticview.Source, flowID string, handler SystemNodeEventHandler) handlerEntityRequirement {
	requirement := handlerEntitylessSafe
	for _, classify := range systemNodeEventHandlerEntityClassifiers {
		requirement = requirement.merge(classify(source, flowID, handler))
	}
	return requirement
}

func noHandlerEntityRequirement(semanticview.Source, string, SystemNodeEventHandler) handlerEntityRequirement {
	return handlerEntitylessSafe
}

func noHandlerRuleEntityRequirement(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
	return handlerEntitylessSafe
}

func rulesEntityRequirement(source semanticview.Source, flowID string, rules []runtimecontracts.HandlerRuleEntry) handlerEntityRequirement {
	requirement := handlerEntitylessSafe
	for _, rule := range rules {
		for _, classify := range handlerRuleEntryEntityClassifiers {
			requirement = requirement.merge(classify(source, flowID, rule))
		}
	}
	return requirement
}

func existingWhen(required bool) handlerEntityRequirement {
	if required {
		return handlerExistingEntityRequired
	}
	return handlerEntitylessSafe
}

func materializingWhen(required bool) handlerEntityRequirement {
	if required {
		return handlerMaterializingEntity
	}
	return handlerEntitylessSafe
}

func actionEntityRequirement(action runtimecontracts.ActionSpec) handlerEntityRequirement {
	if actionMaterializesEntity(action) {
		return handlerMaterializingEntity
	}
	return existingWhen(actionReferencesEntity(action))
}

func activityEntityRequirement(activity runtimecontracts.ActivitySpec) handlerEntityRequirement {
	return existingWhen((activity.Approval != nil && strings.TrimSpace(activity.Approval.Decision) != "") || expressionValueMapReferencesEntity(activity.Input))
}

func guardEntityRequirement(guard *runtimecontracts.GuardSpec) handlerEntityRequirement {
	if guard == nil {
		return handlerEntitylessSafe
	}
	failure, err := guard.FailureSpec()
	return existingWhen((err == nil && failure.Action == runtimecontracts.GuardFailureActionKill) || guardReferencesEntity(guard))
}

func fanOutEntityRequirement(spec *runtimecontracts.FanOutSpec) handlerEntityRequirement {
	if spec == nil {
		return handlerEntitylessSafe
	}
	if emitSpecReferencesEntity(spec.Emit) {
		return handlerMaterializingEntity
	}
	return existingWhen(fanOutReferencesEntity(spec))
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
	for index := range query.Queries {
		if queryReferencesEntity(&query.Queries[index]) {
			return true
		}
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
	return typedPathReferencesEntity(spec.Source, spec.SourcePath) ||
		typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		expressionReferencesEntity(spec.Condition) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func reduceReferencesEntity(spec *runtimecontracts.ReduceSpec) bool {
	if spec == nil {
		return false
	}
	return typedPathReferencesEntity(spec.Source, spec.SourcePath) ||
		typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func countReferencesEntity(spec *runtimecontracts.CountSpec) bool {
	if spec == nil {
		return false
	}
	return typedPathReferencesEntity(spec.Source, spec.SourcePath) ||
		typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		expressionReferencesEntity(spec.Condition) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
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
	return rulesEntityRequirement(source, flowID, []runtimecontracts.HandlerRuleEntry{spec.OnComplete, spec.Timeout.Outcome}) != handlerEntitylessSafe
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
	state.Metadata = workflowCreateEntityFields(source, flowID)
	state.Control = workflowStateControlFromIdentity(instance)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	for field, value := range expected {
		values.Wrap(state.Metadata).SetPath(paths.Parse(field), value)
	}
	return nil
}
