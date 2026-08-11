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
	acquisitionUsesEntity, acquisitionMaterializes, err := deliveryTargetHandlerAcquisition(req.Source, req.Handler, req.Event.Type())
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	usesEntity := acquisitionUsesEntity || handlerUsesEntity(req.Source, flowID, handler)
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
		return events.NewMaterializingEntityTarget(materializing[0])
	}
	if len(existing) == 1 && len(materializing) == 0 {
		return events.NewExistingEntityTarget(existing[0])
	}
	if !usesEntity {
		blueprint.EntityID = ""
		return events.NewEntitylessReceiverTarget(blueprint)
	}
	if acquisitionMaterializes || handlerCanMaterializeEntity(req.Source, flowID, handler) {
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
	acquisitionUsesEntity, _, err := deliveryTargetHandlerAcquisition(source, handlerFact, evt.Type())
	if err != nil {
		return err
	}
	usesEntity := acquisitionUsesEntity || handlerUsesEntity(source, flowID, handler)
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

func handlerCanMaterializeEntity(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
	return handlerMaterializesEntity(source, flowID, handler)
}

type handlerEntityFieldClassifier func(semanticview.Source, string, SystemNodeEventHandler) bool

// Every executable handler field has an explicit semantic disposition here.
// Tests compare these keys with the contract type so new fields fail closed.
var systemNodeEventHandlerEntityClassifiers = map[string]handlerEntityFieldClassifier{
	"Action": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return actionReferencesEntity(handler.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return expressionValueMapReferencesEntity(handler.Activity.Input)
	},
	"CreateEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return handler.CreateEntity
	},
	"SelectEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return handler.SelectEntity != nil && !handler.SelectEntity.Empty()
	},
	"SelectOrCreateEntity": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()
	},
	"Description":    noHandlerEntityUse,
	"EvidenceTarget": noHandlerEntityUse,
	"Emit": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return emitSpecReferencesEntity(handler.Emit)
	},
	"OnSuccess": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return emitSpecReferencesEntity(handler.OnSuccess.Emit)
	},
	"Guard": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return guardReferencesEntity(handler.Guard)
	},
	"AdvancesTo": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return strings.TrimSpace(handler.AdvancesTo) != ""
	},
	"SetsGate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return gateSpecName(handler.SetsGate) != ""
	},
	"ClearGates": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return len(handler.ClearGates) != 0
	},
	"DataAccumulation": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return dataAccumulationReferencesEntity(handler.DataAccumulation)
	},
	"Condition": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return expressionReferencesEntity(handler.Condition)
	},
	"Logic": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return expressionReferencesEntity(handler.Logic)
	},
	"Loop": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return handler.Loop != nil
	},
	"OnComplete": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
		return rulesReferenceEntity(source, flowID, handler.OnComplete)
	},
	"Rules": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
		return rulesReferenceEntity(source, flowID, handler.Rules)
	},
	"Accumulate": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return accumulateReferencesEntity(handler.Accumulate)
	},
	"Join": func(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
		return joinReferencesEntity(source, flowID, handler.Join)
	},
	"Compute": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return computeReferencesEntity(handler.Compute)
	},
	"Query": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return queryReferencesEntity(handler.Query)
	},
	"FanOut": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return fanOutReferencesEntity(handler.FanOut)
	},
	"GroupBy": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return groupByReferencesEntity(handler.GroupBy)
	},
	"Filter": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return filterReferencesEntity(handler.Filter)
	},
	"Reduce": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return reduceReferencesEntity(handler.Reduce)
	},
	"Count": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return countReferencesEntity(handler.Count)
	},
	"Clear": func(_ semanticview.Source, _ string, handler SystemNodeEventHandler) bool {
		return clearReferencesEntity(handler.Clear)
	},
}

type handlerRuleEntityFieldClassifier func(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) bool

var handlerRuleEntryEntityClassifiers = map[string]handlerRuleEntityFieldClassifier{
	"ID":          noHandlerRuleEntityUse,
	"Description": noHandlerRuleEntityUse,
	"Condition": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return expressionReferencesEntity(rule.Condition)
	},
	"PolicyRow": noHandlerRuleEntityUse,
	"AdvancesTo": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return strings.TrimSpace(rule.AdvancesTo) != ""
	},
	"Emit": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return emitSpecReferencesEntity(rule.Emit)
	},
	"Action": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return actionReferencesEntity(rule.Action)
	},
	"Activity": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return expressionValueMapReferencesEntity(rule.Activity.Input)
	},
	"DataAccumulation": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return dataAccumulationReferencesEntity(rule.DataAccumulation)
	},
	"Compute": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return computeReferencesEntity(rule.Compute)
	},
	"FanOut": func(_ semanticview.Source, _ string, rule runtimecontracts.HandlerRuleEntry) bool {
		return fanOutReferencesEntity(rule.FanOut)
	},
}

func handlerUsesEntity(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
	if handlerMaterializesEntity(source, flowID, handler) {
		return true
	}
	for _, classify := range systemNodeEventHandlerEntityClassifiers {
		if classify(source, flowID, handler) {
			return true
		}
	}
	return false
}

func noHandlerEntityUse(semanticview.Source, string, SystemNodeEventHandler) bool { return false }

func noHandlerRuleEntityUse(semanticview.Source, string, runtimecontracts.HandlerRuleEntry) bool {
	return false
}

func rulesReferenceEntity(source semanticview.Source, flowID string, rules []runtimecontracts.HandlerRuleEntry) bool {
	for _, rule := range rules {
		for _, classify := range handlerRuleEntryEntityClassifiers {
			if classify(source, flowID, rule) {
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
		expressionReferencesEntity(spec.Predicate) ||
		expressionReferencesEntity(spec.Condition) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath)
}

func reduceReferencesEntity(spec *runtimecontracts.ReduceSpec) bool {
	if spec == nil {
		return false
	}
	return typedPathReferencesEntity(spec.Source, spec.SourcePath) ||
		typedPathReferencesEntity(spec.ItemsFrom, spec.ItemsPath) ||
		typedPathReferencesEntity(spec.StoreAs, spec.StorePath) ||
		expressionValueMapReferencesEntity(spec.Params)
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

func clearReferencesEntity(spec *runtimecontracts.ClearSpec) bool {
	if spec == nil {
		return false
	}
	for _, target := range spec.Targets {
		if pathReferencesEntity(target) {
			return true
		}
	}
	return false
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
	return rulesReferenceEntity(source, flowID, []runtimecontracts.HandlerRuleEntry{spec.OnComplete, spec.Timeout.Outcome})
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
