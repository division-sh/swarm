package runforkadmission

import (
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type fixedCutConstructor struct {
	instance flowidentity.Instance
	evidence capturedConstructionEvidence
}

type fixedCutConstructionIndex struct {
	byPath map[string]fixedCutConstructor
	byFlow map[string][]fixedCutConstructor
}

func indexFixedCutConstructions(source semanticview.Source, cut runfork.RunForkPlan) (fixedCutConstructionIndex, error) {
	if err := validateFixedConstructionMetadata(cut.Entities); err != nil {
		return fixedCutConstructionIndex{}, err
	}
	index := fixedCutConstructionIndex{byPath: make(map[string]fixedCutConstructor, len(cut.Entities)), byFlow: make(map[string][]fixedCutConstructor)}
	for _, entity := range cut.Entities {
		evidence, err := fixedConstructionEvidence(cut, entity)
		if err != nil {
			return fixedCutConstructionIndex{}, err
		}
		instance := evidence.Identity
		if err := validateSelectedConstruction(source, cut.SourceRunID, instance, entity); err != nil {
			return fixedCutConstructionIndex{}, err
		}
		entry := fixedCutConstructor{instance: instance, evidence: evidence}
		index.byPath[instance.InstancePath] = entry
		index.byFlow[instance.TemplateID] = append(index.byFlow[instance.TemplateID], entry)
	}
	return index, nil
}

func fixedCutConnectTargets(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, selected fixedCutConnectSelection, eventID string, routing events.RoutingSource, receiver pinrouting.ConnectRoutePlan) ([]events.RouteIdentity, bool, error) {
	event, found, err := cut.HistoricalRouteSelector(eventID)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, true, nil
	}
	readback := receiver.Readback()
	admission, admitted := event.PayloadAdmission()
	if !admitted || event.RoutingSource() != routing || readback.ProducerEvent == nil ||
		admission.Binding().FlowID() != readback.ProducerEvent.FlowPath ||
		admission.Binding().EventKey() != readback.ProducerEvent.EventName ||
		admission.Binding().SchemaDigest() != readback.ProducerEvent.AcceptanceSchemaDigest {
		return nil, false, fmt.Errorf("selected keyed receiver selector %s contradicts its captured source/schema: captured=%s/%s/%s selected=%+v", eventID,
			admission.Binding().FlowID(), admission.Binding().EventKey(), admission.Binding().SchemaDigest(), readback.ProducerEvent)
	}
	if receiver.InstanceKey() == nil || receiver.InstanceKey().RequiresDeliveryProjection() {
		// Minted/event-sourced key projection is a separate admitted occurrence.
		return nil, true, nil
	}
	keyed, err := fixedCutSelectedConnectConstructor(source, cut, constructors, selected, eventID, event, admission, receiver)
	if err != nil {
		return nil, false, err
	}
	bundle, _ := semanticview.Bundle(source)
	selectionFlow := receiver.SelectionFlowID()
	instance := keyed
	if selectionFlow != readback.Receiver.FlowID {
		view, found := bundle.FlowViewByID(readback.Receiver.FlowID)
		if !found {
			return nil, false, fmt.Errorf("fixed receiver lacks structural parent")
		}
		var descendants []string
		for cursor := view; cursor.Paths.FlowPath != selectionFlow; cursor = cursor.Parent {
			if cursor.Parent == nil {
				return nil, false, fmt.Errorf("fixed receiver lies outside selected lexical constructor")
			}
			descendants = append(descendants, cursor.Paths.FlowPath)
		}
		instance, err = fixedCutKeylessDescendants(source, constructors, keyed, descendants)
		if err != nil {
			return nil, false, err
		}
	}
	target := receiver.ReceiverRoute(instance.InstancePath, instance.EntityID)
	selected.remember(event, receiver, keyed, target)
	return []events.RouteIdentity{target}, true, nil
}

// A selection belongs to one typed occurrence, not the fixed cut's census.
type fixedCutConnectSelection map[string][]flowidentity.Instance

func (s fixedCutConnectSelection) remember(event events.Event, plan pinrouting.ConnectRoutePlan, instance flowidentity.Instance, target events.RouteIdentity) {
	// A captured selector cannot supply invocation-local provider authorization.
	if plan.ProviderOutputAuthorization() != nil {
		return
	}
	if event.RoutingSource().Kind() == events.RoutingSourceRoot {
		switch event.AdmissionClass() {
		case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected, events.EventAdmissionSelectedForkReplay:
			if explicit := event.TargetRoute(); !explicit.Empty() && (!plan.AcceptsReceiverTarget(explicit, event.RunID()) || explicit != target) {
				return
			}
		}
	}
	s[instance.TemplateID] = append(s[instance.TemplateID], instance)
}

func (s fixedCutConnectSelection) keyedChild(parent flowidentity.Instance, flowID string) (flowidentity.Instance, error) {
	route := flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}
	var child flowidentity.Instance
	for _, candidate := range s[flowID] {
		if candidate.ParentRoute != route {
			continue
		}
		if child != (flowidentity.Instance{}) && child != candidate {
			return flowidentity.Instance{}, fmt.Errorf("fixed selector has competing keyed ancestors for %s", flowID)
		}
		child = candidate
	}
	if child == (flowidentity.Instance{}) {
		return child, fmt.Errorf("fixed selector requires its exact keyed ancestor %s", flowID)
	}
	return child, nil
}

func (s fixedCutConnectSelection) constructionChild(source semanticview.Source, parent flowidentity.Instance, flowID string) (flowidentity.Instance, error) {
	if schema, known := source.FlowSchemaByID(flowID); known && !schema.Instance.Empty() {
		return s.keyedChild(parent, flowID)
	}
	return flowidentity.KeylessChild(source, parent, flowID)
}

func fixedCutSelectedConnectConstructor(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, selected fixedCutConnectSelection, eventID string, event events.Event, admission events.PayloadAdmission, receiver pinrouting.ConnectRoutePlan) (flowidentity.Instance, error) {
	parent, err := fixedCutConnectParent(source, cut, constructors, selected, event, receiver)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return flowidentity.Instance{}, err
	}
	resolved, err := receiver.ResolvedInstanceKey(payload, event.ID())
	if err != nil {
		return flowidentity.Instance{}, err
	}
	bundle, _ := semanticview.Bundle(source)
	selectionFlow := receiver.SelectionFlowID()
	var contract contracts.TemplateInstanceContract
	if selectionFlow == receiver.Readback().Receiver.FlowID {
		contract, err = receiver.ReceiverTemplate(bundle)
	} else {
		contract, err = bundle.ResolveFlowTemplateInstance(selectionFlow)
	}
	if err != nil {
		return flowidentity.Instance{}, err
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(source, contract.FlowID, resolved)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	fact, err := correlation.NewSourceArtifactFact(hash)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, cut.SourceRunID, selectionFlow, parent, keys)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	parentRoute := flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}
	var matches []flowidentity.Instance
	for _, constructor := range constructors.byFlow[selectionFlow] {
		instance, evidence := constructor.instance, constructor.evidence
		if instance.ParentRoute != parentRoute {
			continue
		}
		if evidence.InstanceKey == lookup.InstanceKey() {
			if evidence.Original.BundleHash != admission.Binding().BundleHash() {
				return flowidentity.Instance{}, fmt.Errorf("fixed constructor and selector have foreign source bundles")
			}
			if receiver.InstanceKey().Mode() == contracts.FlowInputResolutionModeCreate {
				creating, found, err := evidence.creatingSelector(cut)
				if err != nil {
					return flowidentity.Instance{}, err
				}
				if !found || creating.ID() != eventID {
					return flowidentity.Instance{}, fmt.Errorf("historical create selector does not own the captured constructor")
				}
			}
			matches = append(matches, instance)
		}
	}
	if len(matches) != 1 {
		return flowidentity.Instance{}, fmt.Errorf("selected captured receiver requires one exact key/parent match; got %d", len(matches))
	}
	switch receiver.InstanceKey().Mode() {
	case contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeSelectOrCreate:
	default:
		return flowidentity.Instance{}, fmt.Errorf("selected captured receiver has unsupported key resolution mode")
	}
	return matches[0], nil
}

// Walk captured ancestry and children proved by this exact occurrence.
func fixedCutConnectParent(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, selected fixedCutConnectSelection, event events.Event, receiver pinrouting.ConnectRoutePlan) (flowidentity.Instance, error) {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("fixed receiver requires admitted construction tree")
	}
	view, found := bundle.FlowViewByID(receiver.SelectionFlowID())
	if !found || view.Parent == nil {
		return flowidentity.Instance{}, fmt.Errorf("fixed receiver lacks structural parent")
	}
	owners := constructors.byPath
	route := event.RoutingSource().Route()
	path := route.FlowInstance
	if path == "" {
		path = cut.SourceRunID
	}
	entry, found := owners[path]
	owner := entry.instance
	if !found || (route.FlowInstance != "" && (owner.TemplateID != route.FlowID || owner.EntityID != route.EntityID)) ||
		(route.FlowInstance == "" && (owner.TemplateID != semanticview.RootExecutionFlowID(source) || owner.EntityID != flowidentity.EntityID(cut.SourceRunID) || route.EntityID != "" && route.EntityID != owner.EntityID)) {
		return flowidentity.Instance{}, fmt.Errorf("fixed selector has no exact constructed source")
	}
	lexical := receiver.Readback().FlowPath
	for steps := 0; owner.TemplateID != lexical; steps++ {
		if steps >= len(owners) || owner.ParentRoute.Empty() {
			return flowidentity.Instance{}, fmt.Errorf("fixed selector lacks exact lexical constructor")
		}
		entry, found := owners[owner.ParentRoute.FlowInstance]
		parent := entry.instance
		if !found || parent.TemplateID != owner.ParentRoute.FlowID || parent.EntityID != owner.ParentRoute.EntityID {
			return flowidentity.Instance{}, fmt.Errorf("fixed selector has conflicting constructor ancestry")
		}
		owner = parent
	}
	var descendants []string
	for cursor := view.Parent; cursor.Paths.FlowPath != owner.TemplateID; cursor = cursor.Parent {
		if cursor.Parent == nil {
			return flowidentity.Instance{}, fmt.Errorf("fixed receiver lies outside selected lexical constructor")
		}
		descendants = append(descendants, cursor.Paths.FlowPath)
	}
	for i := len(descendants) - 1; i >= 0; i-- {
		child, err := selected.constructionChild(source, owner, descendants[i])
		if err != nil {
			return flowidentity.Instance{}, err
		}
		entry, found := owners[child.InstancePath]
		stored := entry.instance
		if !found || stored != child {
			return flowidentity.Instance{}, fmt.Errorf("fixed receiver lacks exact keyless ancestor")
		}
		owner = stored
	}
	return owner, nil
}

func fixedCutKeylessDescendants(source semanticview.Source, constructors fixedCutConstructionIndex, owner flowidentity.Instance, descendants []string) (flowidentity.Instance, error) {
	for i := len(descendants) - 1; i >= 0; i-- {
		child, err := flowidentity.KeylessChild(source, owner, descendants[i])
		if err != nil {
			return flowidentity.Instance{}, err
		}
		entry, found := constructors.byPath[child.InstancePath]
		stored := entry.instance
		if !found || stored != child {
			return flowidentity.Instance{}, fmt.Errorf("fixed receiver lacks exact keyless ancestor")
		}
		owner = stored
	}
	return owner, nil
}

func fixedCutConstructedRouteEvidence(source semanticview.Source, routeTable *runtimebus.RouteTable, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, graph pinrouting.CompiledConnectGraph, frontier runfork.RunForkContractFrontierAdmission, dynamic []string, history []runfork.RunForkSelectedContractRouteEvent) ([]runfork.ConstructedRouteEvidence, error) {
	var out []runfork.ConstructedRouteEvidence
	eventsByID := make(map[string]runfork.RunForkSelectedContractRouteEvent, len(history))
	for _, event := range history {
		eventsByID[event.SourceEventID] = event
	}
	for _, path := range dynamic {
		recipientsAtPath := func(event runfork.RunForkSelectedContractRouteEvent) []runfork.RunForkContractFrontierRecipient {
			return slices.DeleteFunc(slices.Clone(event.DerivedRecipients), func(recipient runfork.RunForkContractFrontierRecipient) bool {
				return recipient.Path != path
			})
		}
		constructor, found := constructors.byPath[path]
		if !found {
			return nil, fmt.Errorf("fixed topology lost captured constructor")
		}
		bound, found, err := fixedCutConstructorRouteEvidence(source, routeTable, cut, constructors, graph, frontier, constructor, history, eventsByID, recipientsAtPath)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, bound)
		}
	}
	return out, nil
}

func fixedCutConstructorRouteEvidence(source semanticview.Source, routeTable *runtimebus.RouteTable, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, graph pinrouting.CompiledConnectGraph, frontier runfork.RunForkContractFrontierAdmission, constructor fixedCutConstructor, history []runfork.RunForkSelectedContractRouteEvent, eventsByID map[string]runfork.RunForkSelectedContractRouteEvent, recipientsAtPath func(runfork.RunForkSelectedContractRouteEvent) []runfork.RunForkContractFrontierRecipient) (runfork.ConstructedRouteEvidence, bool, error) {
	var absent runfork.ConstructedRouteEvidence
	evidence := constructor.evidence
	selector, found, err := evidence.creatingSelector(cut)
	if err != nil {
		return absent, false, err
	}
	if evidence.Original.CreatingInput.Input == "" {
		return fixedCutEagerChildRouteEvidence(source, cut, constructors, graph, frontier, constructor, history, recipientsAtPath)
	}
	if !found {
		if constructor.instance.InstancePath == cut.ForkPoint.RoutingSource.Route().FlowInstance && evidence.InstanceKey != "" && len(evidence.Lineage) != 0 {
			return inheritedKeyedConstructorEvidence(source, routeTable, graph, cut, frontier, constructor)
		}
		return absent, false, nil
	}
	if err := requireFixedCutConstructorSelector(graph, evidence, selector); err != nil {
		return absent, false, err
	}
	event, found := eventsByID[selector.ID()]
	if !found || event.Disposition == runfork.RunForkSelectedContractDispositionFailClosed {
		return absent, false, nil
	}
	recipients := recipientsAtPath(event)
	if len(recipients) == 0 {
		return absent, false, nil
	}
	bound, err := cut.BindConstructedRouteEvidence(evidence.Identity.EntityID, selector.ID(), recipients, frontier)
	return bound, err == nil, err
}

// A keyless child shares the parent's construction occurrence, not its input.
// Only a completed edge to that exact child can supply its historical proof.
func fixedCutEagerChildRouteEvidence(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, graph pinrouting.CompiledConnectGraph, frontier runfork.RunForkContractFrontierAdmission, child fixedCutConstructor, history []runfork.RunForkSelectedContractRouteEvent, recipientsAtPath func(runfork.RunForkSelectedContractRouteEvent) []runfork.RunForkContractFrontierRecipient) (runfork.ConstructedRouteEvidence, bool, error) {
	var absent runfork.ConstructedRouteEvidence
	parent, found := constructors.byPath[child.instance.ParentRoute.FlowInstance]
	if !found || child.evidence.InstanceKey != "" ||
		child.evidence.Original.RunID != parent.evidence.Original.RunID ||
		child.evidence.Original.CreatingInput.EventID != parent.evidence.Original.CreatingInput.EventID {
		return absent, false, nil
	}
	expected, err := flowidentity.KeylessChild(source, parent.instance, child.instance.TemplateID)
	if err != nil || expected != child.instance {
		return absent, false, nil
	}
	target := events.RouteIdentity{FlowID: expected.TemplateID, FlowInstance: expected.InstancePath, EntityID: expected.EntityID}
	for _, event := range history {
		if event.Disposition != runfork.RunForkSelectedContractDispositionEvidenceOnly {
			continue
		}
		recipients := recipientsAtPath(event)
		routes, err := fixedCutEagerChildEdge(source, cut, constructors, graph, target, event, recipients)
		if err != nil {
			return absent, false, err
		}
		if len(routes) != 0 {
			bound, err := cut.BindEagerChildRouteEvidence(expected.EntityID, parent.instance.EntityID, event.SourceEventID, recipients, routes, frontier)
			return bound, err == nil, err
		}
	}
	return absent, false, nil
}

func fixedCutEagerChildEdge(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, graph pinrouting.CompiledConnectGraph, target events.RouteIdentity, history runfork.RunForkSelectedContractRouteEvent, recipients []runfork.RunForkContractFrontierRecipient) ([]events.DeliveryRoute, error) {
	var retained []events.DeliveryRoute
	for _, route := range history.HistoricalDeliveryRoutes {
		if route.Target.Route() == target {
			retained = append(retained, route)
		}
	}
	if len(recipients) == 0 || len(recipients) != len(retained) {
		return nil, nil
	}
	selector, found, err := cut.HistoricalRouteSelector(history.SourceEventID)
	if err != nil || !found {
		return nil, err
	}
	sourceEvent, err := pinrouting.AdmitSourceEvent(selector.Type(), selector.RoutingSource())
	if err != nil {
		return nil, err
	}
	edges := make(map[events.ConnectPlanIdentity]pinrouting.ConnectRoutePlan)
	plans := graph.MatchingSourceEvent(sourceEvent)
	pinrouting.SortConnectPlansAncestorFirst(plans)
	selected := make(fixedCutConnectSelection)
	for _, edge := range plans {
		flowID := edge.ReceiverEndpoint().Readback().FlowID
		// This exact child may be reached by a static edge or by selection of its keyed ancestor.
		exactTarget := flowID == target.FlowID && (!edge.RequiresRuntimeResolution() || edge.SelectionFlowID() != flowID)
		if !exactTarget && (edge.InstanceKey() == nil || !strings.HasPrefix(target.FlowID, flowID+"/")) {
			continue
		}
		targets, matched, err := contractFrontierPlanTargets(source, cut, constructors, selected, selector.ID(), selector.RoutingSource(), edge, nil)
		if err != nil {
			return nil, err
		}
		if !exactTarget || !matched || len(targets) != 1 || targets[0] != target {
			continue
		}
		id, err := pinrouting.ConnectPlanIdentity(edge)
		if err != nil {
			return nil, err
		}
		edges[id] = edge
	}
	if !matchFixedCutEagerChildDeliveries(cut.SourceRunID, recipients, retained, edges) {
		return nil, nil
	}
	return retained, nil
}

func matchFixedCutEagerChildDeliveries(sourceRun string, recipients []runfork.RunForkContractFrontierRecipient, routes []events.DeliveryRoute, edges map[events.ConnectPlanIdentity]pinrouting.ConnectRoutePlan) bool {
	used := make([]bool, len(routes))
	for _, recipient := range recipients {
		id, pin, connect := recipient.Connect()
		edge, found := edges[id]
		if !connect || !found || pin != edge.ReceiverPinIdentity().EvidenceIdentity() {
			return false
		}
		matched := false
		for i, route := range routes {
			if used[i] || !route.Recipient.IsNode() || !matchesHistoricalRecipient(sourceRun, route, recipient) {
				continue
			}
			claim, err := edge.ClaimForRetainedDelivery(route)
			if err == nil && claim.Equal(route.ConnectClaim) {
				used[i], matched = true, true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func requireFixedCutConstructorSelector(graph pinrouting.CompiledConnectGraph, evidence capturedConstructionEvidence, selector events.Event) error {
	receipt := evidence.Original
	admission, _ := selector.PayloadAdmission()
	if receipt.BundleHash != admission.Binding().BundleHash() {
		return fmt.Errorf("fixed constructor and selector have foreign source bundles")
	}
	sourceEvent, err := pinrouting.AdmitSourceEvent(selector.Type(), selector.RoutingSource())
	if err != nil {
		return err
	}
	for _, receiver := range graph.MatchingSourceEvent(sourceEvent) {
		endpoint := receiver.ReceiverEndpoint().Readback()
		if endpoint.FlowID == evidence.Identity.TemplateID && endpoint.LocalEvent == receipt.CreatingInput.Input {
			return nil
		}
	}
	return fmt.Errorf("fixed constructor input differs from selected receiver")
}
