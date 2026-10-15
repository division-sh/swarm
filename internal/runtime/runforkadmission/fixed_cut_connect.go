package runforkadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
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

func fixedCutConnectTargets(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, eventID string, routing events.RoutingSource, receiver pinrouting.ConnectRoutePlan) ([]events.RouteIdentity, bool, error) {
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
	parent, err := fixedCutConnectParent(source, cut, constructors, event, receiver)
	if err != nil {
		return nil, false, err
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return nil, false, err
	}
	resolved, err := receiver.ResolvedInstanceKey(payload, event.ID())
	if err != nil {
		return nil, false, err
	}
	bundle, _ := semanticview.Bundle(source)
	contract, err := receiver.ReceiverTemplate(bundle)
	if err != nil {
		return nil, false, err
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(source, contract.FlowID, resolved)
	if err != nil {
		return nil, false, err
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		return nil, false, err
	}
	fact, err := correlation.NewSourceArtifactFact(hash)
	if err != nil {
		return nil, false, err
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, cut.SourceRunID, readback.Receiver.FlowID, parent, keys)
	if err != nil {
		return nil, false, err
	}
	parentRoute := flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}
	var matches []flowidentity.Instance
	for _, constructor := range constructors.byFlow[readback.Receiver.FlowID] {
		instance, evidence := constructor.instance, constructor.evidence
		if instance.ParentRoute != parentRoute {
			continue
		}
		if evidence.InstanceKey == lookup.InstanceKey() {
			if evidence.Original.BundleHash != admission.Binding().BundleHash() {
				return nil, false, fmt.Errorf("fixed constructor and selector have foreign source bundles")
			}
			if receiver.InstanceKey().Mode() == contracts.FlowInputResolutionModeCreate {
				creating, found, err := evidence.creatingSelector(cut)
				if err != nil {
					return nil, false, err
				}
				if !found || creating.ID() != eventID {
					return nil, false, fmt.Errorf("historical create selector does not own the captured constructor")
				}
			}
			matches = append(matches, instance)
		}
	}
	if len(matches) != 1 {
		return nil, false, fmt.Errorf("selected captured receiver requires one exact key/parent match; got %d", len(matches))
	}
	switch receiver.InstanceKey().Mode() {
	case contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeSelectOrCreate:
	default:
		return nil, false, fmt.Errorf("selected captured receiver has unsupported key resolution mode")
	}
	// This is the lifecycle owner's selector-only leaf: exact keys plus stored
	// construction and parent. The generic descriptor materializer infers a
	// single-level scope from the path, which cannot describe nested constructors.
	return []events.RouteIdentity{receiver.ReceiverRoute(matches[0].InstancePath, matches[0].EntityID)}, true, nil
}

// Walk only captured ancestry and canonical keyless descendants. A keyed
// ancestor not selected by this exact source cannot be chosen from a census.
func fixedCutConnectParent(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, event events.Event, receiver pinrouting.ConnectRoutePlan) (flowidentity.Instance, error) {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("fixed receiver requires admitted construction tree")
	}
	view, found := bundle.FlowViewByID(receiver.ReceiverEndpoint().Readback().FlowID)
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
		child, err := flowidentity.KeylessChild(source, owner, descendants[i])
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

func fixedCutConstructedRouteEvidence(source semanticview.Source, cut runfork.RunForkPlan, constructors fixedCutConstructionIndex, graph pinrouting.CompiledConnectGraph, frontier runfork.RunForkContractFrontierAdmission, dynamic []string, history []runfork.RunForkSelectedContractRouteEvent) ([]runfork.ConstructedRouteEvidence, error) {
	var out []runfork.ConstructedRouteEvidence
	eventsByID := make(map[string]runfork.RunForkSelectedContractRouteEvent, len(history))
	for _, event := range history {
		eventsByID[event.SourceEventID] = event
	}
	for _, path := range dynamic {
		constructor, found := constructors.byPath[path]
		if !found {
			return nil, fmt.Errorf("fixed topology lost captured constructor")
		}
		evidence := constructor.evidence
		selector, found, err := evidence.creatingSelector(cut)
		if err != nil {
			return nil, err
		}
		if !found || evidence.Original.CreatingInput.Input == "" {
			continue
		}
		if err := requireFixedCutConstructorSelector(graph, evidence, selector); err != nil {
			return nil, err
		}
		event, found := eventsByID[selector.ID()]
		if !found || event.Disposition == runfork.RunForkSelectedContractDispositionFailClosed {
			continue
		}
		var recipients []runfork.RunForkContractFrontierRecipient
		for _, recipient := range event.DerivedRecipients {
			if recipient.Path == path {
				recipients = append(recipients, recipient)
			}
		}
		if len(recipients) == 0 {
			continue
		}
		bound, err := cut.BindConstructedRouteEvidence(evidence.Identity.EntityID, selector.ID(), recipients, frontier)
		if err != nil {
			return nil, err
		}
		out = append(out, bound)
	}
	return out, nil
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
