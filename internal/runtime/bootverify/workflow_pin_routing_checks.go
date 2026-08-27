package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkPinTargetResolution(c *checkerContext) []Finding {
	findings := []Finding{}
	flowIDs := []string{""}
	for flowID := range c.source.FlowSchemaEntries() {
		if strings.TrimSpace(flowID) != "" {
			flowIDs = append(flowIDs, flowID)
		}
	}
	for _, flowID := range flowIDs {
		for _, pin := range c.source.FlowOutputEventPins(flowID) {
			consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, flowID, pin.EventType())
			if consumer.InvalidSink() {
				location := strings.TrimSpace(flowID)
				if location == "" {
					location = "root"
				}
				findings = append(findings, Finding{
					CheckID:  "pin_target_resolution",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("output pin %s in %s declares an invalid sink; the only supported value is sink: harness", pin.EventType(), location),
					Location: location,
				})
				continue
			}
			if !consumer.Has(runtimepinrouting.OutputConsumerHarness) || !consumer.HasRuntimeConsumer() {
				continue
			}
			location := strings.TrimSpace(flowID)
			if location == "" {
				location = "root"
			}
			findings = append(findings, Finding{
				CheckID:  "pin_target_resolution",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("output pin %s in %s declares validation-only sink: harness and a canonical runtime consumer; remove sink: harness or remove the runtime consumer", pin.EventType(), location),
				Location: location,
			})
		}
	}
	for _, site := range pinRoutingEmitSites(c.source) {
		if !runtimepinrouting.PinDeclaredOutput(c.source, site.FlowID(), site.Spec.EventType()) {
			continue
		}
		eventType := site.Spec.EventType()
		if runtimepinrouting.OutputHarnessSink(c.source, site.FlowID(), eventType) {
			continue
		}
		consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, site.FlowID(), eventType)
		if !consumer.HasRuntimeConsumer() {
			findings = append(findings, pinTargetFinding(site, runtimepinrouting.FailureTargetRequiredMissing.Code()))
		}
	}
	for _, site := range pinRoutingAgentEmitSites(c.source) {
		if !runtimepinrouting.PinDeclaredOutput(c.source, site.FlowID, site.EventType) {
			continue
		}
		if runtimepinrouting.OutputHarnessSink(c.source, site.FlowID, site.EventType) {
			continue
		}
		consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, site.FlowID, site.EventType)
		if !consumer.HasRuntimeConsumer() {
			findings = append(findings, pinTargetAgentFinding(site, runtimepinrouting.FailureTargetRequiredMissing.Code()))
		}
	}
	return findings
}

func checkRedundantInTopologySelectEntity(c *checkerContext) []Finding {
	findings := []Finding{}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" || !bootverifyFlowStateful(c.source, flowID, schema) {
			continue
		}
		scope, ok := c.source.FlowScopeByID(flowID)
		if !ok {
			continue
		}
		for nodeID, node := range scope.Nodes {
			for eventType, handler := range node.EventHandlers {
				hasSelect := handler.SelectEntity != nil && !handler.SelectEntity.Empty()
				hasSelectOrCreate := handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()
				if !hasSelect && !hasSelectOrCreate {
					continue
				}
				if c.pinRoutingEventExternalSource(flowID, eventType) {
					continue
				}
				if !pinRoutingAllKnownProducersTargeted(c.source, flowID, eventType) {
					continue
				}
				label := "select_entity"
				if hasSelectOrCreate && !hasSelect {
					label = "select_or_create_entity"
				}
				findings = append(findings, Finding{
					CheckID:  "redundant_in_topology_select_entity",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("flow %s handler %s on node %s declares %s for normal in-topology composition; use scalar receiver instance, input resolution.mode, a same-named carry, and parent connect routing instead", flowID, eventType, nodeID, label),
					Location: flowID,
				})
			}
		}
	}
	return findings
}

func checkMissingExternalSelectEntity(c *checkerContext) []Finding {
	findings := []Finding{}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" || !bootverifyFlowStateful(c.source, flowID, schema) || strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			continue
		}
		if retiredStaticMultiEntityAcquisitionFlow(c.source, flowID, schema) {
			continue
		}
		if normalPrimaryEntityFlow(c.source, flowID, schema) {
			continue
		}
		if standingActivatedFlow(c.source, flowID) {
			continue
		}
		inputs := normalizeStringSet(c.source.FlowInputEvents(flowID))
		if len(inputs) == 0 {
			continue
		}
		scope, ok := c.source.FlowScopeByID(flowID)
		if !ok {
			continue
		}
		for nodeID, node := range scope.Nodes {
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				if _, ok := inputs[eventType]; !ok {
					continue
				}
				nodeRef, _ := semanticview.ResolveExecutableNodeDeclaration(c.source, flowID, nodeID)
				policy, err := runtimepipeline.CompileDeliveryTargetCompatibilityPolicy(c.source, nodeRef, flowID, events.EventType(eventType), handler)
				if err == nil && policy.Acquisition != runtimepipeline.DeliveryTargetAcquisitionNone {
					continue
				}
				if !c.pinRoutingEventExternalSource(flowID, eventType) {
					continue
				}
				findings = append(findings, Finding{
					CheckID:  "missing_external_select_entity",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s handler %s on node %s consumes external/no-target event without create_entity, select_entity, or select_or_create_entity", flowID, eventType, nodeID),
					Location: flowID,
				})
			}
		}
	}
	return findings
}

func pinRoutingEmitSites(source semanticview.Source) []semanticview.AuthoredEmitSite {
	return semanticview.AuthoredEmitSites(source)
}

type pinRoutingAgentEmitSite struct {
	FlowID    string
	AgentID   string
	EventType string
}

func pinRoutingAgentEmitSites(source semanticview.Source) []pinRoutingAgentEmitSite {
	if source == nil {
		return nil
	}
	sites := []pinRoutingAgentEmitSite{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
		if endpoint.Kind != semanticview.EventEndpointAgent {
			continue
		}
		sites = append(sites, pinRoutingAgentEmitSite{FlowID: endpoint.FlowID, AgentID: endpoint.AgentID, EventType: endpoint.Event.Authored})
	}
	return sites
}

func pinTargetFinding(site semanticview.AuthoredEmitSite, reason string) Finding {
	flowID := site.FlowID()
	scope := fmt.Sprintf("flow %s", flowID)
	location := flowID
	if flowID == "" {
		scope = "root"
		location = "root"
	}
	return Finding{
		CheckID:  "pin_target_resolution",
		Severity: "error",
		Message:  fmt.Sprintf("%s %s on node %s emits pin-declared output %s without valid target mechanism: %s", scope, site.Site, site.Node.Key(), site.Spec.EventType(), reason),
		Location: location,
	}
}

func pinTargetAgentFinding(site pinRoutingAgentEmitSite, reason string) Finding {
	scope := fmt.Sprintf("flow %s", site.FlowID)
	location := site.FlowID
	if strings.TrimSpace(site.FlowID) == "" {
		scope = "root"
		location = "root"
	}
	return Finding{
		CheckID:  "pin_target_resolution",
		Severity: "error",
		Message:  fmt.Sprintf("%s agent emit_events on agent %s emits pin-declared output %s without valid target mechanism: %s", scope, site.AgentID, site.EventType, reason),
		Location: location,
	}
}

func (c *checkerContext) pinRoutingEventExternalSource(flowID, eventType string) bool {
	if c.source == nil {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	if resolution, ok := c.resolveDeclaredInputProducerSource(flowID, eventType); ok {
		return inputProducerSourceIsExternalNoTarget(resolution)
	}
	entry, _, ok := c.source.ResolveFlowEventCatalogEntry(flowID, eventType)
	return ok && nonInputEventMetadataProducerSource(entry)
}

func pinRoutingAllKnownProducersTargeted(source semanticview.Source, flowID, eventType string) bool {
	producers := 0
	targeted := 0
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	graph := runtimepinrouting.CompileConnectGraph(source)
	sites := pinRoutingEmitSites(source)
	for _, endpoint := range pinRoutingKnownProducers(census, graph, flowID, eventType) {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler {
			continue
		}
		site, ok := pinRoutingEmitSiteForEndpoint(sites, endpoint)
		if !ok {
			continue
		}
		if !runtimepinrouting.PinDeclaredOutput(source, site.FlowID(), site.Spec.EventType()) {
			continue
		}
		producers++
		connectedToReceiver := compiledConnectsProducerToReceiver(graph, endpoint, flowID)
		consumer := runtimepinrouting.ClassifyOutputConsumer(source, site.FlowID(), site.Spec.EventType())
		if connectedToReceiver || consumer.Has(runtimepinrouting.OutputConsumerStructuralParent) {
			targeted++
		}
	}
	return producers > 0 && targeted == producers
}

func pinRoutingKnownProducers(census semanticview.AuthoredEventEndpointCensus, graph runtimepinrouting.CompiledConnectGraph, flowID, eventType string) []semanticview.AuthoredEventEndpoint {
	byID := map[string]semanticview.AuthoredEventEndpoint{}
	for _, endpoint := range census.MatchingProducersAcrossFlows(flowID, eventType) {
		byID[endpoint.ID] = endpoint
	}
	for _, edge := range graph.Edges() {
		if !edge.Consumer().Matches(flowID, events.EventType(eventType)) {
			continue
		}
		producer := edge.Producer()
		for _, endpoint := range census.Producers() {
			if producer.MatchesAuthoredEndpoint(endpoint) {
				byID[endpoint.ID] = endpoint
			}
		}
	}
	out := make([]semanticview.AuthoredEventEndpoint, 0, len(byID))
	for _, endpoint := range byID {
		out = append(out, endpoint)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func pinRoutingEmitSiteForEndpoint(sites []semanticview.AuthoredEmitSite, endpoint semanticview.AuthoredEventEndpoint) (semanticview.AuthoredEmitSite, bool) {
	for _, site := range sites {
		if site.Node.Equal(endpoint.Node) && strings.TrimSpace(site.SiteKey) == strings.TrimSpace(endpoint.Site) {
			return site, true
		}
	}
	return semanticview.AuthoredEmitSite{}, false
}

func compiledConnectsProducerToReceiver(graph runtimepinrouting.CompiledConnectGraph, producer semanticview.AuthoredEventEndpoint, receiverFlowID string) bool {
	for _, edge := range graph.Edges() {
		if !edge.Consumer().BelongsToFlow(strings.TrimSpace(receiverFlowID)) {
			continue
		}
		if edge.Producer().MatchesAuthoredEndpoint(producer) {
			return true
		}
	}
	return false
}
