package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkPinTargetResolution(c *checkerContext) []Finding {
	findings := []Finding{}
	for _, site := range pinRoutingEmitSites(c.source) {
		if !runtimepinrouting.PinDeclaredOutput(c.source, site.FlowID, site.Spec.EventType()) {
			continue
		}
		if failure := runtimepinrouting.ProducerRouteCommonPathFailure(c.source, site.FlowID, site.Spec.EventType(), site.Spec); failure != "" {
			findings = append(findings, pinTargetFinding(site, string(failure)))
			continue
		}
		connectedOutput := compositionConnectsFromOutputEvent(c.source, site.FlowID, site.Spec.EventType())
		structuralParent := pinRoutingStructuralParentRouteEligible(c.source, site.FlowID)
		if connectedOutput {
			structuralParent = true
		}
		if failure := runtimepinrouting.ValidateTargetSpec(c.source, site.FlowID, site.Spec, structuralParent); failure != "" {
			findings = append(findings, pinTargetFinding(site, string(failure)))
			continue
		}
		if connectedOutput {
			continue
		}
		if site.Spec.Target.Normalized().Kind == runtimecontracts.EmitTargetKindSender && c.pinRoutingEventExternalSource(site.FlowID, site.HandlerEvent) {
			findings = append(findings, pinTargetFinding(site, "target_sender_empty_source"))
		}
	}
	for _, site := range pinRoutingAgentEmitSites(c.source) {
		if !runtimepinrouting.PinDeclaredOutput(c.source, site.FlowID, site.EventType) {
			continue
		}
		spec := runtimecontracts.EmitSpec{Event: site.EventType}
		connectedOutput := compositionConnectsFromOutputEvent(c.source, site.FlowID, spec.EventType())
		structuralParent := pinRoutingStructuralParentRouteEligible(c.source, site.FlowID)
		if connectedOutput {
			structuralParent = true
		}
		if failure := runtimepinrouting.ValidateTargetSpec(c.source, site.FlowID, spec, structuralParent); failure != "" {
			findings = append(findings, pinTargetAgentFinding(site, string(failure)))
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
					Message:  fmt.Sprintf("flow %s handler %s on node %s declares %s for normal in-topology composition; use receiver instance.by plus parent connect routing instead", flowID, eventType, nodeID, label),
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
				if handler.CreateEntity || (handler.SelectEntity != nil && !handler.SelectEntity.Empty()) || (handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()) {
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
	scope := fmt.Sprintf("flow %s", site.FlowID)
	location := site.FlowID
	if strings.TrimSpace(site.FlowID) == "" {
		scope = "root"
		location = "root"
	}
	return Finding{
		CheckID:  "pin_target_resolution",
		Severity: "error",
		Message:  fmt.Sprintf("%s %s on node %s emits pin-declared output %s without valid target mechanism: %s", scope, site.Site, site.NodeID, site.Spec.EventType(), reason),
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

func pinRoutingStructuralParentRouteEligible(source semanticview.Source, flowID string) bool {
	if source == nil {
		return false
	}
	if schema, ok := source.FlowSchemaByID(flowID); ok && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
		return true
	}
	path := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	return strings.Contains(path, "/")
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
	topology := routingtopology.Build(source)
	sites := pinRoutingEmitSites(source)
	for _, endpoint := range pinRoutingKnownProducers(census, topology, flowID, eventType) {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler {
			continue
		}
		site, ok := pinRoutingEmitSiteForEndpoint(sites, endpoint)
		if !ok {
			continue
		}
		if !runtimepinrouting.PinDeclaredOutput(source, site.FlowID, site.Spec.EventType()) {
			continue
		}
		producers++
		connectedToReceiver := topologyConnectsProducerToReceiver(topology, endpoint.ID, flowID)
		structuralParent := pinRoutingStructuralParentRouteEligible(source, site.FlowID)
		if connectedToReceiver {
			structuralParent = true
		}
		if (site.Spec.HasTarget() && runtimepinrouting.ProducerRouteCommonPathFailure(source, site.FlowID, site.Spec.EventType(), site.Spec) == "") ||
			(structuralParent && !site.Spec.Broadcast) {
			targeted++
		}
	}
	return producers > 0 && targeted == producers
}

func pinRoutingKnownProducers(census semanticview.AuthoredEventEndpointCensus, topology routingtopology.Topology, flowID, eventType string) []semanticview.AuthoredEventEndpoint {
	byID := map[string]semanticview.AuthoredEventEndpoint{}
	for _, endpoint := range census.MatchingProducersAcrossFlows(flowID, eventType) {
		byID[endpoint.ID] = endpoint
	}
	input, ok := census.ResolveDeclaredInputEndpoint(flowID, eventType).Endpoint()
	if ok {
		for _, edge := range topology.Edges {
			if edge.Scope != routingtopology.DeliveryScopeInterFlowConnect || edge.Consumer.ID != input.ID {
				continue
			}
			if endpoint, exists := census.Endpoint(edge.Producer.ID); exists && endpoint.Direction == semanticview.EventEndpointProducer {
				byID[endpoint.ID] = endpoint
			}
			for _, exposure := range topology.BoundaryExposures {
				if exposure.Output.ID != edge.Producer.ID {
					continue
				}
				if endpoint, exists := census.Endpoint(exposure.Producer.ID); exists {
					byID[endpoint.ID] = endpoint
				}
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
		if strings.TrimSpace(site.FlowID) == strings.TrimSpace(endpoint.FlowID) && strings.TrimSpace(site.NodeID) == strings.TrimSpace(endpoint.NodeID) && strings.TrimSpace(site.SiteKey) == strings.TrimSpace(endpoint.Site) {
			return site, true
		}
	}
	return semanticview.AuthoredEmitSite{}, false
}

func topologyConnectsProducerToReceiver(topology routingtopology.Topology, producerID, receiverFlowID string) bool {
	for _, edge := range topology.Edges {
		if edge.Scope != routingtopology.DeliveryScopeInterFlowConnect || strings.TrimSpace(edge.Consumer.FlowID) != strings.TrimSpace(receiverFlowID) {
			continue
		}
		if edge.Producer.ID == producerID {
			return true
		}
		for _, exposure := range topology.BoundaryExposures {
			if exposure.Producer.ID == producerID && exposure.Output.ID == edge.Producer.ID {
				return true
			}
		}
	}
	return false
}
