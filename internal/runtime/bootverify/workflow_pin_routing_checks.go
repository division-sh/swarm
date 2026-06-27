package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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
		if site.Spec.Target.Normalized().Kind == runtimecontracts.EmitTargetKindSender && pinRoutingEventExternalSource(c.source, site.FlowID, site.HandlerEvent) {
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
		if flowID == "" || strings.TrimSpace(schema.InitialState) == "" {
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
				if !pinRoutingAllKnownProducersTargeted(c.source, flowID, eventType) {
					continue
				}
				findings = append(findings, Finding{
					CheckID:  "redundant_in_topology_select_entity",
					Severity: "warning",
					Message:  fmt.Sprintf("flow %s handler %s on node %s declares receiver-side entity acquisition for in-topology pin-routed event", flowID, eventType, nodeID),
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
		if flowID == "" || strings.TrimSpace(schema.InitialState) == "" || strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
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
				if !pinRoutingEventExternalSource(c.source, flowID, eventType) {
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
	seen := map[string]struct{}{}
	sites := []pinRoutingAgentEmitSite{}
	appendAgent := func(flowID, agentID string, entry runtimecontracts.AgentRegistryEntry) {
		flowID = strings.TrimSpace(flowID)
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			agentID = strings.TrimSpace(entry.ID)
		}
		if agentID == "" {
			return
		}
		for _, rawEventType := range entry.EmitEvents {
			eventType := strings.TrimSpace(rawEventType)
			if eventType == "" {
				continue
			}
			key := flowID + "\x00" + agentID + "\x00" + eventType
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			sites = append(sites, pinRoutingAgentEmitSite{
				FlowID:    flowID,
				AgentID:   agentID,
				EventType: eventType,
			})
		}
	}
	rootAgents := source.AgentEntries()
	for _, agentID := range sortedSetKeysLocal(rootAgents) {
		appendAgent("", agentID, rootAgents[agentID])
	}
	flowScopes := append([]semanticview.FlowScope{}, source.FlowScopes()...)
	sort.SliceStable(flowScopes, func(i, j int) bool {
		if strings.TrimSpace(flowScopes[i].ID) != strings.TrimSpace(flowScopes[j].ID) {
			return strings.TrimSpace(flowScopes[i].ID) < strings.TrimSpace(flowScopes[j].ID)
		}
		if strings.TrimSpace(flowScopes[i].Path) != strings.TrimSpace(flowScopes[j].Path) {
			return strings.TrimSpace(flowScopes[i].Path) < strings.TrimSpace(flowScopes[j].Path)
		}
		return strings.TrimSpace(flowScopes[i].PackageKey) < strings.TrimSpace(flowScopes[j].PackageKey)
	})
	for _, scope := range flowScopes {
		for _, agentID := range sortedSetKeysLocal(scope.Agents) {
			appendAgent(scope.ID, agentID, scope.Agents[agentID])
		}
	}
	sort.SliceStable(sites, func(i, j int) bool {
		if sites[i].FlowID != sites[j].FlowID {
			return sites[i].FlowID < sites[j].FlowID
		}
		if sites[i].AgentID != sites[j].AgentID {
			return sites[i].AgentID < sites[j].AgentID
		}
		return sites[i].EventType < sites[j].EventType
	})
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

func pinRoutingEventExternalSource(source semanticview.Source, flowID, eventType string) bool {
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(proof.Entry.SwarmSource())), "external") {
		return true
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	return ok && strings.HasPrefix(strings.ToLower(strings.TrimSpace(entry.SwarmSource())), "external")
}

func pinRoutingAllKnownProducersTargeted(source semanticview.Source, flowID, eventType string) bool {
	canonical := strings.TrimSpace(source.ResolveFlowEventReference(flowID, eventType))
	if canonical == "" {
		return false
	}
	producers := 0
	targeted := 0
	for _, site := range pinRoutingEmitSites(source) {
		if strings.TrimSpace(source.ResolveFlowEventReference(site.FlowID, site.Spec.EventType())) != canonical {
			continue
		}
		if !runtimepinrouting.PinDeclaredOutput(source, site.FlowID, site.Spec.EventType()) {
			continue
		}
		producers++
		structuralParent := pinRoutingStructuralParentRouteEligible(source, site.FlowID)
		if (site.Spec.HasTarget() && runtimepinrouting.ProducerRouteCommonPathFailure(source, site.FlowID, site.Spec.EventType(), site.Spec) == "") ||
			(structuralParent && !site.Spec.Broadcast) {
			targeted++
		}
	}
	return producers > 0 && targeted == producers
}
