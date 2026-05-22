package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepinrouting "swarm/internal/runtime/core/pinrouting"
	"swarm/internal/runtime/semanticview"
)

type pinEmitSite struct {
	FlowID    string
	NodeID    string
	EventType string
	Site      string
	Spec      runtimecontracts.EmitSpec
	Handler   runtimecontracts.SystemNodeEventHandler
}

func checkPinTargetResolution(c *checkerContext) []Finding {
	findings := []Finding{}
	for _, site := range pinRoutingEmitSites(c.source) {
		if !runtimepinrouting.PinDeclaredOutput(c.source, site.FlowID, site.Spec.EventType()) {
			continue
		}
		structuralParent := pinRoutingStructuralParentRouteEligible(c.source, site.FlowID)
		if failure := runtimepinrouting.ValidateTargetSpec(c.source, site.FlowID, site.Spec, structuralParent); failure != "" {
			findings = append(findings, pinTargetFinding(site, string(failure)))
			continue
		}
		if site.Spec.Target.Normalized().Kind == runtimecontracts.EmitTargetKindSender && pinRoutingEventExternalSource(c.source, site.FlowID, site.EventType) {
			findings = append(findings, pinTargetFinding(site, "target_sender_empty_source"))
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

func pinRoutingEmitSites(source semanticview.Source) []pinEmitSite {
	if source == nil {
		return nil
	}
	out := []pinEmitSite{}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		scope, ok := source.FlowScopeByID(flowID)
		if !ok {
			continue
		}
		for nodeID, node := range scope.Nodes {
			nodeID = strings.TrimSpace(nodeID)
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				appendSite := func(site string, spec runtimecontracts.EmitSpec) {
					if spec.Empty() {
						return
					}
					out = append(out, pinEmitSite{
						FlowID:    flowID,
						NodeID:    nodeID,
						EventType: eventType,
						Site:      site,
						Spec:      spec,
						Handler:   handler,
					})
				}
				appendSite("handler.emit", handler.Emit)
				for _, rule := range handler.Rules {
					appendSite("handler.rules.emit", rule.Emit)
					if rule.FanOut != nil {
						appendSite("handler.rules.fan_out.emit", rule.FanOut.Emit)
					}
				}
				for _, rule := range handler.OnComplete {
					appendSite("handler.on_complete.emit", rule.Emit)
					if rule.FanOut != nil {
						appendSite("handler.on_complete.fan_out.emit", rule.FanOut.Emit)
					}
				}
				if handler.Accumulate != nil {
					for _, rule := range handler.Accumulate.OnComplete {
						appendSite("handler.accumulate.on_complete.emit", rule.Emit)
						if rule.FanOut != nil {
							appendSite("handler.accumulate.on_complete.fan_out.emit", rule.FanOut.Emit)
						}
					}
					if handler.Accumulate.OnTimeout != nil {
						appendSite("handler.accumulate.on_timeout.emit", handler.Accumulate.OnTimeout.Emit)
					}
				}
				if handler.FanOut != nil {
					appendSite("handler.fan_out.emit", handler.FanOut.Emit)
				}
			}
		}
	}
	return out
}

func pinTargetFinding(site pinEmitSite, reason string) Finding {
	return Finding{
		CheckID:  "pin_target_resolution",
		Severity: "error",
		Message:  fmt.Sprintf("flow %s %s on node %s emits pin-declared output %s without valid target mechanism: %s", site.FlowID, site.Site, site.NodeID, site.Spec.EventType(), reason),
		Location: site.FlowID,
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
		if site.Spec.HasTarget() || (structuralParent && !site.Spec.Broadcast) {
			targeted++
		}
	}
	return producers > 0 && targeted == producers
}
