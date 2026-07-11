package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkEventChainIntegrity(c *checkerContext) []Finding {
	return c.eventWarningsByCheck("event_chain_integrity")
}

func checkEventConsumerExists(c *checkerContext) []Finding {
	return c.eventWarningsByCheck("event_consumer_exists")
}

func checkEventProducerExists(c *checkerContext) []Finding {
	return c.eventWarningsByCheck("event_producer_exists")
}

func checkLegacyQualifiedSubscription(c *checkerContext) []Finding {
	return c.eventWarningsByCheck("legacy_qualified_subscription")
}

func checkEventCycleDetection(c *checkerContext) []Finding { return c.eventCycleDetection() }

func (c *checkerContext) eventWarningsByCheck(checkID string) []Finding {
	items := c.eventWarnings()
	out := make([]Finding, 0)
	for _, finding := range items {
		if finding.CheckID == checkID {
			out = append(out, finding)
		}
	}
	return out
}

func (c *checkerContext) eventWarnings() []Finding {
	if c.eventWarningLoaded {
		return c.eventWarningFindings
	}
	c.eventWarningLoaded = true
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	topology := routingtopology.Build(c.source)
	stagedBundle := bundleUsesAuthoredStages(c.source)
	for _, subscription := range topology.LegacyQualifiedSubscriptions {
		message := fmt.Sprintf("legacy qualified subscription '%s' at %s still delivers at runtime but is outside canonical same-flow pub/sub and pin/connect topology; migrate to pins/connect", subscription.Consumer.Event.Authored, subscription.AuthoredLocation)
		remediation := subscription.Migration
		evidence := []string{fmt.Sprintf("legacy qualified subscription %q at %q", subscription.Consumer.Event.Authored, subscription.AuthoredLocation)}
		if stagedBundle {
			c.eventWarningFindings = append(c.eventWarningFindings, NewHardInvalidityFinding("legacy_qualified_subscription", subscription.AuthoredLocation, message, remediation, evidence...))
		} else {
			c.eventWarningFindings = append(c.eventWarningFindings, Finding{
				CheckID:     "legacy_qualified_subscription",
				Severity:    SeveritySemanticDriftWarn,
				Message:     message,
				Location:    subscription.AuthoredLocation,
				Remediation: remediation,
				Evidence:    evidence,
			})
		}
	}
	emitted := topologyWarningEndpoints(census.Producers(), true)
	subscribed := topologyWarningEndpoints(append(census.Consumers(), census.InputPins()...), false)
	for _, key := range sortedSetKeysLocal(emitted) {
		entry := emitted[key]
		ref := entry.Event
		if !ref.HasSchema {
			if strings.HasPrefix(ref.DisplayName(), "timer.") || strings.HasPrefix(ref.DisplayName(), "platform.") {
				continue
			}
			c.eventWarningFindings = append(c.eventWarningFindings, Finding{
				CheckID:  "event_chain_integrity",
				Severity: "warning",
				Message:  fmt.Sprintf("'%s' emitted but no schema in events.yaml", ref.DisplayName()),
				Location: ref.DisplayName(),
			})
			continue
		}
		if topologyRoutesProducer(topology, entry.ID) || eventHasExternalConsumerLocal(ref.Entry) {
			continue
		}
		if legacy := legacyQualifiedConsumersForEvent(topology, ref.Canonical); len(legacy) > 0 {
			location := legacy[0].AuthoredLocation
			message := fmt.Sprintf("'%s' has no canonical consumer (same-flow subscriber or connected pin); legacy qualified subscription '%s' at %s still delivers at runtime; migrate to pins/connect", ref.Canonical, legacy[0].Consumer.Event.Authored, location)
			remediation := fmt.Sprintf("Declare output/input pins and a connect for %s, then replace every legacy qualified subscription with a flow-local subscription.", ref.Canonical)
			evidence := make([]string, 0, len(legacy))
			for _, subscription := range legacy {
				evidence = append(evidence, fmt.Sprintf("legacy qualified subscription %q at %q", subscription.Consumer.Event.Authored, subscription.AuthoredLocation))
			}
			if stagedBundle {
				c.eventWarningFindings = append(c.eventWarningFindings, NewHardInvalidityFinding("event_consumer_exists", ref.Canonical, message, remediation, evidence...))
			} else {
				c.eventWarningFindings = append(c.eventWarningFindings, Finding{
					CheckID:     "event_consumer_exists",
					Severity:    SeveritySemanticDriftWarn,
					Message:     message,
					Location:    ref.Canonical,
					Remediation: remediation,
					Evidence:    evidence,
				})
			}
			continue
		}
		c.eventWarningFindings = append(c.eventWarningFindings, Finding{
			CheckID:  "event_consumer_exists",
			Severity: "warning",
			Message:  fmt.Sprintf("'%s' emitted but nobody subscribes", ref.Canonical),
			Location: ref.Canonical,
		})
	}
	for _, key := range sortedSetKeysLocal(subscribed) {
		entry := subscribed[key]
		ref := entry.Event
		if !ref.HasSchema {
			continue
		}
		if len(census.MatchingProducers(ref.FlowID, ref.Authored)) > 0 || topologyRoutesConsumer(topology, entry.ID) {
			continue
		}
		if runtimecontracts.PlatformEventCatalogContains(c.source.PlatformSpec(), ref.Canonical) {
			continue
		}
		if resolution, ok := c.resolveDeclaredInputProducerSource(ref.FlowID, ref.Authored); ok {
			if resolution.HasEvidence() {
				continue
			}
		} else if nonInputEventMetadataProducerSource(ref.Entry) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(ref.Entry.SwarmStatus()), "planned") {
			continue
		}
		c.eventWarningFindings = append(c.eventWarningFindings, Finding{
			CheckID:  "event_producer_exists",
			Severity: "warning",
			Message:  fmt.Sprintf("'%s' subscribed but nobody emits", ref.Canonical),
			Location: ref.Canonical,
		})
	}
	return c.eventWarningFindings
}

func legacyQualifiedConsumersForEvent(topology routingtopology.Topology, canonical string) []routingtopology.LegacyQualifiedSubscription {
	canonical = strings.TrimSpace(canonical)
	out := make([]routingtopology.LegacyQualifiedSubscription, 0)
	for _, subscription := range topology.LegacyQualifiedSubscriptions {
		if strings.TrimSpace(subscription.Event.Canonical) == canonical {
			out = append(out, subscription)
		}
	}
	return out
}

func topologyWarningEndpoints(endpoints []semanticview.AuthoredEventEndpoint, producers bool) map[string]semanticview.AuthoredEventEndpoint {
	out := map[string]semanticview.AuthoredEventEndpoint{}
	for _, endpoint := range endpoints {
		if producers && (endpoint.Kind == semanticview.EventEndpointExternal || endpoint.Kind == semanticview.EventEndpointPlatform) {
			continue
		}
		if !producers && (endpoint.Kind == semanticview.EventEndpointExternal || endpoint.Kind == semanticview.EventEndpointTimer) {
			continue
		}
		if endpoint.Pattern || strings.TrimSpace(endpoint.Event.DisplayName()) == "" {
			continue
		}
		key := strings.TrimSpace(endpoint.FlowID) + "::" + endpoint.Event.DisplayName()
		if _, exists := out[key]; !exists {
			out[key] = endpoint
		}
	}
	return out
}

func topologyRoutesProducer(topology routingtopology.Topology, endpointID string) bool {
	for _, edge := range topology.Edges {
		if edge.Producer.ID == endpointID {
			return true
		}
	}
	for _, exposure := range topology.BoundaryExposures {
		if exposure.Producer.ID != endpointID {
			continue
		}
		if strings.TrimSpace(exposure.Output.FlowID) == "" {
			return true
		}
		for _, edge := range topology.Edges {
			if edge.Scope == routingtopology.DeliveryScopeInterFlowConnect && edge.Producer.ID == exposure.Output.ID {
				return true
			}
		}
	}
	return false
}

func topologyRoutesConsumer(topology routingtopology.Topology, endpointID string) bool {
	for _, edge := range topology.Edges {
		if edge.Consumer.ID == endpointID && edge.Scope == routingtopology.DeliveryScopeInterFlowConnect {
			return true
		}
		if edge.Consumer.ID != endpointID || edge.Scope != routingtopology.DeliveryScopeTypedPubSub || edge.Producer.Direction != semanticview.EventEndpointInputPin {
			continue
		}
		for _, upstream := range topology.Edges {
			if upstream.Scope == routingtopology.DeliveryScopeInterFlowConnect && upstream.Consumer.ID == edge.Producer.ID {
				return true
			}
		}
	}
	return false
}

func eventHasExternalConsumerLocal(entry runtimecontracts.EventCatalogEntry) bool {
	return len(entry.SwarmConsumer()) > 0
}

func (c *checkerContext) eventCycleDetection() []Finding {
	if c.cycleLoaded {
		return c.cycleFindings
	}
	c.cycleLoaded = true
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(c.source).Producers() {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || strings.TrimSpace(endpoint.HandlerEvent) == "" {
			continue
		}
		trigger := semanticview.ResolveFlowEventProof(c.source, endpoint.FlowID, endpoint.HandlerEvent).EventKey()
		if trigger != "" && endpoint.Event.EventKey() == trigger {
			c.cycleFindings = append(c.cycleFindings, Finding{
				CheckID:  "event_cycle_detection",
				Severity: "error",
				Message:  fmt.Sprintf("node %s handler %s emits its own trigger event", endpoint.NodeID, trigger),
				Location: endpoint.NodeID,
			})
		}
	}
	if err := detectEventCyclesSemanticModel(c.source); err != nil {
		c.cycleFindings = append(c.cycleFindings, Finding{
			CheckID:  "event_cycle_detection",
			Severity: "error",
			Message:  err.Error(),
			Location: "global",
		})
	}
	return uniqueFindings(c.cycleFindings)
}

func detectEventCyclesSemanticModel(source semanticview.Source) error {
	if source == nil {
		return nil
	}
	graph := map[string]map[string]struct{}{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || strings.TrimSpace(endpoint.HandlerEvent) == "" || endpoint.Pattern {
			continue
		}
		trigger := semanticview.ResolveFlowEventProof(source, endpoint.FlowID, endpoint.HandlerEvent).EventKey()
		emitted := endpoint.Event.EventKey()
		if trigger == "" || emitted == "" || strings.Contains(emitted, "*") || emitted == trigger {
			continue
		}
		if graph[trigger] == nil {
			graph[trigger] = map[string]struct{}{}
		}
		graph[trigger][emitted] = struct{}{}
	}
	cycles := workflowFindEventCyclesLocal(graph)
	if len(cycles) == 0 {
		return nil
	}
	return fmt.Errorf("EVENT-CYCLE: node handler emit cycle: %s", strings.Join(cycles[0], " -> "))
}
