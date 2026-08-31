package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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
	connectGraph := runtimepinrouting.CompileConnectGraph(c.source)
	for _, subscription := range census.InvalidAuthoredSubscriptions() {
		location := invalidAuthoredSubscriptionLocation(subscription)
		message := subscription.Admission.Message()
		remediation := "Use a receiver-local exact event name. Declare output/input pins and schema.yaml connect at the nearest common ancestor for delivery across a flow boundary."
		evidence := []string{fmt.Sprintf("rejected authored subscription %q at %q (%s)", subscription.Consumer.Event.Authored, location, subscription.Admission.Failure())}
		c.eventWarningFindings = append(c.eventWarningFindings, NewHardInvalidityFinding("legacy_qualified_subscription", location, message, remediation, evidence...))
	}
	emitted := topologyWarningEndpoints(census.Producers(), true)
	subscribed := topologyWarningEndpoints(append(census.Consumers(), census.InputPins()...), false)
	generatedActivityEvents := generatedActivityResultEventNamesLocal(c.source)
	for _, key := range sortedSetKeysLocal(emitted) {
		entry := emitted[key]
		ref := entry.Event
		if _, generated := generatedActivityEvents[eventidentity.Normalize(ref.Canonical)]; generated {
			// The generated schema and durable attempt journal own these results;
			// authors neither declare their schemas nor need to subscribe to them.
			continue
		}
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
		if runtimepinrouting.OutputHarnessSink(c.source, ref.FlowID, ref.Authored) {
			continue
		}
		if topologyRoutesProducer(topology, connectGraph, entry) || eventHasExternalConsumerLocal(ref.Entry) {
			continue
		}
		if invalid := invalidAuthoredConsumersForEvent(census, ref.Canonical); len(invalid) > 0 {
			location := invalidAuthoredSubscriptionLocation(invalid[0])
			message := fmt.Sprintf("'%s' has no canonical consumer (same-flow subscriber or connected pin); rejected subscription '%s' at %s cannot provide delivery authority", ref.Canonical, invalid[0].Consumer.Event.Authored, location)
			remediation := fmt.Sprintf("Declare output/input pins and a connect for %s, then replace every legacy qualified subscription with a flow-local subscription.", ref.Canonical)
			evidence := make([]string, 0, len(invalid))
			for _, subscription := range invalid {
				evidence = append(evidence, fmt.Sprintf("rejected authored subscription %q at %q", subscription.Consumer.Event.Authored, invalidAuthoredSubscriptionLocation(subscription)))
			}
			c.eventWarningFindings = append(c.eventWarningFindings, NewHardInvalidityFinding("event_consumer_exists", ref.Canonical, message, remediation, evidence...))
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
		if len(census.MatchingProducers(ref.FlowID, ref.Authored)) > 0 || topologyRoutesConsumer(topology, connectGraph, entry) {
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

func generatedActivityResultEventNamesLocal(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for _, site := range runtimecontracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node)) {
			results := runtimecontracts.ActivityResultEventsForSite(site)
			eventTypes := []string{results.SuccessEvent, results.FailureEvent}
			if site.Spec.Approval != nil {
				eventTypes = append(eventTypes, results.RevisionRequested, results.Rejected)
			}
			for _, eventType := range eventTypes {
				if normalized := eventidentity.Normalize(eventType); normalized != "" {
					out[normalized] = struct{}{}
				}
			}
		}
	}
	return out
}

func invalidAuthoredConsumersForEvent(census semanticview.AuthoredEventEndpointCensus, canonical string) []semanticview.InvalidAuthoredSubscription {
	canonical = strings.TrimSpace(canonical)
	out := make([]semanticview.InvalidAuthoredSubscription, 0)
	for _, subscription := range census.InvalidAuthoredSubscriptions() {
		if strings.TrimSpace(subscription.Consumer.Event.Canonical) == canonical {
			out = append(out, subscription)
		}
	}
	return out
}

func invalidAuthoredSubscriptionLocation(subscription semanticview.InvalidAuthoredSubscription) string {
	file := strings.TrimSpace(subscription.Consumer.SourceFile)
	if file != "" && subscription.Consumer.SourceLine > 0 {
		return fmt.Sprintf("%s:%d", file, subscription.Consumer.SourceLine)
	}
	if file != "" {
		return file
	}
	return strings.TrimSpace(subscription.Consumer.SourceLocation)
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

func topologyRoutesProducer(topology routingtopology.Topology, graph runtimepinrouting.CompiledConnectGraph, endpoint semanticview.AuthoredEventEndpoint) bool {
	if graph.HasProducer(endpoint.FlowID, events.EventType(endpoint.Event.Authored)) || graph.HasProducer(endpoint.FlowID, events.EventType(endpoint.Event.Canonical)) {
		return true
	}
	for _, edge := range topology.Edges {
		if edge.Scope == routingtopology.DeliveryScopeTypedPubSub && edge.Producer.ID == endpoint.ID {
			return true
		}
	}
	for _, exposure := range topology.BoundaryExposures {
		if exposure.Producer.ID != endpoint.ID {
			continue
		}
		if strings.TrimSpace(exposure.Output.FlowID) == "" {
			return true
		}
		if graph.HasProducer(exposure.Output.FlowID, events.EventType(exposure.Output.Event.Authored)) ||
			graph.HasProducer(exposure.Output.FlowID, events.EventType(exposure.Output.Event.Canonical)) {
			return true
		}
	}
	return false
}

func topologyRoutesConsumer(topology routingtopology.Topology, graph runtimepinrouting.CompiledConnectGraph, endpoint semanticview.AuthoredEventEndpoint) bool {
	if graph.HasConsumer(endpoint.FlowID, events.EventType(endpoint.Event.Authored)) || graph.HasConsumer(endpoint.FlowID, events.EventType(endpoint.Event.Canonical)) {
		return true
	}
	for _, edge := range topology.Edges {
		if edge.Consumer.ID != endpoint.ID || edge.Scope != routingtopology.DeliveryScopeTypedPubSub {
			continue
		}
		if edge.Producer.Direction != semanticview.EventEndpointInputPin {
			return true
		}
		if graph.HasConsumer(edge.Producer.FlowID, events.EventType(edge.Producer.Event.Authored)) ||
			graph.HasConsumer(edge.Producer.FlowID, events.EventType(edge.Producer.Event.Canonical)) {
			return true
		}
	}
	return false
}

func eventHasExternalConsumerLocal(entry runtimecontracts.EventCatalogEntry) bool {
	return entry.AcceptedConsumerBoundary() == runtimecontracts.EventConsumerBoundaryExternal
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
