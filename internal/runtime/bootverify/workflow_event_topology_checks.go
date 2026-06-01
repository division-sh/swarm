package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	emittedRefs := map[string]semanticview.FlowEventProof{}
	subscribedRefs := map[string]semanticview.FlowEventProof{}
	subscriptionPatterns := map[string]eventPatternLocal{}
	for _, scope := range c.source.FlowScopes() {
		if eventType := strings.TrimSpace(scope.AutoEmitEvent); eventType != "" {
			addEventProofLocal(emittedRefs, c.source, scope.ID, eventType)
		}
		for _, eventType := range scope.InputEvents {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if strings.Contains(eventType, "*") {
				addEventPatternLocal(subscriptionPatterns, scope.ID, eventType)
			} else {
				addEventProofLocal(subscribedRefs, c.source, scope.ID, eventType)
			}
		}
		for _, required := range c.source.FlowRequiredAgents(scope.ID) {
			for _, eventType := range required.Emits {
				addEventProofLocal(emittedRefs, c.source, scope.ID, eventType)
			}
			for _, eventType := range required.SubscribesTo {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if strings.Contains(eventType, "*") {
					addEventPatternLocal(subscriptionPatterns, scope.ID, eventType)
				} else {
					addEventProofLocal(subscribedRefs, c.source, scope.ID, eventType)
				}
			}
		}
	}
	for _, required := range c.source.RequiredAgents() {
		for _, eventType := range required.Emits {
			addEventProofLocal(emittedRefs, c.source, "", eventType)
		}
		for _, eventType := range required.SubscribesTo {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if strings.Contains(eventType, "*") {
				addEventPatternLocal(subscriptionPatterns, "", eventType)
			} else {
				addEventProofLocal(subscribedRefs, c.source, "", eventType)
			}
		}
	}
	for nodeID, node := range c.source.NodeEntries() {
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range node.SubscribesTo {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if strings.Contains(eventType, "*") {
				addEventPatternLocal(subscriptionPatterns, flowID, eventType)
			} else {
				addEventProofLocal(subscribedRefs, c.source, flowID, eventType)
			}
		}
		for _, eventType := range c.source.NodeHandlerSubscriptions(nodeID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if strings.Contains(eventType, "*") {
				addEventPatternLocal(subscriptionPatterns, flowID, eventType)
			} else {
				addEventProofLocal(subscribedRefs, c.source, flowID, eventType)
			}
			handler, ok := c.source.NodeEventHandler(nodeID, eventType)
			if !ok {
				continue
			}
			for _, emitted := range handlerEmits(handler) {
				addEventProofLocal(emittedRefs, c.source, flowID, emitted)
			}
		}
		for _, timer := range node.Timers {
			addEventProofLocal(emittedRefs, c.source, flowID, strings.TrimSpace(timer.Event))
		}
	}
	for agentID, agent := range c.source.AgentEntries() {
		agentSource, _ := c.source.AgentContractSource(agentID)
		flowID := strings.TrimSpace(agentSource.FlowID)
		for _, eventType := range agent.EmitEvents {
			addEventProofLocal(emittedRefs, c.source, flowID, eventType)
		}
		for _, eventType := range append(append([]string{}, agent.Subscriptions...), agent.SubscribesTo...) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if strings.Contains(eventType, "*") {
				addEventPatternLocal(subscriptionPatterns, flowID, eventType)
			} else {
				addEventProofLocal(subscribedRefs, c.source, flowID, eventType)
			}
		}
	}
	for _, key := range sortedSetKeysLocal(emittedRefs) {
		ref := emittedRefs[key]
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
		if eventRefConsumedLocal(c.source, ref.Canonical, subscribedRefs, subscriptionPatterns) || len(c.source.RuntimeEventOwners(ref.Canonical)) > 0 || eventHasExternalConsumerLocal(ref.Entry) || ref.CrossesDeclaredOutputBoundary(c.source) {
			continue
		}
		c.eventWarningFindings = append(c.eventWarningFindings, Finding{
			CheckID:  "event_consumer_exists",
			Severity: "warning",
			Message:  fmt.Sprintf("'%s' emitted but nobody subscribes", ref.Canonical),
			Location: ref.Canonical,
		})
	}
	for _, key := range sortedSetKeysLocal(subscribedRefs) {
		ref := subscribedRefs[key]
		if !ref.HasSchema {
			continue
		}
		if eventRefProducedLocal(c.source, ref, emittedRefs) {
			continue
		}
		if eventProducedExternallyLocal(ref.Entry) || strings.EqualFold(strings.TrimSpace(ref.Entry.SwarmStatus()), "planned") {
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

type eventPatternLocal struct {
	FlowID string
	Base   string
}

func addEventProofLocal(refs map[string]semanticview.FlowEventProof, source semanticview.Source, flowID, eventType string) {
	ref := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	if strings.TrimSpace(ref.DisplayName()) == "" {
		return
	}
	key := strings.TrimSpace(ref.FlowID) + "::" + ref.DisplayName()
	refs[key] = ref
}

func addEventPatternLocal(refs map[string]eventPatternLocal, flowID, eventType string) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return
	}
	ref := eventPatternLocal{Base: eventType, FlowID: strings.TrimSpace(flowID)}
	key := ref.FlowID + "::" + ref.Base
	refs[key] = ref
}

func eventRefConsumedLocal(source semanticview.Source, eventType string, subscribedRefs map[string]semanticview.FlowEventProof, patterns map[string]eventPatternLocal) bool {
	for _, subscribed := range subscribedRefs {
		if source.FlowEventMatches(subscribed.FlowID, subscribed.Authored, eventType) {
			return true
		}
	}
	for _, pattern := range patterns {
		if source.FlowEventMatches(pattern.FlowID, pattern.Base, eventType) {
			return true
		}
	}
	return false
}

func eventRefProducedLocal(source semanticview.Source, ref semanticview.FlowEventProof, emittedRefs map[string]semanticview.FlowEventProof) bool {
	for _, emitted := range emittedRefs {
		if source.FlowEventMatches(ref.FlowID, ref.Authored, emitted.Canonical) {
			return true
		}
	}
	return false
}

func eventHasExternalConsumerLocal(entry runtimecontracts.EventCatalogEntry) bool {
	return len(entry.SwarmConsumer()) > 0
}

func eventProducedExternallyLocal(entry runtimecontracts.EventCatalogEntry) bool {
	if len(entry.SwarmProducer()) > 0 {
		return true
	}
	source := strings.ToLower(strings.TrimSpace(entry.SwarmSource()))
	return strings.HasPrefix(source, "external") || strings.HasPrefix(source, "platform")
}

func (c *checkerContext) eventCycleDetection() []Finding {
	if c.cycleLoaded {
		return c.cycleFindings
	}
	c.cycleLoaded = true
	for nodeID := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range c.source.NodeHandlerSubscriptions(nodeID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			trigger := semanticview.ResolveFlowEventProof(c.source, flowID, eventType).EventKey()
			if trigger == "" {
				continue
			}
			handler, ok := c.source.NodeEventHandler(nodeID, eventType)
			if !ok {
				continue
			}
			for _, emitted := range handlerEmits(handler) {
				emitted = semanticview.ResolveFlowEventProof(c.source, flowID, emitted).EventKey()
				if emitted == "" || emitted != trigger {
					continue
				}
				c.cycleFindings = append(c.cycleFindings, Finding{
					CheckID:  "event_cycle_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s emits its own trigger event", nodeID, trigger),
					Location: nodeID,
				})
			}
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
	for nodeID := range source.NodeEntries() {
		nodeSource, _ := source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range source.NodeHandlerSubscriptions(nodeID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" || strings.Contains(eventType, "*") {
				continue
			}
			trigger := semanticview.ResolveFlowEventProof(source, flowID, eventType).EventKey()
			if trigger == "" {
				continue
			}
			handler, ok := source.NodeEventHandler(nodeID, eventType)
			if !ok {
				continue
			}
			for _, emitted := range handlerEmits(handler) {
				emitted = semanticview.ResolveFlowEventProof(source, flowID, emitted).EventKey()
				if emitted == "" || strings.Contains(emitted, "*") || emitted == trigger {
					continue
				}
				if graph[trigger] == nil {
					graph[trigger] = map[string]struct{}{}
				}
				graph[trigger][emitted] = struct{}{}
			}
		}
	}
	cycles := workflowFindEventCyclesLocal(graph)
	if len(cycles) == 0 {
		return nil
	}
	return fmt.Errorf("EVENT-CYCLE: node handler emit cycle: %s", strings.Join(cycles[0], " -> "))
}
