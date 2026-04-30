package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/eventidentity"
	"swarm/internal/runtime/semanticview"
)

func checkWritePinOwnershipValidation(c *checkerContext) []Finding { return c.writePinOwnership() }
func checkInputPinWiring(c *checkerContext) []Finding              { return c.inputPinWiring() }
func checkCrossFlowPinAmbiguityValidation(c *checkerContext) []Finding {
	return c.crossFlowPinAmbiguityValidation()
}
func checkFlowBoundaryCreateEntityValidation(c *checkerContext) []Finding {
	return c.flowBoundaryCreateEntityValidation()
}

func (c *checkerContext) writePinOwnership() []Finding {
	if c.writePinLoaded {
		return c.writePinFindings
	}
	c.writePinLoaded = true
	pins := map[string]struct{}{}
	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, pin := range c.source.FlowWritePins(flowID) {
			pin = strings.TrimSpace(pin)
			if pin != "" {
				pins[pin] = struct{}{}
			}
		}
	}
	for _, pin := range sortedSetKeysLocal(pins) {
		owners := c.source.WritePinOwners(pin)
		if len(owners) <= 1 {
			continue
		}
		c.writePinFindings = append(c.writePinFindings, Finding{
			CheckID:  "write_pin_ownership_validation",
			Severity: "error",
			Message:  fmt.Sprintf("write pin %s is owned by multiple flows: %s", pin, strings.Join(owners, ", ")),
			Location: strings.TrimSpace(pin),
		})
	}
	return c.writePinFindings
}

func (c *checkerContext) inputPinWiring() []Finding {
	if c.inputPinLoaded {
		return c.inputPinFindings
	}
	c.inputPinLoaded = true

	flowScopedNodes, flowScopedAgents := inputPinRootParticipants(c.source)

	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			producerPaths := c.inputPinProducerPaths(flowID, eventType, flowScopedNodes, flowScopedAgents)
			if producerPaths.hasAny() {
				continue
			}
			c.inputPinFindings = append(c.inputPinFindings, Finding{
				CheckID:  "input_pin_wiring",
				Severity: "warning",
				Message:  producerPaths.message(flowID, eventType),
				Location: flowID,
			})
		}
	}

	return c.inputPinFindings
}

type inputPinProducerPaths struct {
	siblingFlowOutputPin string
	rootAgentEmit        string
	rootNodeHandlerEmit  string
	platformEventCatalog string
	externalSource       string
	sameFlowTimer        string
}

func (p inputPinProducerPaths) hasAny() bool {
	for _, detail := range []string{
		p.siblingFlowOutputPin,
		p.rootAgentEmit,
		p.rootNodeHandlerEmit,
		p.platformEventCatalog,
		p.externalSource,
		p.sameFlowTimer,
	} {
		if !strings.HasPrefix(strings.TrimSpace(detail), "not ") {
			return true
		}
	}
	return false
}

func (p inputPinProducerPaths) message(flowID, eventType string) string {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	return fmt.Sprintf(
		"Flow %s declares input pin event %s but no producer path was found in the authored bundle.\n\nChecked producer paths:\n- Sibling flow output pin: %s\n- Root agent emit_events: %s\n- Root node handler emits: %s\n- Platform event catalog: %s\n- External source metadata (swarm.source): %s\n- Same-flow timer declaration: %s\n\nFix one of:\n- Add %s to a producing flow's pins.outputs.events\n- Add %s to a root agent's emit_events\n- Add swarm.source: external to %s's events.yaml entry if produced externally",
		flowID,
		eventType,
		p.siblingFlowOutputPin,
		p.rootAgentEmit,
		p.rootNodeHandlerEmit,
		p.platformEventCatalog,
		p.externalSource,
		p.sameFlowTimer,
		eventType,
		eventType,
		eventType,
	)
}

func inputPinRootParticipants(source semanticview.Source) (map[string]struct{}, map[string]struct{}) {
	flowScopedNodes := map[string]struct{}{}
	flowScopedAgents := map[string]struct{}{}
	if source == nil {
		return flowScopedNodes, flowScopedAgents
	}
	for _, scope := range source.FlowScopes() {
		for nodeID := range scope.Nodes {
			nodeID = strings.TrimSpace(nodeID)
			if nodeID != "" {
				flowScopedNodes[nodeID] = struct{}{}
			}
		}
		for agentID := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			if agentID != "" {
				flowScopedAgents[agentID] = struct{}{}
			}
		}
	}
	return flowScopedNodes, flowScopedAgents
}

func (c *checkerContext) inputPinProducerPaths(flowID, eventType string, flowScopedNodes, flowScopedAgents map[string]struct{}) inputPinProducerPaths {
	localEvent := eventidentity.Normalize(eventType)
	canonicalEvent := eventidentity.Normalize(c.source.ResolveFlowEventReference(flowID, eventType))
	return inputPinProducerPaths{
		siblingFlowOutputPin: c.inputPinSiblingFlowOutputPath(flowID, localEvent),
		rootAgentEmit:        c.inputPinRootAgentPath(localEvent, canonicalEvent, flowScopedAgents),
		rootNodeHandlerEmit:  c.inputPinRootNodeHandlerPath(localEvent, canonicalEvent, flowScopedNodes),
		platformEventCatalog: inputPinPlatformEventPath(c.source.PlatformSpec(), localEvent),
		externalSource:       c.inputPinExternalSourcePath(flowID, eventType),
		sameFlowTimer:        c.inputPinSameFlowTimerPath(flowID, eventType, canonicalEvent),
	}
}

func (c *checkerContext) inputPinSiblingFlowOutputPath(flowID, localEvent string) string {
	matches := map[string]struct{}{}
	for otherFlowID := range c.source.FlowSchemaEntries() {
		otherFlowID = strings.TrimSpace(otherFlowID)
		if otherFlowID == "" || otherFlowID == strings.TrimSpace(flowID) {
			continue
		}
		for _, output := range c.source.FlowOutputEvents(otherFlowID) {
			if eventidentity.Normalize(output) == localEvent {
				matches[otherFlowID] = struct{}{}
				break
			}
		}
	}
	if len(matches) == 0 {
		return "not found"
	}
	flows := sortedSetKeysLocal(matches)
	if len(flows) == 1 {
		return fmt.Sprintf("found in flow %s", flows[0])
	}
	return fmt.Sprintf("found in flows %s", strings.Join(flows, ", "))
}

func (c *checkerContext) inputPinRootAgentPath(localEvent, canonicalEvent string, flowScopedAgents map[string]struct{}) string {
	matches := map[string]struct{}{}
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := flowScopedAgents[agentID]; ok {
			continue
		}
		for _, emitted := range agent.EmitEvents {
			if inputPinRootProducerMatches(c.source, localEvent, canonicalEvent, emitted) {
				matches[agentID] = struct{}{}
				break
			}
		}
	}
	if len(matches) == 0 {
		return "not found"
	}
	agents := sortedSetKeysLocal(matches)
	if len(agents) == 1 {
		return fmt.Sprintf("found on agent %s", agents[0])
	}
	return fmt.Sprintf("found on agents %s", strings.Join(agents, ", "))
}

func (c *checkerContext) inputPinRootNodeHandlerPath(localEvent, canonicalEvent string, flowScopedNodes map[string]struct{}) string {
	matches := map[string]struct{}{}
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := flowScopedNodes[nodeID]; ok {
			continue
		}
		for handlerEvent, handler := range node.EventHandlers {
			for _, emitted := range handlerEmits(handler) {
				if inputPinRootProducerMatches(c.source, localEvent, canonicalEvent, emitted) {
					matches[fmt.Sprintf("%s handler %s", nodeID, strings.TrimSpace(handlerEvent))] = struct{}{}
					break
				}
			}
		}
	}
	if len(matches) == 0 {
		return "not found"
	}
	handlers := sortedSetKeysLocal(matches)
	if len(handlers) == 1 {
		return fmt.Sprintf("found on %s", handlers[0])
	}
	return fmt.Sprintf("found on %s", strings.Join(handlers, ", "))
}

func inputPinPlatformEventPath(platform runtimecontracts.PlatformSpecDocument, localEvent string) string {
	for eventName := range platform.PlatformEvents.Catalog {
		if eventidentity.Normalize(eventName) == localEvent {
			return "matched"
		}
	}
	return "not matched"
}

func (c *checkerContext) inputPinExternalSourcePath(flowID, eventType string) string {
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return "not found"
	}
	if entry, ok := inputPinRootEventEntry(bundle, flowID, eventType); ok && inputPinExternalSourceMatch(entry.SwarmSource()) {
		return "found"
	}
	if entry, ok := inputPinFlowEventEntry(bundle, flowID, eventType); ok && inputPinExternalSourceMatch(entry.SwarmSource()) {
		return "found"
	}
	return "not found"
}

func inputPinExternalSourceMatch(source string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "external")
}

func inputPinRootEventEntry(bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	if bundle == nil {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	for _, candidate := range []string{
		eventidentity.Normalize(eventType),
		eventidentity.Normalize(bundle.ResolveFlowEventReference(flowID, eventType)),
	} {
		if candidate == "" {
			continue
		}
		if entry, ok := bundle.Events[candidate]; ok {
			return entry, true
		}
	}
	return runtimecontracts.EventCatalogEntry{}, false
}

func inputPinFlowEventEntry(bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	if bundle == nil {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	candidate := eventidentity.Normalize(eventType)
	if candidate == "" {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	if entry, ok := flowView.Events[candidate]; ok {
		return entry, true
	}
	return runtimecontracts.EventCatalogEntry{}, false
}

func (c *checkerContext) inputPinSameFlowTimerPath(flowID, eventType, canonicalEvent string) string {
	matches := map[string]struct{}{}
	for _, timer := range c.source.WorkflowTimers() {
		if strings.TrimSpace(timer.FlowID) != strings.TrimSpace(flowID) {
			continue
		}
		if !inputPinTimerMatches(c.source, flowID, eventType, canonicalEvent, timer.Event) {
			continue
		}
		label := fmt.Sprintf("node %s", strings.TrimSpace(timer.NodeID))
		if timerID := strings.TrimSpace(timer.ID); timerID != "" {
			label = fmt.Sprintf("%s timer %s", label, timerID)
		}
		matches[label] = struct{}{}
	}
	if len(matches) == 0 {
		return "not found"
	}
	timers := sortedSetKeysLocal(matches)
	if len(timers) == 1 {
		return fmt.Sprintf("found on %s", timers[0])
	}
	return fmt.Sprintf("found on %s", strings.Join(timers, ", "))
}

func inputPinRootProducerMatches(source semanticview.Source, localEvent, canonicalEvent, candidate string) bool {
	candidate = eventidentity.Normalize(candidate)
	if candidate == "" {
		return false
	}
	if candidate == localEvent {
		return true
	}
	if canonicalEvent == "" || source == nil {
		return false
	}
	return eventidentity.Normalize(source.ResolveFlowEventReference("", candidate)) == canonicalEvent
}

func inputPinTimerMatches(source semanticview.Source, flowID, eventType, canonicalEvent, candidate string) bool {
	if source == nil {
		return false
	}
	if canonicalEvent != "" && eventidentity.Normalize(source.ResolveFlowEventReference(flowID, candidate)) == canonicalEvent {
		return true
	}
	return eventidentity.Normalize(candidate) == eventidentity.Normalize(eventType)
}

func (c *checkerContext) crossFlowPinAmbiguityValidation() []Finding {
	if c.crossFlowPinAmbiguityLoaded {
		return c.crossFlowPinAmbiguityFindings
	}
	c.crossFlowPinAmbiguityLoaded = true
	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			producers := c.source.ResolveFlowInputAutoWire(flowID, eventType).ProducerFlows
			if len(producers) <= 1 {
				continue
			}
			if flowHasScopedInputEscapeHatch(c.source, flowID, eventType) {
				continue
			}
			c.crossFlowPinAmbiguityFindings = append(c.crossFlowPinAmbiguityFindings, Finding{
				CheckID:  "cross_flow_pin_ambiguity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s input pin %s is ambiguous across producer flows %s; add an explicit scoped subscription escape hatch", flowID, eventType, strings.Join(producers, ", ")),
				Location: flowID,
			})
		}
	}
	return c.crossFlowPinAmbiguityFindings
}

func flowHasScopedInputEscapeHatch(source semanticview.Source, flowID, eventType string) bool {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if source == nil || flowID == "" || eventType == "" {
		return false
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return false
	}
	for _, node := range scope.Nodes {
		for _, sub := range eventidentity.NormalizeList(node.SubscribesTo) {
			if strings.Contains(sub, "/") && strings.HasSuffix(sub, "/"+eventType) {
				return true
			}
		}
	}
	for _, agent := range scope.Agents {
		for _, sub := range eventidentity.NormalizeList(agent.Subscriptions) {
			if strings.Contains(sub, "/") && strings.HasSuffix(sub, "/"+eventType) {
				return true
			}
		}
		for _, sub := range eventidentity.NormalizeList(agent.SubscribesTo) {
			if strings.Contains(sub, "/") && strings.HasSuffix(sub, "/"+eventType) {
				return true
			}
		}
	}
	return false
}

func (c *checkerContext) flowBoundaryCreateEntityValidation() []Finding {
	if c.flowBoundaryCreateEntityLoaded {
		return c.flowBoundaryCreateEntityFindings
	}
	c.flowBoundaryCreateEntityLoaded = true
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			continue
		}
		if strings.TrimSpace(schema.InitialState) == "" {
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
			nodeID = strings.TrimSpace(nodeID)
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := inputs[eventType]; !ok {
					continue
				}
				if handler.CreateEntity {
					continue
				}
				c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
					CheckID:  "flow_boundary_create_entity_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input pin handler %s on node %s must declare create_entity: true", flowID, eventType, nodeID),
					Location: flowID,
				})
			}
		}
	}
	return c.flowBoundaryCreateEntityFindings
}

func normalizeStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}
