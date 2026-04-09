package bootverify

import (
	"fmt"
	"strings"

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

	eventsEmitted := map[string]struct{}{}
	for _, scope := range c.source.FlowScopes() {
		if eventType := strings.TrimSpace(scope.AutoEmitEvent); eventType != "" {
			eventsEmitted[eventType] = struct{}{}
		}
	}
	for _, node := range c.source.NodeEntries() {
		for _, handler := range node.EventHandlers {
			for _, emitted := range handlerEmits(handler) {
				emitted = strings.TrimSpace(emitted)
				if emitted != "" {
					eventsEmitted[emitted] = struct{}{}
				}
			}
		}
	}
	for _, agent := range c.source.AgentEntries() {
		for _, eventType := range agent.EmitEvents {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" {
				eventsEmitted[eventType] = struct{}{}
			}
		}
	}

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
			entry, ok := c.source.EventEntry(eventType)
			if !ok {
				continue
			}
			if _, emitted := eventsEmitted[eventType]; emitted {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(entry.Source), "external") || strings.EqualFold(strings.TrimSpace(entry.Status), "planned") {
				continue
			}
			c.inputPinFindings = append(c.inputPinFindings, Finding{
				CheckID:  "input_pin_wiring",
				Severity: "warning",
				Message:  fmt.Sprintf("flow %s input pin %s has no emitter", flowID, eventType),
				Location: flowID,
			})
		}
	}

	return c.inputPinFindings
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
				if isBackpropEvent(eventType) {
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
