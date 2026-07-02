package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkEventMetadataAuthority(c *checkerContext) []Finding {
	return c.eventMetadataAuthority()
}

func (c *checkerContext) eventMetadataAuthority() []Finding {
	if c.eventMetadataAuthorityLoaded {
		return c.eventMetadataAuthorityFindings
	}
	c.eventMetadataAuthorityLoaded = true
	if c.source == nil {
		return nil
	}
	globalNames := eventMetadataInternalActorNames(c.source)
	for _, decl := range deadEventDeclarations(c.source) {
		producerNames, consumerNames := eventMetadataRoleNames(c.source, decl)
		c.eventMetadataAuthorityFindings = append(c.eventMetadataAuthorityFindings, eventMetadataSourceAuthorityFindings(decl, globalNames, producerNames)...)
		c.eventMetadataAuthorityFindings = append(c.eventMetadataAuthorityFindings, eventMetadataListAuthorityFindings(decl, "swarm.producer", decl.Entry.SwarmProducer(), globalNames, producerNames)...)
		c.eventMetadataAuthorityFindings = append(c.eventMetadataAuthorityFindings, eventMetadataListAuthorityFindings(decl, "swarm.consumer", decl.Entry.SwarmConsumer(), globalNames, consumerNames)...)
	}
	return c.eventMetadataAuthorityFindings
}

type eventMetadataNameIndex map[string]string

func (idx eventMetadataNameIndex) add(value, label string) {
	value = strings.TrimSpace(value)
	label = strings.TrimSpace(label)
	if value == "" || label == "" {
		return
	}
	for _, key := range eventMetadataNameKeys(value) {
		if key == "" {
			continue
		}
		if _, exists := idx[key]; exists {
			continue
		}
		idx[key] = label
	}
}

func (idx eventMetadataNameIndex) merge(other eventMetadataNameIndex) {
	for key, label := range other {
		if _, exists := idx[key]; exists {
			continue
		}
		idx[key] = label
	}
}

func (idx eventMetadataNameIndex) match(value string) (string, bool) {
	for _, key := range eventMetadataNameKeys(value) {
		if label, ok := idx[key]; ok {
			return label, true
		}
	}
	return "", false
}

func eventMetadataNameKeys(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	keys := []string{strings.ToLower(value)}
	if normalized := eventidentity.Normalize(value); normalized != "" {
		keys = append(keys, strings.ToLower(normalized))
	}
	return keys
}

func eventMetadataInternalActorNames(source semanticview.Source) eventMetadataNameIndex {
	names := eventMetadataNameIndex{}
	names.add("runtime", "runtime")
	names.add("sys:runtime", "runtime")
	for nodeKey, node := range source.NodeEntries() {
		nodeKey = strings.TrimSpace(nodeKey)
		names.add(nodeKey, fmt.Sprintf("system node %s", nodeKey))
		if id := runtimecontracts.EffectiveSystemNodeID(nodeKey, node); id != "" {
			names.add(id, fmt.Sprintf("system node %s", id))
		}
	}
	for agentKey, agent := range source.AgentEntries() {
		agentKey = strings.TrimSpace(agentKey)
		names.add(agentKey, fmt.Sprintf("agent %s", agentKey))
		names.add(agent.ID, fmt.Sprintf("agent %s", strings.TrimSpace(agent.ID)))
		names.add(agent.Role, fmt.Sprintf("agent role %s", strings.TrimSpace(agent.Role)))
	}
	for _, required := range source.RequiredAgents() {
		names.add(required.Role, fmt.Sprintf("required agent role %s", strings.TrimSpace(required.Role)))
	}
	for _, scope := range source.FlowScopes() {
		eventMetadataAddGlobalFlowTopologyNames(source, names, strings.TrimSpace(scope.ID))
		for _, required := range source.FlowRequiredAgents(scope.ID) {
			names.add(required.Role, fmt.Sprintf("required agent role %s", strings.TrimSpace(required.Role)))
		}
	}
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.RootSchema != nil {
		eventMetadataAddGlobalFlowTopologyNames(source, names, "")
	}
	eventMetadataAddGlobalCompositionConnectNames(source, names)
	for _, timer := range source.WorkflowTimers() {
		names.add(timer.ID, fmt.Sprintf("timer %s", strings.TrimSpace(timer.ID)))
	}
	return names
}

func eventMetadataAddGlobalFlowTopologyNames(source semanticview.Source, names eventMetadataNameIndex, flowID string) {
	flowID = strings.TrimSpace(flowID)
	if flowID != "" {
		names.add(flowID, eventMetadataFlowLabel(flowID))
	}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		eventMetadataAddFlowRole(names, flowID, pin.PinName(), "output pin producer")
	}
	for _, pin := range source.FlowInputEventPins(flowID) {
		eventMetadataAddFlowRole(names, flowID, pin.PinName(), "input pin consumer")
	}
}

func eventMetadataAddGlobalCompositionConnectNames(source semanticview.Source, names eventMetadataNameIndex) {
	for _, connect := range source.CompositionConnects() {
		if from, err := connect.FromRef(); err == nil {
			if _, ok := source.FlowOutputEventPin(from.FlowID, from.Pin); ok {
				eventMetadataAddFlowPinRefRole(names, from, connect.From, "parent connect output producer")
			}
		}
		if to, err := connect.ToRef(); err == nil {
			if _, ok := source.FlowInputEventPin(to.FlowID, to.Pin); ok {
				eventMetadataAddFlowPinRefRole(names, to, connect.To, "parent connect input consumer")
			}
		}
	}
}

func eventMetadataRoleNames(source semanticview.Source, decl deadEventDeclaration) (eventMetadataNameIndex, eventMetadataNameIndex) {
	producers := eventMetadataNameIndex{}
	consumers := eventMetadataNameIndex{}
	if source == nil {
		return producers, consumers
	}
	for nodeKey, node := range source.NodeEntries() {
		nodeSource, _ := source.NodeContractSource(nodeKey)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for handlerEvent, handler := range node.EventHandlers {
			if deadEventRoleMatches(source, decl, flowID, handlerEvent) {
				eventMetadataAddNodeRole(consumers, nodeKey, node, "handler subscribes")
			}
			for _, emitted := range runtimecontracts.HandlerEmitEvents(handler) {
				if deadEventRoleMatches(source, decl, flowID, emitted) {
					eventMetadataAddNodeRole(producers, nodeKey, node, "handler emits")
				}
			}
		}
		for _, eventType := range runtimecontracts.EffectiveSystemNodeSubscriptions(node) {
			if deadEventRoleMatches(source, decl, flowID, eventType) {
				eventMetadataAddNodeRole(consumers, nodeKey, node, "effective subscription")
			}
		}
	}
	for agentKey, agent := range source.AgentEntries() {
		agentSource, _ := source.AgentContractSource(agentKey)
		flowID := strings.TrimSpace(agentSource.FlowID)
		for _, eventType := range agent.EmitEvents {
			if deadEventRoleMatches(source, decl, flowID, eventType) {
				eventMetadataAddAgentRole(producers, agentKey, agent, "emit_events")
			}
		}
		for _, eventType := range agent.Subscriptions {
			if deadEventRoleMatches(source, decl, flowID, eventType) {
				eventMetadataAddAgentRole(consumers, agentKey, agent, "subscriptions")
			}
		}
	}
	for _, required := range source.RequiredAgents() {
		eventMetadataAddRequiredAgentRoles(source, decl, producers, consumers, "", required)
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		eventMetadataAddFlowRoles(source, decl, producers, consumers, flowID, scope.AutoEmitEvent)
		for _, required := range source.FlowRequiredAgents(flowID) {
			eventMetadataAddRequiredAgentRoles(source, decl, producers, consumers, flowID, required)
		}
	}
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.RootSchema != nil {
		eventMetadataAddFlowRoles(source, decl, producers, consumers, "", bundle.RootSchema.AutoEmitOnCreate.Event)
	}
	eventMetadataAddCompositionConnectRoles(source, decl, producers, consumers)
	for _, timer := range source.WorkflowTimers() {
		flowID := strings.TrimSpace(timer.FlowID)
		if deadEventRoleMatches(source, decl, flowID, timer.Event) {
			producers.add(timer.ID, fmt.Sprintf("timer %s fires event", strings.TrimSpace(timer.ID)))
			producers.add("runtime", "runtime timer producer")
			producers.add("sys:runtime", "runtime timer producer")
		}
		for _, trigger := range []string{timer.StartOn, timer.CancelOn} {
			trigger = strings.TrimSpace(trigger)
			if !strings.HasPrefix(trigger, "event:") {
				continue
			}
			eventType := strings.TrimSpace(strings.TrimPrefix(trigger, "event:"))
			if deadEventRoleMatches(source, decl, flowID, eventType) {
				consumers.add(timer.ID, fmt.Sprintf("timer %s trigger", strings.TrimSpace(timer.ID)))
				consumers.add("runtime", "runtime timer consumer")
				consumers.add("sys:runtime", "runtime timer consumer")
			}
		}
	}
	for _, owner := range source.RuntimeEventOwners(decl.Canonical) {
		consumers.add(owner, fmt.Sprintf("runtime event owner %s", strings.TrimSpace(owner)))
	}
	return producers, consumers
}

func eventMetadataAddNodeRole(names eventMetadataNameIndex, nodeKey string, node runtimecontracts.SystemNodeContract, role string) {
	nodeKey = strings.TrimSpace(nodeKey)
	role = strings.TrimSpace(role)
	if role == "" {
		role = "topology"
	}
	names.add(nodeKey, fmt.Sprintf("system node %s %s", nodeKey, role))
	if id := runtimecontracts.EffectiveSystemNodeID(nodeKey, node); id != "" {
		names.add(id, fmt.Sprintf("system node %s %s", id, role))
	}
}

func eventMetadataAddAgentRole(names eventMetadataNameIndex, agentKey string, agent runtimecontracts.AgentRegistryEntry, role string) {
	agentKey = strings.TrimSpace(agentKey)
	role = strings.TrimSpace(role)
	if role == "" {
		role = "topology"
	}
	names.add(agentKey, fmt.Sprintf("agent %s %s", agentKey, role))
	names.add(agent.ID, fmt.Sprintf("agent %s %s", strings.TrimSpace(agent.ID), role))
	names.add(agent.Role, fmt.Sprintf("agent role %s %s", strings.TrimSpace(agent.Role), role))
}

func eventMetadataAddRequiredAgentRoles(source semanticview.Source, decl deadEventDeclaration, producers, consumers eventMetadataNameIndex, flowID string, required runtimecontracts.FlowRequiredAgent) {
	for _, eventType := range required.Emits {
		if deadEventRoleMatches(source, decl, flowID, eventType) {
			producers.add(required.Role, fmt.Sprintf("required agent role %s emits", strings.TrimSpace(required.Role)))
		}
	}
	for _, eventType := range required.SubscribesTo {
		if deadEventRoleMatches(source, decl, flowID, eventType) {
			consumers.add(required.Role, fmt.Sprintf("required agent role %s subscribes", strings.TrimSpace(required.Role)))
		}
	}
}

func eventMetadataAddFlowRoles(source semanticview.Source, decl deadEventDeclaration, producers, consumers eventMetadataNameIndex, flowID, autoEmitEvent string) {
	flowID = strings.TrimSpace(flowID)
	if deadEventSameScope(decl.FlowID, flowID) && deadEventRoleMatches(source, decl, flowID, autoEmitEvent) {
		eventMetadataAddFlowRole(producers, flowID, "", "auto_emit_on_create producer")
	}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		if deadEventRoleMatches(source, decl, flowID, pin.EventType()) {
			eventMetadataAddFlowRole(producers, flowID, pin.PinName(), "output pin producer")
		}
	}
	for _, pin := range source.FlowInputEventPins(flowID) {
		if deadEventRoleMatches(source, decl, flowID, pin.EventType()) {
			eventMetadataAddFlowRole(consumers, flowID, pin.PinName(), "input pin consumer")
		}
	}
}

func eventMetadataAddCompositionConnectRoles(source semanticview.Source, decl deadEventDeclaration, producers, consumers eventMetadataNameIndex) {
	for _, connect := range source.CompositionConnects() {
		if from, err := connect.FromRef(); err == nil {
			if outputPin, ok := source.FlowOutputEventPin(from.FlowID, from.Pin); ok && deadEventRoleMatches(source, decl, from.FlowID, outputPin.EventType()) {
				eventMetadataAddFlowPinRefRole(producers, from, connect.From, "parent connect output producer")
			}
		}
		if to, err := connect.ToRef(); err == nil {
			if inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin); ok && deadEventRoleMatches(source, decl, to.FlowID, inputPin.EventType()) {
				eventMetadataAddFlowPinRefRole(consumers, to, connect.To, "parent connect input consumer")
			}
		}
	}
}

func eventMetadataAddFlowRole(names eventMetadataNameIndex, flowID, pinName, role string) {
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	role = strings.TrimSpace(role)
	label := eventMetadataFlowLabel(flowID)
	if role != "" {
		label += " " + role
	}
	names.add(flowID, label)
	if pinName != "" {
		names.add(pinName, label)
	}
}

func eventMetadataAddFlowPinRefRole(names eventMetadataNameIndex, ref runtimecontracts.FlowPackagePinRef, rawRef, role string) {
	flowID := strings.TrimSpace(ref.FlowID)
	pinName := strings.TrimSpace(ref.Pin)
	role = strings.TrimSpace(role)
	label := eventMetadataFlowLabel(flowID)
	if pinName != "" {
		label += fmt.Sprintf(" pin %s", pinName)
	}
	if role != "" {
		label += " " + role
	}
	names.add(rawRef, label)
	names.add(flowID, label)
	names.add(pinName, label)
	if ref.Root && pinName != "" {
		names.add("."+pinName, label)
	}
}

func eventMetadataFlowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "root flow"
	}
	return fmt.Sprintf("flow %s", flowID)
}

func eventMetadataSourceAuthorityFindings(decl deadEventDeclaration, globalNames, producerNames eventMetadataNameIndex) []Finding {
	source := strings.TrimSpace(decl.Entry.SwarmSource())
	if source == "" {
		return nil
	}
	if eventMetadataExternalOrPlatformSource(source) {
		return nil
	}
	names := eventMetadataNameIndex{}
	names.merge(producerNames)
	names.merge(globalNames)
	if label, ok := names.match(source); ok {
		return []Finding{eventMetadataAuthorityFinding(decl, "swarm.source", source, fmt.Sprintf("matches derived internal producer %s", label))}
	}
	return []Finding{eventMetadataAuthorityFinding(decl, "swarm.source", source, "is not external/platform source proof")}
}

func eventMetadataListAuthorityFindings(decl deadEventDeclaration, field string, values []string, globalNames, roleNames eventMetadataNameIndex) []Finding {
	names := eventMetadataNameIndex{}
	names.merge(roleNames)
	names.merge(globalNames)
	out := make([]Finding, 0)
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if label, ok := names.match(value); ok {
			out = append(out, eventMetadataAuthorityFinding(decl, field, value, fmt.Sprintf("matches derived internal topology role %s", label)))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Location != out[j].Location {
			return out[i].Location < out[j].Location
		}
		return out[i].Message < out[j].Message
	})
	return out
}

func eventMetadataExternalOrPlatformSource(source string) bool {
	source = strings.ToLower(strings.TrimSpace(source))
	return strings.HasPrefix(source, "external") || strings.HasPrefix(source, "platform")
}

func eventMetadataAuthorityFinding(decl deadEventDeclaration, field, value, reason string) Finding {
	location := strings.TrimSpace(decl.Canonical)
	if location == "" {
		location = deadEventSchemaFileLabel(strings.TrimSpace(decl.File), strings.TrimSpace(decl.FlowID))
	}
	return Finding{
		CheckID:  "event_metadata_authority",
		Severity: SeverityHardInvalidity,
		Location: location,
		Message: fmt.Sprintf(
			"event %s authored %s=%q as event metadata, but %s. Internal event producer/consumer/source facts must be derived from topology; use %s only for external/platform/non-derivable proof.",
			strings.TrimSpace(decl.Canonical),
			strings.TrimSpace(field),
			strings.TrimSpace(value),
			strings.TrimSpace(reason),
			strings.TrimSpace(field),
		),
	}
}
