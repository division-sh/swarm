package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
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
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, endpoint := range census.Producers() {
		if endpointMatchesDeadEventDeclaration(endpoint, decl) {
			eventMetadataAddEndpointRole(source, producers, endpoint)
		}
	}
	for _, endpoint := range census.Consumers() {
		if endpointMatchesDeadEventDeclaration(endpoint, decl) {
			eventMetadataAddEndpointRole(source, consumers, endpoint)
		}
	}
	for _, endpoint := range census.OutputPins() {
		if endpointMatchesDeadEventDeclaration(endpoint, decl) {
			eventMetadataAddFlowRole(producers, endpoint.FlowID, endpoint.PinName, "output pin producer")
		}
	}
	for _, endpoint := range census.InputPins() {
		if endpointMatchesDeadEventDeclaration(endpoint, decl) {
			eventMetadataAddFlowRole(consumers, endpoint.FlowID, endpoint.PinName, "input pin consumer")
		}
	}
	for _, edge := range routingtopology.Build(source).Edges {
		if edge.Scope != routingtopology.DeliveryScopeInterFlow || edge.Boundary == nil {
			continue
		}
		if edge.Producer.Event.Canonical == decl.Canonical || edge.Producer.Event.Local == decl.Canonical {
			producers.add(edge.Boundary.From, "parent connect output producer")
		}
		if edge.Consumer.Event.Canonical == decl.Canonical || edge.Consumer.Event.Local == decl.Canonical {
			consumers.add(edge.Boundary.To, "parent connect input consumer")
		}
	}
	return producers, consumers
}

func endpointMatchesDeadEventDeclaration(endpoint semanticview.AuthoredEventEndpoint, decl deadEventDeclaration) bool {
	if eventidentity.Normalize(endpoint.Event.Canonical) != eventidentity.Normalize(decl.Canonical) {
		return false
	}
	if strings.TrimSpace(decl.FlowID) == "" {
		return strings.TrimSpace(endpoint.FlowID) == ""
	}
	return deadEventSameScope(decl.FlowID, endpoint.FlowID) || strings.Contains(eventidentity.Normalize(endpoint.Event.Authored), "/")
}

func eventMetadataAddEndpointRole(source semanticview.Source, names eventMetadataNameIndex, endpoint semanticview.AuthoredEventEndpoint) {
	switch endpoint.Kind {
	case semanticview.EventEndpointNodeHandler:
		role := "handler subscribes"
		if endpoint.Direction == semanticview.EventEndpointProducer {
			role = "handler emits"
		}
		eventMetadataAddNodeRole(names, endpoint.NodeID, source.NodeEntries()[endpoint.NodeID], role)
	case semanticview.EventEndpointNodeGenerated:
		role := "effective subscriptions"
		if endpoint.Direction == semanticview.EventEndpointProducer {
			role = "effective produces"
		}
		eventMetadataAddNodeRole(names, endpoint.NodeID, source.NodeEntries()[endpoint.NodeID], role)
	case semanticview.EventEndpointAgent:
		role := "subscriptions"
		if endpoint.Direction == semanticview.EventEndpointProducer {
			role = "emit_events"
		}
		eventMetadataAddAgentRole(names, endpoint.AgentID, source.AgentEntries()[endpoint.AgentID], role)
	case semanticview.EventEndpointRequiredAgentRole:
		role := "subscribes_to"
		if endpoint.Direction == semanticview.EventEndpointProducer {
			role = "emits"
		}
		names.add(endpoint.Role, fmt.Sprintf("required agent role %s %s", endpoint.Role, role))
	case semanticview.EventEndpointTimer:
		role := "trigger"
		if endpoint.Direction == semanticview.EventEndpointProducer {
			role = "fires event"
		}
		names.add(endpoint.TimerID, fmt.Sprintf("timer %s %s", endpoint.TimerID, role))
		names.add("runtime", "runtime timer "+role)
		names.add("sys:runtime", "runtime timer "+role)
	case semanticview.EventEndpointAutoEmit:
		eventMetadataAddFlowRole(names, endpoint.FlowID, "", "auto_emit_on_create producer")
	case semanticview.EventEndpointExternal, semanticview.EventEndpointPlatform:
		// Authored external/platform metadata is evidence, not an internal role.
		return
	}
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
