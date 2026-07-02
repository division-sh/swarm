package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkWritePinOwnershipValidation(c *checkerContext) []Finding { return c.writePinOwnership() }
func checkInputPinWiring(c *checkerContext) []Finding              { return c.inputPinWiring() }
func checkCrossFlowPinAmbiguityValidation(c *checkerContext) []Finding {
	return c.crossFlowPinAmbiguityValidation()
}
func checkFlowBoundaryCreateEntityValidation(c *checkerContext) []Finding {
	return c.flowBoundaryCreateEntityValidation()
}
func checkSelectEntityValidation(c *checkerContext) []Finding {
	return c.selectEntityValidation()
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
	parentConnect        string
	siblingFlowOutputPin string
	rootAgentEmit        string
	rootNodeHandlerEmit  string
	platformEventCatalog string
	externalSource       string
	sameFlowTimer        string
}

func (p inputPinProducerPaths) hasAny() bool {
	for _, detail := range []string{
		p.parentConnect,
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
		"Flow %s declares input pin event %s but no producer path was found in the authored bundle.\n\nChecked producer paths:\n- Parent connect: %s\n- Sibling flow output pin: %s\n- Root agent emit_events: %s\n- Root node handler emits: %s\n- Platform event catalog: %s\n- External source metadata (swarm.source): %s\n- Same-flow timer declaration: %s\n\nFix one of:\n- Add a parent package.yaml connect entry to this input pin\n- Add %s to a producing flow's pins.outputs.events\n- Add %s to a root agent's emit_events\n- Add swarm.source: external to %s's events.yaml entry if produced externally",
		flowID,
		eventType,
		p.parentConnect,
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
		parentConnect:        c.inputPinParentConnectPath(flowID, localEvent),
		siblingFlowOutputPin: c.inputPinSiblingFlowOutputPath(flowID, localEvent),
		rootAgentEmit:        c.inputPinRootAgentPath(localEvent, canonicalEvent, flowScopedAgents),
		rootNodeHandlerEmit:  c.inputPinRootNodeHandlerPath(localEvent, canonicalEvent, flowScopedNodes),
		platformEventCatalog: inputPinPlatformEventPath(c.source.PlatformSpec(), localEvent),
		externalSource:       c.inputPinExternalSourcePath(flowID, eventType),
		sameFlowTimer:        c.inputPinSameFlowTimerPath(flowID, eventType, canonicalEvent),
	}
}

func (c *checkerContext) inputPinParentConnectPath(flowID, localEvent string) string {
	for _, pin := range c.source.FlowInputEventPins(flowID) {
		if eventidentity.Normalize(pin.EventType()) != localEvent && eventidentity.Normalize(pin.PinName()) != localEvent {
			continue
		}
		connects := c.source.CompositionConnectsTo(flowID, pin.PinName())
		if len(connects) == 0 {
			return "not found"
		}
		refs := make([]string, 0, len(connects))
		for _, connect := range connects {
			refs = append(refs, strings.TrimSpace(connect.From))
		}
		sort.Strings(refs)
		return fmt.Sprintf("found via parent connect from %s", strings.Join(refs, ", "))
	}
	return "not found"
}

func (c *checkerContext) inputPinSiblingFlowOutputPath(flowID, localEvent string) string {
	aliases := semanticview.ImportBoundaryInputAliases(c.source, flowID, localEvent)
	if len(aliases) > 0 {
		patterns := make([]string, 0, len(aliases))
		for _, alias := range aliases {
			if pattern := eventidentity.Normalize(alias.EventPattern); pattern != "" {
				patterns = append(patterns, pattern)
			}
		}
		sort.Strings(patterns)
		if len(patterns) > 0 {
			return fmt.Sprintf("found via %s", strings.Join(patterns, ", "))
		}
	}
	if semanticview.ImportBoundaryInputAliasRequired(c.source, flowID, localEvent) {
		return "not found"
	}
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
	if runtimecontracts.PlatformEventCatalogContains(platform, localEvent) {
		return "matched"
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
			if compositionConnectsToInput(c.source, flowID, eventType) {
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
		for _, sub := range eventidentity.NormalizeList(runtimecontracts.EffectiveSystemNodeSubscriptions(node)) {
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

func (c *checkerContext) selectEntityValidation() []Finding {
	findings := []Finding{}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		inputs := normalizeStringSet(c.source.FlowInputEvents(flowID))
		scope, ok := c.source.FlowScopeByID(flowID)
		if !ok {
			continue
		}
		for nodeID, node := range scope.Nodes {
			nodeID = strings.TrimSpace(nodeID)
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				hasSelectEntity := handler.SelectEntity != nil && !handler.SelectEntity.Empty()
				hasSelectOrCreateEntity := handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()
				if !hasSelectEntity && !hasSelectOrCreateEntity {
					continue
				}
				location := flowID
				label := "select_entity"
				if hasSelectOrCreateEntity && !hasSelectEntity {
					label = "select_or_create_entity"
				} else if hasSelectEntity && hasSelectOrCreateEntity {
					label = "select_entity/select_or_create_entity"
				}
				if handler.CreateEntity && (hasSelectEntity || hasSelectOrCreateEntity) {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s must not declare create_entity with select_entity or select_or_create_entity", flowID, eventType, nodeID),
						Location: location,
					})
				}
				if hasSelectEntity && hasSelectOrCreateEntity {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s must not declare both select_entity and select_or_create_entity", flowID, eventType, nodeID),
						Location: location,
					})
				}
				if strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s, but template flows must use create_flow_instance routing rather than service-owned static entity acquisition", flowID, eventType, nodeID, label),
						Location: location,
					})
				}
				if strings.TrimSpace(schema.InitialState) == "" {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s, but stateless flows do not have stateful input-pin entity acquisition", flowID, eventType, nodeID, label),
						Location: location,
					})
				}
				if _, ok := inputs[eventType]; !ok {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s outside a declared input pin", flowID, eventType, nodeID, label),
						Location: location,
					})
				}
				findings = append(findings, validateSelectEntityBindings(c.source, flowID, eventType, nodeID, "select_entity", handler.SelectEntity)...)
				findings = append(findings, validateSelectOrCreateEntityBindings(c.source, flowID, eventType, nodeID, handler.SelectOrCreateEntity)...)
			}
		}
	}
	return findings
}

func validateSelectEntityBindings(source semanticview.Source, flowID, eventType, nodeID, label string, spec *runtimecontracts.SelectEntitySpec) []Finding {
	if spec == nil || spec.Empty() {
		return nil
	}
	return validateEntityAcquisitionBindings(source, flowID, eventType, nodeID, label, spec.Bindings)
}

func validateSelectOrCreateEntityBindings(source semanticview.Source, flowID, eventType, nodeID string, spec *runtimecontracts.SelectOrCreateEntitySpec) []Finding {
	if spec == nil || spec.Empty() {
		return nil
	}
	return validateEntityAcquisitionBindings(source, flowID, eventType, nodeID, "select_or_create_entity", spec.Bindings)
}

func validateEntityAcquisitionBindings(source semanticview.Source, flowID, eventType, nodeID, label string, bindings []runtimecontracts.SelectEntityKeyBinding) []Finding {
	location := strings.TrimSpace(flowID)
	contract, ok := entityruntime.ResolveForFlow(source, flowID)
	if !ok {
		return []Finding{{
			CheckID:  "select_entity_validation",
			Severity: "error",
			Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s but the target flow entity contract is unavailable", flowID, eventType, nodeID, label),
			Location: location,
		}}
	}
	findings := []Finding{}
	seen := map[string]struct{}{}
	for _, binding := range bindings {
		field := strings.TrimSpace(binding.Field)
		ref := strings.TrimSpace(binding.Ref)
		if _, ok := seen[field]; ok {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is declared more than once", flowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		seen[field] = struct{}{}
		if selectEntityReservedTargetField(field) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is not an entity contract field selection target", flowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		if _, err := entityruntime.ResolveLeafField(contract, field); err != nil {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is invalid: %v", flowID, eventType, nodeID, label, field, err),
				Location: location,
			})
		}
		parsed := binding.RefPath
		if parsed.IsZero() {
			parsed = paths.Parse(ref)
		}
		if !parsed.HasExplicitRoot() || parsed.Root != paths.RootPayload {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s must resolve from payload.*, got %q", flowID, eventType, nodeID, label, field, ref),
				Location: location,
			})
			continue
		}
		if len(parsed.Segments) == 0 {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s has an empty payload ref", flowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		if selectEntityReservedPayloadRef(parsed) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s must not use source envelope authority %q", flowID, eventType, nodeID, label, field, ref),
				Location: location,
			})
			continue
		}
		if !selectEntityPayloadFieldDeclared(source, flowID, eventType, parsed.Segments[0]) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s references undeclared payload field %q", flowID, eventType, nodeID, label, field, parsed.Segments[0]),
				Location: location,
			})
		}
	}
	return findings
}

func selectEntityReservedTargetField(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "entity_type", "flow_instance", "current_state", "workflow_name", "workflow_version":
		return true
	default:
		return false
	}
}

func selectEntityPayloadFieldDeclared(source semanticview.Source, flowID, eventType, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	resolution := semanticview.ResolveEventSchema(source, flowID, eventType)
	if !resolution.HasSchema {
		return false
	}
	rawProps, ok := resolution.Schema.Schema["properties"]
	if !ok || rawProps == nil {
		return false
	}
	props, ok := rawProps.(map[string]any)
	if !ok {
		return false
	}
	_, ok = props[field]
	return ok
}

func selectEntityReservedPayloadRef(parsed paths.Path) bool {
	if parsed.Root != paths.RootPayload || len(parsed.Segments) == 0 {
		return false
	}
	switch strings.TrimSpace(parsed.Segments[0]) {
	case "entity_id", "entity_type", "flow_instance":
		return true
	default:
		return false
	}
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
				if handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
					continue
				}
				if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
					continue
				}
				c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
					CheckID:  "flow_boundary_create_entity_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input pin handler %s on node %s must declare create_entity: true, select_entity, or select_or_create_entity", flowID, eventType, nodeID),
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
