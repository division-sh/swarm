package semanticview

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"gopkg.in/yaml.v3"
)

type EventEndpointDirection string

const (
	EventEndpointProducer  EventEndpointDirection = "producer"
	EventEndpointConsumer  EventEndpointDirection = "consumer"
	EventEndpointInputPin  EventEndpointDirection = "input_pin"
	EventEndpointOutputPin EventEndpointDirection = "output_pin"
)

type EventEndpointKind string

const (
	EventEndpointNodeHandler       EventEndpointKind = "node_handler"
	EventEndpointNodeGenerated     EventEndpointKind = "node_generated"
	EventEndpointAgent             EventEndpointKind = "agent"
	EventEndpointRequiredAgentRole EventEndpointKind = "required_agent_role"
	EventEndpointTimer             EventEndpointKind = "timer"
	EventEndpointAutoEmit          EventEndpointKind = "auto_emit_on_create"
	EventEndpointExternal          EventEndpointKind = "external"
	EventEndpointPlatform          EventEndpointKind = "platform"
	EventEndpointFlowInputPin      EventEndpointKind = "flow_input_pin"
	EventEndpointFlowOutputPin     EventEndpointKind = "flow_output_pin"
)

type AuthoredEventEndpoint struct {
	ID             string                 `json:"id"`
	Direction      EventEndpointDirection `json:"direction"`
	Kind           EventEndpointKind      `json:"kind"`
	FlowID         string                 `json:"flow_id,omitempty"`
	FlowPath       string                 `json:"flow_path,omitempty"`
	PackageKey     string                 `json:"package_key,omitempty"`
	Event          FlowEventProof         `json:"event"`
	Pattern        bool                   `json:"pattern,omitempty"`
	NodeID         string                 `json:"node_id,omitempty"`
	HandlerEvent   string                 `json:"handler_event,omitempty"`
	AgentID        string                 `json:"agent_id,omitempty"`
	Role           string                 `json:"role,omitempty"`
	TimerID        string                 `json:"timer_id,omitempty"`
	PinName        string                 `json:"pin_name,omitempty"`
	Site           string                 `json:"site,omitempty"`
	SourceFile     string                 `json:"source_file,omitempty"`
	SourceLine     int                    `json:"source_line,omitempty"`
	SourceLocation string                 `json:"source_location,omitempty"`
	ResolutionMode string                 `json:"resolution_mode,omitempty"`
}

type LegacyQualifiedSubscription struct {
	ID             string                `json:"id"`
	Consumer       AuthoredEventEndpoint `json:"consumer"`
	TargetFlowID   string                `json:"target_flow_id"`
	TargetFlowPath string                `json:"target_flow_path"`
	Event          FlowEventProof        `json:"event"`
}

type NodeProducerAssertion struct {
	NodeID     string   `json:"node_id"`
	FlowID     string   `json:"flow_id,omitempty"`
	Declared   bool     `json:"declared"`
	EventTypes []string `json:"event_types"`
}

type AuthoredEventEndpointCensus struct {
	source       Source
	producers    []AuthoredEventEndpoint
	consumers    []AuthoredEventEndpoint
	inputPins    []AuthoredEventEndpoint
	outputPins   []AuthoredEventEndpoint
	assertions   []NodeProducerAssertion
	endpointByID map[string]AuthoredEventEndpoint
}

func BuildAuthoredEventEndpointCensus(source Source) AuthoredEventEndpointCensus {
	builder := endpointCensusBuilder{
		source: source,
		seen:   map[string]struct{}{},
	}
	if source != nil {
		builder.addNodeEndpoints()
		builder.addAgentEndpoints()
		builder.addRequiredAgentEndpoints()
		builder.addTimerEndpoints()
		builder.addAutoEmitEndpoints()
		builder.addPinEndpoints()
		builder.addMetadataBoundaryEndpoints()
	}
	return builder.build()
}

func (c AuthoredEventEndpointCensus) Producers() []AuthoredEventEndpoint {
	return cloneEventEndpoints(c.producers)
}

func (c AuthoredEventEndpointCensus) Consumers() []AuthoredEventEndpoint {
	return cloneEventEndpoints(c.consumers)
}

func (c AuthoredEventEndpointCensus) InputPins() []AuthoredEventEndpoint {
	return cloneEventEndpoints(c.inputPins)
}

func (c AuthoredEventEndpointCensus) OutputPins() []AuthoredEventEndpoint {
	return cloneEventEndpoints(c.outputPins)
}

func (c AuthoredEventEndpointCensus) ProducerAssertions() []NodeProducerAssertion {
	out := make([]NodeProducerAssertion, len(c.assertions))
	for i := range c.assertions {
		out[i] = c.assertions[i]
		out[i].EventTypes = append([]string(nil), c.assertions[i].EventTypes...)
	}
	return out
}

func (c AuthoredEventEndpointCensus) Endpoint(id string) (AuthoredEventEndpoint, bool) {
	endpoint, ok := c.endpointByID[strings.TrimSpace(id)]
	return endpoint, ok
}

func (c AuthoredEventEndpointCensus) MatchingProducers(flowID, eventType string) []AuthoredEventEndpoint {
	return c.matchingEndpoints(c.producers, flowID, eventType)
}

func (c AuthoredEventEndpointCensus) MatchingProducersAcrossFlows(flowID, eventType string) []AuthoredEventEndpoint {
	return c.matchingEndpointsAcrossFlows(c.producers, flowID, eventType)
}

func (c AuthoredEventEndpointCensus) MatchingOutputPinsAcrossFlows(flowID, eventType string) []AuthoredEventEndpoint {
	return c.matchingEndpointsAcrossFlows(c.outputPins, flowID, eventType)
}

func (c AuthoredEventEndpointCensus) MatchingOutputPins(flowID, eventType string) []AuthoredEventEndpoint {
	return c.matchingEndpoints(c.outputPins, flowID, eventType)
}

func (c AuthoredEventEndpointCensus) MatchingConsumers(flowID, eventType string) []AuthoredEventEndpoint {
	return c.matchingEndpoints(c.consumers, flowID, eventType)
}

// LegacyQualifiedSubscriptions returns runtime-deliverable qualified subscriptions
// that cross a flow boundary without using pins and connect. They are intentionally
// excluded from the canonical routing graph.
func (c AuthoredEventEndpointCensus) LegacyQualifiedSubscriptions() []LegacyQualifiedSubscription {
	if c.source == nil {
		return []LegacyQualifiedSubscription{}
	}
	flows := append([]FlowScope(nil), c.source.FlowScopes()...)
	sort.SliceStable(flows, func(i, j int) bool {
		left := eventidentity.Normalize(flows[i].Path)
		right := eventidentity.Normalize(flows[j].Path)
		if len(left) != len(right) {
			return len(left) > len(right)
		}
		return left < right
	})
	out := make([]LegacyQualifiedSubscription, 0)
	for _, consumer := range c.consumers {
		if consumer.Pattern || !runtimeDeliveryConsumer(consumer.Kind) {
			continue
		}
		if c.consumerReachedThroughConnectedInput(consumer) {
			continue
		}
		authored := eventidentity.Normalize(consumer.Event.Authored)
		if !strings.Contains(authored, "/") {
			continue
		}
		for _, flow := range flows {
			flowID := strings.TrimSpace(flow.ID)
			flowPath := eventidentity.Normalize(flow.Path)
			if flowID == "" || flowID == strings.TrimSpace(consumer.FlowID) || flowPath == "" || !strings.HasPrefix(authored, flowPath+"/") {
				continue
			}
			local := eventidentity.Normalize(strings.TrimPrefix(authored, flowPath+"/"))
			if local == "" {
				continue
			}
			proof := ResolveFlowEventProof(c.source, flowID, local)
			if !proof.HasSchema || eventidentity.Normalize(proof.Canonical) != authored {
				continue
			}
			legacy := LegacyQualifiedSubscription{
				Consumer:       consumer,
				TargetFlowID:   flowID,
				TargetFlowPath: flowPath,
				Event:          proof,
			}
			legacy.ID = legacyQualifiedSubscriptionID(legacy)
			out = append(out, legacy)
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c AuthoredEventEndpointCensus) consumerReachedThroughConnectedInput(consumer AuthoredEventEndpoint) bool {
	for _, input := range c.inputPins {
		if strings.TrimSpace(input.FlowID) != strings.TrimSpace(consumer.FlowID) || !declaredInputIdentityMatches(c.source, input, consumer.Event.Authored) {
			continue
		}
		if len(c.source.CompositionConnectsTo(input.FlowID, input.PinName)) > 0 {
			return true
		}
	}
	return false
}

func runtimeDeliveryConsumer(kind EventEndpointKind) bool {
	switch kind {
	case EventEndpointNodeHandler, EventEndpointNodeGenerated, EventEndpointAgent, EventEndpointTimer:
		return true
	default:
		return false
	}
}

func legacyQualifiedSubscriptionID(subscription LegacyQualifiedSubscription) string {
	parts := []string{subscription.Consumer.ID, subscription.TargetFlowID, subscription.Event.EventKey()}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "legacy-subscription-" + hex.EncodeToString(digest[:8])
}

func (c AuthoredEventEndpointCensus) matchingEndpoints(endpoints []AuthoredEventEndpoint, flowID, eventType string) []AuthoredEventEndpoint {
	flowID = strings.TrimSpace(flowID)
	proof := ResolveFlowEventProof(c.source, flowID, eventType)
	if c.source == nil || proof.EventKey() == "" {
		return nil
	}
	out := make([]AuthoredEventEndpoint, 0)
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint.FlowID) != flowID {
			continue
		}
		if endpointMatchesProof(c.source, endpoint, proof) {
			out = append(out, endpoint)
		}
	}
	return cloneEventEndpoints(out)
}

func (c AuthoredEventEndpointCensus) matchingEndpointsAcrossFlows(endpoints []AuthoredEventEndpoint, flowID, eventType string) []AuthoredEventEndpoint {
	flowID = strings.TrimSpace(flowID)
	proof := ResolveFlowEventProof(c.source, flowID, eventType)
	if c.source == nil || proof.EventKey() == "" {
		return nil
	}
	out := make([]AuthoredEventEndpoint, 0)
	for _, endpoint := range endpoints {
		if endpointProofsShareDeclaredIdentity(endpoint.Event, proof) ||
			endpointMatchesProof(c.source, endpoint, proof) ||
			flowEventMatchesWithoutTopology(c.source, endpoint.FlowID, proof.Authored, endpoint.Event.EventKey()) {
			out = append(out, endpoint)
		}
	}
	return cloneEventEndpoints(out)
}

func endpointProofsShareDeclaredIdentity(left, right FlowEventProof) bool {
	leftValues := []string{left.Authored, left.Local}
	rightValues := []string{right.Authored, right.Local}
	for _, leftValue := range leftValues {
		leftValue = eventidentity.Normalize(leftValue)
		if leftValue == "" {
			continue
		}
		for _, rightValue := range rightValues {
			if leftValue == eventidentity.Normalize(rightValue) {
				return true
			}
		}
	}
	return false
}

type EndpointAssociationStatus string

const (
	EndpointAssociationMatched   EndpointAssociationStatus = "matched"
	EndpointAssociationNotFound  EndpointAssociationStatus = "not_found"
	EndpointAssociationAmbiguous EndpointAssociationStatus = "ambiguous"
)

type EndpointAssociationResult struct {
	Status     EndpointAssociationStatus `json:"status"`
	FlowID     string                    `json:"flow_id,omitempty"`
	Identity   string                    `json:"identity,omitempty"`
	NodeID     string                    `json:"node_id,omitempty"`
	Candidates []AuthoredEventEndpoint   `json:"candidates"`
}

func (r EndpointAssociationResult) Matched() bool {
	return r.Status == EndpointAssociationMatched && len(r.Candidates) == 1
}

func (r EndpointAssociationResult) Endpoint() (AuthoredEventEndpoint, bool) {
	if !r.Matched() {
		return AuthoredEventEndpoint{}, false
	}
	return r.Candidates[0], true
}

func (r EndpointAssociationResult) Err() error {
	if r.Matched() {
		return nil
	}
	return &EndpointAssociationError{
		Status:     r.Status,
		FlowID:     r.FlowID,
		Identity:   r.Identity,
		NodeID:     r.NodeID,
		Candidates: cloneEventEndpoints(r.Candidates),
	}
}

type EndpointAssociationError struct {
	Status     EndpointAssociationStatus
	FlowID     string
	Identity   string
	NodeID     string
	Candidates []AuthoredEventEndpoint
}

func (e *EndpointAssociationError) Error() string {
	if e == nil {
		return "event endpoint association failed"
	}
	flow := strings.TrimSpace(e.FlowID)
	if flow == "" {
		flow = "root"
	}
	switch e.Status {
	case EndpointAssociationAmbiguous:
		return fmt.Sprintf("event endpoint %q in flow %s is ambiguous across %d candidates", strings.TrimSpace(e.Identity), flow, len(e.Candidates))
	default:
		return fmt.Sprintf("event endpoint %q was not found in flow %s", strings.TrimSpace(e.Identity), flow)
	}
}

func (c AuthoredEventEndpointCensus) ResolveDeclaredInputEndpoint(flowID, identity string) EndpointAssociationResult {
	flowID = strings.TrimSpace(flowID)
	identity = eventidentity.Normalize(identity)
	candidates := make([]AuthoredEventEndpoint, 0)
	for _, endpoint := range c.inputPins {
		if strings.TrimSpace(endpoint.FlowID) != flowID {
			continue
		}
		if declaredInputIdentityMatches(c.source, endpoint, identity) {
			candidates = append(candidates, endpoint)
		}
	}
	return endpointAssociationResult(flowID, identity, "", candidates)
}

func (c AuthoredEventEndpointCensus) ResolveFanInInputForHandler(flowID, nodeID, handlerEvent string) EndpointAssociationResult {
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	handlerEvent = eventidentity.Normalize(handlerEvent)
	candidates := make([]AuthoredEventEndpoint, 0)
	for _, endpoint := range c.inputPins {
		if strings.TrimSpace(endpoint.FlowID) != flowID || !strings.EqualFold(strings.TrimSpace(endpoint.ResolutionMode), "fan-in") {
			continue
		}
		if fanInInputMatchesHandler(c.source, endpoint, handlerEvent) {
			candidates = append(candidates, endpoint)
		}
	}
	return endpointAssociationResult(flowID, handlerEvent, nodeID, candidates)
}

func endpointAssociationResult(flowID, identity, nodeID string, candidates []AuthoredEventEndpoint) EndpointAssociationResult {
	sort.SliceStable(candidates, func(i, j int) bool {
		left := strings.Join([]string{strings.TrimSpace(candidates[i].FlowID), strings.TrimSpace(candidates[i].PinName), candidates[i].Event.EventKey(), candidates[i].ID}, "\x00")
		right := strings.Join([]string{strings.TrimSpace(candidates[j].FlowID), strings.TrimSpace(candidates[j].PinName), candidates[j].Event.EventKey(), candidates[j].ID}, "\x00")
		return left < right
	})
	status := EndpointAssociationNotFound
	if len(candidates) == 1 {
		status = EndpointAssociationMatched
	} else if len(candidates) > 1 {
		status = EndpointAssociationAmbiguous
	}
	return EndpointAssociationResult{
		Status:     status,
		FlowID:     strings.TrimSpace(flowID),
		Identity:   eventidentity.Normalize(identity),
		NodeID:     strings.TrimSpace(nodeID),
		Candidates: cloneEventEndpoints(candidates),
	}
}

type endpointCensusBuilder struct {
	source     Source
	seen       map[string]struct{}
	yamlFiles  map[string]*yaml.Node
	producers  []AuthoredEventEndpoint
	consumers  []AuthoredEventEndpoint
	inputPins  []AuthoredEventEndpoint
	outputPins []AuthoredEventEndpoint
	assertions []NodeProducerAssertion
}

func (b *endpointCensusBuilder) addNodeEndpoints() {
	authoredByNodeEvent := map[string]struct{}{}
	for _, site := range AuthoredEmitSites(b.source) {
		eventType := site.Spec.EventType()
		endpoint := b.endpoint(EventEndpointProducer, EventEndpointNodeHandler, site.FlowID, eventType)
		endpoint.PackageKey = strings.TrimSpace(site.FlowPackageKey)
		endpoint.FlowPath = strings.TrimSpace(site.FlowPath)
		endpoint.NodeID = strings.TrimSpace(site.NodeID)
		endpoint.HandlerEvent = strings.TrimSpace(site.HandlerEvent)
		endpoint.Site = strings.TrimSpace(site.SiteKey)
		endpoint.SourceLocation = strings.TrimSpace(site.SiteKey)
		if source, ok := b.source.NodeContractSource(site.NodeID); ok {
			endpoint.SourceFile = strings.TrimSpace(source.File)
		}
		b.add(endpoint)
		authoredByNodeEvent[nodeEventKey(endpoint.NodeID, endpoint.Event.EventKey())] = struct{}{}
	}

	nodeIDs := sortedMapKeys(b.source.NodeEntries())
	for _, nodeID := range nodeIDs {
		node := b.source.NodeEntries()[nodeID]
		source, _ := b.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(source.FlowID)
		b.assertions = append(b.assertions, NodeProducerAssertion{
			NodeID:     nodeID,
			FlowID:     flowID,
			Declared:   node.ProducesDeclared || node.Produces != nil,
			EventTypes: normalizedSortedStrings(node.Produces),
		})
		for _, eventType := range NodeEffectiveProduces(b.source, nodeID) {
			proof := ResolveFlowEventProof(b.source, flowID, eventType)
			if _, exists := authoredByNodeEvent[nodeEventKey(nodeID, proof.EventKey())]; exists {
				continue
			}
			endpoint := b.endpoint(EventEndpointProducer, EventEndpointNodeGenerated, flowID, eventType)
			endpoint.NodeID = nodeID
			endpoint.PackageKey = strings.TrimSpace(source.PackageKey)
			endpoint.SourceFile = strings.TrimSpace(source.File)
			endpoint.SourceLocation = "effective generated producer"
			b.add(endpoint)
		}
		consumerEvents := append(NodeEffectiveSubscriptions(b.source, nodeID), b.source.NodeHandlerSubscriptions(nodeID)...)
		for _, eventType := range normalizedSortedStrings(consumerEvents) {
			kind := EventEndpointNodeGenerated
			handlerEvent := ""
			if resolution, ok := resolveNodeHandlerProof(b.source, nodeID, eventType); ok {
				kind = EventEndpointNodeHandler
				handlerEvent = resolution
			}
			endpoint := b.endpoint(EventEndpointConsumer, kind, flowID, eventType)
			endpoint.NodeID = nodeID
			endpoint.HandlerEvent = handlerEvent
			endpoint.PackageKey = strings.TrimSpace(source.PackageKey)
			endpoint.SourceFile = strings.TrimSpace(source.File)
			endpoint.Pattern = strings.Contains(eventType, "*")
			endpoint.SourceLocation = "event_handlers." + eventType
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) addAgentEndpoints() {
	represented := map[string]struct{}{}
	projectScopes := sortedAuthoredProjectScopes(b.source.ProjectScopes())
	for _, scope := range projectScopes {
		if authoredEmitSiteSkipsProjectScope(scope) {
			continue
		}
		b.addAgentScope(scope.OwningFlowID, scope.Key, "", scope.Agents, represented)
	}
	flowScopes := sortedAuthoredFlowScopes(b.source.FlowScopes())
	preferredFlowScopeKeys := authoredPreferredFlowScopeKeys(projectScopes, flowScopes)
	for _, scope := range flowScopes {
		scopeKey := authoredEmitSiteFlowScopeKey(scope)
		if preferred := preferredFlowScopeKeys[strings.TrimSpace(scope.ID)]; preferred != "" && scopeKey != preferred {
			continue
		}
		b.addAgentScope(scope.ID, scopeKey, scope.Path, scope.Agents, represented)
	}
	for _, agentID := range sortedMapKeys(b.source.AgentEntries()) {
		if _, ok := represented[agentID]; ok {
			continue
		}
		agent := b.source.AgentEntries()[agentID]
		contractSource, _ := b.source.AgentContractSource(agentID)
		b.addAgent(agentID, agent, contractSource.FlowID, contractSource.PackageKey, "", contractSource.File)
	}
}

func (b *endpointCensusBuilder) addAgentScope(flowID, packageKey, flowPath string, agents map[string]runtimecontracts.AgentRegistryEntry, represented map[string]struct{}) {
	for _, agentID := range sortedMapKeys(agents) {
		represented[agentID] = struct{}{}
		contractSource, _ := b.source.AgentContractSource(agentID)
		sourceFile := strings.TrimSpace(contractSource.File)
		b.addAgent(agentID, agents[agentID], flowID, packageKey, flowPath, sourceFile)
	}
}

func (b *endpointCensusBuilder) addAgent(agentID string, agent runtimecontracts.AgentRegistryEntry, flowID, packageKey, flowPath, sourceFile string) {
	flowID = strings.TrimSpace(flowID)
	role := strings.TrimSpace(agent.Role)
	for _, eventType := range normalizedSortedStrings(agent.EmitEvents) {
		endpoint := b.endpoint(EventEndpointProducer, EventEndpointAgent, flowID, eventType)
		endpoint.AgentID = agentID
		endpoint.Role = role
		endpoint.PackageKey = strings.TrimSpace(packageKey)
		endpoint.FlowPath = strings.TrimSpace(flowPath)
		endpoint.SourceFile = strings.TrimSpace(sourceFile)
		endpoint.SourceLocation = "emit_events"
		b.add(endpoint)
	}
	for _, eventType := range normalizedSortedStrings(agent.Subscriptions) {
		endpoint := b.endpoint(EventEndpointConsumer, EventEndpointAgent, flowID, eventType)
		endpoint.AgentID = agentID
		endpoint.Role = role
		endpoint.PackageKey = strings.TrimSpace(packageKey)
		endpoint.FlowPath = strings.TrimSpace(flowPath)
		endpoint.SourceFile = strings.TrimSpace(sourceFile)
		endpoint.SourceLocation = "subscriptions"
		endpoint.Pattern = strings.Contains(eventType, "*")
		b.add(endpoint)
	}
}

func (b *endpointCensusBuilder) addRequiredAgentEndpoints() {
	b.addRequiredAgentScope("", b.source.RequiredAgents())
	for _, scope := range sortedFlowScopes(b.source.FlowScopes()) {
		b.addRequiredAgentScope(strings.TrimSpace(scope.ID), b.source.FlowRequiredAgents(scope.ID))
	}
}

func (b *endpointCensusBuilder) addRequiredAgentScope(flowID string, required []runtimecontracts.FlowRequiredAgent) {
	sort.SliceStable(required, func(i, j int) bool { return strings.TrimSpace(required[i].Role) < strings.TrimSpace(required[j].Role) })
	for _, role := range required {
		for _, eventType := range normalizedSortedStrings(role.Emits) {
			endpoint := b.endpoint(EventEndpointProducer, EventEndpointRequiredAgentRole, flowID, eventType)
			endpoint.Role = strings.TrimSpace(role.Role)
			endpoint.SourceLocation = "required_agents.emits"
			b.add(endpoint)
		}
		for _, eventType := range normalizedSortedStrings(role.SubscribesTo) {
			endpoint := b.endpoint(EventEndpointConsumer, EventEndpointRequiredAgentRole, flowID, eventType)
			endpoint.Role = strings.TrimSpace(role.Role)
			endpoint.SourceLocation = "required_agents.subscribes_to"
			endpoint.Pattern = strings.Contains(eventType, "*")
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) addTimerEndpoints() {
	timers := append([]runtimecontracts.WorkflowTimerContract(nil), b.source.WorkflowTimers()...)
	sort.SliceStable(timers, func(i, j int) bool { return strings.TrimSpace(timers[i].ID) < strings.TrimSpace(timers[j].ID) })
	for _, timer := range timers {
		flowID := strings.TrimSpace(timer.FlowID)
		if eventType := strings.TrimSpace(timer.Event); eventType != "" {
			kind := EventEndpointTimer
			location := "timer.event"
			if timer.StageOwned && eventType == runtimecontracts.WorkflowStageTimerInternalEvent {
				kind = EventEndpointPlatform
				location = "stage timer internal event"
			}
			endpoint := b.endpoint(EventEndpointProducer, kind, flowID, eventType)
			endpoint.TimerID = strings.TrimSpace(timer.ID)
			endpoint.NodeID = strings.TrimSpace(timer.NodeID)
			endpoint.SourceLocation = location
			b.add(endpoint)
		}
		for _, trigger := range []struct {
			location string
			value    string
		}{{"timer.start_on", timer.StartOn}, {"timer.cancel_on", timer.CancelOn}} {
			if !strings.HasPrefix(strings.TrimSpace(trigger.value), "event:") {
				continue
			}
			eventType := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(trigger.value), "event:"))
			endpoint := b.endpoint(EventEndpointConsumer, EventEndpointTimer, flowID, eventType)
			endpoint.TimerID = strings.TrimSpace(timer.ID)
			endpoint.SourceLocation = trigger.location
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) addAutoEmitEndpoints() {
	if bundle, ok := Bundle(b.source); ok && bundle != nil && bundle.RootSchema != nil {
		if eventType := strings.TrimSpace(bundle.RootSchema.AutoEmitOnCreate.Event); eventType != "" {
			endpoint := b.endpoint(EventEndpointProducer, EventEndpointAutoEmit, "", eventType)
			endpoint.SourceFile = strings.TrimSpace(bundle.Paths.RootSchemaFile)
			endpoint.SourceLocation = "auto_emit_on_create"
			b.add(endpoint)
		}
	}
	for _, scope := range sortedFlowScopes(b.source.FlowScopes()) {
		if eventType := strings.TrimSpace(scope.AutoEmitEvent); eventType != "" {
			endpoint := b.endpoint(EventEndpointProducer, EventEndpointAutoEmit, scope.ID, eventType)
			endpoint.PackageKey = strings.TrimSpace(scope.PackageKey)
			endpoint.SourceLocation = "auto_emit_on_create"
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) addPinEndpoints() {
	flowIDs := []string{""}
	for _, scope := range sortedFlowScopes(b.source.FlowScopes()) {
		flowIDs = append(flowIDs, strings.TrimSpace(scope.ID))
	}
	for _, flowID := range flowIDs {
		for _, pin := range sortedInputPins(b.source.FlowInputEventPins(flowID)) {
			endpoint := b.endpoint(EventEndpointInputPin, EventEndpointFlowInputPin, flowID, pin.EventType())
			endpoint.PinName = strings.TrimSpace(pin.PinName())
			endpoint.SourceLocation = "pins.inputs.events." + endpoint.PinName
			endpoint.ResolutionMode = strings.TrimSpace(pin.Resolution.Mode)
			b.add(endpoint)
		}
		for _, pin := range sortedOutputPins(b.source.FlowOutputEventPins(flowID)) {
			endpoint := b.endpoint(EventEndpointOutputPin, EventEndpointFlowOutputPin, flowID, pin.EventType())
			endpoint.PinName = strings.TrimSpace(pin.PinName())
			endpoint.SourceLocation = "pins.outputs.events." + endpoint.PinName
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) addMetadataBoundaryEndpoints() {
	for eventType, entry := range b.source.AuthoredEventEntries() {
		source := strings.ToLower(strings.TrimSpace(entry.SwarmSource()))
		if strings.HasPrefix(source, "platform") || strings.HasPrefix(source, "external") {
			kind := EventEndpointExternal
			if strings.HasPrefix(source, "platform") {
				kind = EventEndpointPlatform
			}
			endpoint := b.endpoint(EventEndpointProducer, kind, "", eventType)
			endpoint.SourceLocation = "swarm.source"
			b.add(endpoint)
		}
		for _, consumer := range entry.SwarmConsumer() {
			endpoint := b.endpoint(EventEndpointConsumer, EventEndpointExternal, "", eventType)
			endpoint.Role = strings.TrimSpace(consumer)
			endpoint.SourceLocation = "swarm.consumer"
			b.add(endpoint)
		}
	}
}

func (b *endpointCensusBuilder) endpoint(direction EventEndpointDirection, kind EventEndpointKind, flowID, eventType string) AuthoredEventEndpoint {
	flowID = strings.TrimSpace(flowID)
	flowPath := ""
	packageKey := ""
	if scope, ok := b.source.FlowScopeByID(flowID); ok {
		flowPath = strings.TrimSpace(scope.Path)
		packageKey = strings.TrimSpace(scope.PackageKey)
	}
	return AuthoredEventEndpoint{
		Direction:  direction,
		Kind:       kind,
		FlowID:     flowID,
		FlowPath:   flowPath,
		PackageKey: packageKey,
		Event:      ResolveFlowEventProof(b.source, flowID, eventType),
	}
}

func (b *endpointCensusBuilder) add(endpoint AuthoredEventEndpoint) {
	if endpoint.SourceLine == 0 {
		endpoint.SourceLine = b.endpointSourceLine(endpoint)
	}
	endpoint.ID = eventEndpointID(endpoint)
	if endpoint.ID == "" {
		return
	}
	if _, exists := b.seen[endpoint.ID]; exists {
		return
	}
	b.seen[endpoint.ID] = struct{}{}
	switch endpoint.Direction {
	case EventEndpointProducer:
		b.producers = append(b.producers, endpoint)
	case EventEndpointConsumer:
		b.consumers = append(b.consumers, endpoint)
	case EventEndpointInputPin:
		b.inputPins = append(b.inputPins, endpoint)
	case EventEndpointOutputPin:
		b.outputPins = append(b.outputPins, endpoint)
	}
}

func (b *endpointCensusBuilder) endpointSourceLine(endpoint AuthoredEventEndpoint) int {
	file := strings.TrimSpace(endpoint.SourceFile)
	if file == "" {
		return 0
	}
	root := b.yamlFile(file)
	if root == nil {
		return 0
	}
	eventType := eventidentity.Normalize(endpoint.Event.Authored)
	switch endpoint.Kind {
	case EventEndpointNodeHandler, EventEndpointNodeGenerated:
		return yamlActorEventLine(root, endpoint.NodeID, []string{"subscribes_to", "event_handlers"}, eventType)
	case EventEndpointAgent:
		field := "subscriptions"
		if endpoint.Direction == EventEndpointProducer {
			field = "emit_events"
		}
		return yamlActorEventLine(root, endpoint.AgentID, []string{field}, eventType)
	default:
		return 0
	}
}

func (b *endpointCensusBuilder) yamlFile(file string) *yaml.Node {
	if b.yamlFiles == nil {
		b.yamlFiles = map[string]*yaml.Node{}
	}
	if root, ok := b.yamlFiles[file]; ok {
		return root
	}
	b.yamlFiles[file] = nil
	contents, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var root yaml.Node
	if err := yaml.Unmarshal(contents, &root); err != nil {
		return nil
	}
	b.yamlFiles[file] = &root
	return &root
}

func yamlActorEventLine(root *yaml.Node, actorID string, fields []string, eventType string) int {
	document := yamlDocumentMapping(root)
	actor := yamlMappingValue(document, strings.TrimSpace(actorID))
	if actor == nil || actor.Kind != yaml.MappingNode {
		return 0
	}
	for _, field := range fields {
		value := yamlMappingValue(actor, field)
		if value == nil {
			continue
		}
		switch value.Kind {
		case yaml.MappingNode:
			for i := 0; i+1 < len(value.Content); i += 2 {
				if eventidentity.Normalize(value.Content[i].Value) == eventType {
					return value.Content[i].Line
				}
			}
		case yaml.SequenceNode:
			for _, item := range value.Content {
				if eventidentity.Normalize(item.Value) == eventType {
					return item.Line
				}
			}
		}
	}
	return 0
}

func yamlDocumentMapping(root *yaml.Node) *yaml.Node {
	if root == nil {
		return nil
	}
	if root.Kind == yaml.DocumentNode && len(root.Content) == 1 {
		return root.Content[0]
	}
	return root
}

func yamlMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if strings.TrimSpace(mapping.Content[i].Value) == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}

func (b *endpointCensusBuilder) build() AuthoredEventEndpointCensus {
	sortEventEndpoints(b.producers)
	sortEventEndpoints(b.consumers)
	sortEventEndpoints(b.inputPins)
	sortEventEndpoints(b.outputPins)
	sort.SliceStable(b.assertions, func(i, j int) bool {
		if b.assertions[i].FlowID != b.assertions[j].FlowID {
			return b.assertions[i].FlowID < b.assertions[j].FlowID
		}
		return b.assertions[i].NodeID < b.assertions[j].NodeID
	})
	index := map[string]AuthoredEventEndpoint{}
	for _, endpoints := range [][]AuthoredEventEndpoint{b.producers, b.consumers, b.inputPins, b.outputPins} {
		for _, endpoint := range endpoints {
			index[endpoint.ID] = endpoint
		}
	}
	return AuthoredEventEndpointCensus{
		source:       b.source,
		producers:    cloneEventEndpoints(b.producers),
		consumers:    cloneEventEndpoints(b.consumers),
		inputPins:    cloneEventEndpoints(b.inputPins),
		outputPins:   cloneEventEndpoints(b.outputPins),
		assertions:   append([]NodeProducerAssertion(nil), b.assertions...),
		endpointByID: index,
	}
}

func endpointMatchesProof(source Source, endpoint AuthoredEventEndpoint, proof FlowEventProof) bool {
	if source == nil {
		return false
	}
	if endpoint.Pattern {
		if matched, scoped := ImportBoundaryWildcardSubscriptionMatches(source, endpoint.PackageKey, endpoint.FlowID, "", nil, endpoint.Event.Authored, proof.EventKey()); scoped {
			return matched
		}
	}
	return flowEventMatchesWithoutTopology(source, endpoint.FlowID, endpoint.Event.Authored, proof.EventKey()) ||
		eventidentity.Normalize(endpoint.Event.Canonical) == eventidentity.Normalize(proof.Canonical)
}

func declaredInputIdentityMatches(source Source, endpoint AuthoredEventEndpoint, identity string) bool {
	identity = eventidentity.Normalize(identity)
	if identity == "" {
		return false
	}
	candidates := []string{
		endpoint.PinName,
		endpoint.Event.Authored,
		endpoint.Event.Local,
		endpoint.Event.Canonical,
		endpoint.Event.CatalogKey,
	}
	for _, candidate := range candidates {
		if eventidentity.Normalize(candidate) == identity {
			return true
		}
	}
	if source == nil {
		return false
	}
	return flowEventMatchesWithoutTopology(source, endpoint.FlowID, endpoint.Event.Authored, identity) ||
		flowEventMatchesWithoutTopology(source, endpoint.FlowID, identity, endpoint.Event.Authored)
}

func flowEventMatchesWithoutTopology(source Source, flowID, subscription, eventType string) bool {
	if source == nil {
		return false
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		return bundle.FlowEventMatches(flowID, subscription, eventType)
	}
	return source.FlowEventMatches(flowID, subscription, eventType)
}

func fanInInputMatchesHandler(source Source, endpoint AuthoredEventEndpoint, handlerEvent string) bool {
	handlerEvent = eventidentity.Normalize(handlerEvent)
	if handlerEvent == "" {
		return false
	}
	if eventidentity.Normalize(endpoint.PinName) == handlerEvent {
		return true
	}
	return declaredInputIdentityMatches(source, endpoint, handlerEvent)
}

func resolveNodeHandlerProof(source Source, nodeID, eventType string) (string, bool) {
	if source == nil {
		return "", false
	}
	if _, ok := source.NodeEventHandlers(nodeID)[strings.TrimSpace(eventType)]; ok {
		return strings.TrimSpace(eventType), true
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		resolution := bundle.ResolveNodeEventHandler(nodeID, eventType)
		if resolution.Matched {
			return strings.TrimSpace(resolution.AuthoredEventType), true
		}
	}
	if _, ok := source.NodeEventHandler(nodeID, eventType); ok {
		return strings.TrimSpace(eventType), true
	}
	return "", false
}

func eventEndpointID(endpoint AuthoredEventEndpoint) string {
	eventKey := endpoint.Event.EventKey()
	if eventKey == "" {
		eventKey = endpoint.Event.Authored
	}
	parts := []string{
		string(endpoint.Direction), string(endpoint.Kind), endpoint.FlowID, endpoint.PackageKey, eventKey,
		endpoint.NodeID, endpoint.HandlerEvent, endpoint.AgentID, endpoint.Role,
		endpoint.TimerID, endpoint.PinName, endpoint.Site, endpoint.SourceLocation,
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	if eventKey == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "endpoint-" + hex.EncodeToString(digest[:8])
}

func nodeEventKey(nodeID, eventType string) string {
	return strings.TrimSpace(nodeID) + "\x00" + eventidentity.Normalize(eventType)
}

func cloneEventEndpoints(in []AuthoredEventEndpoint) []AuthoredEventEndpoint {
	if len(in) == 0 {
		return nil
	}
	out := make([]AuthoredEventEndpoint, len(in))
	copy(out, in)
	return out
}

func sortEventEndpoints(endpoints []AuthoredEventEndpoint) {
	sort.SliceStable(endpoints, func(i, j int) bool { return endpoints[i].ID < endpoints[j].ID })
}

func sortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func normalizedSortedStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		if value = eventidentity.Normalize(value); value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedFlowScopes(scopes []FlowScope) []FlowScope {
	out := append([]FlowScope(nil), scopes...)
	sort.SliceStable(out, func(i, j int) bool { return strings.TrimSpace(out[i].ID) < strings.TrimSpace(out[j].ID) })
	return out
}

func sortedInputPins(pins []runtimecontracts.FlowInputEventPin) []runtimecontracts.FlowInputEventPin {
	out := append([]runtimecontracts.FlowInputEventPin(nil), pins...)
	sort.SliceStable(out, func(i, j int) bool { return strings.TrimSpace(out[i].PinName()) < strings.TrimSpace(out[j].PinName()) })
	return out
}

func sortedOutputPins(pins []runtimecontracts.FlowOutputEventPin) []runtimecontracts.FlowOutputEventPin {
	out := append([]runtimecontracts.FlowOutputEventPin(nil), pins...)
	sort.SliceStable(out, func(i, j int) bool { return strings.TrimSpace(out[i].PinName()) < strings.TrimSpace(out[j].PinName()) })
	return out
}
