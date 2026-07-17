package routingtopology

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	SchemaVersion   = "routing-topology/v1"
	SourceAuthority = "projection_only_existing_contract_owners"

	FailureConnectReceiverPinCollision = "connect_receiver_pin_delivery_collision"
)

type DeliveryScope string

const (
	DeliveryScopeTypedPubSub       DeliveryScope = "typed_pubsub"
	DeliveryScopeInterFlowConnect  DeliveryScope = "inter_flow_connect"
	RootInputSourceStandingIngress               = "standing_ingress"
)

type EventIdentity struct {
	Authored  string `json:"authored,omitempty"`
	Local     string `json:"local,omitempty"`
	Canonical string `json:"canonical"`
}

type Endpoint struct {
	ID             string                              `json:"id"`
	Direction      semanticview.EventEndpointDirection `json:"direction"`
	Kind           semanticview.EventEndpointKind      `json:"kind"`
	FlowID         string                              `json:"flow_id,omitempty"`
	FlowPath       string                              `json:"flow_path,omitempty"`
	PackageKey     string                              `json:"package_key,omitempty"`
	Event          EventIdentity                       `json:"event"`
	Pattern        bool                                `json:"pattern,omitempty"`
	NodeID         string                              `json:"node_id,omitempty"`
	HandlerEvent   string                              `json:"handler_event,omitempty"`
	AgentID        string                              `json:"agent_id,omitempty"`
	Role           string                              `json:"role,omitempty"`
	TimerID        string                              `json:"timer_id,omitempty"`
	PinName        string                              `json:"pin_name,omitempty"`
	Site           string                              `json:"site,omitempty"`
	SourceFile     string                              `json:"source_file,omitempty"`
	SourceLine     int                                 `json:"source_line,omitempty"`
	SourceLocation string                              `json:"source_location,omitempty"`
	ResolutionMode string                              `json:"resolution_mode,omitempty"`
}

type BoundaryExposure struct {
	ID       string        `json:"id"`
	Event    EventIdentity `json:"event"`
	Producer Endpoint      `json:"producer"`
	Output   Endpoint      `json:"output"`
}

type RootInputSource struct {
	ID               string             `json:"id"`
	Kind             string             `json:"kind"`
	Alias            string             `json:"alias"`
	Provider         string             `json:"provider"`
	Target           RootInputTarget    `json:"target"`
	AuthoredLocation string             `json:"authored_location,omitempty"`
	Admission        RootInputAdmission `json:"admission"`
}

type RootInputAdmission struct {
	Kind                   string `json:"kind"`
	PackID                 string `json:"pack_id,omitempty"`
	DeclaredAuthentication string `json:"declared_authentication,omitempty"`
	Event                  string `json:"event,omitempty"`
	Acknowledgement        string `json:"acknowledgement,omitempty"`
}

type RootInputTarget struct {
	PackageKey string `json:"package_key,omitempty"`
	FlowID     string `json:"flow_id"`
	FlowPath   string `json:"flow_path,omitempty"`
}

type Edge struct {
	ID                        string        `json:"id"`
	Scope                     DeliveryScope `json:"scope"`
	Event                     EventIdentity `json:"event"`
	Producer                  Endpoint      `json:"producer"`
	Consumer                  Endpoint      `json:"consumer"`
	TypedPubSub               *TypedPubSub  `json:"typed_pubsub,omitempty"`
	Boundary                  *Boundary     `json:"boundary,omitempty"`
	Resolution                *Resolution   `json:"resolution,omitempty"`
	RequiresRuntimeResolution bool          `json:"requires_runtime_resolution"`
}

type TypedPubSub struct {
	Match         string                         `json:"match"`
	Boundary      string                         `json:"boundary"`
	Authorization *TypedPubSubAuthorizationProof `json:"authorization,omitempty"`
}

type TypedPubSubAuthorizationProof struct {
	ParentPackageKey string `json:"parent_package_key,omitempty"`
	ChildPackageKey  string `json:"child_package_key"`
	ImportLabel      string `json:"import_label,omitempty"`
	Source           string `json:"source,omitempty"`
	EventPattern     string `json:"event_pattern"`
	MatchPattern     string `json:"match_pattern"`
	LocalizedEvent   string `json:"localized_event"`
	RouteSource      string `json:"route_source"`
}

type Boundary struct {
	PackageKey       string `json:"package_key,omitempty"`
	AuthoredLocation string `json:"authored_location,omitempty"`
	From             string `json:"from"`
	To               string `json:"to"`
	OutputPin        string `json:"output_pin"`
	InputPin         string `json:"input_pin"`
}

type Resolution struct {
	Mode        string       `json:"mode"`
	TargetKind  string       `json:"target_kind"`
	Address     *Address     `json:"address,omitempty"`
	InstanceKey *InstanceKey `json:"instance_key,omitempty"`
	FanIn       *FanIn       `json:"fan_in,omitempty"`
	Reply       *Reply       `json:"reply,omitempty"`
	Map         []MapEntry   `json:"map,omitempty"`
}

type Address struct {
	By          string `json:"by,omitempty"`
	Source      string `json:"source,omitempty"`
	Target      string `json:"target,omitempty"`
	Cardinality string `json:"cardinality,omitempty"`
	Mode        string `json:"mode,omitempty"`
}

type InstanceKey struct {
	Mode       string               `json:"mode,omitempty"`
	Fields     []string             `json:"fields,omitempty"`
	Mappings   []InstanceKeyMapping `json:"mappings,omitempty"`
	Mint       string               `json:"mint,omitempty"`
	As         string               `json:"as,omitempty"`
	OnMissing  string               `json:"on_missing,omitempty"`
	OnConflict string               `json:"on_conflict,omitempty"`
}

type InstanceKeyMapping struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	Explicit bool   `json:"explicit"`
}

type FanIn struct {
	Aggregation string   `json:"aggregation"`
	Window      string   `json:"window"`
	DedupBy     []string `json:"dedup_by"`
	Singleton   string   `json:"singleton"`
}

type Reply struct {
	Role              string `json:"role"`
	RequesterFlowID   string `json:"requester_flow_id"`
	RequestOutputPin  string `json:"request_output_pin"`
	ReplyInputPin     string `json:"reply_input_pin"`
	ProviderFlowID    string `json:"provider_flow_id"`
	ProviderInputPin  string `json:"provider_input_pin"`
	ProviderOutputPin string `json:"provider_output_pin"`
	CorrelationKey    string `json:"correlation_key,omitempty"`
}

type MapEntry struct {
	Key    string `json:"key"`
	Source string `json:"source,omitempty"`
	Target string `json:"target,omitempty"`
}

type Issue struct {
	ID               string `json:"id"`
	CheckID          string `json:"check_id,omitempty"`
	Severity         string `json:"severity,omitempty"`
	Location         string `json:"location,omitempty"`
	PackageKey       string `json:"package_key,omitempty"`
	From             string `json:"from,omitempty"`
	To               string `json:"to,omitempty"`
	Failure          string `json:"failure"`
	Detail           string `json:"detail,omitempty"`
	Message          string `json:"message,omitempty"`
	Remediation      string `json:"remediation,omitempty"`
	AuthoredLocation string `json:"authored_location,omitempty"`
}

type Topology struct {
	SchemaVersion     string             `json:"schema_version"`
	ProjectionOnly    bool               `json:"projection_only"`
	SourceAuthority   string             `json:"source_authority"`
	Producers         []Endpoint         `json:"producers"`
	Consumers         []Endpoint         `json:"consumers"`
	InputPins         []Endpoint         `json:"input_pins"`
	OutputPins        []Endpoint         `json:"output_pins"`
	RootInputSources  []RootInputSource  `json:"root_input_sources"`
	BoundaryExposures []BoundaryExposure `json:"boundary_exposures"`
	Edges             []Edge             `json:"edges"`
	Issues            []Issue            `json:"issues"`
}

func Build(source semanticview.Source) Topology {
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	relations := census.ResolveTypedPubSubRelations()
	plans, planIssues := pinrouting.LowerCompositionConnectRoutePlans(source)
	builder := topologyBuilder{
		census:        census,
		seenEdges:     map[string]struct{}{},
		seenExposures: map[string]struct{}{},
	}
	builder.addTypedPubSubRelations(relations)
	builder.addBoundaryExposures()
	builder.addConnectEdges(plans)
	topology := Topology{
		SchemaVersion:     SchemaVersion,
		ProjectionOnly:    true,
		SourceAuthority:   SourceAuthority,
		Producers:         endpointViews(census.Producers()),
		Consumers:         endpointViews(census.Consumers()),
		InputPins:         endpointViews(census.InputPins()),
		OutputPins:        endpointViews(census.OutputPins()),
		RootInputSources:  rootInputSourceViews(source),
		BoundaryExposures: builder.sortedExposures(),
		Edges:             builder.sortedEdges(),
		Issues:            issueViews(planIssues, builder.relationIssues),
	}
	topology.Issues = append(topology.Issues, connectReceiverPinCollisionIssues(topology.Edges)...)
	sort.SliceStable(topology.Issues, func(i, j int) bool { return topology.Issues[i].ID < topology.Issues[j].ID })
	return topology
}

type connectReceiverPinDeliveryFact struct {
	sourceKey        string
	subscriberType   string
	subscriberID     string
	targetKey        string
	receiverLocalKey string
	receiverLabel    string
	authoredLocation string
}

func connectReceiverPinCollisionIssues(edges []Edge) []Issue {
	typedConsumers := make(map[string][]Edge)
	for _, edge := range edges {
		if edge.Scope == DeliveryScopeTypedPubSub {
			typedConsumers[edge.Producer.ID] = append(typedConsumers[edge.Producer.ID], edge)
		}
	}

	type collision struct {
		fact   connectReceiverPinDeliveryFact
		locals map[string]string
	}
	byDelivery := make(map[string]*collision)
	for _, edge := range edges {
		if edge.Scope != DeliveryScopeInterFlowConnect || edge.Boundary == nil {
			continue
		}
		for _, local := range typedConsumers[edge.Consumer.ID] {
			fact, ok := connectReceiverPinFact(edge, local)
			if !ok {
				continue
			}
			key := strings.Join([]string{fact.sourceKey, fact.subscriberType, fact.subscriberID, fact.targetKey}, "\x00")
			entry := byDelivery[key]
			if entry == nil {
				entry = &collision{fact: fact, locals: map[string]string{}}
				byDelivery[key] = entry
			}
			entry.locals[fact.receiverLocalKey] = fact.receiverLabel
		}
	}

	keys := make([]string, 0, len(byDelivery))
	for key, entry := range byDelivery {
		if len(entry.locals) > 1 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	issues := make([]Issue, 0, len(keys))
	for _, key := range keys {
		entry := byDelivery[key]
		locals := make([]string, 0, len(entry.locals))
		for _, label := range entry.locals {
			locals = append(locals, label)
		}
		sort.Strings(locals)
		message := fmt.Sprintf(
			"source event %s reaches %s %s at target %s through multiple receiver pins: %s",
			entry.fact.sourceKey,
			entry.fact.subscriberType,
			entry.fact.subscriberID,
			entry.fact.targetKey,
			strings.Join(locals, ", "),
		)
		issue := Issue{
			CheckID:          FailureConnectReceiverPinCollision,
			Severity:         "error",
			Failure:          FailureConnectReceiverPinCollision,
			Location:         entry.fact.targetKey,
			From:             entry.fact.sourceKey,
			To:               entry.fact.subscriberType + ":" + entry.fact.subscriberID,
			Detail:           message,
			Message:          message,
			Remediation:      "Route the source event to distinct subscribers or targets, or consolidate the receiver pins behind one handler. One event x subscriber cannot select multiple receiver-local handlers.",
			AuthoredLocation: entry.fact.authoredLocation,
		}
		issue.ID = issueID(issue)
		issues = append(issues, issue)
	}
	return issues
}

func connectReceiverPinFact(connect Edge, local Edge) (connectReceiverPinDeliveryFact, bool) {
	subscriberType := ""
	subscriberID := ""
	switch local.Consumer.Kind {
	case semanticview.EventEndpointNodeHandler:
		subscriberType = "node"
		subscriberID = strings.TrimSpace(local.Consumer.NodeID)
	case semanticview.EventEndpointAgent:
		subscriberType = "agent"
		subscriberID = strings.TrimSpace(local.Consumer.AgentID)
	default:
		return connectReceiverPinDeliveryFact{}, false
	}
	if subscriberID == "" {
		return connectReceiverPinDeliveryFact{}, false
	}
	sourceKey := strings.Join([]string{
		strings.TrimSpace(connect.Boundary.PackageKey),
		strings.TrimSpace(connect.Boundary.From),
		strings.TrimSpace(connect.Event.Canonical),
	}, ":")
	targetKey, targetProven := connectReceiverPinStaticTargetKey(connect, local)
	if !targetProven {
		return connectReceiverPinDeliveryFact{}, false
	}
	receiverLocalKey := strings.Join([]string{
		strings.TrimSpace(connect.Boundary.To),
		strings.TrimSpace(connect.Consumer.Event.Canonical),
	}, "\x00")
	receiverLabel := strings.TrimSpace(connect.Boundary.To)
	if eventName := strings.TrimSpace(connect.Consumer.Event.Canonical); eventName != "" {
		receiverLabel += " (" + eventName + ")"
	}
	return connectReceiverPinDeliveryFact{
		sourceKey:        sourceKey,
		subscriberType:   subscriberType,
		subscriberID:     subscriberID,
		targetKey:        targetKey,
		receiverLocalKey: receiverLocalKey,
		receiverLabel:    receiverLabel,
		authoredLocation: strings.TrimSpace(connect.Boundary.AuthoredLocation),
	}, true
}

func connectReceiverPinStaticTargetKey(connect Edge, local Edge) (string, bool) {
	targetKey := strings.Trim(strings.TrimSpace(local.Consumer.FlowPath), "/")
	if targetKey == "" {
		return "<global-root>", true
	}
	if connect.RequiresRuntimeResolution || connect.Resolution == nil || strings.TrimSpace(connect.Resolution.Mode) != string(pinrouting.ConnectResolutionStatic) {
		return "", false
	}
	return targetKey, true
}

func rootInputSourceViews(source semanticview.Source) []RootInputSource {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return []RootInputSource{}
	}
	out := make([]RootInputSource, 0)
	for _, pkg := range bundle.PackageTree {
		for flowIndex, ref := range pkg.Manifest.Flows {
			if ref.Ingress == nil {
				continue
			}
			flowID := strings.TrimSpace(ref.ID)
			alias := strings.TrimSpace(ref.Ingress.Alias)
			if alias == "" {
				alias = flowID
			}
			target := RootInputTarget{
				PackageKey: strings.TrimSpace(pkg.Key),
				FlowID:     flowID,
				FlowPath:   strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"),
			}
			sourceFile := strings.TrimSpace(pkg.Paths.PackageFile)
			if sourceFile == "" {
				sourceFile = "package.yaml"
			}
			for providerIndex, binding := range ref.Ingress.Providers {
				admissionKind := strings.ToLower(strings.TrimSpace(binding.Admission.Kind))
				if admissionKind == "" {
					admissionKind = "pack-required"
				}
				item := RootInputSource{
					Kind:             RootInputSourceStandingIngress,
					Alias:            alias,
					Provider:         strings.TrimSpace(binding.Provider),
					Target:           target,
					AuthoredLocation: sourceFile + ":flows[" + strconv.Itoa(flowIndex) + "].ingress.providers[" + strconv.Itoa(providerIndex) + "]",
					Admission:        RootInputAdmission{Kind: admissionKind, Event: strings.TrimSpace(binding.Admission.Event), Acknowledgement: strings.TrimSpace(binding.Admission.Acknowledge)},
				}
				if binding.Admission.Pack != nil {
					item.Admission.PackID = strings.TrimSpace(binding.Admission.Pack.ID)
				}
				if binding.Admission.Authentication != nil {
					item.Admission.DeclaredAuthentication = strings.ToUpper(strings.TrimSpace(binding.Admission.Authentication.Kind))
					if item.Admission.DeclaredAuthentication == "NONE" {
						item.Admission.DeclaredAuthentication = "UNAUTHENTICATED"
					}
				}
				item.ID = rootInputSourceID(item)
				out = append(out, item)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].Alias, out[i].Provider, out[i].Target.PackageKey, out[i].Target.FlowID, out[i].AuthoredLocation, out[i].ID}, "\x00")
		right := strings.Join([]string{out[j].Alias, out[j].Provider, out[j].Target.PackageKey, out[j].Target.FlowID, out[j].AuthoredLocation, out[j].ID}, "\x00")
		return left < right
	})
	return out
}

type topologyBuilder struct {
	census         semanticview.AuthoredEventEndpointCensus
	edges          []Edge
	exposures      []BoundaryExposure
	seenEdges      map[string]struct{}
	seenExposures  map[string]struct{}
	relationIssues []semanticview.TypedPubSubConsumerIssue
}

func (b *topologyBuilder) addTypedPubSubRelations(relations semanticview.TypedPubSubRelations) {
	b.relationIssues = append(b.relationIssues, relations.Issues...)
	for _, match := range relations.Matches {
		if match.Producer.Direction == semanticview.EventEndpointProducer && !isExecutableEndpoint(match.Producer) {
			continue
		}
		if !isExecutableEndpoint(match.Consumer) {
			continue
		}
		b.addEdge(Edge{
			Scope:       DeliveryScopeTypedPubSub,
			Event:       eventView(match.Event),
			Producer:    endpointView(match.Producer),
			Consumer:    endpointView(match.Consumer),
			TypedPubSub: typedPubSubView(match),
		})
	}
}

func typedPubSubView(match semanticview.TypedPubSubConsumerMatch) *TypedPubSub {
	view := &TypedPubSub{
		Match:    string(match.Kind),
		Boundary: string(match.Boundary),
	}
	if match.Authorization != nil {
		view.Authorization = &TypedPubSubAuthorizationProof{
			ParentPackageKey: strings.TrimSpace(match.Authorization.ParentPackageKey),
			ChildPackageKey:  strings.TrimSpace(match.Authorization.ChildPackageKey),
			ImportLabel:      strings.TrimSpace(match.Authorization.ImportLabel),
			Source:           strings.TrimSpace(match.Authorization.Source),
			EventPattern:     strings.TrimSpace(match.Authorization.EventPattern),
			MatchPattern:     strings.TrimSpace(match.Authorization.MatchPattern),
			LocalizedEvent:   strings.TrimSpace(match.Authorization.LocalizedEvent),
			RouteSource:      strings.TrimSpace(match.Authorization.RouteSource),
		}
	}
	return view
}

func (b *topologyBuilder) addBoundaryExposures() {
	for _, output := range b.census.OutputPins() {
		for _, producer := range b.census.MatchingProducers(output.FlowID, output.Event.EventKey()) {
			exposure := BoundaryExposure{
				Event:    eventView(output.Event),
				Producer: endpointView(producer),
				Output:   endpointView(output),
			}
			exposure.ID = strings.Join([]string{producer.ID, output.ID}, "->")
			if _, exists := b.seenExposures[exposure.ID]; exists {
				continue
			}
			b.seenExposures[exposure.ID] = struct{}{}
			b.exposures = append(b.exposures, exposure)
		}
	}
}

func (b *topologyBuilder) addConnectEdges(plans []pinrouting.ConnectRoutePlan) {
	for _, plan := range plans {
		producerEndpoints := b.census.MatchingProducers(plan.Source.FlowID, plan.Source.ResolvedEvent)
		if len(producerEndpoints) == 0 {
			producerEndpoints = b.census.MatchingProducers(plan.Source.FlowID, plan.Source.Event)
		}
		if len(producerEndpoints) == 0 {
			if endpoint, ok := findPinEndpoint(b.census.OutputPins(), plan.Source.FlowID, plan.Source.Pin); ok {
				producerEndpoints = []semanticview.AuthoredEventEndpoint{endpoint}
			}
		}
		consumer, ok := findPinEndpoint(b.census.InputPins(), plan.Receiver.FlowID, plan.Receiver.Pin)
		if !ok {
			continue
		}
		for _, producer := range producerEndpoints {
			b.addEdge(Edge{
				Scope:                     DeliveryScopeInterFlowConnect,
				Event:                     eventIdentity(plan.Source.Event, plan.Source.ResolvedEvent),
				Producer:                  endpointView(producer),
				Consumer:                  endpointView(consumer),
				Boundary:                  boundaryView(plan),
				Resolution:                resolutionView(plan),
				RequiresRuntimeResolution: plan.RequiresRuntimeResolution,
			})
		}
	}
}

func (b *topologyBuilder) addEdge(edge Edge) {
	edge.ID = edgeID(edge)
	if edge.ID == "" {
		return
	}
	if _, exists := b.seenEdges[edge.ID]; exists {
		return
	}
	b.seenEdges[edge.ID] = struct{}{}
	b.edges = append(b.edges, edge)
}

func (b *topologyBuilder) sortedEdges() []Edge {
	out := make([]Edge, len(b.edges))
	copy(out, b.edges)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (b *topologyBuilder) sortedExposures() []BoundaryExposure {
	out := make([]BoundaryExposure, len(b.exposures))
	copy(out, b.exposures)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func isExecutableEndpoint(endpoint semanticview.AuthoredEventEndpoint) bool {
	return endpoint.Kind != semanticview.EventEndpointExternal && endpoint.Kind != semanticview.EventEndpointPlatform
}

func endpointViews(endpoints []semanticview.AuthoredEventEndpoint) []Endpoint {
	out := make([]Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		out = append(out, endpointView(endpoint))
	}
	return out
}

func endpointView(endpoint semanticview.AuthoredEventEndpoint) Endpoint {
	return Endpoint{
		ID:             endpoint.ID,
		Direction:      endpoint.Direction,
		Kind:           endpoint.Kind,
		FlowID:         strings.TrimSpace(endpoint.FlowID),
		FlowPath:       strings.TrimSpace(endpoint.FlowPath),
		PackageKey:     strings.TrimSpace(endpoint.PackageKey),
		Event:          eventView(endpoint.Event),
		Pattern:        endpoint.Pattern,
		NodeID:         strings.TrimSpace(endpoint.NodeID),
		HandlerEvent:   strings.TrimSpace(endpoint.HandlerEvent),
		AgentID:        strings.TrimSpace(endpoint.AgentID),
		Role:           strings.TrimSpace(endpoint.Role),
		TimerID:        strings.TrimSpace(endpoint.TimerID),
		PinName:        strings.TrimSpace(endpoint.PinName),
		Site:           strings.TrimSpace(endpoint.Site),
		SourceFile:     strings.TrimSpace(endpoint.SourceFile),
		SourceLine:     endpoint.SourceLine,
		SourceLocation: strings.TrimSpace(endpoint.SourceLocation),
		ResolutionMode: strings.TrimSpace(endpoint.ResolutionMode),
	}
}

func eventView(proof semanticview.FlowEventProof) EventIdentity {
	canonical := strings.TrimSpace(proof.Canonical)
	if canonical == "" {
		canonical = strings.TrimSpace(proof.EventKey())
	}
	return EventIdentity{
		Authored:  strings.TrimSpace(proof.Authored),
		Local:     strings.TrimSpace(proof.Local),
		Canonical: canonical,
	}
}

func eventIdentity(authored, canonical string) EventIdentity {
	canonical = strings.TrimSpace(canonical)
	if canonical == "" {
		canonical = strings.TrimSpace(authored)
	}
	return EventIdentity{Authored: strings.TrimSpace(authored), Local: strings.TrimSpace(authored), Canonical: canonical}
}

func findPinEndpoint(endpoints []semanticview.AuthoredEventEndpoint, flowID, pinName string) (semanticview.AuthoredEventEndpoint, bool) {
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint.FlowID) == flowID && strings.TrimSpace(endpoint.PinName) == pinName {
			return endpoint, true
		}
	}
	return semanticview.AuthoredEventEndpoint{}, false
}

func boundaryView(plan pinrouting.ConnectRoutePlan) *Boundary {
	return &Boundary{
		PackageKey:       strings.TrimSpace(plan.PackageKey),
		AuthoredLocation: strings.TrimSpace(plan.AuthoredLocation),
		From:             connectEndpointRef(plan.Source),
		To:               connectEndpointRef(plan.Receiver),
		OutputPin:        strings.TrimSpace(plan.Source.Pin),
		InputPin:         strings.TrimSpace(plan.Receiver.Pin),
	}
}

func connectEndpointRef(endpoint pinrouting.ConnectRoutePlanEndpoint) string {
	if endpoint.Root {
		return "." + strings.TrimSpace(endpoint.Pin)
	}
	return strings.TrimSpace(endpoint.FlowID) + "." + strings.TrimSpace(endpoint.Pin)
}

func resolutionView(plan pinrouting.ConnectRoutePlan) *Resolution {
	resolution := &Resolution{
		Mode:       string(plan.ResolutionKind),
		TargetKind: string(plan.TargetKind),
	}
	if plan.Address != nil {
		resolution.Address = &Address{
			By:          strings.TrimSpace(plan.Address.By),
			Source:      strings.TrimSpace(plan.Address.Source),
			Target:      strings.TrimSpace(plan.Address.Target),
			Cardinality: strings.TrimSpace(plan.Address.Cardinality),
			Mode:        strings.TrimSpace(plan.Address.Mode),
		}
	}
	if plan.InstanceKey != nil {
		resolution.Mode = strings.TrimSpace(plan.InstanceKey.Mode)
		if resolution.Mode == "" {
			resolution.Mode = string(plan.ResolutionKind)
		}
		instance := &InstanceKey{
			Mode:       strings.TrimSpace(plan.InstanceKey.Mode),
			Fields:     normalizedStrings(plan.InstanceKey.Fields),
			Mint:       strings.TrimSpace(plan.InstanceKey.Mint),
			As:         strings.TrimSpace(plan.InstanceKey.As),
			OnMissing:  strings.TrimSpace(plan.InstanceKey.OnMissing),
			OnConflict: strings.TrimSpace(plan.InstanceKey.OnConflict),
		}
		for _, mapping := range plan.InstanceKey.Mappings {
			instance.Mappings = append(instance.Mappings, InstanceKeyMapping{Source: strings.TrimSpace(mapping.Source), Target: strings.TrimSpace(mapping.Target), Explicit: mapping.Explicit})
		}
		resolution.InstanceKey = instance
	}
	if plan.FanIn != nil {
		resolution.Mode = runtimecontracts.FlowInputResolutionModeFanIn
		resolution.FanIn = &FanIn{
			Aggregation: strings.TrimSpace(plan.FanIn.Aggregation),
			Window:      strings.TrimSpace(plan.FanIn.Window),
			DedupBy:     normalizedStrings(plan.FanIn.DedupBy),
			Singleton:   strings.TrimSpace(plan.FanIn.Singleton),
		}
	}
	if plan.ReplyResolution != nil {
		resolution.Mode = runtimecontracts.FlowInputResolutionModeReply
		resolution.Reply = &Reply{
			Role:              strings.TrimSpace(plan.ReplyResolution.Role),
			RequesterFlowID:   strings.TrimSpace(plan.ReplyResolution.RequesterFlowID),
			RequestOutputPin:  strings.TrimSpace(plan.ReplyResolution.RequestOutputPin),
			ReplyInputPin:     strings.TrimSpace(plan.ReplyResolution.ReplyInputPin),
			ProviderFlowID:    strings.TrimSpace(plan.ReplyResolution.ProviderFlowID),
			ProviderInputPin:  strings.TrimSpace(plan.ReplyResolution.ProviderInputPin),
			ProviderOutputPin: strings.TrimSpace(plan.ReplyResolution.ProviderOutputPin),
			CorrelationKey:    strings.TrimSpace(plan.ReplyResolution.CorrelationKey),
		}
	}
	for _, entry := range plan.Map {
		resolution.Map = append(resolution.Map, MapEntry{Key: strings.TrimSpace(entry.Key), Source: strings.TrimSpace(entry.Source), Target: strings.TrimSpace(entry.Target)})
	}
	return resolution
}

func issueViews(connectIssues []pinrouting.ConnectRoutePlanIssue, relationIssues []semanticview.TypedPubSubConsumerIssue) []Issue {
	out := make([]Issue, 0, len(connectIssues)+len(relationIssues))
	for _, issue := range connectIssues {
		view := Issue{
			PackageKey:       strings.TrimSpace(issue.Connect.PackageKey),
			From:             strings.TrimSpace(issue.Connect.From),
			To:               strings.TrimSpace(issue.Connect.To),
			Failure:          string(issue.Failure),
			Detail:           strings.TrimSpace(issue.Detail),
			AuthoredLocation: strings.TrimSpace(issue.AuthoredLocation),
		}
		view.ID = issueID(view)
		out = append(out, view)
	}
	for _, issue := range relationIssues {
		view := Issue{
			Location:    strings.TrimSpace(issue.Event.EventKey()),
			From:        strings.TrimSpace(issue.Producer.ID),
			To:          strings.TrimSpace(issue.Consumer.ID),
			Failure:     strings.TrimSpace(issue.Failure),
			Detail:      strings.Join(issue.Evidence(), ", "),
			Message:     issue.Message(),
			Remediation: issue.Remediation(),
		}
		view.ID = issueID(view)
		out = append(out, view)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].PackageKey, out[i].From, out[i].To, out[i].Failure, out[i].Detail}, "\x00")
		right := strings.Join([]string{out[j].PackageKey, out[j].From, out[j].To, out[j].Failure, out[j].Detail}, "\x00")
		return left < right
	})
	return out
}

func NewDiagnosticIssue(checkID, severity, location, message, remediation, authoredLocation string) Issue {
	issue := Issue{
		CheckID:          strings.TrimSpace(checkID),
		Severity:         strings.TrimSpace(severity),
		Failure:          strings.TrimSpace(checkID),
		Detail:           strings.TrimSpace(message),
		Message:          strings.TrimSpace(message),
		Remediation:      strings.TrimSpace(remediation),
		AuthoredLocation: strings.TrimSpace(authoredLocation),
		Location:         strings.TrimSpace(location),
	}
	issue.ID = issueID(issue)
	return issue
}

func WithIssues(topology Topology, additional ...Issue) Topology {
	seen := make(map[string]struct{}, len(topology.Issues)+len(additional))
	issues := make([]Issue, 0, len(topology.Issues)+len(additional))
	for _, issue := range append(append([]Issue(nil), topology.Issues...), additional...) {
		if strings.TrimSpace(issue.ID) == "" {
			issue.ID = issueID(issue)
		}
		if _, ok := seen[issue.ID]; ok {
			continue
		}
		seen[issue.ID] = struct{}{}
		issues = append(issues, issue)
	}
	sort.SliceStable(issues, func(i, j int) bool { return issues[i].ID < issues[j].ID })
	topology.Issues = issues
	return topology
}

func issueID(issue Issue) string {
	parts := []string{issue.CheckID, issue.Severity, issue.Location, issue.PackageKey, issue.From, issue.To, issue.Failure, issue.Detail, issue.AuthoredLocation}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "issue-" + hex.EncodeToString(digest[:8])
}

func edgeID(edge Edge) string {
	if edge.Producer.ID == "" || edge.Consumer.ID == "" {
		return ""
	}
	parts := []string{string(edge.Scope), edge.Event.Canonical, edge.Producer.ID, edge.Consumer.ID}
	if edge.TypedPubSub != nil {
		parts = append(parts, edge.TypedPubSub.Match, edge.TypedPubSub.Boundary)
		if edge.TypedPubSub.Authorization != nil {
			parts = append(parts,
				edge.TypedPubSub.Authorization.RouteSource,
				edge.TypedPubSub.Authorization.ParentPackageKey,
				edge.TypedPubSub.Authorization.ChildPackageKey,
				edge.TypedPubSub.Authorization.ImportLabel,
				edge.TypedPubSub.Authorization.Source,
				edge.TypedPubSub.Authorization.EventPattern,
				edge.TypedPubSub.Authorization.MatchPattern,
				edge.TypedPubSub.Authorization.LocalizedEvent,
			)
		}
	}
	if edge.Boundary != nil {
		parts = append(parts, edge.Boundary.PackageKey, edge.Boundary.From, edge.Boundary.To)
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "route-" + hex.EncodeToString(digest[:8])
}

func rootInputSourceID(source RootInputSource) string {
	parts := []string{
		strings.TrimSpace(source.Kind),
		strings.TrimSpace(source.Alias),
		strings.TrimSpace(source.Provider),
		strings.TrimSpace(source.Target.PackageKey),
		strings.TrimSpace(source.Target.FlowID),
		strings.TrimSpace(source.Target.FlowPath),
		strings.TrimSpace(source.AuthoredLocation),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "root-input-" + hex.EncodeToString(digest[:8])
}

func normalizedStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
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
