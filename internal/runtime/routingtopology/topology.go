package routingtopology

import (
	"crypto/sha256"
	"encoding/binary"
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
	Sink           string                              `json:"sink,omitempty"`
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
	FlowPath string `json:"flow_path"`
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
	SourceFlowPath string `json:"source_flow_path,omitempty"`
	TargetFlowPath string `json:"target_flow_path"`
	RouteLabel     string `json:"route_label,omitempty"`
	Source         string `json:"source,omitempty"`
	EventPattern   string `json:"event_pattern"`
	MatchPattern   string `json:"match_pattern"`
	LocalizedEvent string `json:"localized_event"`
	RouteSource    string `json:"route_source"`
}

type Boundary struct {
	OwnerFlowPath    string `json:"owner_flow_path,omitempty"`
	AuthoredLocation string `json:"authored_location,omitempty"`
	From             string `json:"from"`
	To               string `json:"to"`
	OutputPin        string `json:"output_pin"`
	InputPin         string `json:"input_pin"`
}

type Resolution struct {
	Mode        string       `json:"mode"`
	TargetKind  string       `json:"target_kind"`
	InstanceKey *InstanceKey `json:"instance_key,omitempty"`
	FanIn       *FanIn       `json:"fan_in,omitempty"`
	Reply       *Reply       `json:"reply,omitempty"`
}

type InstanceKey struct {
	Mode        string `json:"mode,omitempty"`
	Field       string `json:"field"`
	SourceKind  string `json:"source_kind,omitempty"`
	SourcePath  string `json:"source_path,omitempty"`
	DerivedFrom string `json:"derived_from,omitempty"`
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

type Issue struct {
	ID               string `json:"id"`
	CheckID          string `json:"check_id,omitempty"`
	Severity         string `json:"severity,omitempty"`
	Location         string `json:"location,omitempty"`
	OwnerFlowPath    string `json:"owner_flow_path,omitempty"`
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
	connectGraph := pinrouting.CompileConnectGraph(source)
	plans, planIssues := connectGraph.Plans(), connectGraph.Issues()
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
	topology.Issues = append(topology.Issues, connectReceiverPinCollisionIssueViews(connectGraph.ReceiverPinCollisions())...)
	sort.SliceStable(topology.Issues, func(i, j int) bool { return topology.Issues[i].ID < topology.Issues[j].ID })
	return topology
}

func connectReceiverPinCollisionIssueViews(collisions []pinrouting.ConnectReceiverPinCollision) []Issue {
	issues := make([]Issue, 0, len(collisions))
	for _, collision := range collisions {
		target := collision.Target().FlowInstance
		if target == "" {
			target = "<global-root>"
		}
		message := collision.Message()
		issue := Issue{
			CheckID:          pinrouting.ConnectReceiverPinCollisionFailure,
			Severity:         "error",
			Failure:          pinrouting.ConnectReceiverPinCollisionFailure,
			Location:         target,
			From:             collision.SourceDiagnostic(),
			To:               collision.SubscriberType() + ":" + collision.SubscriberID(),
			Detail:           message,
			Message:          message,
			Remediation:      "Route the source event to distinct subscribers or targets, or consolidate the receiver pins behind one handler. One event x subscriber cannot select multiple receiver-local handlers.",
			AuthoredLocation: collision.AuthoredLocation(),
		}
		issue.ID = issueID(issue)
		issues = append(issues, issue)
	}
	return issues
}

func rootInputSourceViews(source semanticview.Source) []RootInputSource {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return []RootInputSource{}
	}
	out := make([]RootInputSource, 0)
	for _, view := range bundle.FlowViews() {
		if view.Schema.Ingress == nil {
			continue
		}
		flowPath := strings.TrimSpace(view.Paths.FlowPath)
		alias := strings.TrimSpace(view.Schema.Ingress.Alias)
		target := RootInputTarget{FlowPath: flowPath}
		sourceFile := strings.TrimSpace(view.Paths.SchemaFile)
		for providerIndex, binding := range view.Schema.Ingress.Providers {
			admissionKind := strings.ToLower(strings.TrimSpace(binding.Admission.Kind))
			if admissionKind == "" {
				admissionKind = "pack-required"
			}
			item := RootInputSource{
				Kind:             RootInputSourceStandingIngress,
				Alias:            alias,
				Provider:         strings.TrimSpace(binding.Provider),
				Target:           target,
				AuthoredLocation: sourceFile + ":ingress.providers[" + strconv.Itoa(providerIndex) + "]",
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
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Alias != out[j].Alias {
			return out[i].Alias < out[j].Alias
		}
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		if out[i].Target.FlowPath != out[j].Target.FlowPath {
			return out[i].Target.FlowPath < out[j].Target.FlowPath
		}
		if out[i].AuthoredLocation != out[j].AuthoredLocation {
			return out[i].AuthoredLocation < out[j].AuthoredLocation
		}
		return out[i].ID < out[j].ID
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
			SourceFlowPath: strings.TrimSpace(match.Authorization.SourceFlowPath),
			TargetFlowPath: strings.TrimSpace(match.Authorization.TargetFlowPath),
			RouteLabel:     strings.TrimSpace(match.Authorization.RouteLabel),
			Source:         strings.TrimSpace(match.Authorization.Source),
			EventPattern:   strings.TrimSpace(match.Authorization.EventPattern),
			MatchPattern:   strings.TrimSpace(match.Authorization.MatchPattern),
			LocalizedEvent: strings.TrimSpace(match.Authorization.LocalizedEvent),
			RouteSource:    strings.TrimSpace(match.Authorization.RouteSource),
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
		source := plan.SourceEndpoint().Readback()
		receiver := plan.ReceiverEndpoint().Readback()
		producerEndpoints := b.census.MatchingProducers(source.FlowID, source.ResolvedEvent)
		if len(producerEndpoints) == 0 {
			producerEndpoints = b.census.MatchingProducers(source.FlowID, source.LocalEvent)
		}
		if len(producerEndpoints) == 0 {
			if endpoint, ok := findPinEndpoint(b.census.OutputPins(), source.FlowID, source.Pin); ok {
				producerEndpoints = []semanticview.AuthoredEventEndpoint{endpoint}
			}
		}
		consumer, ok := findPinEndpoint(b.census.InputPins(), receiver.FlowID, receiver.Pin)
		if !ok {
			continue
		}
		for _, producer := range producerEndpoints {
			b.addEdge(Edge{
				Scope:                     DeliveryScopeInterFlowConnect,
				Event:                     eventIdentity(source.LocalEvent, source.ResolvedEvent),
				Producer:                  endpointView(producer),
				Consumer:                  endpointView(consumer),
				Boundary:                  boundaryView(plan),
				Resolution:                resolutionView(plan),
				RequiresRuntimeResolution: plan.RequiresRuntimeResolution(),
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
		ResolutionMode: runtimecontracts.FlowInputResolutionModeCode(endpoint.ResolutionMode),
		Sink:           strings.TrimSpace(endpoint.Sink),
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
	planReadback := plan.Readback()
	source := plan.SourceEndpoint().Readback()
	receiver := plan.ReceiverEndpoint().Readback()
	return &Boundary{
		OwnerFlowPath:    planReadback.FlowPath,
		AuthoredLocation: planReadback.AuthoredLocation,
		From:             connectEndpointRef(plan.SourceEndpoint()),
		To:               connectEndpointRef(plan.ReceiverEndpoint()),
		OutputPin:        source.Pin,
		InputPin:         receiver.Pin,
	}
}

func connectEndpointRef(endpoint pinrouting.ConnectRoutePlanEndpoint) string {
	readback := endpoint.Readback()
	if endpoint.IsRoot() {
		return "." + readback.Pin
	}
	return readback.FlowID + "." + readback.Pin
}

func resolutionView(plan pinrouting.ConnectRoutePlan) *Resolution {
	resolution := &Resolution{
		Mode:       plan.ResolutionKind().Code(),
		TargetKind: plan.TargetKind().Code(),
	}
	if plan.InstanceKey() != nil {
		instanceKey := plan.InstanceKey().Readback()
		resolution.Mode = instanceKey.Mode
		if resolution.Mode == "" {
			resolution.Mode = plan.ResolutionKind().Code()
		}
		instance := &InstanceKey{
			Mode:       instanceKey.Mode,
			Field:      instanceKey.Field,
			SourceKind: instanceKey.SourceKind,
			SourcePath: instanceKey.SourcePath,
		}
		if instance.SourcePath != "" && instance.Field != "" {
			instance.DerivedFrom = fmt.Sprintf("instance.%s + carries.%s.from", instance.Field, instance.Field)
		}
		resolution.InstanceKey = instance
	}
	if plan.FanIn() != nil {
		fanIn := plan.FanIn().Readback()
		resolution.Mode = runtimecontracts.FlowInputResolutionModeCode(runtimecontracts.FlowInputResolutionModeFanIn)
		resolution.FanIn = &FanIn{
			Aggregation: fanIn.Aggregation,
			Window:      fanIn.Window,
			DedupBy:     normalizedStrings(fanIn.DedupBy),
			Singleton:   fanIn.Singleton,
		}
	}
	if plan.ReplyResolution() != nil {
		reply := plan.ReplyResolution().Readback()
		resolution.Mode = runtimecontracts.FlowInputResolutionModeCode(runtimecontracts.FlowInputResolutionModeReply)
		resolution.Reply = &Reply{
			Role:              reply.Role,
			RequesterFlowID:   reply.RequesterFlowID,
			RequestOutputPin:  reply.RequestOutputPin,
			ReplyInputPin:     reply.ReplyInputPin,
			ProviderFlowID:    reply.ProviderFlowID,
			ProviderInputPin:  reply.ProviderInputPin,
			ProviderOutputPin: reply.ProviderOutputPin,
			CorrelationKey:    reply.CorrelationKey,
		}
	}
	return resolution
}

func issueViews(connectIssues []pinrouting.ConnectRoutePlanIssue, relationIssues []semanticview.TypedPubSubConsumerIssue) []Issue {
	out := make([]Issue, 0, len(connectIssues)+len(relationIssues))
	for _, issue := range connectIssues {
		view := Issue{
			OwnerFlowPath:    strings.TrimSpace(issue.Connect.OwnerFlowPath),
			From:             strings.TrimSpace(issue.Connect.From),
			To:               strings.TrimSpace(issue.Connect.To),
			Failure:          issue.Failure.Code(),
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
		if out[i].OwnerFlowPath != out[j].OwnerFlowPath {
			return out[i].OwnerFlowPath < out[j].OwnerFlowPath
		}
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		if out[i].To != out[j].To {
			return out[i].To < out[j].To
		}
		if out[i].Failure != out[j].Failure {
			return out[i].Failure < out[j].Failure
		}
		return out[i].Detail < out[j].Detail
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
	parts := []string{issue.CheckID, issue.Severity, issue.Location, issue.OwnerFlowPath, issue.From, issue.To, issue.Failure, issue.Detail, issue.AuthoredLocation}
	return "issue-" + topologyIdentityDigest(parts...)
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
				edge.TypedPubSub.Authorization.SourceFlowPath,
				edge.TypedPubSub.Authorization.TargetFlowPath,
				edge.TypedPubSub.Authorization.RouteLabel,
				edge.TypedPubSub.Authorization.Source,
				edge.TypedPubSub.Authorization.EventPattern,
				edge.TypedPubSub.Authorization.MatchPattern,
				edge.TypedPubSub.Authorization.LocalizedEvent,
			)
		}
	}
	if edge.Boundary != nil {
		parts = append(parts, edge.Boundary.OwnerFlowPath, edge.Boundary.From, edge.Boundary.To)
	}
	return "route-" + topologyIdentityDigest(parts...)
}

func rootInputSourceID(source RootInputSource) string {
	parts := []string{
		strings.TrimSpace(source.Kind),
		strings.TrimSpace(source.Alias),
		strings.TrimSpace(source.Provider),
		strings.TrimSpace(source.Target.FlowPath),
		strings.TrimSpace(source.AuthoredLocation),
	}
	return "root-input-" + topologyIdentityDigest(parts...)
}

func topologyIdentityDigest(parts ...string) string {
	digest := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(part))
	}
	return hex.EncodeToString(digest.Sum(nil)[:8])
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
