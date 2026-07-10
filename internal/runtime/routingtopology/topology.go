package routingtopology

import (
	"crypto/sha256"
	"encoding/hex"
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
	DeliveryScopeIntraFlow DeliveryScope = "intra_flow"
	DeliveryScopeInterFlow DeliveryScope = "inter_flow_connect"
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

const LegacyQualifiedSubscriptionDisposition = "legacy_qualified_subscription"

type LegacyQualifiedSubscription struct {
	ID               string        `json:"id"`
	Disposition      string        `json:"disposition"`
	Event            EventIdentity `json:"event"`
	Consumer         Endpoint      `json:"consumer"`
	TargetFlowID     string        `json:"target_flow_id"`
	TargetFlowPath   string        `json:"target_flow_path"`
	AuthoredLocation string        `json:"authored_location"`
	RuntimeDelivery  bool          `json:"runtime_delivery"`
	CanonicalEdge    bool          `json:"canonical_edge"`
	FindingID        string        `json:"finding_id,omitempty"`
	Migration        string        `json:"migration"`
}

type BoundaryExposure struct {
	ID       string        `json:"id"`
	Event    EventIdentity `json:"event"`
	Producer Endpoint      `json:"producer"`
	Output   Endpoint      `json:"output"`
}

type Edge struct {
	ID                        string        `json:"id"`
	Scope                     DeliveryScope `json:"scope"`
	Event                     EventIdentity `json:"event"`
	Producer                  Endpoint      `json:"producer"`
	Consumer                  Endpoint      `json:"consumer"`
	Boundary                  *Boundary     `json:"boundary,omitempty"`
	Resolution                *Resolution   `json:"resolution,omitempty"`
	RequiresRuntimeResolution bool          `json:"requires_runtime_resolution"`
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
	Delivery    string       `json:"delivery"`
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
	SchemaVersion                string                        `json:"schema_version"`
	ProjectionOnly               bool                          `json:"projection_only"`
	SourceAuthority              string                        `json:"source_authority"`
	Producers                    []Endpoint                    `json:"producers"`
	Consumers                    []Endpoint                    `json:"consumers"`
	InputPins                    []Endpoint                    `json:"input_pins"`
	OutputPins                   []Endpoint                    `json:"output_pins"`
	BoundaryExposures            []BoundaryExposure            `json:"boundary_exposures"`
	Edges                        []Edge                        `json:"edges"`
	LegacyQualifiedSubscriptions []LegacyQualifiedSubscription `json:"legacy_qualified_subscriptions"`
	Issues                       []Issue                       `json:"issues"`
}

func Build(source semanticview.Source) Topology {
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	plans, planIssues := pinrouting.LowerCompositionConnectRoutePlans(source)
	builder := topologyBuilder{
		source:        source,
		census:        census,
		seenEdges:     map[string]struct{}{},
		seenExposures: map[string]struct{}{},
	}
	builder.addIntraFlowEdges()
	builder.addBoundaryExposures()
	builder.addConnectEdges(plans)
	return Topology{
		SchemaVersion:                SchemaVersion,
		ProjectionOnly:               true,
		SourceAuthority:              SourceAuthority,
		Producers:                    endpointViews(census.Producers()),
		Consumers:                    endpointViews(census.Consumers()),
		InputPins:                    endpointViews(census.InputPins()),
		OutputPins:                   endpointViews(census.OutputPins()),
		BoundaryExposures:            builder.sortedExposures(),
		Edges:                        builder.sortedEdges(),
		LegacyQualifiedSubscriptions: legacyQualifiedSubscriptionViews(census.LegacyQualifiedSubscriptions()),
		Issues:                       issueViews(source, planIssues),
	}
}

type topologyBuilder struct {
	source        semanticview.Source
	census        semanticview.AuthoredEventEndpointCensus
	edges         []Edge
	exposures     []BoundaryExposure
	seenEdges     map[string]struct{}
	seenExposures map[string]struct{}
}

func (b *topologyBuilder) addIntraFlowEdges() {
	for _, producer := range b.census.Producers() {
		if !isExecutableEndpoint(producer) {
			continue
		}
		for _, consumer := range b.census.MatchingConsumers(producer.FlowID, producer.Event.EventKey()) {
			if !isExecutableEndpoint(consumer) {
				continue
			}
			b.addEdge(Edge{
				Scope:    DeliveryScopeIntraFlow,
				Event:    eventView(producer.Event),
				Producer: endpointView(producer),
				Consumer: endpointView(consumer),
			})
		}
		for _, consumer := range b.census.MatchingWildcardConsumersAcrossFlows(producer.FlowID, producer.Event.EventKey()) {
			if !isExecutableEndpoint(consumer) {
				continue
			}
			b.addEdge(Edge{
				Scope:    DeliveryScopeIntraFlow,
				Event:    eventView(producer.Event),
				Producer: endpointView(producer),
				Consumer: endpointView(consumer),
			})
		}
	}
	for _, input := range b.census.InputPins() {
		for _, consumer := range b.census.MatchingConsumers(input.FlowID, input.Event.EventKey()) {
			if !isExecutableEndpoint(consumer) {
				continue
			}
			b.addEdge(Edge{
				Scope:    DeliveryScopeIntraFlow,
				Event:    eventView(input.Event),
				Producer: endpointView(input),
				Consumer: endpointView(consumer),
			})
		}
		for _, consumer := range b.census.MatchingWildcardConsumersAcrossFlows(input.FlowID, input.Event.EventKey()) {
			if !isExecutableEndpoint(consumer) {
				continue
			}
			b.addEdge(Edge{
				Scope:    DeliveryScopeIntraFlow,
				Event:    eventView(input.Event),
				Producer: endpointView(input),
				Consumer: endpointView(consumer),
			})
		}
	}
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
				Scope:                     DeliveryScopeInterFlow,
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

func legacyQualifiedSubscriptionViews(subscriptions []semanticview.LegacyQualifiedSubscription) []LegacyQualifiedSubscription {
	out := make([]LegacyQualifiedSubscription, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		consumer := endpointView(subscription.Consumer)
		out = append(out, LegacyQualifiedSubscription{
			ID:               subscription.ID,
			Disposition:      LegacyQualifiedSubscriptionDisposition,
			Event:            eventView(subscription.Event),
			Consumer:         consumer,
			TargetFlowID:     strings.TrimSpace(subscription.TargetFlowID),
			TargetFlowPath:   strings.TrimSpace(subscription.TargetFlowPath),
			AuthoredLocation: endpointAuthoredLocation(consumer),
			RuntimeDelivery:  true,
			CanonicalEdge:    false,
			Migration:        "Declare output/input pins and a connect, then replace the qualified cross-flow subscription with a flow-local event subscription.",
		})
	}
	if out == nil {
		return []LegacyQualifiedSubscription{}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func endpointAuthoredLocation(endpoint Endpoint) string {
	file := strings.TrimSpace(endpoint.SourceFile)
	if file == "" {
		return strings.TrimSpace(endpoint.SourceLocation)
	}
	if endpoint.SourceLine > 0 {
		return file + ":" + strconv.Itoa(endpoint.SourceLine)
	}
	return file
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
		AuthoredLocation: strings.TrimSpace(plan.PackageKey),
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
		Delivery:   string(plan.Delivery),
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

func issueViews(source semanticview.Source, issues []pinrouting.ConnectRoutePlanIssue) []Issue {
	out := make([]Issue, 0, len(issues))
	for _, issue := range issues {
		view := Issue{
			PackageKey:       strings.TrimSpace(issue.Connect.PackageKey),
			From:             strings.TrimSpace(issue.Connect.From),
			To:               strings.TrimSpace(issue.Connect.To),
			Failure:          string(issue.Failure),
			Detail:           strings.TrimSpace(issue.Detail),
			AuthoredLocation: connectSourceFile(source, issue.Connect.PackageKey),
		}
		view.ID = issueID(view)
		out = append(out, view)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].PackageKey, out[i].From, out[i].To, out[i].Failure}, "\x00")
		right := strings.Join([]string{out[j].PackageKey, out[j].From, out[j].To, out[j].Failure}, "\x00")
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
	topology.LegacyQualifiedSubscriptions = linkLegacyQualifiedSubscriptionFindings(topology.LegacyQualifiedSubscriptions, issues)
	return topology
}

func linkLegacyQualifiedSubscriptionFindings(subscriptions []LegacyQualifiedSubscription, issues []Issue) []LegacyQualifiedSubscription {
	out := append([]LegacyQualifiedSubscription(nil), subscriptions...)
	for i := range out {
		for _, issue := range issues {
			if issue.CheckID != "legacy_qualified_subscription" || strings.TrimSpace(issue.Location) != strings.TrimSpace(out[i].AuthoredLocation) {
				continue
			}
			out[i].FindingID = issue.ID
			break
		}
		if out[i].FindingID != "" {
			continue
		}
		for _, issue := range issues {
			if issue.CheckID == "event_consumer_exists" && strings.TrimSpace(issue.Location) == strings.TrimSpace(out[i].Event.Canonical) {
				out[i].FindingID = issue.ID
				break
			}
		}
	}
	if out == nil {
		return []LegacyQualifiedSubscription{}
	}
	return out
}

func issueID(issue Issue) string {
	parts := []string{issue.CheckID, issue.Severity, issue.Location, issue.PackageKey, issue.From, issue.To, issue.Failure, issue.Detail, issue.AuthoredLocation}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "issue-" + hex.EncodeToString(digest[:8])
}

func connectSourceFile(source semanticview.Source, packageKey string) string {
	packageKey = strings.TrimSpace(packageKey)
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil {
		if packageKey == "" || packageKey == "." {
			return strings.TrimSpace(bundle.Paths.ProjectPackageFile)
		}
		for _, loaded := range bundle.PackageTree {
			if strings.TrimSpace(loaded.Key) == packageKey {
				return strings.TrimSpace(loaded.Paths.PackageFile)
			}
		}
	}
	return packageKey
}

func edgeID(edge Edge) string {
	if edge.Producer.ID == "" || edge.Consumer.ID == "" {
		return ""
	}
	parts := []string{string(edge.Scope), edge.Event.Canonical, edge.Producer.ID, edge.Consumer.ID}
	if edge.Boundary != nil {
		parts = append(parts, edge.Boundary.PackageKey, edge.Boundary.From, edge.Boundary.To)
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "route-" + hex.EncodeToString(digest[:8])
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
