package pinrouting

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ConnectExecutionClaim binds one admitted delivery to the exact compiled
// edge, receiver pin/event, handler, and recipient route that produced it.
func ConnectExecutionClaim(plan ConnectRoutePlan, route events.DeliveryRoute) (events.ConnectExecutionClaim, error) {
	route = route.Normalized()
	route.ConnectClaim = events.ConnectExecutionClaim{}
	if _, err := route.Identity(); err != nil {
		return events.ConnectExecutionClaim{}, fmt.Errorf("connect execution claim route: %w", err)
	}
	canonical, err := json.Marshal(struct {
		Source     ConnectRoutePlanEndpoint `json:"source"`
		Receiver   ConnectRoutePlanEndpoint `json:"receiver"`
		Resolution string                   `json:"resolution"`
		ReplyRole  string                   `json:"reply_role,omitempty"`
		Recipient  events.DeliveryRoute     `json:"recipient"`
	}{
		Source: plan.Source, Receiver: plan.Receiver,
		Resolution: connectRoutePlanResolutionKind(plan).Code(), ReplyRole: connectReplyRole(plan), Recipient: route,
	})
	if err != nil {
		return events.ConnectExecutionClaim{}, fmt.Errorf("encode connect execution claim: %w", err)
	}
	digest := sha256.Sum256(canonical)
	pinDigest := sha256.Sum256([]byte(plan.Receiver.flowID + "\x00" + plan.Receiver.pin))
	return events.AdmitConnectExecutionClaim(digest, pinDigest, route, events.EventType(plan.Receiver.event))
}

func connectReplyRole(plan ConnectRoutePlan) string {
	if plan.ReplyResolution == nil {
		return ""
	}
	return plan.ReplyResolution.Role.Code()
}

type ConnectRoutePlanTargetKind struct{ value uint8 }

var (
	ConnectTargetKindTarget    = ConnectRoutePlanTargetKind{value: 1}
	ConnectTargetKindTargetSet = ConnectRoutePlanTargetKind{value: 2}
)

func (k ConnectRoutePlanTargetKind) Code() string {
	switch k {
	case ConnectTargetKindTarget:
		return "target"
	case ConnectTargetKindTargetSet:
		return "target_set"
	default:
		return ""
	}
}

type ConnectRoutePlanResolutionKind struct{ value uint8 }

var (
	ConnectResolutionStatic      = ConnectRoutePlanResolutionKind{value: 1}
	ConnectResolutionInstanceKey = ConnectRoutePlanResolutionKind{value: 2}
	ConnectResolutionReply       = ConnectRoutePlanResolutionKind{value: 3}
)

func (k ConnectRoutePlanResolutionKind) Code() string {
	switch k {
	case ConnectResolutionStatic:
		return "static"
	case ConnectResolutionInstanceKey:
		return "instance_key"
	case ConnectResolutionReply:
		return "reply"
	default:
		return ""
	}
}

func (k ConnectRoutePlanResolutionKind) empty() bool { return k == ConnectRoutePlanResolutionKind{} }

type ConnectRoutePlanFailure string

const (
	ConnectFailureSourceMissing              ConnectRoutePlanFailure = "source_missing"
	ConnectFailureSourceLocationMissing      ConnectRoutePlanFailure = "connect_source_location_missing"
	ConnectFailurePinRefInvalid              ConnectRoutePlanFailure = "connect_pin_ref_invalid"
	ConnectFailureProducerFlowMissing        ConnectRoutePlanFailure = "producer_flow_missing"
	ConnectFailureProducerOutputPinMissing   ConnectRoutePlanFailure = "producer_output_pin_missing"
	ConnectFailureReceiverFlowMissing        ConnectRoutePlanFailure = "receiver_flow_missing"
	ConnectFailureReceiverInputPinMissing    ConnectRoutePlanFailure = "receiver_input_pin_missing"
	ConnectFailureReceiverResolutionMissing  ConnectRoutePlanFailure = "receiver_resolution_missing"
	ConnectFailureEventAliasAdapterInvalid   ConnectRoutePlanFailure = "event_alias_or_adapter_invalid"
	ConnectFailureSyntheticCarryCollision    ConnectRoutePlanFailure = "synthetic_carry_payload_collision"
	ConnectFailureRootReceiverResolution     ConnectRoutePlanFailure = "root_receiver_resolution_invalid"
	ConnectFailureDeliveryTopologyInvalid    ConnectRoutePlanFailure = "delivery_topology_invalid"
	ConnectFailureReplyLineageMissing        ConnectRoutePlanFailure = "reply_lineage_missing"
	ConnectFailureInstanceSourceValueMissing ConnectRoutePlanFailure = "route_plan_instance_source_value_missing"
	ConnectFailureTargetUnresolved           ConnectRoutePlanFailure = "route_plan_target_unresolved"
	ConnectFailureTargetAmbiguous            ConnectRoutePlanFailure = "route_plan_target_ambiguous"
	ConnectFailureInstanceResolutionInvalid  ConnectRoutePlanFailure = "route_plan_instance_resolution_invalid"
	ConnectFailureInstanceConflict           ConnectRoutePlanFailure = "route_plan_instance_conflict"
	ConnectFailureLifecycleUnavailable       ConnectRoutePlanFailure = "route_plan_lifecycle_unavailable"
)

type connectEndpointKind uint8

const (
	connectEndpointRoot connectEndpointKind = iota + 1
	connectEndpointExternalIngress
	connectEndpointStaticFlow
	connectEndpointTemplateFlow
)

// ConnectRoutePlanEndpoint is an opaque admitted graph endpoint. Local and
// resolved event identities remain distinct and cannot be manufactured by a
// downstream routing consumer.
type ConnectRoutePlanEndpoint struct {
	kind          connectEndpointKind
	flowID        string
	flowPath      string
	pin           string
	event         string
	resolvedEvent string
	key           string
	carries       []string
}

func newConnectRoutePlanEndpoint(root bool, flowID, flowPath, mode, pin, event, resolvedEvent, key string, carries []string) ConnectRoutePlanEndpoint {
	kind := connectEndpointStaticFlow
	switch {
	case strings.TrimSpace(mode) == "external":
		kind = connectEndpointExternalIngress
	case root:
		kind = connectEndpointRoot
	case strings.TrimSpace(mode) == runtimecontracts.FlowModeTemplate:
		kind = connectEndpointTemplateFlow
	}
	return ConnectRoutePlanEndpoint{
		kind: kind, flowID: strings.TrimSpace(flowID), flowPath: strings.Trim(strings.TrimSpace(flowPath), "/"),
		pin: strings.TrimSpace(pin), event: eventidentity.Normalize(event), resolvedEvent: eventidentity.Normalize(resolvedEvent),
		key: strings.TrimSpace(key), carries: append([]string(nil), carries...),
	}
}

func (e ConnectRoutePlanEndpoint) IsRoot() bool { return e.kind == connectEndpointRoot }
func (e ConnectRoutePlanEndpoint) IsExternalIngress() bool {
	return e.kind == connectEndpointExternalIngress
}
func (e ConnectRoutePlanEndpoint) IsTemplate() bool          { return e.kind == connectEndpointTemplateFlow }
func (e ConnectRoutePlanEndpoint) FlowIDCode() string        { return e.flowID }
func (e ConnectRoutePlanEndpoint) FlowPathCode() string      { return e.flowPath }
func (e ConnectRoutePlanEndpoint) PinCode() string           { return e.pin }
func (e ConnectRoutePlanEndpoint) LocalEventCode() string    { return e.event }
func (e ConnectRoutePlanEndpoint) ResolvedEventCode() string { return e.resolvedEvent }
func (e ConnectRoutePlanEndpoint) KeyCode() string           { return e.key }
func (e ConnectRoutePlanEndpoint) CarryCodes() []string      { return append([]string(nil), e.carries...) }

func connectSourceEndpointMatchesEvent(endpoint ConnectRoutePlanEndpoint, evt events.Event) bool {
	return connectSourceEndpointMatches(endpoint, evt.Type(), evt.RoutingSource())
}

func connectSourceEndpointMatches(endpoint ConnectRoutePlanEndpoint, eventType events.EventType, source events.RoutingSource) bool {
	event := eventidentity.Normalize(string(eventType))
	local := endpoint.event
	resolved := endpoint.resolvedEvent
	if event == "" || local == "" {
		return false
	}
	route := source.Route()
	scope := endpoint.flowPath
	if scope == "" {
		scope = endpoint.flowID
	}
	switch source.Kind() {
	case events.RoutingSourceExternalIngress:
		return endpoint.IsExternalIngress() && (event == local || event == resolved)
	case events.RoutingSourceRoot:
		return endpoint.IsRoot() && route.FlowID == "" && route.FlowInstance == "" && (event == local || event == resolved)
	case events.RoutingSourceStaticFlow:
		if endpoint.IsRoot() || endpoint.IsTemplate() || route.FlowID != endpoint.flowID || eventidentity.Normalize(route.FlowInstance) != scope {
			return false
		}
		return event == resolved || event == eventidentity.Normalize(scope+"/"+local)
	case events.RoutingSourceConcreteTemplateInstance:
		if endpoint.IsRoot() || !endpoint.IsTemplate() || route.FlowID != endpoint.flowID {
			return false
		}
		return event == eventidentity.Normalize(route.FlowInstance+"/"+local)
	case events.RoutingSourceFlowOwnedControl:
		return !endpoint.IsRoot() && route.FlowID == endpoint.flowID && event == resolved
	case events.RoutingSourceAbsent, events.RoutingSourcePlatformControl:
		return false
	default:
		return false
	}
}

type ConnectRoutePlanInstanceKey struct {
	Mode   runtimecontracts.FlowInputResolutionMode
	Field  runtimecontracts.TemplateInstanceField
	Source runtimecontracts.FlowInputInstanceSource
}

type ConnectRoutePlanFanIn struct {
	Aggregation ConnectFanInAggregation
	Window      string
	DedupBy     []string
	Singleton   string
}

type ConnectFanInAggregation struct{ value uint8 }

var (
	ConnectFanInStream  = ConnectFanInAggregation{value: 1}
	ConnectFanInBarrier = ConnectFanInAggregation{value: 2}
)

func (a ConnectFanInAggregation) Code() string {
	switch a {
	case ConnectFanInStream:
		return "stream"
	case ConnectFanInBarrier:
		return "barrier"
	default:
		return ""
	}
}

type ConnectRoutePlanReplyResolution struct {
	Role              ConnectReplyRole
	RequesterFlowID   string
	RequestOutputPin  string
	ReplyInputPin     string
	ProviderFlowID    string
	ProviderInputPin  string
	ProviderOutputPin string
	CorrelationKey    string
}

type ConnectReplyRole struct{ value uint8 }

var (
	ConnectReplyRoleRequest  = ConnectReplyRole{value: 1}
	ConnectReplyRoleResponse = ConnectReplyRole{value: 2}
)

func (r ConnectReplyRole) Code() string {
	switch r {
	case ConnectReplyRoleRequest:
		return "request"
	case ConnectReplyRoleResponse:
		return "response"
	default:
		return ""
	}
}

type ConnectRoutePlan struct {
	PackageKey                  string
	AuthoredLocation            string
	Source                      ConnectRoutePlanEndpoint
	Receiver                    ConnectRoutePlanEndpoint
	Adapter                     string
	TargetKind                  ConnectRoutePlanTargetKind
	ResolutionKind              ConnectRoutePlanResolutionKind
	InstanceKey                 *ConnectRoutePlanInstanceKey
	FanIn                       *ConnectRoutePlanFanIn
	ReplyResolution             *ConnectRoutePlanReplyResolution
	Target                      events.RouteIdentity
	TargetSet                   []events.RouteIdentity
	RequiresRuntimeResolution   bool
	ProviderOutputAuthorization *runtimeprovideroutput.Authorization
}

type ConnectRoutePlanIssue struct {
	Connect                     runtimecontracts.FlowPackageConnect
	AuthoredLocation            string
	Failure                     ConnectRoutePlanFailure
	Detail                      string
	ProviderOutputAuthorization *runtimeprovideroutput.Authorization
}

// DiagnosticContext is a one-way authored-location projection. Runtime
// matching and application cannot consume this display text.
func (i ConnectRoutePlanIssue) DiagnosticContext() string {
	from := strings.TrimSpace(i.Connect.From)
	to := strings.TrimSpace(i.Connect.To)
	if from == "" && to == "" {
		return ""
	}
	return strings.TrimSpace("connect " + from + " -> " + to)
}

type ConnectRoutePlanMaterializationInput struct {
	MatchValues map[string]string
	Descriptors []Descriptor
}

type ConnectRoutePlanMaterialization struct {
	Target    events.RouteIdentity
	TargetSet []events.RouteIdentity
	Failure   ConnectRoutePlanFailure
}

type ConnectRoutePlanInstanceKeyMaterial struct {
	Values map[string]any
	Keys   []runtimecontracts.TemplateInstanceKeyValue
}

// CompiledConnectGraph is the immutable owner of all admitted connect rows.
// Authored rows are retained only inside this package while downstream
// consumers receive compiled plans or typed projections.
type CompiledConnectGraph struct {
	source semanticview.Source
	plans  []ConnectRoutePlan
	issues []ConnectRoutePlanIssue
}

type ConnectEndpointRoleKind uint8

const (
	ConnectEndpointRoleProducer ConnectEndpointRoleKind = iota + 1
	ConnectEndpointRoleConsumer
)

type ConnectEndpointRole struct {
	kind   ConnectEndpointRoleKind
	flowID string
	pin    string
	local  events.EventType
	event  events.EventType
	root   bool
}

func (r ConnectEndpointRole) Kind() ConnectEndpointRoleKind { return r.kind }
func (r ConnectEndpointRole) FlowID() string                { return r.flowID }
func (r ConnectEndpointRole) Pin() string                   { return r.pin }
func (r ConnectEndpointRole) Event() events.EventType       { return r.event }
func (r ConnectEndpointRole) LocalEvent() events.EventType  { return r.local }
func (r ConnectEndpointRole) Root() bool                    { return r.root }

func (r ConnectEndpointRole) Matches(flowID string, eventType events.EventType) bool {
	if r.root != (strings.TrimSpace(flowID) == "") || r.flowID != strings.TrimSpace(flowID) {
		return false
	}
	event := eventidentity.Normalize(string(eventType))
	return event != "" && (event == eventidentity.Normalize(string(r.local)) || event == eventidentity.Normalize(string(r.event)))
}

type ConnectEdgeEvidence struct {
	producer   ConnectEndpointRole
	consumer   ConnectEndpointRole
	resolution runtimecontracts.FlowInputResolutionMode
}

type ConnectReceiverKind uint8

const (
	ConnectReceiverCarrier ConnectReceiverKind = iota + 1
	ConnectReceiverRoot
)

type ConnectReceiverLookup struct {
	events                    []events.EventType
	kind                      ConnectReceiverKind
	requiresRuntimeResolution bool
}

func (l ConnectReceiverLookup) EventTypes() []events.EventType {
	return append([]events.EventType(nil), l.events...)
}
func (l ConnectReceiverLookup) Kind() ConnectReceiverKind { return l.kind }
func (l ConnectReceiverLookup) RequiresRuntimeResolution() bool {
	return l.requiresRuntimeResolution
}

func (g CompiledConnectGraph) ReceiverLookup(plan ConnectRoutePlan) ConnectReceiverLookup {
	kind := ConnectReceiverCarrier
	if plan.Receiver.IsRoot() {
		kind = ConnectReceiverRoot
	}
	lookup := ConnectReceiverLookup{kind: kind, requiresRuntimeResolution: plan.RequiresRuntimeResolution}
	if plan.RequiresRuntimeResolution {
		return lookup
	}
	seen := map[events.EventType]struct{}{}
	add := func(raw string) {
		eventType := events.EventType(eventidentity.Normalize(raw))
		if eventType != "" {
			seen[eventType] = struct{}{}
		}
	}
	local := eventidentity.Normalize(plan.Receiver.event)
	receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.flowPath), "/")
	if receiverPath == "" {
		receiverPath = strings.Trim(strings.TrimSpace(plan.Receiver.flowID), "/")
	}
	add(plan.Receiver.resolvedEvent)
	if receiverPath != "" && local != "" {
		add(receiverPath + "/" + local)
	}
	if target := plan.Target.Normalized(); target.FlowInstance != "" && local != "" {
		add(target.FlowInstance + "/" + local)
	}
	for _, target := range plan.TargetSet {
		if target = target.Normalized(); target.FlowInstance != "" && local != "" {
			add(target.FlowInstance + "/" + local)
		}
	}
	if plan.Receiver.IsRoot() {
		add(local)
	}
	lookup.events = make([]events.EventType, 0, len(seen))
	for eventType := range seen {
		lookup.events = append(lookup.events, eventType)
	}
	sort.Slice(lookup.events, func(i, j int) bool { return lookup.events[i] < lookup.events[j] })
	return lookup
}

func (e ConnectEdgeEvidence) Producer() ConnectEndpointRole { return e.producer }
func (e ConnectEdgeEvidence) Consumer() ConnectEndpointRole { return e.consumer }
func (e ConnectEdgeEvidence) ResolutionMode() runtimecontracts.FlowInputResolutionMode {
	return e.resolution
}

func (g CompiledConnectGraph) Edges() []ConnectEdgeEvidence {
	edges := make([]ConnectEdgeEvidence, 0, len(g.plans))
	for _, plan := range g.plans {
		mode := runtimecontracts.FlowInputResolutionModeNone
		switch {
		case plan.InstanceKey != nil:
			mode = plan.InstanceKey.Mode
		case plan.FanIn != nil:
			mode = runtimecontracts.FlowInputResolutionModeFanIn
		case plan.ReplyResolution != nil:
			mode = runtimecontracts.FlowInputResolutionModeReply
		}
		edges = append(edges, ConnectEdgeEvidence{
			producer:   connectEndpointRole(ConnectEndpointRoleProducer, plan.Source),
			consumer:   connectEndpointRole(ConnectEndpointRoleConsumer, plan.Receiver),
			resolution: mode,
		})
	}
	return edges
}

func (g CompiledConnectGraph) HasProducer(flowID string, eventType events.EventType) bool {
	for _, edge := range g.Edges() {
		if edge.Producer().Matches(flowID, eventType) {
			return true
		}
	}
	return false
}

func (g CompiledConnectGraph) HasConsumer(flowID string, eventType events.EventType) bool {
	for _, edge := range g.Edges() {
		if edge.Consumer().Matches(flowID, eventType) {
			return true
		}
	}
	return false
}

func connectEndpointRole(kind ConnectEndpointRoleKind, endpoint ConnectRoutePlanEndpoint) ConnectEndpointRole {
	return ConnectEndpointRole{
		kind: kind, flowID: endpoint.flowID, pin: endpoint.pin,
		local: events.EventType(endpoint.event), event: events.EventType(endpoint.resolvedEvent), root: endpoint.IsRoot(),
	}
}

type SourceEvent struct {
	eventType events.EventType
	source    events.RoutingSource
}

func AdmitSourceEvent(eventType events.EventType, source events.RoutingSource) (SourceEvent, error) {
	canonical := eventidentity.Normalize(string(eventType))
	if canonical == "" || canonical != string(eventType) {
		return SourceEvent{}, fmt.Errorf("connect source event identity is not canonical")
	}
	if source.Empty() {
		return SourceEvent{}, fmt.Errorf("connect source event requires explicit routing provenance")
	}
	return SourceEvent{eventType: eventType, source: source}, nil
}

func SourceEventFromEvent(evt events.Event) (SourceEvent, error) {
	return AdmitSourceEvent(evt.Type(), evt.RoutingSource())
}

func CompileConnectGraph(source semanticview.Source) CompiledConnectGraph {
	if source == nil {
		return CompiledConnectGraph{issues: []ConnectRoutePlanIssue{{Failure: ConnectFailureSourceMissing, Detail: "semantic source is required"}}}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return CompiledConnectGraph{issues: []ConnectRoutePlanIssue{{Failure: ConnectFailureSourceMissing, Detail: "compiled connect input is unavailable"}}}
	}
	connects := bundle.CompositionConnects()
	plans := make([]ConnectRoutePlan, 0, len(connects))
	var issues []ConnectRoutePlanIssue
	for _, connect := range connects {
		plan, issue := lowerCompositionConnectRoutePlanWithLocation(source, connect)
		if issue.Failure != "" {
			issues = append(issues, issue)
			continue
		}
		plans = append(plans, plan)
	}
	if authorizations := source.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations(); len(authorizations) > 0 {
		externalPlans, externalIssues := lowerTargetFreeInputRoutePlans(source, authorizations)
		plans = append(plans, externalPlans...)
		issues = append(issues, externalIssues...)
	}
	sortConnectRoutePlans(plans)
	return CompiledConnectGraph{source: source, plans: plans, issues: issues}
}

func (g CompiledConnectGraph) MatchingPlans(evt events.Event) []ConnectRoutePlan {
	sourceEvent, err := SourceEventFromEvent(evt)
	if err != nil {
		return nil
	}
	return g.MatchingSourceEvent(sourceEvent)
}

func (g CompiledConnectGraph) PlanMatchesEvent(plan ConnectRoutePlan, evt events.Event) bool {
	return connectSourceEndpointMatchesEvent(plan.Source, evt)
}

func (g CompiledConnectGraph) MatchingSourceEvent(sourceEvent SourceEvent) []ConnectRoutePlan {
	out := make([]ConnectRoutePlan, 0)
	for _, plan := range g.plans {
		if connectSourceEndpointMatches(plan.Source, sourceEvent.eventType, sourceEvent.source) {
			out = append(out, plan)
		}
	}
	return out
}

func (g CompiledConnectGraph) Plans() []ConnectRoutePlan {
	return append([]ConnectRoutePlan(nil), g.plans...)
}

func (g CompiledConnectGraph) Issues() []ConnectRoutePlanIssue {
	return append([]ConnectRoutePlanIssue(nil), g.issues...)
}

func (g CompiledConnectGraph) EndpointRoles() []ConnectEndpointRole {
	roles := make([]ConnectEndpointRole, 0, 2*(len(g.plans)+len(g.issues)))
	appendRole := func(kind ConnectEndpointRoleKind, root bool, flowID, pin string, localEvent, event events.EventType) {
		flowID = strings.TrimSpace(flowID)
		pin = strings.TrimSpace(pin)
		if pin == "" {
			return
		}
		candidate := ConnectEndpointRole{kind: kind, flowID: flowID, pin: pin, local: localEvent, event: event, root: root}
		for _, existing := range roles {
			if existing == candidate {
				return
			}
		}
		roles = append(roles, candidate)
	}
	for _, plan := range g.plans {
		appendRole(ConnectEndpointRoleProducer, plan.Source.IsRoot(), plan.Source.flowID, plan.Source.pin, events.EventType(plan.Source.event), events.EventType(plan.Source.resolvedEvent))
		appendRole(ConnectEndpointRoleConsumer, plan.Receiver.IsRoot(), plan.Receiver.flowID, plan.Receiver.pin, events.EventType(plan.Receiver.event), events.EventType(plan.Receiver.resolvedEvent))
	}
	for _, issue := range g.issues {
		if issue.ProviderOutputAuthorization != nil || g.source == nil {
			continue
		}
		from, fromErr := issue.Connect.FromRef()
		to, toErr := issue.Connect.ToRef()
		if fromErr != nil || toErr != nil {
			continue
		}
		if resolved, ok := resolveCompositionConnectEndpoint(g.source, issue.Connect.PackageKey, from); ok {
			event := events.EventType("")
			if endpoint, _, endpointIssue := connectRoutePlanSourceEndpoint(g.source, resolved, issue.Connect); endpointIssue.Failure == "" {
				event = events.EventType(endpoint.resolvedEvent)
			}
			appendRole(ConnectEndpointRoleProducer, resolved.Root, resolved.FlowID, resolved.Pin, event, event)
		}
		if resolved, ok := resolveCompositionConnectEndpoint(g.source, issue.Connect.PackageKey, to); ok {
			event := events.EventType("")
			if endpoint, endpointOK := connectRoutePlanReceiverEndpointRole(g.source, resolved); endpointOK {
				event = events.EventType(endpoint.resolvedEvent)
			}
			appendRole(ConnectEndpointRoleConsumer, resolved.Root, resolved.FlowID, resolved.Pin, event, event)
		}
	}
	sort.SliceStable(roles, func(i, j int) bool {
		if roles[i].kind != roles[j].kind {
			return roles[i].kind < roles[j].kind
		}
		if roles[i].flowID != roles[j].flowID {
			return roles[i].flowID < roles[j].flowID
		}
		if roles[i].pin != roles[j].pin {
			return roles[i].pin < roles[j].pin
		}
		return roles[i].event < roles[j].event
	})
	return roles
}

func (g CompiledConnectGraph) IssueMatchesEvent(issue ConnectRoutePlanIssue, evt events.Event) bool {
	if issue.ProviderOutputAuthorization != nil {
		return evt.RoutingSource().Kind() == events.RoutingSourceExternalIngress &&
			eventidentity.Normalize(string(evt.Type())) == eventidentity.Normalize(issue.ProviderOutputAuthorization.Event())
	}
	if g.source == nil {
		return false
	}
	from, err := issue.Connect.FromRef()
	if err != nil {
		return false
	}
	from, ok := resolveCompositionConnectEndpoint(g.source, issue.Connect.PackageKey, from)
	if !ok {
		return false
	}
	endpoint, _, endpointIssue := connectRoutePlanSourceEndpoint(g.source, from, issue.Connect)
	return endpointIssue.Failure == "" && connectSourceEndpointMatches(endpoint, evt.Type(), evt.RoutingSource())
}

func sortConnectRoutePlans(plans []ConnectRoutePlan) {
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Source.flowID != plans[j].Source.flowID {
			return plans[i].Source.flowID < plans[j].Source.flowID
		}
		if plans[i].Source.pin != plans[j].Source.pin {
			return plans[i].Source.pin < plans[j].Source.pin
		}
		if plans[i].Receiver.flowID != plans[j].Receiver.flowID {
			return plans[i].Receiver.flowID < plans[j].Receiver.flowID
		}
		return plans[i].Receiver.pin < plans[j].Receiver.pin
	})
}

func compileConnectPlans(source semanticview.Source) ([]ConnectRoutePlan, []ConnectRoutePlanIssue) {
	graph := CompileConnectGraph(source)
	return graph.Plans(), graph.Issues()
}

// lowerTargetFreeInputRoutePlans lowers exact external input pins for the
// explicitly authorized target-free event set. It reuses the same instance-key
// materialization model as composition connect routes without inventing a
// synthetic producer output pin.
func lowerTargetFreeInputRoutePlans(source semanticview.Source, authorizations []runtimeprovideroutput.Authorization) ([]ConnectRoutePlan, []ConnectRoutePlanIssue) {
	if source == nil || len(authorizations) == 0 {
		return nil, nil
	}
	allowed := map[string]runtimeprovideroutput.Authorization{}
	for _, authorization := range authorizations {
		if authorization.Valid() {
			allowed[eventidentity.Normalize(authorization.Event())] = authorization
		}
	}
	plans := make([]ConnectRoutePlan, 0)
	issues := make([]ConnectRoutePlanIssue, 0)
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		for _, inputPin := range source.FlowInputEventPins(flowID) {
			if strings.TrimSpace(inputPin.Source) != "external" {
				continue
			}
			resolved := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, inputPin.EventType()))
			authorization, ok := allowed[resolved]
			if !ok {
				continue
			}
			connect := runtimecontracts.FlowPackageConnect{To: flowID + "." + inputPin.PinName()}
			var instanceKey *ConnectRoutePlanInstanceKey
			if receiverRequiresRuntimeResolution(scope) {
				var issue ConnectRoutePlanIssue
				instanceKey, issue = connectResolutionInstanceKey(source, connect, inputPin, inputPin.Resolution, flowID)
				if issue.Failure != "" {
					issue.AuthoredLocation = flowID + "." + inputPin.PinName()
					issue.ProviderOutputAuthorization = &authorization
					issues = append(issues, issue)
					continue
				}
			}
			plan := ConnectRoutePlan{
				AuthoredLocation: flowID + "." + inputPin.PinName(),
				Source:           newConnectRoutePlanEndpoint(true, "", "", "external", inputPin.PinName(), resolved, resolved, "", nil),
				Receiver: newConnectRoutePlanEndpoint(false, flowID, scope.Path, scope.Mode,
					inputPin.PinName(), inputPin.EventType(), resolved, "", nil),
				TargetKind:                  ConnectTargetKindTarget,
				ResolutionKind:              connectResolutionKind(scope, instanceKey),
				InstanceKey:                 instanceKey,
				ProviderOutputAuthorization: &authorization,
			}
			if receiverRequiresRuntimeResolution(scope) {
				if instanceKey == nil {
					issues = append(issues, ConnectRoutePlanIssue{Connect: connect, AuthoredLocation: plan.AuthoredLocation, Failure: ConnectFailureReceiverResolutionMissing, Detail: flowID, ProviderOutputAuthorization: &authorization})
					continue
				}
				plan.RequiresRuntimeResolution = true
			} else {
				plan.Target = staticConnectRoute(source, flowID)
			}
			plans = append(plans, plan)
		}
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Receiver.flowID != plans[j].Receiver.flowID {
			return plans[i].Receiver.flowID < plans[j].Receiver.flowID
		}
		return plans[i].Receiver.pin < plans[j].Receiver.pin
	})
	return plans, issues
}

func lowerCompositionConnectRoutePlanWithLocation(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
	plan, issue := lowerCompositionConnectRoutePlan(source, connect)
	authoredLocation := connect.AuthoredLocation()
	plan.AuthoredLocation = authoredLocation
	issue.AuthoredLocation = authoredLocation
	if issue.Failure == "" && authoredLocation == "" {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{
			Connect:          connect,
			AuthoredLocation: authoredLocation,
			Failure:          ConnectFailureSourceLocationMissing,
			Detail:           "connect requires exact package.yaml source file and line metadata",
		}
	}
	return plan, issue
}

func lowerCompositionConnectRoutePlan(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
	if source == nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureSourceMissing, Detail: "semantic source is required"}
	}
	from, err := connect.FromRef()
	if err != nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailurePinRefInvalid, Detail: err.Error()}
	}
	to, err := connect.ToRef()
	if err != nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailurePinRefInvalid, Detail: err.Error()}
	}
	if from.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerFlowMissing, Detail: strings.TrimSpace(connect.PackageKey)}
		}
		from.FlowID, from.Root = flowID, flowID == ""
	}
	if to.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: strings.TrimSpace(connect.PackageKey)}
		}
		to.FlowID, to.Root = flowID, flowID == ""
	}
	sourceEndpoint, _, sourceIssue := connectRoutePlanSourceEndpoint(source, from, connect)
	if sourceIssue.Failure != "" {
		return ConnectRoutePlan{}, sourceIssue
	}
	if to.Root {
		inputPin, ok := source.FlowInputEventPin("", to.Pin)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverInputPinMissing, Detail: connect.To}
		}
		if !connectEventsCompatible(source, connect, from, to, sourceEndpoint, inputPin) {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureEventAliasAdapterInvalid, Detail: to.Pin}
		}
		if !inputPin.Resolution.Empty() {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureRootReceiverResolution, Detail: to.Pin}
		}
		return ConnectRoutePlan{
			PackageKey:       strings.TrimSpace(connect.PackageKey),
			AuthoredLocation: connect.AuthoredLocation(),
			Source:           sourceEndpoint,
			Receiver: newConnectRoutePlanEndpoint(true, "", "", "root", to.Pin, inputPin.EventType(),
				source.ResolveFlowEventReference("", inputPin.EventType()), "", nil),
			Adapter:        strings.TrimSpace(connect.Adapter),
			TargetKind:     ConnectTargetKindTarget,
			ResolutionKind: ConnectResolutionStatic,
		}, ConnectRoutePlanIssue{}
	}
	receiverScope, ok := source.FlowScopeByID(to.FlowID)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: to.FlowID}
	}
	inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverInputPinMissing, Detail: connect.To}
	}
	if !connectEventsCompatible(source, connect, from, to, sourceEndpoint, inputPin) {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureEventAliasAdapterInvalid, Detail: to.FlowID}
	}
	if detail := connectSyntheticCarryCollision(source, from, sourceEndpoint, inputPin); detail != "" {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureSyntheticCarryCollision, Detail: detail}
	}
	instanceKey, instanceKeyIssue := connectInstanceKey(source, connect, inputPin, to.FlowID)
	if instanceKeyIssue.Failure != "" {
		return ConnectRoutePlan{}, instanceKeyIssue
	}
	fanIn, fanInIssue := connectFanIn(source, connect, inputPin, to.FlowID)
	if fanInIssue.Failure != "" {
		return ConnectRoutePlan{}, fanInIssue
	}
	replyResolution, replyIssue := connectReplyResolution(source, connect, sourceEndpoint, to, inputPin)
	if replyIssue.Failure != "" {
		return ConnectRoutePlan{}, replyIssue
	}
	if receiverRequiresRuntimeResolution(receiverScope) && instanceKey == nil && (replyResolution == nil || replyResolution.Role != ConnectReplyRoleResponse) {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverResolutionMissing, Detail: to.FlowID}
	}
	plan := ConnectRoutePlan{
		PackageKey:       strings.TrimSpace(connect.PackageKey),
		AuthoredLocation: connect.AuthoredLocation(),
		Source:           sourceEndpoint,
		Receiver: newConnectRoutePlanEndpoint(false, to.FlowID, receiverScope.Path, receiverScope.Mode,
			to.Pin, inputPin.EventType(), source.ResolveFlowEventReference(to.FlowID, inputPin.EventType()), "", nil),
		Adapter:         strings.TrimSpace(connect.Adapter),
		TargetKind:      ConnectTargetKindTarget,
		ResolutionKind:  connectResolutionKind(receiverScope, instanceKey),
		InstanceKey:     instanceKey,
		FanIn:           fanIn,
		ReplyResolution: replyResolution,
	}
	if replyResolution != nil && replyResolution.Role == ConnectReplyRoleResponse {
		plan.ResolutionKind = ConnectResolutionReply
		plan.RequiresRuntimeResolution = true
		return plan, ConnectRoutePlanIssue{}
	}
	if !receiverRequiresRuntimeResolution(receiverScope) {
		route := staticConnectRoute(source, to.FlowID)
		if fanIn != nil {
			route = fanInSingletonRoute(to.FlowID, fanIn.Singleton)
		}
		if !route.Empty() {
			plan.Target = route
		}
		return plan, ConnectRoutePlanIssue{}
	}
	plan.RequiresRuntimeResolution = true
	return plan, ConnectRoutePlanIssue{}
}

func connectEventsCompatible(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, from, to runtimecontracts.FlowPackagePinRef, sourceEndpoint ConnectRoutePlanEndpoint, inputPin runtimecontracts.FlowInputEventPin) bool {
	outputEvent := sourceEndpoint.event
	inputEvent := eventidentity.Normalize(inputPin.EventType())
	if outputEvent == "" || inputEvent == "" || outputEvent == inputEvent || strings.TrimSpace(connect.Adapter) != "" {
		return true
	}
	candidates := map[string]struct{}{
		outputEvent: {},
		eventidentity.Normalize(source.ResolveFlowEventReference(from.FlowID, outputEvent)): {},
	}
	for candidate := range candidates {
		for _, parentEvent := range semanticview.ImportBoundaryOutputParentEventsForEvent(source, connect.PackageKey, "", candidate) {
			if parentEvent = eventidentity.Normalize(parentEvent); parentEvent != "" {
				candidates[parentEvent] = struct{}{}
			}
		}
	}
	if _, ok := candidates[inputEvent]; ok {
		return true
	}
	for _, alias := range append(
		semanticview.ImportBoundaryInputAliases(source, to.FlowID, inputPin.PinName()),
		semanticview.ImportBoundaryInputAliases(source, to.FlowID, inputPin.EventType())...,
	) {
		if _, ok := candidates[eventidentity.Normalize(alias.ParentEvent)]; ok {
			return true
		}
		if _, ok := candidates[eventidentity.Normalize(alias.EventPattern)]; ok {
			return true
		}
	}
	return false
}

func connectSyntheticCarryCollision(source semanticview.Source, from runtimecontracts.FlowPackagePinRef, sourceEndpoint ConnectRoutePlanEndpoint, inputPin runtimecontracts.FlowInputEventPin) string {
	if from.Root || inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeCreate {
		return ""
	}
	syntheticFields := map[string]string{}
	for field, carry := range inputPin.Carries {
		field = strings.TrimSpace(field)
		owner, err := runtimecontracts.ResolveFlowInputInstanceSource(inputPin.Resolution.Mode, strings.TrimSpace(carry.From))
		if field != "" && err == nil && owner.RequiresDeliveryProjection() {
			syntheticFields[field] = strings.TrimSpace(carry.From)
		}
	}
	if len(syntheticFields) == 0 {
		return ""
	}
	wantEvent := sourceEndpoint.resolvedEvent
	for _, site := range semanticview.AuthoredEmitSites(source) {
		if strings.TrimSpace(site.FlowID) != strings.TrimSpace(from.FlowID) || eventidentity.Normalize(source.ResolveFlowEventReference(site.FlowID, site.Spec.EventType())) != wantEvent {
			continue
		}
		for field, carrySource := range syntheticFields {
			if _, authored := site.Spec.Fields[field]; authored {
				return fmt.Sprintf("producer %s emit field %s conflicts with receiver-owned carry %s", strings.TrimSpace(site.NodeID), field, carrySource)
			}
		}
	}
	return ""
}

func connectRoutePlanSourceEndpoint(source semanticview.Source, from runtimecontracts.FlowPackagePinRef, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlanEndpoint, runtimecontracts.FlowOutputEventPin, ConnectRoutePlanIssue) {
	if from.Root {
		outputPin, ok := source.FlowOutputEventPin("", from.Pin)
		if !ok {
			return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerOutputPinMissing, Detail: strings.TrimSpace(connect.From)}
		}
		return newConnectRoutePlanEndpoint(true, "", "", "root", from.Pin, outputPin.EventType(),
			source.ResolveFlowEventReference("", outputPin.EventType()), outputPin.Key, normalizedPinCarries(outputPin.Carries)), outputPin, ConnectRoutePlanIssue{}
	}
	sourceScope, ok := source.FlowScopeByID(from.FlowID)
	if !ok {
		return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerFlowMissing, Detail: strings.TrimSpace(from.FlowID)}
	}
	outputPin, ok := source.FlowOutputEventPin(from.FlowID, from.Pin)
	if !ok {
		return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerOutputPinMissing, Detail: strings.TrimSpace(connect.From)}
	}
	return newConnectRoutePlanEndpoint(false, from.FlowID, sourceScope.Path, sourceScope.Mode, from.Pin,
		outputPin.EventType(), source.ResolveFlowEventReference(from.FlowID, outputPin.EventType()), outputPin.Key,
		normalizedPinCarries(outputPin.Carries)), outputPin, ConnectRoutePlanIssue{}
}

func connectRoutePlanReceiverEndpointRole(source semanticview.Source, to runtimecontracts.FlowPackagePinRef) (ConnectRoutePlanEndpoint, bool) {
	flowID := strings.TrimSpace(to.FlowID)
	inputPin, ok := source.FlowInputEventPin(flowID, to.Pin)
	if !ok {
		return ConnectRoutePlanEndpoint{}, false
	}
	if to.Root {
		return newConnectRoutePlanEndpoint(true, "", "", "root", to.Pin, inputPin.EventType(),
			source.ResolveFlowEventReference("", inputPin.EventType()), "", nil), true
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return ConnectRoutePlanEndpoint{}, false
	}
	return newConnectRoutePlanEndpoint(false, flowID, scope.Path, scope.Mode, to.Pin, inputPin.EventType(),
		source.ResolveFlowEventReference(flowID, inputPin.EventType()), "", nil), true
}

func MaterializeConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	if plan.Receiver.IsRoot() {
		return ConnectRoutePlanMaterialization{}
	}
	if !plan.Target.Empty() {
		return ConnectRoutePlanMaterialization{Target: plan.Target}
	}
	if len(plan.TargetSet) > 0 {
		return ConnectRoutePlanMaterialization{TargetSet: append([]events.RouteIdentity{}, plan.TargetSet...)}
	}
	switch connectRoutePlanResolutionKind(plan) {
	case ConnectResolutionInstanceKey:
		return materializeInstanceKeyConnectRoutePlan(plan, input)
	default:
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureReceiverResolutionMissing}
	}
}

func materializeInstanceKeyConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	keyMaterial, failure := InstanceKeyMaterialForConnectRoutePlan(plan, input.MatchValues)
	if failure != "" {
		return ConnectRoutePlanMaterialization{Failure: failure}
	}
	routes := make([]events.RouteIdentity, 0, len(input.Descriptors))
	for _, descriptor := range input.Descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		if ConnectInstanceKeyDescriptorMatches(keyMaterial.Keys, descriptor) {
			routes = append(routes, route)
		}
	}
	routes = uniqueRoutes(routes)
	if len(routes) == 0 {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetUnresolved}
	}
	switch plan.TargetKind {
	case ConnectTargetKindTarget:
		if len(routes) > 1 {
			return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetAmbiguous}
		}
		return ConnectRoutePlanMaterialization{Target: routes[0]}
	case ConnectTargetKindTargetSet:
		return ConnectRoutePlanMaterialization{TargetSet: routes}
	default:
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureDeliveryTopologyInvalid}
	}
}

func InstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, matchValues map[string]string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.InstanceKey
	if instanceKey == nil || instanceKey.Field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	if instanceKey.Source.RequiresDeliveryProjection() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	if instanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	sourcePath := strings.TrimSpace(instanceKey.Source.Path)
	value := firstMatchValue(matchValues, sourcePath)
	if sourcePath == "" || value == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	values := map[string]any{instanceKey.Field.Path(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.flowID,
		Field:  instanceKey.Field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		Values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, ""
}

func EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, eventID string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.InstanceKey
	if instanceKey == nil || instanceKey.Field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	value := ""
	switch instanceKey.Source.Kind {
	case runtimecontracts.FlowInputInstanceSourceGeneratedUUID:
		value = deterministicResolutionUUID(plan, eventID)
	case runtimecontracts.FlowInputInstanceSourceEventID:
		value = eventID
	default:
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	values := map[string]any{instanceKey.Field.Path(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.flowID,
		Field:  instanceKey.Field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		Values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, ""
}

func deterministicResolutionUUID(plan ConnectRoutePlan, eventID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strings.TrimSpace(eventID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(plan.Receiver.flowID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(plan.Receiver.pin)))
	_, _ = h.Write([]byte{0})
	if plan.InstanceKey != nil && !plan.InstanceKey.Field.Empty() {
		_, _ = h.Write([]byte(plan.InstanceKey.Field.Path()))
	}
	sum := h.Sum(nil)
	b := append([]byte{}, sum[:16]...)
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[0:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:32])
}

func InstanceKeyDescriptorRoutesForConnectRoutePlan(plan ConnectRoutePlan, keyMaterial []runtimecontracts.TemplateInstanceKeyValue, descriptors []Descriptor) []events.RouteIdentity {
	if len(keyMaterial) == 0 {
		return nil
	}
	routes := make([]events.RouteIdentity, 0, len(descriptors))
	for _, descriptor := range descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		if ConnectInstanceKeyDescriptorMatches(keyMaterial, descriptor) {
			routes = append(routes, route)
		}
	}
	return uniqueRoutes(routes)
}

func connectInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	if source == nil {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if resolution.Empty() {
		return nil, ConnectRoutePlanIssue{}
	}
	return connectResolutionInstanceKey(source, connect, inputPin, resolution, receiverFlowID)
}

func connectResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	switch resolution.Mode {
	case runtimecontracts.FlowInputResolutionModeCreate, runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return connectCanonicalResolutionInstanceKey(source, connect, inputPin, resolution, receiverFlowID)
	case runtimecontracts.FlowInputResolutionModeFanIn:
		return nil, ConnectRoutePlanIssue{}
	case runtimecontracts.FlowInputResolutionModeReply:
		return nil, ConnectRoutePlanIssue{}
	default:
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", runtimecontracts.FlowInputResolutionModeCode(resolution.Mode))}
	}
}

func connectReplyResolution(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, sourceEndpoint ConnectRoutePlanEndpoint, receiverRef runtimecontracts.FlowPackagePinRef, inputPin runtimecontracts.FlowInputEventPin) (*ConnectRoutePlanReplyResolution, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Mode == runtimecontracts.FlowInputResolutionModeReply {
		resolution := inputPin.Resolution
		if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode reply may only declare replies_to and correlation_key"}
		}
		requestOutputPin := strings.TrimSpace(resolution.RepliesTo)
		if requestOutputPin == "" {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: "resolution mode reply requires replies_to"}
		}
		requestOutput, ok := source.FlowOutputEventPin(receiverRef.FlowID, requestOutputPin)
		if !ok {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply replies_to %q must name a same-flow output pin", requestOutputPin)}
		}
		correlationKey := strings.TrimSpace(resolution.CorrelationKey)
		if correlationKey != "" && !stringListContains(normalizedPinCarries(requestOutput.Carries), correlationKey) {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply correlation_key %q must name a carry declared by output pin %s", correlationKey, requestOutputPin)}
		}
		requestConnects := resolvedCompositionConnectsFrom(source, receiverRef.FlowID, requestOutputPin)
		if len(requestConnects) != 1 {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", receiverRef.FlowID, requestOutputPin, len(requestConnects))}
		}
		requestTarget := requestConnects[0].to
		if requestTarget.Root || strings.TrimSpace(requestTarget.FlowID) != sourceEndpoint.flowID {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: "resolution mode reply request and reply edges must connect the same provider flow"}
		}
		return &ConnectRoutePlanReplyResolution{
			Role:              ConnectReplyRoleResponse,
			RequesterFlowID:   strings.TrimSpace(receiverRef.FlowID),
			RequestOutputPin:  requestOutputPin,
			ReplyInputPin:     strings.TrimSpace(receiverRef.Pin),
			ProviderFlowID:    sourceEndpoint.flowID,
			ProviderInputPin:  strings.TrimSpace(requestTarget.Pin),
			ProviderOutputPin: sourceEndpoint.pin,
			CorrelationKey:    correlationKey,
		}, ConnectRoutePlanIssue{}
	}

	var matches []ConnectRoutePlanReplyResolution
	for _, replyInput := range source.FlowInputEventPins(sourceEndpoint.flowID) {
		if replyInput.Resolution.Mode != runtimecontracts.FlowInputResolutionModeReply || strings.TrimSpace(replyInput.Resolution.RepliesTo) != sourceEndpoint.pin {
			continue
		}
		for _, replyConnect := range resolvedCompositionConnectsTo(source, sourceEndpoint.flowID, replyInput.PinName()) {
			from := replyConnect.from
			if from.Root || strings.TrimSpace(from.FlowID) != strings.TrimSpace(receiverRef.FlowID) {
				continue
			}
			matches = append(matches, ConnectRoutePlanReplyResolution{
				Role:              ConnectReplyRoleRequest,
				RequesterFlowID:   sourceEndpoint.flowID,
				RequestOutputPin:  sourceEndpoint.pin,
				ReplyInputPin:     strings.TrimSpace(replyInput.PinName()),
				ProviderFlowID:    strings.TrimSpace(receiverRef.FlowID),
				ProviderInputPin:  strings.TrimSpace(receiverRef.Pin),
				ProviderOutputPin: strings.TrimSpace(from.Pin),
				CorrelationKey:    strings.TrimSpace(replyInput.Resolution.CorrelationKey),
			})
		}
	}
	if len(matches) > 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("request pin %s.%s participates in multiple reply loops", sourceEndpoint.flowID, sourceEndpoint.pin)}
	}
	if len(matches) == 1 {
		return &matches[0], ConnectRoutePlanIssue{}
	}
	return nil, ConnectRoutePlanIssue{}
}

type resolvedCompositionConnect struct {
	connect runtimecontracts.FlowPackageConnect
	from    runtimecontracts.FlowPackagePinRef
	to      runtimecontracts.FlowPackagePinRef
}

func resolvedCompositionConnectsTo(source semanticview.Source, flowID, pinName string) []resolvedCompositionConnect {
	return resolvedCompositionConnects(source, flowID, pinName, false)
}

func resolvedCompositionConnectsFrom(source semanticview.Source, flowID, pinName string) []resolvedCompositionConnect {
	return resolvedCompositionConnects(source, flowID, pinName, true)
}

func resolvedCompositionConnects(source semanticview.Source, flowID, pinName string, matchSource bool) []resolvedCompositionConnect {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || strings.TrimSpace(pinName) == "" {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	var out []resolvedCompositionConnect
	for _, connect := range bundle.CompositionConnects() {
		from, err := connect.FromRef()
		if err != nil {
			continue
		}
		to, err := connect.ToRef()
		if err != nil {
			continue
		}
		from, ok = resolveCompositionConnectEndpoint(source, connect.PackageKey, from)
		if !ok {
			continue
		}
		to, ok = resolveCompositionConnectEndpoint(source, connect.PackageKey, to)
		if !ok {
			continue
		}
		endpoint := to
		if matchSource {
			endpoint = from
		}
		if endpoint.Root != (flowID == "") || strings.TrimSpace(endpoint.FlowID) != flowID || strings.TrimSpace(endpoint.Pin) != pinName {
			continue
		}
		out = append(out, resolvedCompositionConnect{connect: connect, from: from, to: to})
	}
	return out
}

func resolveCompositionConnectEndpoint(source semanticview.Source, packageKey string, ref runtimecontracts.FlowPackagePinRef) (runtimecontracts.FlowPackagePinRef, bool) {
	ref.FlowID = strings.TrimSpace(ref.FlowID)
	ref.Pin = strings.TrimSpace(ref.Pin)
	if !ref.Root {
		return ref, ref.FlowID != "" && ref.Pin != ""
	}
	flowID, ok := semanticview.PackageRootFlowID(source, packageKey)
	if !ok {
		return runtimecontracts.FlowPackagePinRef{}, false
	}
	ref.FlowID = flowID
	ref.Root = flowID == ""
	return ref, ref.Pin != ""
}

func connectFanIn(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, receiverFlowID string) (*ConnectRoutePlanFanIn, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Empty() || inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeFanIn {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in may only declare aggregation, window, dedup_by, singleton, and carries"}
	}
	if resolution.Aggregation != "stream" && resolution.Aggregation != "barrier" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in aggregation must be stream or barrier, got %q", resolution.Aggregation)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "receiver singleton coordinator owner is unavailable for input pin resolution"}
	}
	if _, err := bundle.ResolveFlowSingletonCoordinator(receiverFlowID); err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	window := strings.TrimSpace(resolution.Window)
	if resolution.Aggregation == "stream" && window == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires window"}
	}
	dedupBy := normalizedStringList(resolution.DedupBy)
	if len(dedupBy) == 0 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires dedup_by; sender identity is not an implicit default"}
	}
	if len(dedupBy) != 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in stream supports exactly one dedup_by field in this slice, got %v", dedupBy)}
	}
	if !connectFanInDedupSupported(dedupBy[0], resolution.Aggregation) {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in dedup_by %q must be event.id or one top-level payload field", dedupBy[0])}
	}
	if window != "" && !connectFanInPayloadFieldSupported(window) {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in window %q must be one top-level payload field", window)}
	}
	singleton := strings.Trim(strings.TrimSpace(resolution.Singleton), "/")
	if singleton == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires explicit singleton receiver identity"}
	}
	scopeKey := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, receiverFlowID)), "/")
	if scopeKey != "" && singleton != scopeKey && !strings.HasPrefix(singleton, scopeKey+"/") {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in singleton %q must be the receiver singleton route or a child of %q", singleton, scopeKey)}
	}
	aggregation := ConnectFanInStream
	if resolution.Aggregation == "barrier" {
		aggregation = ConnectFanInBarrier
	}
	return &ConnectRoutePlanFanIn{
		Aggregation: aggregation,
		Window:      window,
		DedupBy:     dedupBy,
		Singleton:   singleton,
	}, ConnectRoutePlanIssue{}
}

func connectFanInDedupSupported(dedup, aggregation string) bool {
	dedup = strings.TrimSpace(dedup)
	return (strings.TrimSpace(aggregation) == "stream" && dedup == "event.id") || connectFanInPayloadFieldSupported(dedup)
}

func connectFanInPayloadFieldSupported(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".")
}

func connectCanonicalResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	mode := resolution.Mode
	modeText := runtimecontracts.FlowInputResolutionModeCode(mode)
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s may only declare mode and carries", modeText)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureLifecycleUnavailable, Detail: "receiver instance contract owner is unavailable"}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	if instance.Field.Empty() {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s requires receiver `instance: <field>`", modeText)}
	}
	evidence, err := bundle.ResolveFlowInputInstanceSourceType(source, receiverFlowID, inputPin, instance)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	return &ConnectRoutePlanInstanceKey{
		Mode:   mode,
		Field:  instance.Field,
		Source: evidence.Source,
	}, ConnectRoutePlanIssue{}
}

func connectResolutionKind(scope semanticview.FlowScope, instanceKey *ConnectRoutePlanInstanceKey) ConnectRoutePlanResolutionKind {
	if !receiverRequiresRuntimeResolution(scope) {
		return ConnectResolutionStatic
	}
	if instanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return ConnectRoutePlanResolutionKind{}
}

func connectRoutePlanResolutionKind(plan ConnectRoutePlan) ConnectRoutePlanResolutionKind {
	if !plan.ResolutionKind.empty() {
		return plan.ResolutionKind
	}
	if !plan.Target.Empty() || len(plan.TargetSet) > 0 {
		return ConnectResolutionStatic
	}
	if plan.InstanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return ConnectRoutePlanResolutionKind{}
}

func receiverRequiresRuntimeResolution(scope semanticview.FlowScope) bool {
	switch strings.TrimSpace(scope.Mode) {
	case "template", "dynamic":
		return true
	default:
		return false
	}
}

func staticConnectRoute(source semanticview.Source, flowID string) events.RouteIdentity {
	flowInstance := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, flowID)), "/")
	if flowInstance == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowID),
		FlowInstance: flowInstance,
		EntityID:     runtimeflowidentity.EntityID(flowInstance),
	}.Normalized()
}

func fanInSingletonRoute(flowID, singleton string) events.RouteIdentity {
	singleton = strings.Trim(strings.TrimSpace(singleton), "/")
	if strings.TrimSpace(flowID) == "" || singleton == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowID),
		FlowInstance: singleton,
		EntityID:     runtimeflowidentity.EntityID(singleton),
	}.Normalized()
}

func ConnectInstanceKeyDescriptorMatches(keyMaterial []runtimecontracts.TemplateInstanceKeyValue, descriptor Descriptor) bool {
	if len(keyMaterial) == 0 {
		return false
	}
	for _, key := range keyMaterial {
		field := key.Field.Path()
		value := strings.Trim(strings.TrimSpace(key.Value), "/")
		if field == "" || value == "" {
			return false
		}
		actual, ok := descriptor.AddressFields["entity."+field]
		if !ok {
			actual, ok = descriptor.AddressFields[field]
		}
		if !ok || strings.Trim(strings.TrimSpace(actual), "/") != value {
			return false
		}
	}
	return true
}

func descriptorRouteForReceiver(plan ConnectRoutePlan, descriptor Descriptor) events.RouteIdentity {
	if !descriptorBelongsToReceiver(plan, descriptor) {
		return events.RouteIdentity{}
	}
	return descriptorRoute(plan.Receiver.flowID, descriptor)
}

func descriptorBelongsToReceiver(plan ConnectRoutePlan, descriptor Descriptor) bool {
	flowInstance := strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/")
	if flowInstance == "" {
		return false
	}
	receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.flowPath), "/")
	if receiverPath == "" {
		receiverPath = strings.Trim(strings.TrimSpace(plan.Receiver.flowID), "/")
	}
	return receiverPath != "" && (flowInstance == receiverPath || strings.HasPrefix(flowInstance, receiverPath+"/"))
}

func firstMatchValue(values map[string]string, keys ...string) string {
	if len(values) == 0 {
		return ""
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func normalizedPinCarries(in []string) []string {
	return normalizedStringList(in)
}

func normalizedStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringListContains(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}

func duplicateString(values []string) string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := map[string]struct{}{}
	for _, value := range left {
		value = strings.TrimSpace(value)
		if value == "" {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if value == "" {
			return false
		}
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
