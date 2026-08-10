package pinrouting

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
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
	canonical, err := json.Marshal(connectExecutionClaimCodec(plan, route))
	if err != nil {
		return events.ConnectExecutionClaim{}, fmt.Errorf("encode connect execution claim: %w", err)
	}
	digest := sha256.Sum256(canonical)
	pinCanonical, err := json.Marshal(connectEndpointPinCodec(plan.receiver))
	if err != nil {
		return events.ConnectExecutionClaim{}, fmt.Errorf("encode connect receiver pin claim: %w", err)
	}
	pinDigest := sha256.Sum256(pinCanonical)
	return events.AdmitConnectExecutionClaim(digest, pinDigest, route, plan.receiver.event.value)
}

// ConnectPlanIdentity derives the plan-only identity used by evaluation
// evidence. Recipient-specific execution claims are derived separately.
func ConnectPlanIdentity(plan ConnectRoutePlan) (events.ConnectPlanIdentity, error) {
	canonical, err := json.Marshal(connectPlanIdentityCodec(plan))
	if err != nil {
		return events.ConnectPlanIdentity{}, fmt.Errorf("encode connect plan identity: %w", err)
	}
	return events.AdmitConnectPlanIdentity(sha256.Sum256(canonical)), nil
}

type connectExecutionClaimEndpointCodec struct {
	Kind          uint8                   `json:"kind"`
	FlowID        string                  `json:"flow_id,omitempty"`
	FlowPath      string                  `json:"flow_path,omitempty"`
	PinDirection  ConnectEndpointRoleKind `json:"pin_direction"`
	Pin           string                  `json:"pin"`
	LocalEvent    events.EventType        `json:"local_event"`
	ResolvedEvent events.EventType        `json:"resolved_event"`
	Key           string                  `json:"key,omitempty"`
	Carries       []string                `json:"carries,omitempty"`
}

type connectExecutionClaimInstanceCodec struct {
	Mode       runtimecontracts.FlowInputResolutionMode `json:"mode"`
	Field      string                                   `json:"field"`
	SourceKind uint8                                    `json:"source_kind"`
	SourcePath string                                   `json:"source_path"`
}

type connectExecutionClaimFanInCodec struct {
	Aggregation uint8    `json:"aggregation"`
	Window      string   `json:"window"`
	DedupBy     []string `json:"dedup_by"`
	Singleton   string   `json:"singleton"`
}

type connectExecutionClaimReplyCodec struct {
	Role              uint8  `json:"role"`
	RequesterFlowID   string `json:"requester_flow_id"`
	RequestOutputPin  string `json:"request_output_pin"`
	ReplyInputPin     string `json:"reply_input_pin"`
	ProviderFlowID    string `json:"provider_flow_id"`
	ProviderInputPin  string `json:"provider_input_pin"`
	ProviderOutputPin string `json:"provider_output_pin"`
	CorrelationKey    string `json:"correlation_key"`
}

type connectPlanIdentityWire struct {
	Source                      connectExecutionClaimEndpointCodec   `json:"source"`
	Receiver                    connectExecutionClaimEndpointCodec   `json:"receiver"`
	TargetKind                  uint8                                `json:"target_kind"`
	ResolutionKind              uint8                                `json:"resolution_kind"`
	InstanceKey                 *connectExecutionClaimInstanceCodec  `json:"instance_key,omitempty"`
	FanIn                       *connectExecutionClaimFanInCodec     `json:"fan_in,omitempty"`
	Reply                       *connectExecutionClaimReplyCodec     `json:"reply,omitempty"`
	Target                      events.RouteIdentity                 `json:"target"`
	TargetSet                   []events.RouteIdentity               `json:"target_set,omitempty"`
	ProviderOutputAuthorization *runtimeprovideroutput.Authorization `json:"provider_output_authorization,omitempty"`
}

type connectExecutionClaimPlanCodec struct {
	Plan      connectPlanIdentityWire `json:"plan"`
	Recipient events.DeliveryRoute    `json:"recipient"`
}

func connectExecutionClaimCodec(plan ConnectRoutePlan, route events.DeliveryRoute) connectExecutionClaimPlanCodec {
	return connectExecutionClaimPlanCodec{Plan: connectPlanIdentityCodec(plan), Recipient: route}
}

func connectPlanIdentityCodec(plan ConnectRoutePlan) connectPlanIdentityWire {
	codec := connectPlanIdentityWire{
		Source: connectEndpointCodec(plan.source), Receiver: connectEndpointCodec(plan.receiver),
		TargetKind: uint8(plan.targetKind), ResolutionKind: uint8(plan.resolutionKind),
		Target: plan.target.Normalized(), TargetSet: connectClaimTargets(plan.targetSet),
		ProviderOutputAuthorization: plan.ProviderOutputAuthorization(),
	}
	if plan.instanceKey != nil {
		codec.InstanceKey = &connectExecutionClaimInstanceCodec{
			Mode: plan.instanceKey.mode, Field: plan.instanceKey.field.Path(),
			SourceKind: uint8(plan.instanceKey.source.kind), SourcePath: plan.instanceKey.source.path.value,
		}
	}
	if plan.fanIn != nil {
		dedupBy := make([]string, 0, len(plan.fanIn.dedupBy))
		for _, field := range plan.fanIn.dedupBy {
			dedupBy = append(dedupBy, field.value)
		}
		codec.FanIn = &connectExecutionClaimFanInCodec{
			Aggregation: uint8(plan.fanIn.aggregation), Window: plan.fanIn.window.value,
			DedupBy: dedupBy, Singleton: plan.fanIn.singleton.value,
		}
	}
	if plan.replyResolution != nil {
		reply := plan.replyResolution
		codec.Reply = &connectExecutionClaimReplyCodec{
			Role: uint8(reply.role), RequesterFlowID: reply.requesterFlowID.value,
			RequestOutputPin: reply.requestOutputPin.value, ReplyInputPin: reply.replyInputPin.value,
			ProviderFlowID: reply.providerFlowID.value, ProviderInputPin: reply.providerInputPin.value,
			ProviderOutputPin: reply.providerOutputPin.value, CorrelationKey: reply.correlationKey.value,
		}
	}
	return codec
}

func connectClaimTargets(targets []events.RouteIdentity) []events.RouteIdentity {
	out := make([]events.RouteIdentity, 0, len(targets))
	for _, target := range targets {
		out = append(out, target.Normalized())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowID != out[j].FlowID {
			return out[i].FlowID < out[j].FlowID
		}
		if out[i].FlowInstance != out[j].FlowInstance {
			return out[i].FlowInstance < out[j].FlowInstance
		}
		return out[i].EntityID < out[j].EntityID
	})
	return out
}

func connectEndpointCodec(endpoint ConnectRoutePlanEndpoint) connectExecutionClaimEndpointCodec {
	carries := make([]string, 0, len(endpoint.carries))
	for _, carry := range endpoint.carries {
		carries = append(carries, carry.value)
	}
	return connectExecutionClaimEndpointCodec{
		Kind: uint8(endpoint.kind), FlowID: endpoint.flowID.value, FlowPath: endpoint.flowPath.value,
		PinDirection: endpoint.pin.direction, Pin: endpoint.pin.value,
		LocalEvent: endpoint.event.value, ResolvedEvent: endpoint.resolvedEvent.value,
		Key: endpoint.key.value, Carries: carries,
	}
}

func connectEndpointPinCodec(endpoint ConnectRoutePlanEndpoint) any {
	return struct {
		FlowID    string                  `json:"flow_id"`
		FlowPath  string                  `json:"flow_path"`
		Direction ConnectEndpointRoleKind `json:"direction"`
		Pin       string                  `json:"pin"`
		Event     events.EventType        `json:"event"`
	}{endpoint.flowID.value, endpoint.flowPath.value, endpoint.pin.direction, endpoint.pin.value, endpoint.event.value}
}

type ConnectRoutePlanTargetKind uint8

const (
	ConnectTargetKindTarget ConnectRoutePlanTargetKind = iota + 1
	ConnectTargetKindTargetSet
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

type ConnectRoutePlanResolutionKind uint8

const (
	ConnectResolutionStatic ConnectRoutePlanResolutionKind = iota + 1
	ConnectResolutionInstanceKey
	ConnectResolutionReply
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

func (k ConnectRoutePlanResolutionKind) empty() bool { return k == 0 }

type ConnectRoutePlanFailure uint8

const (
	ConnectFailureSourceMissing ConnectRoutePlanFailure = iota + 1
	ConnectFailureSourceLocationMissing
	ConnectFailurePinRefInvalid
	ConnectFailureProducerFlowMissing
	ConnectFailureProducerOutputPinMissing
	ConnectFailureReceiverFlowMissing
	ConnectFailureReceiverInputPinMissing
	ConnectFailureReceiverResolutionMissing
	ConnectFailureEventAliasAdapterInvalid
	ConnectFailureSyntheticCarryCollision
	ConnectFailureRootReceiverResolution
	ConnectFailureDeliveryTopologyInvalid
	ConnectFailureReplyLineageMissing
	ConnectFailureInstanceSourceValueMissing
	ConnectFailureTargetUnresolved
	ConnectFailureTargetAmbiguous
	ConnectFailureInstanceResolutionInvalid
	ConnectFailureInstanceConflict
	ConnectFailureLifecycleUnavailable
)

func (f ConnectRoutePlanFailure) Empty() bool { return f == 0 }

func (f ConnectRoutePlanFailure) Code() string {
	switch f {
	case ConnectFailureSourceMissing:
		return "source_missing"
	case ConnectFailureSourceLocationMissing:
		return "connect_source_location_missing"
	case ConnectFailurePinRefInvalid:
		return "connect_pin_ref_invalid"
	case ConnectFailureProducerFlowMissing:
		return "producer_flow_missing"
	case ConnectFailureProducerOutputPinMissing:
		return "producer_output_pin_missing"
	case ConnectFailureReceiverFlowMissing:
		return "receiver_flow_missing"
	case ConnectFailureReceiverInputPinMissing:
		return "receiver_input_pin_missing"
	case ConnectFailureReceiverResolutionMissing:
		return "receiver_resolution_missing"
	case ConnectFailureEventAliasAdapterInvalid:
		return "event_alias_or_adapter_invalid"
	case ConnectFailureSyntheticCarryCollision:
		return "synthetic_carry_payload_collision"
	case ConnectFailureRootReceiverResolution:
		return "root_receiver_resolution_invalid"
	case ConnectFailureDeliveryTopologyInvalid:
		return "delivery_topology_invalid"
	case ConnectFailureReplyLineageMissing:
		return "reply_lineage_missing"
	case ConnectFailureInstanceSourceValueMissing:
		return "route_plan_instance_source_value_missing"
	case ConnectFailureTargetUnresolved:
		return "route_plan_target_unresolved"
	case ConnectFailureTargetAmbiguous:
		return "route_plan_target_ambiguous"
	case ConnectFailureInstanceResolutionInvalid:
		return "route_plan_instance_resolution_invalid"
	case ConnectFailureInstanceConflict:
		return "route_plan_instance_conflict"
	case ConnectFailureLifecycleUnavailable:
		return "route_plan_lifecycle_unavailable"
	default:
		return ""
	}
}

type connectEndpointKind uint8

const (
	connectEndpointRoot connectEndpointKind = iota + 1
	connectEndpointExternalIngress
	connectEndpointStaticFlow
	connectEndpointTemplateFlow
)

type connectFlowID struct{ value string }
type connectFlowPath struct{ value string }
type connectPinID struct {
	direction ConnectEndpointRoleKind
	value     string
}
type connectLocalEvent struct{ value events.EventType }
type connectResolvedEvent struct{ value events.EventType }
type connectFieldPath struct{ value string }

// ConnectRoutePlanEndpoint is an opaque admitted graph endpoint. Local and
// resolved event identities remain distinct and cannot be manufactured by a
// downstream routing consumer.
type ConnectRoutePlanEndpoint struct {
	kind          connectEndpointKind
	flowID        connectFlowID
	flowPath      connectFlowPath
	pin           connectPinID
	event         connectLocalEvent
	resolvedEvent connectResolvedEvent
	key           connectFieldPath
	carries       []connectFieldPath
}

func newConnectRoutePlanEndpoint(direction ConnectEndpointRoleKind, root bool, flowID, flowPath, mode, pin, event, resolvedEvent, key string, carries []string) ConnectRoutePlanEndpoint {
	kind := connectEndpointStaticFlow
	switch {
	case strings.TrimSpace(mode) == "external":
		kind = connectEndpointExternalIngress
	case root:
		kind = connectEndpointRoot
	case strings.TrimSpace(mode) == runtimecontracts.FlowModeTemplate:
		kind = connectEndpointTemplateFlow
	}
	admittedCarries := make([]connectFieldPath, 0, len(carries))
	for _, carry := range carries {
		if carry = strings.TrimSpace(carry); carry != "" {
			admittedCarries = append(admittedCarries, connectFieldPath{value: carry})
		}
	}
	return ConnectRoutePlanEndpoint{
		kind:          kind,
		flowID:        connectFlowID{value: strings.TrimSpace(flowID)},
		flowPath:      connectFlowPath{value: strings.Trim(strings.TrimSpace(flowPath), "/")},
		pin:           connectPinID{direction: direction, value: strings.TrimSpace(pin)},
		event:         connectLocalEvent{value: events.EventType(eventidentity.Normalize(event))},
		resolvedEvent: connectResolvedEvent{value: events.EventType(eventidentity.Normalize(resolvedEvent))},
		key:           connectFieldPath{value: strings.TrimSpace(key)},
		carries:       admittedCarries,
	}
}

func (e ConnectRoutePlanEndpoint) IsRoot() bool { return e.kind == connectEndpointRoot }
func (e ConnectRoutePlanEndpoint) IsExternalIngress() bool {
	return e.kind == connectEndpointExternalIngress
}
func (e ConnectRoutePlanEndpoint) IsTemplate() bool { return e.kind == connectEndpointTemplateFlow }

// ConnectRoutePlanEndpointReadback is a one-way display projection. No graph
// evaluator or application API accepts this type.
type ConnectRoutePlanEndpointReadback struct {
	FlowID        string
	FlowPath      string
	Pin           string
	LocalEvent    string
	ResolvedEvent string
	Key           string
	Carries       []string
}

func (e ConnectRoutePlanEndpoint) Readback() ConnectRoutePlanEndpointReadback {
	carries := make([]string, 0, len(e.carries))
	for _, carry := range e.carries {
		carries = append(carries, carry.value)
	}
	return ConnectRoutePlanEndpointReadback{
		FlowID: e.flowID.value, FlowPath: e.flowPath.value, Pin: e.pin.value,
		LocalEvent: string(e.event.value), ResolvedEvent: string(e.resolvedEvent.value),
		Key: e.key.value, Carries: carries,
	}
}

func (e ConnectRoutePlanEndpoint) matchesFlowPin(flowID, pin string, direction ConnectEndpointRoleKind) bool {
	return e.pin.direction == direction && e.flowID.value == flowID && e.pin.value == pin
}

func (e ConnectRoutePlanEndpoint) receiverRoute(flowInstance, entityID string) events.RouteIdentity {
	return events.RouteIdentity{FlowID: e.flowID.value, FlowInstance: flowInstance, EntityID: entityID}.Normalized()
}

func (e ConnectRoutePlanEndpoint) Empty() bool {
	return e.kind == 0 && e.flowID.value == "" && e.pin.value == "" && e.event.value == ""
}

func (e ConnectRoutePlanEndpoint) Route(flowInstance, entityID string) events.RouteIdentity {
	return e.receiverRoute(flowInstance, entityID)
}

func (e ConnectRoutePlanEndpoint) receiverFlowTemplate(bundle *runtimecontracts.WorkflowContractBundle) (runtimecontracts.TemplateInstanceContract, error) {
	if bundle == nil || e.IsRoot() || !e.IsTemplate() {
		return runtimecontracts.TemplateInstanceContract{}, fmt.Errorf("connect receiver is not a template flow")
	}
	return bundle.ResolveFlowTemplateInstance(e.flowID.value)
}

type ConnectReceiverPinIdentity struct {
	digest     [32]byte
	diagnostic string
}

func (i ConnectReceiverPinIdentity) Empty() bool { return i == ConnectReceiverPinIdentity{} }
func (i ConnectReceiverPinIdentity) Equal(other ConnectReceiverPinIdentity) bool {
	return i.digest == other.digest
}
func (i ConnectReceiverPinIdentity) Diagnostic() string { return i.diagnostic }
func (i ConnectReceiverPinIdentity) EvidenceIdentity() events.ConnectReceiverIdentity {
	return events.AdmitConnectReceiverIdentity(i.digest)
}

type ConnectRecipientKind uint8

const (
	ConnectRecipientNode ConnectRecipientKind = iota + 1
	ConnectRecipientAgent
)

// ConnectRecipient is an admitted receiver projection. The graph owns the
// receiver pin and target association; consumers may only inspect the typed
// recipient selected by that evaluation.
type ConnectRecipient struct {
	kind          ConnectRecipientKind
	id            string
	path          string
	agentIdentity agentidentity.Identity
	handlerEvent  events.EventType
}

func NewConnectNodeRecipient(id, path string) (ConnectRecipient, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return ConnectRecipient{}, fmt.Errorf("connect node recipient id is required")
	}
	return ConnectRecipient{kind: ConnectRecipientNode, id: id, path: eventidentity.Normalize(path)}, nil
}

func NewConnectAgentRecipient(id, path string, identity agentidentity.Identity) (ConnectRecipient, error) {
	id = strings.TrimSpace(id)
	identity = identity.Normalize()
	if id == "" {
		return ConnectRecipient{}, fmt.Errorf("connect agent recipient id is required")
	}
	if err := identity.Validate(); err != nil || identity.AgentID() != id {
		return ConnectRecipient{}, fmt.Errorf("connect agent recipient requires its exact concrete identity")
	}
	return ConnectRecipient{kind: ConnectRecipientAgent, id: id, path: eventidentity.Normalize(path), agentIdentity: identity}, nil
}

func (r ConnectRecipient) Kind() ConnectRecipientKind            { return r.kind }
func (r ConnectRecipient) ID() string                            { return r.id }
func (r ConnectRecipient) Path() string                          { return r.path }
func (r ConnectRecipient) AgentIdentity() agentidentity.Identity { return r.agentIdentity }
func (r ConnectRecipient) HandlerEvent() events.EventType        { return r.handlerEvent }

type ConnectRecipientRegistration struct {
	receiverPin ConnectReceiverPinIdentity
	recipient   ConnectRecipient
}

type ConnectRecipientEvaluation struct {
	matched                   bool
	requiresRuntimeResolution bool
	recipients                []ConnectRecipient
	plans                     []events.ConnectPlanEvaluation
	err                       error
}

func (e ConnectRecipientEvaluation) Matched() bool { return e.matched }
func (e ConnectRecipientEvaluation) RequiresRuntimeResolution() bool {
	return e.requiresRuntimeResolution
}
func (e ConnectRecipientEvaluation) Recipients() []ConnectRecipient {
	return append([]ConnectRecipient(nil), e.recipients...)
}
func (e ConnectRecipientEvaluation) Ledger() (events.ConnectEvaluationLedger, error) {
	if e.err != nil {
		return events.ConnectEvaluationLedger{}, e.err
	}
	return events.NewConnectEvaluationLedger(e.plans)
}

func (e ConnectRoutePlanEndpoint) receiverPinIdentity() ConnectReceiverPinIdentity {
	if e.pin.direction != ConnectEndpointRoleConsumer || e.pin.value == "" || e.event.value == "" {
		return ConnectReceiverPinIdentity{}
	}
	canonical, _ := json.Marshal(connectEndpointPinCodec(e))
	digest := sha256.Sum256(canonical)
	return ConnectReceiverPinIdentity{digest: digest, diagnostic: e.flowID.value + "." + e.pin.value + ":" + string(e.event.value)}
}

func (e ConnectRoutePlanEndpoint) receiverEventTypes(target events.RouteIdentity) []events.EventType {
	seen := map[events.EventType]struct{}{}
	add := func(eventType events.EventType) {
		if eventType != "" {
			seen[eventType] = struct{}{}
		}
	}
	add(e.resolvedEvent.value)
	receiverPath := e.flowPath.value
	if receiverPath == "" {
		receiverPath = e.flowID.value
	}
	if receiverPath != "" && e.event.value != "" {
		add(events.EventType(receiverPath + "/" + string(e.event.value)))
	}
	target = target.Normalized()
	if target.FlowInstance != "" && e.event.value != "" {
		add(events.EventType(target.FlowInstance + "/" + string(e.event.value)))
	}
	if e.IsRoot() {
		add(e.event.value)
	}
	out := make([]events.EventType, 0, len(seen))
	for eventType := range seen {
		out = append(out, eventType)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (p ConnectRoutePlan) ReceiverPinIdentity() ConnectReceiverPinIdentity {
	return p.receiver.receiverPinIdentity()
}

func (e ConnectRoutePlanEndpoint) subscriberPathMatchesReceiver(subscriberPath string, target events.RouteIdentity) bool {
	receiverPath := e.flowPath.value
	if receiverPath == "" {
		receiverPath = e.flowID.value
	}
	target = target.Normalized()
	return subscriberPath != "" && receiverPath != "" && target.FlowInstance != "" && subscriberPath == receiverPath &&
		(target.FlowInstance == receiverPath || runtimeflowidentity.SemanticScopeFromInstancePath(target.FlowInstance) == receiverPath)
}

func (p ConnectRoutePlan) ReceiverLocalEvent() events.EventType { return p.receiver.event.value }

func (p ConnectRoutePlan) ReceiverRoute(flowInstance, entityID string) events.RouteIdentity {
	return p.receiver.receiverRoute(flowInstance, entityID)
}

func (p ConnectRoutePlan) ReceiverTemplate(bundle *runtimecontracts.WorkflowContractBundle) (runtimecontracts.TemplateInstanceContract, error) {
	return p.receiver.receiverFlowTemplate(bundle)
}

func (p ConnectRoutePlan) DeriveReceiverIdentity(source semanticview.Source, instanceID string) runtimeflowidentity.Instance {
	return runtimeflowidentity.Derive(source, p.receiver.flowID.value, instanceID)
}

func (p ConnectRoutePlan) ReceiverKeyDigest(keyMaterial []runtimecontracts.TemplateInstanceKeyValue) string {
	if p.receiver.flowID.value == "" || len(keyMaterial) == 0 {
		return ""
	}
	type keyValue struct {
		Field string `json:"field"`
		Value string `json:"value"`
	}
	values := make([]keyValue, 0, len(keyMaterial))
	for _, key := range keyMaterial {
		values = append(values, keyValue{Field: key.Field.Path(), Value: strings.TrimSpace(key.Value)})
	}
	canonical, _ := json.Marshal(struct {
		FlowID string     `json:"flow_id"`
		Keys   []keyValue `json:"keys"`
	}{FlowID: p.receiver.flowID.value, Keys: values})
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

func (p ConnectRoutePlan) SourceParentRoute(sourceEvent SourceEvent) runtimeflowidentity.ParentRoute {
	if !connectSourceEndpointMatches(p.source, sourceEvent) {
		return runtimeflowidentity.ParentRoute{}
	}
	return runtimeflowidentity.ParentRoute{
		FlowID:       sourceEvent.route.FlowID,
		FlowInstance: sourceEvent.route.FlowInstance,
		EntityID:     sourceEvent.route.EntityID,
	}
}

func (p ConnectRoutePlan) ReplyRole() ConnectReplyRole {
	if p.replyResolution == nil {
		return 0
	}
	return p.replyResolution.role
}

func (p ConnectRoutePlan) ReplyRequestRecord(evt events.Event, origin events.RouteIdentity, values ConnectRouteMatchValues, now time.Time) (runtimereplycontext.Record, error) {
	if p.replyResolution == nil {
		return runtimereplycontext.Record{}, fmt.Errorf("connect route plan has no reply resolution")
	}
	return p.replyResolution.requestRecord(evt, origin, values, now)
}

func (p ConnectRoutePlan) MatchesReplyRecord(record runtimereplycontext.Record) bool {
	return p.replyResolution != nil && p.replyResolution.matchesRecord(record)
}

func (p ConnectRoutePlan) ReplyResponseCorrelation(values ConnectRouteMatchValues) (string, bool) {
	if p.replyResolution == nil {
		return "", false
	}
	return p.replyResolution.responseCorrelation(values)
}

func connectSourceEndpointMatches(endpoint ConnectRoutePlanEndpoint, sourceEvent SourceEvent) bool {
	event := sourceEvent.eventType
	local := endpoint.event.value
	resolved := endpoint.resolvedEvent.value
	if event == "" || local == "" {
		return false
	}
	route := sourceEvent.route
	scope := endpoint.flowPath.value
	if scope == "" {
		scope = endpoint.flowID.value
	}
	switch sourceEvent.kind {
	case events.RoutingSourceExternalIngress:
		return endpoint.IsExternalIngress() && (event == local || event == resolved)
	case events.RoutingSourceRoot:
		return endpoint.IsRoot() && route.FlowID == "" && route.FlowInstance == "" && (event == local || event == resolved)
	case events.RoutingSourceStaticFlow:
		if endpoint.IsRoot() || endpoint.IsTemplate() || route.FlowID != endpoint.flowID.value || route.FlowInstance != scope {
			return false
		}
		return event == resolved || event == events.EventType(scope+"/"+string(local))
	case events.RoutingSourceConcreteTemplateInstance:
		if endpoint.IsRoot() || !endpoint.IsTemplate() || route.FlowID != endpoint.flowID.value {
			return false
		}
		return event == events.EventType(route.FlowInstance+"/"+string(local))
	case events.RoutingSourceFlowOwnedControl:
		return !endpoint.IsRoot() && route.FlowID == endpoint.flowID.value && event == resolved
	case events.RoutingSourceAbsent, events.RoutingSourcePlatformControl:
		return false
	default:
		return false
	}
}

type ConnectRoutePlanInstanceKey struct {
	mode   runtimecontracts.FlowInputResolutionMode
	field  runtimecontracts.TemplateInstanceField
	source connectInstanceSource
}

type connectInstanceSourceKind uint8

const (
	connectInstanceSourcePayload connectInstanceSourceKind = iota + 1
	connectInstanceSourceEventID
	connectInstanceSourceGeneratedUUID
)

type connectInstanceSource struct {
	kind connectInstanceSourceKind
	path connectFieldPath
}

func newConnectInstanceSource(source runtimecontracts.FlowInputInstanceSource) connectInstanceSource {
	kind := connectInstanceSourceKind(0)
	switch source.Kind {
	case runtimecontracts.FlowInputInstanceSourcePayload:
		kind = connectInstanceSourcePayload
	case runtimecontracts.FlowInputInstanceSourceEventID:
		kind = connectInstanceSourceEventID
	case runtimecontracts.FlowInputInstanceSourceGeneratedUUID:
		kind = connectInstanceSourceGeneratedUUID
	}
	return connectInstanceSource{kind: kind, path: connectFieldPath{value: strings.TrimSpace(source.Path)}}
}

type ConnectRoutePlanInstanceKeyReadback struct {
	Mode       string
	Field      string
	SourceKind string
	SourcePath string
}

func (k ConnectRoutePlanInstanceKey) Mode() runtimecontracts.FlowInputResolutionMode { return k.mode }
func (k ConnectRoutePlanInstanceKey) Field() runtimecontracts.TemplateInstanceField  { return k.field }
func (k ConnectRoutePlanInstanceKey) RequiresDeliveryProjection() bool {
	return k.source.kind == connectInstanceSourceGeneratedUUID || k.source.kind == connectInstanceSourceEventID
}
func (k ConnectRoutePlanInstanceKey) Readback() ConnectRoutePlanInstanceKeyReadback {
	sourceKind := ""
	switch k.source.kind {
	case connectInstanceSourcePayload:
		sourceKind = string(runtimecontracts.FlowInputInstanceSourcePayload)
	case connectInstanceSourceEventID:
		sourceKind = string(runtimecontracts.FlowInputInstanceSourceEventID)
	case connectInstanceSourceGeneratedUUID:
		sourceKind = string(runtimecontracts.FlowInputInstanceSourceGeneratedUUID)
	}
	return ConnectRoutePlanInstanceKeyReadback{
		Mode:       runtimecontracts.FlowInputResolutionModeCode(k.mode),
		Field:      k.field.Path(),
		SourceKind: sourceKind,
		SourcePath: k.source.path.value,
	}
}

type ConnectRoutePlanFanIn struct {
	aggregation ConnectFanInAggregation
	window      connectFieldPath
	dedupBy     []connectFieldPath
	singleton   connectFlowPath
}

type ConnectRoutePlanFanInReadback struct {
	Aggregation string
	Window      string
	DedupBy     []string
	Singleton   string
}

func (f ConnectRoutePlanFanIn) Aggregation() ConnectFanInAggregation { return f.aggregation }
func (f ConnectRoutePlanFanIn) Readback() ConnectRoutePlanFanInReadback {
	dedupBy := make([]string, 0, len(f.dedupBy))
	for _, path := range f.dedupBy {
		dedupBy = append(dedupBy, path.value)
	}
	return ConnectRoutePlanFanInReadback{
		Aggregation: f.aggregation.Code(),
		Window:      f.window.value,
		DedupBy:     dedupBy,
		Singleton:   f.singleton.value,
	}
}

type ConnectFanInAggregation uint8

const (
	ConnectFanInStream ConnectFanInAggregation = iota + 1
	ConnectFanInBarrier
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
	role              ConnectReplyRole
	requesterFlowID   connectFlowID
	requestOutputPin  connectPinID
	replyInputPin     connectPinID
	providerFlowID    connectFlowID
	providerInputPin  connectPinID
	providerOutputPin connectPinID
	correlationKey    connectFieldPath
}

type ConnectRoutePlanReplyReadback struct {
	Role              string
	RequesterFlowID   string
	RequestOutputPin  string
	ReplyInputPin     string
	ProviderFlowID    string
	ProviderInputPin  string
	ProviderOutputPin string
	CorrelationKey    string
}

func (r ConnectRoutePlanReplyResolution) Role() ConnectReplyRole { return r.role }
func (r ConnectRoutePlanReplyResolution) Readback() ConnectRoutePlanReplyReadback {
	return ConnectRoutePlanReplyReadback{
		Role:              r.role.Code(),
		RequesterFlowID:   r.requesterFlowID.value,
		RequestOutputPin:  r.requestOutputPin.value,
		ReplyInputPin:     r.replyInputPin.value,
		ProviderFlowID:    r.providerFlowID.value,
		ProviderInputPin:  r.providerInputPin.value,
		ProviderOutputPin: r.providerOutputPin.value,
		CorrelationKey:    r.correlationKey.value,
	}
}

func (r ConnectRoutePlanReplyResolution) requestRecord(evt events.Event, origin events.RouteIdentity, values ConnectRouteMatchValues, now time.Time) (runtimereplycontext.Record, error) {
	if r.role != ConnectReplyRoleRequest {
		return runtimereplycontext.Record{}, fmt.Errorf("connect route plan is not a reply request")
	}
	origin = origin.Normalized()
	if origin.Empty() {
		return runtimereplycontext.Record{}, fmt.Errorf("reply request has no admitted concrete origin route")
	}
	correlation := strings.TrimSpace(evt.ID())
	if r.correlationKey.value != "" {
		correlation = values.value(connectFieldPath{value: "payload." + r.correlationKey.value})
		if correlation == "" {
			return runtimereplycontext.Record{}, fmt.Errorf("reply request is missing its declared carried correlation value")
		}
	}
	if now.IsZero() {
		return runtimereplycontext.Record{}, fmt.Errorf("reply request requires an admitted evaluation time")
	}
	record := runtimereplycontext.Record{
		RunID:                evt.RunID(),
		RequestEventID:       evt.ID(),
		RequesterFlowID:      r.requesterFlowID.value,
		RequestOutputPin:     r.requestOutputPin.value,
		ReplyInputPin:        r.replyInputPin.value,
		ProviderFlowID:       r.providerFlowID.value,
		ProviderInputPin:     r.providerInputPin.value,
		ProviderOutputPin:    r.providerOutputPin.value,
		Origin:               origin,
		RequestCorrelationID: correlation,
		CorrelationKey:       r.correlationKey.value,
		State:                runtimereplycontext.StateOpen,
		CreatedAt:            now.UTC(),
		UpdatedAt:            now.UTC(),
	}
	record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	return record, record.Validate()
}

func (r ConnectRoutePlanReplyResolution) matchesRecord(record runtimereplycontext.Record) bool {
	record = record.Normalized()
	return record.RequesterFlowID == r.requesterFlowID.value &&
		record.RequestOutputPin == r.requestOutputPin.value &&
		record.ReplyInputPin == r.replyInputPin.value &&
		record.ProviderFlowID == r.providerFlowID.value &&
		record.ProviderInputPin == r.providerInputPin.value &&
		record.ProviderOutputPin == r.providerOutputPin.value &&
		record.CorrelationKey == r.correlationKey.value
}

func (r ConnectRoutePlanReplyResolution) responseCorrelation(values ConnectRouteMatchValues) (string, bool) {
	if r.correlationKey.value == "" {
		return "", true
	}
	value := values.value(connectFieldPath{value: "payload." + r.correlationKey.value})
	return value, value != ""
}

type ConnectReplyRole uint8

const (
	ConnectReplyRoleRequest ConnectReplyRole = iota + 1
	ConnectReplyRoleResponse
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
	packageKey                  string
	authoredLocation            string
	source                      ConnectRoutePlanEndpoint
	receiver                    ConnectRoutePlanEndpoint
	adapter                     string
	targetKind                  ConnectRoutePlanTargetKind
	resolutionKind              ConnectRoutePlanResolutionKind
	instanceKey                 *ConnectRoutePlanInstanceKey
	fanIn                       *ConnectRoutePlanFanIn
	replyResolution             *ConnectRoutePlanReplyResolution
	target                      events.RouteIdentity
	targetSet                   []events.RouteIdentity
	providerOutputAuthorization *runtimeprovideroutput.Authorization
}

type ConnectRoutePlanReadback struct {
	PackageKey       string
	AuthoredLocation string
	Adapter          string
	Source           ConnectRoutePlanEndpointReadback
	Receiver         ConnectRoutePlanEndpointReadback
	Targets          []events.RouteIdentity
}

func (p ConnectRoutePlan) Readback() ConnectRoutePlanReadback {
	targets := append([]events.RouteIdentity(nil), p.targetSet...)
	if !p.target.Empty() {
		targets = append([]events.RouteIdentity{p.target}, targets...)
	}
	return ConnectRoutePlanReadback{
		PackageKey: p.packageKey, AuthoredLocation: p.authoredLocation, Adapter: p.adapter,
		Source: p.source.Readback(), Receiver: p.receiver.Readback(), Targets: targets,
	}
}

func (p ConnectRoutePlan) SourceEndpoint() ConnectRoutePlanEndpoint       { return p.source }
func (p ConnectRoutePlan) ReceiverEndpoint() ConnectRoutePlanEndpoint     { return p.receiver }
func (p ConnectRoutePlan) TargetKind() ConnectRoutePlanTargetKind         { return p.targetKind }
func (p ConnectRoutePlan) ResolutionKind() ConnectRoutePlanResolutionKind { return p.resolutionKind }
func (p ConnectRoutePlan) InstanceKey() *ConnectRoutePlanInstanceKey      { return p.instanceKey }
func (p ConnectRoutePlan) FanIn() *ConnectRoutePlanFanIn                  { return p.fanIn }
func (p ConnectRoutePlan) ReplyResolution() *ConnectRoutePlanReplyResolution {
	return p.replyResolution
}
func (p ConnectRoutePlan) ProviderOutputAuthorization() *runtimeprovideroutput.Authorization {
	if p.providerOutputAuthorization == nil {
		return nil
	}
	authorization := *p.providerOutputAuthorization
	return &authorization
}

func (p ConnectRoutePlan) RequiresRuntimeResolution() bool {
	return p.receiver.IsRoot() || p.resolutionKind != ConnectResolutionStatic
}

// StructuralTargetOwnerEligible reports whether compiled topology proves that
// a static receiver is nested under the currently executing delivery owner.
// The delivery target supplies entity authority; endpoint paths only prove the
// structural relationship.
func (p ConnectRoutePlan) StructuralTargetOwnerEligible() bool {
	if p.fanIn != nil || p.receiver.kind != connectEndpointStaticFlow {
		return false
	}
	receiverPath := strings.Trim(p.receiver.flowPath.value, "/")
	if receiverPath == "" {
		return false
	}
	if p.source.IsRoot() {
		return true
	}
	sourcePath := strings.Trim(p.source.flowPath.value, "/")
	return sourcePath != "" && receiverPath != sourcePath && strings.HasPrefix(receiverPath, sourcePath+"/")
}

type ConnectRoutePlanIssue struct {
	Connect          runtimecontracts.FlowPackageConnect
	AuthoredLocation string
	Failure          ConnectRoutePlanFailure
	Detail           string

	sourceEndpoint              ConnectRoutePlanEndpoint
	receiverEndpoint            ConnectRoutePlanEndpoint
	providerOutputAuthorization *runtimeprovideroutput.Authorization
}

func (i ConnectRoutePlanIssue) ProviderOutputAuthorization() *runtimeprovideroutput.Authorization {
	if i.providerOutputAuthorization == nil {
		return nil
	}
	authorization := *i.providerOutputAuthorization
	return &authorization
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

func admitConnectRoutePlanIssueEndpoints(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, issue ConnectRoutePlanIssue) ConnectRoutePlanIssue {
	if source == nil {
		return issue
	}
	issue.Connect = connect
	if from, err := connect.FromRef(); err == nil {
		if from.Root {
			if flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey); ok {
				from.FlowID, from.Root = flowID, flowID == ""
			}
		}
		if endpoint, _, endpointIssue := connectRoutePlanSourceEndpoint(source, from, connect); endpointIssue.Failure.Empty() {
			issue.sourceEndpoint = endpoint
		}
	}
	if to, err := connect.ToRef(); err == nil {
		if to.Root {
			if flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey); ok {
				to.FlowID, to.Root = flowID, flowID == ""
			}
		}
		if endpoint, ok := connectRoutePlanReceiverEndpointRole(source, to); ok {
			issue.receiverEndpoint = endpoint
		}
	}
	return issue
}

type ConnectRoutePlanMaterializationInput struct {
	MatchValues ConnectRouteMatchValues
	Descriptors []Descriptor
}

// ConnectRouteMatchValues is the admitted schema-value snapshot consumed by a
// compiled plan. Callers cannot mutate or reinterpret its keys after admission.
type ConnectRouteMatchValues struct {
	values map[string]string
}

func AdmitConnectRouteMatchValues(values map[string]string) ConnectRouteMatchValues {
	admitted := make(map[string]string, len(values))
	for key, value := range values {
		if key == "" || key != strings.TrimSpace(key) {
			continue
		}
		admitted[key] = value
	}
	return ConnectRouteMatchValues{values: admitted}
}

func (v ConnectRouteMatchValues) value(path connectFieldPath) string {
	if path.value == "" {
		return ""
	}
	return strings.TrimSpace(v.values[path.value])
}

type ConnectRoutePlanMaterialization struct {
	Target    events.RouteIdentity
	TargetSet []events.RouteIdentity
	Failure   ConnectRoutePlanFailure
}

type ConnectRoutePlanInstanceKeyMaterial struct {
	values map[string]any
	Keys   []runtimecontracts.TemplateInstanceKeyValue
}

func (m ConnectRoutePlanInstanceKeyMaterial) CanonicalValues() map[string]any {
	out := make(map[string]any, len(m.values))
	for key, value := range m.values {
		out[key] = value
	}
	return out
}

// CompiledConnectGraph is the immutable owner of all admitted connect rows.
// Authored rows are retained only inside this package while downstream
// consumers receive compiled plans or typed projections.
type CompiledConnectGraph struct {
	source                semanticview.Source
	plans                 []ConnectRoutePlan
	issues                []ConnectRoutePlanIssue
	receiverPinCollisions []ConnectReceiverPinCollision
}

const ConnectReceiverPinCollisionFailure = "connect_receiver_pin_delivery_collision"

type connectSourceEndpointIdentity struct {
	kind   connectEndpointKind
	flowID connectFlowID
	event  connectResolvedEvent
}

type connectReceiverPinAdmissionKey struct {
	source    connectSourceEndpointIdentity
	recipient events.DeliveryRouteIdentity
}

type connectReceiverPinAdmissionGroup struct {
	sourceDiagnostic string
	route            events.DeliveryRoute
	authoredLocation string
	pins             map[[sha256.Size]byte]ConnectReceiverPinIdentity
}

// ConnectReceiverPinAdmission is the shared owner for the invariant that one
// source event and durable recipient cannot select multiple receiver pins.
// Both graph compilation and live delivery planning admit evidence here.
type ConnectReceiverPinAdmission struct {
	groups map[connectReceiverPinAdmissionKey]*connectReceiverPinAdmissionGroup
}

// ConnectReceiverPinCollision is an immutable diagnostic projection from the
// typed admission owner. Its display values cannot be admitted back into the
// evaluator.
type ConnectReceiverPinCollision struct {
	sourceDiagnostic string
	route            events.DeliveryRoute
	authoredLocation string
	receiverPins     []ConnectReceiverPinIdentity
}

func (c ConnectReceiverPinCollision) SourceDiagnostic() string { return c.sourceDiagnostic }
func (c ConnectReceiverPinCollision) SubscriberType() string   { return c.route.Recipient.Code() }
func (c ConnectReceiverPinCollision) SubscriberID() string     { return c.route.Recipient.ID() }
func (c ConnectReceiverPinCollision) Target() events.RouteIdentity {
	return c.route.Target.Normalized()
}
func (c ConnectReceiverPinCollision) AuthoredLocation() string { return c.authoredLocation }
func (c ConnectReceiverPinCollision) ReceiverPinDiagnostics() []string {
	out := make([]string, 0, len(c.receiverPins))
	for _, pin := range c.receiverPins {
		out = append(out, pin.Diagnostic())
	}
	sort.Strings(out)
	return out
}

func (c ConnectReceiverPinCollision) Message() string {
	target := c.Target().FlowInstance
	if target == "" {
		target = "<global-root>"
	}
	return fmt.Sprintf(
		"source event %s reaches %s %s at target %s through multiple receiver pins: %s",
		c.SourceDiagnostic(),
		c.SubscriberType(),
		c.SubscriberID(),
		target,
		strings.Join(c.ReceiverPinDiagnostics(), ", "),
	)
}

func (a *ConnectReceiverPinAdmission) Admit(plan ConnectRoutePlan, routes []events.DeliveryRoute) error {
	if a == nil {
		return fmt.Errorf("connect receiver-pin admission owner is required")
	}
	identity := plan.ReceiverPinIdentity()
	if identity.Empty() {
		return fmt.Errorf("connect receiver-pin admission requires a compiled receiver pin")
	}
	source, sourceDiagnostic, err := connectSourceEndpointAdmissionIdentity(plan.source)
	if err != nil {
		return err
	}
	if a.groups == nil {
		a.groups = make(map[connectReceiverPinAdmissionKey]*connectReceiverPinAdmissionGroup)
	}
	for _, route := range routes {
		route = route.Normalized()
		route.ConnectClaim = events.ConnectExecutionClaim{}
		recipient, err := route.Identity()
		if err != nil {
			return fmt.Errorf("connect receiver-pin recipient: %w", err)
		}
		key := connectReceiverPinAdmissionKey{source: source, recipient: recipient}
		group := a.groups[key]
		if group == nil {
			group = &connectReceiverPinAdmissionGroup{
				sourceDiagnostic: sourceDiagnostic,
				route:            route,
				authoredLocation: plan.authoredLocation,
				pins:             make(map[[sha256.Size]byte]ConnectReceiverPinIdentity),
			}
			a.groups[key] = group
		}
		group.pins[identity.digest] = identity
	}
	return nil
}

func (a ConnectReceiverPinAdmission) Collisions() []ConnectReceiverPinCollision {
	out := make([]ConnectReceiverPinCollision, 0)
	for _, group := range a.groups {
		if len(group.pins) < 2 {
			continue
		}
		collision := ConnectReceiverPinCollision{
			sourceDiagnostic: group.sourceDiagnostic,
			route:            group.route,
			authoredLocation: group.authoredLocation,
			receiverPins:     make([]ConnectReceiverPinIdentity, 0, len(group.pins)),
		}
		for _, pin := range group.pins {
			collision.receiverPins = append(collision.receiverPins, pin)
		}
		out = append(out, collision)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, _ := out[i].route.Identity()
		right, _ := out[j].route.Identity()
		if out[i].sourceDiagnostic != out[j].sourceDiagnostic {
			return out[i].sourceDiagnostic < out[j].sourceDiagnostic
		}
		return events.EncodeDeliveryRouteIdentity(left) < events.EncodeDeliveryRouteIdentity(right)
	})
	return out
}

func connectSourceEndpointAdmissionIdentity(endpoint ConnectRoutePlanEndpoint) (connectSourceEndpointIdentity, string, error) {
	readback := endpoint.Readback()
	diagnostic := readback.ResolvedEvent
	if diagnostic == "" {
		diagnostic = readback.LocalEvent
	}
	if diagnostic == "" || endpoint.kind == 0 {
		return connectSourceEndpointIdentity{}, "", fmt.Errorf("connect receiver-pin admission requires a compiled source event")
	}
	resolved := endpoint.resolvedEvent
	if resolved.value == "" {
		resolved = connectResolvedEvent{value: endpoint.event.value}
	}
	return connectSourceEndpointIdentity{kind: endpoint.kind, flowID: endpoint.flowID, event: resolved}, diagnostic, nil
}

type ConnectEndpointRoleKind uint8

const (
	ConnectEndpointRoleProducer ConnectEndpointRoleKind = iota + 1
	ConnectEndpointRoleConsumer
)

type ConnectEndpointRole struct {
	kind     ConnectEndpointRoleKind
	endpoint ConnectRoutePlanEndpoint
}

func (r ConnectEndpointRole) Kind() ConnectEndpointRoleKind { return r.kind }
func (r ConnectEndpointRole) Readback() ConnectRoutePlanEndpointReadback {
	return r.endpoint.Readback()
}

func (r ConnectEndpointRole) Matches(flowID string, eventType events.EventType) bool {
	if r.endpoint.IsRoot() != (flowID == "") || r.endpoint.flowID.value != flowID {
		return false
	}
	if eventType == "" {
		return false
	}
	if eventType == r.endpoint.event.value || eventType == r.endpoint.resolvedEvent.value {
		return true
	}
	scope := r.endpoint.flowPath.value
	return scope != "" && eventType == events.EventType(scope+"/"+string(r.endpoint.event.value))
}

func (r ConnectEndpointRole) BelongsToFlow(flowID string) bool {
	return r.endpoint.IsRoot() == (flowID == "") && r.endpoint.flowID.value == flowID
}

func (r ConnectEndpointRole) MatchesAuthoredEndpoint(endpoint semanticview.AuthoredEventEndpoint) bool {
	if r.endpoint.flowID.value != endpoint.FlowID {
		return false
	}
	authored := events.EventType(endpoint.Event.Authored)
	canonical := events.EventType(endpoint.Event.Canonical)
	return r.Matches(endpoint.FlowID, authored) || r.Matches(endpoint.FlowID, canonical)
}

func (r ConnectEndpointRole) MatchesInputPin(flowID string, pin runtimecontracts.FlowInputEventPin) bool {
	return r.kind == ConnectEndpointRoleConsumer && r.endpoint.matchesFlowPin(flowID, pin.PinName(), ConnectEndpointRoleConsumer)
}

func (r ConnectEndpointRole) MatchesOutputPin(flowID string, pin runtimecontracts.FlowOutputEventPin) bool {
	return r.kind == ConnectEndpointRoleProducer && r.endpoint.matchesFlowPin(flowID, pin.PinName(), ConnectEndpointRoleProducer)
}

type ConnectEdgeEvidence struct {
	producer   ConnectEndpointRole
	consumer   ConnectEndpointRole
	resolution runtimecontracts.FlowInputResolutionMode
}

func (g CompiledConnectGraph) AdmitReceiverRecipient(flowID string, eventType events.EventType, recipient ConnectRecipient) []ConnectRecipientRegistration {
	flowID = strings.TrimSpace(flowID)
	eventType = events.EventType(eventidentity.Normalize(string(eventType)))
	if eventType == "" || recipient.id == "" || recipient.kind == 0 {
		return nil
	}
	seen := map[ConnectReceiverPinIdentity]struct{}{}
	out := make([]ConnectRecipientRegistration, 0, 1)
	for _, plan := range g.plans {
		role := connectEndpointRole(ConnectEndpointRoleConsumer, plan.receiver)
		if !role.Matches(flowID, eventType) {
			continue
		}
		pin := plan.ReceiverPinIdentity()
		if pin.Empty() {
			continue
		}
		if _, exists := seen[pin]; exists {
			continue
		}
		seen[pin] = struct{}{}
		out = append(out, ConnectRecipientRegistration{receiverPin: pin, recipient: recipient})
	}
	return out
}

func (g CompiledConnectGraph) EvaluateMaterializedRecipients(plan ConnectRoutePlan, targets []events.RouteIdentity, registrations []ConnectRecipientRegistration) ConnectRecipientEvaluation {
	evaluation := ConnectRecipientEvaluation{matched: true}
	planID, err := ConnectPlanIdentity(plan)
	if err != nil {
		evaluation.err = err
		return evaluation
	}
	if plan.RequiresRuntimeResolution() && len(targets) == 0 {
		evaluation.requiresRuntimeResolution = true
		entry, entryErr := events.NewConnectPlanEvaluation(planID, events.ConnectPlanRuntimeResolutionRequired, nil, nil)
		if entryErr != nil {
			evaluation.err = entryErr
			return evaluation
		}
		evaluation.plans = append(evaluation.plans, entry)
		return evaluation
	}
	if len(targets) == 0 {
		targets = []events.RouteIdentity{{}}
	}
	recipients, candidates, err := evaluateConnectPlanRecipients(plan, targets, registrations)
	if err != nil {
		evaluation.err = err
		return evaluation
	}
	evaluation.recipients = append(evaluation.recipients, recipients...)
	resolution := events.ConnectPlanResolved
	if len(registrations) == 0 {
		resolution = events.ConnectPlanNoRegistration
	}
	entry, entryErr := events.NewConnectPlanEvaluation(planID, resolution, targets, candidates)
	if entryErr != nil {
		evaluation.err = entryErr
		return evaluation
	}
	evaluation.plans = append(evaluation.plans, entry)
	evaluation.recipients = normalizeConnectRecipients(evaluation.recipients)
	return evaluation
}

func (g CompiledConnectGraph) EvaluateSourceRecipients(sourceEvent SourceEvent, registrations []ConnectRecipientRegistration) ConnectRecipientEvaluation {
	evaluation := ConnectRecipientEvaluation{}
	for _, plan := range g.MatchingSourceEvent(sourceEvent) {
		evaluation.matched = true
		if plan.RequiresRuntimeResolution() {
			part := g.EvaluateMaterializedRecipients(plan, nil, registrations)
			evaluation.requiresRuntimeResolution = true
			evaluation.plans = append(evaluation.plans, part.plans...)
			if part.err != nil {
				evaluation.err = part.err
				return evaluation
			}
			continue
		}
		targets := append([]events.RouteIdentity(nil), plan.targetSet...)
		if !plan.target.Empty() {
			targets = append([]events.RouteIdentity{plan.target}, targets...)
		}
		part := g.EvaluateMaterializedRecipients(plan, targets, registrations)
		evaluation.recipients = append(evaluation.recipients, part.recipients...)
		evaluation.plans = append(evaluation.plans, part.plans...)
		if part.err != nil {
			evaluation.err = part.err
			return evaluation
		}
	}
	evaluation.recipients = normalizeConnectRecipients(evaluation.recipients)
	return evaluation
}

func evaluateConnectPlanRecipients(plan ConnectRoutePlan, targets []events.RouteIdentity, registrations []ConnectRecipientRegistration) ([]ConnectRecipient, []events.ConnectCandidateEvidence, error) {
	pin := plan.ReceiverPinIdentity()
	if pin.Empty() {
		return nil, nil, fmt.Errorf("connect plan receiver pin identity is required")
	}
	out := make([]ConnectRecipient, 0, len(registrations))
	evidence := make([]events.ConnectCandidateEvidence, 0, len(registrations))
	for _, registration := range registrations {
		outcome := events.ConnectCandidatePinMismatch
		matchedTarget := false
		if registration.receiverPin.Equal(pin) {
			outcome = events.ConnectCandidatePathMismatch
			for _, target := range targets {
				if connectRecipientMatchesTarget(plan, registration.recipient, target.Normalized()) {
					matchedTarget = true
					outcome = events.ConnectCandidateAccepted
					break
				}
			}
		}
		recipient := registration.recipient
		if matchedTarget {
			recipient.handlerEvent = plan.ReceiverLocalEvent()
			out = append(out, recipient)
		}
		deliveryRecipient, err := connectEvidenceRecipient(recipient)
		if err != nil {
			return nil, nil, err
		}
		candidate, err := events.NewConnectCandidateEvidence(registration.receiverPin.EvidenceIdentity(), deliveryRecipient, recipient.path, recipient.agentIdentity, outcome)
		if err != nil {
			return nil, nil, err
		}
		evidence = append(evidence, candidate)
	}
	return normalizeConnectRecipients(out), evidence, nil
}

func connectEvidenceRecipient(recipient ConnectRecipient) (events.DeliveryRecipient, error) {
	if recipient.kind == ConnectRecipientAgent {
		return events.NewAgentDeliveryRecipient(recipient.id)
	}
	return events.NewNodeDeliveryRecipient(recipient.id)
}

func connectRecipientMatchesTarget(plan ConnectRoutePlan, recipient ConnectRecipient, target events.RouteIdentity) bool {
	if recipient.id == "" || recipient.kind == 0 {
		return false
	}
	if target.Empty() {
		return true
	}
	if recipient.path == "" {
		return plan.receiver.IsRoot() && target.FlowInstance == "" && target.FlowID == "" && target.EntityID != ""
	}
	if target.FlowInstance == recipient.path {
		return true
	}
	return plan.fanIn != nil && plan.receiver.subscriberPathMatchesReceiver(recipient.path, target)
}

type connectRecipientIdentity struct {
	kind          ConnectRecipientKind
	id            string
	path          string
	agentIdentity agentidentity.Identity
	handlerEvent  events.EventType
}

func normalizeConnectRecipients(in []ConnectRecipient) []ConnectRecipient {
	if len(in) == 0 {
		return nil
	}
	seen := map[connectRecipientIdentity]struct{}{}
	out := make([]ConnectRecipient, 0, len(in))
	for _, recipient := range in {
		key := connectRecipientIdentity{
			kind: recipient.kind, id: recipient.id, path: recipient.path,
			agentIdentity: recipient.agentIdentity, handlerEvent: recipient.handlerEvent,
		}
		if recipient.kind == 0 || recipient.id == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	return out
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
		case plan.instanceKey != nil:
			mode = plan.instanceKey.mode
		case plan.fanIn != nil:
			mode = runtimecontracts.FlowInputResolutionModeFanIn
		case plan.replyResolution != nil:
			mode = runtimecontracts.FlowInputResolutionModeReply
		}
		edges = append(edges, ConnectEdgeEvidence{
			producer:   connectEndpointRole(ConnectEndpointRoleProducer, plan.source),
			consumer:   connectEndpointRole(ConnectEndpointRoleConsumer, plan.receiver),
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

func (g CompiledConnectGraph) PlansFromOutputPin(flowID string, pin runtimecontracts.FlowOutputEventPin) []ConnectRoutePlan {
	out := make([]ConnectRoutePlan, 0, 1)
	for _, plan := range g.plans {
		if plan.source.matchesFlowPin(flowID, pin.PinName(), ConnectEndpointRoleProducer) {
			out = append(out, plan)
		}
	}
	return out
}

func (g CompiledConnectGraph) PlansToInputPin(flowID string, pin runtimecontracts.FlowInputEventPin) []ConnectRoutePlan {
	out := make([]ConnectRoutePlan, 0, 1)
	for _, plan := range g.plans {
		if plan.receiver.matchesFlowPin(flowID, pin.PinName(), ConnectEndpointRoleConsumer) {
			out = append(out, plan)
		}
	}
	return out
}

func ConnectEndpointsShareFlow(left, right ConnectRoutePlanEndpoint) bool {
	return left.kind == right.kind && left.flowID == right.flowID
}

func connectEndpointRole(kind ConnectEndpointRoleKind, endpoint ConnectRoutePlanEndpoint) ConnectEndpointRole {
	return ConnectEndpointRole{kind: kind, endpoint: endpoint}
}

func appendUniqueConnectEndpointRole(roles []ConnectEndpointRole, kind ConnectEndpointRoleKind, endpoint ConnectRoutePlanEndpoint) []ConnectEndpointRole {
	candidate := ConnectEndpointRole{kind: kind, endpoint: endpoint}
	for _, existing := range roles {
		if existing.kind == candidate.kind && existing.endpoint.kind == candidate.endpoint.kind &&
			existing.endpoint.flowID == candidate.endpoint.flowID && existing.endpoint.pin == candidate.endpoint.pin &&
			existing.endpoint.event == candidate.endpoint.event && existing.endpoint.resolvedEvent == candidate.endpoint.resolvedEvent {
			return roles
		}
	}
	return append(roles, candidate)
}

type SourceEvent struct {
	eventType events.EventType
	kind      events.RoutingSourceKind
	route     events.RouteIdentity
}

func AdmitSourceEvent(eventType events.EventType, source events.RoutingSource) (SourceEvent, error) {
	canonical := eventidentity.Normalize(string(eventType))
	if canonical == "" || canonical != string(eventType) {
		return SourceEvent{}, fmt.Errorf("connect source event identity is not canonical")
	}
	if source.Empty() {
		return SourceEvent{}, fmt.Errorf("connect source event requires explicit routing provenance")
	}
	return SourceEvent{eventType: eventType, kind: source.Kind(), route: source.Route()}, nil
}

// AdmitRuntimeControlSourceEvent resolves one authored producer event against
// its semantic flow and binds that result to the already-admitted source fact.
// Persistence and firing copy the returned event identity unchanged.
func AdmitRuntimeControlSourceEvent(source semanticview.Source, flowID string, eventType events.EventType, routingSource events.RoutingSource) (events.EventType, error) {
	if source == nil {
		return "", fmt.Errorf("runtime-control source event requires semantic source")
	}
	flowID = strings.TrimSpace(flowID)
	switch routingSource.Kind() {
	case events.RoutingSourceRoot:
		if flowID != "" {
			return "", fmt.Errorf("root runtime-control source event cannot claim flow %q", flowID)
		}
	case events.RoutingSourceFlowOwnedControl:
		if flowID == "" || routingSource.Route().FlowID != flowID {
			return "", fmt.Errorf("flow-owned runtime-control source event requires its exact declared flow")
		}
	default:
		return "", fmt.Errorf("runtime-control source event requires root or flow-owned provenance")
	}
	authored := eventidentity.Normalize(string(eventType))
	if authored == "" || authored != string(eventType) {
		return "", fmt.Errorf("runtime-control authored event identity is not canonical")
	}
	resolved := events.EventType(eventidentity.Normalize(source.ResolveFlowEventReference(flowID, authored)))
	if routingSource.Kind() == events.RoutingSourceFlowOwnedControl {
		scope := eventidentity.Normalize(source.FlowPath(flowID))
		if scope != "" && !strings.HasPrefix(string(resolved), "platform.") &&
			string(resolved) != scope && !strings.HasPrefix(string(resolved), scope+"/") {
			return "", fmt.Errorf("runtime-control event %q is outside declared flow path %q", resolved, scope)
		}
	}
	admitted, err := events.AdmitRuntimeControlEventType(resolved, routingSource)
	if err != nil {
		return "", err
	}
	if _, err := AdmitSourceEvent(admitted, routingSource); err != nil {
		return "", err
	}
	return admitted, nil
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
		if !issue.Failure.Empty() {
			issue = admitConnectRoutePlanIssueEndpoints(source, connect, issue)
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
	return CompiledConnectGraph{
		source:                source,
		plans:                 plans,
		issues:                issues,
		receiverPinCollisions: compileStaticConnectReceiverPinCollisions(source, plans),
	}
}

func compileStaticConnectReceiverPinCollisions(source semanticview.Source, plans []ConnectRoutePlan) []ConnectReceiverPinCollision {
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	var admission ConnectReceiverPinAdmission
	for _, plan := range plans {
		if plan.RequiresRuntimeResolution() {
			continue
		}
		var input *semanticview.AuthoredEventEndpoint
		for _, candidate := range census.InputPins() {
			if plan.receiver.matchesFlowPin(candidate.FlowID, candidate.PinName, ConnectEndpointRoleConsumer) {
				matched := candidate
				input = &matched
				break
			}
		}
		if input == nil {
			continue
		}
		matches, _ := census.ResolveTypedPubSubConsumerMatches(*input)
		targets := append([]events.RouteIdentity(nil), plan.targetSet...)
		if !plan.target.Empty() || len(targets) == 0 {
			targets = append([]events.RouteIdentity{plan.target}, targets...)
		}
		for _, target := range targets {
			routes := make([]events.DeliveryRoute, 0, len(matches))
			for _, match := range matches {
				if route, ok := staticConnectReceiverDeliveryRoute(source, match.Consumer, target); ok {
					routes = append(routes, route)
				}
			}
			if err := admission.Admit(plan, routes); err != nil {
				continue
			}
		}
	}
	return admission.Collisions()
}

func staticConnectReceiverDeliveryRoute(source semanticview.Source, endpoint semanticview.AuthoredEventEndpoint, target events.RouteIdentity) (events.DeliveryRoute, bool) {
	target = target.Normalized()
	switch endpoint.Kind {
	case semanticview.EventEndpointNodeHandler:
		nodeID := strings.TrimSpace(endpoint.NodeID)
		if nodeID == "" {
			return events.DeliveryRoute{}, false
		}
		return events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(nodeID), Target: target}, true
	case semanticview.EventEndpointAgent:
		logicalID := strings.TrimSpace(endpoint.AgentID)
		agentID, owner, ok := staticConnectReceiverAgent(source, endpoint.FlowID, logicalID)
		if !ok {
			return events.DeliveryRoute{}, false
		}
		name, err := agentidentity.DeclaredName(agentID, owner)
		if err != nil {
			return events.DeliveryRoute{}, false
		}
		route := agentidentity.RootRoute()
		if target.FlowInstance != "" {
			route, err = runtimeflowidentity.StoredRoute("", "", target.FlowInstance).AgentIdentityRoute()
			if err != nil {
				return events.DeliveryRoute{}, false
			}
		}
		identity, err := agentidentity.New(name, route)
		if err != nil {
			return events.DeliveryRoute{}, false
		}
		return events.DeliveryRoute{
			Recipient:     events.MustAgentDeliveryRecipient(agentID),
			AgentIdentity: identity,
			Target:        target,
		}, true
	default:
		return events.DeliveryRoute{}, false
	}
}

func staticConnectReceiverAgent(source semanticview.Source, flowID, logicalID string) (string, string, bool) {
	owner, ok := semanticview.AgentDeclarationOwner(source, flowID, logicalID)
	if !ok {
		return "", "", false
	}
	agentID := logicalID
	if scope, found := source.FlowScopeByID(strings.TrimSpace(flowID)); found {
		if entry, exists := scope.Agents[logicalID]; exists && strings.TrimSpace(entry.ID) != "" {
			agentID = strings.TrimSpace(entry.ID)
		}
	} else if entry, exists := source.AgentEntries()[logicalID]; exists && strings.TrimSpace(entry.ID) != "" {
		agentID = strings.TrimSpace(entry.ID)
	}
	return agentID, owner, agentID != ""
}

func (g CompiledConnectGraph) MatchingPlans(evt events.Event) []ConnectRoutePlan {
	sourceEvent, err := SourceEventFromEvent(evt)
	if err != nil {
		return nil
	}
	return g.MatchingSourceEvent(sourceEvent)
}

func (g CompiledConnectGraph) PlanMatchesEvent(plan ConnectRoutePlan, evt events.Event) bool {
	sourceEvent, err := SourceEventFromEvent(evt)
	return err == nil && connectSourceEndpointMatches(plan.source, sourceEvent)
}

func (g CompiledConnectGraph) MatchingSourceEvent(sourceEvent SourceEvent) []ConnectRoutePlan {
	out := make([]ConnectRoutePlan, 0)
	for _, plan := range g.plans {
		if connectSourceEndpointMatches(plan.source, sourceEvent) {
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

func (g CompiledConnectGraph) ReceiverPinCollisions() []ConnectReceiverPinCollision {
	return append([]ConnectReceiverPinCollision(nil), g.receiverPinCollisions...)
}

func (g CompiledConnectGraph) EndpointRoles() []ConnectEndpointRole {
	roles := make([]ConnectEndpointRole, 0, 2*(len(g.plans)+len(g.issues)))
	appendRole := func(kind ConnectEndpointRoleKind, root bool, flowID, pin string, localEvent, event events.EventType) {
		flowID = strings.TrimSpace(flowID)
		pin = strings.TrimSpace(pin)
		if pin == "" {
			return
		}
		roles = appendUniqueConnectEndpointRole(roles, kind, newConnectRoutePlanEndpoint(kind, root, flowID, flowID, "", pin, string(localEvent), string(event), "", nil))
	}
	for _, plan := range g.plans {
		appendRole(ConnectEndpointRoleProducer, plan.source.IsRoot(), plan.source.flowID.value, plan.source.pin.value, plan.source.event.value, plan.source.resolvedEvent.value)
		appendRole(ConnectEndpointRoleConsumer, plan.receiver.IsRoot(), plan.receiver.flowID.value, plan.receiver.pin.value, plan.receiver.event.value, plan.receiver.resolvedEvent.value)
	}
	for _, issue := range g.issues {
		if issue.providerOutputAuthorization != nil || g.source == nil {
			continue
		}
		if !issue.sourceEndpoint.Empty() {
			roles = appendUniqueConnectEndpointRole(roles, ConnectEndpointRoleProducer, issue.sourceEndpoint)
		}
		if !issue.receiverEndpoint.Empty() {
			roles = appendUniqueConnectEndpointRole(roles, ConnectEndpointRoleConsumer, issue.receiverEndpoint)
		}
	}
	sort.SliceStable(roles, func(i, j int) bool {
		if roles[i].kind != roles[j].kind {
			return roles[i].kind < roles[j].kind
		}
		if roles[i].endpoint.flowID != roles[j].endpoint.flowID {
			return roles[i].endpoint.flowID.value < roles[j].endpoint.flowID.value
		}
		if roles[i].endpoint.pin != roles[j].endpoint.pin {
			return roles[i].endpoint.pin.value < roles[j].endpoint.pin.value
		}
		return roles[i].endpoint.resolvedEvent.value < roles[j].endpoint.resolvedEvent.value
	})
	return roles
}

func (g CompiledConnectGraph) IssueMatchesEvent(issue ConnectRoutePlanIssue, evt events.Event) bool {
	sourceEvent, err := SourceEventFromEvent(evt)
	if err != nil || issue.sourceEndpoint.Empty() {
		return false
	}
	return connectSourceEndpointMatches(issue.sourceEndpoint, sourceEvent)
}

func sortConnectRoutePlans(plans []ConnectRoutePlan) {
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].source.flowID != plans[j].source.flowID {
			return plans[i].source.flowID.value < plans[j].source.flowID.value
		}
		if plans[i].source.pin != plans[j].source.pin {
			return plans[i].source.pin.value < plans[j].source.pin.value
		}
		if plans[i].receiver.flowID != plans[j].receiver.flowID {
			return plans[i].receiver.flowID.value < plans[j].receiver.flowID.value
		}
		return plans[i].receiver.pin.value < plans[j].receiver.pin.value
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
			sourceEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "external", inputPin.PinName(), resolved, resolved, "", nil)
			receiverEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, flowID, scope.Path, scope.Mode,
				inputPin.PinName(), inputPin.EventType(), resolved, "", nil)
			var instanceKey *ConnectRoutePlanInstanceKey
			if receiverRequiresRuntimeResolution(scope) {
				var issue ConnectRoutePlanIssue
				instanceKey, issue = connectResolutionInstanceKey(source, connect, inputPin, inputPin.Resolution, flowID)
				if !issue.Failure.Empty() {
					issue.AuthoredLocation = flowID + "." + inputPin.PinName()
					issue.sourceEndpoint = sourceEndpoint
					issue.receiverEndpoint = receiverEndpoint
					issue.providerOutputAuthorization = &authorization
					issues = append(issues, issue)
					continue
				}
			}
			plan := ConnectRoutePlan{
				authoredLocation:            flowID + "." + inputPin.PinName(),
				source:                      sourceEndpoint,
				receiver:                    receiverEndpoint,
				targetKind:                  ConnectTargetKindTarget,
				resolutionKind:              connectResolutionKind(scope, instanceKey),
				instanceKey:                 instanceKey,
				providerOutputAuthorization: &authorization,
			}
			if receiverRequiresRuntimeResolution(scope) {
				if instanceKey == nil {
					issues = append(issues, ConnectRoutePlanIssue{
						Connect: connect, AuthoredLocation: plan.authoredLocation,
						Failure: ConnectFailureReceiverResolutionMissing, Detail: flowID,
						sourceEndpoint: sourceEndpoint, receiverEndpoint: receiverEndpoint,
						providerOutputAuthorization: &authorization,
					})
					continue
				}
			} else {
				plan.target = staticConnectRoute(source, flowID)
			}
			plans = append(plans, plan)
		}
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].receiver.flowID != plans[j].receiver.flowID {
			return plans[i].receiver.flowID.value < plans[j].receiver.flowID.value
		}
		return plans[i].receiver.pin.value < plans[j].receiver.pin.value
	})
	return plans, issues
}

func lowerCompositionConnectRoutePlanWithLocation(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
	plan, issue := lowerCompositionConnectRoutePlan(source, connect)
	authoredLocation := connect.AuthoredLocation()
	plan.authoredLocation = authoredLocation
	issue.AuthoredLocation = authoredLocation
	if issue.Failure.Empty() && authoredLocation == "" {
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
	if !sourceIssue.Failure.Empty() {
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
			packageKey:       strings.TrimSpace(connect.PackageKey),
			authoredLocation: connect.AuthoredLocation(),
			source:           sourceEndpoint,
			receiver: newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, true, "", "", "root", to.Pin, inputPin.EventType(),
				source.ResolveFlowEventReference("", inputPin.EventType()), "", nil),
			adapter:        strings.TrimSpace(connect.Adapter),
			targetKind:     ConnectTargetKindTarget,
			resolutionKind: ConnectResolutionStatic,
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
	if !instanceKeyIssue.Failure.Empty() {
		return ConnectRoutePlan{}, instanceKeyIssue
	}
	fanIn, fanInIssue := connectFanIn(source, connect, inputPin, to.FlowID)
	if !fanInIssue.Failure.Empty() {
		return ConnectRoutePlan{}, fanInIssue
	}
	replyResolution, replyIssue := connectReplyResolution(source, connect, sourceEndpoint, to, inputPin)
	if !replyIssue.Failure.Empty() {
		return ConnectRoutePlan{}, replyIssue
	}
	if receiverRequiresRuntimeResolution(receiverScope) && instanceKey == nil && (replyResolution == nil || replyResolution.role != ConnectReplyRoleResponse) {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverResolutionMissing, Detail: to.FlowID}
	}
	plan := ConnectRoutePlan{
		packageKey:       strings.TrimSpace(connect.PackageKey),
		authoredLocation: connect.AuthoredLocation(),
		source:           sourceEndpoint,
		receiver: newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, to.FlowID, receiverScope.Path, receiverScope.Mode,
			to.Pin, inputPin.EventType(), source.ResolveFlowEventReference(to.FlowID, inputPin.EventType()), "", nil),
		adapter:         strings.TrimSpace(connect.Adapter),
		targetKind:      ConnectTargetKindTarget,
		resolutionKind:  connectResolutionKind(receiverScope, instanceKey),
		instanceKey:     instanceKey,
		fanIn:           fanIn,
		replyResolution: replyResolution,
	}
	if replyResolution != nil && replyResolution.role == ConnectReplyRoleResponse {
		plan.resolutionKind = ConnectResolutionReply
		return plan, ConnectRoutePlanIssue{}
	}
	if !receiverRequiresRuntimeResolution(receiverScope) {
		route := staticConnectRoute(source, to.FlowID)
		if fanIn != nil {
			route = fanInSingletonRoute(to.FlowID, fanIn.singleton.value)
		}
		if !route.Empty() {
			plan.target = route
		}
		return plan, ConnectRoutePlanIssue{}
	}
	return plan, ConnectRoutePlanIssue{}
}

func connectEventsCompatible(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, from, to runtimecontracts.FlowPackagePinRef, sourceEndpoint ConnectRoutePlanEndpoint, inputPin runtimecontracts.FlowInputEventPin) bool {
	outputEvent := string(sourceEndpoint.event.value)
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
	wantEvent := string(sourceEndpoint.resolvedEvent.value)
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
		return newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", from.Pin, outputPin.EventType(),
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
	return newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, from.FlowID, sourceScope.Path, sourceScope.Mode, from.Pin,
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
		return newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, true, "", "", "root", to.Pin, inputPin.EventType(),
			source.ResolveFlowEventReference("", inputPin.EventType()), "", nil), true
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return ConnectRoutePlanEndpoint{}, false
	}
	return newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, flowID, scope.Path, scope.Mode, to.Pin, inputPin.EventType(),
		source.ResolveFlowEventReference(flowID, inputPin.EventType()), "", nil), true
}

func MaterializeConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	if plan.receiver.IsRoot() {
		return ConnectRoutePlanMaterialization{}
	}
	if !plan.target.Empty() {
		return ConnectRoutePlanMaterialization{Target: plan.target}
	}
	if len(plan.targetSet) > 0 {
		return ConnectRoutePlanMaterialization{TargetSet: append([]events.RouteIdentity{}, plan.targetSet...)}
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
	if !failure.Empty() {
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
	switch plan.targetKind {
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

func InstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, matchValues ConnectRouteMatchValues) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.instanceKey
	if instanceKey == nil || instanceKey.field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	if instanceKey.RequiresDeliveryProjection() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	if instanceKey.source.kind != connectInstanceSourcePayload {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	sourcePath := instanceKey.source.path.value
	value := matchValues.value(connectFieldPath{value: sourcePath})
	if sourcePath == "" || value == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	values := map[string]any{instanceKey.field.Path(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.receiver.flowID.value,
		Field:  instanceKey.field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, 0
}

func EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, eventID string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.instanceKey
	if instanceKey == nil || instanceKey.field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	value := ""
	switch instanceKey.source.kind {
	case connectInstanceSourceGeneratedUUID:
		value = deterministicResolutionUUID(plan, eventID)
	case connectInstanceSourceEventID:
		value = eventID
	default:
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	values := map[string]any{instanceKey.field.Path(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.receiver.flowID.value,
		Field:  instanceKey.field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, 0
}

func deterministicResolutionUUID(plan ConnectRoutePlan, eventID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strings.TrimSpace(eventID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(plan.receiver.flowID.value))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(plan.receiver.pin.value))
	_, _ = h.Write([]byte{0})
	if plan.instanceKey != nil && !plan.instanceKey.field.Empty() {
		_, _ = h.Write([]byte(plan.instanceKey.field.Path()))
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
		if requestTarget.Root || strings.TrimSpace(requestTarget.FlowID) != sourceEndpoint.flowID.value {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: "resolution mode reply request and reply edges must connect the same provider flow"}
		}
		return &ConnectRoutePlanReplyResolution{
			role:              ConnectReplyRoleResponse,
			requesterFlowID:   connectFlowID{value: strings.TrimSpace(receiverRef.FlowID)},
			requestOutputPin:  connectPinID{direction: ConnectEndpointRoleProducer, value: requestOutputPin},
			replyInputPin:     connectPinID{direction: ConnectEndpointRoleConsumer, value: strings.TrimSpace(receiverRef.Pin)},
			providerFlowID:    sourceEndpoint.flowID,
			providerInputPin:  connectPinID{direction: ConnectEndpointRoleConsumer, value: strings.TrimSpace(requestTarget.Pin)},
			providerOutputPin: sourceEndpoint.pin,
			correlationKey:    connectFieldPath{value: correlationKey},
		}, ConnectRoutePlanIssue{}
	}

	var matches []ConnectRoutePlanReplyResolution
	for _, replyInput := range source.FlowInputEventPins(sourceEndpoint.flowID.value) {
		if replyInput.Resolution.Mode != runtimecontracts.FlowInputResolutionModeReply || strings.TrimSpace(replyInput.Resolution.RepliesTo) != sourceEndpoint.pin.value {
			continue
		}
		for _, replyConnect := range resolvedCompositionConnectsTo(source, sourceEndpoint.flowID.value, replyInput.PinName()) {
			from := replyConnect.from
			if from.Root || strings.TrimSpace(from.FlowID) != strings.TrimSpace(receiverRef.FlowID) {
				continue
			}
			matches = append(matches, ConnectRoutePlanReplyResolution{
				role:              ConnectReplyRoleRequest,
				requesterFlowID:   sourceEndpoint.flowID,
				requestOutputPin:  sourceEndpoint.pin,
				replyInputPin:     connectPinID{direction: ConnectEndpointRoleConsumer, value: strings.TrimSpace(replyInput.PinName())},
				providerFlowID:    connectFlowID{value: strings.TrimSpace(receiverRef.FlowID)},
				providerInputPin:  connectPinID{direction: ConnectEndpointRoleConsumer, value: strings.TrimSpace(receiverRef.Pin)},
				providerOutputPin: connectPinID{direction: ConnectEndpointRoleProducer, value: strings.TrimSpace(from.Pin)},
				correlationKey:    connectFieldPath{value: strings.TrimSpace(replyInput.Resolution.CorrelationKey)},
			})
		}
	}
	if len(matches) > 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("request pin %s.%s participates in multiple reply loops", sourceEndpoint.flowID.value, sourceEndpoint.pin.value)}
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
		aggregation: aggregation,
		window:      connectFieldPath{value: window},
		dedupBy:     connectFieldPaths(dedupBy),
		singleton:   connectFlowPath{value: singleton},
	}, ConnectRoutePlanIssue{}
}

func connectFieldPaths(paths []string) []connectFieldPath {
	out := make([]connectFieldPath, 0, len(paths))
	for _, path := range paths {
		if path = strings.TrimSpace(path); path != "" {
			out = append(out, connectFieldPath{value: path})
		}
	}
	return out
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
		mode:   mode,
		field:  instance.Field,
		source: newConnectInstanceSource(evidence.Source),
	}, ConnectRoutePlanIssue{}
}

func connectResolutionKind(scope semanticview.FlowScope, instanceKey *ConnectRoutePlanInstanceKey) ConnectRoutePlanResolutionKind {
	if !receiverRequiresRuntimeResolution(scope) {
		return ConnectResolutionStatic
	}
	if instanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return 0
}

func connectRoutePlanResolutionKind(plan ConnectRoutePlan) ConnectRoutePlanResolutionKind {
	if !plan.resolutionKind.empty() {
		return plan.resolutionKind
	}
	if !plan.target.Empty() || len(plan.targetSet) > 0 {
		return ConnectResolutionStatic
	}
	if plan.instanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return 0
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
	return descriptorRoute(plan.receiver.flowID.value, descriptor)
}

func descriptorBelongsToReceiver(plan ConnectRoutePlan, descriptor Descriptor) bool {
	flowInstance := strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/")
	if flowInstance == "" {
		return false
	}
	receiverPath := plan.receiver.flowPath.value
	if receiverPath == "" {
		receiverPath = plan.receiver.flowID.value
	}
	return receiverPath != "" && (flowInstance == receiverPath || runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance) == receiverPath)
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
