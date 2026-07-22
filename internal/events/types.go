package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type EventType string

const (
	EventTypePlatformRuntimeLog     EventType = "platform.runtime_log"
	EventTypePlatformInboundRecord  EventType = "platform.inbound_recorded"
	EventTypePlatformAgentDirective EventType = "platform.agent_directive"
)

func DiagnosticDirectEventTypes() []EventType {
	return []EventType{
		EventTypePlatformRuntimeLog,
		EventTypePlatformAgentDirective,
		EventTypePlatformInboundRecord,
	}
}

func IsDiagnosticDirectEventType(eventType EventType) bool {
	eventType = EventType(strings.TrimSpace(string(eventType)))
	for _, candidate := range DiagnosticDirectEventTypes() {
		if eventType == candidate {
			return true
		}
	}
	return false
}

type EventScope string

const (
	EventScopeGlobal EventScope = "global"
	EventScopeFlow   EventScope = "flow"
	EventScopeEntity EventScope = "entity"
)

type EventAdmissionClass string

const (
	EventAdmissionUnknown            EventAdmissionClass = ""
	EventAdmissionRootIngress        EventAdmissionClass = "root_ingress"
	EventAdmissionOperatorInjected   EventAdmissionClass = "operator_injected"
	EventAdmissionRuntimeControl     EventAdmissionClass = "runtime_control"
	EventAdmissionRuntimeDiagnostic  EventAdmissionClass = "runtime_diagnostic"
	EventAdmissionDiagnosticDirect   EventAdmissionClass = "diagnostic_direct"
	EventAdmissionChild              EventAdmissionClass = "child"
	EventAdmissionReplay             EventAdmissionClass = "replay"
	EventAdmissionSelectedForkReplay EventAdmissionClass = "selected_fork_replay"
)

type EventProducerType string

const (
	EventProducerNode     EventProducerType = "node"
	EventProducerAgent    EventProducerType = "agent"
	EventProducerPlatform EventProducerType = "platform"
	EventProducerExternal EventProducerType = "external"
)

func (t EventProducerType) Valid() bool {
	switch EventProducerType(strings.TrimSpace(string(t))) {
	case EventProducerNode, EventProducerAgent, EventProducerPlatform, EventProducerExternal:
		return true
	default:
		return false
	}
}

// ProducerIdentity is the exact semantic identity of an event producer.
// Its fields are private so producer type and ID cannot drift independently.
type ProducerIdentity struct {
	producerType EventProducerType
	id           string
}

// ProducerClaim is the untrusted construction input for ProducerIdentity.
// A successful semantic event constructor validates and consumes this claim.
type ProducerClaim struct {
	Type EventProducerType
	ID   string
}

func NewProducerIdentity(producerType EventProducerType, id string) (ProducerIdentity, error) {
	producer := normalizedProducerIdentity(producerType, id)
	if err := producer.Validate(); err != nil {
		return ProducerIdentity{}, err
	}
	return producer, nil
}

func normalizedProducerIdentity(producerType EventProducerType, id string) ProducerIdentity {
	return ProducerIdentity{
		producerType: EventProducerType(strings.TrimSpace(string(producerType))),
		id:           strings.TrimSpace(id),
	}
}

func (p ProducerIdentity) Type() EventProducerType {
	return EventProducerType(strings.TrimSpace(string(p.producerType)))
}

func (p ProducerIdentity) ID() string {
	return strings.TrimSpace(p.id)
}

func (p ProducerIdentity) Validate() error {
	if !p.Type().Valid() {
		return fmt.Errorf("event producer_type %q is invalid", p.Type())
	}
	if p.ID() == "" {
		return fmt.Errorf("event producer_id is required")
	}
	return nil
}

func (p ProducerIdentity) Equal(other ProducerIdentity) bool {
	return p.Type() == other.Type() && p.ID() == other.ID()
}

type EventEnvelope struct {
	EntityID     string          `json:"-"`
	FlowInstance string          `json:"-"`
	Scope        EventScope      `json:"-"`
	Source       RouteIdentity   `json:"-"`
	Target       RouteIdentity   `json:"-"`
	TargetSet    []RouteIdentity `json:"-"`
}

type RouteIdentity struct {
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityID     string `json:"entity_id,omitempty"`
	FlowID       string `json:"flow_id,omitempty"`
}

type RoutingSourceKind string

const (
	RoutingSourceAbsent          RoutingSourceKind = ""
	RoutingSourceDeclaredIngress RoutingSourceKind = "declared_ingress"
	RoutingSourceRuntimeInstance RoutingSourceKind = "runtime_instance"
)

// RoutingSource is the opaque event-owned source fact. It records exact source
// identity only; connect-policy interpretation belongs to the routing owner.
type RoutingSource struct {
	kind      RoutingSourceKind
	route     RouteIdentity
	authority string
}

func NoRoutingSource() RoutingSource { return RoutingSource{} }

func NewDeclaredIngressRoutingSource(flowID, flowInstance, entityID, authority string) (RoutingSource, error) {
	source := RoutingSource{
		kind:      RoutingSourceDeclaredIngress,
		route:     RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID}.Normalized(),
		authority: strings.TrimSpace(authority),
	}
	if source.route.FlowID == "" || source.route.EntityID == "" || source.authority == "" {
		return RoutingSource{}, fmt.Errorf("declared ingress routing source requires flow_id, entity_id, and resolution authority")
	}
	return source, nil
}

func NewRuntimeRoutingSource(flowID, flowInstance, entityID string) (RoutingSource, error) {
	source := RoutingSource{
		kind:  RoutingSourceRuntimeInstance,
		route: RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID}.Normalized(),
	}
	if source.route.FlowID == "" || source.route.FlowInstance == "" || source.route.EntityID == "" {
		return RoutingSource{}, fmt.Errorf("runtime routing source requires flow_id, flow_instance, and entity_id")
	}
	return source, nil
}

// RuntimeRoutingSourceFromRoute records a runtime source only when the caller
// claims complete flow/instance/entity identity. Flow scope and entity identity
// remain useful event context, but neither is a routing address on its own.
func RuntimeRoutingSourceFromRoute(route RouteIdentity) (RoutingSource, error) {
	route = route.Normalized()
	if route.FlowID == "" || route.EntityID == "" {
		return NoRoutingSource(), nil
	}
	return NewRuntimeRoutingSource(route.FlowID, route.FlowInstance, route.EntityID)
}

func RestoreRoutingSource(kind RoutingSourceKind, route RouteIdentity, authority string) (RoutingSource, error) {
	switch RoutingSourceKind(strings.TrimSpace(string(kind))) {
	case RoutingSourceAbsent:
		if !route.Normalized().Empty() || strings.TrimSpace(authority) != "" {
			return RoutingSource{}, fmt.Errorf("absent routing source cannot carry route or authority")
		}
		return RoutingSource{}, nil
	case RoutingSourceDeclaredIngress:
		return NewDeclaredIngressRoutingSource(route.FlowID, route.FlowInstance, route.EntityID, authority)
	case RoutingSourceRuntimeInstance:
		if strings.TrimSpace(authority) != "" {
			return RoutingSource{}, fmt.Errorf("runtime routing source cannot carry ingress authority")
		}
		return NewRuntimeRoutingSource(route.FlowID, route.FlowInstance, route.EntityID)
	default:
		return RoutingSource{}, fmt.Errorf("routing source kind %q is invalid", kind)
	}
}

func (s RoutingSource) Kind() RoutingSourceKind { return s.kind }
func (s RoutingSource) Route() RouteIdentity    { return s.route.Normalized() }
func (s RoutingSource) Authority() string       { return strings.TrimSpace(s.authority) }
func (s RoutingSource) Empty() bool             { return s.kind == RoutingSourceAbsent }

type OperatorReferenceProvenance struct {
	referencedEventID string
}

func NewOperatorReferenceProvenance(eventID string) (OperatorReferenceProvenance, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return OperatorReferenceProvenance{}, fmt.Errorf("operator referenced event_id is required")
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return OperatorReferenceProvenance{}, fmt.Errorf("operator referenced event_id must be a UUID: %w", err)
	}
	return OperatorReferenceProvenance{referencedEventID: eventID}, nil
}

func (p OperatorReferenceProvenance) ReferencedEventID() string {
	return strings.TrimSpace(p.referencedEventID)
}

type SelectedForkLineage struct {
	destinationRunID string
	sourceRunID      string
	sourceEventID    string
	authorityStamp   string
	taskID           string
	executionMode    executionmode.Mode
}

func NewSelectedForkLineage(destinationRunID, sourceRunID, sourceEventID, authorityStamp, taskID string, mode executionmode.Mode) (SelectedForkLineage, error) {
	lineage := SelectedForkLineage{
		destinationRunID: strings.TrimSpace(destinationRunID),
		sourceRunID:      strings.TrimSpace(sourceRunID),
		sourceEventID:    strings.TrimSpace(sourceEventID),
		authorityStamp:   strings.TrimSpace(authorityStamp),
		taskID:           strings.TrimSpace(taskID),
		executionMode:    mode,
	}
	if lineage.destinationRunID == "" || lineage.sourceRunID == "" || lineage.sourceEventID == "" || lineage.authorityStamp == "" {
		return SelectedForkLineage{}, fmt.Errorf("selected-fork lineage requires destination run, source run, source event, and selection authority")
	}
	if !lineage.executionMode.Valid() {
		return SelectedForkLineage{}, fmt.Errorf("selected-fork lineage requires live or mock execution_mode")
	}
	return lineage, nil
}

func (l SelectedForkLineage) DestinationRunID() string          { return l.destinationRunID }
func (l SelectedForkLineage) SourceRunID() string               { return l.sourceRunID }
func (l SelectedForkLineage) SourceEventID() string             { return l.sourceEventID }
func (l SelectedForkLineage) AuthorityStamp() string            { return l.authorityStamp }
func (l SelectedForkLineage) TaskID() string                    { return l.taskID }
func (l SelectedForkLineage) ExecutionMode() executionmode.Mode { return l.executionMode }

// ReplyContextRef is an opaque reference to platform-owned reply routing
// state. The full mutable record is never carried through event payloads or
// envelopes.
type ReplyContextRef struct {
	ID string `json:"id"`
}

func (r ReplyContextRef) Normalized() ReplyContextRef {
	return ReplyContextRef{ID: strings.TrimSpace(r.ID)}
}

func (r ReplyContextRef) Empty() bool {
	return r.Normalized().ID == ""
}

// DeliveryContext contains route-scoped platform metadata. It is persisted
// with one delivery route and installed only while that route executes.
type DeliveryContext struct {
	Reply *ReplyContextRef `json:"reply,omitempty"`
}

func (c DeliveryContext) Normalized() DeliveryContext {
	if c.Reply == nil {
		return DeliveryContext{}
	}
	reply := c.Reply.Normalized()
	if reply.Empty() {
		return DeliveryContext{}
	}
	return DeliveryContext{Reply: &reply}
}

func (c DeliveryContext) Empty() bool {
	return c.Normalized().Reply == nil
}

func (c DeliveryContext) ReplyContextID() string {
	c = c.Normalized()
	if c.Reply == nil {
		return ""
	}
	return c.Reply.ID
}

// DeliveryPayloadProjection is receiver-owned synthetic payload material
// stamped on one concrete delivery route. The journal event payload remains
// immutable; only the selected route observes these fields.
type DeliveryPayloadProjection struct {
	canonical string
}

func NewDeliveryPayloadProjection(fields map[string]string) (DeliveryPayloadProjection, error) {
	canonicalFields := make(map[string]string, len(fields))
	for rawField, rawValue := range fields {
		field := strings.TrimSpace(rawField)
		value := strings.TrimSpace(rawValue)
		if field == "" || value == "" {
			return DeliveryPayloadProjection{}, fmt.Errorf("delivery payload projection requires non-empty field names and values")
		}
		if _, exists := canonicalFields[field]; exists {
			return DeliveryPayloadProjection{}, fmt.Errorf("delivery payload projection field %q is declared more than once", field)
		}
		canonicalFields[field] = value
	}
	if len(canonicalFields) == 0 {
		return DeliveryPayloadProjection{}, nil
	}
	raw, err := json.Marshal(canonicalFields)
	if err != nil {
		return DeliveryPayloadProjection{}, fmt.Errorf("encode delivery payload projection: %w", err)
	}
	return DeliveryPayloadProjection{canonical: string(raw)}, nil
}

func (p DeliveryPayloadProjection) Canonical() (DeliveryPayloadProjection, error) {
	if p.Empty() {
		return DeliveryPayloadProjection{}, nil
	}
	var fields map[string]string
	if err := json.Unmarshal([]byte(p.canonical), &fields); err != nil {
		return DeliveryPayloadProjection{}, fmt.Errorf("decode delivery payload projection: %w", err)
	}
	return NewDeliveryPayloadProjection(fields)
}

func (p DeliveryPayloadProjection) Normalized() DeliveryPayloadProjection {
	return DeliveryPayloadProjection{canonical: p.canonical}
}

func (p DeliveryPayloadProjection) Empty() bool {
	return strings.TrimSpace(p.canonical) == ""
}

func (p DeliveryPayloadProjection) Fingerprint() string {
	return p.canonical
}

func (p DeliveryPayloadProjection) Fields() map[string]string {
	if p.Empty() {
		return nil
	}
	var fields map[string]string
	if err := json.Unmarshal([]byte(p.canonical), &fields); err != nil {
		return nil
	}
	return fields
}

func (p DeliveryPayloadProjection) MarshalJSON() ([]byte, error) {
	if p.Empty() {
		return []byte(`{}`), nil
	}
	return json.Marshal(struct {
		Fields map[string]string `json:"fields"`
	}{Fields: p.Fields()})
}

func (p *DeliveryPayloadProjection) UnmarshalJSON(raw []byte) error {
	if p == nil {
		return fmt.Errorf("delivery payload projection destination is nil")
	}
	var decoded struct {
		Fields map[string]string `json:"fields"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("decode delivery payload projection: %w", err)
	}
	canonical, err := NewDeliveryPayloadProjection(decoded.Fields)
	if err != nil {
		return err
	}
	*p = canonical
	return nil
}

type DeliveryRoute struct {
	SubscriberType    string                    `json:"subscriber_type"`
	SubscriberID      string                    `json:"subscriber_id"`
	Target            RouteIdentity             `json:"delivery_target_route,omitempty"`
	Context           DeliveryContext           `json:"delivery_context,omitempty"`
	PayloadProjection DeliveryPayloadProjection `json:"delivery_payload_projection,omitempty"`
}

// DeliveryRouteIdentity is the opaque, canonical identity of one normalized
// delivery route. Persistence stores the string form, while this package alone
// derives identity from route semantics.
type DeliveryRouteIdentity struct {
	value string
}

const deliveryRouteIdentityPrefix = "delivery-route-v1:sha256:"

func (i DeliveryRouteIdentity) String() string { return i.value }

func (i DeliveryRouteIdentity) Valid() bool {
	if !strings.HasPrefix(i.value, deliveryRouteIdentityPrefix) {
		return false
	}
	raw := strings.TrimPrefix(i.value, deliveryRouteIdentityPrefix)
	if len(raw) != sha256.Size*2 || raw != strings.ToLower(raw) {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}

// ParseDeliveryRouteIdentity validates a durable route identity during
// readback. It never derives or repairs route facts.
func ParseDeliveryRouteIdentity(raw string) (DeliveryRouteIdentity, error) {
	identity := DeliveryRouteIdentity{value: strings.TrimSpace(raw)}
	if !identity.Valid() {
		return DeliveryRouteIdentity{}, fmt.Errorf("delivery route identity is invalid")
	}
	return identity, nil
}

// Identity derives the canonical opaque identity from every semantically
// relevant route fact. The normalized route remains persisted for exact
// duplicate comparison and hydration.
func (r DeliveryRoute) Identity() (DeliveryRouteIdentity, error) {
	r = r.Normalized()
	if r.SubscriberType == "" || r.SubscriberID == "" {
		return DeliveryRouteIdentity{}, fmt.Errorf("delivery route subscriber type and id are required")
	}
	projection, err := r.PayloadProjection.Canonical()
	if err != nil {
		return DeliveryRouteIdentity{}, fmt.Errorf("delivery route payload projection: %w", err)
	}
	canonical, err := json.Marshal(struct {
		SubscriberType string            `json:"subscriber_type"`
		SubscriberID   string            `json:"subscriber_id"`
		Target         RouteIdentity     `json:"target"`
		Context        DeliveryContext   `json:"context"`
		Projection     map[string]string `json:"projection"`
	}{
		SubscriberType: r.SubscriberType,
		SubscriberID:   r.SubscriberID,
		Target:         r.Target,
		Context:        r.Context,
		Projection:     projection.Fields(),
	})
	if err != nil {
		return DeliveryRouteIdentity{}, fmt.Errorf("encode delivery route identity: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return DeliveryRouteIdentity{value: deliveryRouteIdentityPrefix + hex.EncodeToString(sum[:])}, nil
}

type Event struct {
	admissionClass  EventAdmissionClass
	rootIntent      rootIngressRunIntent
	runtimeIntent   runtimeLineageIntent
	id              string
	eventType       EventType
	producer        ProducerIdentity
	taskID          string
	payload         json.RawMessage
	chainDepth      int
	runID           string
	parentEventID   string
	envelopeClaim   EventEnvelope
	envelope        EventEnvelope
	deliveryContext DeliveryContext
	createdAt       time.Time
	executionMode   executionmode.Mode
	routingSource   RoutingSource
	operatorRef     *OperatorReferenceProvenance
	selectedFork    *SelectedForkLineage
}

type deliveryContextKey struct{}

func WithDeliveryContext(ctx context.Context, deliveryContext DeliveryContext) context.Context {
	deliveryContext = deliveryContext.Normalized()
	if deliveryContext.Empty() {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, deliveryContextKey{}, deliveryContext)
}

func DeliveryContextFromContext(ctx context.Context) DeliveryContext {
	if ctx == nil {
		return DeliveryContext{}
	}
	deliveryContext, _ := ctx.Value(deliveryContextKey{}).(DeliveryContext)
	return deliveryContext.Normalized()
}

type eventJSON struct {
	ID            string             `json:"id"`
	Type          EventType          `json:"type"`
	SourceAgent   string             `json:"source_agent"`
	ProducerType  EventProducerType  `json:"producer_type,omitempty"`
	TaskID        string             `json:"task_id,omitempty"`
	Payload       json.RawMessage    `json:"payload"`
	CreatedAt     time.Time          `json:"created_at"`
	ExecutionMode executionmode.Mode `json:"execution_mode"`
}

type PersistedReplayEvent struct {
	Event         Event
	ReplayFailure *runtimefailures.Envelope
}

type EventLineage struct {
	RunID         string
	ParentEventID string
	TaskID        string
	ExecutionMode executionmode.Mode
}

type EventFacts struct {
	ID            string
	Type          EventType
	Producer      ProducerClaim
	TaskID        string
	Payload       json.RawMessage
	ChainDepth    int
	Envelope      EventEnvelope
	RoutingSource RoutingSource
	CreatedAt     time.Time
	ExecutionMode executionmode.Mode
}

type RunCreatingRootIngressEventInput struct {
	Facts EventFacts
	RunID string
}

type ExistingRunRootIngressEventInput struct {
	Facts EventFacts
	RunID string
}

type OperatorInjectedEventInput struct {
	Facts      EventFacts
	RunID      string
	Provenance *OperatorReferenceProvenance
}

type ChildEventInput struct {
	Facts   EventFacts
	Lineage EventLineage
}

type ReplayEventInput struct {
	Facts   EventFacts
	Lineage EventLineage
}

type SelectedForkReplayEventInput struct {
	Facts   EventFacts
	Lineage SelectedForkLineage
}

type CausalRuntimeEventInput struct {
	Facts   EventFacts
	Lineage EventLineage
}

type RunScopedRuntimeEventInput struct {
	Facts EventFacts
	RunID string
}

type RunCreatingRuntimeEventInput struct {
	Facts EventFacts
	RunID string
}

type StandaloneRuntimeEventInput struct {
	Facts EventFacts
}

type runtimeLineageIntent string

type rootIngressRunIntent string

const (
	rootIngressRunCreating rootIngressRunIntent = "run_creating"
	rootIngressExistingRun rootIngressRunIntent = "existing_run"

	runtimeLineageCausal      runtimeLineageIntent = "causal"
	runtimeLineageRunScoped   runtimeLineageIntent = "run_scoped"
	runtimeLineageRunCreating runtimeLineageIntent = "run_creating"
	runtimeLineageStandalone  runtimeLineageIntent = "standalone"
)

func NewRunCreatingRootIngressEvent(input RunCreatingRootIngressEventInput) (Event, error) {
	return newSemanticEvent(EventAdmissionRootIngress, rootIngressRunCreating, input.Facts, input.RunID, "", nil, nil)
}

func NewExistingRunRootIngressEvent(input ExistingRunRootIngressEventInput) (Event, error) {
	if strings.TrimSpace(input.RunID) == "" {
		return Event{}, fmt.Errorf("existing-run root ingress requires run_id")
	}
	return newSemanticEvent(EventAdmissionRootIngress, rootIngressExistingRun, input.Facts, input.RunID, "", nil, nil)
}

func NewOperatorInjectedEvent(input OperatorInjectedEventInput) (Event, error) {
	if strings.TrimSpace(input.RunID) == "" {
		return Event{}, fmt.Errorf("operator-injected event requires target run_id")
	}
	return newSemanticEvent(EventAdmissionOperatorInjected, "", input.Facts, input.RunID, "", input.Provenance, nil)
}

func NewChildEvent(input ChildEventInput) (Event, error) {
	lineage := input.Lineage.Normalized()
	if err := validateCausalLineage(EventAdmissionChild, lineage); err != nil {
		return Event{}, err
	}
	if strings.TrimSpace(input.Facts.TaskID) == "" {
		input.Facts.TaskID = lineage.TaskID
	}
	input.Facts.ExecutionMode = lineage.ExecutionMode
	return newSemanticEvent(EventAdmissionChild, "", input.Facts, lineage.RunID, lineage.ParentEventID, nil, nil)
}

func NewReplayEvent(input ReplayEventInput) (Event, error) {
	lineage := input.Lineage.Normalized()
	if err := validateCausalLineage(EventAdmissionReplay, lineage); err != nil {
		return Event{}, err
	}
	if strings.TrimSpace(input.Facts.TaskID) == "" {
		input.Facts.TaskID = lineage.TaskID
	}
	input.Facts.ExecutionMode = lineage.ExecutionMode
	return newSemanticEvent(EventAdmissionReplay, "", input.Facts, lineage.RunID, lineage.ParentEventID, nil, nil)
}

func NewSelectedForkReplayEvent(input SelectedForkReplayEventInput) (Event, error) {
	lineage := input.Lineage
	if strings.TrimSpace(lineage.DestinationRunID()) == "" {
		return Event{}, fmt.Errorf("selected-fork replay requires typed lineage")
	}
	if strings.TrimSpace(input.Facts.TaskID) == "" {
		input.Facts.TaskID = lineage.TaskID()
	}
	input.Facts.ExecutionMode = lineage.ExecutionMode()
	return newSemanticEvent(EventAdmissionSelectedForkReplay, "", input.Facts, lineage.DestinationRunID(), "", nil, &lineage)
}

func NewCausalRuntimeControlEvent(input CausalRuntimeEventInput) (Event, error) {
	return newCausalRuntimeEvent(EventAdmissionRuntimeControl, input)
}

func NewRunScopedRuntimeControlEvent(input RunScopedRuntimeEventInput) (Event, error) {
	return newRunScopedRuntimeEvent(EventAdmissionRuntimeControl, input)
}

func NewStandaloneRuntimeControlEvent(input StandaloneRuntimeEventInput) (Event, error) {
	return newStandaloneRuntimeEvent(EventAdmissionRuntimeControl, input)
}

func NewCausalRuntimeDiagnosticEvent(input CausalRuntimeEventInput) (Event, error) {
	return newCausalRuntimeEvent(EventAdmissionRuntimeDiagnostic, input)
}

func NewRunScopedRuntimeDiagnosticEvent(input RunScopedRuntimeEventInput) (Event, error) {
	return newRunScopedRuntimeEvent(EventAdmissionRuntimeDiagnostic, input)
}

func NewStandaloneRuntimeDiagnosticEvent(input StandaloneRuntimeEventInput) (Event, error) {
	return newStandaloneRuntimeEvent(EventAdmissionRuntimeDiagnostic, input)
}

func NewCausalDiagnosticDirectEvent(input CausalRuntimeEventInput) (Event, error) {
	if err := validateDiagnosticDirectFacts(input.Facts); err != nil {
		return Event{}, err
	}
	return newCausalRuntimeEvent(EventAdmissionDiagnosticDirect, input)
}

func NewRunScopedDiagnosticDirectEvent(input RunScopedRuntimeEventInput) (Event, error) {
	if err := validateDiagnosticDirectFacts(input.Facts); err != nil {
		return Event{}, err
	}
	return newRunScopedRuntimeEvent(EventAdmissionDiagnosticDirect, input)
}

// NewRunCreatingDiagnosticDirectEvent is reserved for a closed subtype whose
// named operation is the authoritative trigger for newly allocated work.
func NewRunCreatingDiagnosticDirectEvent(input RunCreatingRuntimeEventInput) (Event, error) {
	if input.Facts.Type != EventTypePlatformAgentDirective {
		return Event{}, fmt.Errorf("run-creating diagnostic-direct event type %q is not authorized", input.Facts.Type)
	}
	if err := validateDiagnosticDirectFacts(input.Facts); err != nil {
		return Event{}, err
	}
	return newRunCreatingRuntimeEvent(EventAdmissionDiagnosticDirect, input)
}

func NewStandaloneDiagnosticDirectEvent(input StandaloneRuntimeEventInput) (Event, error) {
	if err := validateDiagnosticDirectFacts(input.Facts); err != nil {
		return Event{}, err
	}
	return newStandaloneRuntimeEvent(EventAdmissionDiagnosticDirect, input)
}

func newCausalRuntimeEvent(class EventAdmissionClass, input CausalRuntimeEventInput) (Event, error) {
	lineage := input.Lineage.Normalized()
	if err := validateCausalLineage(class, lineage); err != nil {
		return Event{}, err
	}
	if strings.TrimSpace(input.Facts.TaskID) == "" {
		input.Facts.TaskID = lineage.TaskID
	}
	input.Facts.ExecutionMode = lineage.ExecutionMode
	event, err := newSemanticEvent(class, "", input.Facts, lineage.RunID, lineage.ParentEventID, nil, nil)
	if err != nil {
		return Event{}, err
	}
	event.runtimeIntent = runtimeLineageCausal
	return event, nil
}

func newRunScopedRuntimeEvent(class EventAdmissionClass, input RunScopedRuntimeEventInput) (Event, error) {
	runID := strings.TrimSpace(input.RunID)
	if runID == "" {
		return Event{}, fmt.Errorf("%s run-scoped event requires run_id", class)
	}
	event, err := newSemanticEvent(class, "", input.Facts, runID, "", nil, nil)
	if err != nil {
		return Event{}, err
	}
	event.runtimeIntent = runtimeLineageRunScoped
	return event, nil
}

func newRunCreatingRuntimeEvent(class EventAdmissionClass, input RunCreatingRuntimeEventInput) (Event, error) {
	runID := strings.TrimSpace(input.RunID)
	if runID == "" {
		return Event{}, fmt.Errorf("%s run-creating event requires run_id", class)
	}
	event, err := newSemanticEvent(class, "", input.Facts, runID, "", nil, nil)
	if err != nil {
		return Event{}, err
	}
	event.runtimeIntent = runtimeLineageRunCreating
	return event, nil
}

func newStandaloneRuntimeEvent(class EventAdmissionClass, input StandaloneRuntimeEventInput) (Event, error) {
	event, err := newSemanticEvent(class, "", input.Facts, "", "", nil, nil)
	if err != nil {
		return Event{}, err
	}
	event.runtimeIntent = runtimeLineageStandalone
	return event, nil
}

func validateDiagnosticDirectFacts(facts EventFacts) error {
	if !IsDiagnosticDirectEventType(facts.Type) {
		return fmt.Errorf("diagnostic-direct event type %q is not in the closed catalog", facts.Type)
	}
	if !facts.RoutingSource.Empty() || !facts.Envelope.Source.Normalized().Empty() || !facts.Envelope.Target.Normalized().Empty() || len(facts.Envelope.TargetSet) > 0 {
		return fmt.Errorf("diagnostic-direct event must be non-routed")
	}
	return nil
}

func restoreRuntimeEvent(class EventAdmissionClass, facts EventFacts, runID, parentEventID string) (Event, error) {
	runID = strings.TrimSpace(runID)
	parentEventID = strings.TrimSpace(parentEventID)
	if parentEventID != "" {
		return newCausalRuntimeEvent(class, CausalRuntimeEventInput{Facts: facts, Lineage: EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: facts.TaskID, ExecutionMode: facts.ExecutionMode,
		}})
	}
	if runID != "" {
		return newRunScopedRuntimeEvent(class, RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
	}
	return newStandaloneRuntimeEvent(class, StandaloneRuntimeEventInput{Facts: facts})
}

func restoreDiagnosticDirectEvent(facts EventFacts, runID, parentEventID string) (Event, error) {
	if err := validateDiagnosticDirectFacts(facts); err != nil {
		return Event{}, err
	}
	return restoreRuntimeEvent(EventAdmissionDiagnosticDirect, facts, runID, parentEventID)
}

func LineageFromEvent(parent Event) EventLineage {
	return EventLineage{
		RunID:         parent.RunID(),
		ParentEventID: parent.ID(),
		TaskID:        parent.TaskID(),
		ExecutionMode: parent.ExecutionMode(),
	}
}

func (l EventLineage) Normalized() EventLineage {
	return EventLineage{
		RunID:         strings.TrimSpace(l.RunID),
		ParentEventID: strings.TrimSpace(l.ParentEventID),
		TaskID:        strings.TrimSpace(l.TaskID),
		ExecutionMode: executionmode.Mode(strings.TrimSpace(string(l.ExecutionMode))),
	}
}

func newSemanticEvent(class EventAdmissionClass, rootIntent rootIngressRunIntent, facts EventFacts, runID, parentEventID string, operatorRef *OperatorReferenceProvenance, selectedFork *SelectedForkLineage) (Event, error) {
	producer, err := NewProducerIdentity(facts.Producer.Type, facts.Producer.ID)
	if err != nil {
		return Event{}, fmt.Errorf("event producer identity: %w", err)
	}
	eventType := EventType(strings.TrimSpace(string(facts.Type)))
	if eventType == "" {
		return Event{}, fmt.Errorf("event type is required")
	}
	if facts.ChainDepth < 0 {
		return Event{}, fmt.Errorf("event chain_depth must be nonnegative")
	}
	payload := clonePayload(facts.Payload)
	if !json.Valid(payload) {
		return Event{}, fmt.Errorf("event payload must be valid JSON")
	}
	if !facts.ExecutionMode.Valid() {
		return Event{}, fmt.Errorf("event execution_mode must be live or mock")
	}
	envelope := cloneEventEnvelope(facts.Envelope)
	sourceRoute := facts.RoutingSource.Route()
	switch facts.RoutingSource.Kind() {
	case RoutingSourceAbsent:
		if !envelope.Source.Normalized().Empty() {
			return Event{}, fmt.Errorf("event envelope source requires a typed routing source")
		}
	case RoutingSourceDeclaredIngress:
		if !envelope.Source.Normalized().Empty() {
			return Event{}, fmt.Errorf("declared ingress routing source is opaque and cannot become envelope source evidence")
		}
	case RoutingSourceRuntimeInstance:
		if declared := envelope.Source.Normalized(); !declared.Empty() && declared != sourceRoute {
			return Event{}, fmt.Errorf("event envelope source does not match typed routing source")
		}
		envelope.Source = sourceRoute
	default:
		return Event{}, fmt.Errorf("event routing source kind %q is invalid", facts.RoutingSource.Kind())
	}
	if err := validateEnvelopeClaim(envelope, false); err != nil {
		return Event{}, fmt.Errorf("event envelope: %w", err)
	}
	if operatorRef != nil && class != EventAdmissionOperatorInjected {
		return Event{}, fmt.Errorf("operator provenance is only valid for operator-injected events")
	}
	if selectedFork != nil && class != EventAdmissionSelectedForkReplay {
		return Event{}, fmt.Errorf("selected-fork lineage is only valid for selected-fork replay events")
	}
	evt := Event{
		admissionClass: EventAdmissionClass(strings.TrimSpace(string(class))),
		rootIntent:     rootIntent,
		id:             strings.TrimSpace(facts.ID),
		eventType:      eventType,
		producer:       producer,
		taskID:         strings.TrimSpace(facts.TaskID),
		payload:        payload,
		chainDepth:     facts.ChainDepth,
		runID:          strings.TrimSpace(runID),
		parentEventID:  strings.TrimSpace(parentEventID),
		createdAt:      facts.CreatedAt,
		executionMode:  facts.ExecutionMode,
		routingSource:  facts.RoutingSource,
		operatorRef:    operatorRef,
		selectedFork:   selectedFork,
	}
	evt.setEnvelopeClaim(envelope)
	if !evt.createdAt.IsZero() {
		evt.createdAt = evt.createdAt.UTC().Truncate(time.Microsecond)
	}
	if err := ValidateEventContract(evt); err != nil {
		return Event{}, err
	}
	return evt, nil
}

func validateCausalLineage(class EventAdmissionClass, lineage EventLineage) error {
	if lineage.RunID == "" || lineage.ParentEventID == "" {
		return fmt.Errorf("%s event requires run_id and parent_event_id", class)
	}
	if !lineage.ExecutionMode.Valid() {
		return fmt.Errorf("%s event requires live or mock execution_mode", class)
	}
	return nil
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(eventJSON{
		ID:            e.ID(),
		Type:          e.Type(),
		SourceAgent:   e.SourceAgent(),
		ProducerType:  e.ProducerType(),
		TaskID:        e.TaskID(),
		Payload:       e.Payload(),
		CreatedAt:     e.CreatedAt(),
		ExecutionMode: e.ExecutionMode(),
	})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	return fmt.Errorf("events.Event cannot be reconstructed from partial JSON; use a class-specific constructor or canonical durable readback")
}

func (e Event) ExecutionMode() executionmode.Mode {
	return e.executionMode
}

func clonePayload(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), payload...)
}

// ValidateEnvelope validates the complete event-owned routing identity. It
// does not normalize or repair malformed facts.
func ValidateEnvelope(envelope EventEnvelope) error {
	return validateEnvelope(envelope, false)
}

func validateEnvelope(envelope EventEnvelope, requirePersistentUUIDIdentity bool) error {
	return validateEnvelopeFields(envelope, requirePersistentUUIDIdentity, false)
}

func validateEnvelopeClaim(envelope EventEnvelope, requirePersistentUUIDIdentity bool) error {
	return validateEnvelopeFields(envelope, requirePersistentUUIDIdentity, true)
}

func validateEnvelopeFields(envelope EventEnvelope, requirePersistentUUIDIdentity, allowOmittedScope bool) error {
	entityID := strings.TrimSpace(envelope.EntityID)
	flowInstance := strings.Trim(strings.TrimSpace(envelope.FlowInstance), "/")
	rawScope := EventScope(strings.TrimSpace(string(envelope.Scope)))
	scope := normalizeEventScope(rawScope)
	if scope == "" && (!allowOmittedScope || rawScope != "") {
		return fmt.Errorf("event scope %q is invalid", rawScope)
	}
	source := envelope.Source.Normalized()
	target := envelope.Target.Normalized()
	targetSet := normalizeRouteIdentities(envelope.TargetSet)
	if requirePersistentUUIDIdentity {
		if err := validateOptionalUUID("entity_id", entityID); err != nil {
			return err
		}
		for label, route := range map[string]RouteIdentity{"source": source, "target": target} {
			if err := validateOptionalUUID(label+".entity_id", route.EntityID); err != nil {
				return err
			}
		}
		for index, route := range targetSet {
			if err := validateOptionalUUID(fmt.Sprintf("target_set[%d].entity_id", index), route.EntityID); err != nil {
				return err
			}
		}
	}
	if !target.Empty() && len(targetSet) > 0 {
		return fmt.Errorf("event envelope cannot declare both target and target_set")
	}
	if !target.Empty() && (entityID != target.EntityID || flowInstance != target.FlowInstance) {
		return fmt.Errorf("event target route must exactly match entity_id and flow_instance projections")
	}
	if len(targetSet) > 0 && (entityID != "" || flowInstance != "") {
		return fmt.Errorf("event target_set cannot carry singular entity_id or flow_instance projections")
	}
	if want := inferEventScope(entityID, flowInstance); scope != "" && scope != want {
		return fmt.Errorf("event scope %q does not match entity/flow identity; want %q", scope, want)
	}
	return nil
}

func validateOptionalUUID(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("event %s %q must be a UUID", field, value)
	}
	return nil
}

func (e Event) ID() string {
	return strings.TrimSpace(e.id)
}

func (e Event) AdmissionClass() EventAdmissionClass {
	return EventAdmissionClass(strings.TrimSpace(string(e.admissionClass)))
}

func (e Event) Type() EventType {
	return EventType(strings.TrimSpace(string(e.eventType)))
}

func (e Event) SourceAgent() string {
	return e.producer.ID()
}

func (e Event) ProducerType() EventProducerType {
	return e.producer.Type()
}

func (e Event) Producer() ProducerIdentity {
	return normalizedProducerIdentity(e.producer.Type(), e.producer.ID())
}

func (e Event) Clone() Event {
	cloned := e
	cloned.producer = e.Producer()
	cloned.payload = e.Payload()
	cloned.envelopeClaim = cloneEventEnvelope(e.envelopeClaim)
	cloned.envelope = e.NormalizedEnvelope()
	cloned.deliveryContext = e.DeliveryContext()
	if e.operatorRef != nil {
		ref := *e.operatorRef
		cloned.operatorRef = &ref
	}
	if e.selectedFork != nil {
		lineage := *e.selectedFork
		cloned.selectedFork = &lineage
	}
	return cloned
}

func (e Event) RoutingSource() RoutingSource { return e.routingSource }

func (e Event) OperatorReference() (OperatorReferenceProvenance, bool) {
	if e.operatorRef == nil {
		return OperatorReferenceProvenance{}, false
	}
	return *e.operatorRef, true
}

func (e Event) SelectedForkLineage() (SelectedForkLineage, bool) {
	if e.selectedFork == nil {
		return SelectedForkLineage{}, false
	}
	return *e.selectedFork, true
}

func (e Event) TaskID() string {
	return strings.TrimSpace(e.taskID)
}

func (e Event) Payload() json.RawMessage {
	return clonePayload(e.payload)
}

func (e Event) ChainDepth() int {
	return e.chainDepth
}

func (e Event) RunID() string {
	return strings.TrimSpace(e.runID)
}

func (e Event) ParentEventID() string {
	return strings.TrimSpace(e.parentEventID)
}

func (e Event) Envelope() EventEnvelope {
	return e.envelope.Normalized()
}

func (e Event) CreatedAt() time.Time {
	if e.createdAt.IsZero() {
		return time.Time{}
	}
	return e.createdAt.UTC()
}

func (e Event) DeliveryContext() DeliveryContext {
	return e.deliveryContext.Normalized()
}

func (e Event) ContextMap(currentState string) map[string]any {
	out := map[string]any{}
	if id := e.ID(); id != "" {
		out["id"] = id
	}
	if eventType := strings.TrimSpace(string(e.Type())); eventType != "" {
		out["type"] = eventType
		out["trigger_event_type"] = eventType
	}
	if sourceAgent := e.SourceAgent(); sourceAgent != "" {
		out["source_agent"] = sourceAgent
	}
	if taskID := e.TaskID(); taskID != "" {
		out["task_id"] = taskID
	}
	envelope := e.NormalizedEnvelope()
	if envelope.Scope != "" {
		out["scope"] = string(envelope.Scope)
	}
	if source := routeIdentityMap(envelope.Source); source != nil {
		out["source"] = source
	}
	if target := routeIdentityMap(envelope.Target); target != nil {
		out["target"] = target
	}
	if len(envelope.TargetSet) > 0 {
		items := make([]map[string]any, 0, len(envelope.TargetSet))
		for _, route := range envelope.TargetSet {
			if item := routeIdentityMap(route); item != nil {
				items = append(items, item)
			}
		}
		if len(items) > 0 {
			out["target_set"] = items
		}
	}
	if currentState = strings.TrimSpace(currentState); currentState != "" {
		out["current_state"] = currentState
	}
	if parentEventID := e.ParentEventID(); parentEventID != "" {
		out["source_event_id"] = parentEventID
	}
	if runID := e.RunID(); runID != "" {
		out["run_id"] = runID
	}
	if createdAt := e.CreatedAt(); !createdAt.IsZero() {
		out["emitted_at"] = createdAt.Format(time.RFC3339Nano)
	}
	return out
}

func (e Event) EntityID() string {
	return strings.TrimSpace(e.envelope.Normalized().EntityID)
}

func (e Event) FlowInstance() string {
	return strings.TrimSpace(e.envelope.Normalized().FlowInstance)
}

func (e Event) Scope() EventScope {
	return e.envelope.Normalized().Scope
}

func (e Event) NormalizedEnvelope() EventEnvelope {
	return e.envelope.Normalized()
}

func (e Event) SourceRoute() RouteIdentity {
	return e.envelope.Normalized().Source
}

func (e Event) TargetRoute() RouteIdentity {
	return e.envelope.Normalized().Target
}

func (e Event) TargetRoutes() []RouteIdentity {
	return append([]RouteIdentity{}, e.envelope.Normalized().TargetSet...)
}

func (e Event) HasTargetRoute() bool {
	envelope := e.envelope.Normalized()
	return !envelope.Target.Empty() || len(envelope.TargetSet) > 0
}

// Keep the producer's raw claim separate so admission can reject facts that
// the normalized read view intentionally projects into canonical form.
func (e *Event) setEnvelopeClaim(envelope EventEnvelope) {
	e.envelopeClaim = cloneEventEnvelope(envelope)
	e.envelope = envelope.Normalized()
}

func (e Event) envelopeClaimForAdmission() EventEnvelope {
	return cloneEventEnvelope(e.envelopeClaim)
}

func cloneEventEnvelope(envelope EventEnvelope) EventEnvelope {
	cloned := envelope
	cloned.TargetSet = append([]RouteIdentity(nil), envelope.TargetSet...)
	return cloned
}

func (e EventEnvelope) Normalized() EventEnvelope {
	e.EntityID = strings.TrimSpace(e.EntityID)
	e.FlowInstance = strings.Trim(strings.TrimSpace(e.FlowInstance), "/")
	e.Source = e.Source.Normalized()
	e.Target = e.Target.Normalized()
	e.TargetSet = normalizeRouteIdentities(e.TargetSet)
	if !e.Target.Empty() {
		e.TargetSet = nil
		e.EntityID = e.Target.EntityID
		e.FlowInstance = e.Target.FlowInstance
	}
	e.Scope = normalizeEventScope(e.Scope)
	if e.Scope == "" {
		e.Scope = inferEventScope(e.EntityID, e.FlowInstance)
	}
	return e
}

func EnvelopeForEntityID(envelope EventEnvelope, entityID string) EventEnvelope {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return envelope.Normalized()
	}
	envelope = envelope.Normalized()
	envelope.EntityID = entityID
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func EnvelopeForFlowInstance(envelope EventEnvelope, flowInstance string) EventEnvelope {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return envelope.Normalized()
	}
	envelope = envelope.Normalized()
	envelope.FlowInstance = flowInstance
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func EnvelopeForSourceRoute(envelope EventEnvelope, route RouteIdentity) EventEnvelope {
	route = route.Normalized()
	if route.Empty() {
		return envelope.Normalized()
	}
	envelope = envelope.Normalized()
	envelope.Source = route
	if envelope.EntityID == "" && envelope.FlowInstance == "" && envelope.Target.Empty() && len(envelope.TargetSet) == 0 {
		envelope.EntityID = route.EntityID
		envelope.FlowInstance = route.FlowInstance
	}
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func EnvelopeForTargetRoute(envelope EventEnvelope, route RouteIdentity) EventEnvelope {
	route = route.Normalized()
	if route.Empty() {
		return envelope.Normalized()
	}
	envelope = envelope.Normalized()
	envelope.Target = route
	envelope.TargetSet = nil
	envelope.EntityID = route.EntityID
	envelope.FlowInstance = route.FlowInstance
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func EnvelopeForTargetSet(envelope EventEnvelope, routes []RouteIdentity) EventEnvelope {
	normalized := normalizeRouteIdentities(routes)
	if len(normalized) == 0 {
		return envelope.Normalized()
	}
	envelope = envelope.Normalized()
	envelope.Target = RouteIdentity{}
	envelope.TargetSet = normalized
	envelope.EntityID = ""
	envelope.FlowInstance = ""
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func EnvelopeForBroadcast(envelope EventEnvelope) EventEnvelope {
	envelope = envelope.Normalized()
	envelope.Target = RouteIdentity{}
	envelope.TargetSet = nil
	envelope.EntityID = strings.TrimSpace(envelope.Source.EntityID)
	envelope.FlowInstance = strings.Trim(strings.TrimSpace(envelope.Source.FlowInstance), "/")
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	return envelope.Normalized()
}

func (r RouteIdentity) Normalized() RouteIdentity {
	return RouteIdentity{
		FlowInstance: strings.Trim(strings.TrimSpace(r.FlowInstance), "/"),
		EntityID:     strings.TrimSpace(r.EntityID),
		FlowID:       strings.TrimSpace(r.FlowID),
	}
}

func (r RouteIdentity) Empty() bool {
	r = r.Normalized()
	return r.FlowInstance == "" && r.EntityID == "" && r.FlowID == ""
}

func (r DeliveryRoute) Normalized() DeliveryRoute {
	return DeliveryRoute{
		SubscriberType:    strings.TrimSpace(r.SubscriberType),
		SubscriberID:      strings.TrimSpace(r.SubscriberID),
		Target:            r.Target.Normalized(),
		Context:           r.Context.Normalized(),
		PayloadProjection: r.PayloadProjection.Normalized(),
	}
}

func NormalizeDeliveryRoutes(in []DeliveryRoute) []DeliveryRoute {
	if len(in) == 0 {
		return nil
	}
	out := make([]DeliveryRoute, 0, len(in))
	seen := map[string]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		identity, err := route.Identity()
		if err != nil {
			continue
		}
		key := identity.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

// SameDeliveryRouteIdentity reports whether two routes identify one exact
// durable executable-delivery obligation.
func SameDeliveryRouteIdentity(left, right DeliveryRoute) bool {
	left = left.Normalized()
	right = right.Normalized()
	if left.SubscriberType == "" || left.SubscriberID == "" || right.SubscriberType == "" || right.SubscriberID == "" {
		return false
	}
	leftIdentity, leftErr := left.Identity()
	rightIdentity, rightErr := right.Identity()
	return leftErr == nil && rightErr == nil && leftIdentity == rightIdentity
}

func ValidateDeliveryRouteProjections(in []DeliveryRoute) error {
	seen := make(map[string]string, len(in))
	for _, route := range in {
		route = route.Normalized()
		projection, err := route.PayloadProjection.Canonical()
		if err != nil {
			return fmt.Errorf("delivery route %s=%s has invalid payload projection: %w", route.SubscriberType, route.SubscriberID, err)
		}
		target := route.Target
		key := strings.Join([]string{
			route.SubscriberType,
			route.SubscriberID,
			target.FlowID,
			target.FlowInstance,
			target.EntityID,
			route.Context.ReplyContextID(),
		}, "\x00")
		fingerprint := projection.Fingerprint()
		if previous, exists := seen[key]; exists && previous != fingerprint {
			return fmt.Errorf("delivery route %s=%s has conflicting synthetic payload projections", route.SubscriberType, route.SubscriberID)
		}
		seen[key] = fingerprint
	}
	return nil
}

func normalizeRouteIdentities(in []RouteIdentity) []RouteIdentity {
	if len(in) == 0 {
		return nil
	}
	out := make([]RouteIdentity, 0, len(in))
	seen := map[string]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		key := route.FlowID + "\x00" + route.FlowInstance + "\x00" + route.EntityID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

func routeIdentityMap(route RouteIdentity) map[string]any {
	route = route.Normalized()
	if route.Empty() {
		return nil
	}
	out := map[string]any{}
	if route.FlowInstance != "" {
		out["flow_instance"] = route.FlowInstance
	}
	if route.EntityID != "" {
		out["entity_id"] = route.EntityID
	}
	if route.FlowID != "" {
		out["flow_id"] = route.FlowID
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func inferEventScope(entityID, flowInstance string) EventScope {
	entityID = strings.TrimSpace(entityID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	switch {
	case entityID != "":
		return EventScopeEntity
	case flowInstance != "":
		return EventScopeFlow
	default:
		return EventScopeGlobal
	}
}

func normalizeEventScope(scope EventScope) EventScope {
	switch strings.ToLower(strings.TrimSpace(string(scope))) {
	case "":
		return ""
	case string(EventScopeEntity):
		return EventScopeEntity
	case string(EventScopeFlow):
		return EventScopeFlow
	case string(EventScopeGlobal):
		return EventScopeGlobal
	default:
		return ""
	}
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
