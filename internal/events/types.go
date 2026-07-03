package events

import (
	"encoding/json"
	"strings"
	"time"
)

type EventType string

type EventScope string

const (
	EventScopeGlobal EventScope = "global"
	EventScopeFlow   EventScope = "flow"
	EventScopeEntity EventScope = "entity"
)

type EventAdmissionClass string

const (
	EventAdmissionUnknown           EventAdmissionClass = ""
	EventAdmissionRootIngress       EventAdmissionClass = "root_ingress"
	EventAdmissionRuntimeControl    EventAdmissionClass = "runtime_control"
	EventAdmissionRuntimeDiagnostic EventAdmissionClass = "runtime_diagnostic"
	EventAdmissionDiagnosticDirect  EventAdmissionClass = "diagnostic_direct"
	EventAdmissionChild             EventAdmissionClass = "child"
	EventAdmissionReplay            EventAdmissionClass = "replay"
	EventAdmissionProjection        EventAdmissionClass = "projection"
	EventAdmissionRouteProbe        EventAdmissionClass = "route_probe"
)

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

type DeliveryRoute struct {
	SubscriberType string        `json:"subscriber_type"`
	SubscriberID   string        `json:"subscriber_id"`
	Target         RouteIdentity `json:"delivery_target_route,omitempty"`
}

type Event struct {
	admissionClass EventAdmissionClass
	id             string
	eventType      EventType
	sourceAgent    string
	taskID         string
	payload        json.RawMessage
	chainDepth     int
	runID          string
	parentEventID  string
	envelope       EventEnvelope
	createdAt      time.Time
}

type eventJSON struct {
	ID          string          `json:"id"`
	Type        EventType       `json:"type"`
	SourceAgent string          `json:"source_agent"`
	TaskID      string          `json:"task_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

type PersistedReplayEvent struct {
	Event       Event
	ReplayError string
}

type EventLineage struct {
	RunID         string
	ParentEventID string
	TaskID        string
}

func NewRootIngressEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	return newEvent(EventAdmissionRootIngress, id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func NewRuntimeControlEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	return newEvent(EventAdmissionRuntimeControl, id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func NewRuntimeDiagnosticEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	return newEvent(EventAdmissionRuntimeDiagnostic, id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func NewDiagnosticDirectEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	return newEvent(EventAdmissionDiagnosticDirect, id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func NewChildEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, parent Event, envelope EventEnvelope, createdAt time.Time) Event {
	return NewChildEventWithLineage(id, eventType, sourceAgent, taskID, payload, chainDepth, LineageFromEvent(parent), envelope, createdAt)
}

func NewChildEventWithLineage(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage EventLineage, envelope EventEnvelope, createdAt time.Time) Event {
	lineage = lineage.Normalized()
	if strings.TrimSpace(taskID) == "" {
		taskID = lineage.TaskID
	}
	return newEvent(EventAdmissionChild, id, eventType, sourceAgent, taskID, payload, chainDepth, lineage.RunID, lineage.ParentEventID, envelope, createdAt)
}

func NewReplayEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage EventLineage, envelope EventEnvelope, createdAt time.Time) Event {
	lineage = lineage.Normalized()
	if strings.TrimSpace(taskID) == "" {
		taskID = lineage.TaskID
	}
	return newEvent(EventAdmissionReplay, id, eventType, sourceAgent, taskID, payload, chainDepth, lineage.RunID, lineage.ParentEventID, envelope, createdAt)
}

// NewProjectionEvent reconstructs an event from already-authoritative facts.
// Production call sites are restricted by TestProductionEventConstructionUsesPublicAPI;
// new runtime producers must use the semantic constructors above.
func NewProjectionEvent(id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	return newEvent(EventAdmissionProjection, id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// NewRouteProbeEvent constructs a non-persisted route-query/sentinel event.
// Production call sites are restricted by TestProductionEventConstructionUsesPublicAPI.
func NewRouteProbeEvent(eventType EventType) Event {
	return newEvent(EventAdmissionRouteProbe, "", eventType, "", "", nil, 0, "", "", EventEnvelope{}, time.Time{})
}

func EmptyEvent() Event {
	return Event{}
}

func LineageFromEvent(parent Event) EventLineage {
	return EventLineage{
		RunID:         parent.RunID(),
		ParentEventID: parent.ID(),
		TaskID:        parent.TaskID(),
	}
}

func (l EventLineage) Normalized() EventLineage {
	return EventLineage{
		RunID:         strings.TrimSpace(l.RunID),
		ParentEventID: strings.TrimSpace(l.ParentEventID),
		TaskID:        strings.TrimSpace(l.TaskID),
	}
}

func newEvent(class EventAdmissionClass, id string, eventType EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope EventEnvelope, createdAt time.Time) Event {
	evt := Event{
		admissionClass: EventAdmissionClass(strings.TrimSpace(string(class))),
		id:             strings.TrimSpace(id),
		eventType:      EventType(strings.TrimSpace(string(eventType))),
		sourceAgent:    strings.TrimSpace(sourceAgent),
		taskID:         strings.TrimSpace(taskID),
		payload:        clonePayload(payload),
		chainDepth:     chainDepth,
		runID:          strings.TrimSpace(runID),
		parentEventID:  strings.TrimSpace(parentEventID),
		envelope:       envelope.Normalized(),
		createdAt:      createdAt,
	}
	if !evt.createdAt.IsZero() {
		evt.createdAt = evt.createdAt.UTC()
	}
	return evt
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(eventJSON{
		ID:          e.ID(),
		Type:        e.Type(),
		SourceAgent: e.SourceAgent(),
		TaskID:      e.TaskID(),
		Payload:     e.Payload(),
		CreatedAt:   e.CreatedAt(),
	})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var raw eventJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*e = NewProjectionEvent(
		raw.ID,
		raw.Type,
		raw.SourceAgent,
		raw.TaskID,
		raw.Payload,
		0,
		"",
		"",
		EventEnvelope{},
		raw.CreatedAt,
	)
	return nil
}

func clonePayload(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), payload...)
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
	return strings.TrimSpace(e.sourceAgent)
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

func (e Event) WithRunID(runID string) Event {
	e.runID = strings.TrimSpace(runID)
	return e
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

func (e Event) WithParentEventID(parentEventID string) Event {
	e.parentEventID = strings.TrimSpace(parentEventID)
	return e
}

func (e Event) WithTaskID(taskID string) Event {
	e.taskID = strings.TrimSpace(taskID)
	return e
}

func (e Event) WithLineage(lineage EventLineage) Event {
	lineage = lineage.Normalized()
	if runID := lineage.RunID; runID != "" && e.RunID() == "" {
		e.runID = runID
	}
	if parentEventID := lineage.ParentEventID; parentEventID != "" && e.ParentEventID() == "" {
		e.parentEventID = parentEventID
	}
	if taskID := lineage.TaskID; taskID != "" && e.TaskID() == "" {
		e.taskID = taskID
	}
	return e
}

func (e Event) WithEnvelope(envelope EventEnvelope) Event {
	e.envelope = envelope.Normalized()
	return e
}

func (e Event) WithEntityID(entityID string) Event {
	e.envelope = EnvelopeForEntityID(e.envelope, entityID)
	return e
}

func (e Event) WithFlowInstance(flowInstance string) Event {
	e.envelope = EnvelopeForFlowInstance(e.envelope, flowInstance)
	return e
}

func (e Event) WithSourceRoute(route RouteIdentity) Event {
	e.envelope = EnvelopeForSourceRoute(e.envelope, route)
	return e
}

func (e Event) WithTargetRoute(route RouteIdentity) Event {
	e.envelope = EnvelopeForTargetRoute(e.envelope, route)
	return e
}

func (e Event) WithTargetSet(routes []RouteIdentity) Event {
	e.envelope = EnvelopeForTargetSet(e.envelope, routes)
	return e
}

func (e Event) WithoutTargetRoute() Event {
	e.envelope = EnvelopeForBroadcast(e.envelope)
	return e
}

func (e Event) WithDeliveryTarget(route RouteIdentity) Event {
	e.envelope = EnvelopeForTargetRoute(e.envelope, route)
	return e
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
		SubscriberType: strings.TrimSpace(r.SubscriberType),
		SubscriberID:   strings.TrimSpace(r.SubscriberID),
		Target:         r.Target.Normalized(),
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
		target := route.Target
		key := strings.Join([]string{
			route.SubscriberType,
			route.SubscriberID,
			target.FlowID,
			target.FlowInstance,
			target.EntityID,
		}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
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
