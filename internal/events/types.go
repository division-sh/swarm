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
	ID            string          `json:"id"`
	Type          EventType       `json:"type"`
	SourceAgent   string          `json:"source_agent"`
	TaskID        string          `json:"task_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	ChainDepth    int             `json:"-"`
	RunID         string          `json:"-"`
	ParentEventID string          `json:"-"`
	Envelope      EventEnvelope   `json:"-"`
	CreatedAt     time.Time       `json:"created_at"`
}

type PersistedReplayEvent struct {
	Event       Event
	ReplayError string
}

func (e Event) WithEnvelope(envelope EventEnvelope) Event {
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithEntityID(entityID string) Event {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.EntityID = entityID
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope
	return e
}

func (e Event) WithFlowInstance(flowInstance string) Event {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.FlowInstance = flowInstance
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope
	return e
}

func (e Event) WithSourceRoute(route RouteIdentity) Event {
	route = route.Normalized()
	if route.Empty() {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.Source = route
	if envelope.EntityID == "" && envelope.FlowInstance == "" && envelope.Target.Empty() && len(envelope.TargetSet) == 0 {
		envelope.EntityID = route.EntityID
		envelope.FlowInstance = route.FlowInstance
	}
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithTargetRoute(route RouteIdentity) Event {
	route = route.Normalized()
	if route.Empty() {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.Target = route
	envelope.TargetSet = nil
	envelope.EntityID = route.EntityID
	envelope.FlowInstance = route.FlowInstance
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithTargetSet(routes []RouteIdentity) Event {
	normalized := normalizeRouteIdentities(routes)
	if len(normalized) == 0 {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.Target = RouteIdentity{}
	envelope.TargetSet = normalized
	envelope.EntityID = ""
	envelope.FlowInstance = ""
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithoutTargetRoute() Event {
	envelope := e.Envelope.Normalized()
	envelope.Target = RouteIdentity{}
	envelope.TargetSet = nil
	envelope.EntityID = strings.TrimSpace(envelope.Source.EntityID)
	envelope.FlowInstance = strings.Trim(strings.TrimSpace(envelope.Source.FlowInstance), "/")
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithDeliveryTarget(route RouteIdentity) Event {
	return e.WithTargetRoute(route)
}

func (e Event) ContextMap(currentState string) map[string]any {
	out := map[string]any{}
	if id := strings.TrimSpace(e.ID); id != "" {
		out["id"] = id
	}
	if eventType := strings.TrimSpace(string(e.Type)); eventType != "" {
		out["type"] = eventType
		out["trigger_event_type"] = eventType
	}
	if sourceAgent := strings.TrimSpace(e.SourceAgent); sourceAgent != "" {
		out["source_agent"] = sourceAgent
	}
	if taskID := strings.TrimSpace(e.TaskID); taskID != "" {
		out["task_id"] = taskID
	}
	envelope := e.NormalizedEnvelope()
	if envelope.EntityID != "" {
		out["entity_id"] = envelope.EntityID
	}
	if envelope.FlowInstance != "" {
		out["flow_instance"] = envelope.FlowInstance
	}
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
	if parentEventID := strings.TrimSpace(e.ParentEventID); parentEventID != "" {
		out["source_event_id"] = parentEventID
	}
	if runID := strings.TrimSpace(e.RunID); runID != "" {
		out["run_id"] = runID
	}
	if !e.CreatedAt.IsZero() {
		out["emitted_at"] = e.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func (e Event) EntityID() string {
	return strings.TrimSpace(e.Envelope.Normalized().EntityID)
}

func (e Event) FlowInstance() string {
	return strings.TrimSpace(e.Envelope.Normalized().FlowInstance)
}

func (e Event) Scope() EventScope {
	return e.Envelope.Normalized().Scope
}

func (e Event) NormalizedEnvelope() EventEnvelope {
	return e.Envelope.Normalized()
}

func (e Event) SourceRoute() RouteIdentity {
	return e.Envelope.Normalized().Source
}

func (e Event) TargetRoute() RouteIdentity {
	return e.Envelope.Normalized().Target
}

func (e Event) TargetRoutes() []RouteIdentity {
	return append([]RouteIdentity{}, e.Envelope.Normalized().TargetSet...)
}

func (e Event) HasTargetRoute() bool {
	envelope := e.Envelope.Normalized()
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
