package bus

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/diaglog"
)

type emittedEventsContextKey struct{}

func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimecorrelation.InboundEventFromContext(ctx)
}

type EmittedEventsRecorder struct {
	mu             sync.Mutex
	events         []events.Event
	publishes      []PublishDiagnostic
	flightRecorder []FlightRecorderEntry
}

type FlightRecorderEntry struct {
	Kind                   string                       `json:"kind"`
	LogLevel               string                       `json:"log_level,omitempty"`
	Message                string                       `json:"message,omitempty"`
	Details                any                          `json:"details,omitempty"`
	StackTrace             string                       `json:"stack_trace,omitempty"`
	EventID                string                       `json:"event_id,omitempty"`
	EventType              string                       `json:"event_type,omitempty"`
	EntityID               string                       `json:"entity_id,omitempty"`
	ParentEventID          string                       `json:"parent_event_id,omitempty"`
	RoutedRecipients       []PublishDiagnosticRecipient `json:"routed_recipients,omitempty"`
	SubscriptionRecipients []string                     `json:"subscription_recipients,omitempty"`
}

type PublishDiagnosticRecipient struct {
	ID             string `json:"id"`
	Type           string `json:"type,omitempty"`
	Path           string `json:"path,omitempty"`
	MatchedPattern string `json:"matched_pattern,omitempty"`
	RouteSource    string `json:"route_source,omitempty"`
	LocalizedEvent string `json:"localized_event,omitempty"`
}

type PublishDiagnostic struct {
	EventID                string                       `json:"event_id"`
	EventType              string                       `json:"event_type"`
	EntityID               string                       `json:"entity_id,omitempty"`
	ParentEventID          string                       `json:"parent_event_id,omitempty"`
	RoutedRecipients       []PublishDiagnosticRecipient `json:"routed_recipients,omitempty"`
	SubscriptionRecipients []string                     `json:"subscription_recipients,omitempty"`
}

func NewEmittedEventsRecorder() *EmittedEventsRecorder {
	return &EmittedEventsRecorder{}
}

func WithEmittedEventsRecorder(ctx context.Context, rec *EmittedEventsRecorder) context.Context {
	return context.WithValue(ctx, emittedEventsContextKey{}, rec)
}

func EmittedEventsRecorderFromContext(ctx context.Context) (*EmittedEventsRecorder, bool) {
	v := ctx.Value(emittedEventsContextKey{})
	if v == nil {
		return nil, false
	}
	rec, ok := v.(*EmittedEventsRecorder)
	return rec, ok
}

func (r *EmittedEventsRecorder) Append(evt events.Event) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func (r *EmittedEventsRecorder) Snapshot() []events.Event {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]events.Event, len(r.events))
	copy(out, r.events)
	return out
}

func (r *EmittedEventsRecorder) AppendPublish(diag PublishDiagnostic) {
	if r == nil {
		return
	}
	diag = clonePublishDiagnostic(diag)
	r.mu.Lock()
	r.publishes = append(r.publishes, diag)
	r.flightRecorder = append(r.flightRecorder, FlightRecorderEntry{
		Kind:                   "publish",
		EventID:                strings.TrimSpace(diag.EventID),
		EventType:              strings.TrimSpace(diag.EventType),
		EntityID:               strings.TrimSpace(diag.EntityID),
		ParentEventID:          strings.TrimSpace(diag.ParentEventID),
		RoutedRecipients:       append([]PublishDiagnosticRecipient(nil), diag.RoutedRecipients...),
		SubscriptionRecipients: append([]string(nil), diag.SubscriptionRecipients...),
	})
	r.mu.Unlock()
}

func (r *EmittedEventsRecorder) SnapshotPublishes() []PublishDiagnostic {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PublishDiagnostic, len(r.publishes))
	copy(out, r.publishes)
	return out
}

func (r *EmittedEventsRecorder) AppendRuntimeLog(entry diaglog.RunEntry) {
	if r == nil {
		return
	}
	entry.NormalizeEntityID()
	entry.Level = diaglog.NormalizeLevel(entry.Level.String())
	entry.Message = strings.TrimSpace(entry.Message)
	entry.Component = strings.TrimSpace(entry.Component)
	if entry.Component == "" {
		entry.Component = "runtime"
	}
	entry.Action = strings.TrimSpace(entry.Action)
	if entry.Action == "" {
		entry.Action = "unknown"
	}
	r.mu.Lock()
	r.flightRecorder = append(r.flightRecorder, FlightRecorderEntry{
		Kind:       "runtime_log",
		LogLevel:   entry.Level.String(),
		Message:    entry.Message,
		Details:    cloneJSONValue(runtimeLogDetails(entry)),
		StackTrace: strings.TrimSpace(entry.StackTrace),
	})
	r.mu.Unlock()
}

func (r *EmittedEventsRecorder) SnapshotFlightRecorder() []FlightRecorderEntry {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]FlightRecorderEntry, len(r.flightRecorder))
	copy(out, r.flightRecorder)
	return out
}

func clonePublishDiagnostic(diag PublishDiagnostic) PublishDiagnostic {
	diag.EventID = strings.TrimSpace(diag.EventID)
	diag.EventType = strings.TrimSpace(diag.EventType)
	diag.EntityID = strings.TrimSpace(diag.EntityID)
	diag.ParentEventID = strings.TrimSpace(diag.ParentEventID)
	diag.RoutedRecipients = append([]PublishDiagnosticRecipient(nil), diag.RoutedRecipients...)
	diag.SubscriptionRecipients = append([]string(nil), diag.SubscriptionRecipients...)
	return diag
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneJSONValue(v any) any {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

func runtimeLogDetails(entry diaglog.RunEntry) map[string]any {
	details := map[string]any{}
	if entry.Detail != nil {
		if detailMap, ok := cloneJSONValue(entry.Detail).(map[string]any); ok {
			for k, v := range detailMap {
				key := strings.TrimSpace(k)
				if key == "" {
					continue
				}
				details[key] = v
			}
		} else {
			details["raw_detail"] = cloneJSONValue(entry.Detail)
		}
	}
	if v := strings.TrimSpace(entry.Component); v != "" {
		details["component"] = v
	}
	if v := strings.TrimSpace(entry.Action); v != "" {
		details["action"] = v
	}
	if v := strings.TrimSpace(entry.EventID); v != "" {
		details["event_id"] = v
	}
	if v := strings.TrimSpace(entry.EventType); v != "" {
		details["event_name"] = v
		details["event_type"] = v
	}
	if v := strings.TrimSpace(entry.AgentID); v != "" {
		details["agent_id"] = v
	}
	if v := strings.TrimSpace(entry.EntityID); v != "" {
		details["entity_id"] = v
	}
	if v := strings.TrimSpace(entry.SessionID); v != "" {
		details["session_id"] = v
	}
	if len(entry.Correlation) > 0 {
		details["correlation"] = cloneStringMap(entry.Correlation)
	}
	if v := strings.TrimSpace(entry.Error); v != "" {
		details["error"] = v
	}
	if entry.DurationUS > 0 {
		details["duration_us"] = entry.DurationUS
	}
	return details
}
