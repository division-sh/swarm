package bus

import (
	"context"
	"sync"

	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

type emittedEventsContextKey struct{}

func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimecorrelation.InboundEventFromContext(ctx)
}

type EmittedEventsRecorder struct {
	mu        sync.Mutex
	events    []events.Event
	publishes []PublishDiagnostic
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
	r.mu.Lock()
	r.publishes = append(r.publishes, diag)
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
