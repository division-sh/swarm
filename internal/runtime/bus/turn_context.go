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
	mu     sync.Mutex
	events []events.Event
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
