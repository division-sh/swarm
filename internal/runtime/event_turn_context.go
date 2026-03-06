package runtime

import (
	"context"

	runtimebus "empireai/internal/runtime/bus"

	"empireai/internal/events"
)

func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	return runtimebus.WithInboundEvent(ctx, evt)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimebus.InboundEventFromContext(ctx)
}

func NewEmittedEventsRecorder() *EmittedEventsRecorder {
	return runtimebus.NewEmittedEventsRecorder()
}

func WithEmittedEventsRecorder(ctx context.Context, rec *EmittedEventsRecorder) context.Context {
	return runtimebus.WithEmittedEventsRecorder(ctx, rec)
}

func EmittedEventsRecorderFromContext(ctx context.Context) (*EmittedEventsRecorder, bool) {
	return runtimebus.EmittedEventsRecorderFromContext(ctx)
}
