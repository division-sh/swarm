package correlation

import (
	"context"
	"strings"

	"swarm/internal/events"
	"github.com/google/uuid"
)

type inboundEventContextKey struct{}
type traceIDContextKey struct{}
type handlerIDContextKey struct{}

func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, inboundEventContextKey{}, evt)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	if ctx == nil {
		return events.Event{}, false
	}
	v := ctx.Value(inboundEventContextKey{})
	if v == nil {
		return events.Event{}, false
	}
	evt, ok := v.(events.Event)
	return evt, ok
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		return nil
	}
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	traceID, _ := ctx.Value(traceIDContextKey{}).(string)
	return strings.TrimSpace(traceID)
}

func WithHandlerID(ctx context.Context, handlerID string) context.Context {
	if ctx == nil {
		return nil
	}
	handlerID = strings.TrimSpace(handlerID)
	if handlerID == "" {
		return ctx
	}
	return context.WithValue(ctx, handlerIDContextKey{}, handlerID)
}

func HandlerIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	handlerID, _ := ctx.Value(handlerIDContextKey{}).(string)
	return strings.TrimSpace(handlerID)
}

func CorrelateEvent(ctx context.Context, evt events.Event) (context.Context, events.Event) {
	traceID := strings.TrimSpace(evt.TraceID)
	if traceID == "" {
		traceID = TraceIDFromContext(ctx)
	}
	if traceID == "" {
		if inbound, ok := InboundEventFromContext(ctx); ok {
			traceID = strings.TrimSpace(inbound.TraceID)
		}
	}
	if traceID == "" {
		traceID = uuid.NewString()
	}
	evt.TraceID = traceID
	ctx = WithTraceID(ctx, traceID)

	if strings.TrimSpace(evt.ParentEventID) == "" {
		if inbound, ok := InboundEventFromContext(ctx); ok {
			parentID := strings.TrimSpace(inbound.ID)
			if parentID != "" && parentID != strings.TrimSpace(evt.ID) {
				evt.ParentEventID = parentID
			}
		}
	}
	return ctx, evt
}
