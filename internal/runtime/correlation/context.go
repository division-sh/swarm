package correlation

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"swarm/internal/events"
)

type inboundEventContextKey struct{}
type runIDContextKey struct{}
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

func WithRunID(ctx context.Context, runID string) context.Context {
	if ctx == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ctx
	}
	return context.WithValue(ctx, runIDContextKey{}, runID)
}

func RunIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	runID, _ := ctx.Value(runIDContextKey{}).(string)
	return strings.TrimSpace(runID)
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
	runID := strings.TrimSpace(evt.RunID)
	if runID == "" {
		runID = RunIDFromContext(ctx)
	}
	if runID == "" {
		if inbound, ok := InboundEventFromContext(ctx); ok {
			runID = strings.TrimSpace(inbound.RunID)
		}
	}
	if runID == "" {
		runID = uuid.NewString()
	}
	evt.RunID = runID
	ctx = WithRunID(ctx, runID)

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
