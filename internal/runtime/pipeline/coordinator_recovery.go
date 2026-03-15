package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
)

type missingPipelineReceiptReader interface {
	ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.Event, error)
}

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

type Publisher interface {
	Publish(ctx context.Context, evt events.Event) error
}

type RecoveryManager struct {
	store  EventStore
	bus    Publisher
	window time.Duration
	limit  int
}

func NewRecoveryManager() *RecoveryManager {
	return &RecoveryManager{
		window: 24 * time.Hour,
		limit:  5000,
	}
}

func NewRecoveryManagerWith(store EventStore, bus Publisher) *RecoveryManager {
	rm := NewRecoveryManager()
	rm.store = store
	rm.bus = bus
	return rm
}

func (r *RecoveryManager) Recover(ctx context.Context) error {
	if r == nil || r.store == nil || r.bus == nil {
		return nil
	}
	reader, ok := r.store.(missingPipelineReceiptReader)
	if !ok {
		return nil
	}
	window := r.window
	if window <= 0 {
		window = 15 * time.Minute
	}
	limit := r.limit
	if limit <= 0 {
		limit = 500
	}
	eventsToReplay, err := reader.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-window), limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, evt := range eventsToReplay {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(evt.ID) == "" {
			continue
		}
		if err := r.bus.Publish(ctx, evt); err != nil {
			// Keep replaying remaining events; one poison/bad event should not block full recovery.
			if firstErr == nil {
				firstErr = fmt.Errorf("replay event %s: %w", evt.ID, err)
			}
		}
	}
	return firstErr
}
