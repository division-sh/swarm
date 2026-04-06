package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
)

type missingPipelineReceiptReader interface {
	ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error)
}

type authoritativeDeliveryRecipientReader interface {
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
}

type pipelineReceiptRecorder interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error
}

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

type Publisher interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
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
	recorder, _ := r.store.(pipelineReceiptRecorder)
	recipients, ok := r.store.(authoritativeDeliveryRecipientReader)
	if !ok {
		return fmt.Errorf("recover pipeline receipts: missing authoritative delivery recipient reader")
	}
	var firstErr error
	for _, record := range eventsToReplay {
		evt := record.Event
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(evt.ID) == "" {
			continue
		}
		if replayErr := strings.TrimSpace(record.ReplayError); replayErr != "" {
			if recorder == nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: missing pipeline receipt recorder", evt.ID)
				}
				continue
			}
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID, "error", replayErr); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: %w", evt.ID, err)
				}
			}
			continue
		}
		persistedRecipients, err := recipients.ListEventDeliveryRecipients(ctx, evt.ID)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("load persisted recipients for replay event %s: %w", evt.ID, err)
			}
			continue
		}
		if len(persistedRecipients) == 0 {
			if recorder == nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s delivered receipt: missing pipeline receipt recorder", evt.ID)
				}
				continue
			}
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID, "processed", ""); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s delivered receipt: %w", evt.ID, err)
				}
			}
			continue
		}
		if err := r.bus.PublishDirect(ctx, evt, persistedRecipients); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("replay event %s: %w", evt.ID, err)
			}
		}
	}
	return firstErr
}
