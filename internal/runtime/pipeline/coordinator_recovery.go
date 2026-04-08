package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type pipelineReceiptRecorder interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error
}

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
	runtimereplayclaim.RecipientReader
}

type Publisher interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error
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
	replayStore, participates, err := runtimereplayclaim.RequireStore(r.store)
	if err != nil {
		return fmt.Errorf("recover pipeline receipts: %w", err)
	}
	if !participates {
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
	eventsToReplay, err := replayStore.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-window), limit)
	if err != nil {
		return err
	}
	recorder, _ := r.store.(pipelineReceiptRecorder)
	var firstErr error
	for _, record := range eventsToReplay {
		evt := record.Event
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(evt.ID) == "" {
			continue
		}
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("claim replay event %s: %w", evt.ID, err)
			}
			continue
		}
		if !claimed {
			continue
		}
		if replayErr := strings.TrimSpace(record.ReplayError); replayErr != "" {
			if recorder == nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: missing pipeline receipt recorder", evt.ID)
				}
				_ = lease.Release(ctx)
				continue
			}
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID, "error", replayErr); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s error receipt: %w", evt.ID, err)
				}
			}
			_ = lease.Release(ctx)
			continue
		}
		persistedRecipients, err := r.store.ListEventDeliveryRecipients(ctx, evt.ID)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("load persisted recipients for replay event %s: %w", evt.ID, err)
			}
			_ = lease.Release(ctx)
			continue
		}
		if len(persistedRecipients) == 0 {
			if recorder == nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s delivered receipt: missing pipeline receipt recorder", evt.ID)
				}
				_ = lease.Release(ctx)
				continue
			}
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID, "processed", ""); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("mark replay event %s delivered receipt: %w", evt.ID, err)
				}
			}
			_ = lease.Release(ctx)
			continue
		}
		if err := r.bus.PublishPersistedRecipients(ctx, evt, persistedRecipients); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("replay event %s: %w", evt.ID, err)
			}
			_ = lease.Release(ctx)
			continue
		}
		if recorder != nil {
			if err := recorder.UpsertPipelineReceipt(ctx, evt.ID, "processed", ""); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("mark replay event %s delivered receipt: %w", evt.ID, err)
			}
		}
		_ = lease.Release(ctx)
	}
	return firstErr
}
