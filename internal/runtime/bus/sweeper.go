package bus

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type OutboxSweeperConfig struct {
	Interval time.Duration
	Lookback time.Duration
	Limit    int
}

func DefaultOutboxSweeperConfig() OutboxSweeperConfig {
	return OutboxSweeperConfig{
		Interval: 15 * time.Second,
		Lookback: 24 * time.Hour,
		Limit:    200,
	}
}

func (eb *EventBus) StartOutboxSweeper(ctx context.Context, cfg OutboxSweeperConfig) {
	if eb == nil {
		return
	}
	if cfg.Interval <= 0 || cfg.Lookback <= 0 || cfg.Limit <= 0 {
		defaults := DefaultOutboxSweeperConfig()
		if cfg.Interval <= 0 {
			cfg.Interval = defaults.Interval
		}
		if cfg.Lookback <= 0 {
			cfg.Lookback = defaults.Lookback
		}
		if cfg.Limit <= 0 {
			cfg.Limit = defaults.Limit
		}
	}
	eb.mu.Lock()
	if eb.outboxSweeperActive {
		eb.mu.Unlock()
		return
	}
	eb.outboxSweeperActive = true
	eb.mu.Unlock()

	go func() {
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		defer func() {
			eb.mu.Lock()
			eb.outboxSweeperActive = false
			eb.mu.Unlock()
		}()
		for {
			if _, err := eb.SweepUndispatched(ctx, cfg.Lookback, cfg.Limit); err != nil {
				eb.logRuntime(ctx, "warn", "Outbox sweep failed", "eventbus", "outbox_sweep_failed", "", "", "", "", "", nil, map[string]any{
					"lookback_seconds": int(cfg.Lookback / time.Second),
					"limit":            cfg.Limit,
				}, err.Error(), 0)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (eb *EventBus) SweepUndispatched(ctx context.Context, lookback time.Duration, limit int) (int, error) {
	if eb == nil || eb.store == nil {
		return 0, nil
	}
	paused, err := eb.runtimeIngressDispatchPaused(ctx, events.NewRouteProbeEvent(events.EventType("__runtime_ingress_probe__")))
	if err != nil {
		return 0, err
	}
	if paused {
		return 0, nil
	}
	replayStore, participates, err := runtimereplayclaim.RequireStore(eb.store)
	if err != nil {
		return 0, err
	}
	if !participates {
		return 0, nil
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	if lookback <= 0 {
		lookback = DefaultOutboxSweeperConfig().Lookback
	}
	events, err := replayStore.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-lookback), limit)
	if err != nil {
		return 0, err
	}
	redelivered := 0
	for _, record := range events {
		evt := record.Event
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID())
		if err != nil {
			return redelivered, err
		}
		if !claimed {
			continue
		}
		if replayErr := strings.TrimSpace(record.ReplayError); replayErr != "" {
			eb.markPipelineReceipt(ctx, evt.ID(), "error", replayErr)
			_ = lease.Release(ctx)
			continue
		}
		recipients, err := eb.authoritativeRecipientsForEvent(ctx, evt.ID())
		if err != nil {
			eb.markPipelineReceipt(ctx, evt.ID(), "error", err.Error())
			_ = lease.Release(ctx)
			return redelivered, err
		}
		if err := eb.PublishPersistedRecipients(ctx, evt, recipients); err != nil {
			if errors.Is(err, ErrRuntimeIngressPaused) || errors.Is(err, ErrRunDispatchBlocked) {
				_ = lease.Release(ctx)
				if errors.Is(err, ErrRuntimeIngressPaused) {
					return redelivered, nil
				}
				continue
			}
			if errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) {
				if recordErr := eb.markCommittedReplayScopeUnavailable(ctx, evt, err); recordErr != nil {
					_ = lease.Release(ctx)
					return redelivered, recordErr
				}
				_ = lease.Release(ctx)
				continue
			}
			if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				if recordErr := eb.markPipelineReceipt(ctx, evt.ID(), "error", err.Error()); recordErr != nil {
					_ = lease.Release(ctx)
					return redelivered, recordErr
				}
			}
			_ = lease.Release(ctx)
			return redelivered, err
		}
		eb.markPipelineReceipt(ctx, evt.ID(), "processed", "")
		_ = lease.Release(ctx)
		redelivered++
	}
	return redelivered, nil
}

func (eb *EventBus) ReleaseRuntimeIngressQueue(ctx context.Context, lookback time.Duration, limit int) (int, error) {
	return eb.SweepUndispatched(ctx, lookback, limit)
}

func (eb *EventBus) ReleaseRunQueue(ctx context.Context, runID string, lookback time.Duration, limit int) (int, error) {
	if eb == nil || eb.store == nil {
		return 0, nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return 0, nil
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	if lookback <= 0 {
		lookback = DefaultOutboxSweeperConfig().Lookback
	}
	replayStore, participates, err := runtimereplayclaim.RequireStore(eb.store)
	if err != nil {
		return 0, err
	}
	if !participates {
		return 0, nil
	}
	lister, ok := eb.store.(interface {
		ListEventsMissingPipelineReceiptForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	})
	if !ok || lister == nil {
		return 0, nil
	}
	records, err := lister.ListEventsMissingPipelineReceiptForRun(ctx, runID, time.Now().Add(-lookback), limit)
	if err != nil {
		return 0, err
	}
	redelivered := 0
	for _, record := range records {
		evt := record.Event
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID())
		if err != nil {
			return redelivered, err
		}
		if !claimed {
			continue
		}
		if replayErr := strings.TrimSpace(record.ReplayError); replayErr != "" {
			eb.markPipelineReceipt(ctx, evt.ID(), "error", replayErr)
			_ = lease.Release(ctx)
			continue
		}
		recipients, err := eb.authoritativeRecipientsForEvent(ctx, evt.ID())
		if err != nil {
			eb.markPipelineReceipt(ctx, evt.ID(), "error", err.Error())
			_ = lease.Release(ctx)
			return redelivered, err
		}
		if err := eb.publishPersistedRecipients(ctx, evt, recipients, true); err != nil {
			_ = lease.Release(ctx)
			if errors.Is(err, ErrRunDispatchBlocked) {
				return redelivered, nil
			}
			return redelivered, err
		}
		eb.markPipelineReceipt(ctx, evt.ID(), "processed", "")
		_ = lease.Release(ctx)
		redelivered++
	}
	return redelivered, nil
}

func (eb *EventBus) markCommittedReplayScopeUnavailable(ctx context.Context, evt events.Event, cause error) error {
	errText := strings.TrimSpace(cause.Error())
	if errText == "" {
		errText = runtimereplayclaim.ErrMissingCommittedReplayScope.Error()
	}
	if err := eb.markPipelineReceipt(ctx, evt.ID(), "error", errText); err != nil {
		return err
	}
	eb.logRuntime(ctx, "warn", "Persisted event replay skipped because committed replay scope is unavailable", "eventbus", "outbox_replay_scope_unavailable", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, map[string]any{
		"reason":          "missing_committed_replay_scope",
		"parent_event_id": strings.TrimSpace(evt.ParentEventID()),
	}, errText, 0)
	return nil
}

func (eb *EventBus) authoritativeRecipientsForEvent(ctx context.Context, eventID string) ([]string, error) {
	if !runtimereplayclaim.SupportsPersistedReplay(eb.store) {
		return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
	}
	recipients, err := eb.store.ListEventDeliveryRecipients(ctx, eventID)
	if err != nil {
		return nil, err
	}
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}
	if recipients == nil {
		return []string{}, nil
	}
	return uniqueStrings(recipients), nil
}
