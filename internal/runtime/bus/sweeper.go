package bus

import (
	"context"
	"strings"
	"time"

	runtimeengine "swarm/internal/runtime/engine"
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
	reader, ok := eb.store.(PipelineReceiptSweeperStore)
	if !ok {
		return 0, nil
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	if lookback <= 0 {
		lookback = DefaultOutboxSweeperConfig().Lookback
	}
	events, err := reader.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-lookback), limit)
	if err != nil {
		return 0, err
	}
	dispatcher := engineDispatcher{bus: eb}
	redelivered := 0
	for _, record := range events {
		evt := record.Event
		if replayErr := strings.TrimSpace(record.ReplayError); replayErr != "" {
			eb.markPipelineReceipt(ctx, evt.ID, "error", replayErr)
			continue
		}
		recipients, err := eb.sweeperRecipients(ctx, evt.ID)
		if err != nil {
			eb.markPipelineReceipt(ctx, evt.ID, "error", err.Error())
			return redelivered, err
		}
		intent := runtimeengine.EmitIntent{Event: evt, Recipients: recipients}
		if err := dispatcher.dispatchIntent(ctx, intent); err != nil {
			eb.markPipelineReceipt(ctx, evt.ID, "error", err.Error())
			return redelivered, err
		}
		eb.markPipelineReceipt(ctx, evt.ID, "processed", "")
		redelivered++
	}
	return redelivered, nil
}

func (eb *EventBus) sweeperRecipients(ctx context.Context, eventID string) ([]string, error) {
	reader, ok := eb.store.(EventDeliveryReader)
	if !ok {
		return nil, nil
	}
	recipients, err := reader.ListEventDeliveryRecipients(ctx, eventID)
	if err != nil {
		return nil, err
	}
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}
	return uniqueStrings(recipients), nil
}
