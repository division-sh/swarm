package bus

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
				}, eventBusDependencyFailure(err, "outbox_sweep_failed", "sweep_outbox"), 0)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (eb *EventBus) WaitForOutboxSweeper(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		eb.mu.RLock()
		active := eb.outboxSweeperActive
		eb.mu.RUnlock()
		if !active {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (eb *EventBus) OutboxSweeperActive() bool {
	if eb == nil {
		return false
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.outboxSweeperActive
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
	decisionRoutes, err := eb.sweepDecisionRouteObligations(ctx, limit)
	if err != nil {
		return decisionRoutes, err
	}
	events, err := replayStore.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-lookback), limit)
	if err != nil {
		return decisionRoutes, err
	}
	redelivered := decisionRoutes
	for _, record := range events {
		evt := record.Event
		if eb.eventPublishInFlight(evt.ID()) {
			continue
		}
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID())
		if err != nil {
			return redelivered, err
		}
		if !claimed {
			continue
		}
		workCtx := runtimereplayclaim.BindLeaseContext(ctx, lease)
		if record.ReplayFailure != nil {
			eb.markPipelineReceipt(workCtx, evt.ID(), "error", runtimefailures.CloneEnvelope(record.ReplayFailure))
			_ = lease.Release(workCtx)
			continue
		}
		recipients, err := eb.authoritativeRecipientsForEvent(workCtx, evt.ID())
		if err != nil {
			eb.markPipelineReceipt(workCtx, evt.ID(), "error", eventBusFailure(err, "load_replay_recipients"))
			_ = lease.Release(workCtx)
			return redelivered, err
		}
		if err := eb.RecoverPersistedPipeline(workCtx, evt, recipients); err != nil {
			if errors.Is(err, ErrRuntimeIngressPaused) || errors.Is(err, ErrRunDispatchBlocked) {
				_ = lease.Release(workCtx)
				if errors.Is(err, ErrRuntimeIngressPaused) {
					return redelivered, nil
				}
				continue
			}
			if runtimepipeline.IsPipelineReceiptDeferred(err) {
				_ = eb.deferDecisionRouteObligation(workCtx, evt.ID(), err)
				_ = lease.Release(workCtx)
				continue
			}
			if errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) {
				if recordErr := eb.markCommittedReplayScopeUnavailable(workCtx, evt, err); recordErr != nil {
					_ = lease.Release(workCtx)
					return redelivered, recordErr
				}
				_ = lease.Release(workCtx)
				continue
			}
			if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				if recordErr := eb.markPipelineReceipt(workCtx, evt.ID(), "error", eventBusFailure(err, "publish_replay")); recordErr != nil {
					_ = lease.Release(workCtx)
					return redelivered, recordErr
				}
			}
			_ = lease.Release(workCtx)
			return redelivered, err
		}
		eb.markPipelineReceipt(workCtx, evt.ID(), "processed", nil)
		_ = lease.Release(workCtx)
		redelivered++
	}
	return redelivered, nil
}

func (eb *EventBus) sweepDecisionRouteObligations(ctx context.Context, limit int) (int, error) {
	obligations, ok := eb.store.(runtimepipeline.DecisionRouteObligationStore)
	if !ok || obligations == nil {
		return 0, nil
	}
	records, err := obligations.ListDueDecisionRouteObligations(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	recovered := 0
	settlementOwner, ok := eb.store.(runtimereplayclaim.SettlementOwner)
	if !ok || settlementOwner == nil {
		return 0, errors.New("decision route settlement claim owner is required")
	}
	for _, record := range records {
		evt := record.Event
		if eb.eventPublishInFlight(evt.ID()) {
			continue
		}
		lease, claimed, err := settlementOwner.ClaimPipelineSettlement(ctx, evt.ID())
		if err != nil {
			return recovered, err
		}
		if !claimed {
			continue
		}
		workCtx := runtimereplayclaim.BindLeaseContext(ctx, lease)
		if settled, err := eb.settleProcessedDecisionRouteIfPresent(workCtx, evt); settled || err != nil {
			if err != nil {
				if deferErr := eb.deferDecisionRouteObligation(workCtx, evt.ID(), err); deferErr != nil {
					_ = lease.Release(workCtx)
					return recovered, deferErr
				}
				_ = lease.Release(workCtx)
				continue
			}
			_ = lease.Release(workCtx)
			recovered++
			continue
		}
		recipients, err := eb.authoritativeRecipientsForEvent(workCtx, evt.ID())
		if err == nil {
			err = eb.RecoverPersistedPipeline(workCtx, evt, recipients)
		}
		if runtimepipeline.IsPipelineReceiptDeferred(err) {
			_ = eb.deferDecisionRouteObligation(workCtx, evt.ID(), err)
			_ = lease.Release(workCtx)
			continue
		}
		if err != nil {
			if quarantineErr := eb.QuarantineRecoveredPipelineEvent(workCtx, evt, err); quarantineErr != nil {
				_ = lease.Release(workCtx)
				return recovered, quarantineErr
			}
			_ = lease.Release(workCtx)
			continue
		}
		if err := eb.SettleRecoveredPipelineEvent(workCtx, evt); err != nil {
			if deferErr := eb.deferDecisionRouteObligation(workCtx, evt.ID(), err); deferErr != nil {
				_ = lease.Release(workCtx)
				return recovered, deferErr
			}
			_ = lease.Release(workCtx)
			continue
		}
		_ = lease.Release(workCtx)
		recovered++
	}
	return recovered, nil
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
	since := time.Now().Add(-lookback)
	redelivered := 0
	seen := map[string]struct{}{}
	if lister, ok := eb.store.(interface {
		ListEventsWithPendingDeliveriesForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	}); ok && lister != nil {
		records, err := lister.ListEventsWithPendingDeliveriesForRun(ctx, runID, since, limit)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			evt := record.Event
			eventID := strings.TrimSpace(evt.ID())
			if eventID == "" {
				continue
			}
			seen[eventID] = struct{}{}
			if record.ReplayFailure != nil {
				eb.markPipelineReceipt(ctx, eventID, "error", runtimefailures.CloneEnvelope(record.ReplayFailure))
				continue
			}
			recipients, err := eb.authoritativeRecipientsForEvent(ctx, eventID)
			if err != nil {
				eb.markPipelineReceipt(ctx, eventID, "error", eventBusFailure(err, "load_replay_recipients"))
				return redelivered, err
			}
			if err := eb.publishPersistedRecipients(ctx, evt, recipients, true); err != nil {
				if errors.Is(err, ErrRunDispatchBlocked) {
					return redelivered, nil
				}
				return redelivered, err
			}
			eb.markPipelineReceipt(ctx, eventID, "processed", nil)
			redelivered++
		}
	}
	lister, ok := eb.store.(interface {
		ListEventsMissingPipelineReceiptForRun(context.Context, string, time.Time, int) ([]events.PersistedReplayEvent, error)
	})
	if !ok || lister == nil {
		return redelivered, nil
	}
	records, err := lister.ListEventsMissingPipelineReceiptForRun(ctx, runID, since, limit)
	if err != nil {
		return redelivered, err
	}
	for _, record := range records {
		evt := record.Event
		eventID := strings.TrimSpace(evt.ID())
		if eventID == "" {
			continue
		}
		if _, exists := seen[eventID]; exists {
			continue
		}
		lease, claimed, err := replayStore.ClaimPipelineReplay(ctx, evt.ID())
		if err != nil {
			return redelivered, err
		}
		if !claimed {
			continue
		}
		workCtx := runtimereplayclaim.BindLeaseContext(ctx, lease)
		if record.ReplayFailure != nil {
			eb.markPipelineReceipt(workCtx, evt.ID(), "error", runtimefailures.CloneEnvelope(record.ReplayFailure))
			_ = lease.Release(workCtx)
			continue
		}
		recipients, err := eb.authoritativeRecipientsForEvent(workCtx, evt.ID())
		if err != nil {
			eb.markPipelineReceipt(workCtx, evt.ID(), "error", eventBusFailure(err, "load_replay_recipients"))
			_ = lease.Release(workCtx)
			return redelivered, err
		}
		if err := eb.publishPersistedRecipients(workCtx, evt, recipients, true); err != nil {
			_ = lease.Release(workCtx)
			if errors.Is(err, ErrRunDispatchBlocked) {
				return redelivered, nil
			}
			return redelivered, err
		}
		eb.markPipelineReceipt(workCtx, evt.ID(), "processed", nil)
		_ = lease.Release(workCtx)
		redelivered++
	}
	return redelivered, nil
}

func (eb *EventBus) markCommittedReplayScopeUnavailable(ctx context.Context, evt events.Event, cause error) error {
	canonical := runtimefailures.Normalize(runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "committed_replay_scope_missing", "eventbus", "load_committed_replay_scope", map[string]any{
		"event_id": evt.ID(), "event_type": string(evt.Type()),
	}, cause), "eventbus", "load_committed_replay_scope")
	failure := &canonical
	if err := eb.markPipelineReceipt(ctx, evt.ID(), "error", failure); err != nil {
		return err
	}
	eb.logRuntime(ctx, "warn", "Persisted event replay skipped because committed replay scope is unavailable", "eventbus", "outbox_replay_scope_unavailable", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, map[string]any{
		"reason":          "missing_committed_replay_scope",
		"parent_event_id": strings.TrimSpace(evt.ParentEventID()),
	}, failure, 0)
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
