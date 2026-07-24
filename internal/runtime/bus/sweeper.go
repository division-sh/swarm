package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

const (
	startupRecoveryPipelineReplayAction = "startup_recovery_pipeline_replay_aftermath"

	startupRecoveryPipelineReplayOutcomeReplayed = "replayed"
	startupRecoveryPipelineReplayOutcomeSkipped  = "skipped"
	startupRecoveryPipelineReplayOutcomeDropped  = "dropped"

	startupRecoveryPipelineReplayReasonReplayed              = "persisted_recipients_replayed"
	startupRecoveryPipelineReplayReasonNoPersistedRecipients = "no_persisted_recipients"
	startupRecoveryPipelineReplayReasonQuarantined           = "replay_quarantined"
)

type OutboxSweeperConfig struct {
	Interval time.Duration
	Limit    int
}

func DefaultOutboxSweeperConfig() OutboxSweeperConfig {
	return OutboxSweeperConfig{
		Interval: 15 * time.Second,
		Limit:    200,
	}
}

func (eb *EventBus) StartOutboxSweeper(ctx context.Context, cfg OutboxSweeperConfig) error {
	if eb == nil {
		return nil
	}
	if cfg.Interval <= 0 || cfg.Limit <= 0 {
		defaults := DefaultOutboxSweeperConfig()
		if cfg.Interval <= 0 {
			cfg.Interval = defaults.Interval
		}
		if cfg.Limit <= 0 {
			cfg.Limit = defaults.Limit
		}
	}
	eb.mu.Lock()
	if eb.outboxSweeperActive {
		eb.mu.Unlock()
		return nil
	}
	if eb.workOwner == nil {
		eb.mu.Unlock()
		return errors.New("outbox sweeper requires a runtime work occurrence")
	}
	lease, err := eb.workOwner.Begin(ctx)
	if err != nil {
		eb.mu.Unlock()
		return fmt.Errorf("admit outbox sweeper: %w", err)
	}
	eb.outboxSweeperActive = true
	done := make(chan struct{})
	eb.outboxSweeperDone = done
	eb.mu.Unlock()

	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		defer func() {
			eb.mu.Lock()
			eb.outboxSweeperActive = false
			eb.mu.Unlock()
		}()
		workCtx := lease.Context()
		for {
			if _, err := eb.SweepUndispatched(workCtx, cfg.Limit); err != nil {
				eb.logRuntime(workCtx, "warn", "Outbox sweep failed", "eventbus", "outbox_sweep_failed", "", "", "", "", "", nil, map[string]any{
					"limit": cfg.Limit,
				}, eventBusDependencyFailure(err, "outbox_sweep_failed", "sweep_outbox"), 0)
			}
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (eb *EventBus) WaitForOutboxSweeper(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	done := eb.outboxSweeperDone
	eb.mu.RUnlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func (eb *EventBus) SweepUndispatched(ctx context.Context, limit int) (int, error) {
	if eb == nil || eb.store == nil {
		return 0, nil
	}
	eb.mu.RLock()
	ingressGate := eb.runtimeIngressDispatchGate
	eb.mu.RUnlock()
	paused := false
	var err error
	if ingressGate != nil {
		paused, err = ingressGate.QueueableIngressPaused(ctx)
	}
	if err != nil {
		return 0, err
	}
	if paused {
		return 0, nil
	}
	if eb.pipelineObligations == nil {
		return 0, errors.New("pipeline obligation owner is required")
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	decisionRoutes, err := eb.sweepPipelineObligations(ctx, runtimepipelineobligation.DecisionRouteQuery(), limit)
	if err != nil {
		return decisionRoutes, err
	}
	recovered, err := eb.sweepPipelineObligations(ctx, runtimepipelineobligation.GlobalRecoveryQuery(), limit)
	return decisionRoutes + recovered, err
}

func (eb *EventBus) sweepPipelineObligations(ctx context.Context, query runtimepipelineobligation.ClaimQuery, limit int) (int, error) {
	processed := 0
	for processed < limit {
		work, ok, err := eb.pipelineObligations.ClaimNext(ctx, query)
		if err != nil {
			return processed, err
		}
		if !ok {
			return processed, nil
		}
		settled, err := eb.processClaimedPipelineWork(ctx, work)
		if err != nil {
			if errors.Is(err, ErrRuntimeIngressPaused) || errors.Is(err, ErrRunDispatchBlocked) {
				return processed, nil
			}
			return processed, err
		}
		if settled {
			processed++
		}
	}
	return processed, nil
}

func (eb *EventBus) processClaimedPipelineWork(ctx context.Context, work runtimepipelineobligation.ClaimedWork) (settled bool, err error) {
	claimOpen := true
	defer func() {
		if claimOpen {
			err = errors.Join(err, eb.pipelineObligations.Release(context.WithoutCancel(ctx), work.Claim))
		}
	}()
	if work.Claim.Purpose() == runtimepipelineobligation.PurposeDecisionRoute && work.Acknowledged {
		err = eb.settleClaimedDecisionRoute(ctx, work)
		claimOpen = err != nil
		return err == nil, err
	}
	if disposition, preclassified := work.PreDispatchDisposition(); preclassified {
		if err := eb.pipelineObligations.Settle(ctx, work.Claim, disposition); err != nil {
			return false, err
		}
		claimOpen = false
		eb.logStartupRecoveryPipelineAftermath(
			ctx,
			work.Event,
			startupRecoveryPipelineReplayOutcomeDropped,
			startupRecoveryPipelineReplayReasonQuarantined,
			disposition.Failure(),
			nil,
		)
		return true, nil
	}
	recipients, dispatchErr := eb.authoritativeRecipientsForEvent(ctx, work.Event.ID())
	var outcome runtimepipelineobligation.ExecutionOutcome
	if dispatchErr == nil {
		outcome, dispatchErr = eb.RecoverPersistedPipeline(ctx, work, recipients)
	}
	if dispatchErr != nil {
		if errors.Is(dispatchErr, ErrRuntimeIngressPaused) || errors.Is(dispatchErr, ErrRunDispatchBlocked) || errors.Is(dispatchErr, errAuthoritativeDeliveryIncomplete) {
			return false, dispatchErr
		}
		failure := eventBusFailure(dispatchErr, "recover_pipeline_obligation")
		disposition := runtimepipelineobligation.Terminal("pipeline_recovery_failed", failure)
		if work.Claim.Purpose() == runtimepipelineobligation.PurposeDecisionRoute {
			disposition = runtimepipelineobligation.Quarantined(
				pipelineDispositionFailureReason("decision_route_recovery_failed", failure),
				failure,
			)
		}
		if err := eb.pipelineObligations.Settle(ctx, work.Claim, disposition); err != nil {
			return false, errors.Join(dispatchErr, err)
		}
		claimOpen = false
		eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeDropped, startupRecoveryPipelineReplayReasonQuarantined, disposition.Failure(), recipients)
		return true, nil
	}
	if disposition, ok := outcome.Disposition(); ok {
		if err := eb.pipelineObligations.Settle(ctx, work.Claim, disposition); err != nil {
			return false, err
		}
		claimOpen = false
		if disposition.Terminal() {
			eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeDropped, startupRecoveryPipelineReplayReasonQuarantined, disposition.Failure(), recipients)
		}
		return true, nil
	}
	if work.Claim.Purpose() == runtimepipelineobligation.PurposeDecisionRoute {
		if err := eb.pipelineObligations.MarkDecisionProcessed(ctx, work.Claim); err != nil {
			return false, err
		}
		err = eb.settleClaimedDecisionRoute(ctx, work)
		claimOpen = err != nil
		return err == nil, err
	}
	if err := eb.pipelineObligations.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		return false, err
	}
	claimOpen = false
	if work.Scope == runtimepipelineobligation.ScopeDirect && len(recipients) == 0 {
		eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeSkipped, startupRecoveryPipelineReplayReasonNoPersistedRecipients, nil, nil)
	} else {
		eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeReplayed, startupRecoveryPipelineReplayReasonReplayed, nil, recipients)
	}
	if err := eb.ConvergeNormalRunCompletionForEvent(ctx, work.Event.ID()); err != nil {
		return true, err
	}
	return true, nil
}

func (eb *EventBus) logStartupRecoveryPipelineAftermath(
	ctx context.Context,
	event events.Event,
	outcome string,
	reason string,
	failure *runtimefailures.Envelope,
	recipients []string,
) {
	if !runtimepipelineobligation.StartupRecoveryDiagnosticsEnabled(ctx) || event.Type() == events.EventTypePlatformRuntimeLog {
		return
	}
	recipients = uniqueStrings(recipients)
	level := diaglog.LevelInfo
	message := "Startup recovery replayed persisted pipeline event"
	switch outcome {
	case startupRecoveryPipelineReplayOutcomeDropped:
		level = diaglog.LevelWarn
		message = "Startup recovery dropped persisted pipeline replay"
	case startupRecoveryPipelineReplayOutcomeSkipped:
		message = "Startup recovery skipped persisted pipeline replay"
	}
	detail := map[string]any{
		"decision_family":           "startup_pipeline_replay",
		"decision_outcome":          strings.TrimSpace(outcome),
		"decision_reason_code":      strings.TrimSpace(reason),
		"event_id":                  strings.TrimSpace(event.ID()),
		"event_type":                strings.TrimSpace(string(event.Type())),
		"persisted_run_id":          strings.TrimSpace(event.RunID()),
		"parent_event_id":           strings.TrimSpace(event.ParentEventID()),
		"entity_id":                 event.EntityID(),
		"flow_instance":             event.FlowInstance(),
		"persisted_recipient_count": len(recipients),
	}
	if len(recipients) > 0 {
		detail["persisted_recipients"] = append([]string(nil), recipients...)
	}
	logCtx := runtimecorrelation.WithRunID(ctx, event.RunID())
	_ = eb.LogRuntime(logCtx, startupRecoveryPipelineLogEntry(
		level,
		message,
		startupRecoveryPipelineReplayAction,
		event,
		detail,
		failure,
	))
}

func startupRecoveryPipelineLogEntry(
	level diaglog.Level,
	message string,
	action string,
	event events.Event,
	detail map[string]any,
	failure *runtimefailures.Envelope,
) runtimepipeline.RuntimeLogEntry {
	return runtimepipeline.RuntimeLogEntry{
		Level: level, Message: message, Component: "pipeline-recovery", Action: action,
		EventID: strings.TrimSpace(event.ID()), EventType: strings.TrimSpace(string(event.Type())),
		EntityID: event.EntityID(), Detail: detail, Failure: runtimefailures.CloneEnvelope(failure),
	}
}

func (eb *EventBus) settleClaimedDecisionRoute(ctx context.Context, work runtimepipelineobligation.ClaimedWork) error {
	if err := eb.ConvergeNormalRunCompletionForEvent(ctx, work.Event.ID()); err != nil {
		failure := eventBusDependencyFailure(err, "decision_route_convergence_failed", "converge_decision_route")
		return eb.pipelineObligations.Settle(ctx, work.Claim, runtimepipelineobligation.Deferred(
			"decision_route_convergence_failed",
			time.Now().UTC().Add(runtimepipelineobligation.DecisionRouteRetryDelay),
			failure,
		))
	}
	return eb.pipelineObligations.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("decision_route_converged"))
}

func (eb *EventBus) ReleaseRuntimeIngressQueue(ctx context.Context, limit int) (int, error) {
	return eb.SweepUndispatched(ctx, limit)
}

// ReleaseRunQueue owns only the #2106 half. Executable delivery backlog is
// continuously recovered by #2105's agent/node owners and is not republished
// or acknowledged through this pipeline operation.
func (eb *EventBus) ReleaseRunQueue(ctx context.Context, runID string, limit int) (int, error) {
	if eb == nil || eb.pipelineObligations == nil {
		return 0, nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return 0, nil
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	return eb.sweepPipelineObligations(ctx, runtimepipelineobligation.RunRecoveryQuery(runID), limit)
}

func (eb *EventBus) authoritativeRecipientsForEvent(ctx context.Context, eventID string) ([]string, error) {
	if eb == nil || eb.store == nil {
		return nil, errors.New("authoritative recipient store is required")
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
