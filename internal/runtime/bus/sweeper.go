package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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

var errStandingRestartParked = errors.New("standing restart disposition is non-executable")

type OutboxSweeperConfig struct {
	Interval time.Duration
	Limit    int
}

type pipelineSweepScan struct {
	cursor         runtimepipelineobligation.Scan
	locallyBlocked bool
}

type boundedPipelineRetry struct {
	claim         runtimepipelineobligation.Claim
	standingLease *worklifetime.Lease
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
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	closeLease := true
	defer func() {
		if closeLease {
			_ = lease.Done()
		}
	}()
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
	eb.outboxSweeperActive = true
	done := make(chan struct{})
	eb.outboxSweeperDone = done
	eb.mu.Unlock()
	closeLease = false

	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		defer func() { _ = eb.closeAllPipelineScans(context.WithoutCancel(ctx)) }()
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		defer func() {
			eb.mu.Lock()
			eb.outboxSweeperActive = false
			eb.mu.Unlock()
		}()
		workCtx := ctx
		for {
			if _, err := eb.SweepPipelineObligations(workCtx, cfg.Limit); err != nil {
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
		return eb.closeAllPipelineScans(context.Background())
	}
	select {
	case <-done:
		return eb.closeAllPipelineScans(context.Background())
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

func (eb *EventBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	if eb == nil || eb.store == nil {
		return runtimepipelineobligation.SweepResult{}, errors.New("event bus and event store are required")
	}
	var err error
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return runtimepipelineobligation.SweepResult{}, err
	}
	eb.mu.RLock()
	ingressGate := eb.runtimeIngressDispatchGate
	eb.mu.RUnlock()
	paused := false
	if ingressGate != nil {
		paused, err = ingressGate.QueueableIngressPaused(ctx)
	}
	if err != nil {
		closeErr := eb.closePipelineScan(context.WithoutCancel(ctx), runtimepipelineobligation.GlobalScanRequest().WithExecutionPosture(eb.executionPosture))
		return runtimepipelineobligation.SweepResult{}, errors.Join(err, closeErr)
	}
	if paused {
		if runtimepipelineobligation.StartupRecoveryDiagnosticsEnabled(ctx) {
			slog.WarnContext(ctx, "startup pipeline recovery blocked", "reason", "ingress_paused_before_scan")
		}
		if err := eb.closePipelineScan(context.WithoutCancel(ctx), runtimepipelineobligation.GlobalScanRequest().WithExecutionPosture(eb.executionPosture)); err != nil {
			return runtimepipelineobligation.SweepResult{}, err
		}
		return runtimepipelineobligation.SweepResult{Blocked: true}, nil
	}
	if eb.pipelineObligations == nil {
		return runtimepipelineobligation.SweepResult{}, errors.New("pipeline obligation owner is required")
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	return eb.sweepPipelineObligations(ctx, runtimepipelineobligation.GlobalScanRequest(), limit)
}

func (eb *EventBus) sweepPipelineObligations(ctx context.Context, request runtimepipelineobligation.ScanRequest, limit int) (result runtimepipelineobligation.SweepResult, err error) {
	request = request.WithExecutionPosture(eb.executionPosture)
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return result, err
	}
	if err := eb.pipelineSweepMu.acquire(ctx); err != nil {
		return result, err
	}
	defer eb.pipelineSweepMu.release()
	if eb.pipelineScans == nil {
		eb.pipelineScans = map[runtimepipelineobligation.ScanRequest]*pipelineSweepScan{}
	}
	state := eb.pipelineScans[request]
	if state == nil {
		cursor, openErr := eb.pipelineObligations.OpenScan(ctx, request)
		if openErr != nil {
			return result, openErr
		}
		state = &pipelineSweepScan{cursor: cursor}
		eb.pipelineScans[request] = state
	}
	boundedRetries := make([]boundedPipelineRetry, 0, limit)
	defer func() {
		releaseCtx := context.WithoutCancel(ctx)
		for _, retry := range boundedRetries {
			releaseErr := eb.pipelineObligations.Release(releaseCtx, retry.claim)
			if !errors.Is(releaseErr, runtimepipelineobligation.ErrStaleClaim) {
				err = errors.Join(err, releaseErr)
			}
			if retry.standingLease != nil {
				err = errors.Join(err, retry.standingLease.Done())
			}
		}
	}()
	for result.Examined < limit {
		batch, batchErr := eb.pipelineObligations.ClaimBatch(ctx, state.cursor, limit-result.Examined)
		if batchErr != nil {
			closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
			return result, errors.Join(batchErr, closeErr)
		}
		if len(batch.Work) > 1 {
			closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
			return result, errors.Join(errors.New("pipeline scan returned more than one mutation-ordered work item"), closeErr)
		}
		if batch.Examined == 0 && !batch.Exhausted {
			closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
			return result, errors.Join(errors.New("pipeline scan made no progress without exhaustion"), closeErr)
		}
		result.Examined += batch.Examined
		state.locallyBlocked = state.locallyBlocked || batch.LocallyBlocked
		for _, work := range batch.Work {
			settled, retry, standingLease, processErr := eb.processClaimedPipelineWork(ctx, work)
			if retry {
				state.locallyBlocked = true
				boundedRetries = append(boundedRetries, boundedPipelineRetry{
					claim:         work.Claim,
					standingLease: standingLease,
				})
			}
			if settled {
				result.Settled++
			}
			if processErr != nil {
				if settled || retry {
					closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
					return result, errors.Join(processErr, closeErr)
				}
				if errors.Is(processErr, errStandingRestartParked) {
					continue
				}
				if errors.Is(processErr, ErrRunDispatchBlocked) {
					if runtimepipelineobligation.StartupRecoveryDiagnosticsEnabled(ctx) {
						slog.WarnContext(ctx, "startup pipeline recovery blocked", "reason", "run_dispatch_blocked", "event_id", work.Event.ID(), "event_type", work.Event.Type(), "run_id", work.Event.RunID(), "purpose", work.Claim.Purpose(), "error", processErr)
					}
					state.locallyBlocked = true
					continue
				}
				if errors.Is(processErr, ErrRuntimeIngressPaused) {
					if runtimepipelineobligation.StartupRecoveryDiagnosticsEnabled(ctx) {
						slog.WarnContext(ctx, "startup pipeline recovery blocked", "reason", "ingress_paused_during_dispatch", "event_id", work.Event.ID(), "event_type", work.Event.Type(), "run_id", work.Event.RunID(), "purpose", work.Claim.Purpose(), "error", processErr)
					}
					result.Blocked = true
					closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
					return result, errors.Join(closeErr)
				}
				closeErr := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request)
				return result, errors.Join(processErr, closeErr)
			}
		}
		if batch.Exhausted {
			result.Exhausted = true
			result.Blocked = state.locallyBlocked
			if err := eb.closePipelineScanLocked(context.WithoutCancel(ctx), request); err != nil {
				return result, err
			}
			return result, nil
		}
	}
	return result, nil
}

func (eb *EventBus) closePipelineScan(ctx context.Context, request runtimepipelineobligation.ScanRequest) error {
	if eb == nil || eb.pipelineObligations == nil {
		return nil
	}
	if err := eb.pipelineSweepMu.acquire(ctx); err != nil {
		return err
	}
	defer eb.pipelineSweepMu.release()
	return eb.closePipelineScanLocked(ctx, request)
}

func (eb *EventBus) closePipelineScanLocked(ctx context.Context, request runtimepipelineobligation.ScanRequest) error {
	var err error
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	state := eb.pipelineScans[request]
	if state == nil {
		return nil
	}
	delete(eb.pipelineScans, request)
	err = eb.pipelineObligations.CloseScan(ctx, state.cursor)
	if errors.Is(err, runtimepipelineobligation.ErrStaleScan) {
		return nil
	}
	return err
}

func (eb *EventBus) closeAllPipelineScans(ctx context.Context) error {
	if eb == nil || eb.pipelineObligations == nil {
		return nil
	}
	if err := eb.pipelineSweepMu.acquire(ctx); err != nil {
		return err
	}
	defer eb.pipelineSweepMu.release()
	var err error
	for request := range eb.pipelineScans {
		err = errors.Join(err, eb.closePipelineScanLocked(ctx, request))
	}
	return err
}

func (eb *EventBus) processClaimedPipelineWork(
	ctx context.Context,
	work runtimepipelineobligation.ClaimedWork,
) (settled bool, retry bool, retryLease *worklifetime.Lease, err error) {
	claimOpen := true
	var standingLease *worklifetime.Lease
	defer func() {
		if claimOpen && !retry {
			err = errors.Join(err, eb.pipelineObligations.Release(context.WithoutCancel(ctx), work.Claim))
		}
		if standingLease != nil && !retry {
			err = errors.Join(err, standingLease.Done())
		}
	}()
	if work.Claim.Purpose() == runtimepipelineobligation.PurposeDecisionRoute && work.Acknowledged {
		outcome, settleErr := eb.settleClaimedDecisionRoute(ctx, work)
		claimOpen = !outcome.Committed()
		return outcome.Committed(), false, nil, settleErr
	}
	if disposition, preclassified := work.PreDispatchDisposition(); preclassified {
		settlement, settleErr := eb.settlePipelineObligationOutcome(ctx, work.Claim, disposition)
		claimOpen = !settlement.Committed()
		if !settlement.Committed() {
			return false, false, nil, settleErr
		}
		eb.logStartupRecoveryPipelineAftermath(
			ctx,
			work.Event,
			startupRecoveryPipelineReplayOutcomeDropped,
			startupRecoveryPipelineReplayReasonQuarantined,
			disposition.Failure(),
			nil,
		)
		return true, false, nil, settleErr
	}
	ctx, standingLease, err = eb.bindClaimedRunWork(ctx, work.Event)
	if err != nil {
		return false, false, nil, err
	}
	recipients, dispatchErr := eb.authoritativeRecipientsForEvent(ctx, work.Event.ID())
	var outcome runtimepipelineobligation.ExecutionOutcome
	if dispatchErr == nil {
		outcome, dispatchErr = eb.RecoverPersistedPipeline(ctx, work, recipients)
	}
	decision := classifyPipelineDispatch(outcome, dispatchErr, false, work.Claim.Purpose(), pipelineDispatchRecoveryFinal)
	switch decision.action {
	case pipelineDispatchPending:
		return false, false, nil, dispatchErr
	case pipelineDispatchRetryRelease:
		if runtimepipelineobligation.StartupRecoveryDiagnosticsEnabled(ctx) {
			slog.WarnContext(ctx, "startup pipeline recovery blocked", "reason", "bounded_retry", "event_id", work.Event.ID(), "event_type", work.Event.Type(), "run_id", work.Event.RunID(), "purpose", work.Claim.Purpose(), "retry_reason", decision.retry.ReasonCode(), "failure", decision.retry.Failure())
		}
		return false, true, standingLease, dispatchErr
	case pipelineDispatchSettle:
		settlement, settleErr := eb.settlePipelineObligationOutcome(ctx, work.Claim, decision.disposition)
		claimOpen = !settlement.Committed()
		if !settlement.Committed() {
			return false, false, nil, errors.Join(dispatchErr, settleErr)
		}
		// Acknowledged is terminal for claim ownership, but not a dropped replay.
		if decision.disposition.Successful() && work.Scope == runtimepipelineobligation.ScopeDirect && len(recipients) == 0 {
			eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeSkipped, startupRecoveryPipelineReplayReasonNoPersistedRecipients, nil, nil)
		} else if decision.disposition.Successful() {
			eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeReplayed, startupRecoveryPipelineReplayReasonReplayed, nil, recipients)
		} else if decision.disposition.Terminal() {
			eb.logStartupRecoveryPipelineAftermath(ctx, work.Event, startupRecoveryPipelineReplayOutcomeDropped, startupRecoveryPipelineReplayReasonQuarantined, decision.disposition.Failure(), recipients)
		}
		if decision.failedBeforeSettle {
			return true, false, nil, settleErr
		}
		return true, false, nil, errors.Join(dispatchErr, settleErr)
	case pipelineDispatchMarkDecisionProcessed:
		mark, markErr := eb.pipelineObligations.MarkDecisionProcessed(ctx, work.Claim)
		if mark.DeliveryHandoffCommitted() {
			eb.SignalDeliveryContinuations()
		}
		if !mark.Committed() {
			return false, false, nil, errors.Join(dispatchErr, markErr)
		}
		settlement, settleErr := eb.settleClaimedDecisionRoute(ctx, work)
		claimOpen = !settlement.Committed()
		return settlement.Committed(), false, nil, errors.Join(dispatchErr, markErr, settleErr)
	}
	return false, false, nil, fmt.Errorf("unknown pipeline dispatch decision")
}

func (eb *EventBus) bindClaimedRunWork(
	ctx context.Context,
	event events.Event,
) (bound context.Context, standing *worklifetime.Lease, err error) {
	runID := strings.TrimSpace(event.RunID())
	if runID == "" {
		return ctx, nil, nil
	}
	reader := eb.durable.RunOrigins
	if reader == nil {
		return ctx, nil, errors.New("persisted pipeline recovery requires typed run origin readback")
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	readOwner := eb.workOwnerForContext(ctx)
	if readOwner == nil {
		return ctx, nil, errors.New("pipeline recovery reads require a process work occurrence")
	}
	readLease, err := readOwner.Begin(ctx)
	if err != nil {
		return ctx, nil, err
	}
	defer func() { err = errors.Join(err, readLease.Done()) }()
	// Only these admitted metadata reads drain without cancellation. The lease
	// joins their completion, and its original context fences every late result.
	readCtx := context.WithoutCancel(readLease.Context())
	origin, err := reader.LoadRunOrigin(readCtx, runID)
	if err != nil {
		return ctx, nil, fmt.Errorf("load pipeline recovery run origin: %w", err)
	}
	if err := readLease.Context().Err(); err != nil {
		return ctx, nil, err
	}
	if origin.Kind() != runtimerunlifecycle.OriginStandingGeneration {
		return ctx, nil, nil
	}
	standingReader := eb.durable.StandingRestarts
	if standingReader == nil {
		return ctx, nil, errors.New("persisted standing recovery requires typed restart disposition readback")
	}
	disposition, err := standingReader.StandingRunRestartDisposition(readCtx, runID)
	if err != nil {
		return ctx, nil, fmt.Errorf("classify pipeline recovery standing disposition: %w", err)
	}
	if err := readLease.Context().Err(); err != nil {
		return ctx, nil, err
	}
	if disposition.UsesGenericRecovery() {
		return ctx, nil, nil
	}
	if !disposition.Executable() {
		return ctx, nil, fmt.Errorf("%w: run %s is %s", errStandingRestartParked, runID, disposition.Kind)
	}
	eb.mu.RLock()
	owner := eb.standingRunWorkOwner
	eb.mu.RUnlock()
	if owner == nil {
		return ctx, nil, fmt.Errorf("%w: standing-generation pipeline recovery owner is not installed", ErrRunDispatchBlocked)
	}
	lease, err := owner.BeginStandingRunRecovery(ctx, runID, origin)
	if err != nil {
		return ctx, nil, fmt.Errorf("%w: admit standing-generation pipeline recovery: %v", ErrRunDispatchBlocked, err)
	}
	contextOwner, ok := worklifetime.OccurrenceFromContext(lease.Context())
	if !ok {
		_ = lease.Done()
		return ctx, nil, errors.New("standing-generation pipeline recovery admission omitted its exact occurrence")
	}
	return bindWorkContext(ctx, lease, contextOwner), lease, nil
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

func (eb *EventBus) settleClaimedDecisionRoute(ctx context.Context, work runtimepipelineobligation.ClaimedWork) (runtimepipelineobligation.SettlementOutcome, error) {
	return eb.settlePipelineObligationOutcome(ctx, work.Claim, runtimepipelineobligation.Acknowledged("decision_route_settled"))
}

func (eb *EventBus) ReleaseRuntimeIngressQueue(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return eb.SweepPipelineObligations(ctx, limit)
}

func (eb *EventBus) PreflightRuntimeIngressQueue(ctx context.Context) error {
	return eb.preflightPipelineResume(ctx, runtimepipelineobligation.GlobalResumeAdmissionRequest())
}

// ReleaseRunQueue owns only the #2106 half. Executable delivery backlog is
// continuously recovered by #2105's agent/node owners and is not republished
// or acknowledged through this pipeline operation.
func (eb *EventBus) ReleaseRunQueue(ctx context.Context, runID string, limit int) (runtimepipelineobligation.SweepResult, error) {
	if eb == nil || eb.pipelineObligations == nil {
		return runtimepipelineobligation.SweepResult{}, errors.New("pipeline obligation owner is required")
	}
	var err error
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return runtimepipelineobligation.SweepResult{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return runtimepipelineobligation.SweepResult{}, errors.New("run ID is required")
	}
	if limit <= 0 {
		limit = DefaultOutboxSweeperConfig().Limit
	}
	return eb.sweepPipelineObligations(ctx, runtimepipelineobligation.RunScanRequest(runID), limit)
}

func (eb *EventBus) PreflightRunQueue(ctx context.Context, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("run ID is required")
	}
	return eb.preflightPipelineResume(ctx, runtimepipelineobligation.RunResumeAdmissionRequest(runID))
}

func (eb *EventBus) preflightPipelineResume(ctx context.Context, request runtimepipelineobligation.ResumeAdmissionRequest) error {
	if eb == nil || eb.pipelineObligations == nil {
		return errors.New("pipeline obligation owner is required")
	}
	preflighter, ok := eb.pipelineObligations.(runtimepipelineobligation.ResumeAdmissionPreflighter)
	if !ok {
		return errors.New("pipeline resume admission preflight is required")
	}
	request = request.WithExecutionPosture(eb.executionPosture)
	if err := request.Validate(); err != nil {
		return err
	}
	return preflighter.PreflightResume(ctx, request)
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
