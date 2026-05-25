package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	runtimedelivery "swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type pipelineTransitionSchemaCapabilityProvider interface {
	CanonicalEventReceiptsCapability(context.Context) (bool, error)
}

type targetFailureDeadLetterRecorder interface {
	RecordDeadLetter(context.Context, runtimedeadletters.Record) error
}

type targetFailureDeadLetterTxRecorder interface {
	RecordDeadLetterTx(context.Context, *sql.Tx, runtimedeadletters.Record) error
}

func shouldPersistPipelineReceipt(persisted bool, publishErr error) bool {
	if !persisted {
		return false
	}
	return !errors.Is(publishErr, errAuthoritativeDeliveryIncomplete)
}

func pipelineReceiptStatus(ctx context.Context, publishErr error) (string, string) {
	if publishErr != nil {
		return "error", publishErr.Error()
	}
	if overrideStatus, overrideErr, ok := runtimepipeline.PipelineReceiptOverrideFromContext(ctx); ok {
		return overrideStatus, overrideErr
	}
	return "processed", ""
}

func applyTargetDeliveryFailureReceipt(override *runtimepipeline.PipelineReceiptOverride, failure runtimepinrouting.TargetFailure) {
	if override == nil || failure == "" {
		return
	}
	override.Status = "dead_letter"
	override.ErrText = targetDeliveryFailureMessage(failure)
}

func targetDeliveryFailureMessage(failure runtimepinrouting.TargetFailure) string {
	failure = runtimepinrouting.TargetFailure(strings.TrimSpace(string(failure)))
	if failure == "" {
		return ""
	}
	return fmt.Sprintf("pin routing target delivery failed: %s", failure)
}

var ErrRuntimeIngressPaused = errors.New("runtime ingress is paused")
var ErrRunDispatchBlocked = errors.New("run dispatch is blocked")

const (
	dispatchQueueRuntimeIngress = "runtime_ingress_queued"
	dispatchQueueRunBlocked     = "run_dispatch_blocked"
)

func (eb *EventBus) runtimeIngressDispatchPaused(ctx context.Context, evt events.Event) (bool, error) {
	if eb == nil || runtimeIngressDispatchBypass(evt) {
		return false, nil
	}
	eb.mu.RLock()
	gate := eb.runtimeIngressDispatchGate
	eb.mu.RUnlock()
	if gate == nil {
		return false, nil
	}
	paused, err := gate.QueueableIngressPaused(ctx)
	if err != nil {
		return false, err
	}
	return paused, nil
}

func (eb *EventBus) runDispatchBlocked(ctx context.Context, evt events.Event) (bool, error) {
	if eb == nil {
		return false, nil
	}
	runID := strings.TrimSpace(evt.RunID)
	if runID == "" {
		return false, nil
	}
	eb.mu.RLock()
	gate := eb.runDispatchGate
	eb.mu.RUnlock()
	if gate == nil {
		return false, nil
	}
	return gate.QueueableRunDispatchBlocked(ctx, runID)
}

func runtimeIngressDispatchBypass(evt events.Event) bool {
	if strings.TrimSpace(evt.SourceAgent) != "runtime" {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "platform.")
}

func (eb *EventBus) dispatchQueueReason(ctx context.Context, evt events.Event) (string, error) {
	if paused, err := eb.runtimeIngressDispatchPaused(ctx, evt); err != nil {
		return "", err
	} else if paused {
		return dispatchQueueRuntimeIngress, nil
	}
	if blocked, err := eb.runDispatchBlocked(ctx, evt); err != nil {
		return "", err
	} else if blocked {
		return dispatchQueueRunBlocked, nil
	}
	return "", nil
}

func (eb *EventBus) logDispatchQueued(ctx context.Context, reason string, evt events.Event, recipientsCount int, direct, transactional bool) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	message := "Event persisted without dispatch"
	detail := map[string]any{
		"recipients_count": recipientsCount,
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
	}
	if direct {
		detail["direct"] = true
	}
	if transactional {
		detail["transactional"] = true
	}
	if reason == dispatchQueueRuntimeIngress {
		message = "Runtime ingress is paused; event persisted without dispatch"
	} else if reason == dispatchQueueRunBlocked {
		message = "Run dispatch is blocked; event persisted without dispatch"
		detail["run_id"] = strings.TrimSpace(evt.RunID)
	}
	eb.logRuntime(ctx, "debug", message, "eventbus", reason, evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, detail, "", 0)
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	eb.inFlightPublishes.Add(1)
	defer eb.inFlightPublishes.Add(-1)
	start := time.Now()
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ictx, evt := runtimecorrelation.CorrelateEvent(ctx, evt)

	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]func(), 0, 8)
	afterPublishActions := make([]func(), 0, 4)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ictx = runtimepipeline.WithPipelineTransitionCollector(ictx, &deferredTransitions, eb.pipelineTransitionCapability())
	ictx = runtimepipeline.WithPipelinePostCommitActions(ictx, &postCommitActions)
	ictx = runtimepipeline.WithPipelineAfterPublishActions(ictx, &afterPublishActions)
	ictx = runtimepipeline.WithPipelineReceiptOverride(ictx, receiptOverride)
	defer func() {
		runtimepipeline.FlushPipelineAfterPublishActions(afterPublishActions)
	}()
	if txStore, ok := eb.store.(TransactionalEventStore); ok {
		if err := eb.publishTransactional(ictx, evt, start, &deferredTransitions, txStore); err != nil {
			return err
		}
		eb.recordCommittedPublishConvergence(ictx, evt)
		return nil
	}

	persisted := false
	queued := false
	passthrough := true
	deferred := []events.Event{}
	defer func() {
		if queued {
			return
		}
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ictx, err)
		eb.markPipelineReceipt(ictx, evt.ID, status, errText)
	}()

	plan, err := eb.planSubscribedPublish(ictx, evt)
	if err != nil {
		return err
	}
	if err := eb.persistSubscribedPublishPlan(ictx, evt, plan); err != nil {
		return err
	}
	persisted = true
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ictx, evt, plan)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	if reason, err := eb.dispatchQueueReason(ictx, evt); err != nil {
		return err
	} else if reason != "" {
		queued = true
		eb.logDispatchQueued(ictx, reason, evt, len(plan.Recipients), false, false)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	if pass, out, ierr := eb.runInterceptors(ictx, evt); ierr != nil {
		return ierr
	} else {
		passthrough = pass
		deferred = out
	}
	if passthrough {
		var deliverQueued bool
		if deliverQueued, err = eb.deliverSubscribedPublishPlan(ictx, evt, plan); err != nil {
			return err
		}
		queued = deliverQueued
	}
	if !passthrough {
		persisted = true
	}
	eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(ictx, deferredTransitions)
	for _, d := range deferred {
		if err := eb.publishDeferred(ictx, d); err != nil {
			return err
		}
	}
	return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
}

// PublishTx persists the canonical event record and recipient manifest in a
// caller-owned transaction. Callers use this when another persisted state
// transition must commit atomically with event emission.
func (eb *EventBus) PublishTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	if tx == nil {
		return errors.New("publish tx is required")
	}
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ictx, evt := runtimecorrelation.CorrelateEvent(ctx, evt)
	txStore, ok := eb.store.(TransactionalEventStore)
	if !ok || txStore == nil {
		return errors.New("transactional event store is required")
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ictx, tx)
	if err := txStore.AppendEventTx(txctx, tx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if err := eb.authorizePublishRecipientPlanning(txctx, evt); err != nil {
		return err
	}
	inboundPlan, err := eb.deliveryPlanner.Plan(txctx, evt)
	if err != nil {
		return err
	}
	if err := eb.authorizePublishRecipientPlan(txctx, evt, inboundPlan); err != nil {
		return err
	}
	eb.recordPublishDiagnostic(txctx, evt, inboundPlan)
	if len(inboundPlan.PersistedRecipients) > 0 || len(inboundPlan.DeliveryRoutes) > 0 {
		if err := eb.insertEventDeliveriesTx(txctx, txStore, tx, evt.ID, inboundPlan.PersistedRecipients, inboundPlan.DeliveryTargets, inboundPlan.DeliveryRoutes); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if scopeWriter, ok := eb.store.(TransactionalEventReplayScopePersistence); ok && scopeWriter != nil {
		if err := scopeWriter.UpsertCommittedReplayScopeTx(txctx, tx, evt.ID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return fmt.Errorf("persist committed replay scope: %w", err)
		}
	} else if replayScopePersistenceRequired(eb.store) {
		return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
	}
	if inboundPlan.TargetFailure != "" {
		if err := txStore.UpsertPipelineReceiptTx(txctx, tx, evt.ID, "dead_letter", targetDeliveryFailureMessage(inboundPlan.TargetFailure)); err != nil {
			return fmt.Errorf("persist pipeline receipt: %w", err)
		}
		eb.recordTargetDeliveryFailureTx(txctx, tx, evt, inboundPlan)
		return nil
	}
	if reason, err := eb.dispatchQueueReason(txctx, evt); err != nil {
		return err
	} else if reason != "" {
		eb.logDispatchQueued(txctx, reason, evt, len(inboundPlan.Recipients), false, true)
		return nil
	}
	if !runtimepipeline.QueuePipelinePostCommitAction(txctx, func() {
		eb.completePublishTxDispatch(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(txctx)), evt, inboundPlan)
	}) {
		return errors.New("transactional publish post-commit actions are required")
	}
	return nil
}

func (eb *EventBus) completePublishTxDispatch(ctx context.Context, evt events.Event, inboundPlan eventDeliveryPlan) {
	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]func(), 0, 8)
	afterPublishActions := make([]func(), 0, 4)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions, eb.pipelineTransitionCapability())
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
	ctx = runtimepipeline.WithPipelineAfterPublishActions(ctx, &afterPublishActions)
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	defer runtimepipeline.FlushPipelineAfterPublishActions(afterPublishActions)

	passthrough, deferred, err := eb.runInterceptors(ctx, evt)
	if err != nil {
		eb.recordCommittedPublishReceipt(ctx, evt, err)
		return
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(ctx, deferredTransitions)

	if passthrough {
		if len(inboundPlan.Recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipients, "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverToRecipientsWithRoutes(ctx, evt, inboundPlan.Recipients, inboundPlan.DeliveryRoutes); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return
			}
			eb.logDelivery(ctx, evt, inboundPlan.Recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *inboundPlan.CycleEscalation); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, 0)

	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			eb.recordCommittedPublishReceipt(ctx, evt, err)
			return
		}
	}
	eb.recordCommittedPublishReceipt(ctx, evt, nil)
	eb.recordCommittedPublishConvergence(ctx, evt)
}

func (eb *EventBus) withBundleFingerprint(ctx context.Context) context.Context {
	if ctx == nil || eb == nil {
		return ctx
	}
	if runtimecorrelation.BundleFingerprintFromContext(ctx) != "" {
		return ctx
	}
	return runtimecorrelation.WithBundleFingerprint(ctx, eb.bundleFingerprint)
}

func (eb *EventBus) WithBundleFingerprint(ctx context.Context) context.Context {
	return eb.withBundleFingerprint(ctx)
}

func (eb *EventBus) pipelineTransitionCapability() func(context.Context) (bool, error) {
	if eb == nil || eb.store == nil {
		return nil
	}
	if provider, ok := eb.store.(pipelineTransitionSchemaCapabilityProvider); ok && provider != nil {
		return provider.CanonicalEventReceiptsCapability
	}
	return nil
}

func (eb *EventBus) convergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	if converger, ok := eb.store.(StandaloneRuntimePlatformRunConvergencePersistence); ok && converger != nil {
		if err := converger.ConvergeStandaloneRuntimePlatformRun(ctx, evt); err != nil {
			return err
		}
	}
	return eb.ConvergeNormalRunCompletionForEvent(ctx, evt.ID)
}

func (eb *EventBus) publishTransactional(
	ctx context.Context,
	evt events.Event,
	start time.Time,
	deferredTransitions *[]runtimepipeline.DeferredPipelineTransition,
	txStore TransactionalEventStore,
) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	tx, err := txStore.BeginEventTx(ctx)
	if err != nil {
		return fmt.Errorf("begin publish tx: %w", err)
	}
	postCommitActions := make([]func(), 0, 8)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	txctx = runtimepipeline.WithPipelineReceiptOverride(txctx, receiptOverride)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := txStore.AppendEventTx(txctx, tx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}

	if err := eb.authorizePublishRecipientPlanning(txctx, evt); err != nil {
		return err
	}
	inboundPlan, err := eb.deliveryPlanner.Plan(txctx, evt)
	if err != nil {
		return err
	}
	if err := eb.authorizePublishRecipientPlan(txctx, evt, inboundPlan); err != nil {
		return err
	}
	eb.recordPublishDiagnostic(txctx, evt, inboundPlan)
	if len(inboundPlan.PersistedRecipients) > 0 || len(inboundPlan.DeliveryRoutes) > 0 {
		if err := eb.insertEventDeliveriesTx(txctx, txStore, tx, evt.ID, inboundPlan.PersistedRecipients, inboundPlan.DeliveryTargets, inboundPlan.DeliveryRoutes); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if scopeWriter, ok := eb.store.(TransactionalEventReplayScopePersistence); ok && scopeWriter != nil {
		if err := scopeWriter.UpsertCommittedReplayScopeTx(txctx, tx, evt.ID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return fmt.Errorf("persist committed replay scope: %w", err)
		}
	} else if replayScopePersistenceRequired(eb.store) {
		return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
	}
	if inboundPlan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, inboundPlan.TargetFailure)
		status, errText := pipelineReceiptStatus(txctx, nil)
		if err := txStore.UpsertPipelineReceiptTx(txctx, tx, evt.ID, status, errText); err != nil {
			return fmt.Errorf("persist pipeline receipt: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit publish tx: %w", err)
		}
		committed = true
		runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
		eb.recordTargetDeliveryFailure(ctx, evt, inboundPlan)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return nil
	}

	if reason, err := eb.dispatchQueueReason(txctx, evt); err != nil {
		return err
	} else if reason != "" {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit publish tx: %w", err)
		}
		committed = true
		eb.logDispatchQueued(ctx, reason, evt, len(inboundPlan.Recipients), false, false)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return nil
	}

	passthrough, deferred, err := eb.runInterceptors(txctx, evt)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish tx: %w", err)
	}
	committed = true
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	if deferredTransitions != nil {
		runtimepipeline.FlushDeferredPipelineTransitions(ctx, *deferredTransitions)
	}

	if passthrough {
		if len(inboundPlan.Recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipients, "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverToRecipientsWithRoutes(ctx, evt, inboundPlan.Recipients, inboundPlan.DeliveryRoutes); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return nil
			}
			eb.logDelivery(ctx, evt, inboundPlan.Recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *inboundPlan.CycleEscalation); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return nil
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))

	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			eb.recordCommittedPublishReceipt(ctx, evt, err)
			return nil
		}
	}
	eb.recordCommittedPublishReceipt(ctx, evt, nil)
	return nil
}

func (eb *EventBus) recordCommittedPublishReceipt(ctx context.Context, evt events.Event, publishErr error) {
	status, errText := pipelineReceiptStatus(ctx, publishErr)
	_ = eb.markPipelineReceipt(ctx, evt.ID, status, errText)
}

func (eb *EventBus) recordCommittedPublishConvergence(ctx context.Context, evt events.Event) {
	if err := eb.convergeStandaloneRuntimePlatformRun(ctx, evt); err != nil {
		eb.recordCommittedPublishReceipt(ctx, evt, err)
		eb.logRuntime(ctx, "error", "Post-commit publish convergence failed", "eventbus", "publish_post_commit_convergence_failed", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
			"error": err.Error(),
		}, err.Error(), 0)
	}
}

func txTableExists(ctx context.Context, tx *sql.Tx, table string) bool {
	if tx == nil || strings.TrimSpace(table) == "" {
		return false
	}
	var ok bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+strings.TrimSpace(table)).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func (eb *EventBus) runInterceptors(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	eb.mu.RLock()
	interceptors := append([]EventInterceptor(nil), eb.interceptors...)
	provider := eb.interceptorProvider
	eb.mu.RUnlock()
	if provider != nil {
		for _, it := range provider() {
			if it != nil {
				interceptors = append(interceptors, it)
			}
		}
	}
	if len(interceptors) == 0 {
		return true, nil, nil
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	for _, it := range interceptors {
		pass, out, err := it.Intercept(ctx, evt)
		if err != nil {
			return true, nil, err
		}
		if !pass {
			passthrough = false
		}
		for _, d := range out {
			if d.ID == "" {
				d.ID = uuid.NewString()
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = time.Now()
			}
			deferred = append(deferred, d)
		}
	}
	return passthrough, deferred, nil
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	eb.inFlightPublishes.Add(1)
	defer eb.inFlightPublishes.Add(-1)
	if evt.Type == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ctx, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	queued := false
	defer func() {
		if queued {
			return
		}
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	plan, err := eb.planSubscribedPublish(ctx, evt)
	if err != nil {
		return err
	}
	if err := eb.persistSubscribedPublishPlan(ctx, evt, plan); err != nil {
		return err
	}
	persisted = true
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ctx, evt, plan)
		eb.logPublished(ctx, evt, 0)
		return nil
	}
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return err
	} else if reason != "" {
		queued = true
		eb.logDispatchQueued(ctx, reason, evt, len(plan.Recipients), false, false)
		eb.logPublished(ctx, evt, 0)
		return nil
	}
	passthrough, deferred, err := eb.runInterceptors(ctx, evt)
	if err != nil {
		return err
	}
	if passthrough {
		var deliverQueued bool
		if deliverQueued, err = eb.deliverSubscribedPublishPlan(ctx, evt, plan); err != nil {
			return err
		}
		queued = deliverQueued
	}
	eb.logPublished(ctx, evt, 0)
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) logPublished(ctx context.Context, evt events.Event, durationUS int) {
	eb.logRuntime(ctx, "debug", "Event was published to the event bus", "eventbus", "published", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
		"type":            string(evt.Type),
		"source":          evt.SourceAgent,
		"parent_event_id": strings.TrimSpace(evt.ParentEventID),
	}, "", durationUS)
}

func (eb *EventBus) publishSubscribedNonTransactional(ctx context.Context, evt events.Event) (bool, error) {
	plan, err := eb.planSubscribedPublish(ctx, evt)
	if err != nil {
		return false, err
	}
	if err := eb.persistSubscribedPublishPlan(ctx, evt, plan); err != nil {
		return false, err
	}
	return eb.deliverSubscribedPublishPlan(ctx, evt, plan)
}

func (eb *EventBus) planSubscribedPublish(ctx context.Context, evt events.Event) (eventDeliveryPlan, error) {
	if err := eb.authorizePublishRecipientPlanning(ctx, evt); err != nil {
		return eventDeliveryPlan{}, err
	}
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return eventDeliveryPlan{}, err
	}
	if err := eb.authorizePublishRecipientPlan(ctx, evt, plan); err != nil {
		return eventDeliveryPlan{}, err
	}
	eb.recordPublishDiagnostic(ctx, evt, plan)
	return plan, nil
}

func (eb *EventBus) authorizePublishRecipientPlanning(ctx context.Context, evt events.Event) error {
	if eb == nil || eb.recipientPlanAdmissionGuard == nil {
		return nil
	}
	return eb.recipientPlanAdmissionGuard(ctx, evt)
}

func (eb *EventBus) authorizePublishRecipientPlan(ctx context.Context, evt events.Event, plan eventDeliveryPlan) error {
	if eb == nil || eb.recipientPlanGuard == nil {
		return nil
	}
	return eb.recipientPlanGuard(ctx, evt, PublishRecipientPlan{
		Recipients:             uniqueStrings(plan.Recipients),
		PersistedRecipients:    uniqueStrings(plan.PersistedRecipients),
		RoutedRecipients:       eb.describeSubscribersForEvent(string(evt.Type), plan.RoutedRecipients),
		SubscriptionRecipients: uniqueStrings(plan.SubscribedRecipients),
	})
}

func (eb *EventBus) persistSubscribedPublishPlan(ctx context.Context, evt events.Event, plan eventDeliveryPlan) error {
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients, plan.DeliveryTargets, plan.DeliveryRoutes, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		return err
	}
	eb.logQueuedDeliveries(ctx, evt, plan.PersistedRecipients, "matched_agent_subscription", plan.ExtraDetail)
	return nil
}

func (eb *EventBus) deliverSubscribedPublishPlan(ctx context.Context, evt events.Event, plan eventDeliveryPlan) (bool, error) {
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return false, err
	} else if reason != "" {
		eb.logDispatchQueued(ctx, reason, evt, len(plan.Recipients), false, false)
		return true, nil
	}
	if len(plan.Recipients) > 0 {
		if err := eb.deliverToRecipientsWithRoutes(ctx, evt, plan.Recipients, plan.DeliveryRoutes); err != nil {
			return false, err
		}
		eb.logDelivery(ctx, evt, plan.Recipients, plan.ExtraDetail)
	}
	if plan.BlockedByCycle && plan.CycleEscalation != nil {
		if err := eb.publishDeferred(ctx, *plan.CycleEscalation); err != nil {
			return false, err
		}
	}
	if strings.TrimSpace(plan.ContradictionReason) != "" {
		_ = eb.emitContradiction(ctx, evt, plan.ContradictionReason)
	}
	return false, nil
}

func (eb *EventBus) persistEventRecord(
	ctx context.Context,
	evt events.Event,
	recipients []string,
	deliveryTargets map[string]events.RouteIdentity,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	recipients = uniqueStrings(recipients)
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if atomicStore, ok := eb.store.(AtomicEventDeliveryRouteSetPersistence); ok && len(deliveryRoutes) > 0 {
		if err := atomicStore.PersistEventWithDeliveryRouteSetAndScope(ctx, evt, deliveryRoutes, scope); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if atomicStore, ok := eb.store.(AtomicEventRoutePersistence); ok {
		if err := atomicStore.PersistEventWithDeliveryRoutesAndScope(ctx, evt, recipients, deliveryTargets, scope); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if atomicStore, ok := eb.store.(AtomicEventReplayScopePersistence); ok {
		if err := atomicStore.PersistEventWithDeliveriesAndScope(ctx, evt, recipients, scope); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if atomicStore, ok := eb.store.(AtomicEventPersistence); ok {
		if err := atomicStore.PersistEventWithDeliveries(ctx, evt, recipients); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID, scope); err != nil {
				return fmt.Errorf("persist committed replay scope: %w", err)
			}
			return nil
		}
		if replayScopePersistenceRequired(eb.store) {
			return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		return nil
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if len(recipients) > 0 || len(deliveryRoutes) > 0 {
		if err := eb.insertEventDeliveries(ctx, evt.ID, recipients, deliveryTargets, deliveryRoutes); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
		if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID, scope); err != nil {
			return fmt.Errorf("persist committed replay scope: %w", err)
		}
		return nil
	}
	if replayScopePersistenceRequired(eb.store) {
		return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
	}
	return nil
}

func (eb *EventBus) insertEventDeliveries(ctx context.Context, eventID string, recipients []string, deliveryTargets map[string]events.RouteIdentity, deliveryRoutes []events.DeliveryRoute) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if store, ok := eb.store.(EventDeliveryRouteSetPersistence); ok && store != nil && len(deliveryRoutes) > 0 {
		return store.InsertEventDeliveryRoutes(ctx, eventID, deliveryRoutes)
	}
	if store, ok := eb.store.(EventDeliveryRoutePersistence); ok && store != nil {
		return store.InsertEventDeliveriesWithTargets(ctx, eventID, recipients, deliveryTargets)
	}
	return eb.store.InsertEventDeliveries(ctx, eventID, recipients)
}

func (eb *EventBus) insertEventDeliveriesTx(
	ctx context.Context,
	txStore TransactionalEventStore,
	tx *sql.Tx,
	eventID string,
	recipients []string,
	deliveryTargets map[string]events.RouteIdentity,
	deliveryRoutes []events.DeliveryRoute,
) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if store, ok := txStore.(TransactionalEventDeliveryRouteSetPersistence); ok && store != nil && len(deliveryRoutes) > 0 {
		return store.InsertEventDeliveryRoutesTx(ctx, tx, eventID, deliveryRoutes)
	}
	if store, ok := txStore.(TransactionalEventDeliveryRoutePersistence); ok && store != nil {
		return store.InsertEventDeliveriesWithTargetsTx(ctx, tx, eventID, recipients, deliveryTargets)
	}
	return txStore.InsertEventDeliveriesTx(ctx, tx, eventID, recipients)
}

func (eb *EventBus) logDelivery(ctx context.Context, evt events.Event, recipients []string, extra map[string]any) {
	detail := map[string]any{
		"recipients_count": len(recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
	}
	for k, v := range extra {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "Event was delivered to recipients", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, detail, "", 0)
}

func (eb *EventBus) logQueuedDeliveries(ctx context.Context, evt events.Event, recipients []string, reason string, extra map[string]any) {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return
	}
	for _, recipient := range recipients {
		detail := map[string]any{
			"delivery_state":          string(runtimedelivery.StateQueued),
			"delivery_transition":     string(runtimedelivery.StateQueued),
			"delivery_previous_state": "",
			"delivery_reason":         strings.TrimSpace(reason),
			"subscriber_type":         "agent",
			"subscriber_id":           strings.TrimSpace(recipient),
			"parent_event_id":         strings.TrimSpace(evt.ParentEventID),
		}
		for k, v := range extra {
			detail[k] = v
		}
		eb.logRuntime(ctx, "debug", "Delivery entered queued state", "eventbus", "delivery_lifecycle_transition", evt.ID, string(evt.Type), strings.TrimSpace(recipient), evt.EntityID(), "", nil, detail, "", 0)
	}
}

func subscriberIDs(in []Subscriber) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, subscriber := range in {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		out = append(out, id)
	}
	return uniqueStrings(out)
}

func publishDiagnosticRecipientMaps(in []PublishDiagnosticRecipient) []map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(in))
	for _, recipient := range in {
		item := map[string]any{
			"id": recipient.ID,
		}
		if v := strings.TrimSpace(recipient.Type); v != "" {
			item["type"] = v
		}
		if v := strings.TrimSpace(recipient.Path); v != "" {
			item["path"] = v
		}
		if v := strings.TrimSpace(recipient.MatchedPattern); v != "" {
			item["matched_pattern"] = v
		}
		if v := strings.TrimSpace(recipient.RouteSource); v != "" {
			item["route_source"] = v
		}
		if v := strings.TrimSpace(recipient.LocalizedEvent); v != "" {
			item["localized_event"] = v
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) describeSubscribersForEvent(eventType string, in []Subscriber) []PublishDiagnosticRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]PublishDiagnosticRecipient, 0, len(in))
	for _, subscriber := range in {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		item := PublishDiagnosticRecipient{
			ID:             id,
			Type:           strings.TrimSpace(subscriber.Type),
			Path:           strings.TrimSpace(subscriber.Path),
			MatchedPattern: strings.TrimSpace(subscriber.MatchPattern),
			RouteSource:    strings.TrimSpace(subscriber.RouteSource),
		}
		if localized := eb.localizedSubscriberEvent(eventType, subscriber); localized != "" {
			item.LocalizedEvent = localized
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) localizedSubscriberEvent(eventType string, subscriber Subscriber) string {
	if strings.TrimSpace(subscriber.Type) != "node" {
		return ""
	}
	candidates := []string{eventType, subscriber.MatchPattern}
	if eb != nil && eb.semanticSource != nil {
		flowID := strings.TrimSpace(routeFlowIDForPath(eb.semanticSource, subscriber.Path))
		if flowID != "" {
			scope := eventidentity.Scope{
				Path:        strings.Trim(strings.TrimSpace(subscriber.Path), "/"),
				InputEvents: append([]string{}, eb.semanticSource.FlowInputEvents(flowID)...),
			}
			for _, candidate := range candidates {
				if localized := scope.LocalizeInput(candidate); localized != "" && localized != eventidentity.Normalize(candidate) {
					return localized
				}
			}
		}
	}
	for _, candidate := range candidates {
		normalized := eventidentity.Normalize(candidate)
		if leaf := eventidentity.LeafName(normalized); leaf != "" && leaf != normalized {
			return leaf
		}
	}
	return ""
}

func (eb *EventBus) recordPublishDiagnostic(ctx context.Context, evt events.Event, plan eventDeliveryPlan) {
	rec, ok := EmittedEventsRecorderFromContext(ctx)
	if !ok || rec == nil {
		return
	}
	rec.AppendPublish(PublishDiagnostic{
		EventID:                strings.TrimSpace(evt.ID),
		EventType:              strings.TrimSpace(string(evt.Type)),
		EntityID:               strings.TrimSpace(evt.EntityID()),
		ParentEventID:          strings.TrimSpace(evt.ParentEventID),
		RoutedRecipients:       eb.describeSubscribersForEvent(string(evt.Type), plan.RoutedRecipients),
		SubscriptionRecipients: uniqueStrings(plan.SubscribedRecipients),
	})
}

// PublishDirect persists an event and delivers it to an explicit caller-supplied
// recipient set. The recipient manifest still routes through the canonical
// delivery policy so explicit delivery cannot bypass scoped-recipient rules.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	start := time.Now()
	persisted := false
	queued := false
	defer func() {
		if queued {
			return
		}
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ctx, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	plan, err := eb.deliveryPlanner.PlanDirect(ctx, evt, recipients)
	if err != nil {
		return err
	}
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients, plan.DeliveryTargets, plan.DeliveryRoutes, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		return err
	}
	persisted = true
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ctx, evt, plan)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return nil
	}
	eb.logQueuedDeliveries(ctx, evt, plan.PersistedRecipients, "direct_publish", plan.ExtraDetail)
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return err
	} else if reason != "" {
		queued = true
		eb.logDispatchQueued(ctx, reason, evt, len(plan.Recipients), true, false)
		return nil
	}
	if err := eb.deliverToRecipientsWithRoutes(ctx, evt, plan.Recipients, plan.DeliveryRoutes); err != nil {
		return err
	}
	detail := map[string]any{
		"direct":           true,
		"recipients_count": len(plan.Recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
	}
	for k, v := range plan.ExtraDetail {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "Event was delivered directly to recipients", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, detail, "", int(time.Since(start)/time.Microsecond))
	return nil
}

// CheckDirectRecipients applies the same direct-recipient policy used by
// PublishDirect, then verifies the allowed recipients are currently deliverable.
// It is intentionally side-effect free so public API owners can fail closed
// before creating replay evidence.
func (eb *EventBus) CheckDirectRecipients(ctx context.Context, evt events.Event, recipients []string) (DirectRecipientStatus, error) {
	requested := uniqueStrings(recipients)
	status := DirectRecipientStatus{Requested: append([]string(nil), requested...)}
	if eb == nil {
		status.Missing = append([]string(nil), requested...)
		return status, nil
	}
	if evt.Type == "" {
		return status, errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return status, fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return status, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	plan, err := eb.deliveryPlanner.PlanDirect(ctx, evt, requested)
	if err != nil {
		return status, err
	}
	status.Recipients = append([]string(nil), plan.Recipients...)
	status.Filtered = filteredRecipients(requested, plan.Recipients)
	liveRecipients := eb.snapshotRecipientChans(plan.Recipients)
	live := make(map[string]struct{}, len(liveRecipients))
	for _, recipient := range liveRecipients {
		live[recipient.agentID] = struct{}{}
	}
	for _, recipient := range plan.Recipients {
		if _, ok := live[recipient]; !ok {
			status.Missing = append(status.Missing, recipient)
		}
	}
	status.Missing = uniqueStrings(append(status.Missing, status.Filtered...))
	return status, nil
}

// PublishPersistedRecipients delivers an already-committed event using the
// persisted agent manifest plus the authoritative committed replay scope.
func (eb *EventBus) PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error {
	return eb.publishPersistedRecipients(ctx, evt, recipients, false)
}

func (eb *EventBus) publishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string, replayInterceptors bool) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	recipients = uniqueStrings(recipients)
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return err
	} else if reason != "" {
		if reason == dispatchQueueRuntimeIngress {
			return ErrRuntimeIngressPaused
		}
		return ErrRunDispatchBlocked
	}
	scope, err := eb.authoritativeReplayScopeForEvent(ctx, evt.ID)
	if err != nil {
		return err
	}
	passthrough := true
	deferred := []events.Event(nil)
	if replayInterceptors && scope == runtimereplayclaim.CommittedReplayScopeSubscribed {
		postCommitActions := make([]func(), 0, 8)
		deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
		receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
		ctx = runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions, eb.pipelineTransitionCapability())
		ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
		ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
		var err error
		passthrough, deferred, err = eb.runInterceptors(ctx, evt)
		if err != nil {
			return err
		}
		runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
		runtimepipeline.FlushDeferredPipelineTransitions(ctx, deferredTransitions)
	}
	liveRecipients, internalRecipients, deliveryRoutes, err := eb.replayRecipientsForCommittedEvent(ctx, evt, recipients, scope)
	if err != nil {
		return err
	}
	if passthrough && len(liveRecipients) > 0 {
		if err := eb.deliverToRecipientsWithRoutes(ctx, evt, liveRecipients, deliveryRoutes); err != nil {
			return err
		}
	}
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	if !passthrough || len(liveRecipients) == 0 {
		return nil
	}
	owner := "event_deliveries"
	if scope == runtimereplayclaim.CommittedReplayScopeSubscribed {
		owner = "event_deliveries+committed_replay_scope"
	}
	eb.logRuntime(ctx, "debug", "Persisted event was delivered to authoritative recipients", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, map[string]any{
		"direct":                     scope == runtimereplayclaim.CommittedReplayScopeDirect,
		"delivery_manifest_owner":    owner,
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            strings.TrimSpace(evt.ParentEventID),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
		"replay_scope":               string(scope),
	}, "", 0)
	return nil
}

func (eb *EventBus) deliveryTargetsForEvent(ctx context.Context, eventID string) map[string]events.RouteIdentity {
	reader, ok := eb.store.(EventDeliveryTargetReader)
	if !ok || reader == nil {
		return nil
	}
	targets, err := reader.ListEventDeliveryTargets(ctx, eventID)
	if err != nil {
		return nil
	}
	return targets
}

func (eb *EventBus) deliveryRoutesForEvent(ctx context.Context, eventID string) []events.DeliveryRoute {
	reader, ok := eb.store.(EventDeliveryRouteSetReader)
	if ok && reader != nil {
		routes, err := reader.ListEventDeliveryRoutes(ctx, eventID)
		if err == nil && len(routes) > 0 {
			return events.NormalizeDeliveryRoutes(routes)
		}
	}
	targets := eb.deliveryTargetsForEvent(ctx, eventID)
	if len(targets) == 0 {
		return nil
	}
	recipients := make([]string, 0, len(targets))
	for recipient := range targets {
		recipients = append(recipients, recipient)
	}
	return deliveryRoutesFromTargetMap(recipients, "agent", targets)
}

func (eb *EventBus) recordTargetDeliveryFailure(ctx context.Context, evt events.Event, plan eventDeliveryPlan) {
	failure := runtimepinrouting.TargetFailure(strings.TrimSpace(string(plan.TargetFailure)))
	if failure == "" {
		return
	}
	message, detail, record := targetDeliveryFailureRecord(evt, plan, failure)
	eb.logRuntime(ctx, "warn", "Pin routing target delivery failed", "eventbus", "target_resolution_failed", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, detail, message, 0)

	recorder, ok := eb.store.(targetFailureDeadLetterRecorder)
	if !ok || recorder == nil {
		return
	}
	if err := recorder.RecordDeadLetter(ctx, record); err != nil {
		eb.logRuntime(ctx, "warn", "Pin routing target failure dead-letter record failed", "eventbus", "target_resolution_failed_dead_letter_failed", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, detail, err.Error(), 0)
	}
}

func (eb *EventBus) recordTargetDeliveryFailureTx(ctx context.Context, tx *sql.Tx, evt events.Event, plan eventDeliveryPlan) {
	failure := runtimepinrouting.TargetFailure(strings.TrimSpace(string(plan.TargetFailure)))
	if failure == "" {
		return
	}
	message, detail, record := targetDeliveryFailureRecord(evt, plan, failure)
	eb.logRuntime(ctx, "warn", "Pin routing target delivery failed", "eventbus", "target_resolution_failed", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, detail, message, 0)

	recorder, ok := eb.store.(targetFailureDeadLetterTxRecorder)
	if !ok || recorder == nil {
		return
	}
	if err := recorder.RecordDeadLetterTx(ctx, tx, record); err != nil {
		eb.logRuntime(ctx, "warn", "Pin routing target failure dead-letter record failed", "eventbus", "target_resolution_failed_dead_letter_failed", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, detail, err.Error(), 0)
	}
}

func targetDeliveryFailureRecord(evt events.Event, plan eventDeliveryPlan, failure runtimepinrouting.TargetFailure) (string, map[string]any, runtimedeadletters.Record) {
	message := targetDeliveryFailureMessage(failure)
	target := evt.TargetRoute()
	targetSet := evt.TargetRoutes()
	detail := map[string]any{
		"target_failure_reason": string(failure),
		"source":                evt.SourceRoute(),
		"target":                target,
		"target_set":            targetSet,
		"recipients":            append([]string(nil), plan.Recipients...),
		"persisted_recipients":  append([]string(nil), plan.PersistedRecipients...),
		"delivery_targets":      cloneRouteTargetMap(plan.DeliveryTargets),
	}
	targetContext, _ := json.Marshal(detail)
	deadLetterRoute := target
	if deadLetterRoute.Empty() && len(targetSet) > 0 {
		deadLetterRoute = targetSet[0]
	}
	if deadLetterRoute.Empty() {
		deadLetterRoute = evt.SourceRoute()
	}
	return message, detail, runtimedeadletters.Record{
		OriginalEventID:     strings.TrimSpace(evt.ID),
		OriginalEvent:       strings.TrimSpace(string(evt.Type)),
		OriginalPayload:     evt.Payload,
		EntityID:            firstNonEmptyString(deadLetterRoute.EntityID, evt.EntityID()),
		FlowInstance:        firstNonEmptyString(deadLetterRoute.FlowInstance, evt.FlowInstance(), "runtime"),
		FailureType:         "target_resolution_failed",
		TargetFailureReason: string(failure),
		TargetContext:       json.RawMessage(targetContext),
		ErrorMessage:        message,
		RetryCount:          0,
		ChainDepth:          evt.ChainDepth,
		HandlerNode:         "pin_routing",
	}
}

func cloneRouteTargetMap(in map[string]events.RouteIdentity) map[string]events.RouteIdentity {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]events.RouteIdentity, len(in))
	for recipient, target := range in {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			continue
		}
		out[recipient] = target.Normalized()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
