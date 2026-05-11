package bus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimedelivery "swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type pipelineTransitionSchemaCapabilityProvider interface {
	CanonicalEventReceiptsCapability(context.Context) (bool, error)
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

var ErrRuntimeIngressPaused = errors.New("runtime ingress is paused")

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

func runtimeIngressDispatchBypass(evt events.Event) bool {
	if strings.TrimSpace(evt.SourceAgent) != "runtime" {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "platform.")
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
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
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
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

	// Interceptors execute before fan-out and can consume the event.
	// Deferred events are persisted after the inbound event commits.
	if pass, out, ierr := eb.runInterceptors(ictx, evt); ierr != nil {
		return ierr
	} else {
		passthrough = pass
		deferred = out
	}

	if passthrough {
		var publishQueued bool
		if publishQueued, err = eb.publishSubscribedNonTransactional(ictx, evt); err != nil {
			return err
		}
		queued = publishQueued
		persisted = true
	} else {
		if err := eb.persistEventRecord(ictx, evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return err
		}
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
	if len(inboundPlan.PersistedRecipients) > 0 {
		if err := txStore.InsertEventDeliveriesTx(txctx, tx, evt.ID, inboundPlan.PersistedRecipients); err != nil {
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
	passthrough, deferred, err := eb.runInterceptors(txctx, evt)
	if err != nil {
		return err
	}
	if !passthrough {
		return errors.New("transactional publish interceptors cannot consume v1 API events")
	}
	if len(deferred) > 0 {
		return errors.New("transactional publish interceptors cannot defer v1 API events")
	}
	status, errText := pipelineReceiptStatus(txctx, nil)
	if err := txStore.UpsertPipelineReceiptTx(txctx, tx, evt.ID, status, errText); err != nil {
		return fmt.Errorf("persist pipeline receipt: %w", err)
	}
	return nil
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
	converger, ok := eb.store.(StandaloneRuntimePlatformRunConvergencePersistence)
	if !ok || converger == nil {
		return nil
	}
	return converger.ConvergeStandaloneRuntimePlatformRun(ctx, evt)
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
	if len(inboundPlan.PersistedRecipients) > 0 {
		if err := txStore.InsertEventDeliveriesTx(txctx, tx, evt.ID, inboundPlan.PersistedRecipients); err != nil {
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

	queued := false
	if passthrough {
		paused, err := eb.runtimeIngressDispatchPaused(ctx, evt)
		if err != nil {
			return err
		}
		if paused {
			queued = true
			eb.logRuntime(ctx, "debug", "Runtime ingress is paused; event persisted without dispatch", "eventbus", "runtime_ingress_queued", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
				"recipients_count": len(inboundPlan.Recipients),
				"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
			}, "", 0)
		}
	}
	if passthrough && !queued {
		if len(inboundPlan.Recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipients, "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverToAgents(ctx, evt, inboundPlan.Recipients); err != nil {
				return err
			}
			eb.logDelivery(ctx, evt, inboundPlan.Recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *inboundPlan.CycleEscalation); err != nil {
				return err
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))

	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	if queued {
		return nil
	}
	status, errText := pipelineReceiptStatus(txctx, nil)
	if err := txStore.UpsertPipelineReceiptTx(ctx, nil, evt.ID, status, errText); err != nil {
		return fmt.Errorf("persist pipeline receipt: %w", err)
	}
	return nil
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
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		return err
	}
	eb.logQueuedDeliveries(ctx, evt, plan.PersistedRecipients, "matched_agent_subscription", plan.ExtraDetail)
	return nil
}

func (eb *EventBus) deliverSubscribedPublishPlan(ctx context.Context, evt events.Event, plan eventDeliveryPlan) (bool, error) {
	paused, err := eb.runtimeIngressDispatchPaused(ctx, evt)
	if err != nil {
		return false, err
	}
	if paused {
		eb.logRuntime(ctx, "debug", "Runtime ingress is paused; event persisted without dispatch", "eventbus", "runtime_ingress_queued", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
			"recipients_count": len(plan.Recipients),
			"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
		}, "", 0)
		return true, nil
	}
	if len(plan.Recipients) > 0 {
		if err := eb.deliverToAgents(ctx, evt, plan.Recipients); err != nil {
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
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	recipients = uniqueStrings(recipients)
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
	if len(recipients) > 0 {
		if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
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
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients, runtimereplayclaim.CommittedReplayScopeDirect); err != nil {
		return err
	}
	persisted = true
	eb.logQueuedDeliveries(ctx, evt, plan.PersistedRecipients, "direct_publish", plan.ExtraDetail)
	if paused, err := eb.runtimeIngressDispatchPaused(ctx, evt); err != nil {
		return err
	} else if paused {
		queued = true
		eb.logRuntime(ctx, "debug", "Runtime ingress is paused; direct event persisted without dispatch", "eventbus", "runtime_ingress_queued", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
			"direct":           true,
			"recipients_count": len(plan.Recipients),
			"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
		}, "", 0)
		return nil
	}
	if err := eb.deliverToAgents(ctx, evt, plan.Recipients); err != nil {
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

// PublishPersistedRecipients delivers an already-committed event using the
// persisted agent manifest plus the authoritative committed replay scope.
func (eb *EventBus) PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error {
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
	if paused, err := eb.runtimeIngressDispatchPaused(ctx, evt); err != nil {
		return err
	} else if paused {
		return ErrRuntimeIngressPaused
	}
	scope, err := eb.authoritativeReplayScopeForEvent(ctx, evt.ID)
	if err != nil {
		return err
	}
	liveRecipients, internalRecipients, err := eb.replayRecipientsForCommittedEvent(ctx, evt, recipients, scope)
	if err != nil {
		return err
	}
	if len(liveRecipients) == 0 {
		return nil
	}
	if err := eb.deliverToAgents(ctx, evt, liveRecipients); err != nil {
		return err
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
