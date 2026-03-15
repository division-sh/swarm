package bus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"github.com/google/uuid"
)

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

	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	ictx := runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions)
	if txStore, ok := eb.store.(TransactionalEventStore); ok {
		return eb.publishTransactional(ictx, evt, start, &deferredTransitions, txStore)
	}

	persisted := false
	passthrough := true
	deferred := []events.Event{}
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
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
		if err := eb.routeAndDeliver(ctx, evt); err != nil {
			return err
		}
		persisted = true
	} else {
		if err := eb.persistEventRecord(ctx, evt, nil); err != nil {
			return err
		}
		persisted = true
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
	runtimepipeline.FlushDeferredPipelineTransitions(ctx, deferredTransitions)
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	return nil
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
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	passthrough, deferred, err := eb.runInterceptors(txctx, evt)
	if err != nil {
		return err
	}
	var inboundPlan eventDeliveryPlan
	if passthrough {
		inboundPlan, err = eb.buildDeliveryPlan(txctx, evt)
		if err != nil {
			return err
		}
	}

	if err := txStore.AppendEventTx(txctx, tx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if passthrough && len(inboundPlan.PersistedRecipients) > 0 {
		if err := txStore.InsertEventDeliveriesTx(txctx, tx, evt.ID, inboundPlan.PersistedRecipients); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if err := txStore.UpsertPipelineReceiptTx(txctx, tx, evt.ID, "processed", ""); err != nil {
		return fmt.Errorf("persist pipeline receipt: %w", err)
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
			eb.deliverToAgents(ctx, evt, inboundPlan.Recipients)
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
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	passthrough, deferred, err := eb.runInterceptors(ctx, evt)
	if err != nil {
		return err
	}
	if passthrough {
		if err := eb.routeAndDeliver(ctx, evt); err != nil {
			return err
		}
		persisted = true
	} else {
		if err := eb.persistEventRecord(ctx, evt, nil); err != nil {
			return err
		}
		persisted = true
	}
	eb.logPublished(ctx, evt, 0)
	for _, d := range deferred {
		if err := eb.publishDeferredNoIntercept(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) publishDeferredNoIntercept(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
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
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	if err := eb.routeAndDeliver(ctx, evt); err != nil {
		return err
	}
	persisted = true
	eb.logPublished(ctx, evt, 0)
	return nil
}

func (eb *EventBus) logPublished(ctx context.Context, evt events.Event, durationUS int) {
	eb.logRuntime(ctx, "debug", "eventbus", "published", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", "", "", map[string]any{
		"type":   string(evt.Type),
		"source": evt.SourceAgent,
	}, "", durationUS)
}

func (eb *EventBus) routeAndDeliver(ctx context.Context, evt events.Event) error {
	plan, err := eb.buildDeliveryPlan(ctx, evt)
	if err != nil {
		return err
	}
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients); err != nil {
		return err
	}
	if len(plan.Recipients) > 0 {
		eb.deliverToAgents(ctx, evt, plan.Recipients)
		eb.logDelivery(ctx, evt, plan.Recipients, plan.ExtraDetail)
	}
	if plan.BlockedByCycle && plan.CycleEscalation != nil {
		if err := eb.publishDeferred(ctx, *plan.CycleEscalation); err != nil {
			return err
		}
	}
	if strings.TrimSpace(plan.ContradictionReason) != "" {
		_ = eb.emitContradiction(ctx, evt, plan.ContradictionReason)
	}
	return nil
}

func (eb *EventBus) buildDeliveryPlan(ctx context.Context, evt events.Event) (eventDeliveryPlan, error) {
	plan := eventDeliveryPlan{Event: evt}
	// Budget events are broadcast guardrails. Deliver via delivery manifest so
	// active agents also receive them during backlog replay.
	if strings.HasPrefix(string(evt.Type), "budget.") {
		recipients := []string{}
		if lister, ok := eb.store.(ActiveAgentLister); ok {
			if ids, err := lister.ListActiveAgentIDs(ctx); err == nil {
				recipients = ids
			}
		}
		if len(recipients) == 0 {
			// Best-effort fallback: deliver to currently subscribed agents.
			recipients = eb.resolveSubscribedRecipients(string(evt.Type))
		}
		plan.Recipients = uniqueStrings(recipients)
		plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
		return plan, nil
	}

	// Human task events must always reach the requesting agent (even if active
	// and not subscribed) and should also be visible to subscribers like the
	// control-plane roles. Treat them as global events, not entity-routed events.
	if strings.HasPrefix(string(evt.Type), "human_task.") {
		recipients := eb.resolveSubscribedRecipients(string(evt.Type))
		// Only outcome/decision events are forced to the requesting agent; the
		// initial request event is intended for control-plane review.
		switch string(evt.Type) {
		case "human_task.approved",
			"human_task.rejected",
			"human_task.deferred",
			"human_task.completed",
			"human_task.expired":
			recipients = append(recipients, eb.resolveHumanTaskRecipients(evt)...)
		}
		plan.Recipients = uniqueStrings(recipients)
		plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
		return plan, nil
	}

	plan.Recipients = uniqueStrings(append(
		eb.resolveRoutedRecipients(string(evt.Type)),
		eb.resolveSubscribedRecipients(string(evt.Type))...,
	))
	plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
	if len(plan.Recipients) == 0 {
		plan.ContradictionReason = "contract route resolved zero recipients"
	}
	return plan, nil
}

func (eb *EventBus) persistEventRecord(ctx context.Context, evt events.Event, recipients []string) error {
	recipients = uniqueStrings(recipients)
	if atomicStore, ok := eb.store.(AtomicEventPersistence); ok {
		if err := atomicStore.PersistEventWithDeliveries(ctx, evt, recipients); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if len(recipients) == 0 {
		return nil
	}
	if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
		return fmt.Errorf("persist event deliveries: %w", err)
	}
	return nil
}

func (eb *EventBus) logDelivery(ctx context.Context, evt events.Event, recipients []string, extra map[string]any) {
	detail := map[string]any{"recipients_count": len(recipients)}
	for k, v := range extra {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", "", "", detail, "", 0)
}

// PublishDirect persists an event and delivers it to the specified recipients
// regardless of routing tables or subscription patterns. This is the "message"
// primitive: explicit, point-to-point delivery.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	start := time.Now()
	persisted := false
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return errors.New("direct publish recipients are required")
	}
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
	if err := eb.persistEventRecord(ctx, evt, recipients); err != nil {
		return err
	}
	persisted = true
	eb.deliverToAgents(ctx, evt, recipients)
	eb.logRuntime(ctx, "debug", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", "", "", map[string]any{
		"direct":           true,
		"recipients_count": len(recipients),
	}, "", int(time.Since(start)/time.Microsecond))
	return nil
}
