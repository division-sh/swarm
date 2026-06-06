package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type engineOutbox struct {
	bus *EventBus
}

type engineDispatcher struct {
	bus *EventBus
}

func (eb *EventBus) EngineOutbox() runtimeengine.OutboxWriter {
	if eb == nil {
		return nil
	}
	return engineOutbox{bus: eb}
}

func (eb *EventBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	if eb == nil {
		return nil
	}
	return engineDispatcher{bus: eb}
}

func (o engineOutbox) WriteOutbox(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if o.bus == nil || len(intents) == 0 {
		return nil
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return nil
	}
	txStore, ok := o.bus.store.(TransactionalEventStore)
	if !ok {
		return fmt.Errorf("event bus store does not support transactional outbox")
	}
	for i := range intents {
		intent := &intents[i]
		intent.Event = normalizeOutboxEvent(intent.Event)
		if strings.TrimSpace(string(intent.Event.Type())) == "" {
			continue
		}
		plan, err := o.deliveryPlanForIntent(ctx, *intent)
		if err != nil {
			return err
		}
		if err := txStore.AppendEventTx(ctx, tx, intent.Event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if err := o.bus.insertEventDeliveriesTx(ctx, txStore, tx, intent.Event.ID(), plan.PersistedRecipientIDs(), plan.DeliveryTargets(), plan.DeliveryRoutes()); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
		if scopeWriter, ok := o.bus.store.(TransactionalEventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScopeTx(ctx, tx, intent.Event.ID(), replayScopeForEmitIntent(*intent)); err != nil {
				return fmt.Errorf("persist committed replay scope: %w", err)
			}
		} else if replayScopePersistenceRequired(o.bus.store) {
			return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		if plan.TargetFailure != "" {
			if err := txStore.UpsertPipelineReceiptTx(ctx, tx, intent.Event.ID(), "dead_letter", targetDeliveryFailureMessage(plan.TargetFailure)); err != nil {
				return fmt.Errorf("persist pipeline receipt: %w", err)
			}
			failedEvent := intent.Event
			failedPlan := plan
			runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
				o.bus.recordTargetDeliveryFailure(context.WithoutCancel(ctx), failedEvent, failedPlan)
			})
		}
		o.bus.setPendingInternalDeliveryRoutes(intent.Event.ID(), plan.InternalDeliveryRoutes())
	}
	return nil
}

func (o engineOutbox) deliveryPlanForIntent(ctx context.Context, intent runtimeengine.EmitIntent) (RoutePlan, error) {
	if len(intent.Recipients) > 0 {
		return o.bus.planDirectRoutePlan(ctx, intent.Event, intent.Recipients)
	}
	return o.bus.planSubscribedRoutePlan(ctx, intent.Event, true)
}

func (d engineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if d.bus == nil || len(intents) == 0 {
		return nil
	}
	for i := range intents {
		intents[i].Event = normalizeOutboxEvent(intents[i].Event)
	}
	if runtimepipeline.CollectPipelineEmitIntents(ctx, intents) {
		return nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queuedIntents := clonePostCommitEmitIntents(intents)
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
			postCommitActions := make([]func(), 0, 4)
			dispatchCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
			dispatchCtx = runtimepipeline.WithPipelinePostCommitActions(dispatchCtx, &postCommitActions)
			if err := d.DispatchPostCommit(dispatchCtx, queuedIntents); err != nil {
				d.bus.logRuntime(dispatchCtx, "error", "Post-commit outbox dispatch failed", "eventbus", "post_commit_outbox_dispatch_failed", "", "", "", "", "", nil, map[string]any{
					"error":         err.Error(),
					"intents_count": len(queuedIntents),
				}, err.Error(), 0)
			}
			runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
		}) {
			return errors.New("post-commit dispatch requires pipeline post-commit actions when a SQL transaction is active")
		}
		return nil
	}
	for _, intent := range intents {
		queued, err := d.dispatchIntent(ctx, intent)
		if err != nil {
			if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "error", err.Error())
			}
			return err
		}
		if queued {
			continue
		}
		d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "processed", "")
	}
	return nil
}

func clonePostCommitEmitIntents(intents []runtimeengine.EmitIntent) []runtimeengine.EmitIntent {
	if len(intents) == 0 {
		return nil
	}
	cloned := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		copyIntent := intent
		copyIntent.Event = clonePostCommitEvent(intent.Event)
		if intent.Recipients != nil {
			copyIntent.Recipients = append([]string(nil), intent.Recipients...)
		}
		cloned = append(cloned, copyIntent)
	}
	return cloned
}

func clonePostCommitEvent(evt events.Event) events.Event {
	return events.NewProjectionEvent(
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		evt.RunID(),
		evt.ParentEventID(),
		evt.NormalizedEnvelope(),
		evt.CreatedAt(),
	)
}

func (d engineDispatcher) dispatchIntent(ctx context.Context, intent runtimeengine.EmitIntent) (bool, error) {
	if reason, err := d.bus.dispatchQueueReason(ctx, intent.Event); err != nil {
		return false, err
	} else if reason != "" {
		d.bus.logDispatchQueued(ctx, reason, intent.Event, len(intent.Recipients), len(intent.Recipients) > 0, false)
		return true, nil
	}
	if intent.Recipients == nil {
		passthrough, deferred, err := d.bus.runInterceptors(ctx, intent.Event)
		if err != nil {
			return false, err
		}
		for _, next := range deferred {
			if err := d.bus.publishDeferred(ctx, next); err != nil {
				return false, err
			}
		}
		if !passthrough {
			return false, nil
		}
	}
	recipients, err := d.bus.authoritativeRecipientsForEvent(ctx, intent.Event.ID())
	if err != nil {
		if len(intent.Recipients) > 0 && errors.Is(err, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable) {
			return false, d.dispatchExplicitDirectIntent(ctx, intent)
		}
		return false, err
	}
	deliveryRoutes := d.bus.deliveryRoutesForEvent(ctx, intent.Event.ID())
	pendingInternalRoutes := d.bus.pendingInternalDeliveryRoutes(intent.Event.ID())
	deliveryRoutes = events.NormalizeDeliveryRoutes(append(deliveryRoutes, pendingInternalRoutes...))
	if len(recipients) > 0 {
		deliveryRoutes = events.NormalizeDeliveryRoutes(append(deliveryRoutes, deliveryRoutesFromTargetMap(recipients, "agent", d.bus.deliveryTargetsForEvent(ctx, intent.Event.ID()))...))
	}
	internalRecipients := deliveryRouteRecipientIDsByType(pendingInternalRoutes, "node")
	if len(internalRecipients) == 0 {
		internalRecipients = deliveryRouteRecipientIDsByType(deliveryRoutes, "node")
	}
	liveRecipients := uniqueStrings(append(append([]string(nil), recipients...), internalRecipients...))
	if len(deliveryRoutes) > 0 && len(pendingInternalRoutes) == 0 {
		liveRecipients = uniqueStrings(append(deliveryRouteRecipientIDs(deliveryRoutes), recipients...))
	}
	if len(liveRecipients) == 0 {
		d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
		if intent.Event.HasTargetRoute() {
			plan := newRoutePlan(intent.Event)
			plan.TargetFailure = runtimepinrouting.FailureTargetNotSubscribed
			plan = plan.Normalized()
			d.bus.recordTargetDeliveryFailure(ctx, intent.Event, plan)
			d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "dead_letter", targetDeliveryFailureMessage(plan.TargetFailure))
			return true, nil
		}
		return false, nil
	}
	if len(deliveryRoutes) == 0 {
		deliveryRoutes = deliveryRoutesFromTargetMap(recipients, "agent", d.bus.deliveryTargetsForEvent(ctx, intent.Event.ID()))
	}
	if err := d.bus.deliverToRecipientsWithRoutes(ctx, intent.Event, liveRecipients, deliveryRoutes); err != nil {
		return false, err
	}
	d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
	d.bus.logRuntime(ctx, "debug", "Persisted event intent was delivered", "eventbus", "delivered", intent.Event.ID(), string(intent.Event.Type()), "", intent.Event.EntityID(), "", nil, map[string]any{
		"direct":                     true,
		"delivery_manifest_owner":    "event_deliveries+in_memory_internal",
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            intent.Event.ParentEventID(),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
	}, "", 0)
	return false, nil
}

func (d engineDispatcher) dispatchExplicitDirectIntent(ctx context.Context, intent runtimeengine.EmitIntent) error {
	plan, err := d.bus.planDirectRoutePlan(ctx, intent.Event, intent.Recipients)
	if err != nil {
		return err
	}
	recipients := plan.RecipientIDs()
	if err := d.bus.deliverToRecipientsWithRoutes(ctx, intent.Event, recipients, plan.DeliveryRoutes()); err != nil {
		return err
	}
	detail := map[string]any{
		"direct":           true,
		"recipients_count": len(recipients),
		"parent_event_id":  intent.Event.ParentEventID(),
	}
	for k, v := range plan.ExtraDetail {
		detail[k] = v
	}
	d.bus.logRuntime(ctx, "debug", "Deferred direct event intent was delivered", "eventbus", "delivered", intent.Event.ID(), string(intent.Event.Type()), "", intent.Event.EntityID(), "", nil, detail, "", 0)
	return nil
}

func normalizeOutboxEvent(evt events.Event) events.Event {
	return eventWithPublishDefaults(evt, time.Now().UTC())
}

func replayScopeForEmitIntent(intent runtimeengine.EmitIntent) runtimereplayclaim.CommittedReplayScope {
	if len(intent.Recipients) > 0 {
		return runtimereplayclaim.CommittedReplayScopeDirect
	}
	return runtimereplayclaim.CommittedReplayScopeSubscribed
}

func (eb *EventBus) setPendingInternalDeliveryRoutes(eventID string, deliveryRoutes []events.DeliveryRoute) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eventID == "" || len(deliveryRoutes) == 0 {
		delete(eb.pendingInternalByID, eventID)
		return
	}
	eb.pendingInternalByID[eventID] = append([]events.DeliveryRoute(nil), deliveryRoutes...)
}

func (eb *EventBus) pendingInternalDeliveryRoutes(eventID string) []events.DeliveryRoute {
	if eb == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return append([]events.DeliveryRoute(nil), eb.pendingInternalByID[eventID]...)
}

func (eb *EventBus) clearPendingInternalDeliveryRoutes(eventID string) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	delete(eb.pendingInternalByID, eventID)
}
