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

type pendingOutboxOperation struct {
	sequence uint64
	intent   runtimeengine.EmitIntent
	outcome  EventAppendOutcome
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
	mutation, ok := o.bus.eventMutationFromContext(ctx)
	if !ok || mutation == nil {
		return nil
	}
	ctx = mutation.Context()
	for i := range intents {
		intent := &intents[i]
		if strings.TrimSpace(string(intent.Event.Type())) == "" {
			continue
		}
		intentCtx := events.WithDeliveryContext(ctx, intent.Context)
		var err error
		intent.Event, err = normalizeOutboxEvent(intentCtx, intent.Event)
		if err != nil {
			return err
		}
		intentCtx, err = o.bus.withAuthorActivityEventDescriptor(intentCtx, intent.Event)
		if err != nil {
			return err
		}
		appendOutcome, err := appendEventMutationOutcome(intentCtx, mutation, intent.Event)
		if err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if appendOutcome == EventAppendExactDuplicate {
			o.bus.stagePendingOutboxOperation(ctx, *intent, appendOutcome)
			continue
		}
		plan, err := o.deliveryPlanForIntent(intentCtx, *intent)
		if err != nil {
			return err
		}
		if err := o.bus.insertEventDeliveriesMutation(ctx, mutation, intent.Event.ID(), plan.PersistedRecipientIDs(), plan.DeliveryTargets(), plan.DeliveryRoutes()); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
		if err := o.bus.upsertCommittedReplayScopeMutation(ctx, mutation, intent.Event.ID(), replayScopeForEmitIntent(*intent)); err != nil {
			return err
		}
		if plan.TargetFailure != "" {
			if err := mutation.UpsertPipelineReceipt(ctx, intent.Event.ID(), "dead_letter", targetDeliveryFailureEnvelope(plan.TargetFailure)); err != nil {
				return fmt.Errorf("persist pipeline receipt: %w", err)
			}
			if err := o.bus.recordTargetDeliveryFailureMutation(ctx, mutation, intent.Event, plan); err != nil {
				return err
			}
		}
		if o.bus.testLifecycleProbe != nil {
			event := intent.Event
			routePlan := plan
			runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
				o.bus.notifyTestPublishPersisted(context.WithoutCancel(ctx), event, routePlan)
			})
		}
		o.bus.setPendingInternalDeliveryRoutes(intent.Event.ID(), plan.InternalDeliveryRoutes())
		o.bus.stagePendingOutboxOperation(ctx, *intent, appendOutcome)
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
	normalized := make([]runtimeengine.EmitIntent, 0, len(intents))
	for i := range intents {
		if strings.TrimSpace(string(intents[i].Event.Type())) == "" {
			continue
		}
		intent := intents[i]
		var err error
		intent.Event, err = normalizeOutboxEvent(ctx, intent.Event)
		if err != nil {
			return err
		}
		normalized = append(normalized, intent)
	}
	intents = normalized
	if len(intents) == 0 {
		return nil
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
					"intents_count": len(queuedIntents),
				}, eventBusDependencyFailure(err, "post_commit_outbox_dispatch_failed", "dispatch_outbox"), 0)
			}
			runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
		}) {
			return errors.New("post-commit dispatch requires pipeline post-commit actions when a SQL transaction is active")
		}
		return nil
	}
	for _, intent := range intents {
		handled, err := d.dispatchPendingOutboxOperation(ctx, intent)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		if err := d.dispatchAndRecord(ctx, intent); err != nil {
			return err
		}
	}
	return nil
}

func (d engineDispatcher) dispatchPendingOutboxOperation(ctx context.Context, fallback runtimeengine.EmitIntent) (bool, error) {
	operation, ok := d.bus.takePendingOutboxOperation(fallback.Event.ID())
	if !ok {
		return false, nil
	}
	if operation.intent.Event.Type() != fallback.Event.Type() {
		return true, fmt.Errorf("pending outbox event type mismatch for %s: persisted=%s dispatch=%s", fallback.Event.ID(), operation.intent.Event.Type(), fallback.Event.Type())
	}
	if operation.outcome == EventAppendExactDuplicate {
		return true, nil
	}
	if operation.outcome != EventAppendInserted {
		return true, errors.New("pending outbox operation has invalid append outcome")
	}
	return true, d.dispatchAndRecord(ctx, operation.intent)
}

func (d engineDispatcher) dispatchAndRecord(ctx context.Context, intent runtimeengine.EmitIntent) error {
	queued, err := d.dispatchIntent(ctx, intent)
	if err != nil {
		if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
			d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "error", eventBusFailure(err, "dispatch_outbox"))
		}
		return err
	}
	if !queued {
		d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "processed", nil)
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
	).WithExecutionMode(evt.ExecutionMode())
}

func (d engineDispatcher) dispatchIntent(ctx context.Context, intent runtimeengine.EmitIntent) (bool, error) {
	ctx = events.WithDeliveryContext(ctx, intent.Context)
	if reason, err := d.bus.dispatchQueueReason(ctx, intent.Event); err != nil {
		return false, err
	} else if reason != "" {
		d.bus.logDispatchQueued(ctx, reason, intent.Event, len(intent.Recipients), len(intent.Recipients) > 0, false)
		return true, nil
	}
	deliveryRoutes := d.bus.deliveryRoutesForPostCommitIntent(ctx, intent.Event.ID())
	if intent.Recipients == nil {
		passthrough, deferred, err := d.bus.runInterceptorsForDeliveryRoutes(ctx, intent.Event, deliveryRoutes)
		if err != nil {
			return false, err
		}
		for _, next := range deferred {
			if err := d.bus.publishDeferred(ctx, next); err != nil {
				return false, err
			}
		}
		if !passthrough {
			d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
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
	pendingInternalRoutes := d.bus.pendingInternalDeliveryRoutes(intent.Event.ID())
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
			d.bus.markPipelineReceipt(ctx, intent.Event.ID(), "dead_letter", targetDeliveryFailureEnvelope(plan.TargetFailure))
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
	}, nil, 0)
	return false, nil
}

func (eb *EventBus) deliveryRoutesForPostCommitIntent(ctx context.Context, eventID string) []events.DeliveryRoute {
	persistedRoutes := eb.deliveryRoutesForEvent(ctx, eventID)
	pendingInternalRoutes := eb.pendingInternalDeliveryRoutes(eventID)
	return events.NormalizeDeliveryRoutes(append(persistedRoutes, pendingInternalRoutes...))
}

func (d engineDispatcher) dispatchExplicitDirectIntent(ctx context.Context, intent runtimeengine.EmitIntent) error {
	ctx = events.WithDeliveryContext(ctx, intent.Context)
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
	d.bus.logRuntime(ctx, "debug", "Deferred direct event intent was delivered", "eventbus", "delivered", intent.Event.ID(), string(intent.Event.Type()), "", intent.Event.EntityID(), "", nil, detail, nil, 0)
	return nil
}

func normalizeOutboxEvent(ctx context.Context, evt events.Event) (events.Event, error) {
	_, admitted, err := admitEventForPublish(ctx, evt, time.Now().UTC(), "")
	return admitted, err
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

func (eb *EventBus) stagePendingOutboxOperation(ctx context.Context, intent runtimeengine.EmitIntent, outcome EventAppendOutcome) {
	if eb == nil {
		return
	}
	intent.Event = clonePostCommitEvent(intent.Event)
	if intent.Recipients != nil {
		intent.Recipients = append([]string(nil), intent.Recipients...)
	}
	eventID := strings.TrimSpace(intent.Event.ID())
	if eventID == "" {
		return
	}
	eb.mu.Lock()
	eb.pendingOutboxSequence++
	sequence := eb.pendingOutboxSequence
	eb.pendingOutboxByID[eventID] = append(eb.pendingOutboxByID[eventID], pendingOutboxOperation{sequence: sequence, intent: intent, outcome: outcome})
	eb.mu.Unlock()
	_ = runtimepipeline.QueuePipelineRollbackAction(ctx, func() {
		eb.removePendingOutboxOperation(eventID, sequence)
	})
}

func (eb *EventBus) takePendingOutboxOperation(eventID string) (pendingOutboxOperation, bool) {
	if eb == nil {
		return pendingOutboxOperation{}, false
	}
	eventID = strings.TrimSpace(eventID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	operations := eb.pendingOutboxByID[eventID]
	if len(operations) == 0 {
		return pendingOutboxOperation{}, false
	}
	operation := operations[0]
	if len(operations) == 1 {
		delete(eb.pendingOutboxByID, eventID)
	} else {
		eb.pendingOutboxByID[eventID] = operations[1:]
	}
	return operation, true
}

func (eb *EventBus) removePendingOutboxOperation(eventID string, sequence uint64) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	operations := eb.pendingOutboxByID[eventID]
	for i := range operations {
		if operations[i].sequence != sequence {
			continue
		}
		operations = append(operations[:i], operations[i+1:]...)
		if len(operations) == 0 {
			delete(eb.pendingOutboxByID, eventID)
		} else {
			eb.pendingOutboxByID[eventID] = operations
		}
		return
	}
}

func (eb *EventBus) clearPendingOutboxOperation(eventID string) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	delete(eb.pendingOutboxByID, strings.TrimSpace(eventID))
	eb.mu.Unlock()
}
