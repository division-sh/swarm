package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimepinrouting "swarm/internal/runtime/core/pinrouting"
	runtimeengine "swarm/internal/runtime/engine"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
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
		if strings.TrimSpace(string(intent.Event.Type)) == "" {
			continue
		}
		recipients, deliveryTargets, internalRecipients, targetFailure, err := o.deliveryRecipientsForIntent(ctx, *intent)
		if err != nil {
			return err
		}
		if err := txStore.AppendEventTx(ctx, tx, intent.Event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if err := o.bus.insertEventDeliveriesTx(ctx, txStore, tx, intent.Event.ID, recipients, deliveryTargets); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
		if scopeWriter, ok := o.bus.store.(TransactionalEventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScopeTx(ctx, tx, intent.Event.ID, replayScopeForEmitIntent(*intent)); err != nil {
				return fmt.Errorf("persist committed replay scope: %w", err)
			}
		} else if replayScopePersistenceRequired(o.bus.store) {
			return fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		if targetFailure != "" {
			if err := txStore.UpsertPipelineReceiptTx(ctx, tx, intent.Event.ID, "dead_letter", targetDeliveryFailureMessage(targetFailure)); err != nil {
				return fmt.Errorf("persist pipeline receipt: %w", err)
			}
			failedEvent := intent.Event
			plan := eventDeliveryPlan{
				Event:               failedEvent,
				PersistedRecipients: recipients,
				DeliveryTargets:     deliveryTargets,
				TargetFailure:       targetFailure,
			}
			runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
				o.bus.recordTargetDeliveryFailure(context.WithoutCancel(ctx), failedEvent, plan)
			})
		}
		o.bus.setPendingInternalRecipients(intent.Event.ID, internalRecipients)
	}
	return nil
}

func (o engineOutbox) deliveryRecipientsForIntent(ctx context.Context, intent runtimeengine.EmitIntent) ([]string, map[string]events.RouteIdentity, []string, runtimepinrouting.TargetFailure, error) {
	if len(intent.Recipients) > 0 {
		plan, err := o.bus.deliveryPlanner.PlanDirect(ctx, intent.Event, intent.Recipients)
		if err != nil {
			return nil, nil, nil, "", err
		}
		return plan.PersistedRecipients, plan.DeliveryTargets, nil, plan.TargetFailure, nil
	}
	plan, err := o.bus.deliveryPlanner.Plan(ctx, intent.Event)
	if err != nil {
		return nil, nil, nil, "", err
	}
	o.bus.recordPublishDiagnostic(ctx, intent.Event, plan)
	return plan.PersistedRecipients, plan.DeliveryTargets, filterOutAgentIDs(plan.Recipients, plan.PersistedRecipients), plan.TargetFailure, nil
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
	for _, intent := range intents {
		queued, err := d.dispatchIntent(ctx, intent)
		if err != nil {
			if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				d.bus.markPipelineReceipt(ctx, intent.Event.ID, "error", err.Error())
			}
			return err
		}
		if queued {
			continue
		}
		d.bus.markPipelineReceipt(ctx, intent.Event.ID, "processed", "")
	}
	return nil
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
	recipients, err := d.bus.authoritativeRecipientsForEvent(ctx, intent.Event.ID)
	if err != nil {
		if len(intent.Recipients) > 0 && errors.Is(err, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable) {
			return false, d.dispatchExplicitDirectIntent(ctx, intent)
		}
		return false, err
	}
	internalRecipients := d.bus.pendingInternalRecipients(intent.Event.ID)
	liveRecipients := uniqueStrings(append(append([]string(nil), recipients...), internalRecipients...))
	if len(liveRecipients) == 0 {
		d.bus.clearPendingInternalRecipients(intent.Event.ID)
		if intent.Event.HasTargetRoute() {
			plan := eventDeliveryPlan{Event: intent.Event, TargetFailure: runtimepinrouting.FailureTargetNotSubscribed}
			d.bus.recordTargetDeliveryFailure(ctx, intent.Event, plan)
			d.bus.markPipelineReceipt(ctx, intent.Event.ID, "dead_letter", targetDeliveryFailureMessage(plan.TargetFailure))
			return true, nil
		}
		return false, nil
	}
	deliveryTargets := d.bus.deliveryTargetsForEvent(ctx, intent.Event.ID)
	if err := d.bus.deliverToAgentsWithTargets(ctx, intent.Event, liveRecipients, deliveryTargets); err != nil {
		return false, err
	}
	d.bus.clearPendingInternalRecipients(intent.Event.ID)
	d.bus.logRuntime(ctx, "debug", "Persisted event intent was delivered", "eventbus", "delivered", intent.Event.ID, string(intent.Event.Type), "", intent.Event.EntityID(), "", nil, map[string]any{
		"direct":                     true,
		"delivery_manifest_owner":    "event_deliveries+in_memory_internal",
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            strings.TrimSpace(intent.Event.ParentEventID),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
	}, "", 0)
	return false, nil
}

func (d engineDispatcher) dispatchExplicitDirectIntent(ctx context.Context, intent runtimeengine.EmitIntent) error {
	plan, err := d.bus.deliveryPlanner.PlanDirect(ctx, intent.Event, intent.Recipients)
	if err != nil {
		return err
	}
	if err := d.bus.deliverToAgentsWithTargets(ctx, intent.Event, plan.Recipients, plan.DeliveryTargets); err != nil {
		return err
	}
	detail := map[string]any{
		"direct":           true,
		"recipients_count": len(plan.Recipients),
		"parent_event_id":  strings.TrimSpace(intent.Event.ParentEventID),
	}
	for k, v := range plan.ExtraDetail {
		detail[k] = v
	}
	d.bus.logRuntime(ctx, "debug", "Deferred direct event intent was delivered", "eventbus", "delivered", intent.Event.ID, string(intent.Event.Type), "", intent.Event.EntityID(), "", nil, detail, "", 0)
	return nil
}

func normalizeOutboxEvent(evt events.Event) events.Event {
	if strings.TrimSpace(evt.ID) == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	return evt
}

func replayScopeForEmitIntent(intent runtimeengine.EmitIntent) runtimereplayclaim.CommittedReplayScope {
	if len(intent.Recipients) > 0 {
		return runtimereplayclaim.CommittedReplayScopeDirect
	}
	return runtimereplayclaim.CommittedReplayScopeSubscribed
}

func (eb *EventBus) setPendingInternalRecipients(eventID string, recipients []string) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	recipients = uniqueStrings(recipients)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eventID == "" || len(recipients) == 0 {
		delete(eb.pendingInternalByID, eventID)
		return
	}
	eb.pendingInternalByID[eventID] = append([]string(nil), recipients...)
}

func (eb *EventBus) pendingInternalRecipients(eventID string) []string {
	if eb == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return append([]string(nil), eb.pendingInternalByID[eventID]...)
}

func (eb *EventBus) clearPendingInternalRecipients(eventID string) {
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
