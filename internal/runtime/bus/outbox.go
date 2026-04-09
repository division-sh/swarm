package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
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
		recipients, internalRecipients, err := o.deliveryRecipientsForIntent(ctx, *intent)
		if err != nil {
			return err
		}
		if err := txStore.AppendEventTx(ctx, tx, intent.Event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if err := txStore.InsertEventDeliveriesTx(ctx, tx, intent.Event.ID, recipients); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
		o.bus.setPendingInternalRecipients(intent.Event.ID, internalRecipients)
	}
	return nil
}

func (o engineOutbox) deliveryRecipientsForIntent(ctx context.Context, intent runtimeengine.EmitIntent) ([]string, []string, error) {
	if len(intent.Recipients) > 0 {
		plan, err := o.bus.deliveryPlanner.PlanDirect(ctx, intent.Event, intent.Recipients)
		if err != nil {
			return nil, nil, err
		}
		return plan.PersistedRecipients, nil, nil
	}
	plan, err := o.bus.deliveryPlanner.Plan(ctx, intent.Event)
	if err != nil {
		return nil, nil, err
	}
	o.bus.recordPublishDiagnostic(ctx, intent.Event, plan)
	return plan.PersistedRecipients, filterOutAgentIDs(plan.Recipients, plan.PersistedRecipients), nil
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
		if err := d.dispatchIntent(ctx, intent); err != nil {
			if !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				d.bus.markPipelineReceipt(ctx, intent.Event.ID, "error", err.Error())
			}
			return err
		}
		d.bus.markPipelineReceipt(ctx, intent.Event.ID, "processed", "")
	}
	return nil
}

func (d engineDispatcher) dispatchIntent(ctx context.Context, intent runtimeengine.EmitIntent) error {
	if intent.Recipients == nil {
		passthrough, deferred, err := d.bus.runInterceptors(ctx, intent.Event)
		if err != nil {
			return err
		}
		for _, next := range deferred {
			if err := d.bus.publishDeferred(ctx, next); err != nil {
				return err
			}
		}
		if !passthrough {
			return nil
		}
	}
	recipients, err := d.bus.authoritativeRecipientsForEvent(ctx, intent.Event.ID)
	if err != nil {
		if len(intent.Recipients) > 0 && errors.Is(err, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable) {
			return d.dispatchExplicitDirectIntent(ctx, intent)
		}
		return err
	}
	internalRecipients := d.bus.pendingInternalRecipients(intent.Event.ID)
	liveRecipients := uniqueStrings(append(append([]string(nil), recipients...), internalRecipients...))
	if len(liveRecipients) == 0 {
		d.bus.clearPendingInternalRecipients(intent.Event.ID)
		return nil
	}
	if err := d.bus.deliverToAgents(ctx, intent.Event, liveRecipients); err != nil {
		return err
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
	return nil
}

func (d engineDispatcher) dispatchExplicitDirectIntent(ctx context.Context, intent runtimeengine.EmitIntent) error {
	plan, err := d.bus.deliveryPlanner.PlanDirect(ctx, intent.Event, intent.Recipients)
	if err != nil {
		return err
	}
	if err := d.bus.deliverToAgents(ctx, intent.Event, plan.Recipients); err != nil {
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
