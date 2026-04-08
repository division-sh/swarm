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
		recipients, err := o.persistedRecipientsForIntent(ctx, *intent)
		if err != nil {
			return err
		}
		if err := txStore.AppendEventTx(ctx, tx, intent.Event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if err := txStore.InsertEventDeliveriesTx(ctx, tx, intent.Event.ID, recipients); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	return nil
}

func (o engineOutbox) persistedRecipientsForIntent(ctx context.Context, intent runtimeengine.EmitIntent) ([]string, error) {
	if len(intent.Recipients) > 0 {
		return uniqueStrings(intent.Recipients), nil
	}
	plan, err := o.bus.deliveryPlanner.Plan(ctx, intent.Event)
	if err != nil {
		return nil, err
	}
	o.bus.recordPublishDiagnostic(ctx, intent.Event, plan)
	return plan.PersistedRecipients, nil
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
		recipientReader, err := runtimereplayclaim.RequireRecipientReader(d.bus.store)
		if err != nil {
			return err
		}
		recipients, err := d.bus.authoritativeRecipientsForEvent(ctx, recipientReader, intent.Event.ID)
		if err != nil {
			return err
		}
		intent.Recipients = recipients
	}
	recipients := uniqueStrings(intent.Recipients)
	if len(recipients) > 0 {
		if err := d.bus.deliverToAgents(ctx, intent.Event, recipients); err != nil {
			return err
		}
		d.bus.logRuntime(ctx, "debug", "Deferred event intent was delivered", "eventbus", "delivered", intent.Event.ID, string(intent.Event.Type), "", intent.Event.EntityID(), "", nil, map[string]any{
			"direct":           true,
			"recipients_count": len(recipients),
		}, "", 0)
	}
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
