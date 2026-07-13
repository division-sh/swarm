package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

type InboundDeliveryClaim struct {
	ProviderEventID string
	EntityID        string
	Provider        string
}

type InboundDeliveryBatch struct {
	Claim                     InboundDeliveryClaim
	Events                    []events.Event
	AcknowledgeBeforeDispatch bool
}

type InboundDeliveryBatchResult struct {
	Duplicate bool
}

type acknowledgedInboundMutationContextKey struct{}

func withAcknowledgedInboundMutation(ctx context.Context) context.Context {
	return context.WithValue(ctx, acknowledgedInboundMutationContextKey{}, true)
}

func acknowledgedInboundMutation(ctx context.Context) bool {
	value, _ := ctx.Value(acknowledgedInboundMutationContextKey{}).(bool)
	return value
}

// PublishInboundDelivery persists one provider marker and every derived event
// through one selected-store mutation. Route planning and template lifecycle
// materialization therefore participate in the same transaction.
func (eb *EventBus) PublishInboundDelivery(ctx context.Context, batch InboundDeliveryBatch) (InboundDeliveryBatchResult, error) {
	result := InboundDeliveryBatchResult{}
	if eb == nil || eb.store == nil {
		return result, fmt.Errorf("event bus store is required")
	}
	if strings.TrimSpace(batch.Claim.ProviderEventID) == "" || strings.TrimSpace(batch.Claim.EntityID) == "" || strings.TrimSpace(batch.Claim.Provider) == "" {
		return result, fmt.Errorf("inbound delivery claim requires provider_event_id, entity_id, and provider")
	}
	if len(batch.Events) == 0 {
		return result, fmt.Errorf("inbound delivery batch requires at least one event")
	}
	runner, ok := eb.store.(EventMutationRunner)
	if !ok || runner == nil {
		return result, fmt.Errorf("typed event mutation runner is required for inbound delivery")
	}
	err := runner.RunEventMutation(ctx, func(mutation EventMutation) error {
		inbound, ok := mutation.(InboundDeliveryMutation)
		if !ok || inbound == nil {
			return fmt.Errorf("selected-store event mutation does not support inbound delivery claims")
		}
		txctx := mutation.Context()
		if batch.AcknowledgeBeforeDispatch {
			txctx = withAcknowledgedInboundMutation(txctx)
		}
		inserted, err := inbound.ClaimInboundEvent(txctx, batch.Claim.ProviderEventID, batch.Claim.EntityID, batch.Claim.Provider)
		if err != nil {
			return err
		}
		if !inserted {
			result.Duplicate = true
			return nil
		}
		for _, event := range batch.Events {
			if err := eb.PublishInMutation(txctx, event); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}
