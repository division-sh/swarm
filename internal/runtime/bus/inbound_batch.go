package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type InboundDeliveryClaim struct {
	ProviderEventID string
	EntityID        string
	Provider        string
}

type InboundDeliveryBatch struct {
	Claim                     InboundDeliveryClaim
	Events                    []InboundDeliveryEvent
	AcknowledgeBeforeDispatch bool
}

type InboundDeliveryEvent struct {
	Event         events.Event
	Kind          runtimeprovideroutput.Kind
	Authorization runtimeprovideroutput.Authorization
}

type InboundDeliveryBatchResult struct {
	Duplicate bool
}

// ProviderOutputAuthorizationVerifier is the current immutable verified-pack
// catalog owner used to reject fabricated or stale normalized outputs before a
// selected-store mutation begins.
type ProviderOutputAuthorizationVerifier interface {
	VerifyProviderOutputAuthorization(runtimeprovideroutput.Authorization) error
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
	validated, err := preflightInboundDeliveryBatch(eb.providerOutputAuthorizationVerifier(), batch)
	if err != nil {
		return result, err
	}
	runner, ok := eb.store.(EventMutationRunner)
	if !ok || runner == nil {
		return result, fmt.Errorf("typed event mutation runner is required for inbound delivery")
	}
	err = runner.RunEventMutation(ctx, func(mutation EventMutation) error {
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
		for _, item := range validated.Events {
			event := item.Event
			if item.Kind == runtimeprovideroutput.KindRaw {
				txctx = withoutProviderOutputAuthorization(txctx)
			} else {
				txctx = withProviderOutputAuthorization(txctx, item.Authorization)
			}
			if err := eb.PublishInMutation(txctx, event); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

func preflightInboundDeliveryBatch(verifier ProviderOutputAuthorizationVerifier, batch InboundDeliveryBatch) (InboundDeliveryBatch, error) {
	claimProvider := strings.TrimSpace(batch.Claim.Provider)
	if strings.TrimSpace(batch.Claim.ProviderEventID) == "" || strings.TrimSpace(batch.Claim.EntityID) == "" || claimProvider == "" {
		return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery claim requires provider_event_id, entity_id, and provider")
	}
	if len(batch.Events) == 0 {
		return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery batch requires at least one event")
	}
	validated := batch
	validated.Claim.Provider = claimProvider
	validated.Events = append([]InboundDeliveryEvent(nil), batch.Events...)
	for index := range validated.Events {
		item := &validated.Events[index]
		authorization := item.Authorization.Normalized()
		switch item.Kind {
		case runtimeprovideroutput.KindRaw:
			if !authorization.Empty() {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d raw provider output must not carry normalized-output authorization", index)
			}
			item.Authorization = runtimeprovideroutput.Authorization{}
		case runtimeprovideroutput.KindNormalized:
			if !authorization.Valid() {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output requires complete verified-pack authorization", index)
			}
			eventName := strings.TrimSpace(string(item.Event.Type()))
			if authorization.Provider != claimProvider || authorization.Event != eventName {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output authorization does not match delivery claim/event", index)
			}
			if verifier == nil {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output has no current compiled authorization owner", index)
			}
			if err := verifier.VerifyProviderOutputAuthorization(authorization); err != nil {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output authorization is stale or mismatched against the current compiled owner: %w", index, err)
			}
			item.Authorization = authorization
		default:
			return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d requires raw or normalized output kind", index)
		}
	}
	return validated, nil
}

type providerOutputAuthorizationContextKey struct{}

func withProviderOutputAuthorization(ctx context.Context, authorization runtimeprovideroutput.Authorization) context.Context {
	return context.WithValue(ctx, providerOutputAuthorizationContextKey{}, authorization.Normalized())
}

func withoutProviderOutputAuthorization(ctx context.Context) context.Context {
	return context.WithValue(ctx, providerOutputAuthorizationContextKey{}, runtimeprovideroutput.Authorization{})
}

func providerOutputAuthorizationMatches(ctx context.Context, expected *runtimeprovideroutput.Authorization) bool {
	if expected == nil {
		return true
	}
	actual, _ := ctx.Value(providerOutputAuthorizationContextKey{}).(runtimeprovideroutput.Authorization)
	return expected.Matches(actual)
}
