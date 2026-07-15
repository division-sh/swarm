package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type InboundDeliveryBatch struct {
	Provider          string
	AuthorSubjectType string
	AuthorSubjectID   string
	AuthorSummary     string
	Events            []InboundDeliveryEvent
}

type InboundDeliveryEvent struct {
	Event         events.Event
	Kind          runtimeprovideroutput.Kind
	Authorization runtimeprovideroutput.Authorization
}

// ProviderOutputAuthorizationVerifier is the current immutable verified-pack
// catalog owner used to reject fabricated or stale normalized outputs before a
// selected-store mutation begins.
type ProviderOutputAuthorizationVerifier interface {
	VerifyProviderOutputAuthorization(runtimeprovideroutput.Authorization) error
}

// PrepareInboundDeliveryBatchInMutation persists and plans every executable
// event through the inbound publication mutation that already owns the SQL
// transaction. It never claims request identity or opens another transaction.
func (eb *EventBus) PrepareInboundDeliveryBatchInMutation(ctx context.Context, batch InboundDeliveryBatch) ([]PreparedPublish, error) {
	if eb == nil {
		return nil, fmt.Errorf("event bus is required")
	}
	ctx = eb.withBundleFingerprint(ctx)
	validated, err := preflightInboundDeliveryBatch(eb.providerOutputAuthorizationVerifier(), batch)
	if err != nil {
		return nil, err
	}
	mutation, ok := eb.eventMutationFromContext(ctx)
	if !ok || mutation == nil {
		return nil, fmt.Errorf("typed event mutation context is required for inbound delivery")
	}
	txctx, err := eb.withTransactionRouteOverlay(mutation.Context())
	if err != nil {
		return nil, err
	}
	txctx = runtimeauthoractivity.WithInboundProjection(txctx, runtimeauthoractivity.InboundProjection{
		SubjectType: validated.AuthorSubjectType,
		SubjectID:   validated.AuthorSubjectID,
		Summary:     validated.AuthorSummary,
	})
	prepared := make([]PreparedPublish, 0, len(validated.Events))
	for _, item := range validated.Events {
		itemCtx := txctx
		if item.Kind == runtimeprovideroutput.KindRaw {
			itemCtx = withoutProviderOutputAuthorization(itemCtx)
		} else {
			itemCtx = withProviderOutputAuthorization(itemCtx, item.Authorization)
		}
		publication, err := eb.PreparePublishInMutation(itemCtx, item.Event)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, publication)
	}
	return prepared, nil
}

func preflightInboundDeliveryBatch(verifier ProviderOutputAuthorizationVerifier, batch InboundDeliveryBatch) (InboundDeliveryBatch, error) {
	provider := strings.TrimSpace(batch.Provider)
	if provider == "" {
		return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery batch requires provider")
	}
	if len(batch.Events) < 1 || len(batch.Events) > 2 {
		return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery batch requires raw plus zero or one normalized event")
	}
	validated := batch
	validated.Provider = provider
	validated.AuthorSubjectType = strings.TrimSpace(validated.AuthorSubjectType)
	validated.AuthorSubjectID = strings.TrimSpace(validated.AuthorSubjectID)
	validated.AuthorSummary = strings.TrimSpace(validated.AuthorSummary)
	if (validated.AuthorSubjectType == "") != (validated.AuthorSubjectID == "") {
		return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery author subject requires type and id together")
	}
	validated.Events = append([]InboundDeliveryEvent(nil), batch.Events...)
	for index := range validated.Events {
		item := &validated.Events[index]
		authorization := item.Authorization.Normalized()
		switch item.Kind {
		case runtimeprovideroutput.KindRaw:
			if index != 0 {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery raw provider output must be ordinal 0")
			}
			if !authorization.Empty() {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d raw provider output must not carry normalized-output authorization", index)
			}
			item.Authorization = runtimeprovideroutput.Authorization{}
		case runtimeprovideroutput.KindNormalized:
			if index != 1 {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery normalized provider output must be ordinal 1")
			}
			if !authorization.Valid() {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output requires complete verified-pack authorization", index)
			}
			eventName := strings.TrimSpace(string(item.Event.Type()))
			if authorization.Provider != provider || authorization.Event != eventName {
				return InboundDeliveryBatch{}, fmt.Errorf("inbound delivery event %d normalized provider output authorization does not match provider/event", index)
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
