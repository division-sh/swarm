package effects

import (
	"context"
	"fmt"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type CompletionOriginKind string

const (
	CompletionOriginDelivery  CompletionOriginKind = "delivery"
	CompletionOriginDirective CompletionOriginKind = "directive"
)

type CompletionOrigin struct {
	Kind      CompletionOriginKind
	Delivery  runtimedelivery.Claim
	Directive runtimeagentcontrol.DirectiveExecutionOrigin
}

func DeliveryCompletionOrigin(claim runtimedelivery.Claim) (CompletionOrigin, error) {
	origin := CompletionOrigin{Kind: CompletionOriginDelivery, Delivery: claim}
	return origin, origin.Validate()
}

func DirectiveCompletionOrigin(origin runtimeagentcontrol.DirectiveExecutionOrigin) (CompletionOrigin, error) {
	completion := CompletionOrigin{Kind: CompletionOriginDirective, Directive: origin.Normalize()}
	return completion, completion.Validate()
}

func (o CompletionOrigin) Validate() error {
	switch o.Kind {
	case CompletionOriginDelivery:
		if err := o.Delivery.Validate(); err != nil {
			return fmt.Errorf("delivery completion origin: %w", err)
		}
		if err := o.Directive.Validate(); err == nil {
			return fmt.Errorf("delivery completion origin cannot carry directive authority")
		}
	case CompletionOriginDirective:
		if err := o.Directive.Validate(); err != nil {
			return fmt.Errorf("directive completion origin: %w", err)
		}
		if err := o.Delivery.Validate(); err == nil {
			return fmt.Errorf("directive completion origin cannot carry delivery authority")
		}
	default:
		return fmt.Errorf("completion origin kind %q is invalid", o.Kind)
	}
	return nil
}

func (o CompletionOrigin) Same(other CompletionOrigin) bool {
	if o.Kind != other.Kind {
		return false
	}
	switch o.Kind {
	case CompletionOriginDelivery:
		return o.Delivery.Same(other.Delivery)
	case CompletionOriginDirective:
		return o.Directive.Same(other.Directive)
	default:
		return false
	}
}

// NormalCompletionOriginFromContext selects the same closed work authority for
// session binding and completion admission. Durable authorization still fences
// the exact claim or executing directive before provider launch.
func NormalCompletionOriginFromContext(ctx context.Context, agentID, runID, adapter string) (CompletionOrigin, error) {
	claim, hasDelivery := runtimedelivery.ClaimFromContext(ctx)
	directive, hasDirective := directiveCompletionOriginFromContext(ctx)
	if hasDelivery == hasDirective {
		return CompletionOrigin{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_missing_or_ambiguous", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter})
	}
	var origin CompletionOrigin
	var err error
	if hasDelivery {
		if claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != agentID || claim.RunID() != runID {
			return CompletionOrigin{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_delivery_claim_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter, "delivery_id": claim.DeliveryID()})
		}
		origin, err = DeliveryCompletionOrigin(claim)
	} else {
		origin, err = DirectiveCompletionOrigin(directive)
	}
	if err != nil {
		return CompletionOrigin{}, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_origin_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter}, err)
	}
	return origin, nil
}

type directiveCompletionOriginKey struct{}

func WithDirectiveCompletionOrigin(ctx context.Context, origin runtimeagentcontrol.DirectiveExecutionOrigin) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, directiveCompletionOriginKey{}, origin.Normalize())
}

func directiveCompletionOriginFromContext(ctx context.Context) (runtimeagentcontrol.DirectiveExecutionOrigin, bool) {
	if ctx == nil {
		return runtimeagentcontrol.DirectiveExecutionOrigin{}, false
	}
	origin, ok := ctx.Value(directiveCompletionOriginKey{}).(runtimeagentcontrol.DirectiveExecutionOrigin)
	return origin, ok
}
