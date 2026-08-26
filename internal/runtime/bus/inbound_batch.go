package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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

// providerRawSettlementAdmission is minted only by the typed inbound batch
// owner. Generic publication paths cannot construct deliberate raw emptiness.
type providerRawSettlementAdmission struct {
	eventID string
	source  events.RouteIdentity
	target  events.RouteIdentity
}

func (a providerRawSettlementAdmission) authorizes(projected, inbound events.Event, plan RoutePlan) bool {
	if strings.TrimSpace(a.eventID) == "" || a.eventID != projected.ID() || a.eventID != inbound.ID() ||
		!events.SameRouteIdentity(a.source, projected.RoutingSource().Route()) ||
		!events.SameRouteIdentity(a.source, inbound.RoutingSource().Route()) ||
		!events.SameRouteIdentity(a.target, inbound.TargetRoute()) || len(inbound.TargetRoutes()) != 0 {
		return false
	}
	// A route-less plan canonically clears journal target facts. Any different
	// projected target remains a contradiction rather than settlement authority.
	if target := projected.TargetRoute(); !target.Empty() && !events.SameRouteIdentity(a.target, target) {
		return false
	}
	if len(projected.TargetRoutes()) != 0 {
		return false
	}
	return len(plan.DeliveryRoutes()) == 0 && plan.TargetFailure == runtimepinrouting.FailureTargetNotSubscribed
}

// InboundDeliveryPlan is the immutable runtime half of a closed inbound
// publication operation. The selected store receives only CommitCommands;
// PreparedPublications remain EventBus-owned for post-commit dispatch.
type InboundDeliveryPlan struct {
	events   []InboundDeliveryEvent
	prepared []PreparedPublish
	commands []PublicationCommand
}

func (p InboundDeliveryPlan) PreparedPublications() []PreparedPublish {
	return append([]PreparedPublish(nil), p.prepared...)
}

func (p InboundDeliveryPlan) CommitCommands() []PublicationCommand {
	return append([]PublicationCommand(nil), p.commands...)
}

func (p InboundDeliveryPlan) Events() []InboundDeliveryEvent {
	return append([]InboundDeliveryEvent(nil), p.events...)
}

// ProviderOutputAuthorizationVerifier is the current immutable verified-pack
// catalog owner used to reject fabricated or stale normalized outputs before a
// selected-store mutation begins.
type ProviderOutputAuthorizationVerifier interface {
	VerifyProviderOutputAuthorization(runtimeprovideroutput.Authorization) error
}

// PrepareInboundDeliveryBatch performs admission and canonical route planning
// before the selected-store operation starts. No transaction capability is
// accepted or returned.
func (eb *EventBus) PrepareInboundDeliveryBatch(ctx context.Context, batch InboundDeliveryBatch) (InboundDeliveryPlan, error) {
	if eb == nil {
		return InboundDeliveryPlan{}, fmt.Errorf("event bus is required")
	}
	validated, err := preflightInboundDeliveryBatch(eb.providerOutputAuthorizationVerifier(), batch)
	if err != nil {
		return InboundDeliveryPlan{}, err
	}
	plan := InboundDeliveryPlan{events: append([]InboundDeliveryEvent(nil), validated.Events...)}
	activationOwners := make(map[runtimeflowidentity.Route]int)
	release := func(cause error) (InboundDeliveryPlan, error) {
		for _, prepared := range plan.prepared {
			cause = errors.Join(cause, eb.AbandonPreparedPublish(context.WithoutCancel(ctx), prepared))
		}
		return InboundDeliveryPlan{}, cause
	}
	for _, item := range validated.Events {
		itemCtx := ctx
		if item.Kind == runtimeprovideroutput.KindRaw {
			itemCtx = withoutProviderOutputAuthorization(itemCtx)
		} else {
			itemCtx = withProviderOutputAuthorization(itemCtx, item.Authorization)
		}
		preparedCtx, admitted, err := eb.admitPublishEvent(itemCtx, item.Event)
		if err != nil {
			return release(err)
		}
		if err := eb.requireExistingRunActive(preparedCtx, admitted.Event()); err != nil {
			return release(err)
		}
		rawSettlement := eb.admitProviderRawSettlement(item.Kind, admitted.Event())
		prepared, command, err := eb.prepareClosedPublication(preparedCtx, eventBusCommitPublishPlan{
			bus: eb, event: admitted.Event(), admitted: admitted, providerRawSettlement: rawSettlement,
		})
		if err != nil {
			return release(err)
		}
		ownedActivations := command.Activations[:0]
		for _, activation := range command.Activations {
			route := activation.Identity.Route()
			if _, owned := activationOwners[route]; owned {
				continue
			}
			activationOwners[route] = len(plan.commands)
			ownedActivations = append(ownedActivations, activation)
		}
		command.Activations = ownedActivations
		if len(command.Activations) == 0 {
			command.RouteTopology = nil
		}
		if err := command.Validate(); err != nil {
			return release(fmt.Errorf("canonicalize inbound activation ownership: %w", err))
		}
		plan.prepared = append(plan.prepared, prepared)
		plan.commands = append(plan.commands, command)
	}
	return plan, nil
}

func (eb *EventBus) admitProviderRawSettlement(kind runtimeprovideroutput.Kind, evt events.Event) providerRawSettlementAdmission {
	if eb == nil || eb.semanticSource == nil || kind != runtimeprovideroutput.KindRaw {
		return providerRawSettlementAdmission{}
	}
	source := evt.RoutingSource()
	if source.Kind() != events.RoutingSourceExternalIngress || source.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan {
		return providerRawSettlementAdmission{}
	}
	sourceRoute := source.Route().Normalized()
	target := evt.TargetRoute().Normalized()
	if sourceRoute.FlowID == "" || sourceRoute.EntityID == "" || target.FlowInstance == "" || target.EntityID == "" ||
		len(evt.TargetRoutes()) != 0 || target.EntityID != sourceRoute.EntityID ||
		(target.FlowID != "" && target.FlowID != sourceRoute.FlowID) {
		return providerRawSettlementAdmission{}
	}
	if runtimeflowidentity.SemanticScope(target.FlowInstance) != runtimeflowidentity.ScopeKey(eb.semanticSource, sourceRoute.FlowID) {
		return providerRawSettlementAdmission{}
	}
	producer := runtimepinrouting.ResolveFlowInputProducer(eb.semanticSource, sourceRoute.FlowID, string(evt.Type()))
	if !producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) {
		return providerRawSettlementAdmission{}
	}
	return providerRawSettlementAdmission{eventID: evt.ID(), source: sourceRoute, target: target}
}

func (eb *EventBus) AbandonInboundDeliveryPlan(ctx context.Context, plan InboundDeliveryPlan) error {
	var result error
	for _, prepared := range plan.prepared {
		result = errors.Join(result, eb.AbandonPreparedPublish(ctx, prepared))
	}
	return result
}

// ApplyInboundDeliveryCommit binds selected-store evidence to the exact plans
// that produced it. Dispatch remains a separate post-commit step.
func (eb *EventBus) ApplyInboundDeliveryCommit(ctx context.Context, plan InboundDeliveryPlan, committed []CommittedPublication) ([]PreparedPublish, error) {
	if len(committed) != len(plan.prepared) {
		return nil, fmt.Errorf("committed inbound publication evidence count differs from prepared batch")
	}
	prepared := append([]PreparedPublish(nil), plan.prepared...)
	for index := range prepared {
		if err := committed[index].Validate(); err != nil {
			return nil, fmt.Errorf("validate inbound publication evidence %d: %w", index, err)
		}
		var err error
		prepared[index], err = prepared[index].WithCommitOutcome(committed[index].AppendOutcome)
		if err != nil {
			return nil, err
		}
		prepared[index].committedHandoffs = append([]runtimedelivery.DurableHandoffProof(nil), committed[index].DeliveryHandoffs...)
		if err := eb.finalizeCommittedFlowInstanceActivations(ctx, committed[index].Activations); err != nil {
			return nil, err
		}
		if eb.testLifecycleProbe != nil && !prepared[index].exactDuplicate {
			eb.notifyTestPublishPersisted(ctx, prepared[index].Event, prepared[index].plan)
		}
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
		authorization := item.Authorization
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
			if authorization.Provider() != provider || authorization.Event() != eventName {
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
	return context.WithValue(ctx, providerOutputAuthorizationContextKey{}, authorization)
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
