package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type InboundDeliveryBatch struct {
	Provider          string
	Admission         providertriggers.PublicationAdmission
	AuthorSubjectType string
	AuthorSubjectID   string
	Events            []InboundDeliveryEvent
}

type InboundDeliveryEvent struct {
	Event         events.Event
	Kind          runtimeprovideroutput.Kind
	Authorization runtimeprovideroutput.Authorization
}

// PrepareInboundEvidence uses the same pinned payload owner before the closed
// inbound mutation, so rejection cannot call runtime diagnostics under SQL locks.
func (eb *EventBus) PrepareInboundEvidence(ctx context.Context, event events.Event) (events.Event, error) {
	if eb == nil {
		return events.Event{}, fmt.Errorf("inbound evidence requires the event bus")
	}
	_, admitted, err := eb.admitPublicationEventFacts(ctx, event)
	if err != nil {
		return events.Event{}, err
	}
	if err := events.ValidateNamedEvent(admitted, events.EventAdmissionDiagnosticDirect, events.EventTypePlatformInboundRecord); err != nil {
		return events.Event{}, err
	}
	return admitted.Event(), nil
}

type authenticatedProviderPublicationKey struct{}

type authenticatedProviderPublication struct {
	eventID string
	source  events.RouteIdentity
	kind    runtimeprovideroutput.Kind
}

func authenticatedProviderPublicationForEvent(ctx context.Context, event events.Event) (authenticatedProviderPublication, bool) {
	admission, ok := ctx.Value(authenticatedProviderPublicationKey{}).(authenticatedProviderPublication)
	return admission, ok && admission.eventID == event.ID() && admission.source == event.RoutingSource().Route()
}

// providerRawSettlementAdmission is minted only by the typed inbound batch
// owner. Generic publication paths cannot construct deliberate raw emptiness.
type providerRawSettlementAdmission struct {
	eventID string
	source  events.RouteIdentity
}

func (a providerRawSettlementAdmission) authorizes(projected, inbound events.Event, plan RoutePlan) bool {
	if strings.TrimSpace(a.eventID) == "" || a.eventID != projected.ID() || a.eventID != inbound.ID() ||
		!events.SameRouteIdentity(a.source, projected.RoutingSource().Route()) ||
		!events.SameRouteIdentity(a.source, inbound.RoutingSource().Route()) ||
		inbound.HasTargetRoute() || len(inbound.TargetRoutes()) != 0 {
		return false
	}
	if projected.HasTargetRoute() || len(projected.TargetRoutes()) != 0 {
		return false
	}
	// The exact compiled declaration admits raw transport evidence independently
	// of whether any concrete business receiver exists. Addresses cannot prove it.
	owner := plan.ordinarySource.route
	return owner.FlowID == a.source.FlowID && a.source.EntityID == "" && a.source.FlowInstance == "" &&
		len(plan.DeliveryRoutes()) == 0 && plan.TargetFailure.Empty() && !plan.CanonicalRouteOwnerMatched()
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

// ReconcileInboundConstruction consumes a proven rolled-back coordinate, not
// a generic duplicate error. The complete next batch must be planned afresh.
func (eb *EventBus) ReconcileInboundConstruction(ctx context.Context, plan InboundDeliveryPlan, owner runtimeflowidentity.RunScopedFlowInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, activation, found := plan.proposedConstruction(owner)
	if !found {
		return fmt.Errorf("rolled-back construction is absent from the exact inbound plan")
	}
	if !eb.inboundConstructionMayReuse(prepared, activation) {
		return fmt.Errorf("rolled-back construction is not select-or-create")
	}
	return eb.validateElectedConstruction(ctx, owner, activation)
}

func (p InboundDeliveryPlan) proposedConstruction(owner runtimeflowidentity.RunScopedFlowInstance) (PreparedPublish, pipeline.FlowInstanceActivationPlan, bool) {
	for index, command := range p.commands {
		for _, activation := range command.Activations {
			if activation.Readiness.RunID == owner.RunID && activation.Identity.Route() == owner.Route {
				return p.prepared[index], activation, true
			}
		}
	}
	return PreparedPublish{}, pipeline.FlowInstanceActivationPlan{}, false
}

func (eb *EventBus) validateElectedConstruction(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, activation pipeline.FlowInstanceActivationPlan) error {
	reader, ok := eb.templateInstancePlanner.(pipeline.FlowConstructionPublicationReader)
	if !ok {
		return fmt.Errorf("construction reconciliation requires its durable receipt owner")
	}
	winner, err := reader.LoadFlowConstructionPublication(ctx, owner, activation.Identity.EntityID)
	if err != nil {
		return err
	}
	if winner.Identity != activation.Identity {
		return fmt.Errorf("elected construction contradicts its proposed structural owner")
	}
	if err := winner.Identity.ValidateConstruction(eb.semanticSource, owner.RunID); err != nil {
		return err
	}
	schema, found := eb.semanticSource.FlowSchemaByID(winner.Identity.TemplateID)
	if !found || schema.Instance.Empty() {
		return fmt.Errorf("construction reconciliation requires its declared receiver key")
	}
	key := schema.Instance.Path()
	actual, err := runtimepinrouting.DescriptorAddressFields(winner.Fields)
	if err != nil {
		return err
	}
	proposed, err := runtimepinrouting.DescriptorAddressFields(activation.Instance.Fields)
	if err != nil || actual["entity."+key] == "" || actual["entity."+key] != proposed["entity."+key] {
		return errors.Join(err, fmt.Errorf("elected construction contradicts its selected key"))
	}
	return nil
}

func (eb *EventBus) inboundConstructionMayReuse(prepared PreparedPublish, activation pipeline.FlowInstanceActivationPlan) bool {
	if activation.Identity.TemplateID == semanticview.RootExecutionFlowID(eb.semanticSource) && activation.Identity.InstancePath == prepared.Event.RunID() {
		return prepared.Event.RoutingSource().Kind() == events.RoutingSourceExternalIngress
	}
	graph := runtimepinrouting.CompileConnectGraph(eb.semanticSource)
	for _, compiled := range graph.MatchingPlans(prepared.Event) {
		if compiled.InstanceKey() == nil || compiled.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate {
			continue
		}
		identity, err := runtimepinrouting.ConnectPlanIdentity(compiled)
		if err != nil {
			continue
		}
		for _, evaluated := range prepared.plan.ConnectEvaluation.Plans() {
			if evaluated.PlanIdentity() != identity || evaluated.Resolution() != events.ConnectPlanResolved {
				continue
			}
			for _, target := range evaluated.Targets() {
				if target.FlowID == activation.Identity.TemplateID && target.FlowInstance == activation.Identity.InstancePath && target.EntityID == activation.Identity.EntityID {
					return true
				}
			}
		}
	}
	return false
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
	for index, item := range validated.Events {
		if err := validated.Admission.ValidateOutput(eb.sourceArtifactFact.BundleHash(), validated.Provider, index, len(validated.Events), item.Event, item.Kind, item.Authorization); err != nil {
			return InboundDeliveryPlan{}, err
		}
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
		itemCtx := context.WithValue(ctx, authenticatedProviderPublicationKey{}, authenticatedProviderPublication{
			eventID: item.Event.ID(), source: item.Event.RoutingSource().Route(), kind: item.Kind,
		})
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
	if sourceRoute.FlowID == "" || sourceRoute.EntityID != "" || sourceRoute.FlowInstance != "" || evt.HasTargetRoute() || len(evt.TargetRoutes()) != 0 {
		return providerRawSettlementAdmission{}
	}
	producer := runtimepinrouting.ResolveFlowInputProducer(eb.semanticSource, sourceRoute.FlowID, string(evt.Type()))
	if !producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) {
		return providerRawSettlementAdmission{}
	}
	if runtimepinrouting.ClassifyRoutingSourceOutputConsumer(eb.semanticSource, string(evt.Type()), source).HasRuntimeConsumer() {
		return providerRawSettlementAdmission{}
	}
	return providerRawSettlementAdmission{eventID: evt.ID(), source: sourceRoute}
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
	var finalizationErr error
	for index := range prepared {
		consequences, err := eb.finalizeCommittedPublicationConsequences(ctx, prepared[index], committed[index], false)
		prepared[index] = consequences.prepared
		finalizationErr = errors.Join(finalizationErr, err)
	}
	if finalizationErr != nil {
		return nil, errors.Join(finalizationErr, eb.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), plan))
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
