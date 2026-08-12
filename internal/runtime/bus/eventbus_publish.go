package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TargetFailureDeadLetterRecorder = runtimedeadletters.Recorder

func targetDeliveryFailureEnvelope(failure runtimepinrouting.TargetFailure) *runtimefailures.Envelope {
	if failure.Empty() {
		return nil
	}
	canonical := runtimefailures.Normalize(runtimefailures.NewTarget(failure.Code(), "eventbus", "resolve_delivery_target", nil), "eventbus", "resolve_delivery_target")
	return &canonical
}

func eventBusFailure(err error, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	failure := runtimefailures.Normalize(err, "eventbus", operation)
	return &failure
}

func eventBusDependencyFailure(err error, detailCode, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	failure := runtimefailures.Normalize(runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, detailCode, "eventbus", operation, nil, err), "eventbus", operation)
	return &failure
}

var ErrRuntimeIngressPaused = errors.New("runtime ingress is paused")
var ErrRunDispatchBlocked = errors.New("run dispatch is blocked")

const (
	dispatchQueueRuntimeIngress = "runtime_ingress_queued"
	dispatchQueueRunBlocked     = "run_dispatch_blocked"
)

func (eb *EventBus) runtimeIngressDispatchPaused(ctx context.Context, evt events.Event) (bool, error) {
	if eb == nil || runtimeIngressDispatchBypass(evt) {
		return false, nil
	}
	eb.mu.RLock()
	gate := eb.runtimeIngressDispatchGate
	eb.mu.RUnlock()
	if gate == nil {
		return false, nil
	}
	paused, err := gate.QueueableIngressPaused(ctx)
	if err != nil {
		return false, err
	}
	return paused, nil
}

func (eb *EventBus) runDispatchBlocked(ctx context.Context, evt events.Event) (bool, error) {
	if eb == nil {
		return false, nil
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		return false, nil
	}
	eb.mu.RLock()
	gate := eb.runDispatchGate
	eb.mu.RUnlock()
	if gate == nil {
		return false, nil
	}
	return gate.QueueableRunDispatchBlocked(ctx, runID)
}

func runtimeIngressDispatchBypass(evt events.Event) bool {
	return events.IsRuntimePlatformEvent(evt)
}

func (eb *EventBus) dispatchQueueReason(ctx context.Context, evt events.Event) (string, error) {
	if paused, err := eb.runtimeIngressDispatchPaused(ctx, evt); err != nil {
		return "", err
	} else if paused {
		return dispatchQueueRuntimeIngress, nil
	}
	if blocked, err := eb.runDispatchBlocked(ctx, evt); err != nil {
		return "", err
	} else if blocked {
		return dispatchQueueRunBlocked, nil
	}
	return "", nil
}

func (eb *EventBus) logDispatchQueued(ctx context.Context, reason string, evt events.Event, recipientsCount int, direct, transactional bool) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	message := "Event persisted without dispatch"
	detail := map[string]any{
		"recipients_count": recipientsCount,
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID()),
	}
	if direct {
		detail["direct"] = true
	}
	if transactional {
		detail["transactional"] = true
	}
	if reason == dispatchQueueRuntimeIngress {
		message = "Runtime ingress is paused; event persisted without dispatch"
	} else if reason == dispatchQueueRunBlocked {
		message = "Run dispatch is blocked; event persisted without dispatch"
		detail["run_id"] = strings.TrimSpace(evt.RunID())
	}
	eb.logRuntime(ctx, "debug", message, "eventbus", reason, evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, nil, 0)
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) error {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt})
	if err != nil {
		return err
	}
	return eb.dispatchPreparedPublish(ctx, prepared)
}

// PublishAndWait persists and dispatches one event, then joins the exact tree
// of process-local deliveries accepted from that dispatch. Durable retry work
// remains owned by the store and is not reinterpreted as live local work.
func (eb *EventBus) PublishAndWait(ctx context.Context, evt events.Event) error {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt})
	if err != nil {
		return err
	}
	group := newLocalDeliveryCompletionGroup()
	waitCtx := ctx
	prepared.receiver = prepared.receiver.withCompletion(group)
	return eb.dispatchPreparedPublishWithCompletion(ctx, prepared, func() error {
		group.releaseDispatch()
		return group.wait(waitCtx)
	})
}

// PublishAcknowledged persists the event, recipient manifest, and replay scope
// before returning, then dispatches post-commit pipeline work asynchronously.
// Public API surfaces use this when success means durable acceptance rather than
// downstream handler completion.
func (eb *EventBus) PublishAcknowledged(ctx context.Context, evt events.Event) error {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt})
	if err != nil {
		return err
	}
	if prepared.exactDuplicate {
		return eb.DispatchPreparedPublish(ctx, prepared)
	}
	return eb.DispatchPreparedPublishAsync(ctx, prepared)
}

type eventBusCommitPublishPlan struct {
	bus                 *EventBus
	event               events.Event
	direct              bool
	directRecipients    []string
	directRoutes        []events.DeliveryRoute
	admitted            events.AdmittedEvent
	publicationClaim    *pipelinePublicationClaim
	dynamicFlowCreation *runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest
}

func (eb *EventBus) commitPublish(ctx context.Context, plan eventBusCommitPublishPlan) (PreparedPublish, error) {
	owner, ok := eb.store.(CommitPublicationOwner)
	if !ok || owner == nil {
		return PreparedPublish{}, errors.New("selected store does not support the closed CommitPublish operation")
	}
	preparedCtx, admitted, err := eb.admitPublishEvent(ctx, plan.event)
	if err != nil {
		return PreparedPublish{}, err
	}
	plan.event = admitted.Event()
	plan.admitted = admitted
	if err := eb.executionPosture.Admit(plan.event.ExecutionMode(), "event persistence and delivery"); err != nil {
		return PreparedPublish{}, err
	}
	if err := eb.requireExistingRunActive(preparedCtx, plan.event); err != nil {
		return PreparedPublish{}, err
	}
	prepared, command, err := eb.prepareClosedPublication(preparedCtx, plan)
	if err != nil {
		return PreparedPublish{}, err
	}
	committed, err := owner.CommitPublication(preparedCtx, command)
	if err != nil {
		return PreparedPublish{}, errors.Join(err, prepared.publicationClaim.Release(preparedCtx))
	}
	prepared, err = prepared.WithCommitOutcome(committed.AppendOutcome)
	if err != nil {
		return PreparedPublish{}, errors.Join(err, prepared.publicationClaim.Release(preparedCtx))
	}
	prepared.committedHandoffs = append([]runtimedelivery.DurableHandoffProof(nil), committed.DeliveryHandoffs...)
	if err := eb.finalizeCommittedFlowInstanceActivations(preparedCtx, committed.Activations); err != nil {
		return PreparedPublish{}, errors.Join(err, prepared.publicationClaim.Release(preparedCtx))
	}
	if eb.testLifecycleProbe != nil && !prepared.exactDuplicate {
		eb.notifyTestPublishPersisted(preparedCtx, prepared.Event, prepared.plan)
	}
	return prepared, nil
}

func (eb *EventBus) requireExistingRunActive(ctx context.Context, event events.Event) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	runID := strings.TrimSpace(event.RunID())
	if runID == "" {
		return nil
	}
	if reader, ok := eb.store.(PreparedPublishEventReader); ok {
		_, found, err := reader.LoadPreparedPublishEvent(ctx, event.ID())
		if err != nil {
			return fmt.Errorf("load publication event before run preflight: %w", err)
		}
		if found {
			return nil
		}
	}
	owner, ok := eb.store.(interface {
		RequirePublicationRunActive(context.Context, string) error
	})
	if !ok {
		return nil
	}
	err := owner.RequirePublicationRunActive(ctx, runID)
	if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
		return nil
	}
	return err
}

func (eb *EventBus) finalizeCommittedFlowInstanceActivations(
	ctx context.Context,
	activations []CommittedFlowInstanceActivation,
) error {
	if len(activations) == 0 {
		return nil
	}
	finalizer := eb.flowActivationFinalizer
	if finalizer == nil {
		return errors.New("committed flow activation finalizer is unavailable")
	}
	for index, activation := range activations {
		if err := finalizer.FinalizeCommittedFlowInstanceActivation(ctx, activation); err != nil {
			return fmt.Errorf("finalize committed flow activation %d for %s: %w", index, activation.Plan.Identity.Route().InstancePath, err)
		}
	}
	return nil
}

func (eb *EventBus) prepareClosedPublication(ctx context.Context, publication eventBusCommitPublishPlan) (PreparedPublish, PublicationCommand, error) {
	claim := publication.publicationClaim
	if claim == nil {
		var err error
		claim, err = eb.claimPipelinePublication(ctx, publication.admitted.ID())
		if err != nil {
			return PreparedPublish{}, PublicationCommand{}, err
		}
	}
	releaseFailure := func(err error) (PreparedPublish, PublicationCommand, error) {
		return PreparedPublish{}, PublicationCommand{}, errors.Join(err, claim.Release(ctx))
	}

	admitted := publication.admitted
	evt := admitted.Event()
	descriptor, hasDescriptor, descriptorErr := publicationAuthorDescriptor(ctx, evt)
	if descriptorErr != nil {
		return releaseFailure(descriptorErr)
	}
	authorScope, hasAuthorScope := runtimeauthoractivity.ScopeFromContext(ctx)
	if reader, ok := eb.store.(PreparedPublishEventReader); ok {
		durable, found, err := reader.LoadPreparedPublishEvent(ctx, admitted.ID())
		if err != nil {
			return releaseFailure(fmt.Errorf("load durable event identity: %w", err))
		}
		if found {
			if !publication.direct && len(eb.connectRoutePlanner.matchedPlans(ctx, evt)) > 0 {
				admitted, evt, err = reuseDurableSubscribedEventRouteFacts(admitted, durable.Event)
				if err != nil {
					return releaseFailure(err)
				}
			}
			routePlan := routePlanFromManifest(evt, deliveryRecipientManifest{
				DeliveryRoutes: durable.DeliveryRoutes,
			}, routeIntentProducerRecipientMaterializer)
			routePlan.ConnectEvaluation = durable.Settlement.Ledger()
			prepared := PreparedPublish{
				Event: evt, admitted: admitted, plan: routePlan,
				settlement: durable.Settlement, publicationClaim: claim,
			}
			request := prepared.CommitRequest()
			return prepared, PublicationCommand{
				Commit: request, DynamicFlowCreation: publication.dynamicFlowCreation,
				AuthorScope: authorScope, HasAuthorScope: hasAuthorScope,
				AuthorDescriptor: descriptor, HasAuthorDescriptor: hasDescriptor,
			}, nil
		}
	}

	planRoutes := func(context.Context, events.Event) (RoutePlan, error) {
		return eb.planSubscribedRoutePlan(withClosedPublicationPlanning(ctx), evt, true)
	}
	replayScope := runtimepipelineobligation.ScopeSubscribed
	if publication.direct {
		replayScope = runtimepipelineobligation.ScopeDirect
		switch {
		case len(publication.directRoutes) > 0:
			routes := events.NormalizeDeliveryRoutes(publication.directRoutes)
			planRoutes = func(context.Context, events.Event) (RoutePlan, error) {
				return eb.planExactDirectRoutePlan(withClosedPublicationPlanning(ctx), evt, routes)
			}
		default:
			requested := uniqueStrings(publication.directRecipients)
			if len(requested) == 0 {
				return releaseFailure(errors.New("direct event publication requires at least one recipient"))
			}
			planRoutes = func(context.Context, events.Event) (RoutePlan, error) {
				plan, err := eb.planDirectRoutePlan(withClosedPublicationPlanning(ctx), evt, requested)
				if err != nil {
					return RoutePlan{}, err
				}
				if filtered := filteredRecipients(requested, plan.RecipientIDs()); len(filtered) > 0 {
					return RoutePlan{}, fmt.Errorf("direct delivery rejected recipients: %s", strings.Join(filtered, ", "))
				}
				return plan, nil
			}
		}
	}

	routePlan, err := planRoutes(ctx, evt)
	if err != nil {
		return releaseFailure(err)
	}
	if err := requirePublicInputRoutePlan(ctx, routePlan); err != nil {
		return releaseFailure(err)
	}
	if replayScope == runtimepipelineobligation.ScopeSubscribed {
		resolved, changed, err := resolveCanonicalConnectRouteEvent(evt, routePlan)
		if err != nil {
			return releaseFailure(err)
		}
		if changed {
			admitted, err = events.AdmitForPersistence(resolved, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				return releaseFailure(fmt.Errorf("admit resolved event route facts: %w", err))
			}
			evt = admitted.Event()
		}
		if err := validateCanonicalConnectRouteEvent(evt, routePlan); err != nil {
			return releaseFailure(err)
		}
	}
	if err := routePlan.ValidatePersistentDeliveries(); err != nil {
		return releaseFailure(fmt.Errorf("validate durable route plan: %w", err))
	}
	prepared := PreparedPublish{
		Event: evt, admitted: admitted, plan: routePlan,
		direct:           replayScope == runtimepipelineobligation.ScopeDirect,
		publicationClaim: claim,
	}
	prepared.settlement, err = eb.routeSettlementForPlan(evt, routePlan, events.EventWriteNormalPublication)
	if err != nil {
		return releaseFailure(err)
	}
	if !routePlan.TargetFailure.Empty() {
		prepared.targetFailure = true
	}
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return releaseFailure(err)
	} else if reason != "" {
		prepared.dispatchQueued = true
		prepared.queueReason = reason
	}
	if prepared.requiresReceiver() {
		receiver, receiverErr := eb.receiverProjection(ctx, evt.DeliveryContext())
		if receiverErr != nil {
			return releaseFailure(receiverErr)
		}
		prepared.receiver = receiver
	}
	request := prepared.CommitRequest()
	routeTopology, err := eb.prepareFlowInstanceActivationRouteTopology(ctx, routePlan.ActivationPlans)
	if err != nil {
		return releaseFailure(err)
	}
	return prepared, PublicationCommand{
		Commit:              request,
		Activations:         append([]runtimepipeline.FlowInstanceActivationPlan(nil), routePlan.ActivationPlans...),
		RouteTopology:       routeTopology,
		DynamicFlowCreation: publication.dynamicFlowCreation,
		AuthorScope:         authorScope,
		HasAuthorScope:      hasAuthorScope,
		AuthorDescriptor:    descriptor,
		HasAuthorDescriptor: hasDescriptor,
	}, nil
}

func (eb *EventBus) prepareFlowInstanceActivationRouteTopology(
	ctx context.Context,
	plans []runtimepipeline.FlowInstanceActivationPlan,
) ([]FlowInstanceRouteRecordSet, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	eb.mu.RLock()
	table := eb.routeTable
	lister := eb.durable.ActiveFlows
	eb.mu.RUnlock()
	if table == nil || lister == nil {
		return nil, errors.New("flow activation publication requires route topology owners")
	}
	staged, identities, err := eb.deriveFlowInstanceRouteTopology(
		ctx,
		table,
		lister,
		nil,
		runtimeflowidentity.Route{},
	)
	if err != nil {
		return nil, fmt.Errorf("derive active route topology before activation: %w", err)
	}
	byPath := make(map[string]runtimeflowidentity.Route, len(identities)+len(plans))
	for _, identity := range identities {
		byPath[identity.InstancePath] = identity
	}
	for index, plan := range plans {
		request := FlowInstanceRouteMaterializationRequest{
			Identity:            plan.Identity.Route(),
			ActivationVariables: plan.ActivationVariables,
		}
		if err := staged.AddFlowInstanceRoute(request); err != nil {
			return nil, fmt.Errorf("derive publication activation route %d: %w", index, err)
		}
		identity := request.Normalized().Identity
		byPath[identity.InstancePath] = identity
	}
	identities = identities[:0]
	for _, identity := range byPath {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].InstancePath < identities[j].InstancePath })
	return flowInstanceRouteTopologyRecordSets(staged, identities), nil
}

// CommitFlowInstanceActivation commits the exact instance record and the full
// derived route topology before making either fact process-visible.
func (eb *EventBus) CommitFlowInstanceActivation(
	ctx context.Context,
	plan runtimepipeline.FlowInstanceActivationPlan,
) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	if eb == nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, errors.New("event bus is required")
	}
	owner, ok := eb.store.(FlowInstanceActivationCommitOwner)
	if !ok || owner == nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, errors.New("selected store does not support the closed flow instance activation operation")
	}
	topology, err := eb.prepareFlowInstanceActivationRouteTopology(ctx, []runtimepipeline.FlowInstanceActivationPlan{plan})
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	committed, err := owner.CommitFlowInstanceActivation(ctx, FlowInstanceActivationCommand{Plan: plan, RouteTopology: topology})
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	if err := committed.Validate(); err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	return committed, nil
}

func publicationAuthorDescriptor(ctx context.Context, evt events.Event) (runtimeauthoractivity.EventDescriptor, bool, error) {
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
		return runtimeauthoractivity.EventDescriptor{}, false, nil
	}
	descriptor, found, err := runtimeauthoractivity.ResolvedEventDescriptorFromContext(ctx, scope, strings.TrimSpace(string(evt.Type())))
	if err != nil {
		return runtimeauthoractivity.EventDescriptor{}, false, err
	}
	return descriptor, found, nil
}

// PreparedPublish is the transaction-local result of canonical route planning.
// Its route plan remains EventBus-owned; callers may persist the exported
// delivery-route manifest but cannot reinterpret or replace the plan.
type PreparedPublish struct {
	Event             events.Event
	admitted          events.AdmittedEvent
	plan              RoutePlan
	settlement        events.RouteSettlement
	exactDuplicate    bool
	targetFailure     bool
	dispatchQueued    bool
	queueReason       string
	direct            bool
	publicationClaim  *pipelinePublicationClaim
	receiver          receiverDispatchProjection
	committedHandoffs []runtimedelivery.DurableHandoffProof
}

func validateEventAppendOutcome(outcome EventAppendOutcome) error {
	if outcome != EventAppendInserted && outcome != EventAppendExactDuplicate {
		return errors.New("invalid append outcome")
	}
	return nil
}

func (p PreparedPublish) DeliveryRoutes() []events.DeliveryRoute {
	return p.plan.DeliveryRoutes()
}

func (p PreparedPublish) RecipientIDs() []string {
	return p.plan.RecipientIDs()
}

// CommitRequest returns the exact initial event facts owned by the route plan.
// Callers may pass it only to a closed named store operation; they cannot
// reinterpret or replace the private plan used for later dispatch.
func (p PreparedPublish) CommitRequest() CommitPublishRequest {
	authority, _ := p.publicationClaim.bus.DeliveryAuthority()
	request := CommitPublishRequest{
		Event:             p.admitted,
		RouteSettlement:   p.settlement,
		DeliveryRoutes:    p.plan.DeliveryRoutes(),
		DeliveryAuthority: authority,
		ReplayScope:       runtimepipelineobligation.ScopeSubscribed,
		PipelineClaim:     p.publicationClaim.Claim(),
		ReplyCreations:    append([]runtimereplycontext.Record(nil), p.plan.ReplyCreations...),
		ReplyClaims:       append([]runtimereplycontext.ClaimCommand(nil), p.plan.ReplyClaims...),
	}
	if failure := p.plan.TargetFailure; !failure.Empty() {
		disposition := runtimepipelineobligation.DeadLetter(failure.Code(), targetDeliveryFailureEnvelope(failure))
		request.Disposition = &disposition
		_, _, record := targetDeliveryFailureRecord(p.Event, p.plan, failure, time.Now().UTC())
		request.DeadLetter = &record
	}
	return request
}

func (eb *EventBus) routeSettlementForPlan(evt events.Event, plan RoutePlan, class events.EventWriteClass) (events.RouteSettlement, error) {
	routes := plan.DeliveryRoutes()
	if len(routes) > 0 {
		return events.NewDeliverySettlement(class, plan.ConnectEvaluation)
	}
	reason := events.NoDeliveryDeclaredConsumerNoPlan
	switch {
	case !plan.TargetFailure.Empty():
		reason = events.NoDeliveryResolutionBlocked
	case plan.CanonicalRouteOwnerMatched():
		reason = events.NoDeliveryMatchedNoRecipient
	case runtimepinrouting.ClassifyRoutingSourceOutputConsumer(eb.semanticSource, string(evt.Type()), evt.RoutingSource()).DeliberateNoSubscriber():
		reason = events.NoDeliveryNoSubscriberByDesign
	}
	return events.NewNoDeliverySettlement(class, reason, plan.ConnectEvaluation)
}

func (p PreparedPublish) CommittedDeliveryHandoffs() ([]runtimedelivery.DurableHandoffProof, error) {
	return append([]runtimedelivery.DurableHandoffProof(nil), p.committedHandoffs...), nil
}

func (p PreparedPublish) WithCommitOutcome(outcome EventAppendOutcome) (PreparedPublish, error) {
	if err := validateEventAppendOutcome(outcome); err != nil {
		return PreparedPublish{}, err
	}
	p.exactDuplicate = outcome == EventAppendExactDuplicate
	return p, nil
}

func (p PreparedPublish) WithCommittedDeliveryHandoffs(handoffs []runtimedelivery.DurableHandoffProof) PreparedPublish {
	p.committedHandoffs = append([]runtimedelivery.DurableHandoffProof(nil), handoffs...)
	return p
}

// AbandonPreparedPublish releases preparation-only process state when the
// named durable operation does not commit or dispatch the prepared event.
func (eb *EventBus) AbandonPreparedPublish(ctx context.Context, prepared PreparedPublish) error {
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	return prepared.publicationClaim.Release(dispatchCtx)
}

func (eb *EventBus) admitPreparedPublish(ctx context.Context, prepared PreparedPublish) (context.Context, error) {
	if prepared.publicationClaim == nil {
		return nil, errors.New("prepared publication claim is required")
	}
	if eb == nil || prepared.publicationClaim.bus != eb {
		return nil, errors.New("prepared publication belongs to a different event bus")
	}
	if _, err := eb.admitBundleSourceFact(ctx); err != nil {
		return nil, fmt.Errorf("admit prepared publication caller: %w", err)
	}
	if prepared.requiresReceiver() {
		if err := prepared.receiver.validate(); err != nil {
			return nil, fmt.Errorf("admit prepared publication receiver: %w", err)
		}
	}
	return eb.admitBundleSourceFact(ctx)
}

func (prepared PreparedPublish) requiresReceiver() bool {
	return !prepared.exactDuplicate && !prepared.targetFailure && !prepared.dispatchQueued
}

// PrepareSelectedForkPublish performs canonical admission and route planning
// without persistence. Its sole consumer is the selected-fork named store
// operation, which must commit lineage and initial delivery facts before the
// returned plan may be dispatched.
func (eb *EventBus) PrepareSelectedForkPublish(ctx context.Context, evt events.Event) (PreparedPublish, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return PreparedPublish{}, err
	}
	ctx, err := eb.admitBundleSourceFact(ctx)
	if err != nil {
		return PreparedPublish{}, err
	}
	if evt.AdmissionClass() != events.EventAdmissionSelectedForkReplay {
		return PreparedPublish{}, fmt.Errorf("selected-fork preparation requires selected_fork_replay event class")
	}
	if evt.Type() == "" || !isValidEventTypeName(string(evt.Type())) {
		return PreparedPublish{}, fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return PreparedPublish{}, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	admitted, err := events.AdmitForPersistence(evt, events.AdmissionOptions{
		Now:                           time.Now(),
		RequirePersistentUUIDIdentity: true,
	})
	if err != nil {
		return PreparedPublish{}, err
	}
	evt = admitted.Event()
	preparedCtx := events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		preparedCtx = runtimecorrelation.WithRunID(preparedCtx, runID)
	}
	preparedCtx, err = eb.withAuthorActivityEventDescriptor(preparedCtx, evt)
	if err != nil {
		return PreparedPublish{}, err
	}
	publicationClaim, err := eb.claimPipelinePublication(preparedCtx, evt.ID())
	if err != nil {
		return PreparedPublish{}, err
	}
	plan, err := eb.planSubscribedPublish(preparedCtx, evt)
	if err != nil {
		return PreparedPublish{}, errors.Join(err, publicationClaim.Release(preparedCtx))
	}
	prepared := PreparedPublish{
		Event: evt, admitted: admitted, plan: plan,
		publicationClaim: publicationClaim,
	}
	prepared.settlement, err = eb.routeSettlementForPlan(evt, plan, events.EventWriteSelectedForkPublication)
	if err != nil {
		return PreparedPublish{}, errors.Join(err, publicationClaim.Release(preparedCtx))
	}
	if !plan.TargetFailure.Empty() {
		prepared.targetFailure = true
		return prepared, nil
	}
	if reason, err := eb.dispatchQueueReason(preparedCtx, evt); err != nil {
		return PreparedPublish{}, errors.Join(err, publicationClaim.Release(preparedCtx))
	} else if reason != "" {
		prepared.dispatchQueued = true
		prepared.queueReason = reason
	}
	if prepared.requiresReceiver() {
		receiver, receiverErr := eb.receiverProjection(preparedCtx, evt.DeliveryContext())
		if receiverErr != nil {
			return PreparedPublish{}, errors.Join(receiverErr, publicationClaim.Release(preparedCtx))
		}
		prepared.receiver = receiver
	}
	return prepared, nil
}

func (eb *EventBus) admitPublishEvent(ctx context.Context, evt events.Event) (context.Context, events.AdmittedEvent, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	if evt.Type() == "" {
		return ctx, events.AdmittedEvent{}, errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return ctx, events.AdmittedEvent{}, fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return ctx, events.AdmittedEvent{}, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ictx, admitted, err := admitEventForPublish(ctx, evt, time.Now())
	if err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	evt = admitted.Event()
	ictx, err = eb.withAuthorActivityEventDescriptor(ictx, evt)
	if err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	return ictx, admitted, nil
}

func reuseDurableSubscribedEventRouteFacts(admitted, durable events.AdmittedEvent) (events.AdmittedEvent, events.Event, error) {
	evt := admitted.Event()
	durableEvent := durable.Event()
	if evt.ID() == "" || durableEvent.ID() != evt.ID() {
		return admitted, evt, errors.New("durable event route facts do not match the admitted event identity")
	}
	durableTargets := eventDeliveryTargetRoutes(durableEvent)
	existingTargets := eventDeliveryTargetRoutes(evt)
	if len(existingTargets) > 0 && !sameRouteIdentities(existingTargets, durableTargets) && !routeIdentitiesCanBeExactlyCompleted(existingTargets, durableTargets) {
		return admitted, evt, errors.New("durable event route facts conflict with the admitted event target")
	}
	if len(durableTargets) == 0 || sameRouteIdentities(existingTargets, durableTargets) {
		return admitted, evt, nil
	}
	envelope := evt.NormalizedEnvelope()
	if len(durableTargets) == 1 {
		envelope = events.EnvelopeForTargetRoute(envelope, durableTargets[0])
	} else {
		envelope = events.EnvelopeForTargetSet(envelope, durableTargets)
	}
	resolved, err := events.ResolveEnvelope(evt, envelope)
	if err != nil {
		return admitted, evt, fmt.Errorf("reuse durable event route facts: %w", err)
	}
	resolvedAdmission, err := events.AdmitForPersistence(resolved, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return admitted, evt, fmt.Errorf("admit durable event route facts: %w", err)
	}
	return resolvedAdmission, resolvedAdmission.Event(), nil
}

func validateCanonicalConnectRouteEvent(evt events.Event, plan RoutePlan) error {
	resolved, _, err := resolveCanonicalConnectRouteEvent(evt, plan)
	if err != nil {
		return err
	}
	if !sameEventTargetFacts(evt, resolved) {
		return errors.New("connect route facts changed after event admission")
	}
	return nil
}

func resolveCanonicalConnectRouteEvent(evt events.Event, plan RoutePlan) (events.Event, bool, error) {
	plan = plan.Normalized()
	if plan.AuthorityState != RoutePlanAuthorityCanonicalMatched || plan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		return evt, false, nil
	}
	targets := make([]events.RouteIdentity, 0, len(plan.DeliveryIntents))
	for _, route := range plan.DeliveryRoutes() {
		if target := route.Target.Route(); !target.Empty() {
			targets = append(targets, target)
		}
	}
	targets = uniqueRouteIdentities(targets)
	if len(targets) == 0 {
		return evt, false, nil
	}
	existing := uniqueRouteIdentities(eventDeliveryTargetRoutes(evt))
	if len(existing) > 0 && !sameRouteIdentities(existing, targets) && !routeIdentitiesCanBeExactlyCompleted(existing, targets) {
		return evt, false, fmt.Errorf("connect route facts conflict with the admitted event target: admitted=%#v planned=%#v", existing, targets)
	}
	envelope := evt.NormalizedEnvelope()
	if len(targets) == 1 {
		envelope = events.EnvelopeForTargetRoute(envelope, targets[0])
	} else {
		envelope = events.EnvelopeForTargetSet(envelope, targets)
	}
	resolved, err := events.ResolveEnvelope(evt, envelope)
	if err != nil {
		return evt, false, fmt.Errorf("resolve canonical connect route facts: %w", err)
	}
	return resolved, !sameEventTargetFacts(evt, resolved), nil
}

func sameEventTargetFacts(left, right events.Event) bool {
	return sameRouteIdentities(eventDeliveryTargetRoutes(left), eventDeliveryTargetRoutes(right))
}

func sameRouteIdentities(left, right []events.RouteIdentity) bool {
	left = uniqueRouteIdentities(left)
	right = uniqueRouteIdentities(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Normalized() != right[index].Normalized() {
			return false
		}
	}
	return true
}

func routeIdentitiesCanBeExactlyCompleted(existing, canonical []events.RouteIdentity) bool {
	existing = uniqueRouteIdentities(existing)
	canonical = uniqueRouteIdentities(canonical)
	if len(existing) == 0 || len(existing) > len(canonical) {
		return false
	}
	canonicalRoutes := make(map[events.RouteIdentity]struct{}, len(canonical))
	byEntity := make(map[string]events.RouteIdentity, len(canonical))
	ambiguousEntities := make(map[string]struct{})
	for _, route := range canonical {
		route = route.Normalized()
		if route.EntityID == "" || route.FlowID == "" || route.FlowInstance == "" {
			return false
		}
		canonicalRoutes[route] = struct{}{}
		if _, duplicate := byEntity[route.EntityID]; duplicate {
			delete(byEntity, route.EntityID)
			ambiguousEntities[route.EntityID] = struct{}{}
			continue
		}
		if _, ambiguous := ambiguousEntities[route.EntityID]; !ambiguous {
			byEntity[route.EntityID] = route
		}
	}
	for _, route := range existing {
		route = route.Normalized()
		if _, exact := canonicalRoutes[route]; exact {
			continue
		}
		if route.EntityID == "" || route.FlowID != "" || route.FlowInstance != "" {
			return false
		}
		if _, ok := byEntity[route.EntityID]; !ok {
			return false
		}
	}
	return true
}

// CommitDynamicFlowRuntimeCreationOccurrence commits the creation occurrence
// and its readiness completion through one closed selected-store operation.
// No transaction callback or ambient SQL authority crosses this boundary.
func (eb *EventBus) CommitDynamicFlowRuntimeCreationOccurrence(
	ctx context.Context,
	req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
) error {
	if err := req.Validate(); err != nil {
		return err
	}
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	request := req
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{
		bus:                 eb,
		event:               req.Event,
		dynamicFlowCreation: &request,
	})
	if err != nil {
		return err
	}
	if prepared.exactDuplicate {
		return eb.DispatchPreparedPublish(ctx, prepared)
	}
	return eb.DispatchPreparedPublishAsync(ctx, prepared)
}

// DispatchPreparedPublish consumes only a plan finalized by a closed named
// selected-store operation. It never invokes route planning again.
func (eb *EventBus) DispatchPreparedPublish(ctx context.Context, prepared PreparedPublish) (err error) {
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	return eb.dispatchPreparedPublish(dispatchCtx, prepared)
}

// DispatchPreparedPublishAndWait dispatches one committed publish and joins
// the complete local-delivery tree produced by its handlers. It is intended
// for bounded runtimes that must finish their accepted story before retiring.
func (eb *EventBus) DispatchPreparedPublishAndWait(ctx context.Context, prepared PreparedPublish) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	group := newLocalDeliveryCompletionGroup()
	waitCtx := ctx
	prepared.receiver = prepared.receiver.withCompletion(group)
	return eb.dispatchPreparedPublishWithCompletion(dispatchCtx, prepared, func() error {
		group.releaseDispatch()
		return group.wait(waitCtx)
	})
}

func (eb *EventBus) dispatchPreparedPublish(ctx context.Context, prepared PreparedPublish) error {
	return eb.dispatchPreparedPublishWithCompletion(ctx, prepared, nil)
}

func (eb *EventBus) dispatchPreparedPublishWithCompletion(ctx context.Context, prepared PreparedPublish, completion func() error) (err error) {
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, prepared.publicationClaim.Release(dispatchCtx))
	}()
	if strings.TrimSpace(prepared.Event.ID()) == "" {
		return errors.New("prepared event is required")
	}
	dispatchErr := eb.dispatchPreparedPublishBody(dispatchCtx, prepared)
	if completion == nil {
		return dispatchErr
	}
	return errors.Join(dispatchErr, completion())
}

func (eb *EventBus) dispatchPreparedPublishBody(ctx context.Context, prepared PreparedPublish) error {
	if prepared.exactDuplicate {
		return nil
	}
	handoffs, err := prepared.CommittedDeliveryHandoffs()
	if err != nil {
		return fmt.Errorf("read committed delivery handoffs: %w", err)
	}
	if err := eb.AcceptCommittedDeliveryHandoffs(handoffs); err != nil {
		return err
	}
	if prepared.targetFailure {
		eb.logPublished(ctx, prepared.Event, 0)
		return nil
	}
	if prepared.dispatchQueued {
		eb.logDispatchQueued(ctx, prepared.queueReason, prepared.Event, len(prepared.RecipientIDs()), prepared.direct, true)
		eb.logPublished(ctx, prepared.Event, 0)
		return nil
	}
	return eb.completeCommittedPublishDispatch(prepared.Event, prepared.plan, prepared.publicationClaim, prepared.receiver)
}

// AcceptCommittedDeliveryHandoffs transfers exact selected-store commit
// results into the process-local generation owner before dispatch.
func (eb *EventBus) AcceptCommittedDeliveryHandoffs(handoffs []runtimedelivery.DurableHandoffProof) error {
	if len(handoffs) == 0 {
		return nil
	}
	owner := eb.DeliveryContinuationOwner()
	if owner == nil {
		return errors.New("committed executable deliveries require an exact continuation owner")
	}
	if err := owner.AcceptCommitted(handoffs); err != nil {
		return fmt.Errorf("transfer committed executable deliveries: %w", err)
	}
	return nil
}

func (eb *EventBus) DispatchPreparedPublishAsync(ctx context.Context, prepared PreparedPublish) error {
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	releaseOnFailure := func(err error) error {
		return errors.Join(err, prepared.publicationClaim.Release(dispatchCtx))
	}
	if !prepared.requiresReceiver() {
		return eb.dispatchPreparedPublish(dispatchCtx, prepared)
	}
	if eb == nil || eb.workOwner == nil {
		return releaseOnFailure(errors.New("asynchronous event dispatch requires a runtime work occurrence"))
	}
	if strings.TrimSpace(prepared.Event.ID()) == "" {
		return releaseOnFailure(errors.New("prepared event is required"))
	}
	owner := prepared.receiver.occurrence
	lease, err := owner.Begin(context.Background())
	if err != nil {
		return releaseOnFailure(fmt.Errorf("admit asynchronous event dispatch: %w", err))
	}
	dispatchCtx, closeDispatchContext := eventreceiver.NewContext(lease.Context())
	dispatchCtx, err = eb.admitBundleSourceFact(dispatchCtx)
	if err != nil {
		closeDispatchContext()
		_ = lease.Done()
		return releaseOnFailure(err)
	}
	go func() {
		defer closeDispatchContext()
		defer func() { _ = lease.Done() }()
		if err := eb.dispatchPreparedPublish(dispatchCtx, prepared); err != nil {
			eb.reportLocalDispatchFailure("async_dispatch_failed", prepared.Event, err)
		}
	}()
	return nil
}

func (eb *EventBus) reportLocalDispatchFailure(action string, evt events.Event, err error) {
	if err == nil {
		return
	}
	diaglog.ProcessLog(diaglog.LevelError, "eventbus", "local committed event dispatch failed",
		"action", strings.TrimSpace(action),
		"event_id", strings.TrimSpace(evt.ID()),
		"event_type", strings.TrimSpace(string(evt.Type())),
		"error", err.Error(),
	)
}

func (eb *EventBus) completeCommittedPublishDispatch(evt events.Event, inboundPlan RoutePlan, publicationClaim *pipelinePublicationClaim, projection receiverDispatchProjection) (err error) {
	receiverCtx, closeReceiver, err := eb.beginReceiverDispatch(projection, evt)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeReceiver()) }()
	ctx := receiverCtx.Context
	workCtx := receiverCtx.Context
	eb.notifyTestPostCommitDispatchStarted(workCtx, evt)
	defer eb.notifyTestPostCommitDispatchCompleted(workCtx, evt)

	inboundPlan = inboundPlan.Normalized()

	passthrough, deferred, outcome, err := eb.runInterceptorsForDeliveryRoutes(workCtx, evt, inboundPlan.DeliveryRoutes())
	if err != nil {
		return errors.Join(err, eb.settleCommittedPublish(ctx, publicationClaim, committedPublishFailureDisposition(evt, "pipeline_dispatch_failed", eventBusFailure(err, "dispatch_committed_publish"))))
	}
	if _, retry := outcome.RetryRelease(); retry {
		return nil
	}
	if disposition, ok := outcome.Disposition(); ok {
		return eb.settleCommittedPublish(ctx, publicationClaim, disposition)
	}

	if passthrough {
		recipients := inboundPlan.RecipientIDs()
		if len(recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipientIDs(), "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverRoutePlanWithRoutes(workCtx, evt, inboundPlan); err != nil {
				if errors.Is(err, errAuthoritativeDeliveryIncomplete) {
					return err
				}
				return errors.Join(err, eb.settleCommittedPublish(ctx, publicationClaim, committedPublishFailureDisposition(evt, "pipeline_delivery_failed", eventBusFailure(err, "deliver_route_plan"))))
			}
			eb.logDelivery(ctx, evt, recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(workCtx, *inboundPlan.CycleEscalation); err != nil {
				return errors.Join(err, eb.settleCommittedPublish(ctx, publicationClaim, committedPublishFailureDisposition(evt, "pipeline_deferred_publish_failed", eventBusFailure(err, "publish_deferred"))))
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(workCtx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, 0)

	for _, d := range deferred {
		if err := eb.publishDeferred(workCtx, d); err != nil {
			return errors.Join(err, eb.settleCommittedPublish(ctx, publicationClaim, committedPublishFailureDisposition(evt, "pipeline_deferred_publish_failed", eventBusFailure(err, "publish_deferred"))))
		}
	}
	if evt.Type() == events.EventType("mailbox.card_decided") {
		if err := publicationClaim.MarkDecisionProcessed(ctx); err != nil {
			return err
		}
		return eb.settleCommittedPublish(ctx, publicationClaim, runtimepipelineobligation.Acknowledged("decision_route_settled"))
	}
	if err := eb.settleCommittedPublish(ctx, publicationClaim, runtimepipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		return err
	}
	return nil
}

func (eb *EventBus) settleCommittedPublish(ctx context.Context, claim *pipelinePublicationClaim, disposition runtimepipelineobligation.Disposition) error {
	if claim == nil {
		if eb != nil && eb.pipelineObligations != nil {
			return errors.New("committed pipeline dispatch requires its publication claim")
		}
		return nil
	}
	return claim.Settle(ctx, disposition)
}

func committedPublishFailureDisposition(evt events.Event, reason string, failure *runtimefailures.Envelope) runtimepipelineobligation.Disposition {
	reason = pipelineDispositionFailureReason(reason, failure)
	if evt.Type() == events.EventType("mailbox.card_decided") {
		return runtimepipelineobligation.Quarantined(reason, failure)
	}
	return runtimepipelineobligation.Terminal(reason, failure)
}

func pipelineDispositionFailureReason(fallback string, failure *runtimefailures.Envelope) string {
	if failure != nil {
		if code := strings.TrimSpace(failure.Detail.Code); code != "" {
			return code
		}
	}
	return strings.TrimSpace(fallback)
}

func (eb *EventBus) runInterceptorsForDeliveryRoutes(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	interceptors := eb.interceptorsSnapshot()
	nodeRoutes := nodeDeliveryRoutes(deliveryRoutes)
	if len(nodeRoutes) == 0 {
		return eb.runInterceptorSet(ctx, evt, interceptors)
	}
	eventInterceptors, routeInterceptors := splitDeliveryRouteInterceptors(interceptors)
	passthrough, deferred, outcome, err := eb.runInterceptorSet(ctx, evt, eventInterceptors)
	if err != nil {
		return passthrough, nil, runtimepipelineobligation.Continue(), err
	}
	if !outcome.ContinueDispatch() {
		return passthrough, deferred, outcome, nil
	}
	routePassthrough, routeDeferred, routeOutcome, err := eb.runNodeDeliveryRouteInterceptors(ctx, evt, nodeRoutes, routeInterceptors)
	if err != nil {
		return passthrough, nil, runtimepipelineobligation.Continue(), err
	}
	if len(routeDeferred) > 0 {
		deferred = append(deferred, routeDeferred...)
	}
	return passthrough && routePassthrough, deferred, routeOutcome, nil
}

func nodeDeliveryRoutes(deliveryRoutes []events.DeliveryRoute) []events.DeliveryRoute {
	routes := make([]events.DeliveryRoute, 0)
	for _, route := range events.NormalizeDeliveryRoutes(deliveryRoutes) {
		if !route.Recipient.IsNode() {
			continue
		}
		routes = append(routes, route)
	}
	return routes
}

func splitDeliveryRouteInterceptors(interceptors []EventInterceptor) ([]EventInterceptor, []DeliveryRouteInterceptor) {
	eventInterceptors := make([]EventInterceptor, 0, len(interceptors))
	routeInterceptors := make([]DeliveryRouteInterceptor, 0, len(interceptors))
	for _, it := range interceptors {
		if it == nil {
			continue
		}
		if routeInterceptor, ok := it.(DeliveryRouteInterceptor); ok {
			routeInterceptors = append(routeInterceptors, routeInterceptor)
			continue
		}
		eventInterceptors = append(eventInterceptors, it)
	}
	return eventInterceptors, routeInterceptors
}

func (eb *EventBus) runNodeDeliveryRouteInterceptors(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, interceptors []DeliveryRouteInterceptor) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if err := events.ValidateDeliveryRouteProjections(deliveryRoutes); err != nil {
		return true, nil, runtimepipelineobligation.Continue(), err
	}
	deliveryRoutes = nodeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 || len(interceptors) == 0 {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	passthrough := true
	deferred := make([]events.Event, 0)
	type nodeDeliveryRouteKey struct {
		recipient      events.DeliveryRecipient
		target         events.RouteIdentity
		replyContextID string
		projection     string
	}
	seen := map[nodeDeliveryRouteKey]struct{}{}
	for _, route := range deliveryRoutes {
		target := route.Target.Route()
		key := nodeDeliveryRouteKey{
			recipient: route.Recipient, target: target,
			replyContextID: route.Context.ReplyContextID(), projection: route.PayloadProjection.Fingerprint(),
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		projected, err := projectEventForDeliveryRoute(evt, route)
		if err != nil {
			return passthrough, nil, runtimepipelineobligation.Continue(), err
		}
		routeCtx := events.WithDeliveryContext(ctx, route.Context)
		routeCtx = runtimedelivery.WithRoute(routeCtx, route)
		for _, it := range interceptors {
			pass, out, outcome, err := it.InterceptDeliveryRoute(routeCtx, projected, route)
			if err != nil {
				return passthrough, nil, runtimepipelineobligation.Continue(), err
			}
			if !outcome.ContinueDispatch() {
				return passthrough, deferred, outcome, nil
			}
			if !pass {
				passthrough = false
			}
			admitted, err := admitDeferredEvents(routeCtx, out)
			if err != nil {
				return passthrough, nil, runtimepipelineobligation.Continue(), err
			}
			deferred = append(deferred, admitted...)
		}
	}
	return passthrough, deferred, runtimepipelineobligation.Continue(), nil
}

func projectEventForDeliveryRoute(evt events.Event, route events.DeliveryRoute) (events.DeliveryEvent, error) {
	projected, err := events.NewDeliveryEvent(evt, route)
	if err != nil {
		return events.DeliveryEvent{}, fmt.Errorf("delivery route for %s: %w", route.Recipient.ID(), err)
	}
	return projected, nil
}

func (eb *EventBus) admitBundleSourceFact(ctx context.Context) (context.Context, error) {
	if eb == nil {
		return ctx, errors.New("event bus is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	eb.mu.RLock()
	runtimeInstanceID := eb.runtimeInstanceID
	sourceFact := eb.bundleSourceFact
	eb.mu.RUnlock()

	contextRuntimeInstanceID, hasContextRuntimeInstanceID := runtimecorrelation.RuntimeInstanceIDFromContext(ctx)
	contextScope, hasContextScope := runtimeauthoractivity.ScopeFromContext(ctx)
	if hasContextScope && contextScope.Kind == runtimeauthoractivity.ScopeBundle {
		if hasContextRuntimeInstanceID && contextRuntimeInstanceID != contextScope.RuntimeInstanceID {
			return ctx, errors.New("event bus runtime instance conflicts with bundle scope")
		}
		contextRuntimeInstanceID = contextScope.RuntimeInstanceID
		hasContextRuntimeInstanceID = contextRuntimeInstanceID != ""
	}
	if runtimeInstanceID == "" {
		runtimeInstanceID = contextRuntimeInstanceID
	} else if hasContextRuntimeInstanceID && contextRuntimeInstanceID != runtimeInstanceID {
		return ctx, errors.New("event bus runtime instance conflicts with publication context")
	}
	contextFact, hasContextFact := runtimecorrelation.BundleSourceFactFromContext(ctx)
	hasOwnedFact := sourceFact.Validate() == nil
	if !hasOwnedFact && !eb.ephemeral {
		return ctx, errors.New("event bus bundle source fact is required")
	}
	if !hasOwnedFact && !hasContextFact {
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
		return ctx, nil
	}
	if hasOwnedFact && hasContextFact && !sourceFact.Matches(contextFact) {
		return ctx, errors.New("event bus bundle source fact conflicts with publication context")
	}
	admittedFact := contextFact
	if hasOwnedFact {
		admittedFact = sourceFact
		if !hasContextFact {
			ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
		}
	}
	if hasContextScope && contextScope.Kind == runtimeauthoractivity.ScopeBundle && admittedFact.Validate() == nil && contextScope.BundleHash != admittedFact.BundleHash() {
		return ctx, errors.New("event bus bundle source fact conflicts with bundle scope")
	}
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	if runtimeInstanceID != "" && admittedFact.Validate() == nil {
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, admittedFact.BundleHash()))
	}
	return ctx, nil
}

func (eb *EventBus) withAuthorActivityEventDescriptor(ctx context.Context, evt events.Event) (context.Context, error) {
	ctx = runtimeauthoractivity.WithoutResolvedEventDescriptor(ctx)
	if eb == nil || eb.semanticSource == nil {
		return ctx, nil
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
		return ctx, nil
	}
	name := strings.TrimSpace(string(evt.Type()))
	proof := semanticview.ResolveFlowEventProof(eb.semanticSource, evt.SourceRoute().FlowID, name)
	if !proof.HasSchema {
		return ctx, nil
	}
	disposition := runtimeauthoractivity.StoryDifferent
	if _, authored := eb.semanticSource.AuthoredResolvedEventCatalog()[strings.TrimSpace(proof.CatalogKey)]; authored {
		disposition = runtimeauthoractivity.StoryAuthored
	}
	return runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, runtimeauthoractivity.EventDescriptor{
		EventType:          name,
		Disposition:        disposition,
		AuthorSummaryField: strings.TrimSpace(proof.Entry.AuthorSummaryField),
	})
}

func (eb *EventBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
	return eb.admitBundleSourceFact(ctx)
}

func (eb *EventBus) runInterceptors(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return eb.runInterceptorSet(ctx, evt, eb.interceptorsSnapshot())
}

func (eb *EventBus) interceptorsSnapshot() []EventInterceptor {
	eb.mu.RLock()
	interceptors := append([]EventInterceptor(nil), eb.interceptors...)
	provider := eb.interceptorProvider
	eb.mu.RUnlock()
	if provider != nil {
		for _, it := range provider() {
			if it != nil {
				interceptors = append(interceptors, it)
			}
		}
	}
	return interceptors
}

func (eb *EventBus) runInterceptorSet(ctx context.Context, evt events.Event, interceptors []EventInterceptor) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if len(interceptors) == 0 {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	for _, it := range interceptors {
		pass, out, outcome, err := it.Intercept(ctx, evt)
		if err != nil {
			return true, nil, runtimepipelineobligation.Continue(), runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "event_interceptor_failed", "eventbus", "run_interceptor", map[string]any{
				"event_id": evt.ID(), "event_type": string(evt.Type()),
			}, err)
		}
		if !outcome.ContinueDispatch() {
			return pass, deferred, outcome, nil
		}
		if !pass {
			passthrough = false
		}
		admitted, err := admitDeferredEvents(ctx, out)
		if err != nil {
			return true, nil, runtimepipelineobligation.Continue(), err
		}
		deferred = append(deferred, admitted...)
	}
	return passthrough, deferred, runtimepipelineobligation.Continue(), nil
}

func admitDeferredEvents(ctx context.Context, out []events.Event) ([]events.Event, error) {
	if len(out) == 0 {
		return nil, nil
	}
	deferred := make([]events.Event, 0, len(out))
	for _, d := range out {
		_, admitted, err := admitEventForPublish(ctx, d, time.Now())
		if err != nil {
			return nil, err
		}
		deferred = append(deferred, admitted.Event())
	}
	return deferred, nil
}

func admitEventForPublish(ctx context.Context, evt events.Event, now time.Time) (context.Context, events.AdmittedEvent, error) {
	admitted, err := events.AdmitForPublish(evt, events.AdmissionOptions{Now: now, RequirePersistentUUIDIdentity: true})
	if err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	event := admitted.Event()
	ctx = events.WithDeliveryContext(ctx, event.DeliveryContext())
	if runID := strings.TrimSpace(event.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	return ctx, admitted, nil
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) (err error) {
	if evt.Type() == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type())))
	}
	var admitted events.AdmittedEvent
	ctx, admitted, err = admitEventForPublish(ctx, evt, time.Now())
	if err != nil {
		return err
	}
	evt = admitted.Event()
	if handled, err := (engineDispatcher{bus: eb}).dispatchPendingOutboxOperation(ctx, runtimeengine.EmitIntent{Event: evt, Context: evt.DeliveryContext()}); handled {
		return err
	}
	return eb.Publish(ctx, evt)
}

func (eb *EventBus) logPublished(ctx context.Context, evt events.Event, durationUS int) {
	eb.logRuntime(ctx, "debug", "Event was published to the event bus", "eventbus", "published", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, map[string]any{
		"type":            string(evt.Type()),
		"source":          evt.SourceAgent(),
		"parent_event_id": strings.TrimSpace(evt.ParentEventID()),
	}, nil, durationUS)
}

func (eb *EventBus) planSubscribedPublish(ctx context.Context, evt events.Event) (RoutePlan, error) {
	return eb.planSubscribedRoutePlan(ctx, evt, true)
}

func (eb *EventBus) planSubscribedRoutePlan(ctx context.Context, evt events.Event, recordDiagnostic bool) (RoutePlan, error) {
	if err := eb.authorizePublishRecipientPlanning(ctx, evt); err != nil {
		return RoutePlan{}, err
	}
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	plan, err = eb.materializePublishRecipientPlan(ctx, evt, plan)
	if err != nil {
		return RoutePlan{}, err
	}
	if err := eb.authorizePublishRecipientPlan(ctx, evt, plan); err != nil {
		return RoutePlan{}, err
	}
	routePlan := plan.Normalized()
	routePlan = routePlan.WithDefaultDeliveryContext(events.DeliveryContextFromContext(ctx))
	if recordDiagnostic {
		eb.recordPublishDiagnostic(ctx, evt, routePlan)
	}
	return routePlan, nil
}

func (eb *EventBus) authorizePublishRecipientPlanning(ctx context.Context, evt events.Event) error {
	if eb == nil || eb.recipientPlanAdmissionGuard == nil {
		return nil
	}
	return eb.recipientPlanAdmissionGuard(ctx, evt)
}

func (eb *EventBus) materializePublishRecipientPlan(ctx context.Context, evt events.Event, routePlan RoutePlan) (RoutePlan, error) {
	routePlan = routePlan.Normalized()
	if eb == nil || eb.recipientPlanMaterializer == nil {
		return routePlan, nil
	}
	if !routePlan.AllowsLowerPrecedenceRouteProduction() {
		return routePlan, nil
	}
	routes, err := eb.recipientPlanMaterializer(ctx, evt, eb.publishRecipientPlan(evt, routePlan))
	if err != nil {
		return RoutePlan{}, err
	}
	if len(routes) == 0 {
		return routePlan, nil
	}
	routePlan.MarkLowerPrecedenceRouteProduction(routeIntentProducerRecipientMaterializer)
	routePlan.AddDeliveryIntents(routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerRecipientMaterializer)...)
	projection, err := eb.deliveryPlanner.recipientPolicy.loadSelectedRunTargetOwnerProjection(runtimecorrelation.WithInboundEvent(ctx, evt))
	if err != nil {
		return RoutePlan{}, err
	}
	projection, err = projection.withActivationPlans(routePlan.ActivationPlans)
	if err != nil {
		return RoutePlan{}, err
	}
	return projection.resolveRoutePlan(routePlan)
}

func (eb *EventBus) authorizePublishRecipientPlan(ctx context.Context, evt events.Event, routePlan RoutePlan) error {
	if eb == nil || eb.recipientPlanGuard == nil {
		return nil
	}
	return eb.recipientPlanGuard(ctx, evt, eb.publishRecipientPlan(evt, routePlan))
}

func (eb *EventBus) publishRecipientPlan(evt events.Event, routePlan RoutePlan) PublishRecipientPlan {
	routePlan = routePlan.Normalized()
	deliveryRoutes := routePlan.DeliveryRoutes()
	classification := runtimepinrouting.ClassifyRoutingSourceOutputConsumer(eb.semanticSource, string(evt.Type()), evt.RoutingSource())
	out := PublishRecipientPlan{
		Recipients:             routePlan.RecipientIDs(),
		PersistedRecipients:    routePlan.PersistedRecipientIDs(),
		SubscriptionRecipients: uniqueStrings(routePlan.SubscribedRecipients),
		DeliveryRoutes:         deliveryRoutes,
		TargetFailure:          routePlan.TargetFailure.Code(),
		canonicalAuthority: routePlan.CanonicalRouteOwnerMatched() || len(deliveryRoutes) > 0 ||
			!routePlan.TargetFailure.Empty() || classification.DeliberateNoSubscriber(),
	}
	if eb != nil {
		out.RoutedRecipients = eb.describeSubscribersForEvent(string(evt.Type()), routePlan.RoutedRecipients)
	}
	return out
}

func (eb *EventBus) logDelivery(ctx context.Context, evt events.Event, recipients []string, extra map[string]any) {
	detail := map[string]any{
		"recipients_count": len(recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID()),
	}
	for k, v := range extra {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "Event was delivered to recipients", "eventbus", "delivered", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, detail, nil, 0)
}

func (eb *EventBus) logQueuedDeliveries(ctx context.Context, evt events.Event, recipients []string, reason string, extra map[string]any) {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return
	}
	for _, recipient := range recipients {
		detail := map[string]any{
			"delivery_state":          string(runtimedelivery.StateQueued),
			"delivery_transition":     string(runtimedelivery.StateQueued),
			"delivery_previous_state": "",
			"delivery_reason":         strings.TrimSpace(reason),
			"subscriber_type":         "agent",
			"subscriber_id":           strings.TrimSpace(recipient),
			"parent_event_id":         strings.TrimSpace(evt.ParentEventID()),
		}
		for k, v := range extra {
			detail[k] = v
		}
		eb.logRuntime(ctx, "debug", "Delivery entered queued state", "eventbus", "delivery_lifecycle_transition", evt.ID(), string(evt.Type()), strings.TrimSpace(recipient), evt.EntityID(), "", nil, detail, nil, 0)
	}
}

func subscriberIDs(in []Subscriber) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, subscriber := range in {
		if subscriber.Recipient.Empty() {
			continue
		}
		out = append(out, subscriber.Recipient.ID())
	}
	return uniqueStrings(out)
}

func publishDiagnosticRecipientMaps(in []PublishDiagnosticRecipient) []map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(in))
	for _, recipient := range in {
		item := map[string]any{
			"id": recipient.ID,
		}
		if v := strings.TrimSpace(recipient.Type); v != "" {
			item["type"] = v
		}
		if v := strings.TrimSpace(recipient.Path); v != "" {
			item["path"] = v
		}
		if v := strings.TrimSpace(recipient.MatchedPattern); v != "" {
			item["matched_pattern"] = v
		}
		if v := strings.TrimSpace(recipient.RouteSource); v != "" {
			item["route_source"] = v
		}
		if v := strings.TrimSpace(recipient.LocalizedEvent); v != "" {
			item["localized_event"] = v
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) describeSubscribersForEvent(eventType string, in []Subscriber) []PublishDiagnosticRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]PublishDiagnosticRecipient, 0, len(in))
	for _, subscriber := range in {
		if subscriber.Recipient.Empty() {
			continue
		}
		item := PublishDiagnosticRecipient{
			ID:             subscriber.Recipient.ID(),
			Type:           subscriber.Recipient.Code(),
			Path:           strings.TrimSpace(subscriber.Path),
			MatchedPattern: strings.TrimSpace(subscriber.MatchPattern),
			RouteSource:    subscriber.RouteSourceCode(),
		}
		if localized := eb.localizedSubscriberEvent(eventType, subscriber); localized != "" {
			item.LocalizedEvent = localized
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) localizedSubscriberEvent(eventType string, subscriber Subscriber) string {
	if !subscriber.Recipient.IsNode() {
		return ""
	}
	if localized := eventidentity.Normalize(subscriber.LocalizedEvent); localized != "" {
		return localized
	}
	candidates := []string{eventType, subscriber.MatchPattern}
	if eb != nil && eb.semanticSource != nil {
		flowID := strings.TrimSpace(routeFlowIDForPath(eb.semanticSource, subscriber.Path))
		if flowID != "" {
			scope := eventidentity.Scope{
				Path:        strings.Trim(strings.TrimSpace(subscriber.Path), "/"),
				InputEvents: append([]string{}, eb.semanticSource.FlowInputEvents(flowID)...),
			}
			for _, candidate := range candidates {
				if localized := scope.LocalizeInput(candidate); localized != "" && localized != eventidentity.Normalize(candidate) {
					return localized
				}
			}
		}
	}
	for _, candidate := range candidates {
		normalized := eventidentity.Normalize(candidate)
		if leaf := eventidentity.LeafName(normalized); leaf != "" && leaf != normalized {
			return leaf
		}
	}
	return ""
}

func (eb *EventBus) recordPublishDiagnostic(ctx context.Context, evt events.Event, routePlan RoutePlan) {
	rec, ok := EmittedEventsRecorderFromContext(ctx)
	if !ok || rec == nil {
		return
	}
	routePlan = routePlan.Normalized()
	rec.AppendPublish(PublishDiagnostic{
		EventID:                strings.TrimSpace(evt.ID()),
		EventType:              strings.TrimSpace(string(evt.Type())),
		EntityID:               strings.TrimSpace(evt.EntityID()),
		ParentEventID:          strings.TrimSpace(evt.ParentEventID()),
		RoutedRecipients:       eb.describeSubscribersForEvent(string(evt.Type()), routePlan.RoutedRecipients),
		SubscriptionRecipients: uniqueStrings(routePlan.SubscribedRecipients),
	})
}

func (eb *EventBus) planDirectRoutePlan(ctx context.Context, evt events.Event, recipients []string) (RoutePlan, error) {
	plan, err := eb.deliveryPlanner.PlanDirect(ctx, evt, recipients)
	if err != nil {
		return RoutePlan{}, err
	}
	return plan.WithDefaultDeliveryContext(events.DeliveryContextFromContext(ctx)), nil
}

// planExactDirectRoutePlan uses current policy only to resolve live recipient
// capabilities. The supplied routes remain the sole persistence and delivery
// authority for target, context, and payload projection facts.
func (eb *EventBus) planExactDirectRoutePlan(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) (RoutePlan, error) {
	routes = events.NormalizeDeliveryRoutes(routes)
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return RoutePlan{}, err
	}
	plan, err := eb.deliveryPlanner.PlanExactDirect(ctx, evt, routes)
	if err != nil {
		return RoutePlan{}, err
	}
	if err := plan.ValidatePersistentDeliveries(); err != nil {
		return RoutePlan{}, fmt.Errorf("validate exact direct delivery routes: %w", err)
	}
	return plan.WithDefaultDeliveryContext(events.DeliveryContextFromContext(ctx)), nil
}

// PublishDirect persists an event and delivers it to an explicit caller-supplied
// recipient set. The recipient manifest still routes through the canonical
// delivery policy so explicit delivery cannot bypass scoped-recipient rules.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt, direct: true, directRecipients: uniqueStrings(recipients)})
	if err != nil {
		return err
	}
	return eb.dispatchPreparedPublish(ctx, prepared)
}

// PublishDirectRoutes persists and dispatches exactly the caller-supplied
// agent routes. It is the closed public-replay boundary: current policy may
// prove recipient availability but cannot replace route-owned facts.
func (eb *EventBus) PublishDirectRoutes(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) error {
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{
		bus: eb, event: evt, direct: true, directRoutes: events.NormalizeDeliveryRoutes(routes),
	})
	if err != nil {
		return err
	}
	return eb.dispatchPreparedPublish(ctx, prepared)
}

func (eb *EventBus) beginRuntimeWork(ctx context.Context) (context.Context, *worklifetime.Lease, error) {
	if eb == nil {
		return ctx, nil, errors.New("event bus is required")
	}
	admittedCtx, err := eb.admitBundleSourceFact(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if eb.workOwner == nil {
		return admittedCtx, nil, errors.New("event bus requires a process work occurrence")
	}
	owner := eb.workOwnerForContext(admittedCtx)
	lease, err := owner.Begin(admittedCtx)
	if err != nil {
		return admittedCtx, nil, fmt.Errorf("admit event bus work: %w", err)
	}
	return bindWorkContext(admittedCtx, lease, owner), lease, nil
}

func (eb *EventBus) workOwnerForContext(ctx context.Context) worklifetime.Occurrence {
	if owner, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		return owner
	}
	return eb.workOwner
}

func bindWorkContext(ctx context.Context, lease *worklifetime.Lease, owner worklifetime.Occurrence) context.Context {
	if lease == nil {
		return ctx
	}
	workCtx := lease.Context()
	if _, ok := worklifetime.OccurrenceFromContext(workCtx); !ok {
		workCtx = worklifetime.WithOccurrence(workCtx, owner)
	}
	if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); ok {
		workCtx = runtimeauthoractivity.WithScope(workCtx, scope)
	}
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		workCtx = runtimecorrelation.WithBundleSourceFact(workCtx, fact)
	}
	if runtimeID, ok := runtimecorrelation.RuntimeInstanceIDFromContext(ctx); ok {
		workCtx = runtimecorrelation.WithRuntimeInstanceID(workCtx, runtimeID)
	}
	return workCtx
}

// CheckDirectRoutes applies the same exact-route policy used by
// PublishDirectRoutes without creating replay evidence.
func (eb *EventBus) CheckDirectRoutes(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) (ExactDirectRouteStatus, error) {
	requested := events.NormalizeDeliveryRoutes(routes)
	status := ExactDirectRouteStatus{Requested: append([]events.DeliveryRoute(nil), requested...)}
	if eb == nil {
		status.Missing = append([]events.DeliveryRoute(nil), requested...)
		return status, nil
	}
	if evt.Type() == "" {
		return status, errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return status, fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return status, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	plan, err := eb.planExactDirectRoutePlan(ctx, evt, requested)
	if err != nil {
		var unavailable *exactDirectRecipientsUnavailableError
		if errors.As(err, &unavailable) {
			missing := make(map[agentidentity.Identity]struct{}, len(unavailable.identities))
			for _, identity := range unavailable.identities {
				missing[identity.Normalize()] = struct{}{}
			}
			for _, route := range requested {
				if _, ok := missing[route.AgentIdentity.Normalize()]; ok {
					status.Missing = append(status.Missing, route)
				} else {
					status.Deliverable = append(status.Deliverable, route)
				}
			}
			return status, nil
		}
		return status, err
	}
	plannedRecipients := plan.RecipientIDs()
	liveRecipients := eb.snapshotRoutePlanRecipientChans(plannedRecipients, plan.LiveRecipients)
	live := make(map[agentidentity.Identity]struct{}, len(liveRecipients))
	for _, recipient := range liveRecipients {
		if !recipient.identity.IsZero() {
			live[recipient.identity.Normalize()] = struct{}{}
		}
	}
	for _, route := range requested {
		if _, ok := live[route.AgentIdentity.Normalize()]; ok {
			status.Deliverable = append(status.Deliverable, route)
		} else {
			status.Missing = append(status.Missing, route)
		}
	}
	return status, nil
}

// CheckPublishRecipientPlan applies the same subscribed-publish recipient
// policy used by Publish, but does not persist event, delivery, replay, or
// diagnostic evidence. Public ingress owners use this to fail closed before
// claiming successful publication.
func (eb *EventBus) CheckPublishRecipientPlan(ctx context.Context, evt events.Event) (PublishRecipientPlan, error) {
	if eb == nil {
		return PublishRecipientPlan{}, nil
	}
	if evt.Type() == "" {
		return PublishRecipientPlan{}, errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return PublishRecipientPlan{}, fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return PublishRecipientPlan{}, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ictx, admitted, err := admitEventForPublish(ctx, evt, time.Now())
	if err != nil {
		return PublishRecipientPlan{}, err
	}
	evt = admitted.Event()
	plan, err := eb.planSubscribedRoutePlan(withTemplateInstanceLifecyclePreview(ictx), evt, false)
	if err != nil {
		return PublishRecipientPlan{}, err
	}
	return eb.publishRecipientPlan(evt, plan), nil
}

// RecoverPersistedPipeline replays the complete pipeline for an event whose
// terminal pipeline receipt was never written.
func (eb *EventBus) RecoverPersistedPipeline(ctx context.Context, work runtimepipelineobligation.ClaimedWork, recipients []string) (runtimepipelineobligation.ExecutionOutcome, error) {
	dispatchRecipients := true
	if owner := eb.DeliveryContinuationOwner(); owner != nil && owner.OwnsPersistedRecovery() {
		dispatchRecipients = false
	}
	return eb.publishClaimedPipeline(ctx, work.Event, work.Scope, recipients, dispatchRecipients)
}

func (eb *EventBus) publishClaimedPipeline(ctx context.Context, evt events.Event, scope runtimepipelineobligation.CommittedScope, recipients []string, dispatchRecipients bool) (runtimepipelineobligation.ExecutionOutcome, error) {
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	if err := eb.clearPendingOutboxOperation(ctx, evt.ID()); err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	recipients = uniqueStrings(recipients)
	if evt.Type() == "" {
		return runtimepipelineobligation.Continue(), errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return runtimepipelineobligation.Continue(), fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	admitted, err := events.RevalidatePersistedEvent(evt)
	if err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	evt = admitted.Event()
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return runtimepipelineobligation.Continue(), err
	} else if reason != "" {
		if reason == dispatchQueueRuntimeIngress {
			return runtimepipelineobligation.Continue(), ErrRuntimeIngressPaused
		}
		return runtimepipelineobligation.Continue(), ErrRunDispatchBlocked
	}
	return eb.publishPersistedRecipientsWithScope(ctx, evt, scope, recipients, true, dispatchRecipients)
}

func (eb *EventBus) publishPersistedRecipientsWithScope(ctx context.Context, evt events.Event, scope runtimepipelineobligation.CommittedScope, recipients []string, replayInterceptors, dispatchRecipients bool) (result runtimepipelineobligation.ExecutionOutcome, err error) {
	if _, err := runtimepipelineobligation.ParseCommittedScope(string(scope)); err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	liveRecipients, internalRecipients, deliveryRoutes, err := eb.replayRecipientsForCommittedEvent(ctx, evt, recipients, scope)
	if err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	projection, err := eb.receiverProjection(ctx, evt.DeliveryContext())
	if err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	receiverCtx, closeReceiver, err := eb.beginReceiverDispatch(projection, evt)
	if err != nil {
		return runtimepipelineobligation.Continue(), err
	}
	defer func() { err = errors.Join(err, closeReceiver()) }()
	ctx = receiverCtx.Context
	passthrough := true
	deferred := []events.Event(nil)
	if replayInterceptors && scope == runtimepipelineobligation.ScopeSubscribed {
		var outcome runtimepipelineobligation.ExecutionOutcome
		if dispatchRecipients {
			passthrough, deferred, outcome, err = eb.runInterceptorsForDeliveryRoutes(ctx, evt, deliveryRoutes)
		} else {
			eventInterceptors, _ := splitDeliveryRouteInterceptors(eb.interceptorsSnapshot())
			passthrough, deferred, outcome, err = eb.runInterceptorSet(ctx, evt, eventInterceptors)
		}
		if err != nil {
			return runtimepipelineobligation.Continue(), err
		}
		if !outcome.ContinueDispatch() {
			return outcome, nil
		}
	}
	if dispatchRecipients && passthrough && len(liveRecipients) > 0 {
		if err := eb.deliverToRecipientsWithRoutes(ctx, evt, liveRecipients, deliveryRoutes); err != nil {
			return runtimepipelineobligation.Continue(), err
		}
	}
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return runtimepipelineobligation.Continue(), err
		}
	}
	if !dispatchRecipients {
		return runtimepipelineobligation.Continue(), nil
	}
	if !passthrough || len(liveRecipients) == 0 {
		return runtimepipelineobligation.Continue(), nil
	}
	owner := "event_deliveries"
	if scope == runtimepipelineobligation.ScopeSubscribed {
		owner = "event_deliveries+committed_replay_scope"
	}
	eb.logRuntime(ctx, "debug", "Persisted event was delivered to authoritative recipients", "eventbus", "delivered", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, map[string]any{
		"direct":                     scope == runtimepipelineobligation.ScopeDirect,
		"delivery_manifest_owner":    owner,
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            strings.TrimSpace(evt.ParentEventID()),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
		"replay_scope":               string(scope),
	}, nil, 0)
	return runtimepipelineobligation.Continue(), nil
}

func (eb *EventBus) deliveryTargetsForEvent(ctx context.Context, eventID string) map[string]events.RouteIdentity {
	reader := eb.durable.DeliveryTargets
	if reader == nil {
		return nil
	}
	targets, err := reader.ListEventDeliveryTargets(ctx, eventID)
	if err != nil {
		return nil
	}
	return targets
}

func (eb *EventBus) deliveryRoutesForEvent(ctx context.Context, eventID string) []events.DeliveryRoute {
	reader := eb.durable.DeliveryRouteSets
	if reader != nil {
		routes, err := reader.ListEventDeliveryRoutes(ctx, eventID)
		if err == nil && len(routes) > 0 {
			return events.NormalizeDeliveryRoutes(routes)
		}
	}
	return nil
}

func (eb *EventBus) recordTargetDeliveryFailure(ctx context.Context, evt events.Event, plan RoutePlan) {
	failure := plan.TargetFailure
	if failure.Empty() {
		return
	}
	_, detail, record := targetDeliveryFailureRecord(evt, plan, failure, time.Now().UTC())
	eb.logRuntime(ctx, "warn", "Pin routing target delivery failed", "eventbus", "target_resolution_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, &record.Failure, 0)

	recorder := eb.durable.TargetFailureRecorder
	if recorder == nil {
		return
	}
	if err := recorder.RecordDeadLetter(ctx, record); err != nil {
		eb.logRuntime(ctx, "warn", "Pin routing target failure dead-letter record failed", "eventbus", "target_resolution_failed_dead_letter_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, eventBusDependencyFailure(err, "target_failure_dead_letter_persist_failed", "record_target_failure"), 0)
	}
}

func targetDeliveryFailureRecord(evt events.Event, plan RoutePlan, failure runtimepinrouting.TargetFailure, occurredAt time.Time) (string, map[string]any, runtimedeadletters.Record) {
	plan = plan.Normalized()
	target := evt.TargetRoute()
	targetSet := evt.TargetRoutes()
	detail := map[string]any{
		"target_detail_code":   failure.Code(),
		"source":               evt.SourceRoute(),
		"target":               target,
		"target_set":           targetSet,
		"recipients":           plan.RecipientIDs(),
		"persisted_recipients": plan.PersistedRecipientIDs(),
		"delivery_targets":     cloneRouteTargetMap(plan.DeliveryTargets()),
		"delivery_routes":      plan.DeliveryRoutes(),
	}
	canonical := canonicalTargetDeliveryFailure(failure, detail)
	detail["failure"] = canonical.Failure
	deadLetterRoute := target
	if deadLetterRoute.Empty() && len(targetSet) > 0 {
		deadLetterRoute = targetSet[0]
	}
	if deadLetterRoute.Empty() {
		deadLetterRoute = evt.SourceRoute()
	}
	return canonical.Failure.Message, detail, runtimedeadletters.Record{
		OriginalEventID: strings.TrimSpace(evt.ID()),
		OriginalEvent:   strings.TrimSpace(string(evt.Type())),
		OriginalPayload: evt.Payload(),
		EntityID:        firstNonEmptyString(deadLetterRoute.EntityID, evt.EntityID()),
		FlowInstance:    firstNonEmptyString(deadLetterRoute.FlowInstance, evt.FlowInstance(), "runtime"),
		Failure:         canonical.Failure,
		RetryCount:      0,
		ChainDepth:      evt.ChainDepth(),
		HandlerNode:     "pin_routing",
		Timestamp:       occurredAt.UTC().Format(time.RFC3339Nano),
	}
}

func canonicalTargetDeliveryFailure(failure runtimepinrouting.TargetFailure, detail map[string]any) *runtimefailures.Error {
	var err error
	switch failure {
	case runtimepinrouting.FailureStaleArrival:
		err = runtimefailures.New(runtimefailures.ClassStaleArrival, "stale_arrival", "eventbus", "resolve_delivery_target", detail)
	case runtimepinrouting.FailureReplyAlreadyTerminal:
		err = runtimefailures.New(runtimefailures.ClassReplyAlreadyTerminal, "reply_already_terminal", "eventbus", "resolve_delivery_target", detail)
	default:
		err = runtimefailures.NewTarget(failure.Code(), "eventbus", "resolve_delivery_target", detail)
	}
	return runtimefailures.FromError(err, "eventbus", "resolve_delivery_target")
}

func cloneRouteTargetMap(in map[string]events.RouteIdentity) map[string]events.RouteIdentity {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]events.RouteIdentity, len(in))
	for recipient, target := range in {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			continue
		}
		out[recipient] = target.Normalized()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
