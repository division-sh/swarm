package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type pipelineTransitionSchemaCapabilityProvider interface {
	CanonicalEventReceiptsCapability(context.Context) (bool, error)
}

type targetFailureDeadLetterRecorder interface {
	RecordDeadLetter(context.Context, runtimedeadletters.Record) error
}

func shouldPersistPipelineReceipt(persisted bool, publishErr error) bool {
	if !persisted {
		return false
	}
	return !errors.Is(publishErr, errAuthoritativeDeliveryIncomplete) && !runtimepipeline.IsPipelineReceiptDeferred(publishErr)
}

func pipelineReceiptStatus(ctx context.Context, publishErr error) (string, *runtimefailures.Envelope) {
	if publishErr != nil {
		failure := runtimefailures.Normalize(publishErr, "eventbus", "publish")
		return "error", &failure
	}
	if overrideStatus, overrideFailure, ok := runtimepipeline.PipelineReceiptOverrideFromContext(ctx); ok {
		return overrideStatus, overrideFailure
	}
	return "processed", nil
}

func applyTargetDeliveryFailureReceipt(override *runtimepipeline.PipelineReceiptOverride, failure runtimepinrouting.TargetFailure) {
	if override == nil || failure == "" {
		return
	}
	override.Status = "dead_letter"
	override.Failure = targetDeliveryFailureEnvelope(failure)
}

func targetDeliveryFailureEnvelope(failure runtimepinrouting.TargetFailure) *runtimefailures.Envelope {
	failure = runtimepinrouting.TargetFailure(strings.TrimSpace(string(failure)))
	if failure == "" {
		return nil
	}
	canonical := runtimefailures.Normalize(runtimefailures.NewTarget(string(failure), "eventbus", "resolve_delivery_target", nil), "eventbus", "resolve_delivery_target")
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
	if strings.TrimSpace(evt.SourceAgent()) != "runtime" {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(string(evt.Type())), "platform.")
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

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	start := time.Now()
	if evt.Type() == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ictx, evt, err := admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return err
	}
	eb.beginEventPublish(evt.ID())
	defer eb.endEventPublish(evt.ID())
	publicationClaim, err := eb.claimPipelinePublication(ictx, evt.ID())
	if err != nil {
		return err
	}
	ictx = publicationClaim.BindContext(ictx)
	defer publicationClaim.Release(ictx)
	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]func(), 0, 8)
	afterPublishActions := make([]func(), 0, 4)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ictx = runtimepipeline.WithPipelineTransitionCollector(ictx, &deferredTransitions, eb.pipelineTransitionCapability())
	ictx = runtimepipeline.WithPipelinePostCommitActions(ictx, &postCommitActions)
	ictx = runtimepipeline.WithPipelineAfterPublishActions(ictx, &afterPublishActions)
	ictx = runtimepipeline.WithPipelineReceiptOverride(ictx, receiptOverride)
	defer func() {
		runtimepipeline.FlushPipelineAfterPublishActions(afterPublishActions)
	}()
	if mutationRunner, ok := eb.store.(EventMutationRunner); ok && mutationRunner != nil {
		outcome, err := eb.publishTransactional(ictx, evt, start, &deferredTransitions, mutationRunner)
		if err != nil {
			return err
		}
		if outcome == EventAppendExactDuplicate {
			return nil
		}
		eb.recordCommittedPublishConvergence(ictx, evt)
		return nil
	}
	if _, ok := eb.store.(TransactionalEventStore); ok {
		return errors.New("typed event mutation runner is required for transactional publish")
	}

	persisted := false
	queued := false
	passthrough := true
	deferred := []events.Event{}
	defer func() {
		if queued {
			return
		}
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, failure := pipelineReceiptStatus(ictx, err)
		eb.markPipelineReceipt(ictx, evt.ID(), status, failure)
	}()

	plan, err := eb.planSubscribedPublish(ictx, evt)
	if err != nil {
		return err
	}
	appendOutcome, err := eb.persistSubscribedPublishPlan(ictx, evt, plan)
	if err != nil {
		return err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return nil
	}
	persisted = true
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ictx, evt, plan)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	if reason, err := eb.dispatchQueueReason(ictx, evt); err != nil {
		return err
	} else if reason != "" {
		queued = true
		eb.logDispatchQueued(ictx, reason, evt, len(plan.RecipientIDs()), false, false)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	if pass, out, ierr := eb.runInterceptorsForDeliveryRoutes(ictx, evt, plan.DeliveryRoutes()); ierr != nil {
		return ierr
	} else {
		passthrough = pass
		deferred = out
	}
	if passthrough {
		var deliverQueued bool
		if deliverQueued, err = eb.deliverSubscribedPublishPlan(ictx, evt, plan); err != nil {
			return err
		}
		queued = deliverQueued
	}
	if !passthrough {
		persisted = true
	}
	eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(ictx, deferredTransitions)
	for _, d := range deferred {
		if err := eb.publishDeferred(ictx, d); err != nil {
			return err
		}
	}
	return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
}

// PublishAcknowledged persists the event, recipient manifest, and replay scope
// before returning, then dispatches post-commit pipeline work asynchronously.
// Public API surfaces use this when success means durable acceptance rather than
// downstream handler completion.
func (eb *EventBus) PublishAcknowledged(ctx context.Context, evt events.Event) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	start := time.Now()
	if evt.Type() == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ictx, evt, err := admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return err
	}
	eb.beginEventPublish(evt.ID())
	defer eb.endEventPublish(evt.ID())
	publicationClaim, err := eb.claimPipelinePublication(ictx, evt.ID())
	if err != nil {
		return err
	}
	ictx = publicationClaim.BindContext(ictx)
	claimTransferred := false
	defer func() {
		if !claimTransferred {
			publicationClaim.Release(ictx)
		}
	}()

	if mutationRunner, ok := eb.store.(EventMutationRunner); ok && mutationRunner != nil {
		transferred, _, err := eb.publishAcknowledgedTransactional(ictx, evt, start, mutationRunner, publicationClaim)
		claimTransferred = transferred
		return err
	}
	if _, ok := eb.store.(TransactionalEventStore); ok {
		return errors.New("typed event mutation runner is required for transactional acknowledged publish")
	}

	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ictx = runtimepipeline.WithPipelineReceiptOverride(ictx, receiptOverride)
	plan, err := eb.planSubscribedPublish(ictx, evt)
	if err != nil {
		return err
	}
	appendOutcome, err := eb.persistSubscribedPublishPlan(ictx, evt, plan)
	if err != nil {
		return err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return nil
	}
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ictx, evt, plan)
		eb.recordCommittedPublishReceipt(ictx, evt, nil)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	if reason, err := eb.dispatchQueueReason(ictx, evt); err != nil {
		return err
	} else if reason != "" {
		eb.logDispatchQueued(ictx, reason, evt, len(plan.RecipientIDs()), false, false)
		eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
		return eb.convergeStandaloneRuntimePlatformRun(ictx, evt)
	}
	eb.dispatchCommittedPublishAsync(ictx, evt, plan, publicationClaim)
	claimTransferred = true
	return nil
}

// PreparedPublish is the transaction-local result of canonical route planning.
// Its route plan remains EventBus-owned; callers may persist the exported
// delivery-route manifest but cannot reinterpret or replace the plan.
type PreparedPublish struct {
	Event            events.Event
	plan             RoutePlan
	exactDuplicate   bool
	targetFailure    bool
	dispatchQueued   bool
	queueReason      string
	direct           bool
	publicationClaim *pipelinePublicationClaim
	dispatchContext  context.Context
}

func appendEventMutationOutcome(ctx context.Context, mutation EventMutation, evt events.Event) (EventAppendOutcome, error) {
	outcome, err := mutation.AppendEventOutcome(ctx, evt)
	if err != nil {
		return EventAppendOutcomeUnknown, err
	}
	if err := validateEventAppendOutcome(outcome); err != nil {
		return EventAppendOutcomeUnknown, fmt.Errorf("selected-store event mutation: %w", err)
	}
	return outcome, nil
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

// PreparePublishInMutation persists the event, performs real stateful route
// materialization, and persists the canonical delivery/replay facts through the
// active typed mutation. Dispatch is deliberately separate and may happen only
// after the selected-store transaction commits.
func (eb *EventBus) PreparePublishInMutation(ctx context.Context, evt events.Event) (PreparedPublish, error) {
	return eb.preparePublishInMutation(ctx, evt, runtimereplayclaim.CommittedReplayScopeSubscribed, func(ctx context.Context, evt events.Event) (RoutePlan, error) {
		return eb.planSubscribedRoutePlan(ctx, evt, true)
	})
}

func (eb *EventBus) preparePublishInMutation(ctx context.Context, evt events.Event, replayScope runtimereplayclaim.CommittedReplayScope, planRoutes func(context.Context, events.Event) (RoutePlan, error)) (PreparedPublish, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return PreparedPublish{}, err
	}
	ctx = eb.withBundleFingerprint(ctx)
	if evt.Type() == "" {
		return PreparedPublish{}, errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return PreparedPublish{}, fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return PreparedPublish{}, fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ictx, evt, err := admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return PreparedPublish{}, err
	}
	mutation, ok := eb.eventMutationFromContext(ictx)
	if !ok || mutation == nil {
		return PreparedPublish{}, errors.New("typed event mutation context is required")
	}
	txctx := WithEventMutationContext(ictx, mutation)
	publicationClaim, err := eb.claimPipelinePublication(txctx, evt.ID())
	if err != nil {
		return PreparedPublish{}, err
	}
	txctx = publicationClaim.BindContext(txctx)
	if publicationClaim != nil && !runtimepipeline.QueuePipelineRollbackAction(txctx, func() { publicationClaim.Release(txctx) }) {
		publicationClaim.Release(txctx)
		return PreparedPublish{}, errors.New("event mutation rollback actions are required for pipeline publication claim")
	}
	txctx, err = eb.withTransactionRouteOverlay(txctx)
	if err != nil {
		return PreparedPublish{}, err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	txctx = runtimepipeline.WithPipelineReceiptOverride(txctx, receiptOverride)
	appendOutcome, err := appendEventMutationOutcome(txctx, mutation, evt)
	if err != nil {
		return PreparedPublish{}, fmt.Errorf("persist event: %w", err)
	}
	prepared := PreparedPublish{
		Event:            evt,
		direct:           replayScope == runtimereplayclaim.CommittedReplayScopeDirect,
		publicationClaim: publicationClaim,
		dispatchContext:  txctx,
	}
	if appendOutcome == EventAppendExactDuplicate {
		prepared.exactDuplicate = true
		return prepared, nil
	}
	inboundPlan, err := planRoutes(txctx, evt)
	if err != nil {
		return PreparedPublish{}, err
	}
	if inboundPlan.HasPersistentDeliveries() {
		if err := eb.insertEventDeliveriesMutation(txctx, mutation, evt.ID(), inboundPlan.PersistedRecipientIDs(), inboundPlan.DeliveryTargets(), inboundPlan.DeliveryRoutes()); err != nil {
			return PreparedPublish{}, fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if eb.testLifecycleProbe != nil {
		runtimepipeline.QueuePipelinePostCommitAction(txctx, func() {
			eb.notifyTestPublishPersisted(context.WithoutCancel(txctx), evt, inboundPlan)
		})
	}
	if err := eb.upsertCommittedReplayScopeMutation(txctx, mutation, evt.ID(), replayScope); err != nil {
		return PreparedPublish{}, err
	}
	prepared.plan = inboundPlan
	if inboundPlan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, inboundPlan.TargetFailure)
		status, failure := pipelineReceiptStatus(txctx, nil)
		if err := mutation.UpsertPipelineReceipt(txctx, evt.ID(), status, failure); err != nil {
			return PreparedPublish{}, fmt.Errorf("persist pipeline receipt: %w", err)
		}
		if err := eb.recordTargetDeliveryFailureMutation(txctx, mutation, evt, inboundPlan); err != nil {
			return PreparedPublish{}, err
		}
		prepared.targetFailure = true
		return prepared, nil
	}
	if reason, err := eb.dispatchQueueReason(txctx, evt); err != nil {
		return PreparedPublish{}, err
	} else if reason != "" {
		prepared.dispatchQueued = true
		prepared.queueReason = reason
	}
	return prepared, nil
}

// PublishInMutation preserves the general producer surface by preparing inside
// the active mutation and queueing dispatch after commit.
func (eb *EventBus) PublishInMutation(ctx context.Context, evt events.Event) error {
	prepared, err := eb.PreparePublishInMutation(ctx, evt)
	if err != nil {
		return err
	}
	return eb.queuePreparedPublishInMutation(ctx, prepared)
}

// PublishDirectInMutation is the transactional counterpart of PublishDirect.
// It persists the exact direct-recipient manifest in the caller's active typed
// mutation so payload fields can never become delivery authority.
func (eb *EventBus) PublishDirectInMutation(ctx context.Context, evt events.Event, recipients []string) error {
	requested := uniqueStrings(recipients)
	if len(requested) == 0 {
		return errors.New("direct event publication requires at least one recipient")
	}
	prepared, err := eb.preparePublishInMutation(ctx, evt, runtimereplayclaim.CommittedReplayScopeDirect, func(ctx context.Context, evt events.Event) (RoutePlan, error) {
		plan, err := eb.planDirectRoutePlan(ctx, evt, requested)
		if err != nil {
			return RoutePlan{}, err
		}
		if filtered := filteredRecipients(requested, plan.RecipientIDs()); len(filtered) > 0 {
			return RoutePlan{}, fmt.Errorf("transactional direct delivery rejected recipients: %s", strings.Join(filtered, ", "))
		}
		return plan, nil
	})
	if err != nil {
		return err
	}
	return eb.queuePreparedPublishInMutation(ctx, prepared)
}

func (eb *EventBus) queuePreparedPublishInMutation(ctx context.Context, prepared PreparedPublish) error {
	mutation, ok := eb.eventMutationFromContext(ctx)
	if !ok || mutation == nil {
		return errors.New("typed event mutation context is required")
	}
	txctx := mutation.Context()
	if !runtimepipeline.QueuePipelinePostCommitAction(txctx, func() {
		dispatchCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(txctx))
		_ = eb.DispatchPreparedPublish(dispatchCtx, prepared)
	}) {
		return errors.New("event mutation post-commit actions are required")
	}
	return nil
}

// DispatchPreparedPublish consumes only the plan finalized by
// PreparePublishInMutation. It never invokes route planning again.
func (eb *EventBus) DispatchPreparedPublish(ctx context.Context, prepared PreparedPublish) error {
	if strings.TrimSpace(prepared.Event.ID()) == "" {
		return errors.New("prepared event is required")
	}
	if prepared.dispatchContext != nil {
		ctx = WithoutEventMutationContext(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(prepared.dispatchContext)))
	}
	defer prepared.publicationClaim.Release(ctx)
	if prepared.exactDuplicate {
		return nil
	}
	if prepared.targetFailure {
		eb.logPublished(ctx, prepared.Event, 0)
		eb.recordCommittedPublishConvergence(ctx, prepared.Event)
		return nil
	}
	if prepared.dispatchQueued {
		eb.logDispatchQueued(ctx, prepared.queueReason, prepared.Event, len(prepared.RecipientIDs()), prepared.direct, true)
		eb.logPublished(ctx, prepared.Event, 0)
		eb.recordCommittedPublishConvergence(ctx, prepared.Event)
		return nil
	}
	return eb.completeCommittedPublishDispatch(ctx, prepared.Event, prepared.plan)
}

func (eb *EventBus) DispatchPreparedPublishAsync(ctx context.Context, prepared PreparedPublish) {
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
	eb.beginEventPublish(prepared.Event.ID())
	go func() {
		defer eb.endEventPublish(prepared.Event.ID())
		_ = eb.DispatchPreparedPublish(dispatchCtx, prepared)
	}()
}

func (eb *EventBus) dispatchCommittedPublishAsync(ctx context.Context, evt events.Event, inboundPlan RoutePlan, publicationClaim *pipelinePublicationClaim) {
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
	eb.beginEventPublish(evt.ID())
	go func() {
		defer eb.endEventPublish(evt.ID())
		defer publicationClaim.Release(dispatchCtx)
		_ = eb.completeCommittedPublishDispatch(dispatchCtx, evt, inboundPlan)
	}()
}

func (eb *EventBus) completeCommittedPublishDispatch(ctx context.Context, evt events.Event, inboundPlan RoutePlan) error {
	ctx = WithoutEventMutationContext(ctx)
	eb.notifyTestPostCommitDispatchStarted(ctx, evt)
	defer eb.notifyTestPostCommitDispatchCompleted(ctx, evt)

	inboundPlan = inboundPlan.Normalized()
	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]func(), 0, 8)
	afterPublishActions := make([]func(), 0, 4)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions, eb.pipelineTransitionCapability())
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
	ctx = runtimepipeline.WithPipelineAfterPublishActions(ctx, &afterPublishActions)
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	defer runtimepipeline.FlushPipelineAfterPublishActions(afterPublishActions)

	passthrough, deferred, err := eb.runInterceptorsForDeliveryRoutes(ctx, evt, inboundPlan.DeliveryRoutes())
	if err != nil {
		eb.recordCommittedPublishReceipt(ctx, evt, err)
		return err
	}

	if passthrough {
		recipients := inboundPlan.RecipientIDs()
		if len(recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipientIDs(), "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverRoutePlanWithRoutes(ctx, evt, inboundPlan); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return err
			}
			eb.logDelivery(ctx, evt, recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *inboundPlan.CycleEscalation); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return err
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, 0)
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(ctx, deferredTransitions)

	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			eb.recordCommittedPublishReceipt(ctx, evt, err)
			return err
		}
	}
	eb.recordCommittedPublishReceipt(ctx, evt, nil)
	eb.recordCommittedPublishConvergence(ctx, evt)
	return nil
}

func (eb *EventBus) runInterceptorsForDeliveryRoutes(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute) (bool, []events.Event, error) {
	interceptors := eb.interceptorsSnapshot()
	nodeRoutes := nodeDeliveryRoutes(deliveryRoutes)
	if len(nodeRoutes) == 0 {
		return eb.runInterceptorSet(ctx, evt, interceptors)
	}
	eventInterceptors, routeInterceptors := splitDeliveryRouteInterceptors(interceptors)
	passthrough, deferred, err := eb.runInterceptorSet(ctx, evt, eventInterceptors)
	if err != nil {
		return passthrough, nil, err
	}
	routePassthrough, routeDeferred, err := eb.runNodeDeliveryRouteInterceptors(ctx, evt, nodeRoutes, routeInterceptors)
	if err != nil {
		return passthrough, nil, err
	}
	if len(routeDeferred) > 0 {
		deferred = append(deferred, routeDeferred...)
	}
	return passthrough && routePassthrough, deferred, nil
}

func nodeDeliveryRoutes(deliveryRoutes []events.DeliveryRoute) []events.DeliveryRoute {
	routes := make([]events.DeliveryRoute, 0)
	for _, route := range events.NormalizeDeliveryRoutes(deliveryRoutes) {
		if strings.TrimSpace(route.SubscriberType) != "node" {
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

func (eb *EventBus) runNodeDeliveryRouteInterceptors(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, interceptors []DeliveryRouteInterceptor) (bool, []events.Event, error) {
	if err := events.ValidateDeliveryRouteProjections(deliveryRoutes); err != nil {
		return true, nil, err
	}
	deliveryRoutes = nodeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 || len(interceptors) == 0 {
		return true, nil, nil
	}
	passthrough := true
	deferred := make([]events.Event, 0)
	seen := map[string]struct{}{}
	for _, route := range deliveryRoutes {
		target := route.Target.Normalized()
		key := strings.Join([]string{
			route.SubscriberID,
			target.FlowID,
			target.FlowInstance,
			target.EntityID,
			route.Context.ReplyContextID(),
			route.PayloadProjection.Fingerprint(),
		}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		projected, err := projectEventForDeliveryRoute(evt, route)
		if err != nil {
			return passthrough, nil, err
		}
		for _, it := range interceptors {
			pass, out, err := it.InterceptDeliveryRoute(ctx, projected, route)
			if err != nil {
				return passthrough, nil, err
			}
			if !pass {
				passthrough = false
			}
			admitted, err := admitDeferredEvents(ctx, out)
			if err != nil {
				return passthrough, nil, err
			}
			deferred = append(deferred, admitted...)
		}
	}
	return passthrough, deferred, nil
}

func projectEventForDeliveryRoute(evt events.Event, route events.DeliveryRoute) (events.Event, error) {
	payload := evt.Payload()
	projection, err := route.PayloadProjection.Canonical()
	if err != nil {
		return events.EmptyEvent(), fmt.Errorf("delivery route for %s has invalid synthetic payload projection: %w", strings.TrimSpace(route.SubscriberID), err)
	}
	if !projection.Empty() {
		var fields map[string]any
		if err := canonicaljson.DecodeInto(payload, &fields); err != nil {
			return events.EmptyEvent(), fmt.Errorf("delivery route for %s cannot project synthetic fields into a non-object payload: %w", strings.TrimSpace(route.SubscriberID), err)
		}
		if fields == nil {
			return events.EmptyEvent(), fmt.Errorf("delivery route for %s cannot project synthetic fields into a non-object payload", strings.TrimSpace(route.SubscriberID))
		}
		for field, value := range projection.Fields() {
			if _, exists := fields[field]; exists {
				return events.EmptyEvent(), fmt.Errorf("delivery route for %s rejects payload field %q: the producer payload and receiver-owned synthetic carry both declare it; remove or rename the producer field, or choose a different carry 'as' field", strings.TrimSpace(route.SubscriberID), field)
			}
			fields[field] = value
		}
		payload, err = canonicaljson.Bytes(fields)
		if err != nil {
			return events.EmptyEvent(), fmt.Errorf("encode synthetic delivery payload for %s: %w", strings.TrimSpace(route.SubscriberID), err)
		}
	}
	envelope := evt.NormalizedEnvelope()
	target := route.Target.Normalized()
	if target.Empty() && strings.TrimSpace(route.SubscriberType) == "node" {
		envelope.Target = events.RouteIdentity{}
		envelope.TargetSet = nil
		envelope = envelope.Normalized()
	} else if !target.Empty() {
		envelope = events.EnvelopeForTargetRoute(envelope, target)
	}
	return events.NewProjectionEvent(
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		payload,
		evt.ChainDepth(),
		evt.RunID(),
		evt.ParentEventID(),
		envelope,
		evt.CreatedAt(),
	).WithExecutionMode(evt.ExecutionMode()).WithDeliveryContext(route.Context), nil
}

func (eb *EventBus) withBundleFingerprint(ctx context.Context) context.Context {
	if ctx == nil || eb == nil {
		return ctx
	}
	if _, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		return ctx
	}
	eb.mu.RLock()
	sourceFact := eb.bundleSourceFact
	fingerprint := eb.bundleFingerprint
	eb.mu.RUnlock()
	if sourceFact.BundleFingerprint == "" {
		sourceFact.BundleFingerprint = fingerprint
	}
	if !sourceFact.Empty() {
		return runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	}
	if runtimecorrelation.BundleFingerprintFromContext(ctx) != "" {
		return ctx
	}
	return runtimecorrelation.WithBundleFingerprint(ctx, fingerprint)
}

func (eb *EventBus) WithBundleFingerprint(ctx context.Context) context.Context {
	return eb.withBundleFingerprint(ctx)
}

func (eb *EventBus) pipelineTransitionCapability() func(context.Context) (bool, error) {
	if eb == nil || eb.store == nil {
		return nil
	}
	if provider, ok := eb.store.(pipelineTransitionSchemaCapabilityProvider); ok && provider != nil {
		return provider.CanonicalEventReceiptsCapability
	}
	return nil
}

func (eb *EventBus) convergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	if converger, ok := eb.store.(StandaloneRuntimePlatformRunConvergencePersistence); ok && converger != nil {
		if err := converger.ConvergeStandaloneRuntimePlatformRun(ctx, evt); err != nil {
			return err
		}
	}
	return eb.ConvergeNormalRunCompletionForEvent(ctx, evt.ID())
}

func (eb *EventBus) publishTransactional(
	ctx context.Context,
	evt events.Event,
	start time.Time,
	deferredTransitions *[]runtimepipeline.DeferredPipelineTransition,
	runner EventMutationRunner,
) (EventAppendOutcome, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return EventAppendOutcomeUnknown, err
	}
	appendOutcome := EventAppendOutcomeUnknown
	var inboundPlan RoutePlan
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	targetFailure := false
	dispatchQueued := false
	queueReason := ""
	if err := runner.RunEventMutation(ctx, func(mutation EventMutation) error {
		txctx := runtimepipeline.WithPipelineReceiptOverride(mutation.Context(), receiptOverride)
		var err error
		txctx, err = eb.withTransactionRouteOverlay(txctx)
		if err != nil {
			return err
		}
		appendOutcome, err = appendEventMutationOutcome(txctx, mutation, evt)
		if err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if appendOutcome == EventAppendExactDuplicate {
			return nil
		}
		plan, err := eb.planSubscribedRoutePlan(txctx, evt, true)
		if err != nil {
			return err
		}
		inboundPlan = plan
		if inboundPlan.HasPersistentDeliveries() {
			if err := eb.insertEventDeliveriesMutation(txctx, mutation, evt.ID(), inboundPlan.PersistedRecipientIDs(), inboundPlan.DeliveryTargets(), inboundPlan.DeliveryRoutes()); err != nil {
				return fmt.Errorf("persist event deliveries: %w", err)
			}
		}
		if err := eb.upsertCommittedReplayScopeMutation(txctx, mutation, evt.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return err
		}
		if inboundPlan.TargetFailure != "" {
			applyTargetDeliveryFailureReceipt(receiptOverride, inboundPlan.TargetFailure)
			status, failure := pipelineReceiptStatus(txctx, nil)
			if err := mutation.UpsertPipelineReceipt(txctx, evt.ID(), status, failure); err != nil {
				return fmt.Errorf("persist pipeline receipt: %w", err)
			}
			if err := eb.recordTargetDeliveryFailureMutation(txctx, mutation, evt, inboundPlan); err != nil {
				return err
			}
			targetFailure = true
			return nil
		}
		if reason, err := eb.dispatchQueueReason(txctx, evt); err != nil {
			return err
		} else if reason != "" {
			dispatchQueued = true
			queueReason = reason
			return nil
		}
		return nil
	}); err != nil {
		return EventAppendOutcomeUnknown, err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return appendOutcome, nil
	}
	eb.notifyTestPublishPersisted(ctx, evt, inboundPlan)
	if targetFailure {
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return appendOutcome, nil
	}
	if dispatchQueued {
		eb.logDispatchQueued(ctx, queueReason, evt, len(inboundPlan.RecipientIDs()), false, false)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return appendOutcome, nil
	}

	postCommitActions := make([]func(), 0, 8)
	dispatchCtx := runtimepipeline.WithoutPipelineSQLTxContext(ctx)
	dispatchCtx = runtimepipeline.WithPipelinePostCommitActions(dispatchCtx, &postCommitActions)
	dispatchCtx = runtimepipeline.WithPipelineReceiptOverride(dispatchCtx, receiptOverride)
	passthrough, deferred, err := eb.runInterceptorsForDeliveryRoutes(dispatchCtx, evt, inboundPlan.DeliveryRoutes())
	if err != nil {
		eb.recordCommittedPublishReceipt(dispatchCtx, evt, err)
		return EventAppendOutcomeUnknown, err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	if deferredTransitions != nil {
		runtimepipeline.FlushDeferredPipelineTransitions(dispatchCtx, *deferredTransitions)
	}

	if passthrough {
		recipients := inboundPlan.RecipientIDs()
		if len(recipients) > 0 {
			eb.logQueuedDeliveries(dispatchCtx, evt, inboundPlan.PersistedRecipientIDs(), "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverRoutePlanWithRoutes(dispatchCtx, evt, inboundPlan); err != nil {
				eb.recordCommittedPublishReceipt(dispatchCtx, evt, err)
				return appendOutcome, nil
			}
			eb.logDelivery(dispatchCtx, evt, recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(dispatchCtx, *inboundPlan.CycleEscalation); err != nil {
				eb.recordCommittedPublishReceipt(dispatchCtx, evt, err)
				return appendOutcome, nil
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(dispatchCtx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(dispatchCtx, evt, int(time.Since(start)/time.Microsecond))

	for _, d := range deferred {
		if err := eb.publishDeferred(dispatchCtx, d); err != nil {
			eb.recordCommittedPublishReceipt(dispatchCtx, evt, err)
			return appendOutcome, nil
		}
	}
	eb.recordCommittedPublishReceipt(dispatchCtx, evt, nil)
	return appendOutcome, nil
}

func (eb *EventBus) publishAcknowledgedTransactional(
	ctx context.Context,
	evt events.Event,
	start time.Time,
	runner EventMutationRunner,
	publicationClaim *pipelinePublicationClaim,
) (bool, EventAppendOutcome, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return false, EventAppendOutcomeUnknown, err
	}
	appendOutcome := EventAppendOutcomeUnknown
	var inboundPlan RoutePlan
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	targetFailure := false
	dispatchQueued := false
	queueReason := ""
	if err := runner.RunEventMutation(ctx, func(mutation EventMutation) error {
		txctx := runtimepipeline.WithPipelineReceiptOverride(mutation.Context(), receiptOverride)
		var err error
		txctx, err = eb.withTransactionRouteOverlay(txctx)
		if err != nil {
			return err
		}
		appendOutcome, err = appendEventMutationOutcome(txctx, mutation, evt)
		if err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
		if appendOutcome == EventAppendExactDuplicate {
			return nil
		}
		plan, err := eb.planSubscribedRoutePlan(txctx, evt, true)
		if err != nil {
			return err
		}
		inboundPlan = plan
		if inboundPlan.HasPersistentDeliveries() {
			if err := eb.insertEventDeliveriesMutation(txctx, mutation, evt.ID(), inboundPlan.PersistedRecipientIDs(), inboundPlan.DeliveryTargets(), inboundPlan.DeliveryRoutes()); err != nil {
				return fmt.Errorf("persist event deliveries: %w", err)
			}
		}
		if err := eb.upsertCommittedReplayScopeMutation(txctx, mutation, evt.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return err
		}
		if inboundPlan.TargetFailure != "" {
			applyTargetDeliveryFailureReceipt(receiptOverride, inboundPlan.TargetFailure)
			status, failure := pipelineReceiptStatus(txctx, nil)
			if err := mutation.UpsertPipelineReceipt(txctx, evt.ID(), status, failure); err != nil {
				return fmt.Errorf("persist pipeline receipt: %w", err)
			}
			if err := eb.recordTargetDeliveryFailureMutation(txctx, mutation, evt, inboundPlan); err != nil {
				return err
			}
			targetFailure = true
			return nil
		}
		if reason, err := eb.dispatchQueueReason(txctx, evt); err != nil {
			return err
		} else if reason != "" {
			dispatchQueued = true
			queueReason = reason
			return nil
		}
		return nil
	}); err != nil {
		return false, EventAppendOutcomeUnknown, err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return false, appendOutcome, nil
	}
	eb.notifyTestPublishPersisted(ctx, evt, inboundPlan)
	if targetFailure {
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		eb.recordCommittedPublishConvergence(ctx, evt)
		return false, appendOutcome, nil
	}
	if dispatchQueued {
		eb.logDispatchQueued(ctx, queueReason, evt, len(inboundPlan.RecipientIDs()), false, true)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		eb.recordCommittedPublishConvergence(ctx, evt)
		return false, appendOutcome, nil
	}
	eb.dispatchCommittedPublishAsync(ctx, evt, inboundPlan, publicationClaim)
	return true, appendOutcome, nil
}

func (eb *EventBus) recordCommittedPublishReceipt(ctx context.Context, evt events.Event, publishErr error) {
	if runtimepipeline.IsPipelineReceiptDeferred(publishErr) {
		_ = eb.deferDecisionRouteObligation(ctx, evt.ID(), publishErr)
		return
	}
	if publishErr != nil && evt.Type() == events.EventType("mailbox.card_decided") {
		if err := eb.QuarantineRecoveredPipelineEvent(ctx, evt, publishErr); err != nil {
			failure := eventBusDependencyFailure(err, "decision_route_obligation_quarantine_failed", "quarantine_decision_route")
			eb.logRuntime(ctx, "error", "Quarantining the failed foreground decision route failed", "eventbus", "decision_route_obligation_quarantine_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, nil, failure, 0)
		}
		return
	}
	if publishErr == nil && evt.Type() == events.EventType("mailbox.card_decided") {
		if err := eb.SettleRecoveredPipelineEvent(ctx, evt); err != nil {
			_ = eb.deferDecisionRouteObligation(ctx, evt.ID(), err)
		}
		return
	}
	if !shouldPersistPipelineReceipt(true, publishErr) {
		return
	}
	status, failure := pipelineReceiptStatus(ctx, publishErr)
	if err := eb.markPipelineReceipt(ctx, evt.ID(), status, failure); err == nil && evt.Type() == events.EventType("mailbox.card_decided") {
		_ = eb.completeDecisionRouteObligation(ctx, evt.ID())
	}
}

func (eb *EventBus) recordCommittedPublishConvergence(ctx context.Context, evt events.Event) {
	// Decision-route convergence is part of its durable settlement state machine.
	// Sending a post-route failure through this generic path can overwrite a
	// processed receipt and quarantine a verdict that already executed.
	if evt.Type() == events.EventType("mailbox.card_decided") {
		return
	}
	if err := eb.convergeStandaloneRuntimePlatformRun(ctx, evt); err != nil {
		failureErr := runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "normal_run_completion_failed", "eventbus", "post_commit_convergence", map[string]any{
			"event_id": evt.ID(), "event_type": string(evt.Type()),
		}, err)
		eb.recordCommittedPublishReceipt(ctx, evt, failureErr)
		failure := runtimefailures.Normalize(failureErr, "eventbus", "post_commit_convergence")
		eb.logRuntime(ctx, "error", "Post-commit publish convergence failed", "eventbus", "publish_post_commit_convergence_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, map[string]any{
			"failure": failure,
		}, &failure, 0)
	}
}

func (eb *EventBus) runInterceptors(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
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

func (eb *EventBus) runInterceptorSet(ctx context.Context, evt events.Event, interceptors []EventInterceptor) (bool, []events.Event, error) {
	if len(interceptors) == 0 {
		return true, nil, nil
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	for _, it := range interceptors {
		pass, out, err := it.Intercept(ctx, evt)
		if err != nil {
			if runtimepipeline.IsPipelineReceiptDeferred(err) {
				return true, nil, err
			}
			return true, nil, runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "event_interceptor_failed", "eventbus", "run_interceptor", map[string]any{
				"event_id": evt.ID(), "event_type": string(evt.Type()),
			}, err)
		}
		if !pass {
			passthrough = false
		}
		admitted, err := admitDeferredEvents(ctx, out)
		if err != nil {
			return true, nil, err
		}
		deferred = append(deferred, admitted...)
	}
	return passthrough, deferred, nil
}

func admitDeferredEvents(ctx context.Context, out []events.Event) ([]events.Event, error) {
	if len(out) == 0 {
		return nil, nil
	}
	deferred := make([]events.Event, 0, len(out))
	for _, d := range out {
		_, admitted, err := admitEventForPublish(ctx, d, time.Now(), "")
		if err != nil {
			return nil, err
		}
		deferred = append(deferred, admitted)
	}
	return deferred, nil
}

func admitEventForPublish(ctx context.Context, evt events.Event, now time.Time, sourceAgentDefault string) (context.Context, events.Event, error) {
	opts := events.AdmissionOptions{
		Now:                      now,
		RunIDCandidate:           publishRunIDCandidate(ctx, evt),
		ParentEventIDCandidate:   publishParentEventIDCandidate(ctx, evt),
		SelectedForkLineageOwner: publishSelectedForkLineageOwner(ctx),
		SourceAgentDefault:       sourceAgentDefault,
	}
	admitted, err := events.AdmitForPublish(evt, opts)
	if err != nil {
		return ctx, events.EmptyEvent(), err
	}
	ctx = events.WithDeliveryContext(ctx, admitted.DeliveryContext())
	if runID := strings.TrimSpace(admitted.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	return ctx, admitted, nil
}

func publishRunIDCandidate(ctx context.Context, evt events.Event) string {
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		return runID
	}
	if runID := runtimecorrelation.RunIDFromContext(ctx); runID != "" {
		return runID
	}
	if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok {
		if runID := strings.TrimSpace(lineage.RunID); runID != "" {
			return runID
		}
	}
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		return strings.TrimSpace(inbound.RunID())
	}
	return ""
}

func publishParentEventIDCandidate(ctx context.Context, evt events.Event) string {
	if parentID := strings.TrimSpace(evt.ParentEventID()); parentID != "" {
		return parentID
	}
	if parentID := runtimecorrelation.RuntimeLineageParentForEvent(ctx, evt.ID()); parentID != "" {
		return parentID
	}
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		parentID := strings.TrimSpace(inbound.ID())
		if parentID != "" && parentID != strings.TrimSpace(evt.ID()) {
			return parentID
		}
	}
	return ""
}

func publishSelectedForkLineageOwner(ctx context.Context) string {
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	if !ok || !lineage.SelectedForkContext {
		return ""
	}
	if lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal {
		return ""
	}
	if lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer {
		return ""
	}
	return strings.TrimSpace(lineage.SelectedForkOwner)
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) (err error) {
	if evt.Type() == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type())))
	}
	ctx = WithoutEventMutationContext(runtimepipeline.WithoutPipelineSQLTxContext(ctx))
	ctx, evt, err = admitEventForPublish(ctx, evt, time.Now(), "runtime")
	if err != nil {
		return err
	}
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

func (eb *EventBus) publishSubscribedNonTransactional(ctx context.Context, evt events.Event) (bool, error) {
	plan, err := eb.planSubscribedPublish(ctx, evt)
	if err != nil {
		return false, err
	}
	appendOutcome, err := eb.persistSubscribedPublishPlan(ctx, evt, plan)
	if err != nil {
		return false, err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return false, nil
	}
	return eb.deliverSubscribedPublishPlan(ctx, evt, plan)
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
	return routePlan.Normalized(), nil
}

func (eb *EventBus) authorizePublishRecipientPlan(ctx context.Context, evt events.Event, routePlan RoutePlan) error {
	if eb == nil || eb.recipientPlanGuard == nil {
		return nil
	}
	return eb.recipientPlanGuard(ctx, evt, eb.publishRecipientPlan(evt, routePlan))
}

func (eb *EventBus) publishRecipientPlan(evt events.Event, routePlan RoutePlan) PublishRecipientPlan {
	routePlan = routePlan.Normalized()
	out := PublishRecipientPlan{
		Recipients:             routePlan.RecipientIDs(),
		PersistedRecipients:    routePlan.PersistedRecipientIDs(),
		SubscriptionRecipients: uniqueStrings(routePlan.SubscribedRecipients),
		DeliveryRoutes:         routePlan.DeliveryRoutes(),
		TargetFailure:          strings.TrimSpace(string(routePlan.TargetFailure)),
		canonicalAuthority:     routePlan.CanonicalRouteOwnerMatched() && routePlan.AuthorityOwner == routePlanSourceConnectRoutePlan,
	}
	if eb != nil {
		out.RoutedRecipients = eb.describeSubscribersForEvent(string(evt.Type()), routePlan.RoutedRecipients)
	}
	return out
}

func (eb *EventBus) persistSubscribedPublishPlan(ctx context.Context, evt events.Event, routePlan RoutePlan) (EventAppendOutcome, error) {
	routePlan = routePlan.Normalized()
	persistedRecipients := routePlan.PersistedRecipientIDs()
	appendOutcome, err := eb.persistEventRecord(ctx, evt, persistedRecipients, routePlan.DeliveryTargets(), routePlan.DeliveryRoutes(), runtimereplayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		return EventAppendOutcomeUnknown, err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return appendOutcome, nil
	}
	eb.notifyTestPublishPersisted(ctx, evt, routePlan)
	eb.logQueuedDeliveries(ctx, evt, persistedRecipients, "matched_agent_subscription", routePlan.ExtraDetail)
	return appendOutcome, nil
}

func (eb *EventBus) deliverSubscribedPublishPlan(ctx context.Context, evt events.Event, routePlan RoutePlan) (bool, error) {
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return false, err
	} else if reason != "" {
		eb.logDispatchQueued(ctx, reason, evt, len(routePlan.RecipientIDs()), false, false)
		return true, nil
	}
	routePlan = routePlan.Normalized()
	recipients := routePlan.RecipientIDs()
	if len(recipients) > 0 {
		if err := eb.deliverRoutePlanWithRoutes(ctx, evt, routePlan); err != nil {
			return false, err
		}
		eb.logDelivery(ctx, evt, recipients, routePlan.ExtraDetail)
	}
	if routePlan.BlockedByCycle && routePlan.CycleEscalation != nil {
		if err := eb.publishDeferred(ctx, *routePlan.CycleEscalation); err != nil {
			return false, err
		}
	}
	if strings.TrimSpace(routePlan.ContradictionReason) != "" {
		_ = eb.emitContradiction(ctx, evt, routePlan.ContradictionReason)
	}
	return false, nil
}

func (eb *EventBus) persistEventRecord(
	ctx context.Context,
	evt events.Event,
	recipients []string,
	deliveryTargets map[string]events.RouteIdentity,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) (EventAppendOutcome, error) {
	recipients = uniqueStrings(recipients)
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if atomicStore, ok := eb.store.(AtomicEventDeliveryRouteSetOutcomePersistence); ok && len(deliveryRoutes) > 0 {
		outcome, err := atomicStore.PersistEventWithDeliveryRouteSetAndScopeOutcome(ctx, evt, deliveryRoutes, scope)
		if err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if err := validateEventAppendOutcome(outcome); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return outcome, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventDeliveryRouteSetPersistence); ok && len(deliveryRoutes) > 0 {
		if err := atomicStore.PersistEventWithDeliveryRouteSetAndScope(ctx, evt, deliveryRoutes, scope); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return EventAppendInserted, nil
	}
	if deliveryRouteSetPersistenceRequired(deliveryRoutes) {
		if _, ok := eb.store.(EventDeliveryRouteSetPersistence); !ok {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist typed event delivery routes: %w", errMissingEventDeliveryRouteSetPersistence)
		}
		outcome, err := eb.appendEventRecordOutcome(ctx, evt)
		if err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event: %w", err)
		}
		if outcome == EventAppendExactDuplicate {
			return outcome, nil
		}
		if err := eb.insertEventDeliveries(ctx, evt.ID(), recipients, deliveryTargets, deliveryRoutes); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event deliveries: %w", err)
		}
		if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID(), scope); err != nil {
				return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", err)
			}
			return outcome, nil
		}
		if replayScopePersistenceRequired(eb.store) {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		return outcome, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventRouteOutcomePersistence); ok {
		outcome, err := atomicStore.PersistEventWithDeliveryRoutesAndScopeOutcome(ctx, evt, recipients, deliveryTargets, scope)
		if err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if err := validateEventAppendOutcome(outcome); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return outcome, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventRoutePersistence); ok {
		if err := atomicStore.PersistEventWithDeliveryRoutesAndScope(ctx, evt, recipients, deliveryTargets, scope); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return EventAppendInserted, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventReplayScopeOutcomePersistence); ok {
		outcome, err := atomicStore.PersistEventWithDeliveriesAndScopeOutcome(ctx, evt, recipients, scope)
		if err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if err := validateEventAppendOutcome(outcome); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return outcome, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventReplayScopePersistence); ok {
		if err := atomicStore.PersistEventWithDeliveriesAndScope(ctx, evt, recipients, scope); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		return EventAppendInserted, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventOutcomePersistence); ok {
		outcome, err := atomicStore.PersistEventWithDeliveriesOutcome(ctx, evt, recipients)
		if err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if err := validateEventAppendOutcome(outcome); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if outcome == EventAppendExactDuplicate {
			return outcome, nil
		}
		if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID(), scope); err != nil {
				return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", err)
			}
			return outcome, nil
		}
		if replayScopePersistenceRequired(eb.store) {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		return outcome, nil
	}
	if atomicStore, ok := eb.store.(AtomicEventPersistence); ok {
		if err := atomicStore.PersistEventWithDeliveries(ctx, evt, recipients); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event transaction: %w", err)
		}
		if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
			if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID(), scope); err != nil {
				return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", err)
			}
			return EventAppendInserted, nil
		}
		if replayScopePersistenceRequired(eb.store) {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
		}
		return EventAppendInserted, nil
	}
	outcome, err := eb.appendEventRecordOutcome(ctx, evt)
	if err != nil {
		return EventAppendOutcomeUnknown, fmt.Errorf("persist event: %w", err)
	}
	if outcome == EventAppendExactDuplicate {
		return outcome, nil
	}
	if len(recipients) > 0 || len(deliveryRoutes) > 0 {
		if err := eb.insertEventDeliveries(ctx, evt.ID(), recipients, deliveryTargets, deliveryRoutes); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if scopeWriter, ok := eb.store.(EventReplayScopePersistence); ok && scopeWriter != nil {
		if err := scopeWriter.UpsertCommittedReplayScope(ctx, evt.ID(), scope); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", err)
		}
		return outcome, nil
	}
	if replayScopePersistenceRequired(eb.store) {
		return EventAppendOutcomeUnknown, fmt.Errorf("persist committed replay scope: %w", runtimereplayclaim.ErrMissingCommittedReplayScope)
	}
	return outcome, nil
}

func (eb *EventBus) appendEventRecordOutcome(ctx context.Context, evt events.Event) (EventAppendOutcome, error) {
	if owner, ok := eb.store.(EventAppendOutcomePersistence); ok && owner != nil {
		outcome, err := owner.AppendEventOutcome(ctx, evt)
		if err != nil {
			return EventAppendOutcomeUnknown, err
		}
		if err := validateEventAppendOutcome(outcome); err != nil {
			return EventAppendOutcomeUnknown, fmt.Errorf("event store: %w", err)
		}
		return outcome, nil
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return EventAppendOutcomeUnknown, err
	}
	return EventAppendInserted, nil
}

func (eb *EventBus) insertEventDeliveries(ctx context.Context, eventID string, recipients []string, deliveryTargets map[string]events.RouteIdentity, deliveryRoutes []events.DeliveryRoute) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if store, ok := eb.store.(EventDeliveryRouteSetPersistence); ok && store != nil && len(deliveryRoutes) > 0 {
		return store.InsertEventDeliveryRoutes(ctx, eventID, deliveryRoutes)
	}
	if deliveryRouteSetPersistenceRequired(deliveryRoutes) {
		return errMissingEventDeliveryRouteSetPersistence
	}
	if store, ok := eb.store.(EventDeliveryRoutePersistence); ok && store != nil {
		return store.InsertEventDeliveriesWithTargets(ctx, eventID, recipients, deliveryTargets)
	}
	return eb.store.InsertEventDeliveries(ctx, eventID, recipients)
}

func (eb *EventBus) insertEventDeliveriesMutation(
	ctx context.Context,
	mutation EventMutation,
	eventID string,
	recipients []string,
	deliveryTargets map[string]events.RouteIdentity,
	deliveryRoutes []events.DeliveryRoute,
) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) > 0 {
		if err := mutation.InsertEventDeliveryRoutes(ctx, eventID, deliveryRoutes); err != nil {
			return err
		}
		return nil
	}
	if deliveryRouteSetPersistenceRequired(deliveryRoutes) {
		return errMissingEventDeliveryRouteSetPersistence
	}
	if len(deliveryTargets) > 0 {
		return mutation.InsertEventDeliveriesWithTargets(ctx, eventID, recipients, deliveryTargets)
	}
	return mutation.InsertEventDeliveries(ctx, eventID, recipients)
}

func (eb *EventBus) upsertCommittedReplayScopeMutation(ctx context.Context, mutation EventMutation, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	err := mutation.UpsertCommittedReplayScope(ctx, eventID, scope)
	if err == nil {
		return nil
	}
	if errors.Is(err, runtimereplayclaim.ErrMissingCommittedReplayScope) && !replayScopePersistenceRequired(eb.store) {
		return nil
	}
	return fmt.Errorf("persist committed replay scope: %w", err)
}

func (eb *EventBus) eventMutationFromContext(ctx context.Context) (EventMutation, bool) {
	if mutation, ok := EventMutationFromContext(ctx); ok {
		return mutation, true
	}
	provider, ok := eb.store.(EventMutationContextProvider)
	if !ok || provider == nil {
		return nil, false
	}
	return provider.EventMutationFromContext(ctx)
}

var errMissingEventDeliveryRouteSetPersistence = errors.New("event store does not support typed delivery route persistence")

func deliveryRouteSetPersistenceRequired(routes []events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if strings.TrimSpace(route.SubscriberType) != routePlanSubscriberAgent {
			return true
		}
	}
	return false
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
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		out = append(out, id)
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
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		item := PublishDiagnosticRecipient{
			ID:             id,
			Type:           strings.TrimSpace(subscriber.Type),
			Path:           strings.TrimSpace(subscriber.Path),
			MatchedPattern: strings.TrimSpace(subscriber.MatchPattern),
			RouteSource:    strings.TrimSpace(subscriber.RouteSource),
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
	if strings.TrimSpace(subscriber.Type) != "node" {
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

// PublishDirect persists an event and delivers it to an explicit caller-supplied
// recipient set. The recipient manifest still routes through the canonical
// delivery policy so explicit delivery cannot bypass scoped-recipient rules.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	start := time.Now()
	persisted := false
	queued := false
	defer func() {
		if queued {
			return
		}
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, failure := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID(), status, failure)
	}()
	if evt.Type() == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(ctx, string(evt.Type()), evt.Payload()); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type())), err)
		}
	}
	ctx, evt, err = admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return err
	}
	plan, err := eb.planDirectRoutePlan(ctx, evt, recipients)
	if err != nil {
		return err
	}
	appendOutcome, err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipientIDs(), plan.DeliveryTargets(), plan.DeliveryRoutes(), runtimereplayclaim.CommittedReplayScopeDirect)
	if err != nil {
		return err
	}
	if appendOutcome == EventAppendExactDuplicate {
		return nil
	}
	persisted = true
	eb.notifyTestPublishPersisted(ctx, evt, plan)
	if plan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, plan.TargetFailure)
		eb.recordTargetDeliveryFailure(ctx, evt, plan)
		eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
		return nil
	}
	eb.logQueuedDeliveries(ctx, evt, plan.PersistedRecipientIDs(), "direct_publish", plan.ExtraDetail)
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return err
	} else if reason != "" {
		queued = true
		eb.logDispatchQueued(ctx, reason, evt, len(plan.RecipientIDs()), true, false)
		return nil
	}
	recipients = plan.RecipientIDs()
	if err := eb.deliverToRecipientsWithRoutes(ctx, evt, recipients, plan.DeliveryRoutes()); err != nil {
		return err
	}
	detail := map[string]any{
		"direct":           true,
		"recipients_count": len(recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID()),
	}
	for k, v := range plan.ExtraDetail {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "Event was delivered directly to recipients", "eventbus", "delivered", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, detail, nil, int(time.Since(start)/time.Microsecond))
	return nil
}

// CheckDirectRecipients applies the same direct-recipient policy used by
// PublishDirect, then verifies the allowed recipients are currently deliverable.
// It is intentionally side-effect free so public API owners can fail closed
// before creating replay evidence.
func (eb *EventBus) CheckDirectRecipients(ctx context.Context, evt events.Event, recipients []string) (DirectRecipientStatus, error) {
	requested := uniqueStrings(recipients)
	status := DirectRecipientStatus{Requested: append([]string(nil), requested...)}
	if eb == nil {
		status.Missing = append([]string(nil), requested...)
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
	plan, err := eb.planDirectRoutePlan(ctx, evt, requested)
	if err != nil {
		return status, err
	}
	plannedRecipients := plan.RecipientIDs()
	status.Recipients = append([]string(nil), plannedRecipients...)
	status.Filtered = filteredRecipients(requested, plannedRecipients)
	liveRecipients := eb.snapshotRecipientChans(plannedRecipients)
	live := make(map[string]struct{}, len(liveRecipients))
	for _, recipient := range liveRecipients {
		live[recipient.agentID] = struct{}{}
	}
	for _, recipient := range plannedRecipients {
		if _, ok := live[recipient]; !ok {
			status.Missing = append(status.Missing, recipient)
		}
	}
	status.Missing = uniqueStrings(append(status.Missing, status.Filtered...))
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
	ictx, evt, err := admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return PublishRecipientPlan{}, err
	}
	plan, err := eb.planSubscribedRoutePlan(withTemplateInstanceLifecyclePreview(ictx), evt, false)
	if err != nil {
		return PublishRecipientPlan{}, err
	}
	return eb.publishRecipientPlan(evt, plan), nil
}

// PublishPersistedRecipients delivers an already-committed event using the
// persisted agent manifest plus the authoritative committed replay scope.
func (eb *EventBus) PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error {
	return eb.publishPersistedRecipients(ctx, evt, recipients, false)
}

// RecoverPersistedPipeline replays the complete pipeline for an event whose
// terminal pipeline receipt was never written.
func (eb *EventBus) RecoverPersistedPipeline(ctx context.Context, evt events.Event, recipients []string) error {
	return eb.publishPersistedRecipients(ctx, evt, recipients, true)
}

func (eb *EventBus) ReleasePendingPersistedDeliveriesForEvent(ctx context.Context, evt events.Event) error {
	if eb == nil {
		return nil
	}
	return eb.publishPersistedRecipients(ctx, evt, nil, true)
}

func (eb *EventBus) publishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string, replayInterceptors bool) error {
	eb.clearPendingOutboxOperation(evt.ID())
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	recipients = uniqueStrings(recipients)
	if evt.Type() == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type())) {
		return fmt.Errorf("%w: %s", ErrInvalidEventType, strings.TrimSpace(string(evt.Type())))
	}
	var err error
	ctx, evt, err = admitEventForPublish(ctx, evt, time.Now(), "")
	if err != nil {
		return err
	}
	if reason, err := eb.dispatchQueueReason(ctx, evt); err != nil {
		return err
	} else if reason != "" {
		if reason == dispatchQueueRuntimeIngress {
			return ErrRuntimeIngressPaused
		}
		return ErrRunDispatchBlocked
	}
	scope, err := eb.authoritativeReplayScopeForEvent(ctx, evt.ID())
	if err != nil {
		return err
	}
	liveRecipients, internalRecipients, deliveryRoutes, err := eb.replayRecipientsForCommittedEvent(ctx, evt, recipients, scope)
	if err != nil {
		return err
	}
	passthrough := true
	deferred := []events.Event(nil)
	if replayInterceptors && scope == runtimereplayclaim.CommittedReplayScopeSubscribed {
		postCommitActions := make([]func(), 0, 8)
		deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
		receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
		ctx = runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions, eb.pipelineTransitionCapability())
		ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
		ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
		var err error
		passthrough, deferred, err = eb.runInterceptorsForDeliveryRoutes(ctx, evt, deliveryRoutes)
		if err != nil {
			return err
		}
		runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
		runtimepipeline.FlushDeferredPipelineTransitions(ctx, deferredTransitions)
	}
	if passthrough && len(liveRecipients) > 0 {
		if err := eb.deliverToRecipientsWithRoutes(ctx, evt, liveRecipients, deliveryRoutes); err != nil {
			return err
		}
	}
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	if !passthrough || len(liveRecipients) == 0 {
		return nil
	}
	owner := "event_deliveries"
	if scope == runtimereplayclaim.CommittedReplayScopeSubscribed {
		owner = "event_deliveries+committed_replay_scope"
	}
	eb.logRuntime(ctx, "debug", "Persisted event was delivered to authoritative recipients", "eventbus", "delivered", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, map[string]any{
		"direct":                     scope == runtimereplayclaim.CommittedReplayScopeDirect,
		"delivery_manifest_owner":    owner,
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            strings.TrimSpace(evt.ParentEventID()),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
		"replay_scope":               string(scope),
	}, nil, 0)
	return nil
}

func (eb *EventBus) deliveryTargetsForEvent(ctx context.Context, eventID string) map[string]events.RouteIdentity {
	reader, ok := eb.store.(EventDeliveryTargetReader)
	if !ok || reader == nil {
		return nil
	}
	targets, err := reader.ListEventDeliveryTargets(ctx, eventID)
	if err != nil {
		return nil
	}
	return targets
}

func (eb *EventBus) deliveryRoutesForEvent(ctx context.Context, eventID string) []events.DeliveryRoute {
	reader, ok := eb.store.(EventDeliveryRouteSetReader)
	if ok && reader != nil {
		routes, err := reader.ListEventDeliveryRoutes(ctx, eventID)
		if err == nil && len(routes) > 0 {
			return events.NormalizeDeliveryRoutes(routes)
		}
	}
	targets := eb.deliveryTargetsForEvent(ctx, eventID)
	if len(targets) == 0 {
		return nil
	}
	recipients := make([]string, 0, len(targets))
	for recipient := range targets {
		recipients = append(recipients, recipient)
	}
	return deliveryRoutesFromTargetMap(recipients, "agent", targets)
}

func (eb *EventBus) recordTargetDeliveryFailure(ctx context.Context, evt events.Event, plan RoutePlan) {
	failure := runtimepinrouting.TargetFailure(strings.TrimSpace(string(plan.TargetFailure)))
	if failure == "" {
		return
	}
	_, detail, record := targetDeliveryFailureRecord(evt, plan, failure)
	eb.logRuntime(ctx, "warn", "Pin routing target delivery failed", "eventbus", "target_resolution_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, &record.Failure, 0)

	recorder, ok := eb.store.(targetFailureDeadLetterRecorder)
	if !ok || recorder == nil {
		return
	}
	if err := recorder.RecordDeadLetter(ctx, record); err != nil {
		eb.logRuntime(ctx, "warn", "Pin routing target failure dead-letter record failed", "eventbus", "target_resolution_failed_dead_letter_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, eventBusDependencyFailure(err, "target_failure_dead_letter_persist_failed", "record_target_failure"), 0)
	}
}

func (eb *EventBus) recordTargetDeliveryFailureMutation(ctx context.Context, mutation EventMutation, evt events.Event, plan RoutePlan) error {
	failure := runtimepinrouting.TargetFailure(strings.TrimSpace(string(plan.TargetFailure)))
	if failure == "" || mutation == nil {
		return nil
	}
	_, detail, record := targetDeliveryFailureRecord(evt, plan, failure)
	eb.logRuntime(ctx, "warn", "Pin routing target delivery failed", "eventbus", "target_resolution_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, &record.Failure, 0)

	if err := mutation.RecordDeadLetter(ctx, record); err != nil {
		eb.logRuntime(ctx, "warn", "Pin routing target failure dead-letter record failed", "eventbus", "target_resolution_failed_dead_letter_failed", evt.ID(), string(evt.Type()), evt.SourceAgent(), evt.EntityID(), "", nil, detail, eventBusDependencyFailure(err, "target_failure_dead_letter_persist_failed", "record_target_failure"), 0)
		return fmt.Errorf("persist target failure dead letter: %w", err)
	}
	return nil
}

func targetDeliveryFailureRecord(evt events.Event, plan RoutePlan, failure runtimepinrouting.TargetFailure) (string, map[string]any, runtimedeadletters.Record) {
	plan = plan.Normalized()
	target := evt.TargetRoute()
	targetSet := evt.TargetRoutes()
	detail := map[string]any{
		"target_detail_code":   string(failure),
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
		err = runtimefailures.NewTarget(string(failure), "eventbus", "resolve_delivery_target", detail)
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
