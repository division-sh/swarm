package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

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
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
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
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt})
	if err != nil {
		return err
	}
	group := newLocalDeliveryCompletionGroup()
	waitCtx := ctx
	ctx = withLocalDeliveryCompletionGroup(ctx, group)
	if prepared.dispatchContext != nil {
		prepared.dispatchContext = withLocalDeliveryCompletionGroup(prepared.dispatchContext, group)
	}
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
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
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
	bus              *EventBus
	event            events.Event
	direct           bool
	directRecipients []string
	admitted         events.AdmittedEvent
	publicationClaim *pipelinePublicationClaim
}

func (eb *EventBus) commitPublish(ctx context.Context, plan eventBusCommitPublishPlan) (PreparedPublish, error) {
	owner, ok := eb.store.(CommitPublishOwner)
	if !ok || owner == nil {
		return PreparedPublish{}, errors.New("selected store does not support the closed CommitPublish operation")
	}
	preparedCtx, admitted, err := eb.admitPublishEvent(ctx, plan.event)
	if err != nil {
		return PreparedPublish{}, err
	}
	claim, err := eb.claimPipelinePublication(preparedCtx, admitted.ID())
	if err != nil {
		return PreparedPublish{}, err
	}
	preparedCtx = claim.BindContext(preparedCtx)
	plan.event = admitted.Event()
	plan.admitted = admitted
	plan.publicationClaim = claim
	prepared, err := owner.CommitPublish(preparedCtx, plan)
	if err != nil {
		claim.Release(preparedCtx)
		return PreparedPublish{}, err
	}
	return prepared, nil
}

func (eventBusCommitPublishPlan) commitPublishPlan() {}

func (p eventBusCommitPublishPlan) PrepareCommitPublish(ctx context.Context) (PreparedPublish, error) {
	if p.bus == nil {
		return PreparedPublish{}, errors.New("event bus is required")
	}
	if p.admitted.ID() == "" || p.admitted.ID() != p.event.ID() {
		return PreparedPublish{}, errors.New("event publish plan requires pre-admitted event identity")
	}
	if !p.direct {
		return p.bus.prepareAdmittedPublishInMutation(ctx, p.admitted, p.publicationClaim, runtimereplayclaim.CommittedReplayScopeSubscribed, func(ctx context.Context, evt events.Event) (RoutePlan, error) {
			return p.bus.planSubscribedRoutePlan(ctx, evt, true)
		})
	}
	requested := uniqueStrings(p.directRecipients)
	if len(requested) == 0 {
		return PreparedPublish{}, errors.New("direct event publication requires at least one recipient")
	}
	return p.bus.prepareAdmittedPublishInMutation(ctx, p.admitted, p.publicationClaim, runtimereplayclaim.CommittedReplayScopeDirect, func(ctx context.Context, evt events.Event) (RoutePlan, error) {
		plan, err := p.bus.planDirectRoutePlan(ctx, evt, requested)
		if err != nil {
			return RoutePlan{}, err
		}
		if filtered := filteredRecipients(requested, plan.RecipientIDs()); len(filtered) > 0 {
			return RoutePlan{}, fmt.Errorf("direct delivery rejected recipients: %s", strings.Join(filtered, ", "))
		}
		return plan, nil
	})
}

// PreparedPublish is the transaction-local result of canonical route planning.
// Its route plan remains EventBus-owned; callers may persist the exported
// delivery-route manifest but cannot reinterpret or replace the plan.
type PreparedPublish struct {
	Event            events.Event
	admitted         events.AdmittedEvent
	plan             RoutePlan
	exactDuplicate   bool
	targetFailure    bool
	dispatchQueued   bool
	queueReason      string
	direct           bool
	publicationClaim *pipelinePublicationClaim
	dispatchContext  context.Context
}

func beginPreparedPublish(ctx context.Context, transaction CommitPublishTransaction, event events.AdmittedEvent) (EventAppendOutcome, error) {
	outcome, err := transaction.BeginPreparedPublish(ctx, PreparedPublishEvent{event: event})
	if err != nil {
		return EventAppendOutcomeUnknown, err
	}
	if err := validateEventAppendOutcome(outcome); err != nil {
		return EventAppendOutcomeUnknown, fmt.Errorf("selected-store event commit: %w", err)
	}
	return outcome, nil
}

func finalizePreparedPublish(ctx context.Context, transaction CommitPublishTransaction, req CommitPublishRequest) error {
	return transaction.FinalizePreparedPublish(ctx, PreparedPublishFinalization{request: req})
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
	request := CommitPublishRequest{
		Event:          p.admitted,
		DeliveryRoutes: p.plan.DeliveryRoutes(),
		ReplayScope:    runtimereplayclaim.CommittedReplayScopeSubscribed,
	}
	if failure := runtimepinrouting.TargetFailure(strings.TrimSpace(string(p.plan.TargetFailure))); failure != "" {
		request.PipelineReceipt = &InitialPipelineReceipt{Status: "dead_letter", Failure: targetDeliveryFailureEnvelope(failure)}
		_, _, record := targetDeliveryFailureRecord(p.Event, p.plan, failure)
		request.DeadLetter = &record
	}
	return request
}

func (p PreparedPublish) WithCommitOutcome(outcome EventAppendOutcome) (PreparedPublish, error) {
	if err := validateEventAppendOutcome(outcome); err != nil {
		return PreparedPublish{}, err
	}
	p.exactDuplicate = outcome == EventAppendExactDuplicate
	return p, nil
}

// AbandonPreparedPublish releases preparation-only process state when the
// named durable operation does not commit or dispatch the prepared event.
func (eb *EventBus) AbandonPreparedPublish(ctx context.Context, prepared PreparedPublish) {
	prepared.publicationClaim.Release(ctx)
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
	ctx = eb.withBundleFingerprint(ctx)
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
	preparedCtx = publicationClaim.BindContext(preparedCtx)
	plan, err := eb.planSubscribedPublish(preparedCtx, evt)
	if err != nil {
		publicationClaim.Release(preparedCtx)
		return PreparedPublish{}, err
	}
	prepared := PreparedPublish{
		Event: evt, admitted: admitted, plan: plan,
		publicationClaim: publicationClaim, dispatchContext: preparedCtx,
	}
	if plan.TargetFailure != "" {
		prepared.targetFailure = true
		return prepared, nil
	}
	if reason, err := eb.dispatchQueueReason(preparedCtx, evt); err != nil {
		publicationClaim.Release(preparedCtx)
		return PreparedPublish{}, err
	} else if reason != "" {
		prepared.dispatchQueued = true
		prepared.queueReason = reason
	}
	return prepared, nil
}

// PreparePublishInMutation persists the event, performs real stateful route
// materialization, and persists the canonical delivery/replay facts through the
// active typed mutation. Dispatch is deliberately separate and may happen only
// after the selected-store transaction commits.
func (eb *EventBus) PreparePublishInMutation(ctx context.Context, evt events.Event) (PreparedPublish, error) {
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return PreparedPublish{}, err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	return eb.preparePublishInMutation(ctx, evt, runtimereplayclaim.CommittedReplayScopeSubscribed, func(ctx context.Context, evt events.Event) (RoutePlan, error) {
		return eb.planSubscribedRoutePlan(ctx, evt, true)
	})
}

func (eb *EventBus) preparePublishInMutation(ctx context.Context, evt events.Event, replayScope runtimereplayclaim.CommittedReplayScope, planRoutes func(context.Context, events.Event) (RoutePlan, error)) (PreparedPublish, error) {
	ictx, admitted, err := eb.admitPublishEvent(ctx, evt)
	if err != nil {
		return PreparedPublish{}, err
	}
	publicationClaim, err := eb.claimPipelinePublication(ictx, admitted.ID())
	if err != nil {
		return PreparedPublish{}, err
	}
	return eb.prepareAdmittedPublishInMutation(ictx, admitted, publicationClaim, replayScope, planRoutes)
}

func (eb *EventBus) admitPublishEvent(ctx context.Context, evt events.Event) (context.Context, events.AdmittedEvent, error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return ctx, events.AdmittedEvent{}, err
	}
	ctx = eb.withBundleFingerprint(ctx)
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

func (eb *EventBus) prepareAdmittedPublishInMutation(
	ctx context.Context,
	admitted events.AdmittedEvent,
	publicationClaim *pipelinePublicationClaim,
	replayScope runtimereplayclaim.CommittedReplayScope,
	planRoutes func(context.Context, events.Event) (RoutePlan, error),
) (PreparedPublish, error) {
	evt := admitted.Event()
	transaction, ok := CommitPublishTransactionFromContext(ctx)
	if !ok || transaction == nil {
		return PreparedPublish{}, errors.New("typed CommitPublish transaction context is required")
	}
	txctx := WithCommitPublishTransaction(ctx, transaction)
	txctx = publicationClaim.BindContext(txctx)
	if publicationClaim != nil && !runtimepipeline.QueuePipelineRollbackAction(txctx, func(actionCtx context.Context) { publicationClaim.Release(actionCtx) }) {
		publicationClaim.Release(txctx)
		return PreparedPublish{}, errors.New("event mutation rollback actions are required for pipeline publication claim")
	}
	txctx, err := eb.withTransactionRouteOverlay(txctx)
	if err != nil {
		return PreparedPublish{}, err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	txctx = runtimepipeline.WithPipelineReceiptOverride(txctx, receiptOverride)
	prepared := PreparedPublish{
		Event:            evt,
		admitted:         admitted,
		direct:           replayScope == runtimereplayclaim.CommittedReplayScopeDirect,
		publicationClaim: publicationClaim,
		dispatchContext:  txctx,
	}
	appendOutcome, err := beginPreparedPublish(txctx, transaction, admitted)
	if err != nil {
		return PreparedPublish{}, fmt.Errorf("persist event: %w", err)
	}
	if appendOutcome == EventAppendExactDuplicate {
		prepared.exactDuplicate = true
		return prepared, nil
	}
	inboundPlan, err := planRoutes(txctx, evt)
	if err != nil {
		return PreparedPublish{}, err
	}
	if err := inboundPlan.ValidatePersistentDeliveries(); err != nil {
		return PreparedPublish{}, fmt.Errorf("validate durable route plan: %w", err)
	}
	prepared.plan = inboundPlan
	var initialReceipt *InitialPipelineReceipt
	var initialDeadLetter *runtimedeadletters.Record
	if inboundPlan.TargetFailure != "" {
		applyTargetDeliveryFailureReceipt(receiptOverride, inboundPlan.TargetFailure)
		status, failure := pipelineReceiptStatus(txctx, nil)
		initialReceipt = &InitialPipelineReceipt{Status: status, Failure: failure}
		_, _, deadLetter := targetDeliveryFailureRecord(evt, inboundPlan, inboundPlan.TargetFailure)
		initialDeadLetter = &deadLetter
	}
	if err := finalizePreparedPublish(txctx, transaction, CommitPublishRequest{
		Event: admitted, DeliveryRoutes: inboundPlan.DeliveryRoutes(), ReplayScope: replayScope,
		PipelineReceipt: initialReceipt, DeadLetter: initialDeadLetter,
	}); err != nil {
		return PreparedPublish{}, fmt.Errorf("finalize event publish: %w", err)
	}
	if eb.testLifecycleProbe != nil {
		runtimepipeline.QueuePipelinePostCommitAction(txctx, func(actionCtx context.Context) {
			eb.notifyTestPublishPersisted(actionCtx, evt, inboundPlan)
		})
	}
	if inboundPlan.TargetFailure != "" {
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
	if _, ok := worklifetime.OccurrenceFromContext(ctx); !ok && eb != nil && eb.workOwner != nil {
		ctx = worklifetime.WithOccurrence(ctx, eb.workOwner)
	}
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
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
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
	_, ok := CommitPublishTransactionFromContext(ctx)
	if !ok {
		return errors.New("typed CommitPublish transaction context is required")
	}
	txctx := ctx
	if !runtimepipeline.QueuePipelinePostCommitAction(txctx, func(actionCtx context.Context) {
		dispatchCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(actionCtx))
		if err := eb.DispatchPreparedPublish(dispatchCtx, prepared); err != nil {
			eb.reportLocalDispatchFailure("post_commit_dispatch_failed", prepared.Event, err)
		}
	}) {
		return errors.New("event mutation post-commit actions are required")
	}
	return nil
}

// DispatchPreparedPublish consumes only the plan finalized by
// PreparePublishInMutation. It never invokes route planning again.
func (eb *EventBus) DispatchPreparedPublish(ctx context.Context, prepared PreparedPublish) error {
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	return eb.dispatchPreparedPublish(ctx, prepared)
}

// DispatchPreparedPublishAndWait dispatches one committed publish and joins
// the complete local-delivery tree produced by its handlers. It is intended
// for bounded runtimes that must finish their accepted story before retiring.
func (eb *EventBus) DispatchPreparedPublishAndWait(ctx context.Context, prepared PreparedPublish) error {
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	group := newLocalDeliveryCompletionGroup()
	waitCtx := ctx
	ctx = withLocalDeliveryCompletionGroup(ctx, group)
	if prepared.dispatchContext != nil {
		prepared.dispatchContext = withLocalDeliveryCompletionGroup(prepared.dispatchContext, group)
	}
	return eb.dispatchPreparedPublishWithCompletion(ctx, prepared, func() error {
		group.releaseDispatch()
		return group.wait(waitCtx)
	})
}

func (eb *EventBus) dispatchPreparedPublish(ctx context.Context, prepared PreparedPublish) error {
	return eb.dispatchPreparedPublishWithCompletion(ctx, prepared, nil)
}

func (eb *EventBus) dispatchPreparedPublishWithCompletion(ctx context.Context, prepared PreparedPublish, completion func() error) error {
	if strings.TrimSpace(prepared.Event.ID()) == "" {
		return errors.New("prepared event is required")
	}
	if prepared.dispatchContext != nil {
		ctx = WithoutCommitPublishTransaction(runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(prepared.dispatchContext))))
	}
	ctx = prepared.publicationClaim.BindContext(ctx)
	defer prepared.publicationClaim.Release(ctx)
	dispatchErr := eb.dispatchPreparedPublishBody(ctx, prepared)
	if completion == nil {
		return dispatchErr
	}
	return errors.Join(dispatchErr, completion())
}

func (eb *EventBus) dispatchPreparedPublishBody(ctx context.Context, prepared PreparedPublish) error {
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

func (eb *EventBus) DispatchPreparedPublishAsync(ctx context.Context, prepared PreparedPublish) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if eb == nil || eb.workOwner == nil {
		return errors.New("asynchronous event dispatch requires a runtime work occurrence")
	}
	owner := eb.workOwnerForContext(ctx)
	admissionCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx)))
	lease, err := owner.Begin(admissionCtx)
	if err != nil {
		return fmt.Errorf("admit asynchronous event dispatch: %w", err)
	}
	dispatchCtx := lease.Context()
	go func() {
		defer func() { _ = lease.Done() }()
		if err := eb.dispatchPreparedPublish(bindWorkContext(dispatchCtx, lease, owner), prepared); err != nil {
			eb.reportLocalDispatchFailure("async_dispatch_failed", prepared.Event, err)
		}
	}()
	return nil
}

func (eb *EventBus) dispatchCommittedPublishAsync(ctx context.Context, evt events.Event, inboundPlan RoutePlan, publicationClaim *pipelinePublicationClaim) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if eb == nil || eb.workOwner == nil {
		return errors.New("asynchronous committed dispatch requires a runtime work occurrence")
	}
	owner := eb.workOwnerForContext(ctx)
	admissionCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx)))
	lease, err := owner.Begin(admissionCtx)
	if err != nil {
		return fmt.Errorf("admit asynchronous committed dispatch: %w", err)
	}
	dispatchCtx := lease.Context()
	go func() {
		defer func() { _ = lease.Done() }()
		dispatchCtx = bindWorkContext(dispatchCtx, lease, owner)
		defer publicationClaim.Release(dispatchCtx)
		if err := eb.completeCommittedPublishDispatch(dispatchCtx, evt, inboundPlan); err != nil {
			eb.reportLocalDispatchFailure("async_committed_dispatch_failed", evt, err)
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

func (eb *EventBus) completeCommittedPublishDispatch(ctx context.Context, evt events.Event, inboundPlan RoutePlan) error {
	ctx = WithoutCommitPublishTransaction(ctx)
	workCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(ctx))
	eb.notifyTestPostCommitDispatchStarted(workCtx, evt)
	defer eb.notifyTestPostCommitDispatchCompleted(workCtx, evt)

	inboundPlan = inboundPlan.Normalized()
	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]runtimepipeline.OwnerAction, 0, 8)
	afterPublishActions := make([]runtimepipeline.OwnerAction, 0, 4)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	workCtx = runtimepipeline.WithPipelineTransitionCollector(workCtx, &deferredTransitions)
	workCtx = runtimepipeline.WithPipelinePostCommitActions(workCtx, &postCommitActions)
	workCtx = runtimepipeline.WithPipelineAfterPublishActions(workCtx, &afterPublishActions)
	workCtx = runtimepipeline.WithPipelineReceiptOverride(workCtx, receiptOverride)
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	defer runtimepipeline.FlushPipelineAfterPublishActions(afterPublishActions)

	passthrough, deferred, err := eb.runInterceptorsForDeliveryRoutes(workCtx, evt, inboundPlan.DeliveryRoutes())
	if err != nil {
		eb.recordCommittedPublishReceipt(ctx, evt, err)
		return err
	}

	if passthrough {
		recipients := inboundPlan.RecipientIDs()
		if len(recipients) > 0 {
			eb.logQueuedDeliveries(ctx, evt, inboundPlan.PersistedRecipientIDs(), "matched_agent_subscription", inboundPlan.ExtraDetail)
			if err := eb.deliverRoutePlanWithRoutes(workCtx, evt, inboundPlan); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return err
			}
			eb.logDelivery(ctx, evt, recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(workCtx, *inboundPlan.CycleEscalation); err != nil {
				eb.recordCommittedPublishReceipt(ctx, evt, err)
				return err
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(workCtx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, 0)
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(workCtx, deferredTransitions)

	for _, d := range deferred {
		if err := eb.publishDeferred(workCtx, d); err != nil {
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

func projectEventForDeliveryRoute(evt events.Event, route events.DeliveryRoute) (events.DeliveryEvent, error) {
	projected, err := events.NewDeliveryEvent(evt, route)
	if err != nil {
		return events.DeliveryEvent{}, fmt.Errorf("delivery route for %s: %w", strings.TrimSpace(route.SubscriberID), err)
	}
	return projected, nil
}

func (eb *EventBus) withBundleFingerprint(ctx context.Context) context.Context {
	if ctx == nil || eb == nil {
		return ctx
	}
	eb.mu.RLock()
	runtimeInstanceID := eb.runtimeInstanceID
	sourceFact := eb.bundleSourceFact
	fingerprint := eb.bundleFingerprint
	eb.mu.RUnlock()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	if runtimeInstanceID != "" && sourceFact.BundleHash != "" {
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, sourceFact.BundleHash))
	}
	if _, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		return ctx
	}
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

func (eb *EventBus) WithBundleFingerprint(ctx context.Context) context.Context {
	return eb.withBundleFingerprint(ctx)
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
	ctx = WithoutCommitPublishTransaction(runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(ctx)))
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
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
		ctx = bindWorkContext(ctx, lease, eb.workOwner)
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	ctx = eb.withBundleFingerprint(ctx)
	prepared, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: evt, direct: true, directRecipients: uniqueStrings(recipients)})
	if err != nil {
		return err
	}
	return eb.dispatchPreparedPublish(ctx, prepared)
}

func (eb *EventBus) beginRuntimeWork(ctx context.Context) (*worklifetime.Lease, error) {
	if eb == nil {
		return nil, errors.New("event bus is required")
	}
	if eb.workOwner == nil {
		return nil, errors.New("event bus requires a process work occurrence")
	}
	lease, err := eb.workOwnerForContext(ctx).Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit event bus work: %w", err)
	}
	return lease, nil
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
	admitted, err := events.RevalidatePersistedEvent(evt)
	if err != nil {
		return err
	}
	evt = admitted.Event()
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
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
		postCommitActions := make([]runtimepipeline.OwnerAction, 0, 8)
		deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
		receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
		ctx = runtimepipeline.WithPipelineTransitionCollector(ctx, &deferredTransitions)
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
