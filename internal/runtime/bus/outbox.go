package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type engineDispatcher struct {
	bus *EventBus
}

type pendingOutboxOperation struct {
	sequence         uint64
	intent           runtimeengine.EmitIntent
	outcome          EventAppendOutcome
	publicationClaim *pipelinePublicationClaim
	deliveryHandoffs []runtimedelivery.DurableHandoffProof
}

type pendingOutboxDispatch struct {
	handled                     bool
	deliveryHandoffsTransferred bool
}

// EnginePublicationPlan is immutable publication data prepared before the
// selected-store engine mutation begins. The private store adapter can inspect
// the closed command but cannot invoke EventBus or acquire runtime authority.
type EnginePublicationPlan struct {
	prepared PreparedPublish
	command  PublicationCommand
	intent   runtimeengine.EmitIntent
}

func (p EnginePublicationPlan) DurablePublicationEventID() string {
	return strings.TrimSpace(p.command.Commit.Event.ID())
}

func (p EnginePublicationPlan) ValidateDurablePublicationPlan() error {
	if err := p.command.Validate(); err != nil {
		return err
	}
	if p.prepared.Event.ID() != p.DurablePublicationEventID() || p.intent.Event.ID() != p.DurablePublicationEventID() {
		return fmt.Errorf("engine publication plan event identity is inconsistent")
	}
	return nil
}

func (p EnginePublicationPlan) PublicationCommand() PublicationCommand { return p.command }

// CommittedEnginePublication pairs one immutable plan with exact selected-
// store evidence. It contains no executable post-commit callback.
type CommittedEnginePublication struct {
	plan      EnginePublicationPlan
	committed CommittedPublication
}

func NewCommittedEnginePublication(plan EnginePublicationPlan, committed CommittedPublication) (CommittedEnginePublication, error) {
	if err := plan.ValidateDurablePublicationPlan(); err != nil {
		return CommittedEnginePublication{}, err
	}
	if err := committed.Validate(); err != nil {
		return CommittedEnginePublication{}, err
	}
	return CommittedEnginePublication{plan: plan, committed: committed}, nil
}

func (p CommittedEnginePublication) CommittedDurablePublicationEventID() string {
	return p.plan.DurablePublicationEventID()
}

func (p CommittedEnginePublication) CommittedDurablePublicationIntent() runtimeengine.EmitIntent {
	return p.plan.intent
}

func (p CommittedEnginePublication) ValidateCommittedDurablePublication() error {
	if err := p.plan.ValidateDurablePublicationPlan(); err != nil {
		return err
	}
	return p.committed.Validate()
}

// NewlyInserted reports whether this commit inserted the canonical occurrence.
// Consumers use this typed evidence to avoid repeating post-commit projections
// for exact duplicate replays.
func (p CommittedEnginePublication) NewlyInserted() bool {
	return p.committed.AppendOutcome == EventAppendInserted
}

// PrepareEnginePublications resolves exact route/delivery facts before the
// engine mutation enters its selected-store transaction.
func (eb *EventBus) PrepareEnginePublications(ctx context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if eb == nil || len(intents) == 0 {
		return nil, nil
	}
	plans := make([]runtimeengine.DurablePublicationPlan, 0, len(intents))
	release := func() {
		_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
	}
	for _, original := range intents {
		intent := original
		if strings.TrimSpace(string(intent.Event.Type())) == "" {
			continue
		}
		intentCtx := events.WithDeliveryContext(ctx, intent.Context)
		preparedCtx, admitted, err := eb.admitPublishEvent(intentCtx, intent.Event)
		if err != nil {
			release()
			return nil, err
		}
		intent.Event = admitted.Event()
		publication := eventBusCommitPublishPlan{bus: eb, event: intent.Event, admitted: admitted}
		if len(intent.Recipients) > 0 {
			publication.direct = true
			publication.directRecipients = append([]string(nil), intent.Recipients...)
		}
		prepared, command, err := eb.prepareClosedPublication(preparedCtx, publication)
		if err != nil {
			release()
			return nil, err
		}
		// Post-commit dispatch must consume the same canonical route facts that
		// the selected store committed, not the pre-projection engine event.
		intent.Event = prepared.Event
		plan := EnginePublicationPlan{prepared: prepared, command: command, intent: intent}
		if err := plan.ValidateDurablePublicationPlan(); err != nil {
			_ = prepared.publicationClaim.Release(context.WithoutCancel(preparedCtx))
			release()
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (eb *EventBus) ReleaseEnginePublications(ctx context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	var result error
	for _, value := range plans {
		plan, ok := value.(EnginePublicationPlan)
		if !ok {
			result = errors.Join(result, fmt.Errorf("engine publication plan has unexpected type %T", value))
			continue
		}
		if plan.prepared.publicationClaim != nil {
			result = errors.Join(result, plan.prepared.publicationClaim.Release(ctx))
		}
	}
	return result
}

func (eb *EventBus) FinalizeEnginePublications(ctx context.Context, evidence []runtimeengine.CommittedDurablePublication) error {
	for _, value := range evidence {
		committed, ok := value.(CommittedEnginePublication)
		if !ok {
			return fmt.Errorf("committed engine publication has unexpected type %T", value)
		}
		if err := committed.ValidateCommittedDurablePublication(); err != nil {
			return err
		}
		prepared, err := committed.plan.prepared.WithCommitOutcome(committed.committed.AppendOutcome)
		if err != nil {
			return err
		}
		prepared.committedHandoffs = append([]runtimedelivery.DurableHandoffProof(nil), committed.committed.DeliveryHandoffs...)
		if err := eb.finalizeCommittedFlowInstanceActivations(ctx, committed.committed.Activations); err != nil {
			return err
		}
		if eb.testLifecycleProbe != nil && !prepared.exactDuplicate {
			eb.notifyTestPublishPersisted(ctx, prepared.Event, prepared.plan)
		}
		eb.setPendingInternalDelivery(prepared.Event.ID(), prepared.plan.InternalRecipientIDs())
		eb.stageCommittedOutboxOperation(committed.plan.intent, committed.committed.AppendOutcome, prepared.publicationClaim, committed.committed.DeliveryHandoffs)
	}
	return nil
}

func (eb *EventBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	if eb == nil {
		return nil
	}
	return engineDispatcher{bus: eb}
}

func (d engineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if d.bus == nil || len(intents) == 0 {
		return nil
	}
	ctx, lease, err := d.bus.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	normalized := make([]runtimeengine.EmitIntent, 0, len(intents))
	for i := range intents {
		if strings.TrimSpace(string(intents[i].Event.Type())) == "" {
			continue
		}
		intent := intents[i]
		_, admitted, err := admitEventForPublish(ctx, intent.Event, time.Now().UTC())
		if err != nil {
			return err
		}
		intent.Event = admitted.Event()
		normalized = append(normalized, intent)
	}
	intents = normalized
	if len(intents) == 0 {
		return nil
	}
	for _, intent := range intents {
		result, err := d.dispatchPendingOutboxOperation(ctx, intent)
		if err != nil {
			return err
		}
		if result.handled {
			continue
		}
		if err := d.dispatchAndRecord(ctx, intent, nil); err != nil {
			return err
		}
	}
	return nil
}

// dispatchCommittedInterceptorPublications consumes only exact post-commit
// operations staged by the selected-store mutation that ran the interceptor.
// A continuation must never reinterpret a missing operation as permission to
// append or dispatch a fresh event.
func (d engineDispatcher) dispatchCommittedInterceptorPublications(ctx context.Context, events []events.Event) error {
	for _, event := range events {
		result, err := d.dispatchPendingOutboxOperation(ctx, runtimeengine.EmitIntent{
			Event:   event,
			Context: event.DeliveryContext(),
		})
		if err != nil {
			if result.deliveryHandoffsTransferred && errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				continue
			}
			return err
		}
		if !result.handled {
			return fmt.Errorf("deferred interceptor publication %s has no committed post-commit operation", strings.TrimSpace(event.ID()))
		}
	}
	return nil
}

func (d engineDispatcher) dispatchPendingOutboxOperation(ctx context.Context, fallback runtimeengine.EmitIntent) (result pendingOutboxDispatch, err error) {
	ctx, err = d.bus.admitBundleSourceFact(ctx)
	if err != nil {
		return result, err
	}
	operation, ok := d.bus.takePendingOutboxOperation(fallback.Event.ID())
	if !ok {
		return result, nil
	}
	result.handled = true
	defer func() {
		err = errors.Join(err, operation.publicationClaim.Release(ctx))
	}()
	if operation.intent.Event.Type() != fallback.Event.Type() {
		return result, fmt.Errorf("pending outbox event type mismatch for %s: persisted=%s dispatch=%s", fallback.Event.ID(), operation.intent.Event.Type(), fallback.Event.Type())
	}
	if operation.outcome == EventAppendExactDuplicate {
		return result, nil
	}
	if operation.outcome != EventAppendInserted {
		return result, errors.New("pending outbox operation has invalid append outcome")
	}
	handoffs := append([]runtimedelivery.DurableHandoffProof(nil), operation.deliveryHandoffs...)
	if err := d.bus.AcceptCommittedDeliveryHandoffs(handoffs); err != nil {
		return result, err
	}
	result.deliveryHandoffsTransferred = len(handoffs) > 0
	return result, d.dispatchAndRecord(ctx, operation.intent, operation.publicationClaim)
}

func (d engineDispatcher) dispatchAndRecord(ctx context.Context, intent runtimeengine.EmitIntent, publicationClaim *pipelinePublicationClaim) (err error) {
	ctx, err = d.bus.admitBundleSourceFact(ctx)
	if err != nil {
		return err
	}
	var recoveryClaim runtimepipelineobligation.Claim
	claimOpen := false
	if publicationClaim == nil && d.bus.pipelineObligations != nil {
		work, claimErr := d.bus.pipelineObligations.ClaimEvent(ctx, intent.Event.ID(), runtimepipelineobligation.PurposeRecovery)
		if errors.Is(claimErr, runtimepipelineobligation.ErrBusy) || errors.Is(claimErr, runtimepipelineobligation.ErrIneligible) {
			// A post-commit duplicate cannot acquire an already-owned or
			// terminal obligation and therefore has no dispatch work.
			return nil
		}
		if claimErr != nil {
			return claimErr
		}
		recoveryClaim = work.Claim
		claimOpen = true
		defer func() {
			if claimOpen {
				err = errors.Join(err, d.bus.pipelineObligations.Release(context.WithoutCancel(ctx), recoveryClaim))
			}
		}()
	}
	settle := func(disposition runtimepipelineobligation.Disposition) error {
		if publicationClaim != nil {
			return publicationClaim.Settle(ctx, disposition)
		}
		if d.bus.pipelineObligations == nil {
			return nil
		}
		if err := d.bus.settlePipelineObligation(ctx, recoveryClaim, disposition); err != nil {
			return err
		}
		claimOpen = false
		return nil
	}
	queued, outcome, err := d.dispatchIntent(ctx, intent)
	if err != nil {
		if errors.Is(err, ErrRuntimeIngressPaused) || errors.Is(err, ErrRunDispatchBlocked) || errors.Is(err, errAuthoritativeDeliveryIncomplete) {
			return err
		}
		return errors.Join(err, settle(runtimepipelineobligation.Terminal("pipeline_outbox_dispatch_failed", eventBusFailure(err, "dispatch_outbox"))))
	}
	if queued {
		return nil
	}
	if _, retry := outcome.RetryRelease(); retry {
		return nil
	}
	if disposition, ok := outcome.Disposition(); ok {
		return settle(disposition)
	}
	return settle(runtimepipelineobligation.Acknowledged("pipeline_persisted"))
}

func clonePostCommitEmitIntents(intents []runtimeengine.EmitIntent) []runtimeengine.EmitIntent {
	if len(intents) == 0 {
		return nil
	}
	cloned := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		copyIntent := intent
		copyIntent.Event = clonePostCommitPublish(intent.Event)
		if intent.Recipients != nil {
			copyIntent.Recipients = append([]string(nil), intent.Recipients...)
		}
		cloned = append(cloned, copyIntent)
	}
	return cloned
}

func clonePostCommitPublish(evt events.Event) events.Event {
	return evt.Clone()
}

func (d engineDispatcher) dispatchIntent(ctx context.Context, intent runtimeengine.EmitIntent) (queued bool, result runtimepipelineobligation.ExecutionOutcome, err error) {
	ctx = events.WithDeliveryContext(ctx, intent.Context)
	if reason, err := d.bus.dispatchQueueReason(ctx, intent.Event); err != nil {
		return false, runtimepipelineobligation.Continue(), err
	} else if reason != "" {
		d.bus.logDispatchQueued(ctx, reason, intent.Event, len(intent.Recipients), len(intent.Recipients) > 0, false)
		return true, runtimepipelineobligation.Continue(), nil
	}
	deliveryRoutes, err := d.bus.deliveryRoutesForPostCommitIntent(ctx, intent.Event.ID())
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	projection, err := d.bus.receiverProjection(ctx, intent.Context)
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	receiverCtx, closeReceiver, err := d.bus.beginReceiverDispatch(projection, intent.Event)
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	defer func() { err = errors.Join(err, closeReceiver()) }()
	ctx = receiverCtx.Context
	nodePassthrough := true
	if intent.Recipients == nil {
		interception, err := d.bus.runInterceptorsForDeliveryRoutes(ctx, intent.Event, deliveryRoutes)
		if err != nil {
			return false, runtimepipelineobligation.Continue(), err
		}
		if !interception.Outcome.ContinueDispatch() {
			return false, interception.Outcome, nil
		}
		for _, next := range interception.Deferred {
			if err := d.bus.publishDeferred(ctx, next); err != nil {
				return false, runtimepipelineobligation.Continue(), err
			}
		}
		if !interception.EventPassthrough {
			d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
			return false, runtimepipelineobligation.Continue(), nil
		}
		nodePassthrough = interception.NodePassthrough
	}
	recipients, err := d.bus.authoritativeRecipientsForEvent(ctx, intent.Event.ID())
	if err != nil {
		if intent.Recipients == nil || !errors.Is(err, ErrAuthoritativeRecipientManifestUnavailable) {
			return false, runtimepipelineobligation.Continue(), err
		}
		recipients = uniqueStrings(intent.Recipients)
	}
	if !nodePassthrough {
		d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
		deliveryRoutes = nonCollidingAgentDeliveryRoutesAfterNodeConsume(deliveryRoutes)
		recipients = deliveryRouteAgentRecipientIDs(deliveryRoutes)
		if len(recipients) == 0 {
			return false, runtimepipelineobligation.Continue(), nil
		}
	}
	pendingInternal := d.bus.pendingInternalDeliveryForEvent(intent.Event.ID())
	if len(recipients) > 0 && !deliveryRoutesCoverAgentRecipients(deliveryRoutes, recipients) {
		return false, runtimepipelineobligation.Continue(), fmt.Errorf("event %s has persisted agent recipients without exact identity-bearing delivery routes", intent.Event.ID())
	}
	internalRecipients := append([]string(nil), pendingInternal.recipients...)
	if !nodePassthrough {
		internalRecipients = nil
	} else if len(internalRecipients) == 0 {
		internalRecipients = deliveryRouteNodeRecipientIDs(deliveryRoutes)
	}
	liveRecipients := uniqueStrings(append(append([]string(nil), recipients...), internalRecipients...))
	if len(liveRecipients) == 0 {
		d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
		if intent.Event.HasTargetRoute() {
			plan := newRoutePlan(intent.Event)
			plan.TargetFailure = runtimepinrouting.FailureTargetNotSubscribed
			plan = plan.Normalized()
			d.bus.recordTargetDeliveryFailure(ctx, intent.Event, plan)
			return false, runtimepipelineobligation.DeadLetterExecution(plan.TargetFailure.Code(), targetDeliveryFailureEnvelope(plan.TargetFailure)), nil
		}
		return false, runtimepipelineobligation.Continue(), nil
	}
	if err := d.bus.deliverToRecipientsWithRoutes(ctx, intent.Event, liveRecipients, deliveryRoutes); err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
	d.bus.logRuntime(ctx, "debug", "Persisted event intent was delivered", "eventbus", "delivered", intent.Event.ID(), string(intent.Event.Type()), "", intent.Event.EntityID(), "", nil, map[string]any{
		"direct":                     true,
		"delivery_manifest_owner":    "event_deliveries+in_memory_internal",
		"recipients_count":           len(liveRecipients),
		"parent_event_id":            intent.Event.ParentEventID(),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
	}, nil, 0)
	return false, runtimepipelineobligation.Continue(), nil
}

func (eb *EventBus) deliveryRoutesForPostCommitIntent(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	prepared, err := eb.preparedEventForReplay(ctx, eventID)
	if err != nil {
		return nil, err
	}
	return prepared.DeliveryRoutes, nil
}

func replayScopeForEmitIntent(intent runtimeengine.EmitIntent) runtimepipelineobligation.CommittedScope {
	if len(intent.Recipients) > 0 {
		return runtimepipelineobligation.ScopeDirect
	}
	return runtimepipelineobligation.ScopeSubscribed
}

type pendingInternalDelivery struct {
	recipients []string
}

func (eb *EventBus) setPendingInternalDelivery(eventID string, recipients []string) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	recipients = uniqueStrings(recipients)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eventID == "" || len(recipients) == 0 {
		delete(eb.pendingInternalByID, eventID)
		return
	}
	eb.pendingInternalByID[eventID] = pendingInternalDelivery{
		recipients: append([]string(nil), recipients...),
	}
}

func (eb *EventBus) pendingInternalDeliveryForEvent(eventID string) pendingInternalDelivery {
	if eb == nil {
		return pendingInternalDelivery{}
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return pendingInternalDelivery{}
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	pending := eb.pendingInternalByID[eventID]
	return pendingInternalDelivery{
		recipients: append([]string(nil), pending.recipients...),
	}
}

func (eb *EventBus) clearPendingInternalDeliveryRoutes(eventID string) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	delete(eb.pendingInternalByID, eventID)
}

func (eb *EventBus) stageCommittedOutboxOperation(intent runtimeengine.EmitIntent, outcome EventAppendOutcome, publicationClaim *pipelinePublicationClaim, handoffs []runtimedelivery.DurableHandoffProof) {
	if eb == nil {
		return
	}
	intent.Event = clonePostCommitPublish(intent.Event)
	intent.Recipients = append([]string(nil), intent.Recipients...)
	eventID := strings.TrimSpace(intent.Event.ID())
	if eventID == "" {
		return
	}
	eb.mu.Lock()
	eb.pendingOutboxSequence++
	eb.pendingOutboxByID[eventID] = append(eb.pendingOutboxByID[eventID], pendingOutboxOperation{
		sequence: eb.pendingOutboxSequence, intent: intent, outcome: outcome, publicationClaim: publicationClaim,
		deliveryHandoffs: append([]runtimedelivery.DurableHandoffProof(nil), handoffs...),
	})
	eb.mu.Unlock()
}

func (eb *EventBus) takePendingOutboxOperation(eventID string) (pendingOutboxOperation, bool) {
	if eb == nil {
		return pendingOutboxOperation{}, false
	}
	eventID = strings.TrimSpace(eventID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	operations := eb.pendingOutboxByID[eventID]
	if len(operations) == 0 {
		return pendingOutboxOperation{}, false
	}
	operation := operations[0]
	if len(operations) == 1 {
		delete(eb.pendingOutboxByID, eventID)
	} else {
		eb.pendingOutboxByID[eventID] = operations[1:]
	}
	return operation, true
}

func (eb *EventBus) removePendingOutboxOperation(eventID string, sequence uint64) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	operations := eb.pendingOutboxByID[eventID]
	for i := range operations {
		if operations[i].sequence != sequence {
			continue
		}
		operations = append(operations[:i], operations[i+1:]...)
		if len(operations) == 0 {
			delete(eb.pendingOutboxByID, eventID)
		} else {
			eb.pendingOutboxByID[eventID] = operations
		}
		return
	}
}

func (eb *EventBus) clearPendingOutboxOperation(ctx context.Context, eventID string) error {
	if eb == nil {
		return nil
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
	}
	eb.mu.Lock()
	operations := eb.pendingOutboxByID[strings.TrimSpace(eventID)]
	delete(eb.pendingOutboxByID, strings.TrimSpace(eventID))
	eb.mu.Unlock()
	for _, operation := range operations {
		err = errors.Join(err, operation.publicationClaim.Release(ctx))
	}
	return err
}
