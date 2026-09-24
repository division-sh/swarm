package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type engineDispatcher struct {
	bus *EventBus
}

type pendingOutboxOperation struct {
	sequence         uint64
	intent           runtimeengine.EmitIntent
	source           events.Event
	outcome          EventAppendOutcome
	publicationClaim *pipelinePublicationClaim
	deliveryHandoffs []runtimedelivery.DurableHandoffProof
	finalizationErr  error
}

type pendingOutboxDispatch struct {
	handled                     bool
	deliveryHandoffsTransferred bool
}

// EnginePublicationPlan is immutable publication data prepared before the
// selected-store engine mutation begins. The private store adapter can inspect
// the closed command but cannot invoke EventBus or acquire runtime authority.
type EnginePublicationPlan struct {
	prepared       PreparedPublish
	command        PublicationCommand
	intent         runtimeengine.EmitIntent
	admittedSource events.AdmittedEvent
}

func (p EnginePublicationPlan) DurablePublicationEventID() string {
	return strings.TrimSpace(p.command.Commit.Event.ID())
}

func (p EnginePublicationPlan) ValidateDurablePublicationPlan() error {
	command := p.command
	command.prospective = runtimepipeline.PreparedWorkflowPublicationState{}
	if err := command.ValidateFanOut(); err != nil {
		return err
	}
	if p.prepared.Event.ID() != p.DurablePublicationEventID() || p.intent.Event.ID() != p.DurablePublicationEventID() {
		return fmt.Errorf("engine publication plan event identity is inconsistent")
	}
	return nil
}

func (p EnginePublicationPlan) PublicationCommand() PublicationCommand { return p.command }

// ValidatePreparedFanOutEvent binds the canonical planner's input to its output.
// Routing may project receiver facts, but cannot substitute the admitted source.
func (p EnginePublicationPlan) ValidatePreparedFanOutEvent(original events.Event) error {
	if err := p.ValidateDurablePublicationPlan(); err != nil {
		return err
	}
	want, err := events.IntegrityProjection(original)
	if err != nil {
		return err
	}
	actual, err := events.IntegrityProjection(p.admittedSource.Event())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, want) {
		return errors.New("prepared fan-out publication substitutes its admitted source")
	}
	return nil
}

// PublicationCommandForMutation discharges prospective ownership only for the
// exact state record that the named engine transaction has compare-and-written.
func (p EnginePublicationPlan) PublicationCommandForMutation(state runtimepipeline.WorkflowEngineStateRecord, lifecycle runtimepipeline.WorkflowLifecycleMutationPlan) (PublicationCommand, error) {
	command := p.command
	if !command.prospective.Empty() {
		if err := command.prospective.ValidateMutation(state, lifecycle); err != nil {
			return PublicationCommand{}, err
		}
		command.prospective = runtimepipeline.PreparedWorkflowPublicationState{}
	}
	return command, nil
}

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
	return eb.prepareEnginePublications(ctx, intents, runtimepipeline.PreparedWorkflowPublicationState{})
}

func (eb *EventBus) PrepareEngineMutationPublications(ctx context.Context, intents []runtimeengine.EmitIntent, prospective runtimepipeline.PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error) {
	if prospective.Empty() {
		return nil, fmt.Errorf("engine mutation publication requires prospective receiver state")
	}
	return eb.prepareEnginePublications(ctx, intents, prospective)
}

func (eb *EventBus) prepareEnginePublications(ctx context.Context, intents []runtimeengine.EmitIntent, prospective runtimepipeline.PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error) {
	return eb.prepareEnginePublicationsWithMember(ctx, intents, prospective, nil)
}

func (eb *EventBus) prepareEnginePublicationsWithMember(ctx context.Context, intents []runtimeengine.EmitIntent, prospective runtimepipeline.PreparedWorkflowPublicationState, member *fanOutPublicationMember) ([]runtimeengine.DurablePublicationPlan, error) {
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
		preparedCtx := intentCtx
		var admitted events.AdmittedEvent
		var err error
		if member != nil {
			if member.claim.EventID() == "" || member.context == nil {
				release()
				return nil, errors.New("fan-out preparation requires exact admitted member evidence")
			}
			preparedCtx, admitted = member.context, member.admitted
		} else {
			preparedCtx, admitted, err = eb.admitEnginePublishEvent(intentCtx, intent.Event)
		}
		if err != nil {
			release()
			return nil, err
		}
		intent.Event = admitted.Event()
		if !prospective.Empty() {
			if err := prospective.ValidatePublication(eb.sourceArtifactFact, intent.Event); err != nil {
				release()
				return nil, err
			}
		}
		publication := eventBusCommitPublishPlan{bus: eb, event: intent.Event, admitted: admitted, prospective: prospective}
		if member != nil {
			publication.outputConsumers = member.outputConsumers
			claim := member.claim
			if claim.EventID() != intent.Event.ID() {
				release()
				return nil, errors.New("fan-out preparation lacks its exact pre-admitted claim")
			}
			publication.publicationClaim = &pipelinePublicationClaim{bus: eb, eventID: intent.Event.ID(), claim: claim}
		}
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
		command.prospective = prospective
		plan := EnginePublicationPlan{prepared: prepared, command: command, intent: intent, admittedSource: admitted}
		if err := plan.ValidateDurablePublicationPlan(); err != nil {
			_ = prepared.publicationClaim.Release(context.WithoutCancel(preparedCtx))
			release()
			return nil, err
		}
		if member != nil {
			if err := member.group.RecordPrepared(preparedCtx, prepared.publicationClaim.Claim(), plan); err != nil {
				err = errors.Join(err, prepared.publicationClaim.Release(context.WithoutCancel(preparedCtx)))
				release()
				return nil, err
			}
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
	var result error
	for _, value := range evidence {
		committed, ok := value.(CommittedEnginePublication)
		if !ok {
			result = errors.Join(result, fmt.Errorf("committed engine publication has unexpected type %T", value))
			continue
		}
		result = errors.Join(result, eb.finalizeOneEnginePublication(ctx, committed))
	}
	return result
}

func (eb *EventBus) finalizeOneEnginePublication(ctx context.Context, committed CommittedEnginePublication) (err error) {
	claim := committed.plan.prepared.publicationClaim
	staged := false
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("finalize committed publication %s panic: %v", committed.CommittedDurablePublicationEventID(), recovered))
		}
		if err != nil && !staged && claim != nil {
			err = errors.Join(err, claim.Release(context.WithoutCancel(ctx)))
		}
	}()
	if err := committed.ValidateCommittedDurablePublication(); err != nil {
		return err
	}
	consequences, err := eb.finalizeCommittedPublicationConsequences(ctx, committed.plan.prepared, committed.committed, true)
	if consequences.prerequisiteErr != nil {
		eb.stageCommittedOutboxOperationWithFinalization(committed.plan.intent, committed.plan.admittedSource.Event(), committed.committed.AppendOutcome, claim, committed.committed.DeliveryHandoffs, consequences.prerequisiteErr)
		staged = true
		return errors.Join(err, claim.Release(context.WithoutCancel(ctx)))
	}
	if !consequences.bound {
		return err
	}
	if !consequences.ready {
		blockErr := errors.Join(err, errors.New("committed publication prerequisites did not finish"))
		eb.stageCommittedOutboxOperationWithFinalization(committed.plan.intent, committed.plan.admittedSource.Event(), committed.committed.AppendOutcome, claim, committed.committed.DeliveryHandoffs, blockErr)
		staged = true
		return errors.Join(blockErr, claim.Release(context.WithoutCancel(ctx)))
	}
	eb.stageCommittedOutboxOperationWithFinalization(committed.plan.intent, committed.plan.admittedSource.Event(), committed.committed.AppendOutcome, claim, committed.committed.DeliveryHandoffs, nil)
	staged = true
	return err
}

type committedPublicationConsequences struct {
	prepared        PreparedPublish
	prerequisiteErr error
	bound           bool
	ready           bool
}

func (eb *EventBus) finalizeCommittedPublicationConsequences(ctx context.Context, prepared PreparedPublish, committed CommittedPublication, stageEngineInternalDelivery bool) (result committedPublicationConsequences, err error) {
	result.prepared = prepared
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("finalize committed publication %s panic: %v", prepared.Event.ID(), recovered))
			if result.bound && !result.ready {
				result.prerequisiteErr = errors.Join(result.prerequisiteErr, err)
			}
		}
	}()
	if err := committed.Validate(); err != nil {
		return result, err
	}
	result.prepared, err = prepared.WithCommitOutcome(committed.AppendOutcome)
	if err != nil {
		return result, err
	}
	result.bound = true
	result.prepared.committedHandoffs = append([]runtimedelivery.DurableHandoffProof(nil), committed.DeliveryHandoffs...)
	activationErr := eb.finalizeCommittedFlowInstanceActivations(ctx, committed.Activations)
	readinessErr := eb.finalizeEngineAgentReadiness(ctx, result.prepared.Event, result.prepared.plan.DeliveryRoutes())
	var internalErr error
	if stageEngineInternalDelivery {
		internalErr = eb.finalizeEngineInternalDelivery(result.prepared.Event.ID(), result.prepared.plan.InternalRecipientIDs())
	}
	result.prerequisiteErr = errors.Join(activationErr, readinessErr, internalErr)
	if result.prerequisiteErr != nil {
		return result, result.prerequisiteErr
	}
	result.ready = true
	if eb.testLifecycleProbe != nil && !result.prepared.exactDuplicate {
		eb.notifyTestPublishPersisted(ctx, result.prepared.Event, result.prepared.plan)
	}
	return result, nil
}

func (eb *EventBus) finalizeEngineInternalDelivery(eventID string, recipients []string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("finalize committed internal delivery for %s panic: %v", eventID, recovered))
		}
	}()
	eb.setPendingInternalDelivery(eventID, recipients)
	return nil
}

func (eb *EventBus) finalizeEngineAgentReadiness(ctx context.Context, event events.Event, routes []events.DeliveryRoute) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("finalize committed agent readiness for %s panic: %v", event.ID(), recovered))
		}
	}()
	return eb.finalizeCommittedAgentReadiness(ctx, event, routes)
}

func (eb *EventBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	if eb == nil {
		return nil
	}
	return engineDispatcher{bus: eb}
}

func (d engineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) (err error) {
	if d.bus == nil || len(intents) == 0 {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("dispatch committed publication batch panic: %v", recovered), d.releaseUndispatchedPostCommit(context.WithoutCancel(ctx), intents))
		}
	}()
	if err := d.flushCommittedPublicationPredecessor(ctx, intents); err != nil {
		return err
	}
	ctx, lease, err := d.bus.beginRuntimeWork(ctx)
	if err != nil {
		if errors.Is(err, worklifetime.ErrAdmissionFenced) || errors.Is(err, worklifetime.ErrRetired) {
			return errors.Join(err, d.releaseUndispatchedPostCommit(context.WithoutCancel(ctx), intents))
		}
		return err
	}
	if lease != nil {
		defer func() { _ = lease.Done() }()
	}
	var dispatchErr error
	for _, intent := range intents {
		dispatchErr = errors.Join(dispatchErr, d.dispatchOnePostCommit(ctx, intent))
	}
	return dispatchErr
}

func (d engineDispatcher) releaseUndispatchedPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	var result error
	for _, intent := range intents {
		if strings.TrimSpace(string(intent.Event.Type())) != "" {
			result = errors.Join(result, d.releaseUnadmittedPostCommit(context.WithoutCancel(ctx), intent.Event))
		}
	}
	return result
}

func (d engineDispatcher) flushCommittedPublicationPredecessor(ctx context.Context, intents []runtimeengine.EmitIntent) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("committed publication predecessor panic: %v", recovered))
		}
		if err != nil {
			err = errors.Join(err, d.releaseUndispatchedPostCommit(context.WithoutCancel(ctx), intents))
		}
	}()
	return flushEnclosingPublicationSettlement(ctx)
}

func (d engineDispatcher) dispatchOnePostCommit(ctx context.Context, intent runtimeengine.EmitIntent) (err error) {
	if strings.TrimSpace(string(intent.Event.Type())) == "" {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("dispatch committed publication %s panic: %v", intent.Event.ID(), recovered), d.releaseUnadmittedPostCommit(context.WithoutCancel(ctx), intent.Event))
		}
	}()
	var admitted events.AdmittedEvent
	if intent.Event.AdmissionClass() == events.EventAdmissionInheritedFanOut {
		// Dispatch requires the staged operation or exact durable readback below.
		admitted, err = events.RevalidatePersistedEvent(intent.Event)
	} else {
		_, admitted, err = admitEventForPublish(ctx, intent.Event, time.Now().UTC())
	}
	if err != nil {
		if ctx.Err() != nil {
			err = errors.Join(err, d.releaseUnadmittedPostCommit(context.WithoutCancel(ctx), intent.Event))
		}
		return err
	}
	intent.Event = admitted.Event()
	result, err := d.dispatchPendingOutboxOperation(ctx, intent)
	if err != nil || result.handled {
		return err
	}
	if intent.Event.AdmissionClass() == events.EventAdmissionInheritedFanOut {
		if err := d.bus.requireCommittedInheritedFanOut(ctx, intent.Event); err != nil {
			return err
		}
	}
	return d.dispatchAndRecord(ctx, intent, nil)
}

func (d engineDispatcher) releaseUnadmittedPostCommit(ctx context.Context, event events.Event) error {
	operation, ok, err := d.bus.takeMatchingPendingOutboxOperation(event)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return operation.publicationClaim.Release(ctx)
}

func (eb *EventBus) takeMatchingPendingOutboxOperation(event events.Event) (pendingOutboxOperation, bool, error) {
	want, err := events.IntegrityProjection(event)
	if err != nil {
		return pendingOutboxOperation{}, false, err
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	operations := eb.pendingOutboxByID[strings.TrimSpace(event.ID())]
	if len(operations) == 0 {
		return pendingOutboxOperation{}, false, nil
	}
	actual, err := events.IntegrityProjection(operations[0].source)
	if err != nil {
		return pendingOutboxOperation{}, false, err
	}
	if !reflect.DeepEqual(actual, want) {
		projected, err := events.IntegrityProjection(operations[0].intent.Event)
		if err != nil {
			return pendingOutboxOperation{}, false, err
		}
		if !reflect.DeepEqual(projected, want) {
			return pendingOutboxOperation{}, false, events.ErrEventIdentityConflict
		}
	}
	if operations[0].finalizationErr != nil {
		return operations[0], true, nil
	}
	operation := operations[0]
	if len(operations) == 1 {
		delete(eb.pendingOutboxByID, strings.TrimSpace(event.ID()))
	} else {
		eb.pendingOutboxByID[strings.TrimSpace(event.ID())] = operations[1:]
	}
	return operation, true, nil
}

// A repeated post-commit callback has no process-local operation to take. It
// must prove the immutable occurrence through canonical readback before the
// existing persisted-obligation recovery path can classify it (including an
// already-terminal no-op). This never grants generic publication permission.
func (eb *EventBus) requireCommittedInheritedFanOut(ctx context.Context, event events.Event) error {
	reader, ok := eb.store.(PreparedPublishEventReader)
	if !ok {
		return fmt.Errorf("inherited fan-out dispatch requires canonical committed-event readback")
	}
	prepared, found, err := loadValidatedPreparedPublishEvent(ctx, reader, event.ID())
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("inherited fan-out dispatch requires exact committed chunk evidence")
	}
	actual, err := events.IntegrityProjection(event)
	if err != nil {
		return err
	}
	want, err := events.IntegrityProjection(prepared.Event.Event())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, want) {
		return events.ErrEventIdentityConflict
	}
	return nil
}

// dispatchCommittedInterceptorPublications consumes only exact post-commit
// operations staged by the selected-store mutation that ran the interceptor.
// A continuation must never reinterpret a missing operation as permission to
// append or dispatch a fresh event.
func (d engineDispatcher) dispatchCommittedInterceptorPublications(ctx context.Context, events []events.Event) error {
	intents := make([]runtimeengine.EmitIntent, 0, len(events))
	for _, event := range events {
		intents = append(intents, runtimeengine.EmitIntent{Event: event, Context: event.DeliveryContext()})
	}
	if len(intents) > 0 {
		if err := d.flushCommittedPublicationPredecessor(ctx, intents); err != nil {
			return err
		}
	}
	var dispatchErr error
	for _, intent := range intents {
		result, err := d.dispatchPendingOutboxOperation(ctx, intent)
		if err != nil {
			if result.deliveryHandoffsTransferred && onlyAuthoritativeDeliveryIncomplete(err) {
				continue
			}
			dispatchErr = errors.Join(dispatchErr, err)
			continue
		}
		if !result.handled {
			dispatchErr = errors.Join(dispatchErr, fmt.Errorf("deferred interceptor publication %s has no committed post-commit operation", strings.TrimSpace(intent.Event.ID())))
		}
	}
	return dispatchErr
}

// A transferred handoff owns incomplete live delivery, but never an independent
// error joined by claim release, receiver cleanup, or another interceptor.
func onlyAuthoritativeDeliveryIncomplete(err error) bool {
	if err == errAuthoritativeDeliveryIncomplete {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyAuthoritativeDeliveryIncomplete(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyAuthoritativeDeliveryIncomplete(wrapped.Unwrap())
	}
	return false
}

func (d engineDispatcher) dispatchPendingOutboxOperation(ctx context.Context, fallback runtimeengine.EmitIntent) (result pendingOutboxDispatch, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("dispatch committed publication %s panic: %v", fallback.Event.ID(), recovered))
			if !result.handled {
				err = errors.Join(err, d.releaseUnadmittedPostCommit(context.WithoutCancel(ctx), fallback.Event))
			}
		}
	}()
	ctx, err = d.bus.admitSourceArtifactFact(ctx)
	if err != nil {
		return result, err
	}
	operation, ok, err := d.bus.takeMatchingPendingOutboxOperation(fallback.Event)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, nil
	}
	result.handled = true
	defer func() {
		err = errors.Join(err, operation.publicationClaim.Release(context.WithoutCancel(ctx)))
	}()
	if operation.finalizationErr != nil {
		return result, fmt.Errorf("committed publication %s has incomplete prerequisites: %w", fallback.Event.ID(), operation.finalizationErr)
	}
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
	ctx, err = d.bus.admitSourceArtifactFact(ctx)
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
		outcome, settleErr := d.bus.settlePipelineObligationOutcome(ctx, recoveryClaim, disposition)
		claimOpen = !outcome.Committed()
		return settleErr
	}
	disposition, completed, err := d.dispatchIntentDisposition(ctx, intent)
	if !completed {
		return err
	}
	return errors.Join(err, settle(disposition))
}

// A completed dispatch disposition is not a persisted acknowledgement. The
// enclosing claim owner decides when to commit it and retains ownership until
// that write and its required handoff have completed.
func (d engineDispatcher) dispatchIntentDisposition(ctx context.Context, intent runtimeengine.EmitIntent) (runtimepipelineobligation.Disposition, bool, error) {
	return d.dispatchIntentDispositionWithBoundary(ctx, intent, nil)
}

func (d engineDispatcher) dispatchIntentDispositionWithBoundary(ctx context.Context, intent runtimeengine.EmitIntent, boundary publicationSettlementBoundary) (runtimepipelineobligation.Disposition, bool, error) {
	queued, outcome, err := d.dispatchIntentWithBoundary(ctx, intent, boundary)
	decision := classifyPipelineDispatch(outcome, err, queued, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal)
	if decision.action != pipelineDispatchSettle {
		return runtimepipelineobligation.Disposition{}, false, err
	}
	return decision.disposition, true, err
}

func clonePostCommitPublish(evt events.Event) events.Event {
	return evt.Clone()
}

func (d engineDispatcher) dispatchIntent(ctx context.Context, intent runtimeengine.EmitIntent) (queued bool, result runtimepipelineobligation.ExecutionOutcome, err error) {
	return d.dispatchIntentWithBoundary(ctx, intent, nil)
}

func (d engineDispatcher) dispatchIntentWithBoundary(ctx context.Context, intent runtimeengine.EmitIntent, boundary publicationSettlementBoundary) (queued bool, result runtimepipelineobligation.ExecutionOutcome, err error) {
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
	receiverCtx, closeReceiver, err := d.bus.beginReceiverDispatch(ctx, projection, intent.Event)
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	defer func() { err = errors.Join(err, closeReceiver()) }()
	ctx = receiverCtx.Context
	ctx, dispatchScope, closeDispatch, err := d.bus.beginDeliveryDispatch(ctx, intent.Event, deliveryRoutes)
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	if dispatchScope != nil {
		dispatchScope.publicationSettlement = boundary
	}
	defer func() { err = errors.Join(err, closeDispatch()) }()
	nodePassthrough := true
	if intent.Recipients == nil {
		var interception deliveryRouteInterception
		interception, err = d.bus.runInterceptorsForDeliveryRoutes(ctx, intent.Event, deliveryRoutes)
		if err != nil && !interception.Outcome.Committed && interception.Outcome.ContinueDispatch() {
			for _, deferred := range interception.Deferred {
				err = errors.Join(err, d.bus.publishDeferred(ctx, deferred))
			}
			return false, runtimepipelineobligation.Continue(), err
		}
		result = interception.Outcome
		postCommitErr := err
		defer func() { err = errors.Join(postCommitErr, err) }()
		if !interception.Outcome.ContinueDispatch() {
			for _, next := range interception.Deferred {
				postCommitErr = errors.Join(postCommitErr, d.bus.publishDeferred(ctx, next))
			}
			return false, interception.Outcome, nil
		}
		var deferredErr error
		for _, next := range interception.Deferred {
			deferredErr = errors.Join(deferredErr, d.bus.publishDeferred(ctx, next))
		}
		if deferredErr != nil {
			return false, runtimepipelineobligation.Continue(), deferredErr
		}
		if !interception.EventPassthrough {
			d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
			return false, result, nil
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
			return false, result, nil
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
		return false, result, nil
	}
	dispatch, err := d.bus.deliverToRecipientsWithRoutes(ctx, intent.Event, liveRecipients, deliveryRoutes)
	if err != nil {
		return false, runtimepipelineobligation.Continue(), err
	}
	d.bus.clearPendingInternalDeliveryRoutes(intent.Event.ID())
	if len(dispatch.delivered) == 0 {
		return false, runtimepipelineobligation.Continue(), nil
	}
	d.bus.logRuntime(ctx, "debug", "Persisted event intent was delivered", "eventbus", "delivered", intent.Event.ID(), string(intent.Event.Type()), "", intent.Event.EntityID(), "", nil, map[string]any{
		"direct":                     true,
		"delivery_manifest_owner":    "event_deliveries+in_memory_internal",
		"recipients_count":           len(dispatch.delivered),
		"already_owned_count":        len(dispatch.alreadyOwned),
		"parent_event_id":            intent.Event.ParentEventID(),
		"requested_recipients":       append([]string(nil), liveRecipients...),
		"requested_recipients_count": len(liveRecipients),
		"persisted_recipients":       append([]string(nil), recipients...),
		"internal_recipients":        append([]string(nil), internalRecipients...),
	}, nil, 0)
	return false, result, nil
}

func (eb *EventBus) deliveryRoutesForPostCommitIntent(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	prepared, err := eb.preparedEventForReplay(ctx, eventID)
	if err != nil {
		return nil, err
	}
	return prepared.DeliveryRoutes, nil
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
	eb.stageCommittedOutboxOperationWithFinalization(intent, intent.Event, outcome, publicationClaim, handoffs, nil)
}

func (eb *EventBus) stageCommittedOutboxOperationWithFinalization(intent runtimeengine.EmitIntent, source events.Event, outcome EventAppendOutcome, publicationClaim *pipelinePublicationClaim, handoffs []runtimedelivery.DurableHandoffProof, finalizationErr error) {
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
		sequence: eb.pendingOutboxSequence, intent: intent, source: source.Clone(), outcome: outcome, publicationClaim: publicationClaim,
		deliveryHandoffs: append([]runtimedelivery.DurableHandoffProof(nil), handoffs...),
		finalizationErr:  finalizationErr,
	})
	eb.mu.Unlock()
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
	ctx, err = eb.admitSourceArtifactFact(ctx)
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
