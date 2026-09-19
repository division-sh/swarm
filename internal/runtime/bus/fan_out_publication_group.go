package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type fanOutPublicationMember struct {
	group    runtimepipelineobligation.PublicationGroup
	claim    runtimepipelineobligation.Claim
	context  context.Context
	admitted events.AdmittedEvent
}

func (eb *EventBus) PrepareFanOutPublications(ctx context.Context, group runtimepipelineobligation.PublicationGroup, requests []runtimepipeline.FanOutPublicationRequest) ([]runtimepipeline.FanOutPublicationPreparation, error) {
	if group == nil {
		return nil, errors.New("fan-out publication group is required")
	}
	if len(requests) > fanoutobligation.MaxChunkSize {
		return nil, fmt.Errorf("fan-out publication preparation exceeds %d members", fanoutobligation.MaxChunkSize)
	}
	if err := flushEnclosingPublicationSettlement(ctx); err != nil {
		return nil, err
	}
	results := make([]runtimepipeline.FanOutPublicationPreparation, len(requests))
	members := make([]fanOutPublicationMember, len(requests))
	claims := make([]runtimepipelineobligation.PublicationClaimRequest, 0, len(requests))
	accepted := make([]int, 0, len(requests))
	seen := make(map[int]bool, len(requests))
	for i, request := range requests {
		if request.Ordinal < 0 || seen[request.Ordinal] {
			return nil, errors.New("fan-out publication requests require distinct nonnegative ordinals")
		}
		seen[request.Ordinal] = true
		results[i].Ordinal = request.Ordinal
		preparedCtx, admitted, err := eb.admitEnginePublishEvent(events.WithDeliveryContext(ctx, request.Intent.Context), request.Intent.Event)
		if err != nil {
			results[i].Err = err
			continue
		}
		members[i] = fanOutPublicationMember{group: group, context: preparedCtx, admitted: admitted}
		claims = append(claims, runtimepipelineobligation.PublicationClaimRequest{Ordinal: request.Ordinal, Event: admitted.Event()})
		accepted = append(accepted, i)
	}
	issued, err := group.ClaimBatch(ctx, claims)
	if err != nil {
		return nil, err
	}
	if len(issued) != len(claims) {
		return nil, errors.New("publication claim batch omitted an exact member")
	}
	for j, i := range accepted {
		if issued[j].EventID() != claims[j].Event.ID() {
			return nil, errors.New("publication claim batch substitutes event identity")
		}
		members[i].claim = issued[j]
		plans, err := eb.prepareEnginePublicationsWithMember(ctx, []runtimeengine.EmitIntent{requests[i].Intent}, runtimepipeline.PreparedWorkflowPublicationState{}, &members[i])
		if err == nil && len(plans) != 1 {
			err = errors.Join(errors.New("fan-out publication requires exactly one plan"), eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
		}
		if err != nil {
			results[i].Err = err
		} else {
			results[i].Publication = plans[0]
		}
	}
	return results, nil
}

func (eb *EventBus) PrepareFanOutPublication(ctx context.Context, group runtimepipelineobligation.PublicationGroup, ordinal int, intent runtimeengine.EmitIntent) (runtimeengine.DurablePublicationPlan, error) {
	results, err := eb.PrepareFanOutPublications(ctx, group, []runtimepipeline.FanOutPublicationRequest{{Ordinal: ordinal, Intent: intent}})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("fan-out publication requires exactly one result")
	}
	return results[0].Publication, results[0].Err
}

func (eb *EventBus) SealFanOutPublications(ctx context.Context, group runtimepipelineobligation.PublicationGroup, end int, plans []runtimeengine.DurablePublicationPlan) error {
	if group == nil {
		return errors.New("fan-out publication group is required")
	}
	claims := make([]runtimepipelineobligation.Claim, 0, len(plans))
	for _, value := range plans {
		plan, ok := value.(EnginePublicationPlan)
		if !ok || plan.prepared.publicationClaim == nil {
			return errors.New("fan-out seal requires canonical publication plans")
		}
		if err := plan.ValidateDurablePublicationPlan(); err != nil {
			return err
		}
		claims = append(claims, plan.prepared.publicationClaim.Claim())
	}
	return group.Seal(ctx, end, claims)
}

func (eb *EventBus) validateCommittedFanOutPublications(ctx context.Context, group runtimepipelineobligation.PublicationGroup, values []runtimeengine.CommittedDurablePublication) (func(), error) {
	if group == nil {
		return nil, errors.New("fan-out publication group is required")
	}
	claims := make([]runtimepipelineobligation.Claim, 0, len(values))
	for _, value := range values {
		publication, ok := value.(CommittedEnginePublication)
		if !ok || publication.plan.prepared.publicationClaim == nil || publication.plan.prepared.publicationClaim.bus != eb {
			return nil, errors.New("fan-out committed evidence requires exact bus-owned publication claims")
		}
		if err := publication.ValidateCommittedDurablePublication(); err != nil {
			return nil, err
		}
		claims = append(claims, publication.plan.prepared.publicationClaim.Claim())
	}
	if err := group.ValidateCommittedMembership(claims); err != nil {
		return nil, err
	}
	// Exact membership permits callback retirement even if execution admission
	// fails or has already closed. It never permits mutation or dispatch.
	retire := func() { eb.retireFanOutOutboxOperations(values) }
	return retire, group.ValidateCommitted(ctx, claims)
}

func (eb *EventBus) FinalizeFanOutPublications(ctx context.Context, group runtimepipelineobligation.PublicationGroup, values []runtimeengine.CommittedDurablePublication) (err error) {
	retire, err := eb.validateCommittedFanOutPublications(ctx, group, values)
	defer func() {
		if err != nil && retire != nil {
			retire()
		}
	}()
	if err != nil {
		return err
	}
	return eb.FinalizeEnginePublications(ctx, values)
}

// DispatchFanOutPublications consumes only the exact committed range. Its
// collector never treats a returned dispatch disposition as an acknowledgement.
func (eb *EventBus) DispatchFanOutPublications(ctx context.Context, group runtimepipelineobligation.PublicationGroup, values []runtimeengine.CommittedDurablePublication) (err error) {
	if eb == nil || group == nil {
		return errors.New("fan-out dispatch requires its bus and publication group")
	}
	retire, err := eb.validateCommittedFanOutPublications(ctx, group, values)
	defer func() {
		if err != nil && retire != nil {
			retire()
		}
	}()
	if err != nil {
		return err
	}
	if err := flushEnclosingPublicationSettlement(ctx); err != nil {
		return err
	}
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer func() { err = errors.Join(err, lease.Done()) }()
	}
	settlement := &fanOutPublicationSettlement{ctx: ctx, bus: eb, group: group}
	defer func() { err = errors.Join(err, settlement.finish()) }()
	dispatcher := engineDispatcher{bus: eb}
	for _, value := range values {
		committed, ok := value.(CommittedEnginePublication)
		if !ok {
			return fmt.Errorf("fan-out dispatch has noncanonical committed publication %T", value)
		}
		if err := committed.ValidateCommittedDurablePublication(); err != nil {
			return err
		}
		operation, found, takeErr := eb.takeFanOutOutboxOperation(committed)
		if takeErr != nil {
			return takeErr
		}
		if !found {
			// Missing process-local state is handled by the same exact recovery
			// path as an ordinary repeated postcommit callback, never re-minted.
			if err := settlement.flushBeforeNestedPublication(); err != nil {
				return err
			}
			if err := dispatcher.DispatchPostCommit(ctx, []runtimeengine.EmitIntent{committed.CommittedDurablePublicationIntent()}); err != nil {
				return err
			}
			continue
		}
		if err := dispatcher.dispatchFanOutOperation(ctx, operation, settlement); err != nil {
			return err
		}
		if eb.testLifecycleProbe != nil {
			eb.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
				Kind:    runtimelifecycleprobe.PostCommitDispatchCompleted,
				EventID: operation.intent.Event.ID(), EventType: string(operation.intent.Event.Type()),
				Status: "fan_out_group_member_returned",
			})
		}
	}
	return nil
}

// The caller has admitted the exact committed group. Its closing claims must
// not survive as executable process-local callbacks; durable recovery owns any
// unfinished deliveries. A newer operation for the same event stays untouched.
func (eb *EventBus) retireFanOutOutboxOperations(values []runtimeengine.CommittedDurablePublication) {
	claims := make([]*pipelinePublicationClaim, 0, len(values))
	eb.mu.Lock()
	for _, value := range values {
		publication, ok := value.(CommittedEnginePublication)
		if !ok || publication.plan.prepared.publicationClaim == nil || publication.plan.prepared.publicationClaim.bus != eb {
			continue
		}
		claims = append(claims, publication.plan.prepared.publicationClaim)
		id := publication.CommittedDurablePublicationEventID()
		operations := eb.pendingOutboxByID[id]
		retained := operations[:0]
		for _, operation := range operations {
			if operation.publicationClaim != publication.plan.prepared.publicationClaim {
				retained = append(retained, operation)
			}
		}
		clear(operations[len(retained):])
		if len(retained) == 0 {
			delete(eb.pendingOutboxByID, id)
		} else {
			eb.pendingOutboxByID[id] = retained
		}
	}
	eb.mu.Unlock()
	// Reset can already hold a snapshot of these exact callback pointers. Retire
	// their local authority too, outside the bus lock, before the group closes.
	for _, claim := range claims {
		claim.retireToPublicationGroup()
	}
}

func (eb *EventBus) takeFanOutOutboxOperation(committed CommittedEnginePublication) (pendingOutboxOperation, bool, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	id := committed.CommittedDurablePublicationEventID()
	operations := eb.pendingOutboxByID[id]
	if len(operations) == 0 {
		return pendingOutboxOperation{}, false, nil
	}
	operation := operations[0]
	actual, actualErr := events.IntegrityProjection(operation.intent.Event)
	want, wantErr := events.IntegrityProjection(committed.plan.intent.Event)
	if err := errors.Join(actualErr, wantErr); err != nil {
		return pendingOutboxOperation{}, false, err
	}
	if operation.publicationClaim == nil || operation.publicationClaim != committed.plan.prepared.publicationClaim ||
		!reflect.DeepEqual(actual, want) ||
		!reflect.DeepEqual(operation.intent.Context, committed.plan.intent.Context) ||
		operation.outcome != committed.committed.AppendOutcome {
		return pendingOutboxOperation{}, false, errors.New("fan-out pending operation differs from exact committed publication")
	}
	if len(operations) == 1 {
		delete(eb.pendingOutboxByID, id)
	} else {
		eb.pendingOutboxByID[id] = operations[1:]
	}
	return operation, true, nil
}

func (d engineDispatcher) dispatchFanOutOperation(ctx context.Context, operation pendingOutboxOperation, settlement *fanOutPublicationSettlement) (err error) {
	claim := operation.publicationClaim
	retained := false
	defer func() {
		if !retained {
			err = errors.Join(err, claim.Release(context.WithoutCancel(ctx)))
		}
	}()
	if operation.outcome == EventAppendExactDuplicate {
		return nil
	}
	if operation.outcome != EventAppendInserted {
		return errors.New("fan-out dispatch requires an exact append outcome")
	}
	if err := d.bus.AcceptCommittedDeliveryHandoffs(operation.deliveryHandoffs); err != nil {
		return err
	}
	disposition, completed, dispatchErr := d.dispatchIntentDispositionWithBoundary(ctx, operation.intent, settlement)
	if !completed {
		return errors.Join(dispatchErr, settlement.flushBeforeNestedPublication())
	}
	if disposition.Kind() == runtimepipelineobligation.DispositionDeferred {
		if err := settlement.flushBeforeNestedPublication(); err != nil {
			return errors.Join(dispatchErr, err)
		}
		return errors.Join(dispatchErr, claim.Settle(ctx, disposition))
	}
	if err := settlement.collect(claim, disposition); err != nil {
		return errors.Join(dispatchErr, err)
	}
	retained = true
	return dispatchErr
}

type fanOutPublicationSettlement struct {
	mu      sync.Mutex
	ctx     context.Context
	bus     *EventBus
	group   runtimepipelineobligation.PublicationGroup
	members []runtimepipelineobligation.PublicationSettlementMember
	claims  []*pipelinePublicationClaim
	failure error
	closed  bool
}

func (s *fanOutPublicationSettlement) collect(claim *pipelinePublicationClaim, disposition runtimepipelineobligation.Disposition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failure != nil {
		return errors.Join(errors.New("fan-out settlement collection is closed"), s.failure)
	}
	if claim == nil || claim.bus != s.bus || claim.retired.Load() || claim.released.Load() {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := disposition.ValidateFor(claim.claim.Purpose()); err != nil {
		return err
	}
	if disposition.Kind() == runtimepipelineobligation.DispositionDeferred {
		return errors.New("durable deferred processing remains a singleton mutation")
	}
	for _, existing := range s.claims {
		if existing.claim == claim.claim {
			return errors.New("duplicate completed publication member")
		}
	}
	s.members = append(s.members, runtimepipelineobligation.PublicationSettlementMember{Claim: claim.claim, Disposition: disposition})
	s.claims = append(s.claims, claim)
	return nil
}

func (s *fanOutPublicationSettlement) flushBeforeNestedPublication() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *fanOutPublicationSettlement) flushLocked() error {
	if s.failure != nil {
		return s.failure
	}
	if len(s.members) == 0 {
		return nil
	}
	if s.closed {
		return errors.New("fan-out settlement is closed")
	}
	outcome, err := s.group.Settle(s.ctx, s.members)
	results := make(map[runtimepipelineobligation.Claim]runtimepipelineobligation.SettlementOutcome, len(outcome.Results))
	for _, result := range outcome.Results {
		if _, duplicate := results[result.Claim]; duplicate || !result.Outcome.Committed() {
			err = errors.Join(err, errors.New("invalid committed publication settlement result"))
		}
		results[result.Claim] = result.Outcome
	}
	handoff := false
	for _, claim := range s.claims {
		if result, found := results[claim.claim]; found && result.Committed() {
			claim.released.Store(true)
			handoff = handoff || result.DeliveryHandoffCommitted()
			delete(results, claim.claim)
		} else if err == nil {
			err = errors.New("publication settlement omitted an exact committed member")
		}
	}
	if len(results) != 0 {
		err = errors.Join(err, errors.New("publication settlement returned a foreign member"))
	}
	if handoff {
		s.bus.SignalDeliveryContinuations()
	}
	if err != nil {
		// Observation can diagnose uncertain satisfaction, but never becomes
		// a synthetic committed result or permission to retry this segment.
		observation, readErr := s.group.ReadPublicationSettlement(context.WithoutCancel(s.ctx), s.members)
		for _, row := range observation.Rows {
			if row.State == runtimepipelineobligation.PublicationSettlementConflict {
				readErr = errors.Join(readErr, fmt.Errorf("publication %s settlement conflict: %s", row.Claim.EventID(), row.Reason))
			}
		}
		s.failure = errors.Join(err, readErr)
		return s.failure
	}
	s.members = nil
	s.claims = nil
	return nil
}

func (s *fanOutPublicationSettlement) finish() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.flushLocked()
	s.closed = true
	return err
}
