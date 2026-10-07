package bus

import (
	"context"
	"errors"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (d engineDispatcher) DispatchCommittedPublication(ctx context.Context, value runtimeengine.CommittedDurablePublication) (err error) {
	if d.bus == nil {
		return errors.New("committed publication requires its event bus")
	}
	committed, ok := value.(CommittedEnginePublication)
	if !ok || committed.plan.prepared.publicationClaim == nil || committed.plan.prepared.publicationClaim.bus != d.bus {
		return errors.New("committed publication requires its exact bus-owned claim")
	}
	if err := committed.ValidateCommittedDurablePublication(); err != nil {
		return err
	}
	if !committed.committed.Acknowledged {
		return errors.New("publication handoff requires selected-store commit acknowledgment")
	}
	ctx, err = d.bus.admitSourceArtifactFact(context.WithoutCancel(ctx))
	if err != nil {
		return err
	}
	if err := flushEnclosingPublicationSettlement(ctx); err != nil {
		return errors.Join(err, committed.plan.prepared.publicationClaim.Release(ctx))
	}
	operation, found, err := d.bus.takeCommittedOutboxOperation(committed)
	if err != nil {
		return errors.Join(err, committed.plan.prepared.publicationClaim.Release(ctx))
	}
	if !found {
		return errors.New("committed publication handoff requires its finalized outbox operation")
	}
	defer func() { err = errors.Join(err, operation.publicationClaim.Release(ctx)) }()
	if operation.outcome == EventAppendExactDuplicate {
		return nil
	}
	if err := d.bus.validateContinuationPublication(operation, committed.plan.prepared.plan); err != nil {
		return err
	}
	if err := d.bus.AcceptCommittedDeliveryHandoffs(operation.deliveryHandoffs); err != nil {
		return err
	}
	// The same selected-store decision boundary used by finite fan-out makes
	// the exact routes executable. The coordinator owns their bounded workers,
	// independent receiver lifetimes, retry, restart recovery and shutdown join.
	if _, err := operation.publicationClaim.Settle(ctx, runtimepipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		return err
	}
	d.bus.clearPendingInternalDeliveryRoutes(operation.intent.Event.ID())
	return nil
}

func (eb *EventBus) validateContinuationPublication(operation pendingOutboxOperation, plan RoutePlan) error {
	if operation.outcome != EventAppendInserted || !plan.TargetFailure.Empty() {
		return errors.New("publication handoff requires accepted route evidence")
	}
	if len(plan.DeliveryRoutes()) != len(operation.deliveryHandoffs) || !nodeRoutesCoverLiveRecipients(plan.LiveRecipients, plan.DeliveryRoutes()) {
		return errors.New("publication handoff requires every live recipient's exact durable route")
	}
	eventInterceptors, _ := splitDeliveryRouteInterceptors(eb.interceptorsSnapshot())
	if len(eventInterceptors) != 0 {
		return errors.New("publication handoff cannot bypass event-wide coordination")
	}
	return nil
}
