package bus

import (
	"context"
	"errors"
	"fmt"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

func (eb *EventBus) beginPublicCompletion(ctx context.Context) (context.Context, *worklifetime.Lease, error) {
	ctx, err := eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return ctx, nil, err
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	owner := eb.workOwnerForContext(ctx)
	lease, err := worklifetime.BeginPublicCompletion(ctx, owner)
	if err != nil {
		return ctx, nil, err
	}
	// Native preparation/COMMIT still uses the original cancellable request.
	ctx = worklifetime.WithOccurrence(worklifetime.CarryAcceptedLease(ctx, lease.Context()), owner)
	process, _ := worklifetime.ProcessFromContext(lease.Context())
	ctx = worklifetime.WithProcess(ctx, process)
	ctx = context.WithValue(ctx, runtimeWorkAdmissionContextKey{}, runtimeWorkAdmission{owner: owner, context: ctx})
	return ctx, lease, nil
}

func (eb *EventBus) finishPublicPublication(ctx context.Context, lease *worklifetime.Lease, prepared PreparedPublish, committed CommittedPublication) {
	prepared.receiver.occurrence = eb.workOwnerForContext(ctx)
	claim, event := prepared.publicationClaim, prepared.Event
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("public publication consequence panic: %v", recovered))
		}
		err = errors.Join(err, claim.Release(ctx), lease.Done())
		eb.reportLocalDispatchFailure("api_event_publication_consequence_failed", event, err)
	}()
	prepared, ready, err := eb.applyCommittedPublication(ctx, prepared, committed)
	if !ready {
		return
	}
	err = errors.Join(err, eb.dispatchPublicPublication(ctx, prepared))
}

func (eb *EventBus) dispatchPublicPublication(ctx context.Context, prepared PreparedPublish) (err error) {
	if !eb.canTransferPreparedNodeDeliveries(prepared) {
		// This worker retains the reservation until the actual coordinated/mixed
		// dispatch disposition; a second asynchronous spawn would release it early.
		return eb.DispatchPreparedPublish(ctx, prepared)
	}
	dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)
	if err != nil {
		return err
	}
	receiver, closeReceiver, err := eb.beginReceiverDispatch(dispatchCtx, prepared.receiver, prepared.Event)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeReceiver()) }()
	eb.notifyTestPostCommitDispatchStarted(receiver.Context, prepared.Event)
	defer eb.notifyTestPostCommitDispatchCompleted(receiver.Context, prepared.Event)
	handoff, err := eb.transferPreparedNodeDeliveries(receiver.Context, prepared)
	if handoff.DeliveryHandoffCommitted() {
		err = errors.Join(err, eb.DeliveryContinuationOwner().DispatchPublished(prepared.Event, prepared.plan.DeliveryRoutes()))
	}
	eb.logPublished(receiver.Context, prepared.Event, 0)
	return err
}

func (eb *EventBus) publicCompletionContext(ctx context.Context, lease *worklifetime.Lease, runID string) context.Context {
	ctx = bindWorkContext(ctx, lease, eb.workOwnerForContext(lease.Context()))
	return runtimecorrelation.WithRunID(ctx, runID)
}

func (eb *EventBus) finishPublicDeploymentStart(ctx context.Context, lease *worklifetime.Lease, committed CommittedDeploymentRunCreation) {
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("deployment start consequence panic: %v", recovered))
		}
		if err != nil {
			err = errors.Join(err, eb.logRuntime(ctx, "error", "Committed deployment activation failed", "eventbus", "api_deployment_activation_failed", "", "", "", "", "", nil, nil,
				eventBusDependencyFailure(err, "api_deployment_activation_failed", "finalize_deployment_start"), 0))
		}
		if settleErr := lease.Done(); settleErr != nil {
			diaglog.ProcessLog(diaglog.LevelError, "eventbus", "Public deployment responsibility settlement failed", "error", settleErr.Error())
		}
	}()
	err = eb.finalizeCommittedFlowInstanceActivations(ctx, committed.Activations)
}
