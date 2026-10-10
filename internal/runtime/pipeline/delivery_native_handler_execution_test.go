package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Component execution returns the actual engine result while using the same
// admission, heartbeat and finalization owners as the production dispatcher.
func executeNativeClaimedPipelineHandlerForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler contracts.SystemNodeEventHandler, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	t.Helper()
	route, found := workflowNodeDeliveryRoute(ctx)
	if !found {
		t.Fatal("native component execution requires its exact published route")
	}
	admission, err := admitWorkflowNodeDelivery(ctx, trigger.Event, route, pc.deliveryRuntime, pc.deliveryStore, func(err error) { t.Error(err) })
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	if admission.handled {
		return contractHandlerExecutionResult{Handled: true, Outcome: &handlerExecutionOutcome{Status: HandlerOutcomeDiscarded}}, admission.postCommitErr
	}
	ctx = deliverylifecycle.WithClaim(ctx, admission.claim)
	heartbeat, err := deliverylifecycle.StartClaimHeartbeatFromClaim(ctx, pc.workOwner, pc.deliveryStore, admission.claim, admission.renewal)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	defer func() {
		if err := heartbeat.Stop(); err != nil {
			t.Error(err)
		}
	}()
	started := time.Now()
	result, executionErr := pc.executeNodeContractHandler(heartbeat.Context(), node, handler, trigger, false, true)
	if result.Committed {
		executionErr = errors.Join(executionErr, pc.transferCommittedHandlerFollowUp(heartbeat.Context(), result.FollowUp, nil))
	}
	_, finishErr, probeErr := pc.finishClaimedNodeAttempt(claimedNodeAttempt{
		ctx: heartbeat.Context(), statusCtx: ctx, node: node, event: trigger.Event,
		claim: admission.claim, heartbeat: heartbeat, started: started,
		result: result, executionErr: executionErr, retryBase: semanticview.HandlerRetryBase(pc.SemanticSource()),
	})
	return result, errors.Join(admission.postCommitErr, finishErr, probeErr)
}
