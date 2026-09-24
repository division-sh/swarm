package bus

import (
	"errors"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type pipelineDispatchAction uint8

const (
	pipelineDispatchPending pipelineDispatchAction = iota
	pipelineDispatchRetryRelease
	pipelineDispatchSettle
	pipelineDispatchMarkDecisionProcessed
)

type pipelineDispatchDecision struct {
	action             pipelineDispatchAction
	disposition        runtimepipelineobligation.Disposition
	retry              runtimepipelineobligation.RetryRelease
	failedBeforeSettle bool
}

// This is the sole conversion from a handler execution outcome to an event-
// obligation decision. Claim owners still perform their distinct fenced writes.
func classifyPipelineDispatch(outcome runtimepipelineobligation.ExecutionOutcome, dispatchErr error, queued bool, purpose runtimepipelineobligation.Purpose, recovery bool) pipelineDispatchDecision {
	if recovery && purpose == runtimepipelineobligation.PurposeDecisionRoute {
		if retry, ok := outcome.RetryRelease(); ok {
			return pipelineDispatchDecision{action: pipelineDispatchRetryRelease, retry: retry}
		}
		if disposition, ok := outcome.Disposition(); ok {
			return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: disposition}
		}
		if dispatchErr == nil || outcome.Committed {
			return pipelineDispatchDecision{action: pipelineDispatchMarkDecisionProcessed}
		}
	}
	if outcome.Committed && outcome.ContinueDispatch() {
		return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: runtimepipelineobligation.Acknowledged("pipeline_persisted")}
	}
	if retry, ok := outcome.RetryRelease(); ok {
		return pipelineDispatchDecision{action: pipelineDispatchRetryRelease, retry: retry}
	}
	if disposition, ok := outcome.Disposition(); ok {
		return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: disposition}
	}
	if dispatchErr != nil {
		if errors.Is(dispatchErr, ErrRuntimeIngressPaused) || errors.Is(dispatchErr, ErrRunDispatchBlocked) || errors.Is(dispatchErr, errAuthoritativeDeliveryIncomplete) {
			return pipelineDispatchDecision{action: pipelineDispatchPending}
		}
		operation, reason := "dispatch_outbox", "pipeline_outbox_dispatch_failed"
		if recovery {
			operation, reason = "recover_pipeline_obligation", "pipeline_recovery_failed"
		}
		failure := eventBusFailure(dispatchErr, operation)
		disposition := runtimepipelineobligation.Terminal(reason, failure)
		if recovery && purpose == runtimepipelineobligation.PurposeDecisionRoute {
			disposition = runtimepipelineobligation.Quarantined(pipelineDispositionFailureReason("decision_route_recovery_failed", failure), failure)
		}
		return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: disposition, failedBeforeSettle: recovery}
	}
	if queued {
		return pipelineDispatchDecision{action: pipelineDispatchPending}
	}
	if recovery && purpose == runtimepipelineobligation.PurposeDecisionRoute {
		return pipelineDispatchDecision{action: pipelineDispatchMarkDecisionProcessed}
	}
	return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: runtimepipelineobligation.Acknowledged("pipeline_persisted")}
}
