package bus

import (
	"errors"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type pipelineDispatchAction uint8

const (
	pipelineDispatchPending pipelineDispatchAction = iota
	pipelineDispatchContinue
	pipelineDispatchRetryRelease
	pipelineDispatchSettle
	pipelineDispatchMarkDecisionProcessed
)

type pipelineDispatchPhase uint8

const (
	pipelineDispatchOutboxFinal pipelineDispatchPhase = iota + 1
	pipelineDispatchRecoveryFinal
	pipelineDispatchPublishInterceptors
	pipelineDispatchPublishRoutes
	pipelineDispatchPublishDeferred
	pipelineDispatchPublishFinal
)

type pipelineDispatchDecision struct {
	action             pipelineDispatchAction
	disposition        runtimepipelineobligation.Disposition
	retry              runtimepipelineobligation.RetryRelease
	failedBeforeSettle bool
}

// This is the sole conversion from a handler execution outcome to an event-
// obligation decision. Claim owners still perform their distinct fenced writes.
func classifyPipelineDispatch(outcome runtimepipelineobligation.ExecutionOutcome, dispatchErr error, queued bool, purpose runtimepipelineobligation.Purpose, phase pipelineDispatchPhase) pipelineDispatchDecision {
	recovery := phase == pipelineDispatchRecoveryFinal
	foreground := phase == pipelineDispatchPublishInterceptors || phase == pipelineDispatchPublishRoutes || phase == pipelineDispatchPublishDeferred || phase == pipelineDispatchPublishFinal
	intermediate := foreground && phase != pipelineDispatchPublishFinal
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
		if intermediate {
			return pipelineDispatchDecision{action: pipelineDispatchContinue}
		}
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
		switch phase {
		case pipelineDispatchRecoveryFinal:
			operation, reason = "recover_pipeline_obligation", "pipeline_recovery_failed"
		case pipelineDispatchPublishInterceptors:
			operation, reason = "dispatch_committed_publish", "pipeline_dispatch_failed"
		case pipelineDispatchPublishRoutes:
			operation, reason = "deliver_route_plan", "pipeline_delivery_failed"
		case pipelineDispatchPublishDeferred:
			operation, reason = "publish_deferred", "pipeline_deferred_publish_failed"
		}
		failure := eventBusFailure(dispatchErr, operation)
		if foreground {
			reason = pipelineDispositionFailureReason(reason, failure)
		}
		disposition := runtimepipelineobligation.Terminal(reason, failure)
		if purpose == runtimepipelineobligation.PurposeDecisionRoute {
			if recovery {
				reason = pipelineDispositionFailureReason("decision_route_recovery_failed", failure)
			}
			disposition = runtimepipelineobligation.Quarantined(reason, failure)
		}
		return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: disposition, failedBeforeSettle: recovery}
	}
	if queued {
		return pipelineDispatchDecision{action: pipelineDispatchPending}
	}
	if recovery && purpose == runtimepipelineobligation.PurposeDecisionRoute {
		return pipelineDispatchDecision{action: pipelineDispatchMarkDecisionProcessed}
	}
	if phase == pipelineDispatchPublishFinal && purpose == runtimepipelineobligation.PurposeDecisionRoute {
		return pipelineDispatchDecision{action: pipelineDispatchMarkDecisionProcessed}
	}
	if intermediate {
		return pipelineDispatchDecision{action: pipelineDispatchContinue}
	}
	return pipelineDispatchDecision{action: pipelineDispatchSettle, disposition: runtimepipelineobligation.Acknowledged("pipeline_persisted")}
}

func pipelinePublishPurpose(evt events.Event) runtimepipelineobligation.Purpose {
	if evt.Type() == events.EventType("mailbox.card_decided") {
		return runtimepipelineobligation.PurposeDecisionRoute
	}
	return runtimepipelineobligation.PurposePublication
}

func pipelineDispositionFailureReason(fallback string, failure *runtimefailures.Envelope) string {
	if failure != nil {
		if code := strings.TrimSpace(failure.Detail.Code); code != "" {
			return code
		}
	}
	return strings.TrimSpace(fallback)
}
