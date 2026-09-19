package pipelinepersistence

import (
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func pipelineParentClaimBusy(eventID string, state *pipelineClaimState) error {
	attributes := map[string]any{"stage": "pipeline_claim", "event_id": eventID, "claim_owner": "external"}
	if state != nil {
		attributes["purpose"] = string(state.claim.Purpose())
		attributes["claim_owner"] = "local"
		if state.scanToken != "" {
			attributes["claim_owner"] = "recovery"
		}
	}
	return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "pipeline_parent_claim_busy", "runtime.pipeline", "terminalize_parent", attributes, runtimepipelineobligation.ErrBusy)
}

func pipelineParentAcquisitionBusy(eventID string) error {
	attributes := map[string]any{"stage": "pipeline_claim", "event_id": eventID, "claim_owner": "acquiring"}
	return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "pipeline_parent_claim_busy", "runtime.pipeline", "terminalize_parent", attributes, runtimepipelineobligation.ErrBusy)
}

func fanOutCancellationFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	return runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "fan_out_cancellation_failed", "runtime.fan_out", "cancel_run", map[string]any{"stage": stage}, err)
}
