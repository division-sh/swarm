package registration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func ChannelWriteFingerprint(toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any) ([]byte, error) {
	if tool.Category() != runtimecontracts.ToolCategoryProviderConnector || tool.Effect() != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return nil, fmt.Errorf("channel write tool %q has an invalid contract", toolID)
	}
	if err := tool.InputSchema().Validate(input); err != nil {
		return nil, fmt.Errorf("channel write tool %q input: %w", toolID, err)
	}
	return semanticProviderRequest(toolID, tool, input)
}

func channelWriteSource(handle *runtimeeffects.Handle) (string, error) {
	if handle != nil {
		switch handle.Attempt().Kind {
		case runtimeeffects.KindChannelConfirmation:
			return "channel_confirmation", nil
		case runtimeeffects.KindChannelDelivery:
			return "channel_delivery", nil
		case runtimeeffects.KindChannelActionAck:
			return "channel_action_ack", nil
		}
	}
	return "", fmt.Errorf("channel write requires its authorized journal handle")
}

// FailChannelWrite shares the pre/post-launch mapping across admitted transports.
// It does not authorize, execute, or retry a provider operation.
func FailChannelWrite(ctx context.Context, handle *runtimeeffects.Handle, toolID string, launched bool, launchErr, cause error) (DeliveryResult, error) {
	source, err := channelWriteSource(handle)
	if err != nil {
		return DeliveryResult{}, errors.Join(cause, err)
	}
	result := DeliveryResult{OperationID: handle.Attempt().OperationID}
	state, class, detail := runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, source+"_acknowledgment_lost"
	attributes := map[string]any{"tool": strings.TrimSpace(toolID)}
	if !launched {
		state, class, detail = runtimeeffects.StateTerminalFailure, runtimefailures.ClassDependencyUnavailable, source+"_prelaunch_rejected"
		attributes["launch_rejected"] = true
		if launchErr != nil {
			ctx = context.WithoutCancel(ctx)
			class, detail = runtimefailures.ClassLifecycleConflict, source+"_launch_dispatch_blocked"
			attributes["no_dispatch"] = true
		}
	}
	return result, errors.Join(launchErr, handle.Fail(ctx, state, class, detail, source, "dispatch", attributes, cause))
}

// CompleteChannelWrite persists observed provider evidence and the compiled
// receipt through the same journal/selected-store settlement owner for HTTP/SDK.
func CompleteChannelWrite(ctx context.Context, handle *runtimeeffects.Handle, toolID string, tool runtimecontracts.ToolSchemaEntry, output any,
	observation map[string]any, raw []byte, launchErr, responseErr error,
) (DeliveryResult, error) {
	source, err := channelWriteSource(handle)
	if err != nil {
		return DeliveryResult{}, err
	}
	result := DeliveryResult{OperationID: handle.Attempt().OperationID}
	observationErr := handle.MarkResponseObserved(ctx, observation)
	if observationErr != nil && !runtimeeffects.CommittedMutationPhase(observationErr, runtimeeffects.MutationObservation, handle.Attempt()) {
		return result, errors.Join(launchErr, observationErr)
	}
	if responseErr == nil {
		responseErr = tool.OutputSchema().Validate(output)
	}
	if responseErr != nil {
		attributes := map[string]any{"tool": strings.TrimSpace(toolID)}
		if status, present := observation["status"]; present {
			attributes["status"] = status
		}
		return result, errors.Join(launchErr, observationErr, handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
			source+"_response_unconfirmed", source, "validate_response", attributes, responseErr))
	}
	settlement := make(map[string]any, len(observation)+2)
	for key, value := range observation {
		settlement[key] = value
	}
	settlement["response_fingerprint"] = runtimeeffects.Fingerprint(raw)
	if source == "channel_delivery" {
		projection, compiled := tool.CompiledResultExecution()
		if !compiled {
			err = fmt.Errorf("channel delivery requires the compiled result projection")
			return result, errors.Join(launchErr, observationErr, handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
				"channel_delivery_projection_unconfirmed", source, "project_result", map[string]any{"tool": strings.TrimSpace(toolID)}, err))
		}
		output, err = projection.Project(output)
		if err != nil {
			return result, errors.Join(launchErr, observationErr, handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
				"channel_delivery_projection_unconfirmed", source, "project_result", map[string]any{"tool": strings.TrimSpace(toolID)}, err))
		}
		projected, encodeErr := json.Marshal(output)
		if encodeErr == nil {
			projected, encodeErr = canonicaljson.Canonicalize(projected)
		}
		if encodeErr != nil {
			return result, errors.Join(launchErr, observationErr, handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
				"channel_delivery_receipt_unconfirmed", source, "validate_response", map[string]any{"tool": strings.TrimSpace(toolID)}, encodeErr))
		}
		settlement["projected_output"] = json.RawMessage(projected)
	}
	settleErr := handle.Succeed(ctx, settlement)
	if settleErr != nil && !runtimeeffects.CommittedMutationPhase(settleErr, runtimeeffects.MutationSettlement, handle.Attempt()) {
		return result, errors.Join(launchErr, observationErr, settleErr)
	}
	result.Output = output
	return result, errors.Join(launchErr, observationErr, settleErr)
}
