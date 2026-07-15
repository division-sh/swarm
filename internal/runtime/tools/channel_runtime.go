package tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type DurableActivityExecutor interface {
	ExecuteDurableActivity(context.Context, runtimeengine.ActivityIntent) (runtimepipeline.ActivityAttemptRecord, error)
}

type channelOperation struct {
	binding   packs.OutboundBindingPlan
	operation string
}

func compileChannelOperations(bindings []packs.OutboundBindingPlan) map[string]channelOperation {
	out := map[string]channelOperation{}
	for _, binding := range bindings {
		binding = binding.Clone()
		for operation := range binding.Structural.Operations {
			operation = strings.TrimSpace(operation)
			if operation == "" {
				continue
			}
			out[binding.RuntimeToolID(operation)] = channelOperation{binding: binding, operation: operation}
		}
	}
	return out
}

func (e *Executor) execChannelOperation(ctx context.Context, actor models.AgentConfig, toolID string, input any) (any, error) {
	if e == nil || e.activityExecutor == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "channel_activity_runtime_unavailable", "channel-runtime", "execute", nil)
	}
	operation, ok := e.channelOperations[strings.TrimSpace(toolID)]
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassTargetUnreachable, "channel_operation_not_configured", "channel-runtime", "execute", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	compiled := operation.binding.Structural.Operations[operation.operation]
	connectorToolID, prepared, err := operation.binding.PrepareOperation(operation.operation, input)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "channel_operation_input_invalid", "channel-runtime", "prepare", map[string]any{"tool": strings.TrimSpace(toolID)}, err)
	}
	// PrepareOperation returns the public runtime id. The durable activity must
	// execute the immutable connector selected by the compiled satisfaction plan.
	_ = connectorToolID
	semanticInput, err := canonicaljson.FromGo(prepared)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "channel_connector_input_invalid", "channel-runtime", "admit_connector_input", map[string]any{"tool": strings.TrimSpace(toolID)}, err)
	}

	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	if !ok || strings.TrimSpace(inbound.ID()) == "" || strings.TrimSpace(inbound.RunID()) == "" {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_source_event_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	logicalOperationID, ok := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_logical_operation_identity_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	bundleFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_bundle_identity_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	bundleFact = bundleFact.Normalized()
	bundleHash := bundleFact.BundleHash
	if bundleHash == "" {
		bundleHash = bundleFact.BundleFingerprint
	}
	workflowVersion := ""
	if e.workflowSource != nil {
		workflowVersion = strings.TrimSpace(e.workflowSource.WorkflowVersion())
	}
	if bundleHash == "" || workflowVersion == "" {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_contract_pin_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	if !actor.ExecutionMode.Valid() {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_execution_mode_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}

	target := inbound.TargetRoute().Normalized()
	entityID := firstChannelString(target.EntityID, inbound.EntityID(), actor.EffectiveEntityID())
	flowID := firstChannelString(target.FlowID, actor.FlowID)
	flowInstance := firstChannelString(target.FlowInstance, inbound.FlowInstance(), actor.CanonicalFlowPath(), flowID)
	activityCoordinate := strings.Join([]string{strings.TrimSpace(toolID), logicalOperationID}, "\x00")
	activityID := "channel_" + strings.ReplaceAll(strings.TrimPrefix(strings.TrimSpace(toolID), "channel."), ".", "_") + "_" + uuid.NewSHA1(uuid.NameSpaceURL, []byte(activityCoordinate)).String()
	effectClass := runtimecontracts.NormalizeActivityEffectClass(compiled.Interface.EffectClass)
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(effectClass)
	intent := runtimeengine.ActivityIntent{
		Context:          events.DeliveryContextFromContext(ctx),
		ActivityID:       activityID,
		Tool:             operation.binding.RuntimeActivityToolID(operation.operation),
		BundleHash:       bundleHash,
		WorkflowVersion:  workflowVersion,
		Input:            semanticInput,
		EffectClass:      effectClass,
		SuccessEvent:     strings.TrimSpace(toolID) + ".succeeded",
		FailureEvent:     strings.TrimSpace(toolID) + ".failed",
		RetryMaxAttempts: defaults.MaxAttempts,
		RetryBackoff:     defaults.Backoff,
		ForkPolicy:       runtimecontracts.ActivityForkPolicyForEffectClass(effectClass),
		EntityID:         identity.NormalizeEntityID(entityID),
		NodeID:           identity.NormalizeNodeID(actor.ID),
		FlowID:           identity.NormalizeFlowID(flowID),
		FlowInstance:     flowInstance,
		HandlerEventKey:  strings.TrimSpace(toolID),
		SourceEventID:    inbound.ID(),
		SourceRunID:      inbound.RunID(),
		SourceTaskID:     inbound.TaskID(),
		ParentEventID:    inbound.ID(),
		ChainDepth:       inbound.ChainDepth(),
		Attempt:          1,
		ExecutionMode:    actor.ExecutionMode,
	}.Normalized()
	record, err := e.activityExecutor.ExecuteDurableActivity(ctx, intent)
	if err != nil {
		return nil, err
	}
	switch record.Status {
	case runtimepipeline.ActivityAttemptStatusSucceeded:
		result, ok := record.ResultPayload["result"]
		if !ok {
			return nil, runtimefailures.New(runtimefailures.ClassInternalFailure, "channel_activity_result_missing", "channel-runtime", "read_result", map[string]any{"tool": strings.TrimSpace(toolID)})
		}
		return result, nil
	case runtimepipeline.ActivityAttemptStatusFailed, runtimepipeline.ActivityAttemptStatusUncertain:
		if record.Failure != nil {
			return nil, runtimefailures.FromEnvelope(*record.Failure)
		}
		return nil, runtimefailures.New(runtimefailures.ClassInternalFailure, "channel_activity_failure_missing", "channel-runtime", "read_result", map[string]any{"tool": strings.TrimSpace(toolID)})
	case runtimepipeline.ActivityAttemptStatusStarted:
		return nil, runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "channel_activity_outcome_pending", "channel-runtime", "read_result", map[string]any{"tool": strings.TrimSpace(toolID), "request_event_id": record.RequestEventID})
	default:
		return nil, fmt.Errorf("channel activity returned unsupported status %q", record.Status)
	}
}

func firstChannelString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
