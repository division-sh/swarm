package tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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

type channelRuntimeLeaseContextKey struct{}
type channelActivationGenerationContextKey struct{}

type channelActivationPresentation struct {
	lease *runtimechannelactivation.Lease
}

type channelRuntimeExecutionLease struct {
	toolID string
	lease  *runtimechannelactivation.Lease
}

func withChannelRuntimeExecutionLease(ctx context.Context, toolID string, lease *runtimechannelactivation.Lease) context.Context {
	ctx = runtimechannelactivation.WithExecutionLease(ctx, lease)
	return context.WithValue(ctx, channelRuntimeLeaseContextKey{}, channelRuntimeExecutionLease{
		toolID: strings.TrimSpace(toolID),
		lease:  lease,
	})
}

func channelRuntimeExecutionLeaseFromContext(ctx context.Context, toolID string) (*runtimechannelactivation.Lease, bool) {
	if ctx == nil {
		return nil, false
	}
	admission, ok := ctx.Value(channelRuntimeLeaseContextKey{}).(channelRuntimeExecutionLease)
	if !ok || admission.lease == nil || admission.toolID != strings.TrimSpace(toolID) {
		return nil, false
	}
	return admission.lease, true
}

func channelRuntimeExecutionPublicationFromContext(ctx context.Context) (*runtimechannelactivation.Lease, bool) {
	if ctx == nil {
		return nil, false
	}
	admission, ok := ctx.Value(channelRuntimeLeaseContextKey{}).(channelRuntimeExecutionLease)
	return admission.lease, ok && admission.lease != nil
}

func withChannelActivationGeneration(ctx context.Context, lease *runtimechannelactivation.Lease) context.Context {
	if ctx == nil || lease == nil || !lease.Generation().Valid() {
		return ctx
	}
	return context.WithValue(ctx, channelActivationGenerationContextKey{}, channelActivationPresentation{lease: lease})
}

func channelActivationPresentationFromContext(ctx context.Context) (*runtimechannelactivation.Lease, bool) {
	if ctx == nil {
		return nil, false
	}
	presentation, ok := ctx.Value(channelActivationGenerationContextKey{}).(channelActivationPresentation)
	return presentation.lease, ok && presentation.lease != nil
}

func (e *Executor) admitChannelRuntimeExecution(ctx context.Context, toolID string) (context.Context, *runtimechannelactivation.Lease, error) {
	if e == nil || e.channelActivations == nil {
		return ctx, nil, nil
	}
	toolID = strings.TrimSpace(toolID)
	if presentation, pinned := channelActivationPresentationFromContext(ctx); pinned {
		if operation, found := presentation.BorrowRuntimeOperation(toolID); found {
			return withChannelRuntimeExecutionLease(ctx, toolID, operation), nil, nil
		}
		return ctx, nil, nil
	}
	var lease *runtimechannelactivation.Lease
	var found bool
	lease, found = e.channelActivations.AcquireRuntimeOperation(toolID)
	if found {
		return withChannelRuntimeExecutionLease(ctx, toolID, lease), lease, nil
	}
	if e.channelActivations.HasRuntimeTool(toolID) {
		return ctx, nil, runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"channel_activation_replacement_in_progress",
			"channel-runtime",
			"admit",
			map[string]any{"tool": toolID},
		)
	}
	return ctx, nil, nil
}

func (e *Executor) execChannelOperation(ctx context.Context, actor models.AgentConfig, toolID string, input any) (any, error) {
	if e == nil || e.activityExecutor == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "channel_activity_runtime_unavailable", "channel-runtime", "execute", nil)
	}
	if e.channelActivations == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "channel_activation_owner_unavailable", "channel-runtime", "execute", nil)
	}
	lease, found := channelRuntimeExecutionLeaseFromContext(ctx, toolID)
	if !found {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "channel_operation_lease_required", "channel-runtime", "execute", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	current := lease.Operation()
	operation := channelOperation{binding: current.Binding, operation: current.Name}
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
	bundleFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_bundle_identity_required", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)})
	}
	bundleHash := bundleFact.BundleHash()
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
	privateTarget, err := operation.binding.RuntimeActivityTarget(operation.operation)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "channel_activity_plan_generation_invalid", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)}, err)
	}
	effectClass, err := operation.binding.OperationEffectClass(operation.operation)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "channel_operation_plan_invalid", "channel-runtime", "build_activity", map[string]any{"tool": strings.TrimSpace(toolID)}, err)
	}
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(effectClass)
	routingSource, err := runtimepinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor, entityID)
	if err != nil {
		return nil, fmt.Errorf("admit channel activity producer source: %w", err)
	}
	intent := runtimeengine.ActivityIntent{
		Context:                     events.DeliveryContextFromContext(ctx),
		ActivityID:                  activityID,
		Tool:                        privateTarget.ToolID(),
		PlanGeneration:              privateTarget.Generation(),
		ChannelActivationGeneration: lease.Generation(),
		BundleHash:                  bundleHash,
		WorkflowVersion:             workflowVersion,
		Input:                       semanticInput,
		EffectClass:                 effectClass,
		SuccessEvent:                strings.TrimSpace(toolID) + ".succeeded",
		FailureEvent:                strings.TrimSpace(toolID) + ".failed",
		RetryMaxAttempts:            defaults.MaxAttempts,
		RetryBackoff:                defaults.Backoff,
		ForkPolicy:                  runtimecontracts.ActivityForkPolicyForEffectClass(effectClass),
		EntityID:                    identity.NormalizeEntityID(entityID),
		Owner:                       activityidentity.MustAgentOwner(actor.ID),
		ExecutionFlowID:             identity.NormalizeFlowID(flowID),
		FlowInstance:                flowInstance,
		HandlerEventKey:             strings.TrimSpace(toolID),
		SourceEventID:               inbound.ID(),
		SourceRunID:                 inbound.RunID(),
		SourceTaskID:                inbound.TaskID(),
		ParentEventID:               inbound.ID(),
		ChainDepth:                  inbound.ChainDepth(),
		Attempt:                     1,
		ExecutionMode:               actor.ExecutionMode,
		RoutingSource:               routingSource,
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
