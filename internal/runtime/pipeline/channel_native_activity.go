package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type nativeChannelActivityKey struct{}

func (d pipelineActivityDispatcher) executeClaimedNativeActivity(ctx context.Context, intent runtimeengine.ActivityIntent,
	tool runtimecontracts.ToolSchemaEntry, started ActivityAttemptRecord, success, failure activityPublication,
) error {
	result, err := d.executeNativeChannelActivity(ctx, intent, tool, started)
	terminal := started.withTerminal(ActivityAttemptStatusSucceeded, success.eventID, string(success.eventType), activitySuccessPayload(intent, result), nil)
	if err != nil {
		cause := runtimefailures.FromError(err, "activity-runtime", "execute_native_channel")
		status := ActivityAttemptStatusFailed
		if cause.Failure.Class == runtimefailures.ClassOutcomeUncertain {
			status = ActivityAttemptStatusUncertain
		}
		terminal = started.withTerminal(status, failure.eventID, string(failure.eventType), activityFailurePayload(intent, cause), &cause.Failure)
	}
	var stored ActivityAttemptRecord
	var committed bool
	if terminal.Status == ActivityAttemptStatusUncertain {
		stored, committed, err = d.coordinator.workflowStore.MarkActivityAttemptUncertain(ctx, terminal)
	} else {
		stored, committed, err = d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
	}
	return d.publishCommittedActivityAttempt(ctx, intent, stored, committed, err, "complete_native_activity_attempt")
}

func (d pipelineActivityDispatcher) executeNativeChannelActivity(ctx context.Context, intent runtimeengine.ActivityIntent,
	tool runtimecontracts.ToolSchemaEntry, started ActivityAttemptRecord,
) (any, error) {
	target, private := activityChannelTargetFromContext(ctx)
	if !private || target.value == nil || target.value.lease == nil || d.coordinator.nativeChannelExecution == nil {
		return nil, fmt.Errorf("native activity requires its installed original session and activation owners")
	}
	value := target.value
	native, err := d.coordinator.nativeChannelExecution(ctx, value.activation)
	if err != nil {
		return nil, err
	}
	launch, close, err := withNativeChannelActivityLaunch(ctx, started, intent, value.activation, value.operation, tool, value.lease.ValidateAdmission)
	if err != nil {
		return nil, err
	}
	defer close()
	toolID, _, err := value.activation.Plan.ConnectorOperation(value.operation)
	if err != nil {
		return nil, err
	}
	input, ok := intent.Input.Interface().(map[string]any)
	if !ok {
		return nil, fmt.Errorf("native activity requires an exact input object")
	}
	result, err := native.ExecuteChannelActivity(launch, value.activation.OnboardingOperationID, value.operation, toolID, tool, input)
	if err != nil {
		return nil, err
	}
	if projection, compiled := tool.CompiledResultExecution(); compiled && intent.NativeSessionTarget == "" {
		result, err = projection.Project(result)
		if err != nil {
			return nil, runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "native_activity_result_projection_invalid", "activity-runtime", "project_native_channel_result", nil, err)
		}
	}
	return result, nil
}

type nativeChannelActivityLaunch struct {
	ctx                            context.Context
	activation                     channelonboarding.CompiledActivation
	requestID, toolHash, inputHash string
	input                          semanticvalue.Value
	current                        func(context.Context) error
	live, consumed                 atomic.Bool
}

// Only the selected activity dispatcher can issue this process-local handoff.
// It does not introduce a second durable journal or authorize operator delivery.
func withNativeChannelActivityLaunch(ctx context.Context, started ActivityAttemptRecord, intent runtimeengine.ActivityIntent,
	activation channelonboarding.CompiledActivation, operation string, tool runtimecontracts.ToolSchemaEntry, current func(context.Context) error,
) (context.Context, func(), error) {
	privateTool := intent.Tool
	if intent.NativeSessionTarget != "" {
		privateTool = intent.NativeSessionTarget
	}
	if ctx == nil || ctx.Err() != nil || current == nil || started.Status != ActivityAttemptStatusStarted || started.StartedAt.IsZero() ||
		!strings.HasPrefix(privateTool, runtimecontracts.PrivateChannelActivityPrefix) || intent.BundleHash != activation.Coordinate.BundleHash ||
		intent.ExecutionMode != runtimeeffects.ExecutionModeLive {
		return nil, nil, fmt.Errorf("native activity requires its exact newly claimed private attempt")
	}
	if err := activation.Validate(); err != nil {
		return nil, nil, err
	}
	if err := activation.SessionAccount.Validate(); err != nil {
		return nil, nil, err
	}
	privateTarget, err := activation.Plan.RuntimeActivityTarget(operation)
	if err != nil || privateTool != privateTarget.ToolID() || !intent.PlanGeneration.Equal(privateTarget.Generation()) {
		return nil, nil, fmt.Errorf("native activity differs from its exact compiled private target")
	}
	compiled, err := activation.Plan.OperationTool(operation)
	if err != nil {
		return nil, nil, err
	}
	compiledHash, err := compiled.CanonicalHash()
	if err != nil {
		return nil, nil, err
	}
	if err := validateActivityAttemptClaimIdentity(started, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); err != nil {
		return nil, nil, err
	}
	if err := ValidateActivityAttemptStart(started); err != nil {
		return nil, nil, err
	}
	target, native := tool.InProcess()
	if !native || target != runtimecontracts.ToolInProcessWhatsAppSendText || tool.Effect() != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return nil, nil, fmt.Errorf("native activity has no installed exact write target")
	}
	if err := tool.InputSchema().Validate(intent.Input.Interface()); err != nil {
		return nil, nil, err
	}
	if err := admitNativeActivityDestination(ctx, intent, activation, operation, privateTool); err != nil {
		return nil, nil, err
	}
	hash, err := tool.CanonicalHash()
	if err != nil {
		return nil, nil, err
	}
	if hash != compiledHash {
		return nil, nil, fmt.Errorf("native activity substituted its compiled connector")
	}
	if err := current(ctx); err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil {
		return nil, nil, context.Cause(ctx)
	}
	launch := &nativeChannelActivityLaunch{ctx: ctx, activation: activation, requestID: started.RequestEventID,
		toolHash: hash, inputHash: started.InputHash, input: intent.Input, current: current}
	launch.live.Store(true)
	return context.WithValue(ctx, nativeChannelActivityKey{}, launch), func() { launch.live.Store(false) }, nil
}

func admitNativeActivityDestination(ctx context.Context, intent runtimeengine.ActivityIntent, activation channelonboarding.CompiledActivation,
	operation, privateTool string,
) error {
	fields, object := intent.Input.ObjectMap()
	if !object {
		return fmt.Errorf("native activity requires exact object input")
	}
	if intent.NativeSessionTarget != "" {
		target, private := activityChannelTargetFromContext(ctx)
		if !private || target.value == nil || target.value.authored.AuthoredToolID() != intent.Tool {
			return fmt.Errorf("native business activity has no compiler-owned declaration projection")
		}
		projection := target.value.authored
		selected := projection.Activation()
		if selected.OnboardingOperationID != activation.OnboardingOperationID || selected.SessionAccount != activation.SessionAccount ||
			selected.Coordinate != activation.Coordinate || projection.Operation() != operation ||
			projection.PrivateTarget().ToolID() != privateTool || !projection.PublicationGeneration().Equal(intent.ChannelActivationGeneration) {
			return fmt.Errorf("native business activity substituted its original responsibility")
		}
	} else if !fields["destination"].Equal(activation.Plan.Destination()) {
		return fmt.Errorf("native activity destination differs from its compiled binding")
	}
	return nil
}

// NativeChannelActivityPermit has no public constructor or serializable authority.
// The SDK consumes immutable admitted input, not the caller's mutable map.
type NativeChannelActivityPermit struct{ launch *nativeChannelActivityLaunch }

func ConsumeNativeChannelActivity(ctx context.Context, operationID string, account operatorchannel.SessionAccountAdmission,
	tool runtimecontracts.ToolSchemaEntry, input map[string]any,
) (NativeChannelActivityPermit, error) {
	var absent NativeChannelActivityPermit
	if ctx == nil {
		return absent, fmt.Errorf("native activity launch is absent")
	}
	launch, ok := ctx.Value(nativeChannelActivityKey{}).(*nativeChannelActivityLaunch)
	if !ok || launch == nil || launch.activation.OnboardingOperationID != operationID || launch.activation.SessionAccount != account {
		return absent, fmt.Errorf("native activity launch belongs to another connection responsibility")
	}
	permit := NativeChannelActivityPermit{launch: launch}
	if err := permit.Validate(ctx); err != nil {
		return absent, err
	}
	hash, err := tool.CanonicalHash()
	if err != nil || hash != launch.toolHash {
		return absent, fmt.Errorf("native activity tool differs from its claimed contract")
	}
	value, err := canonicaljson.FromGo(input)
	if err != nil {
		return absent, err
	}
	inputHash, err := canonicaljson.HashValue(value)
	if err != nil || inputHash != launch.inputHash {
		return absent, fmt.Errorf("native activity input differs from its claimed bytes")
	}
	if !launch.consumed.CompareAndSwap(false, true) {
		return absent, fmt.Errorf("native activity launch has already been consumed")
	}
	return permit, nil
}

func (p NativeChannelActivityPermit) Validate(ctx context.Context) error {
	if p.launch == nil || ctx == nil || ctx.Err() != nil || p.launch.ctx == nil || p.launch.ctx.Err() != nil ||
		!p.launch.live.Load() || p.launch.current == nil {
		return fmt.Errorf("native activity requires its live original launch")
	}
	if err := p.launch.current(ctx); err != nil {
		return err
	}
	if ctx.Err() != nil || p.launch.ctx.Err() != nil || !p.launch.live.Load() {
		return fmt.Errorf("native activity launch was fenced")
	}
	return nil
}

func (p NativeChannelActivityPermit) Input() semanticvalue.Value {
	if p.launch == nil {
		return semanticvalue.Value{}
	}
	return p.launch.input
}

func (p NativeChannelActivityPermit) MessageID() string {
	if p.launch == nil {
		return ""
	}
	return strings.ReplaceAll(p.launch.requestID, "-", "")
}

func (p NativeChannelActivityPermit) ValidateBusinessBinding(ctx context.Context, op channelonboarding.Operation,
	activation channelonboarding.ConnectedChannelActivation, binding operatorchannel.Binding,
) error {
	if err := p.Validate(ctx); err != nil {
		return err
	}
	if op.Phase != channelonboarding.PhaseSucceeded {
		return fmt.Errorf("native activity requires completed channel readiness")
	}
	expected, err := p.launch.activation.AdmissionResponsibility()
	if err != nil {
		return err
	}
	if !expected.MatchesBusinessBinding(op, activation, binding, activation.BindingRevision) {
		return fmt.Errorf("native activity lost its original selected business binding")
	}
	return nil
}
