package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const (
	joinTimeoutEvent  = "platform.join_timeout"
	joinCompleteEvent = "platform.join_complete"
)

func (pc *PipelineCoordinator) applyWorkflowJoinIntents(ctx context.Context, entityID, currentStage, nextStage string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || pc.SemanticSource() == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if entityID == "" || nextStage == "" || currentStage == nextStage {
		return nil
	}

	toSchedule := make([]Schedule, 0, 2)
	toCancel := make([]Schedule, 0, 2)
	now := time.Now().UTC()
	var lifecycleErr error
	err := pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
		if err != nil {
			lifecycleErr = fmt.Errorf("decode join state: %w", err)
			return
		}
		activations, err := joinruntime.List(carrier.StateBuckets)
		if err != nil {
			lifecycleErr = fmt.Errorf("list join state: %w", err)
			return
		}
		for _, activation := range activations {
			if activation.Stage != currentStage || activation.Stage == nextStage || !activation.CloseForStageExit() {
				continue
			}
			kind := timeridentity.TimerHandleJoinTimeout
			if activation.TimerEventType == joinCompleteEvent {
				kind = timeridentity.TimerHandleJoinComplete
			}
			activation.TimerCancelled = true
			if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
				lifecycleErr = fmt.Errorf("close join %s on stage exit: %w", activation.Key(), err)
				return
			}
			toCancel = append(toCancel, joinSchedule(entityID, *instance, activation, kind))
		}

		for _, plan := range workflowJoinPlansForStage(pc.SemanticSource(), instance.WorkflowName, nextStage) {
			if plan.ResultType.Empty() {
				lifecycleErr = fmt.Errorf("join %s has no resolved output type in the semantic plan", plan.Spec.EffectiveID())
				return
			}
			members, ok := joinMemberSnapshot(instance.Metadata, plan.Spec.Members.From)
			if !ok {
				lifecycleErr = fmt.Errorf("join %s members source %s is not a unique list of non-empty text", plan.Spec.EffectiveID(), plan.Spec.Members.From)
				return
			}
			window := ""
			if plan.Spec.Window != nil {
				window = strings.TrimSpace(asString(instance.Metadata[joinTopLevelField(plan.Spec.Window.From, "entity")]))
				if window == "" {
					lifecycleErr = fmt.Errorf("join %s window source %s resolved empty", plan.Spec.EffectiveID(), plan.Spec.Window.From)
					return
				}
			}
			key := joinruntime.ActivationKey(plan.Spec.Stage, plan.Spec.EffectiveID(), window)
			if _, found, err := joinruntime.Load(carrier.StateBuckets, plan.NodeID, key); err != nil {
				lifecycleErr = fmt.Errorf("load join %s: %w", key, err)
				return
			} else if found {
				continue
			}
			delay := workflowTimerRenderedDelay(plan.Spec.Timeout.After, workflowTimerPolicy(pc.SemanticSource(), plan.FlowID))
			interval, ok := timeridentity.ParseDelayDuration(delay)
			if !ok {
				lifecycleErr = fmt.Errorf("join %s timeout.after %q did not resolve to a positive duration", plan.Spec.EffectiveID(), plan.Spec.Timeout.After)
				return
			}
			ref := timeridentity.NewJoinRef(plan.NodeID, plan.HandlerEvent, plan.Spec.Stage, plan.Spec.EffectiveID(), window)
			handle := timeridentity.JoinTimeoutHandle(ref)
			activation, err := joinruntime.NewActivation(
				plan.Spec.EffectiveID(), plan.Spec.Stage, plan.NodeID, plan.HandlerEvent, window, members,
				now, now.Add(interval), handle.TaskID(), joinTimeoutEvent,
			)
			if err != nil {
				lifecycleErr = fmt.Errorf("arm join %s: %w", plan.Spec.EffectiveID(), err)
				return
			}
			kind := timeridentity.TimerHandleJoinTimeout
			complete, err := joinruntime.CompletionSatisfied(activation, plan.Spec.CompleteWhen, func(expression string, joinContext map[string]any) (bool, error) {
				return workflowexpr.EvalJoinBool(expression, joinContext, plan.ResultType)
			})
			if err != nil {
				lifecycleErr = fmt.Errorf("evaluate join %s completion at arm: %w", plan.Spec.EffectiveID(), err)
				return
			}
			if complete {
				activation.Close(joinruntime.CloseReasonComplete, true, false)
				kind = timeridentity.TimerHandleJoinComplete
				completionHandle := timeridentity.JoinCompleteHandle(ref)
				activation.FireAt = now
				activation.TimerTaskID = completionHandle.TaskID()
				activation.TimerEventType = joinCompleteEvent
			}
			if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
				lifecycleErr = fmt.Errorf("persist join %s: %w", activation.Key(), err)
				return
			}
			toSchedule = append(toSchedule, joinSchedule(entityID, *instance, activation, kind))
		}
		instance.StateBuckets = carrier.PersistedStateBuckets()
	})
	if err != nil {
		return err
	}
	if lifecycleErr != nil {
		return lifecycleErr
	}
	for _, schedule := range toCancel {
		if err := pc.persistWorkflowTimerCancellation(ctx, scheduleWithRunIDFromContext(ctx, schedule)); err != nil {
			return err
		}
	}
	for _, schedule := range toSchedule {
		if err := pc.persistWorkflowTimerSchedule(ctx, scheduleWithRunIDFromContext(ctx, schedule)); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) reconcileClosedJoinSchedules(ctx context.Context, entityID string, carrier runtimeengine.StateCarrier) error {
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list join activations: %w", err)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, strings.TrimSpace(entityID))
	if err != nil || !ok {
		return err
	}
	changed := false
	for _, activation := range activations {
		if activation.Status != joinruntime.StatusClosed || activation.TimerTaskID == "" || activation.TimerCancelled {
			continue
		}
		if activation.TimerEventType == joinCompleteEvent && activation.OutcomePending && !activation.OutcomeFired {
			continue
		}
		kind := timeridentity.TimerHandleJoinTimeout
		if activation.TimerEventType == joinCompleteEvent {
			kind = timeridentity.TimerHandleJoinComplete
		}
		if err := pc.persistWorkflowTimerCancellation(ctx, scheduleWithRunIDFromContext(ctx, joinSchedule(entityID, instance, activation, kind))); err != nil {
			return err
		}
		activation.TimerCancelled = true
		if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
			return fmt.Errorf("persist join timer cancellation %s: %w", activation.Key(), err)
		}
		changed = true
	}
	if changed {
		return pc.workflowStore.Mutate(ctx, strings.TrimSpace(entityID), func(instance *WorkflowInstance) {
			instance.StateBuckets = carrier.PersistedStateBuckets()
		})
	}
	return nil
}

func (pc *PipelineCoordinator) armWorkflowCurrentStageLifecycle(ctx context.Context, entityID, sourceEvent string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		instance, ok, err := pc.workflowStore.Load(txctx, entityID)
		if err != nil || !ok {
			return err
		}
		stage := strings.TrimSpace(instance.CurrentState)
		if stage == "" {
			return nil
		}
		if strings.TrimSpace(sourceEvent) == "" {
			sourceEvent = "state:" + stage
		}
		if err := pc.applyWorkflowTimerIntents(txctx, entityID, "", stage, sourceEvent); err != nil {
			return err
		}
		return pc.applyWorkflowJoinIntents(txctx, entityID, "", stage)
	})
}

func workflowJoinPlansForStage(source semanticview.Source, flowID, stage string) []runtimecontracts.WorkflowJoinPlan {
	if source == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	stage = strings.TrimSpace(stage)
	out := make([]runtimecontracts.WorkflowJoinPlan, 0, 1)
	for _, plan := range source.WorkflowJoins() {
		planFlowID := strings.TrimSpace(plan.FlowID)
		flowMatches := planFlowID == flowID
		if planFlowID == "" {
			flowMatches = flowID == "" || flowID == strings.TrimSpace(source.WorkflowName())
		}
		if flowMatches && strings.TrimSpace(plan.Spec.Stage) == stage {
			out = append(out, plan)
		}
	}
	return out
}

func joinMemberSnapshot(metadata map[string]any, path string) ([]string, bool) {
	value, ok := metadata[joinTopLevelField(path, "entity")]
	if !ok {
		return nil, false
	}
	var raw []any
	switch typed := value.(type) {
	case []any:
		raw = typed
	case []string:
		raw = make([]any, len(typed))
		for i := range typed {
			raw[i] = typed[i]
		}
	default:
		return nil, false
	}
	members := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		member, ok := item.(string)
		member = strings.TrimSpace(member)
		if !ok || member == "" {
			return nil, false
		}
		if _, duplicate := seen[member]; duplicate {
			return nil, false
		}
		seen[member] = struct{}{}
		members = append(members, member)
	}
	return members, true
}

func joinTopLevelField(path, root string) string {
	path = strings.TrimSpace(path)
	prefix := strings.TrimSpace(root) + "."
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	field := strings.TrimPrefix(path, prefix)
	if field == "" || strings.Contains(field, ".") {
		return ""
	}
	return field
}

func joinSchedule(entityID string, instance WorkflowInstance, activation joinruntime.Activation, kind timeridentity.TimerHandleKind) Schedule {
	ref := timeridentity.NewJoinRef(activation.NodeID, activation.HandlerEvent, activation.Stage, activation.JoinID, activation.Window)
	handle := timeridentity.JoinTimeoutHandle(ref)
	eventType := joinTimeoutEvent
	if kind == timeridentity.TimerHandleJoinComplete {
		handle = timeridentity.JoinCompleteHandle(ref)
		eventType = joinCompleteEvent
	}
	return Schedule{
		AgentID:      runtimeWorkflowID,
		EventType:    eventType,
		Mode:         "once",
		At:           activation.FireAt,
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(instance.StorageRef), "/"),
		TaskID:       handle.TaskID(),
		Payload:      mustJSON(handle.PayloadMetadata()),
	}
}
