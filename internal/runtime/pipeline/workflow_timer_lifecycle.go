package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func (pc *FactoryPipelineCoordinator) reconcileWorkflowStageTimers(ctx context.Context, verticalID, currentStage, nextStage, sourceEvent string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	bundle := pc.ContractBundle()
	if bundle == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || nextStage == "" || currentStage == nextStage {
		return
	}
	now := time.Now().UTC()
	toSchedule := make([]Schedule, 0, 2)
	toCancel := make([]Schedule, 0, 2)
	if err := pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.TimerState == nil {
			instance.TimerState = []WorkflowTimerState{}
		}
		for i := range instance.TimerState {
			timerState := &instance.TimerState[i]
			if timerState.Cancelled {
				continue
			}
			timer, ok := bundle.WorkflowTimerByID(timerState.TimerID)
			if !ok || !workflowTimerLifecycleMatches(timer.CancelOn, nextStage, sourceEvent) {
				continue
			}
			timerState.Cancelled = true
			toCancel = append(toCancel, workflowTimerSchedule(timer, verticalID, timerState.FiresAt))
		}
		for _, timer := range bundle.WorkflowTimers() {
			if timer.Recurring || !workflowTimerLifecycleMatches(timer.StartOn, nextStage, sourceEvent) {
				continue
			}
			if workflowTimerStateActive(instance.TimerState, timer.ID) {
				continue
			}
			fireAt, ok := workflowTimerFireAt(timer, now, workflowTimerPolicy(bundle))
			if !ok {
				continue
			}
			instance.TimerState = append(instance.TimerState, WorkflowTimerState{
				TimerID:   strings.TrimSpace(timer.ID),
				EventType: strings.TrimSpace(timer.Event),
				CreatedAt: now,
				FiresAt:   fireAt,
				StartedBy: "state:" + nextStage,
				Recurring: timer.Recurring,
			})
			toSchedule = append(toSchedule, workflowTimerSchedule(timer, verticalID, fireAt))
		}
	}); err != nil {
		runtimeWarn(runtimeWorkflowID, "workflow timer projection failed vertical_id=%s stage=%s: %v", verticalID, nextStage, err)
		return
	}
	for _, sc := range toCancel {
		pc.cancelWorkflowTimerSchedule(ctx, sc)
	}
	for _, sc := range toSchedule {
		pc.registerWorkflowTimerSchedule(ctx, sc)
	}
}

func (pc *FactoryPipelineCoordinator) reconcileWorkflowEventTimers(ctx context.Context, verticalID, sourceEvent string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	bundle := pc.ContractBundle()
	if bundle == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || sourceEvent == "" {
		return
	}
	if _, ok, err := pc.workflowStore.Load(ctx, verticalID); err != nil {
		runtimeWarn(runtimeWorkflowID, "workflow event timer load failed vertical_id=%s event=%s: %v", verticalID, sourceEvent, err)
		return
	} else if !ok {
		return
	}
	now := time.Now().UTC()
	toSchedule := make([]Schedule, 0, 1)
	toCancel := make([]Schedule, 0, 1)
	if err := pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.TimerState == nil {
			instance.TimerState = []WorkflowTimerState{}
		}
		for i := range instance.TimerState {
			timerState := &instance.TimerState[i]
			if timerState.Cancelled {
				continue
			}
			timer, ok := bundle.WorkflowTimerByID(timerState.TimerID)
			if !ok || !workflowTimerLifecycleMatches(timer.CancelOn, "", sourceEvent) {
				continue
			}
			timerState.Cancelled = true
			toCancel = append(toCancel, workflowTimerSchedule(timer, verticalID, timerState.FiresAt))
		}
		for _, timer := range bundle.WorkflowTimers() {
			if timer.Recurring || !workflowTimerLifecycleMatches(timer.StartOn, "", sourceEvent) {
				continue
			}
			if workflowTimerStateActive(instance.TimerState, timer.ID) {
				continue
			}
			fireAt, ok := workflowTimerFireAt(timer, now, workflowTimerPolicy(bundle))
			if !ok {
				continue
			}
			instance.TimerState = append(instance.TimerState, WorkflowTimerState{
				TimerID:   strings.TrimSpace(timer.ID),
				EventType: strings.TrimSpace(timer.Event),
				CreatedAt: now,
				FiresAt:   fireAt,
				StartedBy: "event:" + sourceEvent,
				Recurring: timer.Recurring,
			})
			toSchedule = append(toSchedule, workflowTimerSchedule(timer, verticalID, fireAt))
		}
	}); err != nil {
		runtimeWarn(runtimeWorkflowID, "workflow event timer projection failed vertical_id=%s event=%s: %v", verticalID, sourceEvent, err)
		return
	}
	for _, sc := range toCancel {
		pc.cancelWorkflowTimerSchedule(ctx, sc)
	}
	for _, sc := range toSchedule {
		pc.registerWorkflowTimerSchedule(ctx, sc)
	}
}

func workflowTimerLifecycleMatches(trigger, stage, sourceEvent string) bool {
	trigger = strings.TrimSpace(trigger)
	stage = strings.TrimSpace(stage)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if trigger == "" {
		return false
	}
	switch {
	case strings.HasPrefix(trigger, "state:"):
		return strings.TrimSpace(strings.TrimPrefix(trigger, "state:")) == stage
	case strings.HasPrefix(trigger, "event:"):
		return strings.TrimSpace(strings.TrimPrefix(trigger, "event:")) == sourceEvent
	default:
		return trigger == stage || trigger == sourceEvent
	}
}

func workflowTimerStateActive(items []WorkflowTimerState, timerID string) bool {
	timerID = strings.TrimSpace(timerID)
	if timerID == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.TimerID) == timerID && !item.Cancelled {
			return true
		}
	}
	return false
}

func workflowTimerFireAt(timer runtimecontracts.WorkflowTimerContract, now time.Time, policy map[string]any) (time.Time, bool) {
	interval := workflowTimerDuration(timer, policy)
	if interval <= 0 {
		return time.Time{}, false
	}
	return now.Add(interval), true
}

func workflowTimerDuration(timer runtimecontracts.WorkflowTimerContract, policy map[string]any) time.Duration {
	var interval time.Duration
	if delay := workflowTimerRenderedDelay(timer.Delay, policy); delay != "" {
		if parsed, err := time.ParseDuration(delay); err == nil && parsed > 0 {
			interval += parsed
		}
	}
	interval += time.Duration(timer.DelaySeconds) * time.Second
	interval += time.Duration(timer.DelayMinutes) * time.Minute
	interval += time.Duration(timer.DelayHours) * time.Hour
	interval += time.Duration(timer.DelayDays) * 24 * time.Hour
	return interval
}

func workflowTimerRenderedDelay(delay string, policy map[string]any) string {
	delay = strings.TrimSpace(delay)
	if delay == "" || !strings.Contains(delay, "{{") {
		return delay
	}
	return workflowExpressionPolicyPlaceholder.ReplaceAllStringFunc(delay, func(token string) string {
		match := workflowExpressionPolicyPlaceholder.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		key := strings.TrimSpace(match[1])
		value, ok := workflowExpressionPolicyValue(policy, key)
		if !ok || value == nil {
			return token
		}
		return fmt.Sprint(value)
	})
}

func workflowTimerPolicy(bundle *runtimecontracts.WorkflowContractBundle) map[string]any {
	if bundle == nil {
		return nil
	}
	if len(bundle.MergedPolicy.Values) > 0 {
		return policyDocumentToMap(bundle.MergedPolicy)
	}
	return policyDocumentToMap(bundle.Policy)
}

func workflowTimerSchedule(timer runtimecontracts.WorkflowTimerContract, verticalID string, fireAt time.Time) Schedule {
	return Schedule{
		AgentID:    strings.TrimSpace(timer.Owner),
		EventType:  strings.TrimSpace(timer.Event),
		Mode:       "once",
		At:         fireAt,
		VerticalID: strings.TrimSpace(verticalID),
		TaskID:     strings.TrimSpace(timer.ID),
		Payload:    mustJSON(map[string]any{"timer_id": strings.TrimSpace(timer.ID), "trigger_reason": strings.TrimSpace(timer.ID)}),
	}
}

func (pc *FactoryPipelineCoordinator) registerWorkflowTimerSchedule(ctx context.Context, sc Schedule) {
	if pc == nil {
		return
	}
	if pc.timerScheduler != nil {
		if err := pc.timerScheduler.Register(sc); err != nil {
			runtimeWarn(runtimeWorkflowID, "workflow timer register failed agent=%s event=%s vertical_id=%s: %v", sc.AgentID, sc.EventType, sc.VerticalID, err)
		}
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.UpsertSchedule(ctx, sc); err != nil {
			runtimeWarn(runtimeWorkflowID, "workflow timer persist failed agent=%s event=%s vertical_id=%s: %v", sc.AgentID, sc.EventType, sc.VerticalID, err)
		}
	}
}

func (pc *FactoryPipelineCoordinator) cancelWorkflowTimerSchedule(ctx context.Context, sc Schedule) {
	if pc == nil {
		return
	}
	if pc.timerScheduler != nil {
		if err := pc.timerScheduler.CancelExact(sc); err != nil {
			runtimeWarn(runtimeWorkflowID, "workflow timer cancel failed agent=%s event=%s vertical_id=%s: %v", sc.AgentID, sc.EventType, sc.VerticalID, err)
		}
	}
	if pc.timerScheduleStore == nil {
		return
	}
	if exactStore, ok := pc.timerScheduleStore.(ExactSchedulePersistence); ok {
		if err := exactStore.CancelScheduleExact(ctx, sc); err != nil {
			runtimeWarn(runtimeWorkflowID, "workflow timer cancel persist failed agent=%s event=%s vertical_id=%s: %v", sc.AgentID, sc.EventType, sc.VerticalID, err)
		}
		return
	}
	if err := pc.timerScheduleStore.CancelSchedule(ctx, sc.AgentID, sc.EventType); err != nil {
		runtimeWarn(runtimeWorkflowID, "workflow timer cancel persist failed agent=%s event=%s vertical_id=%s: %v", sc.AgentID, sc.EventType, sc.VerticalID, err)
	}
}
