package pipeline

import (
	"context"
	"errors"
	"strings"
)

var errGenericScheduleOwnerRequired = errors.New("generic schedule persistence requires a durable store or process scheduler owner")

// persistGenericSchedule is reserved for non-workflow schedule families such
// as join closure. Workflow timer declarations use WorkflowTimerLifecycle.
func (pc *PipelineCoordinator) persistGenericSchedule(ctx context.Context, schedule Schedule) error {
	if pc == nil {
		return nil
	}
	schedule = scheduleWithRunIDFromContext(ctx, schedule)
	if pc.timerScheduleStore == nil && pc.timerScheduler == nil {
		return errGenericScheduleOwnerRequired
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.UpsertSchedule(ctx, schedule); err != nil {
			return err
		}
	}
	if pc.timerScheduler == nil {
		return nil
	}
	if _, err := ClaimAndRegisterSchedule(ctx, pc.timerScheduleStore, pc.timerScheduler, schedule); err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "generic_schedule_register_failed", "", schedule.EventType, schedule.AgentID, schedule.EffectiveEntityID(), map[string]any{
			"task_id": strings.TrimSpace(schedule.TaskID),
			"mode":    strings.TrimSpace(schedule.Mode),
		}, err)
	}
	return nil
}

func (pc *PipelineCoordinator) cancelGenericSchedule(ctx context.Context, schedule Schedule) error {
	if pc == nil {
		return nil
	}
	schedule = scheduleWithRunIDFromContext(ctx, schedule)
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.CancelScheduleExactTerminal(ctx, schedule); err != nil && !TerminalTransitionApplied(err) {
			return err
		}
	}
	if pc.timerScheduler == nil {
		return nil
	}
	if err := pc.timerScheduler.CancelExact(schedule); err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "generic_schedule_cancel_failed", "", schedule.EventType, schedule.AgentID, schedule.EffectiveEntityID(), map[string]any{
			"task_id": strings.TrimSpace(schedule.TaskID),
			"mode":    strings.TrimSpace(schedule.Mode),
		}, err)
	}
	return nil
}
