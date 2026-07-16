package pipeline

import (
	"context"
	"fmt"
	"strings"
)

// persistGenericSchedule is reserved for non-workflow schedule families such
// as join closure. Workflow timer declarations use WorkflowTimerLifecycle.
func (pc *PipelineCoordinator) persistGenericSchedule(ctx context.Context, schedule Schedule) error {
	if pc == nil {
		return nil
	}
	schedule = scheduleWithRunIDFromContext(ctx, schedule)
	if schedule.EffectiveTimerID() != "" {
		return fmt.Errorf("typed workflow timer cannot enter generic schedule persistence")
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.UpsertSchedule(ctx, schedule); err != nil {
			return err
		}
	}
	if pc.timerScheduler == nil {
		return nil
	}
	register := func(registerCtx context.Context) {
		if _, err := ClaimAndRegisterSchedule(registerCtx, pc.timerScheduleStore, pc.timerScheduler, schedule); err != nil {
			pc.logRuntimeWarn(registerCtx, runtimeWorkflowID, "generic_schedule_register_failed", "", schedule.EventType, schedule.AgentID, schedule.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(schedule.TaskID),
				"mode":    strings.TrimSpace(schedule.Mode),
			}, err)
		}
	}
	if !queuePipelinePostCommitAction(ctx, func() { register(withoutSQLTxContext(ctx)) }) {
		register(ctx)
	}
	return nil
}

func (pc *PipelineCoordinator) cancelGenericSchedule(ctx context.Context, schedule Schedule) error {
	if pc == nil {
		return nil
	}
	schedule = scheduleWithRunIDFromContext(ctx, schedule)
	if schedule.EffectiveTimerID() != "" {
		return fmt.Errorf("typed workflow timer cannot enter generic schedule cancellation")
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.CancelScheduleExactTerminal(ctx, schedule); err != nil && !TerminalTransitionApplied(err) {
			return err
		}
	}
	if pc.timerScheduler == nil {
		return nil
	}
	cancel := func() {
		if err := pc.timerScheduler.CancelExact(schedule); err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "generic_schedule_cancel_failed", "", schedule.EventType, schedule.AgentID, schedule.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(schedule.TaskID),
				"mode":    strings.TrimSpace(schedule.Mode),
			}, err)
		}
	}
	if !queuePipelinePostCommitAction(ctx, cancel) {
		cancel()
	}
	return nil
}
