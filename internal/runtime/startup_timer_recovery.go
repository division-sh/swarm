package runtime

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const startupTimerRecoveryAction = "startup_recovery_timer_aftermath"

type startupTimerRecoveryOutcome string

const (
	startupTimerRecoveryOutcomeReplayed startupTimerRecoveryOutcome = "replayed"
	startupTimerRecoveryOutcomeSkipped  startupTimerRecoveryOutcome = "skipped"
	startupTimerRecoveryOutcomeDropped  startupTimerRecoveryOutcome = "dropped"
)

type startupTimerRecoveryReasonCode string

const (
	startupTimerRecoveryReasonRestored         startupTimerRecoveryReasonCode = "persisted_schedule_restored"
	startupTimerRecoveryReasonClaimNotAcquired startupTimerRecoveryReasonCode = "schedule_claim_not_acquired"
	startupTimerRecoveryReasonRestoreFailed    startupTimerRecoveryReasonCode = "schedule_restore_failed"
)

type startupTimerRecoveryResult struct {
	Schedule   runtimepipeline.Schedule
	Outcome    startupTimerRecoveryOutcome
	ReasonCode startupTimerRecoveryReasonCode
	Failure    *runtimefailures.Envelope
}

func (r startupTimerRecoveryResult) detail() map[string]any {
	sc := r.Schedule
	detail := map[string]any{
		"decision_family":      "startup_timer_recovery",
		"decision_outcome":     string(r.Outcome),
		"decision_reason_code": string(r.ReasonCode),
		"agent_id":             strings.TrimSpace(sc.AgentID),
		"event_type":           strings.TrimSpace(sc.EventType),
		"entity_id":            sc.EffectiveEntityID(),
		"flow_instance":        sc.EffectiveFlowInstance(),
		"task_id":              strings.TrimSpace(sc.TaskID),
		"schedule_mode":        strings.TrimSpace(sc.Mode),
	}
	if !sc.At.IsZero() {
		detail["scheduled_fire_at"] = sc.At.UTC().Format(time.RFC3339Nano)
	}
	if r.Failure != nil {
		detail["failure"] = *r.Failure
	}
	return detail
}

func (r startupTimerRecoveryResult) level() diaglog.Level {
	if r.Outcome == startupTimerRecoveryOutcomeDropped {
		return diaglog.LevelWarn
	}
	return diaglog.LevelInfo
}

func (r startupTimerRecoveryResult) message() string {
	switch r.Outcome {
	case startupTimerRecoveryOutcomeDropped:
		return "Startup recovery dropped persisted timer"
	case startupTimerRecoveryOutcomeSkipped:
		return "Startup recovery skipped persisted timer"
	default:
		return "Startup recovery replayed persisted timer"
	}
}

func logStartupTimerRecoveryAftermath(ctx context.Context, logger *RuntimeLogger, result startupTimerRecoveryResult) {
	if logger == nil {
		return
	}
	entry := RuntimeLogEntry{
		Level:     result.level(),
		Message:   result.message(),
		Component: "scheduler",
		Action:    startupTimerRecoveryAction,
		EventType: strings.TrimSpace(result.Schedule.EventType),
		AgentID:   strings.TrimSpace(result.Schedule.AgentID),
		EntityID:  result.Schedule.EffectiveEntityID(),
		Detail:    result.detail(),
		Failure:   runtimefailures.CloneEnvelope(result.Failure),
	}
	handleRuntimeLogPersistenceError("scheduler", startupTimerRecoveryAction, logger.Log(ctx, entry))
}

func restoreStartupTimerSchedule(ctx context.Context, store runtimepipeline.SchedulePersistence, scheduler *runtimepipeline.Scheduler, logger *RuntimeLogger, sc runtimepipeline.Schedule) startupTimerRecoveryResult {
	claimed, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, scheduler, sc)
	switch {
	case err != nil:
		failure := runtimefailures.Normalize(runtimefailures.Wrap(
			runtimefailures.ClassDependencyUnavailable,
			"schedule_restore_failed",
			"scheduler",
			"restore_startup_timer",
			map[string]any{"event_type": strings.TrimSpace(sc.EventType), "task_id": strings.TrimSpace(sc.TaskID)},
			err,
		), "scheduler", "restore_startup_timer")
		result := startupTimerRecoveryResult{
			Schedule:   sc,
			Outcome:    startupTimerRecoveryOutcomeDropped,
			ReasonCode: startupTimerRecoveryReasonRestoreFailed,
			Failure:    &failure,
		}
		logStartupTimerRecoveryAftermath(ctx, logger, result)
		return result
	case !claimed:
		result := startupTimerRecoveryResult{
			Schedule:   sc,
			Outcome:    startupTimerRecoveryOutcomeSkipped,
			ReasonCode: startupTimerRecoveryReasonClaimNotAcquired,
		}
		logStartupTimerRecoveryAftermath(ctx, logger, result)
		return result
	default:
		result := startupTimerRecoveryResult{
			Schedule:   sc,
			Outcome:    startupTimerRecoveryOutcomeReplayed,
			ReasonCode: startupTimerRecoveryReasonRestored,
		}
		logStartupTimerRecoveryAftermath(ctx, logger, result)
		return result
	}
}

func restoreStartupTimerSchedules(ctx context.Context, store runtimepipeline.SchedulePersistence, scheduler *runtimepipeline.Scheduler, logger *RuntimeLogger, schedules []runtimepipeline.Schedule) []startupTimerRecoveryResult {
	results := make([]startupTimerRecoveryResult, 0, len(schedules))
	for _, sc := range schedules {
		results = append(results, restoreStartupTimerSchedule(ctx, store, scheduler, logger, sc))
	}
	return results
}

func summarizeStartupTimerRecovery(results []startupTimerRecoveryResult) (replayed, skipped, dropped int, firstFailure *runtimefailures.Envelope) {
	for _, result := range results {
		switch result.Outcome {
		case startupTimerRecoveryOutcomeReplayed:
			replayed++
		case startupTimerRecoveryOutcomeSkipped:
			skipped++
		case startupTimerRecoveryOutcomeDropped:
			dropped++
			if firstFailure == nil && result.Failure != nil {
				firstFailure = runtimefailures.CloneEnvelope(result.Failure)
			}
		}
	}
	if dropped > 0 && firstFailure == nil {
		fallback := runtimefailures.Normalize(runtimefailures.New(
			runtimefailures.ClassInternalFailure,
			"schedule_restore_dropped_without_failure",
			"scheduler",
			"summarize_startup_timer_recovery",
			map[string]any{"dropped_count": dropped},
		), "scheduler", "summarize_startup_timer_recovery")
		firstFailure = &fallback
	}
	return replayed, skipped, dropped, firstFailure
}
