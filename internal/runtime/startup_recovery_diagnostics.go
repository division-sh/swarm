package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"swarm/internal/runtime/diaglog"
	runtimemanager "swarm/internal/runtime/manager"
)

type startupRecoveryOutcome string

const (
	startupRecoveryOutcomeAllowed  startupRecoveryOutcome = "allowed"
	startupRecoveryOutcomeDenied   startupRecoveryOutcome = "denied"
	startupRecoveryOutcomeDegraded startupRecoveryOutcome = "degraded"
)

type startupRecoveryReasonCode string

const (
	startupRecoveryReasonDisabledNoWork   startupRecoveryReasonCode = "recovery_disabled_no_persisted_work"
	startupRecoveryReasonDisabledWithWork startupRecoveryReasonCode = "recovery_disabled_with_persisted_work"
	startupRecoveryReasonEnabledNoWork    startupRecoveryReasonCode = "recovery_enabled_no_persisted_work"
	startupRecoveryReasonEnabledWithWork  startupRecoveryReasonCode = "recovery_enabled_with_persisted_work"
	startupRecoveryReasonScheduleRestore  startupRecoveryReasonCode = "schedule_restore_failed"
	startupRecoveryReasonRecoverFailed    startupRecoveryReasonCode = "startup_recovery_failed"
)

type startupRecoverySnapshot struct {
	RecoveryOnStartup   bool
	ActiveScheduleCount int
	Manager             runtimemanager.RecoverableStateSnapshot
}

func (s startupRecoverySnapshot) HasRecoverableWork() bool {
	return s.ActiveScheduleCount > 0 || s.Manager.HasRecoverableWork()
}

func (s startupRecoverySnapshot) WorkClasses() []string {
	classes := make([]string, 0, 1+len(s.Manager.Classes()))
	if s.ActiveScheduleCount > 0 {
		classes = append(classes, "active schedules")
	}
	classes = append(classes, s.Manager.Classes()...)
	sort.Strings(classes)
	return classes
}

func (s startupRecoverySnapshot) Detail() map[string]any {
	detail := map[string]any{
		"recovery_on_startup":      s.RecoveryOnStartup,
		"active_schedule_count":    s.ActiveScheduleCount,
		"recoverable_work_present": s.HasRecoverableWork(),
		"recoverable_work_classes": s.WorkClasses(),
	}
	for key, value := range s.Manager.Detail() {
		detail[key] = value
	}
	return detail
}

type startupRecoveryDecisionReport struct {
	Snapshot startupRecoverySnapshot

	Outcome                  startupRecoveryOutcome
	ReasonCode               startupRecoveryReasonCode
	ErrorText                string
	ScheduleRestoreAttempted bool
	ScheduleRestoreSuccesses int
	ScheduleRestoreFailures  int
	ManagerRecoveryAttempted bool
	ManagerResetAttempted    bool
	ManagerResetError        string
}

func newStartupRecoveryDecisionReport(snapshot startupRecoverySnapshot) startupRecoveryDecisionReport {
	report := startupRecoveryDecisionReport{
		Snapshot: snapshot,
		Outcome:  startupRecoveryOutcomeAllowed,
	}
	switch {
	case snapshot.RecoveryOnStartup && snapshot.HasRecoverableWork():
		report.ReasonCode = startupRecoveryReasonEnabledWithWork
	case snapshot.RecoveryOnStartup:
		report.ReasonCode = startupRecoveryReasonEnabledNoWork
	case snapshot.HasRecoverableWork():
		report.Outcome = startupRecoveryOutcomeDenied
		report.ReasonCode = startupRecoveryReasonDisabledWithWork
	default:
		report.ReasonCode = startupRecoveryReasonDisabledNoWork
	}
	return report
}

func (r startupRecoveryDecisionReport) denialError() error {
	if r.Outcome != startupRecoveryOutcomeDenied {
		return nil
	}
	return fmt.Errorf("runtime.recovery_on_startup=false but persisted runtime-owned work exists: %s", strings.Join(r.Snapshot.WorkClasses(), ", "))
}

func (r startupRecoveryDecisionReport) message() string {
	switch r.Outcome {
	case startupRecoveryOutcomeDenied:
		return "Runtime startup denied by recovery admission"
	case startupRecoveryOutcomeDegraded:
		return "Runtime startup recovery completed in a degraded state"
	default:
		return "Runtime startup recovery decision recorded"
	}
}

func (r startupRecoveryDecisionReport) level() string {
	switch r.Outcome {
	case startupRecoveryOutcomeDenied:
		return "warn"
	case startupRecoveryOutcomeDegraded:
		return "error"
	default:
		return "info"
	}
}

func (r startupRecoveryDecisionReport) detail() map[string]any {
	detail := r.Snapshot.Detail()
	detail["decision_outcome"] = string(r.Outcome)
	detail["decision_reason_code"] = string(r.ReasonCode)
	detail["schedule_restore_attempted"] = r.ScheduleRestoreAttempted
	detail["schedule_restore_success_count"] = r.ScheduleRestoreSuccesses
	detail["schedule_restore_failure_count"] = r.ScheduleRestoreFailures
	detail["manager_recovery_attempted"] = r.ManagerRecoveryAttempted
	detail["manager_reset_attempted"] = r.ManagerResetAttempted
	if errText := strings.TrimSpace(r.ErrorText); errText != "" {
		detail["error"] = errText
	}
	if resetErr := strings.TrimSpace(r.ManagerResetError); resetErr != "" {
		detail["manager_reset_error"] = resetErr
	}
	return detail
}

func (rt *Runtime) inspectStartupRecoverySnapshot(ctx context.Context) (startupRecoverySnapshot, error) {
	snapshot := startupRecoverySnapshot{
		RecoveryOnStartup: rt != nil && rt.Config != nil && rt.Config.Runtime.RecoveryOnStartup,
	}
	if rt == nil {
		return snapshot, nil
	}
	if rt.Stores.ScheduleStore != nil {
		schedules, err := rt.Stores.ScheduleStore.LoadActiveSchedules(ctx)
		if err != nil {
			return startupRecoverySnapshot{}, fmt.Errorf("inspect active schedules: %w", err)
		}
		snapshot.ActiveScheduleCount = len(schedules)
	}
	if rt.Manager != nil {
		managerSnapshot, err := rt.Manager.RecoverableStateSnapshot(ctx)
		if err != nil {
			return startupRecoverySnapshot{}, fmt.Errorf("inspect recoverable manager state: %w", err)
		}
		snapshot.Manager = managerSnapshot
	}
	return snapshot, nil
}

func (rt *Runtime) logStartupRecoveryDecision(ctx context.Context, report startupRecoveryDecisionReport) {
	if rt == nil || rt.Logger == nil {
		return
	}
	entry := RuntimeLogEntry{
		Level:     diaglog.Level(report.level()),
		Message:   report.message(),
		Component: "runtime",
		Action:    "startup_recovery_decision",
		Error:     strings.TrimSpace(report.ErrorText),
		Detail:    report.detail(),
	}
	if strings.TrimSpace(entry.Error) != "" {
		entry.Detail.(map[string]any)["error_code"] = string(report.ReasonCode)
	}
	handleRuntimeLogPersistenceError("runtime", "startup_recovery_decision", rt.Logger.Log(ctx, entry))
}
