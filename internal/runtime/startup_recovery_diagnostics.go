package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type startupRecoveryOutcome string

const (
	startupRecoveryOutcomeAllowed  startupRecoveryOutcome = "allowed"
	startupRecoveryOutcomeDenied   startupRecoveryOutcome = "denied"
	startupRecoveryOutcomeDegraded startupRecoveryOutcome = "degraded"
)

type startupRecoveryReasonCode string

const (
	startupRecoveryReasonDisabledNoWork          startupRecoveryReasonCode = "recovery_disabled_no_persisted_work"
	startupRecoveryReasonDisabledWithWork        startupRecoveryReasonCode = "recovery_disabled_with_persisted_work"
	startupRecoveryReasonDisabledWithDelivery    startupRecoveryReasonCode = "recovery_disabled_with_delivery_work"
	startupRecoveryReasonDisabledWithIntrinsic   startupRecoveryReasonCode = "recovery_disabled_with_intrinsic_timer_work"
	startupRecoveryReasonDisabledWithManagerWork startupRecoveryReasonCode = "recovery_disabled_with_manager_snapshot_work"
	startupRecoveryReasonEnabledNoWork           startupRecoveryReasonCode = "recovery_enabled_no_persisted_work"
	startupRecoveryReasonEnabledWithWork         startupRecoveryReasonCode = "recovery_enabled_with_persisted_work"
	startupRecoveryReasonInspectFailed           startupRecoveryReasonCode = "startup_recovery_inspection_failed"
	startupRecoveryReasonScheduleRestore         startupRecoveryReasonCode = "schedule_restore_failed"
	startupRecoveryReasonRecoverFailed           startupRecoveryReasonCode = "startup_recovery_failed"
)

type startupRecoverySnapshot struct {
	RecoveryOnStartup             bool
	InspectionComplete            bool
	StartupBlockingTimers         int
	StartupBlockingWorkflowTimers int
	StandingTimerObligations      int
	StandingDeliveryObligations   int
	TimerObligations              runtimetimerobligation.Snapshot
	Manager                       runtimemanager.RecoverableStateSnapshot
	Delivery                      runtimedelivery.RecoveryInventory
}

func canonicalTimerObligationSnapshot(snapshot runtimetimerobligation.Snapshot) runtimetimerobligation.Snapshot {
	if snapshot.GlobalFamilies == nil {
		snapshot.GlobalFamilies = []runtimetimerobligation.FamilyObligation{}
	}
	if snapshot.Runs == nil {
		snapshot.Runs = []runtimetimerobligation.RunObligations{}
	}
	if snapshot.Activations == nil {
		snapshot.Activations = []runtimetimerobligation.Activation{}
	}
	return snapshot
}

func (s startupRecoverySnapshot) HasRecoverableWork() bool {
	return s.StartupBlockingTimers > 0 || s.StandingTimerObligations > 0 || s.StandingDeliveryObligations > 0 ||
		s.Manager.HasRecoverableWork() || s.Delivery.HasWork()
}

func (s startupRecoverySnapshot) HasStartupBlockingRecoverableWork() bool {
	return s.StartupBlockingTimers > 0 || s.Delivery.HasWork()
}

func (s startupRecoverySnapshot) WorkClasses() []string {
	classes := make([]string, 0, 2+len(s.Manager.Classes()))
	if s.StartupBlockingTimers > 0 || s.StandingTimerObligations > 0 {
		classes = append(classes, "timer obligations")
	}
	if s.Delivery.HasWork() || s.StandingDeliveryObligations > 0 {
		classes = append(classes, "executable delivery obligations")
	}
	classes = append(classes, s.Manager.Classes()...)
	sort.Strings(classes)
	return classes
}

func (s startupRecoverySnapshot) StartupBlockingWorkClasses() []string {
	classes := make([]string, 0, 2)
	if s.StartupBlockingTimers > 0 {
		classes = append(classes, "timer obligations")
	}
	if s.Delivery.HasWork() {
		classes = append(classes, "executable delivery obligations")
	}
	return classes
}

func (s startupRecoverySnapshot) Detail() map[string]any {
	detail := map[string]any{
		"recovery_on_startup":          s.RecoveryOnStartup,
		"recovery_inspection_complete": s.InspectionComplete,
	}
	if s.InspectionComplete {
		detail["startup_blocking_timer_count"] = s.StartupBlockingTimers
		detail["startup_blocking_workflow_timer_count"] = s.StartupBlockingWorkflowTimers
		detail["standing_timer_obligation_count"] = s.StandingTimerObligations
		detail["standing_delivery_obligation_count"] = s.StandingDeliveryObligations
		detail["timer_obligations"] = canonicalTimerObligationSnapshot(s.TimerObligations)
		detail["recoverable_work_present"] = s.HasRecoverableWork()
		detail["startup_blocking_recoverable_work_present"] = s.HasStartupBlockingRecoverableWork()
		detail["manager_recoverable_work_present"] = s.Manager.HasRecoverableWork()
		detail["delivery_recovery_inventory"] = s.Delivery
		detail["delivery_recoverable_work_present"] = s.Delivery.HasWork()
		detail["recoverable_work_classes"] = s.WorkClasses()
		detail["startup_blocking_recoverable_work_classes"] = s.StartupBlockingWorkClasses()
		for key, value := range s.Manager.Detail() {
			detail[key] = value
		}
	}
	return detail
}

func (s startupRecoverySnapshot) summary() string {
	if !s.InspectionComplete {
		return "inspection incomplete"
	}
	classes := s.WorkClasses()
	if len(classes) == 0 {
		return "no recovery state"
	}
	return strings.Join(classes, ", ")
}

type startupRecoveryDecisionReport struct {
	Snapshot startupRecoverySnapshot

	Outcome                  startupRecoveryOutcome
	ReasonCode               startupRecoveryReasonCode
	Failure                  *runtimefailures.Envelope
	ScheduleRestoreAttempted bool
	ScheduleReplayCount      int
	ScheduleSkipCount        int
	ScheduleDropCount        int
	ManagerRecoveryAttempted bool
	ManagerReplayCount       int
	ManagerSkipCount         int
	ManagerDropCount         int
	ManagerResetAttempted    bool
	ManagerResetFailure      *runtimefailures.Envelope
	InspectionFailure        *runtimefailures.Envelope
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
	case snapshot.HasStartupBlockingRecoverableWork():
		report.Outcome = startupRecoveryOutcomeDenied
		if snapshot.Delivery.HasWork() {
			report.ReasonCode = startupRecoveryReasonDisabledWithDelivery
		} else {
			report.ReasonCode = startupRecoveryReasonDisabledWithWork
		}
	case snapshot.Manager.HasRecoverableWork():
		report.ReasonCode = startupRecoveryReasonDisabledWithManagerWork
	case snapshot.StandingTimerObligations > 0 || snapshot.StandingDeliveryObligations > 0:
		report.ReasonCode = startupRecoveryReasonDisabledWithIntrinsic
	default:
		report.ReasonCode = startupRecoveryReasonDisabledNoWork
	}
	return report
}

func (r startupRecoveryDecisionReport) denialError() error {
	if r.Outcome != startupRecoveryOutcomeDenied {
		return nil
	}
	return fmt.Errorf("runtime.recovery_on_startup=false but persisted runtime-owned work exists: %s", strings.Join(r.Snapshot.StartupBlockingWorkClasses(), ", "))
}

func (r startupRecoveryDecisionReport) message() string {
	switch r.Outcome {
	case startupRecoveryOutcomeDenied:
		return "Runtime startup denied by recovery admission"
	case startupRecoveryOutcomeDegraded:
		return "Runtime startup recovery completed in a degraded state"
	case startupRecoveryOutcomeAllowed:
		if r.ReasonCode == startupRecoveryReasonDisabledWithManagerWork {
			return "Runtime startup allowed with manager recovery skipped"
		}
		if r.ReasonCode == startupRecoveryReasonDisabledWithIntrinsic {
			return "Runtime startup allowed with intrinsic timer restoration"
		}
		return "Runtime startup recovery decision recorded"
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
		if r.ReasonCode == startupRecoveryReasonDisabledWithManagerWork {
			return "warn"
		}
		return "info"
	}
}

func (r startupRecoveryDecisionReport) detail() map[string]any {
	detail := r.Snapshot.Detail()
	detail["decision_outcome"] = string(r.Outcome)
	detail["decision_reason_code"] = string(r.ReasonCode)
	detail["schedule_restore_attempted"] = r.ScheduleRestoreAttempted
	detail["schedule_replayed_count"] = r.ScheduleReplayCount
	detail["schedule_skipped_count"] = r.ScheduleSkipCount
	detail["schedule_dropped_count"] = r.ScheduleDropCount
	detail["manager_recovery_attempted"] = r.ManagerRecoveryAttempted
	detail["manager_replayed_count"] = r.ManagerReplayCount
	detail["manager_skipped_count"] = r.ManagerSkipCount
	detail["manager_dropped_count"] = r.ManagerDropCount
	detail["manager_reset_attempted"] = r.ManagerResetAttempted
	if r.Failure != nil {
		detail["failure"] = *r.Failure
	}
	if r.InspectionFailure != nil {
		detail["recovery_inspection_failure"] = *r.InspectionFailure
	}
	if r.ManagerResetFailure != nil {
		detail["manager_reset_failure"] = *r.ManagerResetFailure
	}
	return detail
}

func (r startupRecoveryDecisionReport) bootPayload() map[string]any {
	payload := map[string]any{
		"outcome":                      string(r.Outcome),
		"reason_code":                  string(r.ReasonCode),
		"recovery_on_startup":          r.Snapshot.RecoveryOnStartup,
		"recovery_inspection_complete": r.Snapshot.InspectionComplete,
		"schedule_replay_count":        r.ScheduleReplayCount,
		"schedule_skip_count":          r.ScheduleSkipCount,
		"schedule_drop_count":          r.ScheduleDropCount,
		"manager_replay_count":         r.ManagerReplayCount,
		"manager_skip_count":           r.ManagerSkipCount,
		"manager_drop_count":           r.ManagerDropCount,
	}
	if r.Failure != nil {
		payload["failure"] = *runtimefailures.CloneEnvelope(r.Failure)
	}
	if r.Snapshot.InspectionComplete {
		payload["startup_blocking_timer_count"] = r.Snapshot.StartupBlockingTimers
		payload["standing_timer_obligation_count"] = r.Snapshot.StandingTimerObligations
		payload["timer_obligations"] = canonicalTimerObligationSnapshot(r.Snapshot.TimerObligations)
		payload["delivery_recovery_inventory"] = r.Snapshot.Delivery
	}
	return payload
}

func (rt *Runtime) inspectStartupRecoverySnapshot(ctx context.Context, observedAt time.Time) (startupRecoverySnapshot, error) {
	snapshot := startupRecoverySnapshot{
		RecoveryOnStartup:  rt != nil && rt.Config != nil && rt.Config.Runtime.RecoveryOnStartup,
		InspectionComplete: true,
	}
	if rt == nil {
		return snapshot, nil
	}
	delivery, standingDelivery, err := rt.inspectDeliveryRecoveryInventory(ctx)
	if err != nil {
		snapshot.InspectionComplete = false
		return snapshot, err
	}
	snapshot.Delivery = delivery
	snapshot.StandingDeliveryObligations = standingDelivery
	reader, err := rt.startupTimerObligationReader()
	if err != nil {
		snapshot.InspectionComplete = false
		return snapshot, err
	}
	if reader != nil {
		obligations, err := reader.ReadTimerObligations(ctx, runtimetimerobligation.All(), observedAt)
		if err != nil {
			snapshot.InspectionComplete = false
			return snapshot, fmt.Errorf("inspect timer obligations: %w", err)
		}
		snapshot.TimerObligations = obligations
		snapshot.StartupBlockingTimers += obligations.GlobalTotals().RecoverableCount
		for _, run := range obligations.Runs {
			recoverable := run.Totals().RecoverableCount
			if recoverable == 0 {
				continue
			}
			workflowTimers := 0
			for _, family := range run.Families {
				if family.Family == runtimetimerobligation.FamilyWorkflowTimer {
					workflowTimers = family.RecoverableCount
					break
				}
			}
			if rt.Pipeline == nil {
				snapshot.InspectionComplete = false
				return snapshot, errors.New("classify timer obligations: standing restart disposition reader is required")
			}
			disposition, err := rt.Pipeline.StandingRunRestartDisposition(ctx, run.RunID)
			if err != nil {
				snapshot.InspectionComplete = false
				return snapshot, fmt.Errorf("classify standing timer obligations: %w", err)
			}
			if disposition.Executable() {
				snapshot.StandingTimerObligations += recoverable
				continue
			}
			if disposition.ExactCurrent() {
				continue
			}
			snapshot.StartupBlockingTimers += recoverable
			snapshot.StartupBlockingWorkflowTimers += workflowTimers
		}
	}
	if rt.Manager != nil {
		managerSnapshot, err := rt.Manager.RecoverableStateSnapshot(ctx)
		if err != nil {
			snapshot.InspectionComplete = false
			return snapshot, fmt.Errorf("inspect recoverable manager state: %w", err)
		}
		snapshot.Manager = managerSnapshot
	}
	return snapshot, nil
}

func (rt *Runtime) inspectDeliveryRecoveryInventory(ctx context.Context) (runtimedelivery.RecoveryInventory, int, error) {
	if rt == nil || rt.deliveryStore == nil {
		return runtimedelivery.RecoveryInventory{}, 0, nil
	}
	inventory, err := rt.deliveryStore.InspectDeliveryRecovery(ctx, rt.Options.BundleSourceFact)
	if err != nil {
		return runtimedelivery.RecoveryInventory{}, 0, fmt.Errorf("inspect executable delivery recovery: %w", err)
	}
	return partitionDeliveryRecoveryInventory(ctx, inventory, rt.Pipeline)
}

func partitionDeliveryRecoveryInventory(
	ctx context.Context,
	inventory runtimedelivery.RecoveryInventory,
	restarts runtimepipeline.StandingRestartDispositionReader,
) (runtimedelivery.RecoveryInventory, int, error) {
	if !inventory.HasWork() {
		return runtimedelivery.RecoveryInventory{}, 0, nil
	}
	if restarts == nil {
		return runtimedelivery.RecoveryInventory{}, 0, errors.New("classify delivery obligations: standing restart disposition reader is required")
	}
	blocking := runtimedelivery.RecoveryInventory{}
	standing := 0
	for _, run := range inventory.Runs {
		disposition, err := restarts.StandingRunRestartDisposition(ctx, run.RunID)
		if err != nil {
			return runtimedelivery.RecoveryInventory{}, 0, fmt.Errorf("classify standing delivery obligations for run %s: %w", run.RunID, err)
		}
		switch {
		case disposition.UsesGenericRecovery():
			blocking.Runs = append(blocking.Runs, run)
		case disposition.Executable():
			standing += run.Total()
		}
	}
	return blocking, standing, nil
}

func (rt *Runtime) startupTimerObligationReader() (runtimetimerobligation.Reader, error) {
	if rt == nil {
		return nil, nil
	}
	if rt.GenericSchedules != nil {
		reader := rt.timerObligationReader
		if reader == nil {
			return nil, fmt.Errorf("inspect timer obligations: selected schedule store lacks timer obligation authority")
		}
		return reader, nil
	}
	if rt.Pipeline != nil {
		return rt.Pipeline, nil
	}
	return nil, nil
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
		Failure:   runtimefailures.CloneEnvelope(report.Failure),
		Detail:    report.detail(),
	}
	handleRuntimeLogPersistenceError("runtime", "startup_recovery_decision", rt.Logger.Log(ctx, entry))
}

func newStartupRecoveryFailure(class runtimefailures.Class, detailCode, operation string, attributes map[string]any, cause error) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.Wrap(class, detailCode, "runtime", operation, attributes, cause), "runtime", operation)
	return &failure
}
