package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
	"swarm/internal/testutil"
)

type startupRecoveryTestLease struct{}

func (startupRecoveryTestLease) Release(context.Context) error { return nil }

type startupRecoveryManagerStore struct {
	loadErr error
}

func (s startupRecoveryManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s startupRecoveryManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return nil, nil
}

func (startupRecoveryManagerStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (startupRecoveryManagerStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (startupRecoveryManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, string) error {
	return nil
}
func (startupRecoveryManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (startupRecoveryManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type startupRecoveryCapabilityEventStore struct{}

func (startupRecoveryCapabilityEventStore) CanonicalRuntimeLogCapability(context.Context) (bool, bool, error) {
	return true, true, nil
}

func (startupRecoveryCapabilityEventStore) AppendEvent(context.Context, events.Event) error {
	return nil
}

func (startupRecoveryCapabilityEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (startupRecoveryCapabilityEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (startupRecoveryCapabilityEventStore) SupportsPersistedReplay() bool { return false }

type startupRecoveryEventStore struct {
	missing  []events.PersistedReplayEvent
	claimErr error
}

func (startupRecoveryEventStore) CanonicalRuntimeLogCapability(context.Context) (bool, bool, error) {
	return true, true, nil
}

func (startupRecoveryEventStore) AppendEvent(context.Context, events.Event) error { return nil }

func (startupRecoveryEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (startupRecoveryEventStore) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}

func (startupRecoveryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s startupRecoveryEventStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}

func (s startupRecoveryEventStore) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	if s.claimErr != nil {
		return nil, false, s.claimErr
	}
	return startupRecoveryTestLease{}, true, nil
}

func (startupRecoveryEventStore) SupportsPersistedReplay() bool { return true }

func testRecoveryDiagnosticsConfig(recoveryOnStartup bool) *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: recoveryOnStartup,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
	}
}

func latestStartupRecoveryDecisionLog(t *testing.T, db *sql.DB) (level, message, errorText string, detail map[string]any) {
	t.Helper()
	var payloadRaw []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->'details'->>'component' = 'runtime'
		  AND payload->'details'->>'action' = 'startup_recovery_decision'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&payloadRaw); err != nil {
		t.Fatalf("load startup recovery decision runtime log: %v", err)
	}
	payload, err := DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload: %v", err)
	}
	return payload.LogLevel, payload.Message, payload.Error, payload.Detail
}

func detailString(v any) string {
	switch typed := v.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(asString(v))
	}
}

func detailBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func detailInt(v any) int {
	switch typed := v.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}

func detailClasses(v any) []string {
	out := []string{}
	switch typed := v.(type) {
	case []string:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	case []any:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	}
	return out
}

func assertContainsClass(t *testing.T, classes []string, want string) {
	t.Helper()
	for _, item := range classes {
		if item == want {
			return
		}
	}
	t.Fatalf("recoverable_work_classes = %#v, want %q present", classes, want)
}

func TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{{
			AgentID:   "runtime",
			EventType: "timer.check",
			Mode:      "once",
			At:        time.Now().Add(time.Minute),
			TaskID:    "recover-me",
		}},
	}

	rt, err := NewRuntime(ctx, testRecoveryDiagnosticsConfig(false), Stores{
		SQLDB:         db,
		EventStore:    startupRecoveryCapabilityEventStore{},
		ManagerStore:  &recoveryGuardManagerStore{},
		ScheduleStore: scheduleStore,
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") {
		t.Fatalf("Start error = %v, want explicit startup denial", err)
	}

	level, _, errorText, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	if errorText != err.Error() {
		t.Fatalf("log error = %q, want %q", errorText, err.Error())
	}
	if got := detailString(detail["decision_outcome"]); got != "denied" {
		t.Fatalf("decision_outcome = %q, want denied", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithWork)
	}
	if got := detailInt(detail["active_schedule_count"]); got != 1 {
		t.Fatalf("active_schedule_count = %d, want 1", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "active schedules")
}

func TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForReplayEligibleWork(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: events.Event{ID: "evt-1", Type: "support.item_created"},
		}},
	}

	rt, err := NewRuntime(ctx, testRecoveryDiagnosticsConfig(false), Stores{
		SQLDB:        db,
		EventStore:   eventStore,
		ManagerStore: &recoveryGuardManagerStore{},
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "events missing pipeline receipts") {
		t.Fatalf("Start error = %v, want replay-eligible denial", err)
	}

	level, _, _, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	if got := detailString(detail["decision_outcome"]); got != "denied" {
		t.Fatalf("decision_outcome = %q, want denied", got)
	}
	if !detailBool(detail["replay_eligible_event_present"]) {
		t.Fatalf("replay_eligible_event_present = %#v, want true", detail["replay_eligible_event_present"])
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "events missing pipeline receipts")
}

func TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{{
			AgentID:   "runtime",
			EventType: "timer.check",
			Mode:      "once",
			At:        time.Now().Add(time.Minute),
			TaskID:    "recover-me",
		}},
	}

	rt, err := NewRuntime(ctx, testRecoveryDiagnosticsConfig(true), Stores{
		SQLDB:         db,
		EventStore:    startupRecoveryCapabilityEventStore{},
		ManagerStore:  &recoveryGuardManagerStore{},
		ScheduleStore: scheduleStore,
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, errorText, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "info" {
		t.Fatalf("log level = %q, want info", level)
	}
	if errorText != "" {
		t.Fatalf("log error = %q, want empty", errorText)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonEnabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonEnabledWithWork)
	}
	if !detailBool(detail["schedule_restore_attempted"]) {
		t.Fatalf("schedule_restore_attempted = %#v, want true", detail["schedule_restore_attempted"])
	}
	if got := detailInt(detail["schedule_restore_success_count"]); got != 1 {
		t.Fatalf("schedule_restore_success_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_restore_failure_count"]); got != 0 {
		t.Fatalf("schedule_restore_failure_count = %d, want 0", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "active schedules")
}

func TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: events.Event{ID: "evt-1", Type: "support.item_created"},
		}},
		claimErr: errors.New("claim failed"),
	}

	rt, err := NewRuntime(ctx, testRecoveryDiagnosticsConfig(true), Stores{
		SQLDB:        db,
		EventStore:   eventStore,
		ManagerStore: &recoveryGuardManagerStore{},
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, errorText, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	if !strings.Contains(errorText, "claim replay event evt-1: claim failed") {
		t.Fatalf("log error = %q, want degraded recovery error", errorText)
	}
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
	if !detailBool(detail["manager_reset_attempted"]) {
		t.Fatalf("manager_reset_attempted = %#v, want true", detail["manager_reset_attempted"])
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "events missing pipeline receipts")
}

func TestRuntimeStart_RecoveryInspectionFailureDoesNotBlockRecoveryEnabledStartup(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)

	rt, err := NewRuntime(ctx, testRecoveryDiagnosticsConfig(true), Stores{
		SQLDB:        db,
		EventStore:   startupRecoveryCapabilityEventStore{},
		ManagerStore: startupRecoveryManagerStore{loadErr: errors.New("load agents failed")},
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, errorText, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	if !strings.Contains(errorText, "load agents: load agents failed") {
		t.Fatalf("log error = %q, want manager recovery failure detail", errorText)
	}
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if got := detailBool(detail["recovery_inspection_complete"]); got {
		t.Fatalf("recovery_inspection_complete = %#v, want false", detail["recovery_inspection_complete"])
	}
	if got := detailString(detail["recovery_inspection_error"]); !strings.Contains(got, "inspect recoverable manager state: load persisted agents: load agents failed") {
		t.Fatalf("recovery_inspection_error = %q, want startup inspection failure detail", got)
	}
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
}
