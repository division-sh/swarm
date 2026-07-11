package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
)

type startupRecoveryTestLease struct{}

func (startupRecoveryTestLease) Release(context.Context) error { return nil }

type startupRecoveryManagerStore struct {
	loadErr error
	agents  []runtimemanager.PersistedAgent
}

func startupRecoveryLifecycleResult(req runtimemanager.AgentLifecycleTransition) runtimemanager.AgentLifecycleTransitionResult {
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: req.OperationID, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
	}
}

func (startupRecoveryManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s startupRecoveryManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s startupRecoveryManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (startupRecoveryManagerStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (startupRecoveryManagerStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (startupRecoveryManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}
func (startupRecoveryManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (startupRecoveryManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type startupRecoveryFlakyManagerStore struct {
	remainingFailures int
	loadErr           error
}

func (*startupRecoveryFlakyManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s *startupRecoveryFlakyManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *startupRecoveryFlakyManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.remainingFailures > 0 {
		s.remainingFailures--
		if s.loadErr != nil {
			return nil, s.loadErr
		}
	}
	return nil, nil
}

func (*startupRecoveryFlakyManagerStore) MarkAgentTerminated(context.Context, string) error {
	return nil
}
func (*startupRecoveryFlakyManagerStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}
func (*startupRecoveryFlakyManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}
func (*startupRecoveryFlakyManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*startupRecoveryFlakyManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type startupManagerReplayRuntimeStore struct {
	agents   []runtimemanager.PersistedAgent
	pending  map[string][]events.Event
	receipts map[string]runtimemanager.EventReceipt
}

func (*startupManagerReplayRuntimeStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (*startupManagerReplayRuntimeStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *startupManagerReplayRuntimeStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*startupManagerReplayRuntimeStore) MarkAgentTerminated(context.Context, string) error {
	return nil
}
func (*startupManagerReplayRuntimeStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}
func (s *startupManagerReplayRuntimeStore) UpsertEventReceipt(_ context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) error {
	if s.receipts == nil {
		s.receipts = map[string]runtimemanager.EventReceipt{}
	}
	s.receipts[strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)] = runtimemanager.EventReceipt{
		EventID: eventID,
		AgentID: agentID,
		Status:  status,
		Failure: failure,
	}
	return nil
}
func (s *startupManagerReplayRuntimeStore) ListPendingEventsForAgent(_ context.Context, agentID string, _ time.Time, _ int) ([]events.Event, error) {
	return append([]events.Event(nil), s.pending[strings.TrimSpace(agentID)]...), nil
}
func (*startupManagerReplayRuntimeStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *startupManagerReplayRuntimeStore) GetEventReceipt(_ context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	receipt, ok := s.receipts[strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)]
	return receipt, ok, nil
}

type startupManagerReplayRuntimeAgent struct{ id string }

func (a startupManagerReplayRuntimeAgent) ID() string                      { return a.id }
func (startupManagerReplayRuntimeAgent) Type() string                      { return "generic" }
func (startupManagerReplayRuntimeAgent) Subscriptions() []events.EventType { return nil }
func (startupManagerReplayRuntimeAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	switch evt.Type() {
	case events.EventType("support.replay.drop"):
		return nil, errors.New("boom")
	case events.EventType("support.replay.leased"):
		return nil, errors.New("session currently leased")
	default:
		return nil, nil
	}
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
	routes   []runtimeflowidentity.Route
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

func (startupRecoveryEventStore) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}

func (startupRecoveryEventStore) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (s startupRecoveryEventStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return append([]runtimeflowidentity.Route(nil), s.routes...), nil
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
			Backend: "anthropic",
		},
	}
}

func latestStartupRecoveryDecisionLog(t *testing.T, db *sql.DB) (level, message string, failure *runtimefailures.Envelope, detail map[string]any) {
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
	return payload.LogLevel, payload.Message, payload.Failure, payload.Detail
}

type runtimeAftermathLog struct {
	level     string
	action    string
	failure   *runtimefailures.Envelope
	eventType string
	detail    map[string]any
}

func requireFailureCode(t testing.TB, failure *runtimefailures.Envelope, code string) {
	t.Helper()
	if failure == nil || failure.Detail.Code != code {
		t.Fatalf("failure = %#v, want detail code %q", failure, code)
	}
}

func requireNestedFailureCode(t testing.TB, detail map[string]any, key, code string) {
	t.Helper()
	raw, err := json.Marshal(detail[key])
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode %s: %v (value=%#v)", key, err, detail[key])
	}
	if failure.Detail.Code != code {
		t.Fatalf("%s failure = %#v, want detail code %q", key, failure, code)
	}
}

func listRuntimeLogsByAction(t *testing.T, db *sql.DB, action string) []runtimeAftermathLog {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->'details'->>'action' = $1
		ORDER BY created_at ASC
	`, action)
	if err != nil {
		t.Fatalf("query runtime logs by action: %v", err)
	}
	defer rows.Close()

	out := []runtimeAftermathLog{}
	for rows.Next() {
		var payloadRaw []byte
		if err := rows.Scan(&payloadRaw); err != nil {
			t.Fatalf("scan runtime log payload: %v", err)
		}
		payload, err := DecodeCanonicalRuntimeLogPayload(payloadRaw)
		if err != nil {
			t.Fatalf("DecodeCanonicalRuntimeLogPayload: %v", err)
		}
		out = append(out, runtimeAftermathLog{
			level:     payload.LogLevel,
			action:    payload.Action,
			failure:   payload.Failure,
			eventType: payload.EventType,
			detail:    payload.Detail,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("runtime log rows: %v", err)
	}
	return out
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

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") {
		t.Fatalf("Start error = %v, want explicit startup denial", err)
	}

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	requireFailureCode(t, failure, "startup_recovery_disabled_with_work")
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

func TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RootIngress("evt-1", "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		routes: []runtimeflowidentity.Route{
			runtimeflowidentity.DeriveRoute("child", "inst-1"),
		},
	}
	managerStore := &startupRecoveryManagerStore{
		agents: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ID: "persisted-agent"},
		}},
	}

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      eventStore,
		ManagerStore:    managerStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, message, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	if message != "Runtime startup allowed with manager recovery skipped" {
		t.Fatalf("log message = %q, want manager recovery skipped", message)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithManagerWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithManagerWork)
	}
	if !detailBool(detail["manager_recoverable_work_present"]) {
		t.Fatalf("manager_recoverable_work_present = %#v, want true", detail["manager_recoverable_work_present"])
	}
	if detailBool(detail["startup_blocking_recoverable_work_present"]) {
		t.Fatalf("startup_blocking_recoverable_work_present = %#v, want false", detail["startup_blocking_recoverable_work_present"])
	}
	if got := detailInt(detail["persisted_agent_count"]); got != 1 {
		t.Fatalf("persisted_agent_count = %d, want 1", got)
	}
	if got := detailInt(detail["persisted_flow_instance_route_count"]); got != 1 {
		t.Fatalf("persisted_flow_instance_route_count = %d, want 1", got)
	}
	if !detailBool(detail["replay_eligible_event_present"]) {
		t.Fatalf("replay_eligible_event_present = %#v, want true", detail["replay_eligible_event_present"])
	}
	classes := detailClasses(detail["recoverable_work_classes"])
	assertContainsClass(t, classes, "persisted agents")
	assertContainsClass(t, classes, "persisted flow instance routes")
	assertContainsClass(t, classes, "events missing pipeline receipts")
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

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "info" {
		t.Fatalf("log level = %q, want info", level)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
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
	if got := detailInt(detail["schedule_replayed_count"]); got != 1 {
		t.Fatalf("schedule_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 0 {
		t.Fatalf("schedule_skipped_count = %d, want 0", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 0 {
		t.Fatalf("schedule_dropped_count = %d, want 0", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "active schedules")
}

func TestRuntimeStart_RecoveryEnabledEmitsTimerRecoveryAftermathAndSummary(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:   "runtime",
				EventType: "timer.replay",
				Mode:      "once",
				At:        time.Now().Add(time.Minute),
				TaskID:    "replay-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.skip",
				Mode:      "once",
				At:        time.Now().Add(2 * time.Minute),
				TaskID:    "skip-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.drop",
				Mode:      "once",
				At:        time.Now().Add(3 * time.Minute),
				TaskID:    "drop-me",
			},
		},
		claims: []recordedScheduleClaim{
			{Claimed: true},
			{Claimed: false},
			{Err: errors.New("claim failed")},
		},
	}

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "schedule_restore_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonScheduleRestore) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonScheduleRestore)
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 1 {
		t.Fatalf("schedule_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 1 {
		t.Fatalf("schedule_skipped_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 1 {
		t.Fatalf("schedule_dropped_count = %d, want 1", got)
	}

	logs := listRuntimeLogsByAction(t, db, startupTimerRecoveryAction)
	if len(logs) != 3 {
		t.Fatalf("timer recovery runtime logs = %d, want 3", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing timer recovery log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}

	replayed := findByEventType("timer.replay")
	if replayed.level != "info" {
		t.Fatalf("replayed log level = %q, want info", replayed.level)
	}
	if got := detailString(replayed.detail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed decision_outcome = %q, want replayed", got)
	}
	if got := detailString(replayed.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonRestored) {
		t.Fatalf("replayed decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonRestored)
	}

	skipped := findByEventType("timer.skip")
	if skipped.level != "info" {
		t.Fatalf("skipped log level = %q, want info", skipped.level)
	}
	if got := detailString(skipped.detail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped decision_outcome = %q, want skipped", got)
	}
	if got := detailString(skipped.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonClaimNotAcquired) {
		t.Fatalf("skipped decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonClaimNotAcquired)
	}

	dropped := findByEventType("timer.drop")
	if dropped.level != "warn" {
		t.Fatalf("dropped log level = %q, want warn", dropped.level)
	}
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	if got := detailString(dropped.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonRestoreFailed) {
		t.Fatalf("dropped decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonRestoreFailed)
	}
	requireFailureCode(t, dropped.failure, "schedule_restore_failed")
}

func TestRuntimeStart_RecoveryEnabledEmitsManagerReplayAftermathAndSummary(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	managerStore := &startupManagerReplayRuntimeStore{
		agents: []runtimemanager.PersistedAgent{{
			Config:    runtimeactors.AgentConfig{ID: "agent-a"},
			StartedAt: time.Now().UTC(),
		}},
		pending: map[string][]events.Event{
			"agent-a": {
				eventtest.RootIngress("evt-replay", events.EventType("support.replay.ok"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-4*time.Minute).UTC()),
				eventtest.RootIngress("evt-skip", events.EventType("support.replay.skip"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-3*time.Minute).UTC()),
				eventtest.RootIngress("evt-leased", events.EventType("support.replay.leased"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
				eventtest.RootIngress("evt-drop", events.EventType("support.replay.drop"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
			},
		},
		receipts: map[string]runtimemanager.EventReceipt{
			"evt-skip|agent-a": {
				EventID: "evt-skip",
				AgentID: "agent-a",
				Status:  runtimemanager.ReceiptStatusProcessed,
			},
		},
	}

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    managerStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return startupManagerReplayRuntimeAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{
		SemanticSource:                 module.SemanticSource(),
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
	}, managerStore)

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "unclassified_runtime_error")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if got := detailInt(detail["manager_replayed_count"]); got != 1 {
		t.Fatalf("manager_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["manager_skipped_count"]); got != 1 {
		t.Fatalf("manager_skipped_count = %d, want 1", got)
	}
	if got := detailInt(detail["manager_dropped_count"]); got != 2 {
		t.Fatalf("manager_dropped_count = %d, want 2", got)
	}

	logs := listRuntimeLogsByAction(t, db, "startup_recovery_manager_replay_aftermath")
	if len(logs) != 4 {
		t.Fatalf("manager replay runtime logs = %d, want 4", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing manager replay log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}
	replayed := findByEventType("support.replay.ok")
	if got := detailString(replayed.detail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed decision_outcome = %q, want replayed", got)
	}
	skippedReceipt := findByEventType("support.replay.skip")
	if got := detailString(skippedReceipt.detail["decision_reason_code"]); got != "event_receipt_already_processed" {
		t.Fatalf("receipt skip decision_reason_code = %q, want event_receipt_already_processed", got)
	}
	droppedLeased := findByEventType("support.replay.leased")
	if got := detailString(droppedLeased.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("leased decision_outcome = %q, want dropped without prose classification", got)
	}
	if got := detailString(droppedLeased.detail["decision_reason_code"]); got != "event_processing_failed" {
		t.Fatalf("leased decision_reason_code = %q, want event_processing_failed", got)
	}
	requireFailureCode(t, droppedLeased.failure, "unclassified_runtime_error")
	dropped := findByEventType("support.replay.drop")
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	requireFailureCode(t, dropped.failure, "unclassified_runtime_error")
}

func TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RootIngress("evt-1", "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		claimErr: errors.New("claim failed"),
	}

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      eventStore,
		ManagerStore:    &recoveryGuardManagerStore{},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_manager_recovery_failed")
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

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    startupRecoveryManagerStore{loadErr: errors.New("load agents failed")},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_manager_recovery_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if got := detailBool(detail["recovery_inspection_complete"]); got {
		t.Fatalf("recovery_inspection_complete = %#v, want false", detail["recovery_inspection_complete"])
	}
	requireNestedFailureCode(t, detail, "recovery_inspection_failure", "startup_recovery_inspection_failed")
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
}

func TestRuntimeStart_InspectionFailurePreservesDecisionErrorAcrossTimerSkipAndDrop(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:   "runtime",
				EventType: "timer.skip",
				Mode:      "once",
				At:        time.Now().Add(2 * time.Minute),
				TaskID:    "skip-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.drop",
				Mode:      "once",
				At:        time.Now().Add(3 * time.Minute),
				TaskID:    "drop-me",
			},
		},
		claims: []recordedScheduleClaim{
			{Claimed: false},
			{Err: errors.New("claim failed")},
		},
	}
	managerStore := &startupRecoveryFlakyManagerStore{
		remainingFailures: 1,
		loadErr:           errors.New("load agents failed"),
	}

	rt, err := NewRuntime(ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogCapabilityStub{enabled: true, hasRunID: true, db: db},
		EventStore:      startupRecoveryCapabilityEventStore{},
		ManagerStore:    managerStore,
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

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

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_recovery_inspection_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonInspectFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonInspectFailed)
	}
	if got := detailBool(detail["recovery_inspection_complete"]); got {
		t.Fatalf("recovery_inspection_complete = %#v, want false", detail["recovery_inspection_complete"])
	}
	requireNestedFailureCode(t, detail, "recovery_inspection_failure", "startup_recovery_inspection_failed")
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 0 {
		t.Fatalf("schedule_replayed_count = %d, want 0", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 1 {
		t.Fatalf("schedule_skipped_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 1 {
		t.Fatalf("schedule_dropped_count = %d, want 1", got)
	}

	logs := listRuntimeLogsByAction(t, db, startupTimerRecoveryAction)
	if len(logs) != 2 {
		t.Fatalf("timer recovery runtime logs = %d, want 2", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing timer recovery log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}

	skipped := findByEventType("timer.skip")
	if got := detailString(skipped.detail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped decision_outcome = %q, want skipped", got)
	}
	dropped := findByEventType("timer.drop")
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	requireFailureCode(t, dropped.failure, "schedule_restore_failed")
}
