package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	builderpkg "swarm/internal/builder"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type delayedRunStatusAgent struct {
	id            string
	subscriptions []events.EventType
	started       chan struct{}
	release       chan struct{}
}

func (a delayedRunStatusAgent) ID() string { return a.id }
func (delayedRunStatusAgent) Type() string { return "test" }
func (a delayedRunStatusAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}
func (a delayedRunStatusAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	out := (events.Event{
		ID:          uuid.NewString(),
		RunID:       evt.RunID,
		Type:        events.EventType("scan.completed"),
		SourceAgent: a.id,
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(evt.EntityID())
	return []events.Event{out}, nil
}

func TestPrintRunStatusReport(t *testing.T) {
	var buf bytes.Buffer
	started := time.Unix(1700000000, 0).UTC()
	last := started.Add(5 * time.Minute)
	report := runStatusReport{
		RunID:             "run-123",
		RunTableStatus:    "running",
		OperationalState:  "stalled",
		BlockingLayer:     "scoring_terminal_outcome",
		BlockingReason:    "terminal_scoring_outcome_missing",
		RootEventID:       "evt-1",
		RootEventType:     "scan.requested",
		StartedAt:         started,
		LastEventAt:       last,
		EventCount:        7,
		WarnErrorLogCount: 1,
		Heuristics: []string{
			"run appears settled after scoring started but no terminal scoring outcome was emitted",
		},
		EventCounts: []runStatusEventCount{
			{EventName: "scan.requested", Count: 1},
			{EventName: "vertical.discovered", Count: 2},
		},
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "analysis-agent", Status: "delivered", Count: 2},
		},
		AgentTurns: []runStatusAgentTurn{
			{AgentID: "analysis-agent", Turns: 2, ErrorCount: 0, LastAt: last},
		},
		RuntimeLogSummary: []runStatusRuntimeSummary{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Count: 3},
		},
		RuntimeLogs: []runStatusRuntimeLog{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Error: "missing or invalid mcp context token", CreatedAt: last},
		},
		RecentEvents: []runStatusEvent{
			{EventName: "vertical.discovered", EntityID: "ent-1", CreatedAt: last},
		},
	}

	printRunStatusReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Run run-123",
		"Root: scan.requested (evt-1)",
		"Run Table Status: running",
		"Operational State: stalled",
		"Blocking Layer: scoring_terminal_outcome",
		"Blocking Reason: terminal_scoring_outcome_missing",
		"Summary: events=7 deliveries=1 dead_letters=0 agent_turns=1 runtime_warn_errors=1",
		"Heuristics:",
		"run appears settled after scoring started but no terminal scoring outcome was emitted",
		"Event Counts:",
		"analysis-agent  status=delivered  count=2",
		"Runtime Log Summary:",
		"WARN  mcp-gateway/mcp.context.fallback_used  count=3",
		"Runtime Warnings/Errors:",
		"WARN  mcp-gateway/mcp.context.fallback_used",
		"Recent Events:",
		"vertical.discovered  entity=ent-1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
}

func TestProjectRunOperationalStatus_UsesDeliveryLifecycleWhenRunIsOperationallyStalled(t *testing.T) {
	report := runStatusReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 2},
			{SubscriberID: "agent-2", Status: "failed", Count: 1},
		},
	}

	got := projectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_UsesScoringOutcomeBlockingLayer(t *testing.T) {
	report := runStatusReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []runStatusEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
		},
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := projectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "scoring_terminal_outcome" {
		t.Fatalf("blocking_layer = %q, want scoring_terminal_outcome", got.BlockingLayer)
	}
	if got.BlockingReason != "terminal_scoring_outcome_missing" {
		t.Fatalf("blocking_reason = %q, want terminal_scoring_outcome_missing", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_TreatsScopedShortlistAsTerminalScoringOutcome(t *testing.T) {
	report := runStatusReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []runStatusEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
			{EventName: "scoring/vertical.shortlisted", Count: 1},
		},
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := projectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_PreservesHealthyRunningWhenActiveDeliveriesRemain(t *testing.T) {
	report := runStatusReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
			{SubscriberID: "agent-2", Status: "delivered", Count: 1},
		},
	}

	got := projectRunOperationalStatus(report)
	if got.State != "running" {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.BlockingLayer != "" || got.BlockingReason != "" {
		t.Fatalf("unexpected blocking projection: %#v", got)
	}
}

func TestLoadRunStatusRuntimeLogs_UsesSpecShapedPayload(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	summaryCount := sqlmock.NewRows([]string{"count"}).AddRow(2)
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM events`).
		WithArgs("run-123", sqlmock.AnyArg(), "tool-executor").
		WillReturnRows(summaryCount)

	rollupRows := sqlmock.NewRows([]string{"log_level", "component", "action", "count"}).
		AddRow("warn", "tool-executor", "tool_execution_denied", 2)
	mock.ExpectQuery(`SELECT\s+COALESCE\(payload->>'log_level', ''\)`).
		WithArgs("run-123", sqlmock.AnyArg(), "tool-executor").
		WillReturnRows(rollupRows)

	logRows := sqlmock.NewRows([]string{"log_level", "component", "action", "error", "created_at"}).
		AddRow("warn", "tool-executor", "tool_execution_denied", "tool not allowed", time.Unix(1700000000, 0).UTC())
	mock.ExpectQuery(`SELECT\s+COALESCE\(payload->>'log_level', ''\)`).
		WithArgs("run-123", sqlmock.AnyArg(), "tool-executor", 100).
		WillReturnRows(logRows)

	var report runStatusReport
	err = loadRunStatusRuntimeLogs(context.Background(), db, "run-123", runStatusOptions{
		LogsOnly:  true,
		Component: "tool-executor",
	}, &report)
	if err != nil {
		t.Fatalf("loadRunStatusRuntimeLogs: %v", err)
	}
	if report.WarnErrorLogCount != 2 {
		t.Fatalf("WarnErrorLogCount = %d, want 2", report.WarnErrorLogCount)
	}
	if len(report.RuntimeLogSummary) != 1 {
		t.Fatalf("RuntimeLogSummary len = %d, want 1", len(report.RuntimeLogSummary))
	}
	if got := report.RuntimeLogSummary[0]; got.Level != "warn" || got.Component != "tool-executor" || got.Action != "tool_execution_denied" || got.Count != 2 {
		t.Fatalf("RuntimeLogSummary[0] = %#v", got)
	}
	if len(report.RuntimeLogs) != 1 {
		t.Fatalf("RuntimeLogs len = %d, want 1", len(report.RuntimeLogs))
	}
	if got := report.RuntimeLogs[0]; got.Level != "warn" || got.Component != "tool-executor" || got.Action != "tool_execution_denied" || got.Error != "tool not allowed" {
		t.Fatalf("RuntimeLogs[0] = %#v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLoadRunStatusRuntimeLogs_UsesCanonicalPersistedRunID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	targetRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	insertRuntimeLog := func(runID string, payloadRunID string, component string, action string, createdAt time.Time) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"log_level": "warn",
			"message":   action,
			"details": map[string]any{
				"run_id":    payloadRunID,
				"component": component,
				"action":    action,
				"error":     action + "-error",
			},
		})
		if err != nil {
			t.Fatalf("marshal runtime log payload: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO runs (run_id, status)
			VALUES ($1::uuid, 'running')
			ON CONFLICT (run_id) DO NOTHING
		`, runID); err != nil {
			t.Fatalf("insert run %s: %v", runID, err)
		}
		if _, err := db.Exec(`
			INSERT INTO events (
				run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
			)
			VALUES (
				$1::uuid, gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $2::jsonb, 'test', 'agent', $3
			)
		`, runID, string(payload), createdAt); err != nil {
			t.Fatalf("insert runtime log for run %s: %v", runID, err)
		}
	}

	insertRuntimeLog(targetRunID, otherRunID, "scheduler", "canonical-owner", now)
	insertRuntimeLog(otherRunID, targetRunID, "scheduler", "payload-only", now.Add(1*time.Minute))

	var report runStatusReport
	err := loadRunStatusRuntimeLogs(context.Background(), db, targetRunID, runStatusOptions{
		LogsOnly:  true,
		Component: "scheduler",
	}, &report)
	if err != nil {
		t.Fatalf("loadRunStatusRuntimeLogs: %v", err)
	}
	if report.WarnErrorLogCount != 1 {
		t.Fatalf("WarnErrorLogCount = %d, want 1", report.WarnErrorLogCount)
	}
	if len(report.RuntimeLogSummary) != 1 {
		t.Fatalf("RuntimeLogSummary len = %d, want 1", len(report.RuntimeLogSummary))
	}
	if got := report.RuntimeLogSummary[0]; got.Component != "scheduler" || got.Action != "canonical-owner" || got.Count != 1 {
		t.Fatalf("RuntimeLogSummary[0] = %#v", got)
	}
	if len(report.RuntimeLogs) != 1 {
		t.Fatalf("RuntimeLogs len = %d, want 1", len(report.RuntimeLogs))
	}
	if got := report.RuntimeLogs[0]; got.Component != "scheduler" || got.Action != "canonical-owner" || got.Error != "canonical-owner-error" {
		t.Fatalf("RuntimeLogs[0] = %#v", got)
	}
}

func TestDefaultRuntimeConfig_RejectsUnsupportedRuntimeControlEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_RUNTIME_MODE", "api")
	t.Setenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "4")
	t.Setenv("SWARM_CLAUDE_DEFAULT_MODEL", "test-model")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS") {
		t.Fatalf("defaultRuntimeConfig error = %v, want unsupported env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on unsupported env", cfg)
	}
}

func TestLoadRuntimeConfig_RejectsUnsupportedRuntimeControlsFromFile(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  max_concurrent_agents: 4",
		"llm:",
		"  runtime_mode: api",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := loadRuntimeConfig(p); err == nil || !strings.Contains(err.Error(), "runtime.max_concurrent_agents") {
		t.Fatalf("loadRuntimeConfig error = %v, want unsupported runtime control rejection", err)
	}
}

func TestLoadRunStatusReport_UsesDurableCompletedRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	handler := builderpkg.NewHandler(builderpkg.Options{
		CurrentRuntime: func() *runtimepkg.Runtime { return rt },
		AuthToken:      "builder-test-token",
	})

	runID := uuid.NewString()
	entityID := uuid.NewString()
	reqBody, err := json.Marshal(builderpkg.Request{
		JSONRPC: "2.0",
		ID:      "run-start-1",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": runID,
			"inputs": map[string]any{
				"scan.requested": map[string]any{
					"entity_id": entityID,
					"topic":     "sample",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal run.start request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer builder-test-token")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var status string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, '')
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status)
		if err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach durable completed state: last err=%v", runID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	report, err := loadRunStatusReport(ctx, db, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "completed" {
		t.Fatalf("RunTableStatus = %q, want completed", report.RunTableStatus)
	}
	if report.EndedAt == nil || report.EndedAt.IsZero() {
		t.Fatal("expected durable ended_at in run status report")
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "runs table still says running") {
			t.Fatalf("unexpected running heuristic after durable completion: %#v", report.Heuristics)
		}
	}
}

func TestLoadRunStatusReport_KeepsSupportedRunRunningUntilManagerWorkSettles(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	defer func() { _ = am.Shutdown() }()

	rt := &runtimepkg.Runtime{Bus: eb, Manager: am}
	handler := builderpkg.NewHandler(builderpkg.Options{
		CurrentRuntime: func() *runtimepkg.Runtime { return rt },
		AuthToken:      "builder-test-token",
	})

	runID := uuid.NewString()
	entityID := uuid.NewString()
	reqBody, err := json.Marshal(builderpkg.Request{
		JSONRPC: "2.0",
		ID:      "run-start-1",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": runID,
			"inputs": map[string]any{
				"scan.requested": map[string]any{
					"entity_id": entityID,
					"topic":     "sample",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal run.start request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer builder-test-token")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	ctx := context.Background()
	var (
		status           string
		eventCount       int
		entityCount      int
		activeDeliveries int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count, entity_count
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount); err != nil {
		t.Fatalf("load in-flight run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("in-flight run status = %q, want running", status)
	}
	if eventCount != 1 {
		t.Fatalf("in-flight event_count = %d, want 1 root event", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("in-flight entity_count = %d, want 1", entityCount)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected active delivery while agent work is blocked")
	}

	close(releaseAgent)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), event_count, entity_count
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &eventCount, &entityCount)
		if err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state: last err=%v status=%q event_count=%d entity_count=%d", runID, err, status, eventCount, entityCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if eventCount < 2 {
		t.Fatalf("completed event_count = %d, want downstream event activity", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("completed entity_count = %d, want 1", entityCount)
	}
	var extraRunningRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE run_id <> $1::uuid
		  AND status = 'running'
	`, runID).Scan(&extraRunningRows); err != nil {
		t.Fatalf("count extra running rows: %v", err)
	}
	if extraRunningRows != 0 {
		t.Fatalf("extra running rows = %d, want 0", extraRunningRows)
	}

	report, err := loadRunStatusReport(ctx, db, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "completed" {
		t.Fatalf("RunTableStatus = %q, want completed", report.RunTableStatus)
	}
}

func TestLoadRunStatusReport_PreservesRunningTruthAcrossBuilderQuiescenceTimeout(t *testing.T) {
	restore := builderpkg.SetRunCompletionTimeoutForTest(25 * time.Millisecond)
	defer restore()

	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	defer func() { _ = am.Shutdown() }()

	rt := &runtimepkg.Runtime{Bus: eb, Manager: am}
	handler := builderpkg.NewHandler(builderpkg.Options{
		CurrentRuntime: func() *runtimepkg.Runtime { return rt },
		AuthToken:      "builder-test-token",
	})

	runID := uuid.NewString()
	entityID := uuid.NewString()
	reqBody, err := json.Marshal(builderpkg.Request{
		JSONRPC: "2.0",
		ID:      "run-start-timeout-1",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": runID,
			"inputs": map[string]any{
				"scan.requested": map[string]any{
					"entity_id": entityID,
					"topic":     "sample",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal run.start request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer builder-test-token")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	time.Sleep(120 * time.Millisecond)

	ctx := context.Background()
	var (
		status           string
		activeDeliveries int
		endedAt          sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &endedAt); err != nil {
		t.Fatalf("load timed-out run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("timed-out run status = %q, want running", status)
	}
	if endedAt.Valid {
		t.Fatalf("timed-out run ended_at = %s, want NULL while same-run work remains active", endedAt.Time)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries after timeout window: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected same-run active delivery after builder timeout window")
	}
	if got := am.InFlightCount(); got == 0 {
		t.Fatal("expected live in-flight manager work after builder timeout window")
	}

	report, err := loadRunStatusReport(ctx, db, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "running" {
		t.Fatalf("RunTableStatus after timeout window = %q, want running", report.RunTableStatus)
	}
	if report.OperationalState != "running" {
		t.Fatalf("OperationalState after timeout window = %q, want running", report.OperationalState)
	}
	foundActiveDelivery := false
	for _, item := range report.Deliveries {
		if item.SubscriberID == "agent-1" && item.Status == "in_progress" && item.Count > 0 {
			foundActiveDelivery = true
			break
		}
	}
	if !foundActiveDelivery {
		t.Fatalf("expected supported status report to preserve active same-run delivery, got %#v", report.Deliveries)
	}

	close(releaseAgent)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), ended_at
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &endedAt)
		if err == nil && status == "completed" && endedAt.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state after release: last err=%v status=%q ended_at_valid=%v", runID, err, status, endedAt.Valid)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var lastEventAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(created_at), '-infinity'::timestamptz)
		FROM events
		WHERE run_id = $1::uuid
	`, runID).Scan(&lastEventAt); err != nil {
		t.Fatalf("load last event timestamp: %v", err)
	}
	if !endedAt.Time.IsZero() && lastEventAt.After(endedAt.Time) {
		t.Fatalf("last same-run event at %s is after ended_at %s", lastEventAt, endedAt.Time)
	}
}

func TestLoadRunStatusReport_ProjectsExplicitStalledRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	runID := uuid.NewString()
	rootEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now() - interval '10 minutes')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	for _, eventID := range []string{rootEventID, deliveredEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '5 minutes'
			)
		`, runID, eventID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, delivered_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'delivered', now() - interval '2 minutes', now() - interval '4 minutes'
		)
	`, runID, deliveredEventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	report, err := loadRunStatusReport(ctx, db, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "running" {
		t.Fatalf("run_table_status = %q, want running", report.RunTableStatus)
	}
	if report.OperationalState != "stalled" {
		t.Fatalf("operational_state = %q, want stalled", report.OperationalState)
	}
	if report.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", report.BlockingLayer)
	}
	if report.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", report.BlockingReason)
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "runs table still says running") {
			t.Fatalf("unexpected stalled heuristic fallback: %#v", report.Heuristics)
		}
	}
}

func TestLoadRunStatusReport_ProjectsScoringOutcomeStall(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	runID := uuid.NewString()
	rootEventID := uuid.NewString()
	scoringEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now() - interval '10 minutes')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES
			($1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '9 minutes'),
			($1::uuid, $3::uuid, 'scoring/scoring.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '2 minutes')
	`, runID, rootEventID, scoringEventID); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	report, err := loadRunStatusReport(ctx, db, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.OperationalState != "stalled" {
		t.Fatalf("operational_state = %q, want stalled", report.OperationalState)
	}
	if report.BlockingLayer != "scoring_terminal_outcome" {
		t.Fatalf("blocking_layer = %q, want scoring_terminal_outcome", report.BlockingLayer)
	}
	if report.BlockingReason != "terminal_scoring_outcome_missing" {
		t.Fatalf("blocking_reason = %q, want terminal_scoring_outcome_missing", report.BlockingReason)
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "run appears settled after scoring started") {
			t.Fatalf("unexpected scoring heuristic fallback: %#v", report.Heuristics)
		}
	}
}

func TestLoadDotEnvFileSetsMissingVarsOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("ALPHA=one\nBETA=\"two words\"\nexport GAMMA='three'\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv("ALPHA", "shell")

	if err := loadDotEnvFile(path); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}

	if got := os.Getenv("ALPHA"); got != "shell" {
		t.Fatalf("ALPHA = %q, want shell override", got)
	}
	if got := os.Getenv("BETA"); got != "two words" {
		t.Fatalf("BETA = %q", got)
	}
	if got := os.Getenv("GAMMA"); got != "three" {
		t.Fatalf("GAMMA = %q", got)
	}
}

func TestLoadDotEnvFileMissingIsNoop(t *testing.T) {
	if err := loadDotEnvFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}
}

func TestLoadDotEnvFileRejectsMalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("BROKEN\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	err := loadDotEnvFile(path)
	if err == nil || !strings.Contains(err.Error(), "expected KEY=VALUE") {
		t.Fatalf("loadDotEnvFile error = %v", err)
	}
}

func TestRunVerifyCommand_BadContractsPath(t *testing.T) {
	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", filepath.Join(t.TempDir(), "missing"),
	}, &buf)
	if code == 0 {
		t.Fatal("expected non-zero exit code")
	}
	if out := buf.String(); !strings.Contains(out, "verify failed: resolve contracts") {
		t.Fatalf("output = %q", out)
	}
}

func testWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func loadWorkflowValidationFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func TestVerifyBundle_AgreesWithRuntimeValidationOnTouchedToolAndEventClasses(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	cases := []struct {
		name        string
		bundle      *runtimecontracts.WorkflowContractBundle
		errContains string
		wantErr     bool
	}{
		{
			name: "missing tool reference",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"missing_tool"}},
				}
				return bundle
			}(),
			wantErr: false,
		},
		{
			name: "missing emitted event schema warning",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", EmitEvents: []string{"missing.event"}},
				}
				return bundle
			}(),
			errContains: "'missing.event' emitted but no schema in events.yaml",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": {
						HandlerType: "api_call",
					},
				}
				return bundle
			}(),
			errContains: "tool implementation warnings",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(tc.bundle)
			verifyErr := verifyBundle(context.Background(), source)
			if tc.wantErr {
				if verifyErr == nil || !strings.Contains(verifyErr.Error(), tc.errContains) {
					t.Fatalf("verifyBundle error = %v, want substring %q", verifyErr, tc.errContains)
				}
			} else if verifyErr != nil {
				t.Fatalf("verifyBundle error = %v, want nil", verifyErr)
			}

			result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil))
			if tc.wantErr {
				if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), tc.errContains) {
					t.Fatalf("ValidateWorkflowContractSurface error = %v, want substring %q", runtimeErr, tc.errContains)
				}
				return
			}
			if runtimeErr != nil {
				t.Fatalf("ValidateWorkflowContractSurface error = %v, want nil", runtimeErr)
			}
			if warnings := result.BootReport.Warnings(); len(warnings) == 0 || !strings.Contains(warnings[0].Message, "missing tool missing_tool") {
				t.Fatalf("BootReport warnings = %#v, want tool_resolution warning", warnings)
			}
		})
	}
}

func TestVerifyBundle_DoesNotWarnForFlowLocalEmittedEventsWithOwningFlowSchemas(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events")))

	err := verifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'child/child.internal' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
	if strings.Contains(err.Error(), "'child/child.done' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
}

func TestVerifyBundle_DoesNotWarnForFlowOwnedAgentOutputEvents(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-required-agents-child")))

	err := verifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'analysis.done' emitted but nobody subscribes") {
		t.Fatalf("unexpected flow-owned agent output warning: %v", err)
	}
}
