package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	builderpkg "swarm/internal/builder"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
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
		Mutations: []runStatusMutation{
			{Field: "current_state", EntityID: "ent-1", WriterType: "workflow", WriterID: "router", HandlerStep: "step-1", CreatedAt: last},
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
		"Recent Mutations:",
		"current_state  entity=ent-1  writer=workflow/router  step=step-1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
}

func TestProjectRunOperationalStatus_UsesDeliveryLifecycleWhenRunIsOperationallyStalled(t *testing.T) {
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 2},
			{SubscriberID: "agent-2", Status: "failed", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
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
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []store.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
		},
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
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
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []store.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
			{EventName: "scoring/vertical.shortlisted", Count: 1},
		},
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
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
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
			{SubscriberID: "agent-2", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
	if got.State != "running" {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.BlockingLayer != "" || got.BlockingReason != "" {
		t.Fatalf("unexpected blocking projection: %#v", got)
	}
}

func TestLoadRunStatusReport_UsesCanonicalPersistedRunIDForRuntimeLogsAndMutations(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	targetRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	targetEntityID := uuid.NewString()
	otherEntityID := uuid.NewString()
	targetEventID := uuid.NewString()
	otherEventID := uuid.NewString()
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

	for _, runID := range []string{targetRunID, otherRunID} {
		if _, err := db.Exec(`
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
			ON CONFLICT (run_id) DO NOTHING
		`, runID, now.Add(-5*time.Minute)); err != nil {
			t.Fatalf("insert run %s: %v", runID, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', $3::uuid, NULL, 'global', '{}'::jsonb, 'test', 'agent', $4),
			($5::uuid, $6::uuid, 'scan.requested', $7::uuid, NULL, 'global', '{}'::jsonb, 'test', 'agent', $8)
	`, targetRunID, targetEventID, targetEntityID, now.Add(-4*time.Minute), otherRunID, otherEventID, otherEntityID, now.Add(-3*time.Minute)); err != nil {
		t.Fatalf("insert root events: %v", err)
	}
	insertRuntimeLog(targetRunID, otherRunID, "scheduler", "canonical-owner", now)
	insertRuntimeLog(otherRunID, targetRunID, "scheduler", "payload-only", now.Add(1*time.Minute))
	if _, err := db.Exec(`
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', $4::jsonb, $5::jsonb, $3::uuid, 'platform', 'runner', 'step-a', $6),
			($7::uuid, $8::uuid, 'current_state', $10::jsonb, $11::jsonb, $9::uuid, 'platform', 'runner', 'step-b', $12)
	`, targetRunID, targetEntityID, targetEventID, `"queued"`, `"running"`, now.Add(2*time.Minute), otherRunID, otherEntityID, otherEventID, `"queued"`, `"failed"`, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("insert mutations: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-1', 'delivered', $3, $4)
	`, targetRunID, targetEventID, now.Add(10*time.Second), now.Add(5*time.Second)); err != nil {
		t.Fatalf("insert delivery: %v", err)
	}
	if err := runtimedeadletters.Insert(context.Background(), db, runtimedeadletters.Record{
		OriginalEventID: targetEventID,
		OriginalEvent:   "scan.requested",
		EntityID:        targetEntityID,
		FailureType:     "handler_error",
		ErrorMessage:    "boom",
		HandlerNode:     "node-a",
		Timestamp:       now.Add(4 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("insert dead letter: %v", err)
	}

	report, err := loadRunStatusReport(context.Background(), pg, targetRunID, runStatusOptions{
		LogsOnly:  true,
		Component: "scheduler",
	})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
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
	if len(report.Mutations) != 1 {
		t.Fatalf("Mutations len = %d, want 1", len(report.Mutations))
	}
	if got := report.Mutations[0]; got.EntityID != targetEntityID || got.Field != "current_state" || got.WriterType != "platform" || got.WriterID != "runner" {
		t.Fatalf("Mutations[0] = %#v", got)
	}
	if len(report.DeadLetters) != 1 {
		t.Fatalf("DeadLetters len = %d, want 1", len(report.DeadLetters))
	}
}

func TestRunForkCommand_DryRunUsesCanonicalPlannerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000300, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	var buf bytes.Buffer
	code := runForkCommand(ctx, t.TempDir(), []string{
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkCommand code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if plan.SourceRunID != runID {
		t.Fatalf("SourceRunID = %q, want %q", plan.SourceRunID, runID)
	}
	if plan.ForkPoint.EventID != eventID {
		t.Fatalf("ForkPoint.EventID = %q, want %q", plan.ForkPoint.EventID, eventID)
	}
	if plan.PendingWorkCount != 0 || len(plan.PendingWork) != 0 {
		t.Fatalf("pending work = %#v, want none", plan.PendingWork)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for state-only dry-run; blockers=%#v", plan.UnsupportedBlockers)
	}
	if plan.UnsupportedBlockerCount != 0 {
		t.Fatalf("UnsupportedBlockerCount = %d, want 0; blockers=%#v", plan.UnsupportedBlockerCount, plan.UnsupportedBlockers)
	}
}

func TestRunForkCommand_MaterializeOnlyUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000310, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli.materialize', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	var buf bytes.Buffer
	code := runForkCommand(ctx, t.TempDir(), []string{
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkCommand code=%d output=%s", code, buf.String())
	}
	var result store.RunForkMaterialization
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork materialization json: %v\n%s", err, buf.String())
	}
	if result.SourceRunID != runID || result.ForkRunID == "" || result.ForkRunStatus != store.RunForkMaterializedStatus {
		t.Fatalf("materialization result = %#v", result)
	}
	var forkState string
	if err := db.QueryRowContext(ctx, `
		SELECT current_state
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkState); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if forkState != "ready" {
		t.Fatalf("fork state = %q, want ready", forkState)
	}
}

func TestRunForkCommand_ActivateUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000320, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}

	var buf bytes.Buffer
	code := runForkCommand(ctx, t.TempDir(), []string{
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkCommand code=%d output=%s", code, buf.String())
	}
	var result store.RunForkActivation
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork activation json: %v\n%s", err, buf.String())
	}
	if !result.Activated || !result.SourceFrozen || !result.HistoricalReplayBlocked {
		t.Fatalf("activation result = %#v", result)
	}
	if result.SourceRunID != runID || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation lineage = %#v", result)
	}
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != store.RunForkSourceFrozenStatus || forkStatus != store.RunForkActivatedStatus {
		t.Fatalf("source/fork status = %s/%s, want forked/running", sourceStatus, forkStatus)
	}
}

func TestRunForkCommand_NonDryRunWithoutMaterializeOnlyStaysFailClosed(t *testing.T) {
	var buf bytes.Buffer
	code := runForkCommand(context.Background(), t.TempDir(), []string{
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 2 {
		t.Fatalf("runForkCommand code=%d, want 2; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "mutating fork execution is not implemented; use --dry-run, --materialize-only, or --activate") {
		t.Fatalf("output = %q, want fail-closed fork execution message", buf.String())
	}
}

func seedRunForkCLIActivationSource(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli.activate', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func setPostgresEnvFromDSN(t *testing.T, dsn string) {
	t.Helper()
	values := map[string]string{}
	for _, part := range strings.Fields(dsn) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	for _, item := range []struct {
		env string
		key string
	}{
		{"PGHOST", "host"},
		{"PGPORT", "port"},
		{"PGDATABASE", "dbname"},
		{"PGUSER", "user"},
		{"PGPASSWORD", "password"},
		{"SWARM_DB_SSLMODE", "sslmode"},
	} {
		if value := strings.TrimSpace(values[item.key]); value != "" {
			t.Setenv(item.env, value)
		}
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

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
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

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
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

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
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
	pg := &store.PostgresStore{DB: db}
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

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
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
	pg := &store.PostgresStore{DB: db}
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

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
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

func TestRunVerifyCommand_SurfacesLintEvidence(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-lint-evidence
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-lint-evidence`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
case:
  untouched:
    type: integer
    _unused_reason: verify command lint evidence proof field
  priority:
    type: integer
    _unused_reason: child read-pin coverage proof field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: [task.assigned]
    reads: [priority]
  outputs:
    events: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  _source: external (verify lint evidence test)
  entity_id: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
reader:
  id: reader
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      create_entity: true
      guard:
        check: "entity.priority >= 0"
      advances_to: done
`)

	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", root,
	}, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "lint_evidence: entity_reader_coverage [root] flow root entity_type case declares field untouched with no detected internal reader coverage") {
		t.Fatalf("verify output missing lint evidence:\n%s", out)
	}
	if !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForPromptDeclaredSaveWithoutEntityWrites(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-prompt-writer-coverage
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-prompt-writer-coverage`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
case:
  business_brief:
    type: text
    _unused_reason: verify prompt writer proof field
  research_context:
    type: text
    _unused_reason: verify prompt writer proof field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `
writer:
  id: writer
  type: factory
  role: writer
  prompt_ref: writer
  model_tier: sonnet
  conversation_mode: task
  subscriptions: []
  entity_writes:
    case:
      save:
      - research_context
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "prompts", "writer.md"), `
Use save_entity_field for `+"`business_brief`"+`.
`)

	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", root,
	}, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "entity_writer_coverage") {
		t.Fatalf("verify output missing entity_writer_coverage:\n%s", out)
	}
	if !strings.Contains(out, "business_brief") {
		t.Fatalf("verify output missing offending field:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForPseudoStateSchemaTypes(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-state-schema-pseudo-types
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-state-schema-pseudo-types`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  state_schema:
    fields:
      dimensions_received: dimension score receipts keyed by dimension name
`)

	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", root,
	}, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify failed: load Swarm contracts:") || !strings.Contains(out, "state_schema field type") {
		t.Fatalf("unexpected output = %q", out)
	}
}

func TestRunVerifyCommand_AllowsCanonicalStateSchemaFloat(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-state-schema-float
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-state-schema-float`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  _source: external (state schema float verify test)
  entity_id: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      create_entity: true
      advances_to: done
  state_schema:
    fields:
      composite: float
`)

	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", root,
	}, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
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

func loadWorkflowValidationBundleAt(t *testing.T, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func writeWorkflowValidationFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeWorkflowValidationDeadEventSchemaFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: dead-event-schema
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: dead-event-schema\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.unused: {}
`)
	return root
}

func firstWorkflowValidationFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) (string, string, string, runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	for _, view := range bundle.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		for nodeID, node := range view.Nodes {
			for eventType, handler := range node.EventHandlers {
				return flowID, nodeID, eventType, handler
			}
		}
	}
	t.Fatal("expected fixture to include at least one flow handler")
	return "", "", "", runtimecontracts.SystemNodeEventHandler{}
}

func writeWorkflowValidationFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
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
			name: "builtin runtime tool reference",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"schedule"}, Permissions: []string{"schedule"}},
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
			switch tc.name {
			case "missing tool reference":
				if warnings := result.BootReport.Warnings(); len(warnings) == 0 || !strings.Contains(warnings[0].Message, "missing tool missing_tool") {
					t.Fatalf("BootReport warnings = %#v, want tool_resolution warning", warnings)
				}
			case "builtin runtime tool reference":
				for _, warning := range result.BootReport.Warnings() {
					if strings.TrimSpace(warning.CheckID) == "tool_resolution" && strings.Contains(warning.Message, "schedule") {
						t.Fatalf("BootReport warnings = %#v, unexpected builtin tool_resolution warning", result.BootReport.Warnings())
					}
				}
			}
		})
	}
}

func TestLogBootSkeleton_UsesRuntimeToolInventoryCount(t *testing.T) {
	source := semanticview.Wrap(testWorkflowValidationBundle())
	wantTools := len(runtimetools.RuntimeAvailableToolNamesForSource(source))
	if wantTools == 0 {
		t.Fatal("runtime tool inventory unexpectedly empty")
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	previous := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(previous)

	logBootSkeleton(source, "/tmp/contracts", "/tmp/platform-spec.yaml", runtimebootverify.Report{}, "state stores ready")

	out := buf.String()
	want := "name=build_registries"
	if !strings.Contains(out, want) {
		t.Fatalf("log output missing %q:\n%s", want, out)
	}
	if !strings.Contains(out, "tools="+strconv.Itoa(wantTools)) {
		t.Fatalf("log output missing runtime tool count %d:\n%s", wantTools, out)
	}
	if strings.Contains(out, "tools=0") {
		t.Fatalf("log output still reports zero tools:\n%s", out)
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

func TestVerifyBundle_CreateEntityAccumulatePreemptsDynamicComputeWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	bundle.RootEntities = runtimecontracts.EntityContractsDocument{
		"tracking": {
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"expected_count":  {Type: "integer", Initial: 1},
				"composite_score": {Type: "numeric"},
			},
		},
	}
	nodeID := "complete-task"
	eventType := "task.requested"
	node, ok := bundle.Nodes[nodeID]
	if !ok {
		t.Fatalf("node %s missing from test fixture bundle", nodeID)
	}
	handler := node.EventHandlers[eventType]
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{ExpectedFrom: "entity.expected_count"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler

	err := verifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("verifyBundle error = %v, want create_entity/accumulate boot error", err)
	}
}

func TestVerifyBundle_EmittedPayloadCompletenessReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"scan": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scan_id":   {Type: "string"},
					"geography": {Type: "string"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				SubscribesTo: []string{"scan.corpus_dispatch"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.corpus_dispatch": {
				Source: "external",
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
					},
				},
				Required: []string{"scan_id", "geography"},
			},
			"market_research.scan_assigned": {
				ConsumerType: []string{"dashboard"},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id":          {Type: "string"},
						"current_state":      {Type: "string"},
						"trigger_event_type": {Type: "string"},
						"scan_id":            {Type: "string"},
					},
				},
				Required: []string{"entity_id", "scan_id"},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"

	err := verifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want emitted payload completeness invalidity")
	}
	if !strings.Contains(err.Error(), "scan_id is not statically provable") {
		t.Fatalf("verifyBundle error = %v, want emitted payload completeness invalidity", err)
	}
	if strings.Contains(err.Error(), "definitely missing") {
		t.Fatalf("verifyBundle error = %v, want approved warning wording only", err)
	}
}

func TestVerifyBundle_InputPinProducerPathReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-missing-pin"))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from missing producer path")
	}
	for _, want := range []string{
		"no producer path was found in the authored bundle",
		"Sibling flow output pin: not found",
		"Root agent emit_events: not found",
		"Root node handler emits: not found",
		"Platform event catalog: not matched",
		"External source annotation (_source): not found",
		"Same-flow timer declaration: not found",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func TestVerifyBundle_UnreachableStateReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-state-machine-unreachable"))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unreachable declared state")
	}
	for _, want := range []string{
		"semantic_drift_unreachable_state",
		"declares state review but no transition path from initial_state waiting reaches review",
		"Reachable states: active, done, waiting",
		"Unreachable states: review",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func TestVerifyBundle_DeadDeclaredEventSchemaReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeWorkflowValidationDeadEventSchemaFixture(t)))

	verifyErr := verifyBundle(context.Background(), source)
	if verifyErr == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(verifyErr.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", verifyErr, want)
		}
	}

	result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil))
	if runtimeErr == nil {
		t.Fatal("ValidateWorkflowContractSurface error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(runtimeErr.Error(), want) {
			t.Fatalf("ValidateWorkflowContractSurface error = %v, want substring %q", runtimeErr, want)
		}
	}
	if !strings.Contains(strings.TrimSpace(result.BootReport.Warnings()[0].CheckID), "semantic_drift_dead_event_schema") {
		t.Fatalf("BootReport warnings = %#v, want semantic_drift_dead_event_schema", result.BootReport.Warnings())
	}
}

func TestVerifyBundle_CreateEntityAccumulateReturnsBootError(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate"))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want create_entity/accumulate boot error")
	}
	if !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("verifyBundle error = %v, want create_entity/accumulate boot error", err)
	}
}
