package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func seedRunDebugAgent(t *testing.T, pg *PostgresStore, ctx context.Context, agentID string, entityID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       agentID,
			Role:     agentID,
			Mode:     "operating",
			Model:    "regular",
			EntityID: entityID,
			Config:   json.RawMessage(`{"system_prompt":"You are a trace test agent.","tools":[],"subscriptions":["trace.*"]}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent %s: %v", agentID, err)
	}
}

func assertRunTestQuiescence(t *testing.T, got RunTestQuiescence, want RunTestQuiescence) {
	t.Helper()
	if got != want {
		t.Fatalf("TestQuiescence = %#v, want %#v", got, want)
	}
}

func TestRunDebugReadSurface_ListRunDebugRuns_UsesCanonicalRunScope(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	olderRunID := uuid.NewString()
	newerRunID := uuid.NewString()
	olderEventID := uuid.NewString()
	newerEventID := uuid.NewString()
	olderEntityID := uuid.NewString()
	newerEntityA := uuid.NewString()
	newerEntityB := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at, ended_at)
		VALUES
			($1::uuid, 'completed', $3, $4),
			($2::uuid, 'running', $5, NULL)
	`, olderRunID, newerRunID, now.Add(-2*time.Hour), now.Add(-90*time.Minute), now.Add(-1*time.Hour)); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'test', 'agent', $5),
			($1::uuid, gen_random_uuid(), 'scan.completed', 'global', '{}'::jsonb, 'test', 'agent', $6),
			($3::uuid, $4::uuid, 'scan.requested', 'global', '{}'::jsonb, 'test', 'agent', $7)
	`, olderRunID, olderEventID, newerRunID, newerEventID, now.Add(-119*time.Minute), now.Add(-91*time.Minute), now.Add(-59*time.Minute)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	seedPostgresEntityStateRows(t, db, ctx, olderRunID, olderEntityID)
	seedPostgresEntityStateRows(t, db, ctx, newerRunID, newerEntityA, newerEntityB)

	runs, err := pg.ListRunDebugRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListRunDebugRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("ListRunDebugRuns len = %d, want 2", len(runs))
	}
	if runs[0].RunID != newerRunID {
		t.Fatalf("runs[0].RunID = %q, want %q", runs[0].RunID, newerRunID)
	}
	if runs[0].RootEventID != newerEventID || runs[0].RootEventType != "scan.requested" {
		t.Fatalf("runs[0] root = %#v", runs[0])
	}
	if runs[0].EntityCount != 2 {
		t.Fatalf("runs[0].EntityCount = %d, want entity_state count 2", runs[0].EntityCount)
	}
	if runs[1].RunID != olderRunID {
		t.Fatalf("runs[1].RunID = %q, want %q", runs[1].RunID, olderRunID)
	}
	if runs[1].EventCount != 2 {
		t.Fatalf("runs[1].EventCount = %d, want 2", runs[1].EventCount)
	}
	if runs[1].EntityCount != 1 {
		t.Fatalf("runs[1].EntityCount = %d, want entity_state count 1", runs[1].EntityCount)
	}
}

func TestRunDebugReadSurface_ResolveLatestRunDebugRunID_UsesLatestPersistedRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	targetRunID := uuid.NewString()
	olderRunID := uuid.NewString()
	emptyRunID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES
			($1::uuid, 'running', $4),
			($2::uuid, 'completed', $5),
			($3::uuid, 'running', $6)
	`, targetRunID, olderRunID, emptyRunID, now, now.Add(-1*time.Hour), now.Add(1*time.Hour)); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, gen_random_uuid(), 'scan.corpus_file_requested', 'global', '{}'::jsonb, 'builder', 'agent', $3),
			($2::uuid, gen_random_uuid(), 'scan.requested', 'global', '{}'::jsonb, 'builder', 'agent', $4)
	`, targetRunID, olderRunID, now.Add(time.Second), now.Add(-59*time.Minute)); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	got, err := pg.ResolveLatestRunDebugRunID(ctx)
	if err != nil {
		t.Fatalf("ResolveLatestRunDebugRunID: %v", err)
	}
	if got != targetRunID {
		t.Fatalf("latest run = %q, want %q", got, targetRunID)
	}
}

func TestRunDebugReadSurface_LoadRunDebugReport_UsesCanonicalRunIDForLogsAndMutations(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	targetRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	targetEntityID := uuid.NewString()
	targetSecondEntityID := uuid.NewString()
	otherEntityID := uuid.NewString()
	targetEventID := uuid.NewString()
	otherEventID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	for _, runID := range []string{targetRunID, otherRunID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
		`, runID, now.Add(-5*time.Minute)); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', $3::uuid, 'global', '{}'::jsonb, 'test', 'agent', $4),
			($5::uuid, $6::uuid, 'scan.requested', $7::uuid, 'global', '{}'::jsonb, 'test', 'agent', $8)
	`, targetRunID, targetEventID, targetEntityID, now.Add(-4*time.Minute), otherRunID, otherEventID, otherEntityID, now.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed root events: %v", err)
	}
	seedPostgresEntityStateRows(t, db, ctx, targetRunID, targetEntityID, targetSecondEntityID)
	seedPostgresEntityStateRows(t, db, ctx, otherRunID, otherEntityID)

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
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			)
			VALUES ($1::uuid, gen_random_uuid(), 'platform.runtime_log', 'global', $2::jsonb, 'test', 'agent', $3)
		`, runID, string(payload), createdAt); err != nil {
			t.Fatalf("seed runtime log for run %s: %v", runID, err)
		}
	}

	insertRuntimeLog(targetRunID, otherRunID, "scheduler", "canonical-owner", now)
	insertRuntimeLog(otherRunID, targetRunID, "scheduler", "payload-only", now.Add(1*time.Minute))

	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', $4::jsonb, $5::jsonb, $3::uuid, 'platform', 'runner', 'step-a', $6),
			($7::uuid, $8::uuid, 'current_state', $10::jsonb, $11::jsonb, $9::uuid, 'platform', 'runner', 'step-b', $12)
	`, targetRunID, targetEntityID, targetEventID, `"queued"`, `"running"`, now.Add(2*time.Minute), otherRunID, otherEntityID, otherEventID, `"queued"`, `"failed"`, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, delivered_at, created_at
			)
			VALUES ($1::uuid, $2::uuid, 'agent', 'agent-1', 'dead_letter', 2, 'handler_error', $3::jsonb, $4, $5)
		`, targetRunID, targetEventID, mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "handler_failed", nil)), now.Add(10*time.Second), now.Add(5*time.Second)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	successDeliveryID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'node', 'node-success', 'delivered', 0, 'node_processed', $4, $5)
		`, successDeliveryID, targetRunID, targetEventID, now.Add(20*time.Second), now.Add(15*time.Second)); err != nil {
		t.Fatalf("seed successful delivery: %v", err)
	}
	if err := runtimedeadletters.Insert(ctx, db, runtimedeadletters.Record{
		OriginalEventID: targetEventID,
		OriginalEvent:   "scan.requested",
		EntityID:        targetEntityID,
		Failure:         testFailureEnvelope(runtimefailures.ClassInternalFailure, "test_handler_failure", nil),
		HandlerNode:     "node-a",
		Timestamp:       now.Add(4 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}

	report, err := pg.LoadRunDebugReport(ctx, targetRunID, RunDebugQueryOptions{
		LogsAllLevels:   false,
		Component:       "scheduler",
		EventLimit:      10,
		MutationLimit:   10,
		RuntimeLogLimit: 10,
		DeadLetterLimit: 10,
	})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if report.RunID != targetRunID {
		t.Fatalf("RunID = %q, want %q", report.RunID, targetRunID)
	}
	if report.RootEventID != targetEventID {
		t.Fatalf("RootEventID = %q, want %q", report.RootEventID, targetEventID)
	}
	if report.EntityCount != 2 {
		t.Fatalf("EntityCount = %d, want entity_state count 2", report.EntityCount)
	}
	if report.WarnErrorLogCount != 1 {
		t.Fatalf("WarnErrorLogCount = %d, want 1", report.WarnErrorLogCount)
	}
	if len(report.RuntimeLogs) != 1 {
		t.Fatalf("RuntimeLogs len = %d, want 1", len(report.RuntimeLogs))
	}
	if got := report.RuntimeLogs[0]; got.Component != "scheduler" || got.Action != "canonical-owner" {
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
	if len(report.Deliveries) != 2 {
		t.Fatalf("Deliveries len = %d, want 2", len(report.Deliveries))
	}
	if len(report.FailedDeliveries) != 1 {
		t.Fatalf("FailedDeliveries len = %d, want 1: %#v", len(report.FailedDeliveries), report.FailedDeliveries)
	}
	if got := report.FailedDeliveries[0]; got.SubscriberType != "agent" || got.RetryCount != 2 || got.RetryEligible || !got.Terminal || len(got.DeadLetters) != 1 {
		t.Fatalf("FailedDeliveries[0] = %#v", got)
	}
	if report.FailedDeliveries[0].DeliveryID == successDeliveryID {
		t.Fatalf("successful delivered/node_processed delivery appeared in FailedDeliveries: %#v", report.FailedDeliveries)
	}
}

func TestRunDebugReadSurface_LoadRunDebugReport_ProjectsTestQuiescenceCounts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	blockedRunID := uuid.NewString()
	readyRunID := uuid.NewString()
	activeEventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	runtimeLogEventID := uuid.NewString()
	readyEventID := uuid.NewString()
	now := time.Now().UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES
			($1::uuid, 'running', $3),
			($2::uuid, 'running', $3)
	`, blockedRunID, readyRunID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'quiescence.active_delivery', 'global', '{}'::jsonb, 'test', 'platform', $6),
			($1::uuid, $3::uuid, 'quiescence.missing_pipeline_receipt', 'global', '{}'::jsonb, 'test', 'platform', $7),
			($1::uuid, $4::uuid, '`+runtimeLogEventName+`', 'global', '{}'::jsonb, 'test', 'platform', $8),
			($9::uuid, $5::uuid, 'quiescence.ready', 'global', '{}'::jsonb, 'test', 'platform', $10)
	`, blockedRunID, activeEventID, unsettledEventID, runtimeLogEventID, readyEventID,
		now.Add(-50*time.Second), now.Add(-40*time.Second), now.Add(-30*time.Second), readyRunID, now.Add(-20*time.Second)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, activeEventID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt active event: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, readyEventID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt ready event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-active', 'pending', 0, 'matched_agent_subscription', now()),
			($1::uuid, $2::uuid, $3, $4, 'pending', 0, 'replay_scope_marker', now()),
			($5::uuid, $6::uuid, 'agent', 'agent-done', 'delivered', 0, 'handled', now())
	`, blockedRunID, activeEventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, readyRunID, readyEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, fire_event, fire_payload,
			fire_at, owner_agent, task_type, status, created_at
		)
		VALUES
			(gen_random_uuid(), $1::uuid, 'due', 'quiescence.timeout', '{}'::jsonb, now() - interval '1 minute', 'timer-agent', 'timer', 'active', now()),
			(gen_random_uuid(), $2::uuid, 'settled', 'quiescence.timeout', '{}'::jsonb, now() - interval '1 minute', 'timer-agent', 'timer', 'fired', now())
	`, blockedRunID, readyRunID); err != nil {
		t.Fatalf("seed timers: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, llm_backend, conversation_mode, created_at)
		VALUES ('quiescence-agent', 'worker', 'standard', 'mock', 'session', now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, runtime_mode, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES
			(gen_random_uuid(), $1::uuid, 'quiescence-agent', 'global', 'global', 'session', '{}'::jsonb,
				'worker-1', now() + interval '1 minute', 'active', now(), now()),
			(gen_random_uuid(), $2::uuid, 'quiescence-agent', 'global-ready', 'global', 'session', '{}'::jsonb,
				'worker-1', now() - interval '1 minute', 'active', now(), now())
	`, blockedRunID, readyRunID); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}

	blocked, err := pg.LoadRunDebugReport(ctx, blockedRunID, RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport blocked: %v", err)
	}
	assertRunTestQuiescence(t, blocked.TestQuiescence, RunTestQuiescence{
		Ready:                   false,
		ActiveDeliveries:        1,
		UnsettledPipelineEvents: 1,
		DueTimers:               1,
		ActiveSessionLeases:     1,
	})

	ready, err := pg.LoadRunDebugReport(ctx, readyRunID, RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport ready: %v", err)
	}
	assertRunTestQuiescence(t, ready.TestQuiescence, RunTestQuiescence{Ready: true})
}

func TestRunDebugReadSurface_LoadRunDebugTrace_JoinsEventDeliverySessionAndTurn(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	entityID := uuid.NewString()
	replyContextID := "reply-v1:trace-context"
	projectedInstanceID := uuid.NewString()
	now := time.Unix(1700000400, 0).UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, now.Add(-5*time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'scan.requested', $3::uuid, 'entity', '{}'::jsonb, 'builder', 'platform', $4)
	`, runID, eventID, entityID, now); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	seedRunDebugAgent(t, pg, ctx, "agent-source", entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-source', $3::uuid, 'flow-a', 'entity:' || $3::text, 'entity',
			'[]'::jsonb, 1, 'session', '{}'::jsonb,
			NULL, NULL, 'active', $4, $5
		)
	`, sessionID, runID, entityID, now.Add(1*time.Second), now.Add(3*time.Second)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status,
			retry_count, reason_code, failure, active_session_id, delivery_context,
			delivery_payload_projection, started_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-source', 'failed',
			2, 'handler_error', $4::jsonb, $5::uuid, jsonb_build_object('reply', jsonb_build_object('id', $8::text)),
			jsonb_build_object('fields', jsonb_build_object('validation_case_id', $9::text)), $6, $7
		)
	`, deliveryID, runID, eventID, mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "trace_failure", nil)), sessionID, now.Add(1*time.Second), now.Add(500*time.Millisecond), replyContextID, projectedInstanceID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-source', $3::uuid, 'session', 'entity:' || $4::text, $4::uuid,
			$5::uuid, 'scan.requested', 'task-1', '[]'::jsonb, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb,
			'{}'::jsonb, '{}'::jsonb, true, 12, 1, NULL, $6
		)
	`, turnID, runID, sessionID, entityID, eventID, now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed turn: %v", err)
	}

	rows, err := pg.LoadRunDebugTrace(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTrace: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("trace len = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.EventID != eventID || got.EventName != "scan.requested" {
		t.Fatalf("event trace = %#v", got)
	}
	if got.DeliveryID != deliveryID || got.DeliveryStatus != "failed" || got.SubscriberID != "agent-source" {
		t.Fatalf("delivery trace = %#v", got)
	}
	if got.DeliveryReasonCode != "handler_error" || got.DeliveryFailure == nil || got.DeliveryFailure.Detail.Code != "trace_failure" || got.DeliveryRetryCount != 2 || !got.DeliveryRetryEligible || got.DeliveryTerminal {
		t.Fatalf("delivery failure trace evidence = %#v", got)
	}
	if got.ReplyContextID != replyContextID {
		t.Fatalf("delivery reply context = %q, want %q", got.ReplyContextID, replyContextID)
	}
	if got.DeliveryPayloadProjection == nil || got.DeliveryPayloadProjection.Fields()["validation_case_id"] != projectedInstanceID {
		t.Fatalf("delivery payload projection = %#v, want validation_case_id %q", got.DeliveryPayloadProjection, projectedInstanceID)
	}
	if got.SessionID != sessionID || got.SessionKind != "live_session" || got.SessionRuntimeMode != "session" {
		t.Fatalf("session trace = %#v", got)
	}
	if got.TurnID != turnID || got.TurnTriggerEventID != eventID || got.TurnTaskID != "task-1" || got.TurnRetryCount != 1 {
		t.Fatalf("turn trace = %#v", got)
	}
	if got.TurnCreatedAt == nil || got.DeliveryStartedAt == nil {
		t.Fatalf("expected non-nil turn/delivery timestamps: %#v", got)
	}
}

func TestRunDebugReadSurface_LoadRunDebugTrace_SinceUsesRowMaterializationWatermark(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	entityID := uuid.NewString()
	base := time.Unix(1700000450, 0).UTC()
	since := base.Add(time.Second)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, base.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'scan.requested', $3::uuid, 'entity', '{}'::jsonb, 'builder', 'platform', $4)
	`, runID, eventID, entityID, base); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	seedRunDebugAgent(t, pg, ctx, "agent-late", entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-late', $3::uuid, 'flow-a', 'entity:' || $3::text, 'entity',
			'[]'::jsonb, 1, 'session', '{}'::jsonb,
			NULL, NULL, 'active', $4, $5
		)
	`, sessionID, runID, entityID, base.Add(2*time.Second), base.Add(2*time.Second)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, active_session_id, started_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-late', 'in_progress', 'session_started',
			$4::uuid, $5, $5
		)
	`, deliveryID, runID, eventID, sessionID, base.Add(2*time.Second)); err != nil {
		t.Fatalf("seed late delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-late', $3::uuid, 'session', 'entity:' || $4::text, $4::uuid,
			$5::uuid, 'scan.requested', 'task-late', '[]'::jsonb, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb,
			'{}'::jsonb, '{}'::jsonb, true, 12, 0, NULL, $6
		)
	`, turnID, runID, sessionID, entityID, eventID, base.Add(3*time.Second)); err != nil {
		t.Fatalf("seed late turn: %v", err)
	}

	rows, err := pg.LoadRunDebugTrace(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Since: &since})
	if err != nil {
		t.Fatalf("LoadRunDebugTrace since: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("trace len = %d, want late composed row: %#v", len(rows), rows)
	}
	got := rows[0]
	if got.EventID != eventID || got.DeliveryID != deliveryID || got.TurnID != turnID {
		t.Fatalf("late materialized trace row = %#v", got)
	}
}

func TestRunDebugReadSurface_LoadRunDebugTrace_UsesTaskAuditSessionWhenLiveSessionDoesNotExist(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	sessionID := uuid.NewString()
	turnID := uuid.NewString()
	entityID := uuid.NewString()
	now := time.Unix(1700000500, 0).UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, now.Add(-5*time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'task.started', $3::uuid, 'entity', '{}'::jsonb, 'builder', 'platform', $4)
	`, runID, eventID, entityID, now); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	seedRunDebugAgent(t, pg, ctx, "agent-task", entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope, conversation,
			turn_count, runtime_mode, runtime_state, run_id, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, 'agent-task', $2::uuid, 'flow-a', 'entity:' || $2::text, 'entity', '[]'::jsonb,
			1, 'task', '{}'::jsonb, $3::uuid, 'active', $4, $5
		)
	`, sessionID, entityID, runID, now.Add(1*time.Second), now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed audit session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-task', 'delivered', 'handled', $4
		)
	`, deliveryID, runID, eventID, now.Add(500*time.Millisecond)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-task', $3::uuid, 'task', 'entity:' || $4::text, $4::uuid,
			$5::uuid, 'task.started', 'task-2', '[]'::jsonb, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb,
			'{}'::jsonb, '{}'::jsonb, true, 8, 0, NULL, $6
		)
	`, turnID, runID, sessionID, entityID, eventID, now.Add(3*time.Second)); err != nil {
		t.Fatalf("seed turn: %v", err)
	}

	rows, err := pg.LoadRunDebugTrace(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTrace: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("trace len = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.SessionID != sessionID || got.SessionKind != "turn_audit" || got.SessionRuntimeMode != "task" {
		t.Fatalf("task audit trace = %#v", got)
	}
}

func TestRunDebugTraceSessionSources_NullsMissingRunIDColumnsForCanonicalConversationVariants(t *testing.T) {
	caps := StoreSchemaCapabilities{
		Conversations: ConversationSchemaCapabilities{
			Sessions:     SchemaFlavorCanonical,
			Audits:       SchemaFlavorCanonical,
			SessionRunID: false,
			AuditRunID:   false,
		},
	}

	sql := runDebugTraceSessionSources(caps)
	if strings.Contains(sql, "SELECT\n\t\t\t\tsession_id,\n\t\t\t\trun_id,") {
		t.Fatalf("session source sql still selects raw run_id without capability guard:\n%s", sql)
	}
	if strings.Count(sql, "NULL::uuid") < 2 {
		t.Fatalf("session source sql = %q, want NULL::uuid projection for both canonical variants", sql)
	}
	if !strings.Contains(sql, "FROM agent_sessions") || !strings.Contains(sql, "FROM agent_conversation_audits") {
		t.Fatalf("session source sql = %q, want both canonical conversation sources", sql)
	}
}
