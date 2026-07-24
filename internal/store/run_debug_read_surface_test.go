package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func seedRunDebugAgent(t *testing.T, pg *PostgresStore, ctx context.Context, agentID string, entityID string, memory agentmemory.Plan, flowPath string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          agentID,
			FlowID:        "operating",
			Model:         "regular",
			ExecutionMode: "live",
			Memory:        memory,
			FlowPath:      flowPath,
			EntityID:      entityID,
			Config:        json.RawMessage(`{"system_prompt":"You are a trace test agent.","tools":[],"subscriptions":["trace.*"]}`),
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, olderEventID, olderRunID, "scan.requested", events.EventProducerAgent, "test", "", "", now.Add(-119*time.Minute))
	seedPostgresSemanticEventRecordFixture(t, ctx, db, uuid.NewString(), olderRunID, "scan.completed", events.EventProducerAgent, "test", "", "", now.Add(-91*time.Minute))
	seedPostgresSemanticEventRecordFixture(t, ctx, db, newerEventID, newerRunID, "scan.requested", events.EventProducerAgent, "test", "", "", now.Add(-59*time.Minute))
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, uuid.NewString(), targetRunID, "scan.corpus_file_requested", events.EventProducerAgent, "builder", "", "", now.Add(time.Second))
	seedPostgresSemanticEventRecordFixture(t, ctx, db, uuid.NewString(), olderRunID, "scan.requested", events.EventProducerAgent, "builder", "", "", now.Add(-59*time.Minute))

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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, targetEventID, targetRunID, "scan.requested", events.EventProducerAgent, "test", targetEntityID, "", now.Add(-4*time.Minute))
	targetEvent := loadPostgresDeliveryFixtureEvent(t, ctx, db, targetEventID)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, otherEventID, otherRunID, "scan.requested", events.EventProducerAgent, "test", otherEntityID, "", now.Add(-3*time.Minute))
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
		seedPostgresRuntimeLogEventRecordFixture(t, ctx, pg, uuid.NewString(), runID, "", payload, createdAt)
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
	failedDeliveryEnvelope := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "handler_failed", nil)
	failedDelivery := seedDeliveryStateFixture(t, ctx, pg, targetEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-1",
	}, runtimedelivery.StateExhausted, &failedDeliveryEnvelope)
	setPostgresDeliveryFixtureTimes(t, ctx, db, failedDelivery, now.Add(5*time.Second), now.Add(10*time.Second))
	successfulDelivery := seedDeliveryStateFixture(t, ctx, pg, targetEvent, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberNode),
		SubscriberID:   "node-success",
	}, runtimedelivery.StateDelivered, nil)
	setPostgresDeliveryFixtureTimes(t, ctx, db, successfulDelivery, now.Add(15*time.Second), now.Add(20*time.Second))
	successDeliveryID := successfulDelivery.DeliveryID
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
	if got := report.FailedDeliveries[0]; got.SubscriberType != "agent" || got.RetryCount != 0 || got.RetryEligible || !got.Terminal || len(got.DeadLetters) != 1 {
		t.Fatalf("FailedDeliveries[0] = %#v", got)
	}
	if report.FailedDeliveries[0].DeliveryID == successDeliveryID {
		t.Fatalf("successful delivered/node_processed delivery appeared in FailedDeliveries: %#v", report.FailedDeliveries)
	}
}

func TestRunDebugReadSurface_LoadRunDebugReport_ProjectsTestQuiescenceCounts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	blockedRunID := uuid.NewString()
	readyRunID := uuid.NewString()
	activeEventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	runtimeLogEventID := uuid.NewString()
	readyEventID := uuid.NewString()
	inboundEvidenceEventID := uuid.NewString()
	directiveEvidenceEventID := uuid.NewString()
	now := time.Now().UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES
			($1::uuid, 'running', $3),
			($2::uuid, 'running', $3)
	`, blockedRunID, readyRunID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	activeEvent := eventtest.PersistedRuntimeControlForProducer(
		activeEventID, events.EventType("quiescence.active_delivery"), eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0,
		blockedRunID, "", events.EventEnvelope{}, now.Add(-50*time.Second),
	)
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, activeEvent, []string{"agent-active"}); err != nil {
		t.Fatalf("seed active delivery event: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, unsettledEventID, blockedRunID, "quiescence.missing_pipeline_receipt", events.EventProducerPlatform, "test", "", "", now.Add(-40*time.Second))
	seedPostgresRuntimeLogEventRecordFixture(t, ctx, pg, runtimeLogEventID, blockedRunID, "", []byte(`{}`), now.Add(-30*time.Second))
	readyEvent := eventtest.PersistedRuntimeControlForProducer(
		readyEventID, events.EventType("quiescence.ready"), eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0,
		readyRunID, "", events.EventEnvelope{}, now.Add(-20*time.Second),
	)
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, readyEvent, []string{"agent-done"}); err != nil {
		t.Fatalf("seed ready delivery event: %v", err)
	}
	readyRoute := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-done"}
	readyClaim, err := pg.ClaimAgentDelivery(ctx, readyEvent, readyRoute)
	if err != nil {
		t.Fatalf("claim ready delivery: %v", err)
	}
	if _, err := pg.SettleSuccess(ctx, readyClaim.Claim, nil, 0); err != nil {
		t.Fatalf("settle ready delivery: %v", err)
	}
	seedPostgresSemanticEventRecordFixture(t, ctx, db, inboundEvidenceEventID, readyRunID, events.EventTypePlatformInboundRecord, events.EventProducerPlatform, "test", "", "", now.Add(-20*time.Second))
	seedPostgresSemanticEventRecordFixture(t, ctx, db, directiveEvidenceEventID, readyRunID, events.EventTypePlatformAgentDirective, events.EventProducerPlatform, "test", "", "", now.Add(-20*time.Second))
	if err := acknowledgePipelineEventFixture(ctx, pg, activeEventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt active event: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, readyEventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt ready event: %v", err)
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
		INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, created_at)
		VALUES ('quiescence-agent', 'quiescence', 'worker', 'regular', 'mock', TRUE, 'authored', now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES
			(gen_random_uuid(), $1::uuid, 'quiescence-agent', 'quiescence', TRUE, 'authored', '{}'::jsonb,
				'worker-1', now() + interval '1 minute', 'active', now(), now()),
			(gen_random_uuid(), $2::uuid, 'quiescence-agent', 'quiescence', TRUE, 'authored', '{}'::jsonb,
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "scan.requested", events.EventProducerPlatform, "builder", entityID, "", now)
	event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	seedRunDebugAgent(t, pg, ctx, "agent-source", entityID, agentmemory.Authored(true), "flow-a")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-source', 'flow-a', TRUE, 'authored',
			'[]'::jsonb, 1, '{}'::jsonb,
			NULL, NULL, 'active', $3, $4
		)
	`, sessionID, runID, now.Add(1*time.Second), now.Add(3*time.Second)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"validation_case_id": projectedInstanceID})
	if err != nil {
		t.Fatalf("construct delivery payload projection: %v", err)
	}
	route := events.DeliveryRoute{
		SubscriberType:    string(runtimedelivery.SubscriberAgent),
		SubscriberID:      "agent-source",
		Context:           events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}},
		PayloadProjection: projection,
	}
	if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
		t.Fatalf("commit delivery: %v", err)
	}
	claimed, err := pg.ClaimAgentDelivery(ctx, event, route)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if _, err := pg.BindAgentSession(ctx, claimed.Claim, sessionID); err != nil {
		t.Fatalf("bind delivery session: %v", err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "trace_failure", nil)
	failedDelivery, err := pg.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureRetry,
		ReasonCode:  "handler_error",
		Failure:     &failure,
		RetryBase:   time.Hour,
	})
	if err != nil {
		t.Fatalf("settle delivery failure: %v", err)
	}
	setPostgresDeliveryFixtureTimes(t, ctx, db, failedDelivery, now.Add(500*time.Millisecond), now.Add(1*time.Second))
	deliveryID := failedDelivery.DeliveryID
	capabilitySurfaceID := seedManagedAgentTurnCapabilitySurface(t, pg, runID, "agent-source", sessionID, turnID, "session", "entity:"+entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-source', $3::uuid, 'flow-a', TRUE, 'authored', $4::uuid,
			$5::uuid, 'scan.requested', 'task-1', $6::uuid, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '{}'::jsonb, true, 12, 1, NULL, 'live', $7
		)
	`, turnID, runID, sessionID, entityID, eventID, capabilitySurfaceID, now.Add(2*time.Second)); err != nil {
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
	if got.DeliveryReasonCode != "handler_error" || got.DeliveryFailure == nil || got.DeliveryFailure.Detail.Code != "trace_failure" || got.DeliveryRetryCount != 1 || !got.DeliveryRetryEligible || got.DeliveryTerminal {
		t.Fatalf("delivery failure trace evidence = %#v", got)
	}
	if got.ReplyContextID != replyContextID {
		t.Fatalf("delivery reply context = %q, want %q", got.ReplyContextID, replyContextID)
	}
	if got.DeliveryPayloadProjection == nil || got.DeliveryPayloadProjection.Fields()["validation_case_id"] != projectedInstanceID {
		t.Fatalf("delivery payload projection = %#v, want validation_case_id %q", got.DeliveryPayloadProjection, projectedInstanceID)
	}
	if got.SessionID != sessionID || got.SessionKind != "live_session" || !got.SessionMemory || got.SessionMemorySource != "authored" {
		t.Fatalf("session trace = %#v", got)
	}
	if !got.TurnMemory || got.TurnMemorySource != "authored" || got.TurnFlowInstance != "flow-a" {
		t.Fatalf("turn memory trace = %#v", got)
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "scan.requested", events.EventProducerPlatform, "builder", entityID, "", base)
	event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	seedRunDebugAgent(t, pg, ctx, "agent-late", entityID, agentmemory.Authored(true), "flow-a")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-late', 'flow-a', TRUE, 'authored',
			'[]'::jsonb, 1, '{}'::jsonb,
			NULL, NULL, 'active', $3, $4
		)
	`, sessionID, runID, base.Add(2*time.Second), base.Add(2*time.Second)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-late"}
	if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
		t.Fatalf("commit late delivery: %v", err)
	}
	claimed, err := pg.ClaimAgentDelivery(ctx, event, route)
	if err != nil {
		t.Fatalf("claim late delivery: %v", err)
	}
	lateDelivery, err := pg.BindAgentSession(ctx, claimed.Claim, sessionID)
	if err != nil {
		t.Fatalf("bind late delivery session: %v", err)
	}
	setPostgresDeliveryFixtureTimes(t, ctx, db, lateDelivery, base.Add(2*time.Second), base.Add(2*time.Second))
	deliveryID := lateDelivery.DeliveryID
	capabilitySurfaceID := seedManagedAgentTurnCapabilitySurface(t, pg, runID, "agent-late", sessionID, turnID, "session", "entity:"+entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-late', $3::uuid, 'flow-a', TRUE, 'authored', $4::uuid,
			$5::uuid, 'scan.requested', 'task-late', $6::uuid, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '{}'::jsonb, true, 12, 0, NULL, 'live', $7
		)
	`, turnID, runID, sessionID, entityID, eventID, capabilitySurfaceID, base.Add(3*time.Second)); err != nil {
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
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
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
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "task.started", events.EventProducerPlatform, "builder", entityID, "", now)
	event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	seedRunDebugAgent(t, pg, ctx, "agent-task", entityID, agentmemory.PlatformDefault(), "")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, memory_enabled, memory_source, conversation,
			turn_count, runtime_state, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $3::uuid, 'agent-task', $2::uuid, 'flow-a', FALSE, 'platform_default', '[]'::jsonb,
			1, '{}'::jsonb, 'active', $4, $5
		)
	`, sessionID, entityID, runID, now.Add(1*time.Second), now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed audit session: %v", err)
	}
	delivered := seedDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "agent-task",
	}, runtimedelivery.StateDelivered, nil)
	setPostgresDeliveryFixtureTimes(t, ctx, db, delivered, now.Add(500*time.Millisecond), now.Add(500*time.Millisecond))
	capabilitySurfaceID := seedManagedAgentTurnCapabilitySurface(t, pg, runID, "agent-task", sessionID, turnID, "task", "entity:"+entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure, execution_mode, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'agent-task', $3::uuid, 'flow-a', FALSE, 'platform_default', $4::uuid,
			$5::uuid, 'task.started', 'task-2', $6::uuid, '[]'::jsonb,
			'[]'::jsonb, '{}'::jsonb, '{}'::jsonb, true, 8, 0, NULL, 'live', $7
		)
	`, turnID, runID, sessionID, entityID, eventID, capabilitySurfaceID, now.Add(3*time.Second)); err != nil {
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
	if got.SessionID != sessionID || got.SessionKind != "turn_audit" || got.SessionMemory || got.SessionMemorySource != "platform_default" {
		t.Fatalf("task audit trace = %#v", got)
	}
	if got.TurnMemory || got.TurnMemorySource != "platform_default" || got.TurnFlowInstance != "flow-a" {
		t.Fatalf("stateless turn trace = %#v", got)
	}
}

func TestRunDebugTraceSessionSourcesUseCanonicalRunIDColumns(t *testing.T) {
	sql := runDebugTraceSessionSources()
	if strings.Count(sql, "run_id") < 2 {
		t.Fatalf("session source sql = %q, want canonical run_id projection for both sources", sql)
	}
	if strings.Contains(sql, "NULL::uuid") {
		t.Fatalf("session source sql = %q, must not preserve missing-run_id fallback", sql)
	}
	if !strings.Contains(sql, "FROM agent_sessions") || !strings.Contains(sql, "FROM agent_conversation_audits") {
		t.Fatalf("session source sql = %q, want both canonical conversation sources", sql)
	}
}
