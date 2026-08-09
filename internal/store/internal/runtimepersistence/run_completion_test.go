package runtimepersistence

import (
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type normalRunCompletionFixture struct {
	RunID    string
	EventID  string
	EntityID string
}

func seedNormalRunCompletionFixture(t *testing.T, db *sql.DB, state, flowInstance, flowTemplate string) normalRunCompletionFixture {
	t.Helper()
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if flowInstance == "" {
		flowInstance = "example"
	}
	if flowTemplate == "" {
		flowTemplate = "example"
	}
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{
		RunID: runID, Origin: semanticEventRunOriginForTest(t, eventID, "example.started"),
		StartedAt: time.Now().UTC(),
	})
	seedPostgresSemanticEventRecordFixture(
		t, ctx, db, eventID, runID, events.EventType("example.started"),
		events.EventProducerExternal, "test", entityID, flowInstance, time.Now().UTC(),
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin pipeline scope fixture: %v", err)
	}
	if err := insertCommittedPipelineScopeTx(ctx, tx, eventID, runtimepipelineobligation.ScopeDirect, true, time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed committed pipeline scope: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit pipeline scope fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES ($1, $2, 'static', '{}'::jsonb, 'active', now())
			ON CONFLICT (instance_id) DO UPDATE SET flow_template = EXCLUDED.flow_template
		`, flowInstance, flowTemplate); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, NULLIF($3,''), 'default', 'example', 'Example', $4,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID, flowInstance, state); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	return normalRunCompletionFixture{RunID: runID, EventID: eventID, EntityID: entityID}
}

func normalRunCompletionRootFlowTerminals() map[string][]string {
	return map[string][]string{"example": []string{"done"}}
}

func assertRunCompletionStatus(t *testing.T, db *sql.DB, runID, want string, wantEnded bool) {
	t.Helper()
	var (
		status  string
		endedAt sql.NullTime
	)
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT COALESCE(status, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &endedAt); err != nil {
		t.Fatalf("load run status: %v", err)
	}
	if status != want {
		t.Fatalf("run status = %q, want %q", status, want)
	}
	if endedAt.Valid != wantEnded {
		t.Fatalf("ended_at valid = %v, want %v", endedAt.Valid, wantEnded)
	}
}

func TestPostgresStore_ConvergeNormalRunCompletion_MarksCompletedWhenTerminalAndIdle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "review/inst-1", "review")
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := commitDiagnosticRuntimeLogFixture(ctx, pg, eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventType(runtimeLogEventName), "runtime", "", []byte(`{"message":"diagnostic"}`), 0,
		fixture.RunID, fixture.EventID, events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	)); err != nil {
		t.Fatalf("seed runtime log: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, map[string][]string{"review": []string{"done"}}); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)

	header, err := pg.LoadRunHeader(ctx, fixture.RunID)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.Status != "completed" || header.EndedAt == nil {
		t.Fatalf("run header = status:%q ended:%v, want completed with ended_at", header.Status, header.EndedAt)
	}
	report, err := pg.LoadRunDebugReport(ctx, fixture.RunID, operatorread.RunDebugQueryOptions{})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if got := operatorread.ProjectRunOperationalStatus(report).State; got != "completed" {
		t.Fatalf("run operational state = %q, want completed", got)
	}
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedWithMissingPipelineReceipt(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "", "")
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedWhileDeliveryActive(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "", "")
	deliveryEvent := eventtest.PersistedChildForProducer(
		uuid.NewString(), events.EventType("completion.agent.delivery"), eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0,
		fixture.RunID, fixture.EventID, events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, deliveryEvent, []string{"agent-1"}); err != nil {
		t.Fatalf("seed active delivery: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, deliveryEvent.ID()); err != nil {
		t.Fatalf("seed delivery-event pipeline receipt: %v", err)
	}
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-1")}
	claimed, err := claimDeliveryFixture(ctx, pg, deliveryEvent, route)
	if err != nil {
		t.Fatalf("claim active delivery: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion active: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)

	if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, 0); err != nil {
		t.Fatalf("SettleSuccess: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion settled: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedUntilNodeDeliverySettled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "", "")
	deliveryEvent := eventtest.PersistedChildForProducer(
		uuid.NewString(), events.EventType("completion.node.delivery"), eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0,
		fixture.RunID, fixture.EventID, events.EventEnvelope{}, time.Now().UTC(),
	)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("terminal-node")}
	if err := commitSemanticEventFixtureWithRoutes(ctx, pg, deliveryEvent, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("seed active node delivery: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, deliveryEvent.ID()); err != nil {
		t.Fatalf("seed delivery-event pipeline receipt: %v", err)
	}
	claimed, err := claimDeliveryFixture(ctx, pg, deliveryEvent, route)
	if err != nil {
		t.Fatalf("claim active node delivery: %v", err)
	}
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion active node: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)

	if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, 0); err != nil {
		t.Fatalf("settle node delivery: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion settled node: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedWhileTimerActiveThenCompletesAfterTimerSettled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "", "")
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	timer := admitGenericScheduleFixture(t, ctx, pg, testRootGenericScheduleCommand(
		t, fixture.RunID, fixture.EntityID, "run-completion-wait", runtimegenericschedule.DelayDue(time.Minute),
	))
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion active timer: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)

	cancelGenericScheduleFixture(t, ctx, pg, timer, "test_settled", time.Now().UTC())
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion settled timer: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedWhileSessionLeaseActive(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "done", "", "")
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	sessionID := uuid.NewString()
	identity := testAgentIdentity(t, "agent-1", "completion")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			conversation, turn_count, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'[]'::jsonb, 0, '{}'::jsonb,
			'worker-1', now() + interval '1 minute', 'active', now(), now()
		)
	`, sessionID, fixture.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
		t.Fatalf("seed active session lease: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion active session: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET lease_holder = NULL,
		    lease_expires_at = NULL
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("release session lease: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion released session: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)
}

func TestPostgresStore_ConvergeNormalRunCompletion_FailsClosedWhenEntityNotTerminal(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	fixture := seedNormalRunCompletionFixture(t, db, "working", "", "")
	if err := acknowledgePipelineEventFixture(ctx, pg, fixture.EventID); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "running", false)

	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET current_state = 'done' WHERE run_id = $1::uuid`, fixture.RunID); err != nil {
		t.Fatalf("advance entity terminal: %v", err)
	}
	if err := executeRunCompletionCandidateForEvent(ctx, pg, fixture.EventID, []string{"done"}, normalRunCompletionRootFlowTerminals()); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion terminal: %v", err)
	}
	assertRunCompletionStatus(t, db, fixture.RunID, "completed", true)
}
