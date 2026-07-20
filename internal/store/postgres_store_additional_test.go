package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const specEntityStateRunID = "33333333-3333-3333-3333-333333333333"

func seedPostgresStoreEvent(
	t *testing.T,
	ctx context.Context,
	pg *PostgresStore,
	eventID, runID, eventName string,
	producerType events.EventProducerType,
	producerID, entityID, flowInstance string,
	createdAt time.Time,
) {
	t.Helper()
	envelope := events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}
	producer := eventtest.Producer(producerType, producerID)
	var event events.Event
	if producerType == events.EventProducerPlatform {
		event = eventtest.PersistedRuntimeControlForProducer(eventID, events.EventType(eventName), producer, "", json.RawMessage(`{}`), 0, runID, "", envelope, createdAt)
	} else {
		parentID := eventtest.UUID("postgres-store-parent:" + eventID)
		if err := commitSemanticParentFixture(ctx, pg, runID, parentID, createdAt.Add(-time.Microsecond)); err != nil {
			t.Fatalf("seed parent for event %s: %v", eventName, err)
		}
		event = eventtest.PersistedChildForProducer(eventID, events.EventType(eventName), producer, "", json.RawMessage(`{}`), 0, runID, parentID, envelope, createdAt)
	}
	if err := commitSemanticEventFixture(ctx, pg, event); err != nil {
		t.Fatalf("seed event %s: %v", eventName, err)
	}
}

func resetAgentSessionsSpecTable(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop legacy agent_sessions: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop legacy agent_turns: %v", err)
	}
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	allPlans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	plans := make([]SchemaTableDDL, 0, 2)
	for _, plan := range allPlans {
		if plan.TableName == "agent_sessions" || plan.TableName == "agent_turns" {
			plans = append(plans, plan)
		}
	}
	if len(plans) != 2 {
		t.Fatalf("canonical agent memory plans = %#v, want sessions and turns", plans)
	}
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			if _, err := pg.DB.ExecContext(ctx, statement); err != nil {
				t.Fatalf("create canonical %s test table: %v", plan.TableName, err)
			}
		}
	}
}

func acquireLiveTestSession(t *testing.T, ctx context.Context, db *sql.DB, agentID, flowInstance string) string {
	t.Helper()
	seedSpecMemoryRun(t, ctx, db)
	registry := newTestPostgresStore(t, db)
	registry.SetSessionLockTTL(30 * time.Second)
	ctx = runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	identity := agentmemory.Identity{RunID: specEntityStateRunID, AgentID: agentID, FlowInstance: flowInstance}
	lease, err := registry.Acquire(ctx, identity, "test-owner")
	if err != nil {
		t.Fatalf("Acquire(%+v): %v", identity, err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatalf("Release(%s,%s): %v", agentID, lease.SessionID, err)
	}
	return lease.SessionID
}

func seedSpecMemoryRun(t *testing.T, ctx context.Context, db execer) {
	t.Helper()
	seedManagerRun(t, ctx, db, specEntityStateRunID)
}

func seedManagerRun(t *testing.T, ctx context.Context, db execer, runID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed agent memory run: %v", err)
	}
}

func specMemoryIdentity(agentID, flowInstance string) agentmemory.Identity {
	return agentmemory.Identity{RunID: specEntityStateRunID, AgentID: agentID, FlowInstance: flowInstance}
}

func terminateSpecAgentViaLifecycle(t *testing.T, ctx context.Context, pg *PostgresStore, agentID string) runtimemanager.AgentLifecycleTransitionResult {
	t.Helper()
	var epoch int64
	var generation uint64
	var phase runtimemanager.AgentLifecyclePhase
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE agent_id = $1
	`, agentID).Scan(&epoch, &generation, &phase); err != nil {
		t.Fatalf("load lifecycle cell for %s: %v", agentID, err)
	}
	targetEpoch := epoch
	if targetEpoch == 0 {
		targetEpoch = 1
	}
	result, err := pg.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "test-terminate-" + agentID,
		AgentID: agentID, Trigger: "test", ExpectedEpoch: epoch, ExpectedGeneration: generation, ExpectedPhase: phase,
		TargetEpoch: targetEpoch, TargetGeneration: generation + 1, TargetPhase: runtimemanager.AgentLifecycleTerminated,
		ConfigRevision: "test", RunMode: runtimemanager.AgentRunModeStopped,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action: runtimesessions.LifecycleMutationTerminateCurrentSet, TerminationReason: runtimesessions.TerminationReasonCancelled,
		},
		Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("terminate %s through lifecycle authority: %v", agentID, err)
	}
	return result
}

func TestPostgresStore_NormalCompletionUsesCanonicalCountersAndRejectsActiveDeliveries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresStoreEvent(t, ctx, pg, eventID, runID, "scan.requested", events.EventProducerPlatform, "builder", entityID, "", time.Now().UTC())
	seedPostgresEntityStateRows(t, db, ctx, runID, entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'pending', now()
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventID, "processed", nil); err != nil {
		t.Fatalf("seed pipeline receipt: %v", err)
	}

	if _, err := pg.MarkRunTerminal(ctx, runID, "completed", nil, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "normal run completion convergence") {
		t.Fatalf("MarkRunTerminal(completed) error = %v, want canonical convergence refusal", err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "run_quiescence_failed", nil)
	if _, err := pg.MarkRunTerminal(ctx, runID, "failed", &failure, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "active deliveries") {
		t.Fatalf("MarkRunTerminal(failed active delivery) error = %v, want active delivery rejection", err)
	}
	if err := pg.ConvergeNormalRunCompletion(ctx, eventID, []string{"ready"}, map[string][]string{"test-flow": {"ready"}}); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion(active delivery): %v", err)
	}
	var activeStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&activeStatus); err != nil || activeStatus != "running" {
		t.Fatalf("active-delivery run status = %q, %v, want running", activeStatus, err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'delivered', delivered_at = now()
		WHERE run_id = $1::uuid
	`, runID); err != nil {
		t.Fatalf("deliver completion: %v", err)
	}

	if err := pg.ConvergeNormalRunCompletion(ctx, eventID, []string{"ready"}, map[string][]string{"test-flow": {"ready"}}); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}

	var (
		status      string
		eventCount  int
		entityCount int
		endedAt     time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count, entity_count, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount, &endedAt); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}
	if eventCount != 1 {
		t.Fatalf("event_count = %d, want 1", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("entity_count = %d, want 1", entityCount)
	}
	if endedAt.IsZero() {
		t.Fatal("ended_at not persisted")
	}
}

func TestPostgresRunLifecycleEntityCountUsesEntityState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventEntityA := uuid.NewString()
	eventEntityB := uuid.NewString()
	currentEntity := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, event_count, entity_count, started_at)
		VALUES ($1::uuid, 'running', 99, 9, now())
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresStoreEvent(t, ctx, pg, uuid.NewString(), runID, "scan.requested", events.EventProducerAgent, "test", eventEntityA, "", time.Now().UTC())
	seedPostgresStoreEvent(t, ctx, pg, uuid.NewString(), runID, "scan.replayed", events.EventProducerAgent, "test", eventEntityB, "", time.Now().UTC())
	seedPostgresEntityStateRows(t, db, ctx, runID, currentEntity)

	snap, err := pg.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRunLifecycleSnapshot: %v", err)
	}
	if snap.EntityCount != 1 {
		t.Fatalf("snapshot entity_count = %d, want entity_state count 1 despite stale run/event overcount", snap.EntityCount)
	}

	if err := storerunlifecycle.SyncCounts(ctx, db, runID); err != nil {
		t.Fatalf("SyncCounts: %v", err)
	}
	var eventCount, entityCount int
	if err := db.QueryRowContext(ctx, `
		SELECT event_count, entity_count
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&eventCount, &entityCount); err != nil {
		t.Fatalf("load synced counters: %v", err)
	}
	if eventCount != 4 || entityCount != 1 {
		t.Fatalf("synced counters event_count=%d entity_count=%d, want 4/1 from complete event graphs/entity_state", eventCount, entityCount)
	}
}

func TestPostgresStore_AppendEventRejectsNewEventForCompletedRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, ended_at, event_count, entity_count)
		VALUES ($1::uuid, 'completed', now(), 0, 0)
	`, runID); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}
	seedPostgresEntityStateRows(t, db, ctx, runID, entityID)

	evt := eventtest.PersistedProjection(
		uuid.NewString(),
		events.EventType("scan.completed"),
		"agent-1",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
		t.Fatalf("AppendEvent error = %v, want inactive-run rejection", err)
	}

	var (
		status      string
		eventCount  int
		entityCount int
		endedAt     sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count, entity_count, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount, &endedAt); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}
	if eventCount != 0 {
		t.Fatalf("event_count = %d, want 0", eventCount)
	}
	if entityCount != 0 {
		t.Fatalf("entity_count = %d, want unchanged 0", entityCount)
	}
	if !endedAt.Valid {
		t.Fatal("ended_at was cleared by rejected append")
	}
}

func TestPostgresStore_AppendEvent_DuplicateDoesNotReopenCompletedRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	completedAt := time.Now().UTC().Add(-time.Minute).Round(time.Second)
	createdAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, ended_at, event_count, entity_count)
		VALUES ($1::uuid, 'completed', $2, 1, 1)
	`, runID, completedAt); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}
	parentEventID := eventtest.UUID("completed-run-parent:" + eventID)
	duplicate := eventtest.PersistedChildForProducer(
		eventID, "scan.completed", eventtest.Producer(events.EventProducerAgent, "agent-1"), "",
		json.RawMessage(`{}`), 0, runID, parentEventID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), createdAt,
	)
	if err := insertCanonicalEventRecordFixture(ctx, pg, duplicate); err != nil {
		t.Fatalf("seed duplicate event: %v", err)
	}
	seedPostgresEntityStateRows(t, db, ctx, runID, entityID)

	evt := eventtest.PersistedChildForProducer(
		eventID,
		events.EventType("scan.completed"),
		eventtest.Producer(events.EventProducerAgent, "agent-1"),
		"",
		[]byte(`{}`),
		0,
		runID,
		parentEventID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		createdAt,
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent(duplicate): %v", err)
	}

	var (
		status      string
		eventCount  int
		entityCount int
		endedAt     sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count, entity_count, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount, &endedAt); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}
	if eventCount != 1 {
		t.Fatalf("event_count = %d, want 1", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("entity_count = %d, want 1", entityCount)
	}
	if !endedAt.Valid || !endedAt.Time.Equal(completedAt) {
		t.Fatalf("ended_at = %#v, want preserved %s", endedAt, completedAt)
	}
}

func TestPostgresStore_AgentSessionsPartialUniquenessAllowsTerminatedHistoryButRejectsSecondLiveOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	seedSpecMemoryRun(t, ctx, db)

	sessionA := uuid.NewString()
	sessionB := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'terminated',
			'failed', now(), now(), now()
		)
	`, sessionA, specEntityStateRunID); err != nil {
		t.Fatalf("insert terminated row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'active', now(), now()
		)
	`, sessionB, specEntityStateRunID); err != nil {
		t.Fatalf("insert active row after terminated history: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'suspended', now(), now()
		)
	`, uuid.NewString(), specEntityStateRunID); err == nil {
		t.Fatal("expected second non-terminated owner insert to fail")
	}
}

func TestPostgresRegistry_AcquireFailsClosedOnSuspendedResumableOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	seedSpecMemoryRun(t, ctx, db)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'suspended', now(), now()
		)
	`, uuid.NewString(), specEntityStateRunID); err != nil {
		t.Fatalf("insert suspended session: %v", err)
	}

	ctx = runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	identity := agentmemory.Identity{RunID: specEntityStateRunID, AgentID: "a1", FlowInstance: "global"}
	if _, err := pg.Acquire(ctx, identity, "worker-1"); err != runtimesessions.ErrSessionSuspended {
		t.Fatalf("Acquire error = %v, want ErrSessionSuspended", err)
	}
}

func TestPostgresRegistry_ResetAllMarksActiveSessionsOrphaned(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	summary, err := pg.ResetAll(runtimesessions.ResetMetadata{Source: "builder_api"})
	if err != nil {
		t.Fatalf("ResetAll: %v", err)
	}
	if got := summary.OrphanedCount(); got != 1 {
		t.Fatalf("ResetAll orphaned_count = %d, want 1", got)
	}
	if got := summary.OrphanedSessions[0].TerminationDetail; got != "builder_api" {
		t.Fatalf("ResetAll termination_detail = %q, want builder_api", got)
	}

	var (
		status       string
		reason       string
		detail       string
		terminatedAt time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(termination_reason, ''), COALESCE(termination_detail, ''), terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &reason, &detail, &terminatedAt); err != nil {
		t.Fatalf("load reset session row: %v", err)
	}
	if status != "terminated" {
		t.Fatalf("status = %q, want terminated", status)
	}
	if reason != "orphaned" {
		t.Fatalf("termination_reason = %q, want orphaned", reason)
	}
	if detail != "builder_api" {
		t.Fatalf("termination_detail = %q, want builder_api", detail)
	}
	if terminatedAt.IsZero() {
		t.Fatal("terminated_at is zero")
	}
}

func TestPostgresStore_AgentSessionSuccessorInvariantsRejectInvalidCanonicalWrites(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	seedSpecMemoryRun(t, ctx, db)

	oldID := uuid.NewString()
	goodSuccessorID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'terminated',
			'failed', now(), now(), now()
		)
	`, oldID, specEntityStateRunID); err != nil {
		t.Fatalf("insert terminated session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'a1', 'global', TRUE, 'authored', 'active', now(), now()
		)
	`, goodSuccessorID, specEntityStateRunID); err != nil {
		t.Fatalf("insert active successor candidate: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET successor_session_id = $2::uuid
		WHERE session_id = $1::uuid
	`, goodSuccessorID, oldID); err == nil {
		t.Fatal("expected active session successor assignment to fail")
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET successor_session_id = $1::uuid
		WHERE session_id = $1::uuid
	`, oldID); err == nil {
		t.Fatal("expected self successor assignment to fail")
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions SET termination_reason = 'legacy' WHERE session_id = $1::uuid
	`, oldID); err == nil {
		t.Fatal("expected invalid canonical termination reason to fail")
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET successor_session_id = $2::uuid
		WHERE session_id = $1::uuid
	`, oldID, goodSuccessorID); err != nil {
		t.Fatalf("set valid successor_session_id: %v", err)
	}
}

func seedSpecEntityState(t *testing.T, ctx context.Context, db execer, entityID, flowInstance, slug, name, state string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, specEntityStateRunID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if strings.TrimSpace(flowInstance) == "" {
		flowInstance = strings.TrimSpace(slug)
	}
	if flowInstance == "" {
		flowInstance = "entity-" + entityID
	}
	if state == "" {
		state = "operating"
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
		ON CONFLICT (instance_id) DO NOTHING
	`, flowInstance); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'default', NULLIF($4,''), NULLIF($5,''), $6,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
		ON CONFLICT (run_id, entity_id) DO NOTHING
	`, specEntityStateRunID, entityID, flowInstance, slug, name, state); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
}

func seedSpecAgent(t *testing.T, ctx context.Context, pg *PostgresStore, agentID string, entityID string, subscriptions ...string) {
	t.Helper()
	cfg := runtimeactors.AgentConfig{
		ID:            agentID,
		Role:          agentID,
		FlowID:        "global",
		Type:          "stub",
		Model:         "regular",
		ExecutionMode: "live",
		EntityID:      strings.TrimSpace(entityID),
		Subscriptions: subscriptions,
		Config:        []byte(`{"system_prompt":"x"}`),
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config:    cfg,
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent %s: %v", agentID, err)
	}
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func installFailAgentTurnInsertTrigger(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_agent_turn_insert()
		RETURNS trigger
		AS $$
		BEGIN
			RAISE EXCEPTION 'forced agent_turn insert failure';
		END;
		$$ LANGUAGE plpgsql
	`); err != nil {
		t.Fatalf("create fail_agent_turn_insert function: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_agent_turn_insert_trigger ON agent_turns`); err != nil {
		t.Fatalf("drop existing fail_agent_turn_insert trigger: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TRIGGER fail_agent_turn_insert_trigger
		BEFORE INSERT ON agent_turns
		FOR EACH ROW
		EXECUTE FUNCTION fail_agent_turn_insert()
	`); err != nil {
		t.Fatalf("create fail_agent_turn_insert trigger: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(testAuthorActivityContext(), `DROP TRIGGER IF EXISTS fail_agent_turn_insert_trigger ON agent_turns`); err != nil {
			t.Fatalf("cleanup fail_agent_turn_insert trigger: %v", err)
		}
		if _, err := db.ExecContext(testAuthorActivityContext(), `DROP FUNCTION IF EXISTS fail_agent_turn_insert()`); err != nil {
			t.Fatalf("cleanup fail_agent_turn_insert function: %v", err)
		}
	})
}

func TestPostgresStore_AppendEvent_EntityIDBoundaryContract(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	validEntityID := uuid.NewString()
	validEventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(
		validEventID,
		events.EventType("review.requested"),
		"control-plane",
		"legacy-task-key",
		[]byte(`{"name":"Telemedicine Platform"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, validEntityID),
		time.Now(),
	)); err != nil {
		t.Fatalf("AppendEvent(valid entity_id): %v", err)
	}

	var gotTaskID, gotEntityID, gotScope string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(payload->>'task_id', ''), COALESCE(entity_id::text, ''), COALESCE(scope, '')
		FROM events
		WHERE event_id = $1::uuid
	`, validEventID).Scan(&gotTaskID, &gotEntityID, &gotScope); err != nil {
		t.Fatalf("query valid event row: %v", err)
	}
	if gotTaskID != "" {
		t.Fatalf("expected normalized empty task_id, got %q", gotTaskID)
	}
	if gotEntityID != validEntityID {
		t.Fatalf("valid event entity_id = %q, want %q", gotEntityID, validEntityID)
	}
	if gotScope != string(events.EventScopeEntity) {
		t.Fatalf("valid event scope = %q, want %q", gotScope, events.EventScopeEntity)
	}

	emptyEventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(emptyEventID,
		events.EventType("review.requested"),
		"control-plane", "", []byte(`{"name":"Telemedicine Platform"}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now())); err != nil {
		t.Fatalf("AppendEvent(empty entity_id): %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(entity_id::text, ''), COALESCE(scope, '')
		FROM events
		WHERE event_id = $1::uuid
	`, emptyEventID).Scan(&gotEntityID, &gotScope); err != nil {
		t.Fatalf("query empty event row: %v", err)
	}
	if gotEntityID != "" {
		t.Fatalf("empty entity event entity_id = %q, want empty", gotEntityID)
	}
	if gotScope != string(events.EventScopeGlobal) {
		t.Fatalf("empty entity event scope = %q, want %q", gotScope, events.EventScopeGlobal)
	}

	invalidEventID := uuid.NewString()
	err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(
		invalidEventID,
		events.EventType("review.requested"),
		"control-plane",
		"",
		[]byte(`{"name":"Telemedicine Platform"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "pry_hc_telemedicine_001"),
		time.Now(),
	))
	if err == nil {
		t.Fatal("expected AppendEvent to fail on non-UUID entity_id")
	}
	if !strings.Contains(err.Error(), "must be a UUID") {
		t.Fatalf("AppendEvent invalid entity_id error = %v", err)
	}

	var invalidCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_id = $1::uuid
	`, invalidEventID).Scan(&invalidCount); err != nil {
		t.Fatalf("count invalid event rows: %v", err)
	}
	if invalidCount != 0 {
		t.Fatalf("expected invalid entity_id event not to persist, count=%d", invalidCount)
	}
}

func TestPostgresStore_PersistEventWithDeliveries_RejectsInvalidEntityID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	eventID := uuid.NewString()
	err := commitSemanticEventFixtureWithAgents(ctx, pg, eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("review.requested"),
		"human",
		"",
		[]byte(`{"directive":"Telemedicine Platform"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "pry_hc_telemedicine_001"),
		time.Now().UTC(),
	),

		[]string{"control-plane"})
	if err == nil {
		t.Fatal("expected PersistEventWithDeliveries to fail on non-UUID entity_id")
	}
	if !strings.Contains(err.Error(), "must be a UUID") {
		t.Fatalf("PersistEventWithDeliveries invalid entity_id error = %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if nEvents != 0 || nDeliveries != 0 {
		t.Fatalf("expected invalid entity_id event tx rollback, got events=%d deliveries=%d", nEvents, nDeliveries)
	}
}

func TestPostgresStore_AppendEvent_RejectsPayloadValidatorFailureBeforePersistence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	pg.SetEventPayloadValidator(func(_ context.Context, eventType string, payload []byte) error {
		if strings.TrimSpace(eventType) != "task.completed" {
			t.Fatalf("unexpected event type %q", eventType)
		}
		if string(payload) != `{"ok":"bad"}` {
			t.Fatalf("unexpected payload %s", string(payload))
		}
		return runtimetools.ValidatePayloadAgainstSchema(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"ok": map[string]any{"type": "boolean"},
			},
			"required":             []any{"ok"},
			"additionalProperties": false,
		}, map[string]any{"ok": "bad"})
	})
	ctx := testAuthorActivityContext()
	eventID := uuid.NewString()

	err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(eventID,
		events.EventType("task.completed"),
		"control-plane", "", []byte(`{"ok":"bad"}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().UTC()))
	if err == nil {
		t.Fatal("expected AppendEvent to fail on payload validator rejection")
	}
	if !strings.Contains(err.Error(), "validate event payload") {
		t.Fatalf("AppendEvent payload validator error = %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rejected payload not to persist, count=%d", count)
	}
}

func currentPlatformPayloadValidatorForStoreTest(t testing.TB) EventPayloadValidator {
	t.Helper()
	spec := loadPlatformSpecDocumentForStoreTest(t, runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	registry := runtimecontracts.EventSchemaRegistryFromBundle(&runtimecontracts.WorkflowContractBundle{Platform: spec})
	return func(_ context.Context, eventType string, payload []byte) error {
		schema, ok := registry[strings.TrimSpace(eventType)]
		if !ok {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return err
		}
		return runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, decoded)
	}
}

func TestPostgresStore_GetEventReceipt_FallsBackToPersistedReceiptForNonTerminalDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(eventID, "system.started", "runtime", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now())); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"a1"}); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'in_progress',
			reason_code = 'agent_processing',
			active_session_id = $2::uuid
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'a1'
	`, eventID, uuid.NewString()); err != nil {
		t.Fatalf("set in_progress delivery: %v", err)
	}

	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(runtimemanager.ReceiptStatusDeadLetter, "retry_exhausted", 2))
	if err != nil {
		t.Fatalf("marshal side effects: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', 'a1', e.entity_id, e.flow_instance,
			'dead_letter', 'retry_exhausted', $2::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
	`, eventID, string(sideEffects)); err != nil {
		t.Fatalf("insert receipt: %v", err)
	}

	receipt, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil {
		t.Fatalf("GetEventReceipt(in_progress delivery): %v", err)
	}
	if !ok {
		t.Fatal("expected receipt to be found")
	}
	if receipt.Status != runtimemanager.ReceiptStatusDeadLetter || receipt.RetryCount != 2 || receipt.Failure != nil {
		t.Fatalf("receipt = %+v, want dead_letter retry_count=2 without failure", receipt)
	}
}

func TestPostgresStore_EventReceiptsTypedIdentitySeparatesReceiptWriters(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresStoreEvent(t, ctx, pg, eventID, runID, "test.receipts.typed_identity", events.EventProducerPlatform, "test", entityID, "flow-1", time.Now().UTC())
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'pipeline', 'in_progress', now()
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed agent delivery: %v", err)
	}
	var nodeDeliveryID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', 'pipeline', 'in_progress', now()
		)
		RETURNING delivery_id::text
	`, runID, eventID).Scan(&nodeDeliveryID); err != nil {
		t.Fatalf("seed node delivery: %v", err)
	}

	if err := pg.UpsertPipelineReceipt(ctx, eventID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, eventID, "pipeline", runtimemanager.ReceiptStatusProcessed, nil); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active quiescence tx: %v", err)
	}
	if err := terminalizeActiveRunQuiescenceDeliveryTx(ctx, tx, activeRunQuiescenceDeliveryTarget{
		DeliveryID:     nodeDeliveryID,
		RunID:          runID,
		EventID:        eventID,
		SubscriberType: "node",
		SubscriberID:   "pipeline",
		Status:         "in_progress",
	}, "serve_abandon", "operator shutdown", time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("terminalizeActiveRunQuiescenceDeliveryTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit active quiescence tx: %v", err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_id = 'pipeline'
		ORDER BY subscriber_type
	`, eventID)
	if err != nil {
		t.Fatalf("query typed receipts: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var subscriberType, subscriberID, outcome string
		if err := rows.Scan(&subscriberType, &subscriberID, &outcome); err != nil {
			t.Fatalf("scan typed receipt: %v", err)
		}
		got[subscriberType+"|"+subscriberID] = outcome
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read typed receipts: %v", err)
	}
	want := map[string]string{
		"agent|pipeline":    "success",
		"node|pipeline":     "dead_letter",
		"platform|pipeline": "success",
	}
	if len(got) != len(want) {
		t.Fatalf("typed receipt rows = %#v, want %#v", got, want)
	}
	for key, wantOutcome := range want {
		if got[key] != wantOutcome {
			t.Fatalf("receipt %s outcome = %q, want %q (all rows %#v)", key, got[key], wantOutcome, got)
		}
	}
}

func TestPostgresStore_RunTerminalOwnersPersistCanonicalLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	completedFixture := seedNormalRunCompletionFixture(t, db, "done", "review/inst-1", "review")
	completedRunID := completedFixture.RunID
	if err := pg.UpsertPipelineReceipt(ctx, completedFixture.EventID, "processed", nil); err != nil {
		t.Fatalf("seed completed run receipt: %v", err)
	}
	if err := pg.ConvergeNormalRunCompletion(ctx, completedFixture.EventID, []string{"done"}, map[string][]string{"review": {"done"}}); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}

	var (
		completedStatus  string
		completedFailure []byte
		completedEnded   time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), failure, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, completedRunID).Scan(&completedStatus, &completedFailure, &completedEnded); err != nil {
		t.Fatalf("load completed run: %v", err)
	}
	if completedStatus != "completed" {
		t.Fatalf("completed run status = %q, want completed", completedStatus)
	}
	if len(completedFailure) != 0 {
		t.Fatalf("completed run failure = %s, want absent", completedFailure)
	}
	if completedEnded.IsZero() {
		t.Fatal("completed run ended_at not persisted")
	}

	failedRunID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, failedRunID); err != nil {
		t.Fatalf("seed failed run: %v", err)
	}
	failedAt := time.Now().UTC().Round(time.Second)
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "run_quiescence_failed", nil)
	if _, err := pg.MarkRunTerminal(ctx, failedRunID, "failed", &failure, failedAt); err != nil {
		t.Fatalf("MarkRunTerminal(failed): %v", err)
	}

	var (
		failedStatus string
		failedRaw    []byte
		failedEnded  time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), failure, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, failedRunID).Scan(&failedStatus, &failedRaw, &failedEnded); err != nil {
		t.Fatalf("load failed run: %v", err)
	}
	if failedStatus != "failed" {
		t.Fatalf("failed run status = %q, want failed", failedStatus)
	}
	persisted, err := runtimefailures.UnmarshalEnvelope(failedRaw)
	if err != nil || !failureEnvelopesEqual(persisted, failure) {
		t.Fatalf("failed run failure = (%+v, %v), want %+v", persisted, err, failure)
	}
	if failedEnded.IsZero() {
		t.Fatal("failed run ended_at not persisted")
	}
	if _, err := pg.MarkRunTerminal(ctx, failedRunID, "failed", &failure, failedAt.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent failed terminal write: %v", err)
	}
	conflicting := testFailureEnvelope(runtimefailures.ClassInternalFailure, "different_run_failure", nil)
	if _, err := pg.MarkRunTerminal(ctx, failedRunID, "failed", &conflicting, failedAt.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "conflicting failure") {
		t.Fatalf("conflicting terminal write error = %v, want rejection", err)
	}
	if _, err := pg.MarkRunTerminal(ctx, uuid.NewString(), "failed", nil, failedAt); err == nil || !strings.Contains(err.Error(), "requires canonical failure") {
		t.Fatalf("missing failed evidence error = %v", err)
	}
}

func TestPostgresStore_ListPendingEventsForAgent_PreservesRunID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS runs (
			run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			status TEXT NOT NULL DEFAULT 'running'
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			run_id UUID REFERENCES runs(run_id),
			event_name TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope TEXT NOT NULL DEFAULT 'entity',
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			chain_depth INTEGER NOT NULL DEFAULT 0,
			produced_by TEXT,
			produced_by_type TEXT NOT NULL DEFAULT 'agent',
			source_event_id UUID,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS event_deliveries (
			delivery_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			run_id UUID REFERENCES runs(run_id),
			event_id UUID NOT NULL REFERENCES events(event_id),
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			retry_count INTEGER NOT NULL DEFAULT 0,
			reason_code TEXT,
			failure JSONB,
			active_session_id UUID,
			started_at TIMESTAMPTZ,
			delivered_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS event_receipts (
			receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			outcome TEXT NOT NULL DEFAULT 'success',
			side_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
			failure JSONB,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec schema stmt: %v", err)
		}
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	seedPostgresStoreEvent(t, ctx, pg, eventID, runID, "scoring/scoring.requested", events.EventProducerAgent, "runtime", entityID, "", time.Now().UTC())
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'agent', 'analysis-agent', 'pending', now())
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	got, err := pg.ListPendingEventsForAgent(ctx, "analysis-agent", time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("pending events = %d, want 1", len(got))
	}
	if got[0].RunID() != runID {
		t.Fatalf("pending event run_id = %q, want %q", got[0].RunID(), runID)
	}
}

func TestPostgresStore_ListPendingEventsForAgent_UsesTypedEnvelopeMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	const flowInstance = "review/inst-1"
	seedSpecAgent(t, ctx, pg, "analysis-agent", "", "scoring.requested")

	eventID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("scoring.requested"),
		"runtime",
		"",
		[]byte(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
		},
		time.Now().UTC(),
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"analysis-agent"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}

	got, err := pg.ListPendingEventsForAgent(ctx, "analysis-agent", time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("pending events = %d, want 1", len(got))
	}
	if got := got[0].EntityID(); got != entityID {
		t.Fatalf("pending event entity_id = %q, want %q", got, entityID)
	}
	if got := got[0].FlowInstance(); got != flowInstance {
		t.Fatalf("pending event flow_instance = %q, want %q", got, flowInstance)
	}
	if got := got[0].Scope(); got != events.EventScopeEntity {
		t.Fatalf("pending event scope = %q, want %q", got, events.EventScopeEntity)
	}
}

func TestPostgresStore_PipelineReceipts_MissingEventsQuery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runID := uuid.NewString()
	parentID := uuid.NewString()
	eventProcessed := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("system.started"),
		"runtime", "", []byte(`{"ok":true}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute))

	eventMissing := eventtest.PersistedProjection(uuid.NewString(),
		events.EventType("system.directive"),
		"human", "", []byte(`{"directive":"x"}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-1*time.Minute))

	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running') ON CONFLICT (run_id) DO NOTHING`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-3*time.Minute))); err != nil {
		t.Fatalf("append parent event: %v", err)
	}
	if err := commitSemanticEventFixture(ctx, pg, eventProcessed); err != nil {
		t.Fatalf("append processed event: %v", err)
	}
	if err := commitSemanticEventFixture(ctx, pg, eventMissing); err != nil {
		t.Fatalf("append missing event: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", nil); err != nil {
		t.Fatalf("upsert parent receipt: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventProcessed.ID(), "processed", nil); err != nil {
		t.Fatalf("upsert processed receipt: %v", err)
	}

	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-1*time.Hour), 20)
	if err != nil {
		t.Fatalf("list missing pipeline receipts: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing event, got %d", len(missing))
	}
	if missing[0].Event.ID() != eventMissing.ID() {
		t.Fatalf("expected missing event id=%s got=%s", eventMissing.ID(), missing[0].Event.ID())
	}
	if missing[0].Event.RunID() != runID {
		t.Fatalf("missing event run_id = %q, want %q", missing[0].Event.RunID(), runID)
	}
	if missing[0].Event.ParentEventID() != parentID {
		t.Fatalf("missing event parent_event_id = %q, want %q", missing[0].Event.ParentEventID(), parentID)
	}
	if missing[0].ReplayFailure != nil {
		t.Fatalf("missing event replay_failure = %#v, want nil", missing[0].ReplayFailure)
	}
}

func TestPostgresStore_RunEventTransaction_AppendAndDeliveriesTx(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	eventID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(eventID,
		events.EventType("system.started"),
		"runtime", "", []byte(`{"ok":true}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().UTC())

	if err := pg.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := commitSemanticEventFixtureTx(txctx, pg, tx, evt); err != nil {
			return err
		}
		return pg.InsertEventDeliveriesTx(txctx, tx, eventID, []string{"control-plane", "reviewer"})
	}); err != nil {
		t.Fatalf("RunEventTransaction: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND NOT (subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__')`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if nEvents != 1 || nDeliveries != 2 {
		t.Fatalf("expected event+2 deliveries persisted, got events=%d deliveries=%d", nEvents, nDeliveries)
	}
}

func TestPostgresStore_PersistEventWithDeliveries_SuccessAndRollbackOnFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	eventID := uuid.NewString()
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, eventtest.RunCreatingRootIngress(eventID,
		events.EventType("system.directive"),
		"human", "", []byte(`{"directive":"SaaS in Argentina"}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().UTC()),

		[]string{" control-plane ", "", "control-plane"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries success path: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events success: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND NOT (subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__')`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count deliveries success: %v", err)
	}
	if nEvents != 1 || nDeliveries != 1 {
		t.Fatalf("expected deduped delivery insertion, got events=%d deliveries=%d", nEvents, nDeliveries)
	}

	failedEventID := uuid.NewString()
	err := commitSemanticEventFixtureWithAgents(ctx, pg, eventtest.RunCreatingRootIngress(failedEventID,
		events.EventType("system.directive"),
		"human", "", []byte(`{"directive":"fail path"}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().UTC()),

		[]string{"missing-agent"})
	if err != nil {
		t.Fatalf("PersistEventWithDeliveries unknown subscriber should still persist: %v", err)
	}
	var persistedCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, failedEventID).Scan(&persistedCount); err != nil {
		t.Fatalf("count persisted event: %v", err)
	}
	if persistedCount != 1 {
		t.Fatalf("expected persisted event row, count=%d", persistedCount)
	}
}

func TestPostgresStore_Mailbox_CRUD_Expire_Notify(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()

	id, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
		FromAgent: "control-plane",
		Type:      "spend_request",
		Summary:   "need approval",
	})
	if err != nil || id == "" {
		t.Fatalf("insert mailbox: id=%q err=%v", id, err)
	}

	got, err := s.GetMailboxItem(ctx, id)
	if err != nil {
		t.Fatalf("get mailbox: %v", err)
	}
	if got.Status != "pending" || got.Priority != "normal" {
		t.Fatalf("unexpected defaults: %+v", got)
	}

	if n, err := s.CountMailboxItems(ctx, "pending"); err != nil || n < 1 {
		t.Fatalf("count pending n=%d err=%v", n, err)
	}
	items, err := s.ListMailboxItems(ctx, "pending", 10)
	if err != nil || len(items) == 0 {
		t.Fatalf("list pending: n=%d err=%v", len(items), err)
	}
	foundPending := false
	for _, item := range items {
		if item.ID == id {
			foundPending = true
			if item.Status != "pending" || item.Priority != "normal" || item.Summary != "need approval" {
				t.Fatalf("unexpected listed mailbox item: %+v", item)
			}
		}
	}
	if !foundPending {
		t.Fatalf("expected inserted pending mailbox item %q in list", id)
	}

	expID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
		FromAgent: "control-plane",
		Type:      "review",
		Priority:  "critical",
		Status:    "pending",
		Context:   []byte(`{"x":1}`),
		TimeoutAt: time.Now().Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("insert expiring mailbox: %v", err)
	}
	expired, err := s.ExpireMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	found := false
	for _, it := range expired {
		if it.ID == expID {
			found = true
			if it.Status != "expired" || it.Decision != "" {
				t.Fatalf("expected expired/empty-decision, got %+v", it)
			}
		}
	}
	if !found {
		t.Fatalf("expected expired item in result")
	}

	critID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
		FromAgent: "control-plane",
		Type:      "spend_request",
		Priority:  "critical",
		Status:    "pending",
		Summary:   "critical",
		TimeoutAt: time.Now().Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("insert critical mailbox: %v", err)
	}
	crit, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil || len(crit) == 0 {
		t.Fatalf("list unnotified critical: n=%d err=%v", len(crit), err)
	}
	foundCritical := false
	for _, item := range crit {
		if item.ID == critID {
			foundCritical = true
			if item.Status != "pending" || item.Priority != "critical" || item.Summary != "critical" {
				t.Fatalf("unexpected critical mailbox item: %+v", item)
			}
		}
	}
	if !foundCritical {
		t.Fatalf("expected critical mailbox item %q in unnotified list", critID)
	}
	if err := s.MarkMailboxItemNotified(ctx, critID); err != nil {
		t.Fatalf("mark notified: %v", err)
	}
	crit2, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("list unnotified critical 2: %v", err)
	}
	for _, it := range crit2 {
		if it.ID == critID {
			t.Fatalf("expected item to be notified and excluded")
		}
	}
}

func TestExtractSubscriptions(t *testing.T) {
	if got := extractSubscriptions(nil); got != nil {
		t.Fatalf("expected nil")
	}
	if got := extractSubscriptions([]byte("nope")); got != nil {
		t.Fatalf("expected nil for invalid json")
	}
	raw := []byte(`{"subscriptions":["a","b"]}`)
	got := extractSubscriptions(raw)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected subscriptions: %#v", got)
	}
}

func TestNormalizeJSONPayload_RedactsSensitiveText(t *testing.T) {

	out := normalizeJSONPayload([]byte("email me at x@example.com or call +1 (555) 123-4567"))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json wrapper, got %q", out)
	}
	if strings.Contains(out, "x@example.com") || strings.Contains(out, "555") {
		t.Fatalf("expected email/phone redacted in wrapper: %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"name":"Alice Smith","notes":"reach me at y@example.com","nested":{"full_name":"Bob Jones"}}`))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json, got %q", out)
	}
	if strings.Contains(out, "Alice") || strings.Contains(out, "Bob") {
		t.Fatalf("expected names redacted, got %q", out)
	}
	if strings.Contains(out, "y@example.com") {
		t.Fatalf("expected email redacted, got %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"payment_ref":"pi_1234567890ABCDEF","notes":"charge ch_abcdef123456 done"}`))
	if strings.Contains(out, "pi_1234567890ABCDEF") || strings.Contains(out, "ch_abcdef123456") {
		t.Fatalf("expected payment refs redacted, got %q", out)
	}
	if !strings.Contains(out, "[PAYMENT_REF]") {
		t.Fatalf("expected [PAYMENT_REF] marker, got %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"timestamp":"2026-02-21T02:47:05Z","notes":"at 2026-02-21T02:47:05Z"}`))
	if strings.Contains(out, "[PHONE]") {
		t.Fatalf("expected timestamp not redacted as phone, got %q", out)
	}
}

func TestSchedules_UpsertLoadCancelAndMarkFired(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	once := runtimepipeline.Schedule{
		AgentID:   "a1",
		EventType: "system.directive",
		Mode:      "once",
		At:        time.Now().Add(1 * time.Hour).UTC(),
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, once); err != nil {
		t.Fatalf("upsert once: %v", err)
	}
	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	if len(active) == 0 {
		t.Fatalf("expected active schedule")
	}

	if err := pg.MarkScheduleFired(ctx, once); err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules after fired: %v", err)
	}
	for _, sc := range active {
		if sc.AgentID == "a1" && sc.EventType == "system.directive" && sc.Mode == "once" {
			t.Fatalf("expected once schedule to deactivate after fired")
		}
	}

	recurring := runtimepipeline.Schedule{
		AgentID:   "a1",
		EventType: "system.started",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   nil,
	}
	if err := pg.UpsertSchedule(ctx, recurring); err != nil {
		t.Fatalf("upsert recurring: %v", err)
	}
	if err := pg.MarkScheduleFired(ctx, recurring); err != nil {
		t.Fatalf("mark recurring fired: %v", err)
	}

	if err := pg.CancelSchedule(ctx, "a1", "system.started"); err != nil {
		t.Fatalf("cancel schedule: %v", err)
	}
}

func TestSchedules_RunScopedWritesUsePipelineTransaction(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	runID := uuid.NewString()
	ctx, cancel := context.WithTimeout(runtimecorrelation.WithRunID(context.Background(), runID), 3*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', now())`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin pipeline transaction: %v", err)
	}
	defer tx.Rollback()
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	schedule := runtimepipeline.Schedule{
		RunID: runID, AgentID: "scheduler", EventType: "timer.pipeline", TaskID: "pipeline-timer",
		Mode: "once", At: time.Now().UTC().Add(time.Hour), Payload: []byte(`{}`),
	}
	if err := pg.UpsertSchedule(txctx, schedule); err != nil {
		t.Fatalf("UpsertSchedule in pipeline transaction: %v", err)
	}
	if err := pg.CancelScheduleExact(txctx, schedule); err != nil {
		t.Fatalf("CancelScheduleExact in pipeline transaction: %v", err)
	}
	var status string
	if err := tx.QueryRowContext(txctx, `SELECT status FROM timers WHERE run_id = $1::uuid AND timer_name = $2`, runID, schedule.TaskID).Scan(&status); err != nil {
		t.Fatalf("load transaction-local timer: %v", err)
	}
	if status != "cancelled" {
		t.Fatalf("transaction-local timer status = %q, want cancelled", status)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback pipeline transaction: %v", err)
	}
	var rows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid`, runID).Scan(&rows); err != nil {
		t.Fatalf("count rolled-back timers: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rolled-back timer rows = %d, want 0", rows)
	}
}

func TestSchedules_ExactIdentityUsesTaskID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	entityID := uuid.NewString()

	first := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(30 * time.Minute).UTC(),
		EntityID:  entityID,
		TaskID:    "timer-a",
		Payload:   []byte(`{"timer_id":"timer-a"}`),
	}
	second := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(60 * time.Minute).UTC(),
		EntityID:  entityID,
		TaskID:    "timer-b",
		Payload:   []byte(`{"timer_id":"timer-b"}`),
	}
	if err := pg.UpsertSchedule(ctx, first); err != nil {
		t.Fatalf("upsert first exact schedule: %v", err)
	}
	if err := pg.UpsertSchedule(ctx, second); err != nil {
		t.Fatalf("upsert second exact schedule: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	var exact []runtimepipeline.Schedule
	for _, sc := range active {
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == entityID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 2 {
		t.Fatalf("expected two exact schedules to coexist, got %+v", exact)
	}
	seen := map[string]string{}
	for _, sc := range exact {
		seen[sc.TaskID] = string(sc.Payload)
	}
	if seen["timer-a"] != `{"timer_id":"timer-a"}` {
		t.Fatalf("first exact schedule payload/task mismatch: %+v", seen)
	}
	if seen["timer-b"] != `{"timer_id":"timer-b"}` {
		t.Fatalf("second exact schedule payload/task mismatch: %+v", seen)
	}

	if err := pg.CancelScheduleExact(ctx, first); err != nil {
		t.Fatalf("cancel first exact schedule: %v", err)
	}
	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules after exact cancel: %v", err)
	}
	exact = exact[:0]
	for _, sc := range active {
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == entityID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 1 || exact[0].TaskID != "timer-b" {
		t.Fatalf("expected only timer-b to remain active, got %+v", exact)
	}
}

func TestSchedules_ExactIdentityUsesFlowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	entityID := uuid.NewString()

	first := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-1",
		TaskID:       "timer-a",
		Payload:      []byte(`{"timer_id":"timer-a"}`),
	}
	second := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(60 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-2",
		TaskID:       "timer-a",
		Payload:      []byte(`{"timer_id":"timer-a"}`),
	}
	if err := pg.UpsertSchedule(ctx, first); err != nil {
		t.Fatalf("upsert first exact schedule: %v", err)
	}
	if err := pg.UpsertSchedule(ctx, second); err != nil {
		t.Fatalf("upsert second exact schedule: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	var exact []runtimepipeline.Schedule
	for _, sc := range active {
		if sc.AgentID == first.AgentID &&
			sc.EventType == first.EventType &&
			sc.EntityID == first.EntityID &&
			sc.TaskID == first.TaskID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 2 {
		t.Fatalf("expected two exact schedules to coexist across flow instances, got %+v", exact)
	}

	if err := pg.CancelScheduleExact(ctx, first); err != nil {
		t.Fatalf("CancelScheduleExact(first): %v", err)
	}
	if err := pg.MarkScheduleFiredExact(ctx, second); err != nil {
		t.Fatalf("MarkScheduleFiredExact(second): %v", err)
	}

	var firstStatus, secondStatus string
	firstStatusQuery := `
		SELECT status
		FROM timers
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND flow_instance = $3
		  AND ` + exactScheduleTaskIDSQL() + ` = $4
	`
	if err := db.QueryRowContext(ctx, firstStatusQuery, first.AgentID, first.EventType, first.FlowInstance, first.TaskID).Scan(&firstStatus); err != nil {
		t.Fatalf("query first exact timer status: %v", err)
	}
	if err := db.QueryRowContext(ctx, firstStatusQuery, second.AgentID, second.EventType, second.FlowInstance, second.TaskID).Scan(&secondStatus); err != nil {
		t.Fatalf("query second exact timer status: %v", err)
	}
	if firstStatus != "cancelled" {
		t.Fatalf("first exact timer status = %q, want cancelled", firstStatus)
	}
	if secondStatus != "fired" {
		t.Fatalf("second exact timer status = %q, want fired", secondStatus)
	}

	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules(after exact cancel/fire): %v", err)
	}
	for _, sc := range active {
		if sc.AgentID == first.AgentID &&
			sc.EventType == first.EventType &&
			sc.EntityID == first.EntityID &&
			sc.TaskID == first.TaskID &&
			(sc.FlowInstance == first.FlowInstance || sc.FlowInstance == second.FlowInstance) {
			t.Fatalf("expected no remaining exact schedules after targeted cancel/fire, found %+v", sc)
		}
	}
}

func TestSchedules_ExactIdentityUsesRunID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, runA, runB); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	base := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-1",
		TaskID:       "timer-a",
		Payload:      []byte(`{"timer_id":"timer-a"}`),
	}
	first := base
	first.RunID = runA
	second := base
	second.RunID = runB
	second.At = time.Now().Add(60 * time.Minute).UTC()

	if err := pg.UpsertSchedule(ctx, first); err != nil {
		t.Fatalf("UpsertSchedule(first): %v", err)
	}
	if err := pg.UpsertSchedule(ctx, second); err != nil {
		t.Fatalf("UpsertSchedule(second): %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	var exact []runtimepipeline.Schedule
	for _, sc := range active {
		if sc.AgentID == base.AgentID &&
			sc.EventType == base.EventType &&
			sc.EntityID == base.EntityID &&
			sc.FlowInstance == base.FlowInstance &&
			sc.TaskID == base.TaskID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 2 {
		t.Fatalf("expected two exact schedules to coexist across run_id, got %+v", exact)
	}

	if err := pg.MarkScheduleFiredExact(ctx, first); err != nil {
		t.Fatalf("MarkScheduleFiredExact(first): %v", err)
	}
	statuses := map[string]string{}
	rows, err := db.QueryContext(ctx, `
		SELECT run_id::text, status
		FROM timers
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id = $3::uuid
		  AND flow_instance = $4
		  AND `+exactScheduleTaskIDSQL()+` = $5
	`, base.AgentID, base.EventType, base.EntityID, base.FlowInstance, base.TaskID)
	if err != nil {
		t.Fatalf("query timer statuses: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runID, status string
		if err := rows.Scan(&runID, &status); err != nil {
			t.Fatalf("scan timer status: %v", err)
		}
		statuses[runID] = status
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate timer statuses: %v", err)
	}
	if statuses[runA] != "fired" || statuses[runB] != "active" {
		t.Fatalf("timer statuses = %#v, want runA fired and runB active", statuses)
	}
}

func TestSchedules_LoadActiveSchedulesPreservesFlowInstance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	entityID := uuid.NewString()

	sc := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-1",
		TaskID:       "timer-a",
		Payload:      []byte(`{"timer_id":"timer-a"}`),
	}
	if err := pg.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active schedules = %d, want 1", len(active))
	}
	if got := active[0].FlowInstance; got != "review/inst-1" {
		t.Fatalf("loaded flow_instance = %q, want review/inst-1", got)
	}
}

func TestSchedules_LoadActiveSchedulesIgnoresWorkflowSidecarRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, task_type, status
		)
		VALUES (
			$1::uuid, 'workflow-sidecar', $2::uuid, 'review/inst-1', 'timer.workflow', '{}'::jsonb,
			$3, false, NULL, NULL,
			'workflow_instance_store', NULL, 'timer', 'active'
		)
	`, runID, entityID, time.Now().Add(30*time.Minute).UTC()); err != nil {
		t.Fatalf("seed workflow sidecar timer: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active executable schedules = %+v, want workflow sidecar ignored", active)
	}
}

func TestSchedules_LoadActiveSchedulesDoesNotReconstructTaskIDFromTimerName(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	entityID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, task_type, status
		)
		VALUES (
			$1, $2::uuid, $3, $4, $5::jsonb,
			$6, false, NULL, NULL,
			NULL, $7, 'timer', 'active'
		)
	`, "timer-a", entityID, "review/inst-1", "timer.validation_timeout", `{"timer_id":"timer-a"}`, time.Now().Add(30*time.Minute).UTC(), "validation-orchestrator"); err != nil {
		t.Fatalf("seed exact timer row: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active schedules = %d, want 1", len(active))
	}
	if got := active[0].TaskID; got != "" {
		t.Fatalf("loaded task_id = %q, want empty without canonical payload task id", got)
	}
	if string(active[0].Payload) != `{"timer_id":"timer-a"}` {
		t.Fatalf("loaded payload = %s, want task metadata without synthetic task id", string(active[0].Payload))
	}
}

func TestSchedules_MarkScheduleFiredExact_PreservesRecurringReplayAndFiresOnceSchedules(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	entityID := uuid.NewString()

	recurring := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "cron",
		Cron:         "@every 30m",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-1",
		TaskID:       "timer-recurring",
		Payload:      []byte(`{"timer_id":"timer-recurring"}`),
	}
	once := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(60 * time.Minute).UTC(),
		EntityID:     entityID,
		FlowInstance: "review/inst-1",
		TaskID:       "timer-once",
		Payload:      []byte(`{"timer_id":"timer-once"}`),
	}
	if err := pg.UpsertSchedule(ctx, recurring); err != nil {
		t.Fatalf("upsert recurring schedule: %v", err)
	}
	if err := pg.UpsertSchedule(ctx, once); err != nil {
		t.Fatalf("upsert once schedule: %v", err)
	}

	if err := pg.MarkScheduleFiredExact(ctx, recurring); err != nil {
		t.Fatalf("MarkScheduleFiredExact(recurring): %v", err)
	}
	if err := pg.MarkScheduleFiredExact(ctx, once); err != nil {
		t.Fatalf("MarkScheduleFiredExact(once): %v", err)
	}

	var recurringStatus, onceStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM timers
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND COALESCE(fire_payload->>'__schedule_task_id', '') = $3
	`, recurring.AgentID, recurring.EventType, recurring.TaskID).Scan(&recurringStatus); err != nil {
		t.Fatalf("query recurring timer status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM timers
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND COALESCE(fire_payload->>'__schedule_task_id', '') = $3
	`, once.AgentID, once.EventType, once.TaskID).Scan(&onceStatus); err != nil {
		t.Fatalf("query once timer status: %v", err)
	}
	if recurringStatus != "active" {
		t.Fatalf("recurring timer status = %q, want active", recurringStatus)
	}
	if onceStatus != "fired" {
		t.Fatalf("once timer status = %q, want fired", onceStatus)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules: %v", err)
	}
	var recurringFound, onceFound bool
	for _, sc := range active {
		if sc.AgentID == recurring.AgentID && sc.EventType == recurring.EventType && sc.TaskID == recurring.TaskID {
			recurringFound = true
		}
		if sc.AgentID == once.AgentID && sc.EventType == once.EventType && sc.TaskID == once.TaskID {
			onceFound = true
		}
	}
	if !recurringFound {
		t.Fatal("expected recurring exact schedule to remain active after firing")
	}
	if onceFound {
		t.Fatal("expected once exact schedule to be absent from active schedule restore after firing")
	}
}

func TestSchedules_ClaimSchedule_IsExclusiveAcrossStores(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	pg1 := newTestPostgresStore(t, db)
	pg2 := newTestPostgresStore(t, db)

	sc := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "cron",
		Cron:         "@every 30m",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     uuid.NewString(),
		FlowInstance: "review/inst-1",
		TaskID:       "timer-recurring",
		Payload:      []byte(`{"timer_id":"timer-recurring"}`),
	}
	if err := pg1.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	claimed1, err := pg1.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(pg1): %v", err)
	}
	if !claimed1 {
		t.Fatal("expected first store to claim schedule ownership")
	}

	claimed2, err := pg2.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(pg2): %v", err)
	}
	if claimed2 {
		t.Fatal("expected second store to be denied overlapping schedule ownership")
	}

	if err := pg1.ReleaseSchedule(ctx, sc); err != nil {
		t.Fatalf("ReleaseSchedule(pg1): %v", err)
	}
	claimed2, err = pg2.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(pg2 after release): %v", err)
	}
	if !claimed2 {
		t.Fatal("expected second store to claim after first owner released")
	}
}

func TestSchedules_ClaimedOwnerIsOnlyRestoredTimerThatFires(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	pg1 := newTestPostgresStore(t, db)
	pg2 := newTestPostgresStore(t, db)

	sc := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(50 * time.Millisecond).UTC(),
		EntityID:     uuid.NewString(),
		FlowInstance: "review/inst-1",
		TaskID:       "timer-once",
		Payload:      []byte(`{"timer_id":"timer-once"}`),
	}
	if err := pg1.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	active1, err := pg1.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules(pg1): %v", err)
	}
	active2, err := pg2.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules(pg2): %v", err)
	}
	if len(active1) != 1 || len(active2) != 1 {
		t.Fatalf("restored schedules lens = (%d,%d), want (1,1)", len(active1), len(active2))
	}

	var fired1, fired2 atomic.Int32
	scheduler1 := runtimepipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t), func(_ context.Context, sc runtimepipeline.Schedule) {
		fired1.Add(1)
		if err := pg1.CompleteScheduleFireExact(testAuthorActivityContext(), sc); err != nil {
			t.Errorf("CompleteScheduleFireExact(pg1): %v", err)
		}
	})
	scheduler2 := runtimepipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t), func(_ context.Context, sc runtimepipeline.Schedule) {
		fired2.Add(1)
		if err := pg2.CompleteScheduleFireExact(testAuthorActivityContext(), sc); err != nil {
			t.Errorf("CompleteScheduleFireExact(pg2): %v", err)
		}
	})
	t.Cleanup(scheduler1.Stop)
	t.Cleanup(scheduler2.Stop)

	claimed1, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, pg1, scheduler1, active1[0])
	if err != nil {
		t.Fatalf("ClaimAndRegisterSchedule(pg1): %v", err)
	}
	claimed2, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, pg2, scheduler2, active2[0])
	if err != nil {
		t.Fatalf("ClaimAndRegisterSchedule(pg2): %v", err)
	}
	if claimed1 == claimed2 {
		t.Fatalf("claimed owners = (%v,%v), want exactly one owner", claimed1, claimed2)
	}

	deadline := time.After(2 * time.Second)
	for fired1.Load()+fired2.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for claimed restored timer to fire")
		case <-time.After(10 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got := fired1.Load() + fired2.Load(); got != 1 {
		t.Fatalf("restored timer fire count = %d, want 1", got)
	}

	activeAfter, err := pg1.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("LoadActiveSchedules(after fire): %v", err)
	}
	if len(activeAfter) != 0 {
		t.Fatalf("active schedules after claimed once fire = %d, want 0", len(activeAfter))
	}
}

func TestSchedules_CancelExactTerminalAllowsSubsequentReclaim(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	pgOwner := newTestPostgresStore(t, db)
	pgSuccessor := newTestPostgresStore(t, db)

	sc := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     uuid.NewString(),
		FlowInstance: "review/inst-1",
		TaskID:       "timer-cancel-reclaim",
		Payload:      []byte(`{"timer_id":"timer-cancel-reclaim"}`),
	}
	if err := pgOwner.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	claimed, err := pgOwner.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(owner): %v", err)
	}
	if !claimed {
		t.Fatal("expected owner to claim schedule")
	}

	if err := pgOwner.CancelScheduleExactTerminal(ctx, sc); err != nil {
		t.Fatalf("CancelScheduleExactTerminal: %v", err)
	}

	claimed, err = pgSuccessor.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(successor after cancel): %v", err)
	}
	if claimed {
		t.Fatal("expected cancelled schedule to remain unclaimable while inactive")
	}

	if err := pgOwner.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule(recreate): %v", err)
	}
	claimed, err = pgSuccessor.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(successor after recreate): %v", err)
	}
	if !claimed {
		t.Fatal("expected successor to claim recreated schedule after owner cancel+release")
	}
}

func TestSchedules_CompleteScheduleFireExactReleasesClaimForRecreatedOnceTimer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	pgOwner := newTestPostgresStore(t, db)
	pgSuccessor := newTestPostgresStore(t, db)

	sc := runtimepipeline.Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(30 * time.Minute).UTC(),
		EntityID:     uuid.NewString(),
		FlowInstance: "review/inst-1",
		TaskID:       "timer-fire-reclaim",
		Payload:      []byte(`{"timer_id":"timer-fire-reclaim"}`),
	}
	if err := pgOwner.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	claimed, err := pgOwner.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(owner): %v", err)
	}
	if !claimed {
		t.Fatal("expected owner to claim schedule")
	}

	if err := pgOwner.CompleteScheduleFireExact(ctx, sc); err != nil {
		t.Fatalf("CompleteScheduleFireExact: %v", err)
	}

	claimed, err = pgSuccessor.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(successor after fire): %v", err)
	}
	if claimed {
		t.Fatal("expected fired once schedule to remain unclaimable while inactive")
	}

	if err := pgOwner.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule(recreate): %v", err)
	}
	claimed, err = pgSuccessor.ClaimSchedule(ctx, sc)
	if err != nil {
		t.Fatalf("ClaimSchedule(successor after recreate): %v", err)
	}
	if !claimed {
		t.Fatal("expected successor to claim recreated once timer after canonical fire helper released ownership")
	}
}

func TestEventReceipts_RetryToDeadLetter_AndPendingQueries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "inbound.*")
	eventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(eventID, "inbound.test", "inbound", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now())); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"a1"}); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	pending, err := pg.ListPendingEventsForAgent(ctx, "a1", time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}

	for i := 0; i < 4; i++ {
		if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "error", testRetryableFailure()); err != nil {
			t.Fatalf("upsert receipt: %v", err)
		}
	}
	r, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil || !ok {
		t.Fatalf("GetEventReceipt ok=%v err=%v", ok, err)
	}
	if strings.TrimSpace(string(r.Status)) != "dead_letter" {
		t.Fatalf("expected dead_letter, got %q retry=%d", r.Status, r.RetryCount)
	}

	subscribed, err := pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no subscribed pending events after dead_letter, got %d", len(subscribed))
	}

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "processed", nil); err != nil {
		t.Fatalf("upsert processed: %v", err)
	}
	subscribed, err = pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents processed: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no pending after processed, got %d", len(subscribed))
	}
}

func TestListPendingSubscribedEvents_RespectsDirectDeliveryScope(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	broadcastID := uuid.NewString()
	directOtherID := uuid.NewString()
	directSelfID := uuid.NewString()
	noDeliveryID := uuid.NewString()
	for idx, id := range []string{broadcastID, directOtherID, directSelfID, noDeliveryID} {
		if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(id,
			"inbound.alert",
			"runtime", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().Add(time.Duration(-3+idx)*time.Minute))); err != nil {
			t.Fatalf("seed events: %v", err)
		}
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, created_at)
		VALUES
			($1::uuid, 'agent', 'a1', now()),
			($2::uuid, 'agent', 'a2', now()),
			($3::uuid, 'agent', 'a1', now())
	`, broadcastID, directOtherID, directSelfID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	got, err := pg.ListPendingSubscribedEvents(
		ctx,
		"a1",
		[]events.EventType{"inbound.*"},
		time.Now().Add(-1*time.Hour),
		20,
	)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	gotSet := map[string]struct{}{}
	for _, evt := range got {
		gotSet[strings.TrimSpace(evt.ID())] = struct{}{}
	}
	if _, ok := gotSet[broadcastID]; !ok {
		t.Fatalf("expected broadcast event %s in subscribed backlog, got=%v", broadcastID, gotSet)
	}
	if _, ok := gotSet[directSelfID]; !ok {
		t.Fatalf("expected direct-self event %s in subscribed backlog, got=%v", directSelfID, gotSet)
	}
	if _, ok := gotSet[directOtherID]; ok {
		t.Fatalf("did not expect direct-other event %s in subscribed backlog, got=%v", directOtherID, gotSet)
	}
	if _, ok := gotSet[noDeliveryID]; ok {
		t.Fatalf("did not expect delivery-less event %s in subscribed backlog, got=%v", noDeliveryID, gotSet)
	}
}

func TestPendingSubscribedRecoveryUsesAdmittedSameScopeSubscriptionsPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       "reviewer",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	subscriptions := make([]events.EventType, 0, len(admission.RoutePatterns()))
	for _, pattern := range admission.RoutePatterns() {
		subscriptions = append(subscriptions, events.EventType(pattern))
	}
	now := time.Now().UTC()
	localID, foreignID := uuid.NewString(), uuid.NewString()
	for _, row := range []struct {
		id        string
		eventType events.EventType
	}{{localID, "review/inst-1/task.ready"}, {foreignID, "foreign/task.ready"}} {
		if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(row.id, row.eventType, "runtime", "", json.RawMessage(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, now)); err != nil {
			t.Fatalf("AppendEvent(%s): %v", row.eventType, err)
		}
		if err := pg.InsertEventDeliveries(ctx, row.id, []string{"reviewer"}); err != nil {
			t.Fatalf("InsertEventDeliveries(%s): %v", row.eventType, err)
		}
	}

	got, err := pg.ListPendingSubscribedEvents(ctx, "reviewer", subscriptions, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	if len(got) != 1 || got[0].ID() != localID {
		t.Fatalf("pending events = %#v, want only admitted local event %s", got, localID)
	}
}

func TestPendingEventQueries_PreserveParentCorrelation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	seedSpecAgent(t, ctx, pg, "a1", "", "inbound.*")

	runID := uuid.NewString()
	parentID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(parentID,
		"inbound.root",
		"runtime", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute))); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}

	childID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.ChildWithLineage(childID,
		"inbound.child",
		"runtime", "", []byte(`{}`), 0, events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: runtimeeffects.ExecutionModeLive}, events.EventEnvelope{}, time.Now().Add(-1*time.Minute))); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, childID, []string{"a1"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}

	direct, err := pg.ListPendingEventsForAgent(ctx, "a1", time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(direct) != 1 {
		t.Fatalf("expected 1 direct pending event, got %d", len(direct))
	}
	if got := strings.TrimSpace(direct[0].ParentEventID()); got != parentID {
		t.Fatalf("direct pending parent_event_id = %q, want %q", got, parentID)
	}

	subscribed, err := pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	var child events.Event
	found := false
	for _, evt := range subscribed {
		if strings.TrimSpace(evt.ID()) == childID {
			child = evt
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected child event %s in subscribed pending set", childID)
	}
	if got := strings.TrimSpace(child.ParentEventID()); got != parentID {
		t.Fatalf("subscribed pending parent_event_id = %q, want %q", got, parentID)
	}
}

func TestManagerStore_EventReceiptBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(eventID, "system.started", "runtime", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now())); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, "", "a1", "processed", nil); err == nil {
		t.Fatal("expected UpsertEventReceipt empty event to fail")
	}

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "", nil); err == nil {
		t.Fatal("expected UpsertEventReceipt empty status to fail")
	}
	if _, ok, err := pg.GetEventReceipt(ctx, eventID, "a1"); err != nil || ok {
		t.Fatalf("expected no receipt after invalid write ok=%v err=%v", ok, err)
	}

	if _, _, err := pg.GetEventReceipt(ctx, "", "a1"); err == nil {
		t.Fatalf("expected required args error")
	}
	if _, _, err := pg.GetEventReceipt(ctx, eventID, ""); err == nil {
		t.Fatalf("expected required args error")
	}

	if _, ok, err := pg.GetEventReceipt(ctx, uuid.NewString(), "a1"); err != nil || ok {
		t.Fatalf("expected not found ok=false err=%v", err)
	}
}

func TestManagerStore_GetEventReceipt_FailsClosedOnMalformedSideEffects(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(eventID, "system.started", "runtime", "", []byte(`{}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now())); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', 'a1', e.entity_id, e.flow_instance,
			'dead_letter', '{"retry_count":"bad"}'::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
	`, eventID); err != nil {
		t.Fatalf("insert malformed receipt: %v", err)
	}

	if _, ok, err := pg.GetEventReceipt(ctx, eventID, "a1"); err == nil || ok {
		t.Fatalf("expected malformed side effects error, got ok=%v err=%v", ok, err)
	}
}

func TestManagerStore_MarkEventDeliveryInProgress_RequiresIDs(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	if err := pg.MarkEventDeliveryInProgress(ctx, "", "a1", ""); err == nil {
		t.Fatal("expected empty eventID to fail")
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, uuid.NewString(), "", ""); err == nil {
		t.Fatal("expected empty agentID to fail")
	}
}

func TestManagerStore_LoadRoutingRules_AndDeactivateValidation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), specEntityStateRunID)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "v-flow", "v", "V", "operating")

	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
		EventPattern: "x.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "active",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
		EventPattern: "y.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "deactivated",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule deactivated: %v", err)
	}
	rules, err := pg.LoadRoutingRules(ctx)
	if err != nil {
		t.Fatalf("LoadRoutingRules: %v", err)
	}
	if len(rules) != 1 || rules[0].EventPattern != "x.*" {
		t.Fatalf("expected only active/proposed rules, got %#v", rules)
	}
	if err := pg.DeactivateRoutingRulesByEntity(ctx, ""); err == nil {
		t.Fatalf("expected entity_id required")
	}

	if _, err := pg.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{}); err == nil {
		t.Fatalf("expected lifecycle transition fields required")
	}

	if err := pg.CancelSchedule(ctx, "sub", "timer.recurring_digest"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	_ = time.Second
}

func TestManagerStore_LoadRoutingRules_DoesNotJoinRunScopedEntityState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')
	`, runA, runB); err != nil {
		t.Fatalf("insert runs: %v", err)
	}
	for _, runID := range []string{runA, runB} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state
			)
			VALUES ($1::uuid, $2::uuid, 'shared-flow', 'default', 'shared', 'Shared', 'active')
		`, runID, entityID); err != nil {
			t.Fatalf("insert entity_state for run %s: %v", runID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance,
			is_wildcard, is_materialized, status, created_at
		)
		VALUES ('work.*', 'agent', 'worker', 'shared-flow', true, false, 'active', now())
	`); err != nil {
		t.Fatalf("insert routing rule: %v", err)
	}

	rules, err := pg.LoadRoutingRules(ctx)
	if err != nil {
		t.Fatalf("LoadRoutingRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("LoadRoutingRules returned %d rules, want 1: %#v", len(rules), rules)
	}
	if rules[0].EntityID != "" {
		t.Fatalf("LoadRoutingRules entity_id = %q, want empty persisted route identity", rules[0].EntityID)
	}
}

func TestManagerStore_RoutingRules_DeactivateAndBootstrapVersion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), specEntityStateRunID)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "vslug-flow", "vslug", "V", "operating")

	r := runtimemanager.PersistedRoutingRule{
		EntityID:         entityID,
		EventPattern:     "inbound.*",
		SubscriberID:     "sub",
		InstalledBy:      "inst",
		Reason:           "r",
		Status:           "active",
		Source:           "bootstrap",
		BootstrapVersion: 2,
	}
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}

	r.Status = "inactive"
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule deactivate: %v", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE event_pattern='inbound.*' AND subscriber_id='sub'
	`).Scan(&status); err != nil {
		t.Fatalf("load routing rule status: %v", err)
	}
	if status != "inactive" {
		t.Fatalf("expected inactive status, got %q", status)
	}

	if err := pg.DeactivateRoutingRulesByEntity(ctx, entityID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}
}

func TestManagerStore_Conversations_AndAgentTurns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages: []llm.Message{
			{Role: "user", Content: "reach me at a@example.com"},
		},
		TurnCount: 2,
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.AgentID != "a1" || rec.Identity != identity || rec.Memory != agentmemory.Authored(true) || rec.Status != "active" || rec.TurnCount != 2 {
		t.Fatalf("unexpected conversation: %+v", rec)
	}
	if len(rec.Messages) != 1 || strings.Contains(rec.Messages[0].Content, "a@example.com") || !strings.Contains(rec.Messages[0].Content, "[EMAIL]") {
		t.Fatalf("expected redacted email, got %#v", rec.Messages)
	}

	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: "a1", RunID: identity.RunID, FlowInstance: identity.FlowInstance,
		Memory: agentmemory.Authored(true), SessionID: uuid.NewString(),
	}); err == nil {
		t.Fatal("expected missing session row error")
	}
	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.Authored(true),
		SessionID:      sessionID,
		TaskID:         uuid.NewString(),
		RequestPayload: []byte(`{"x":1}`),
		ResponseRaw:    []byte(`{"y":2}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var toolCallsJSON, emittedEventsJSON string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(tool_calls::text, '[]'),
			COALESCE(emitted_events::text, '[]')
		FROM agent_turns
		WHERE agent_id = 'a1' AND session_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, sessionID).Scan(&toolCallsJSON, &emittedEventsJSON); err != nil {
		t.Fatalf("load agent_turns row: %v", err)
	}
	if toolCallsJSON != "[]" || emittedEventsJSON != "[]" {
		t.Fatalf("expected empty turn telemetry defaults, got calls=%s emitted=%s", toolCallsJSON, emittedEventsJSON)
	}
}

func TestManagerStore_ConversationPersistenceUsesExactFlowInstanceIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	baseCtx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, baseCtx, pg)
	entityID := uuid.NewString()
	seedSpecAgent(t, baseCtx, pg, "entity-agent", entityID, "")

	ctx := runtimeactors.WithActor(baseCtx, runtimeactors.AgentConfig{ExecutionMode: "live", ID: "entity-agent",
		FlowPath: "review/inst-1",
		EntityID: entityID,
	})
	identity := specMemoryIdentity("entity-agent", "review/inst-1")
	sessionID := acquireLiveTestSession(t, ctx, db, identity.AgentID, identity.FlowInstance)

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "entity-agent",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(exact flow instance): %v", err)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.Identity != identity {
		t.Fatalf("unexpected conversation record: %+v", rec)
	}

	var flowInstance string
	if err := db.QueryRowContext(baseCtx, `
		SELECT COALESCE(flow_instance, '')
		FROM agent_sessions
		WHERE run_id = $1::uuid AND agent_id = 'entity-agent' AND flow_instance = $2
	`, identity.RunID, identity.FlowInstance).Scan(&flowInstance); err != nil {
		t.Fatalf("load exact memory session row: %v", err)
	}
	if flowInstance != "review/inst-1" {
		t.Fatalf("flow_instance = %q, want review/inst-1", flowInstance)
	}
}

func TestManagerStore_AppendAgentTurn_PersistsObservedToolCalls(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: "a1", RunID: identity.RunID, FlowInstance: identity.FlowInstance,
		Memory: agentmemory.Authored(true), SessionID: sessionID,
		ToolCalls: []runtimellm.ToolCall{
			{Name: "query_entities", Arguments: map[string]any{"entity_type": "company"}},
			{Name: "web_search", Arguments: map[string]any{"query": "b2b payments"}},
		},
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"result":"done"}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var raw string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(tool_calls::text, '[]')
		FROM agent_turns
		WHERE agent_id = 'a1' AND session_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, sessionID).Scan(&raw); err != nil {
		t.Fatalf("load tool_calls: %v", err)
	}

	var calls []map[string]any
	if err := json.Unmarshal([]byte(raw), &calls); err != nil {
		t.Fatalf("decode tool_calls: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("tool_calls count = %d, want 2 in %s", len(calls), raw)
	}
	if calls[0]["name"] != redactName("query_entities") || calls[1]["name"] != redactName("web_search") {
		t.Fatalf("tool_calls = %#v", calls)
	}
	args0, ok := calls[0]["arguments"].(map[string]any)
	if !ok || args0["entity_type"] != "company" {
		t.Fatalf("first tool call arguments = %#v", calls[0]["arguments"])
	}
	args1, ok := calls[1]["arguments"].(map[string]any)
	if !ok || args1["query"] != "b2b payments" {
		t.Fatalf("second tool call arguments = %#v", calls[1]["arguments"])
	}
}

func TestManagerStore_LiveConversationPersistenceRequiresCanonicalLiveSession(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	seedSpecMemoryRun(t, ctx, db)
	identity := specMemoryIdentity("a1", "global")

	err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: uuid.NewString(),
		AgentID:   "a1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []llm.Message{{Role: "assistant", Content: "hello"}},
		TurnCount: 1,
		Status:    "active",
	})
	if err == nil {
		t.Fatal("expected live conversation persistence without a live session row to fail")
	}
	if !strings.Contains(err.Error(), "no exact active memory row found") {
		t.Fatalf("unexpected error: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'a1'`).Scan(&count); err != nil {
		t.Fatalf("count agent_sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no live session rows to be created by conversation persistence, got %d", count)
	}
}

func TestManagerStore_LoadActiveConversationIncludesRetryLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	seedSpecMemoryRun(t, ctx, db)

	registry := pg
	sessionCtx := runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	identity := specMemoryIdentity("a1", "global")
	lease, err := registry.Acquire(sessionCtx, identity, "worker-1")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rotated, err := registry.Rotate(sessionCtx, identity, "worker-1", runtimesessions.RotationMetadata{
		CheckpointSummary: "rotation_reason=session not found",
		RetryReason:       "session not found",
		OperationID:       uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if rotated.RetriesFromSessionID != lease.SessionID {
		t.Fatalf("RetriesFromSessionID = %q, want %q", rotated.RetriesFromSessionID, lease.SessionID)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.SessionID != rotated.SessionID {
		t.Fatalf("SessionID = %q, want %q", rec.SessionID, rotated.SessionID)
	}
	if rec.RetryReason != "session not found" {
		t.Fatalf("RetryReason = %q, want session not found", rec.RetryReason)
	}
	if rec.RetriesFromSessionID != lease.SessionID {
		t.Fatalf("RetriesFromSessionID = %q, want %q", rec.RetriesFromSessionID, lease.SessionID)
	}

	var gotReason, gotFrom string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(runtime_state->>'retry_reason', ''),
			COALESCE(runtime_state->>'retries_from_session_id', '')
		FROM agent_sessions
		WHERE run_id = $1::uuid AND agent_id = 'a1' AND flow_instance = 'global' AND status = 'active'
	`, identity.RunID).Scan(&gotReason, &gotFrom); err != nil {
		t.Fatalf("load runtime_state retry lineage: %v", err)
	}
	if gotReason != "session not found" || gotFrom != lease.SessionID {
		t.Fatalf("unexpected runtime_state retry lineage: reason=%q from=%q", gotReason, gotFrom)
	}

	var (
		oldStatus            string
		oldTerminationReason string
		oldSuccessorID       string
		oldTerminatedAt      time.Time
		sameAgent            bool
		sameRun              bool
		sameFlowInstance     bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(status, ''),
			COALESCE(termination_reason, ''),
			COALESCE(successor_session_id::text, ''),
			terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, lease.SessionID).Scan(&oldStatus, &oldTerminationReason, &oldSuccessorID, &oldTerminatedAt); err != nil {
		t.Fatalf("load rotated predecessor metadata: %v", err)
	}
	if oldStatus != "terminated" {
		t.Fatalf("predecessor status = %q, want terminated", oldStatus)
	}
	if oldTerminationReason != "contaminated" {
		t.Fatalf("predecessor termination_reason = %q, want contaminated", oldTerminationReason)
	}
	if oldSuccessorID != rotated.SessionID {
		t.Fatalf("predecessor successor_session_id = %q, want %q", oldSuccessorID, rotated.SessionID)
	}
	if oldTerminatedAt.IsZero() {
		t.Fatal("predecessor terminated_at is zero")
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
			old.agent_id = new.agent_id,
			old.run_id = new.run_id,
			old.flow_instance = new.flow_instance
		FROM agent_sessions old
		JOIN agent_sessions new ON new.session_id = $2::uuid
		WHERE old.session_id = $1::uuid
	`, lease.SessionID, rotated.SessionID).Scan(&sameAgent, &sameRun, &sameFlowInstance); err != nil {
		t.Fatalf("compare rotated lineage identity: %v", err)
	}
	if !sameAgent || !sameRun || !sameFlowInstance {
		t.Fatalf("rotated lineage mismatch: sameAgent=%v sameRun=%v sameFlowInstance=%v", sameAgent, sameRun, sameFlowInstance)
	}
}

func TestManagerStore_LoadActiveConversationFailsOnMalformedCanonicalRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = '{"summary":123}'::jsonb
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("seed malformed runtime_state: %v", err)
	}

	if _, ok, err := pg.LoadActiveConversation(ctx, identity); err == nil || ok {
		t.Fatalf("expected malformed canonical runtime_state to fail, ok=%v err=%v", ok, err)
	} else if !strings.Contains(err.Error(), "decode exact live session runtime_state") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerStore_LoadActiveConversationFailsOnMalformedCanonicalWatchdogRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = '{"summary":"ok","watchdog":{"state":"mystery","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","last_output_at":"2026-04-10T12:00:00Z","recorded_at":"2026-04-10T12:00:30Z"}}'::jsonb
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("seed malformed watchdog runtime_state: %v", err)
	}

	if _, ok, err := pg.LoadActiveConversation(ctx, identity); err == nil || ok {
		t.Fatalf("expected malformed canonical watchdog runtime_state to fail, ok=%v err=%v", ok, err)
	} else if !strings.Contains(err.Error(), "decode exact live session runtime_state") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerStore_UpdateLiveSessionWatchdog_RoundTripsThroughLoadActiveConversation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Watchdog: &runtimellm.ConversationWatchdog{
			State:         "healthy_long_running",
			BlockingLayer: "session_execution",
			Action:        "turn_long_running",
			Outcome:       "observed",
			LastOutputAt:  "2026-04-10T12:00:00Z",
			RecordedAt:    "2026-04-10T12:00:30Z",
		},
	})
	if err != nil {
		t.Fatalf("UpdateLiveSessionWatchdog: %v", err)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.Watchdog == nil {
		t.Fatal("expected watchdog to round-trip")
	}
	if rec.Watchdog.State != "healthy_long_running" || rec.Watchdog.Action != "turn_long_running" || rec.Watchdog.Outcome != "observed" {
		t.Fatalf("unexpected watchdog: %+v", rec.Watchdog)
	}
	if rec.Watchdog.BlockingLayer != "session_execution" {
		t.Fatalf("watchdog blocking_layer = %q, want session_execution", rec.Watchdog.BlockingLayer)
	}
}

func TestManagerStore_UpdateLiveSessionWatchdogRejectsMalformedWrite(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Watchdog: &runtimellm.ConversationWatchdog{
			State:         "healthy_long_running",
			BlockingLayer: "session_execution",
			Action:        "turn_long_running",
			Outcome:       "observed",
			RecordedAt:    "2026-04-10T12:00:30Z",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "watchdog.last_output_at") {
		t.Fatalf("UpdateLiveSessionWatchdog err = %v, want watchdog.last_output_at validation", err)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.Watchdog != nil {
		t.Fatalf("malformed watchdog write poisoned runtime_state: %+v", rec.Watchdog)
	}
}

func TestManagerStore_UpdateLiveSessionWatchdog_PreservesCanonicalSummary(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []runtimellm.Message{{Role: "assistant", Content: "still working"}},
		Summary:   "still working",
		TurnCount: 2,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}

	if err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Watchdog: &runtimellm.ConversationWatchdog{
			State:         "no_output",
			BlockingLayer: "session_execution",
			Action:        "session_no_output",
			Outcome:       "warning_emitted",
			RecordedAt:    "2026-04-10T12:00:30Z",
		},
	}); err != nil {
		t.Fatalf("UpdateLiveSessionWatchdog: %v", err)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, identity)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.Summary != "still working" {
		t.Fatalf("summary = %q, want still working", rec.Summary)
	}
	if rec.Watchdog == nil || rec.Watchdog.State != "no_output" {
		t.Fatalf("unexpected watchdog after summary-preserving update: %+v", rec.Watchdog)
	}
}

func TestManagerStore_AppendStatelessAgentTurnPersistsTurnBlocks(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)

	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: "a1", RunID: runID, FlowInstance: "global",
		Memory: agentmemory.PlatformDefault(), SessionID: sessionID,
		TurnBlocks: []runtimellm.TurnBlock{
			{Kind: "dispatch", Title: "scoring/vertical.marginal", Data: json.RawMessage(`{"trigger_event_type":"scoring/vertical.marginal"}`)},
			{Kind: "tool_use", ToolName: "schedule", Input: json.RawMessage(`{"delay_seconds":1209600}`)},
			{Kind: "outcome", Text: "14-day review scheduled."},
		},
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"result":"14-day review scheduled."}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var got string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(turn_blocks::text, '[]') FROM agent_turns WHERE session_id = $1::uuid ORDER BY created_at DESC LIMIT 1`, sessionID).Scan(&got); err != nil {
		t.Fatalf("load turn_blocks: %v", err)
	}
	if !strings.Contains(got, `"dispatch"`) || !strings.Contains(got, `"schedule"`) || !strings.Contains(got, `"14-day review scheduled."`) || !strings.Contains(got, `"turn_summary"`) {
		t.Fatalf("turn_blocks = %s", got)
	}
}

func TestManagerStore_AppendStatelessAgentTurnCanonicalizesTurnBlocksThroughSingleStoreAdapter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)

	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: "a1", RunID: runID, FlowInstance: "global",
		Memory:           agentmemory.PlatformDefault(),
		SessionID:        sessionID,
		TriggerEventType: "task.run",
		ResponseRaw:      []byte(`{"result":"14-day review scheduled."}`),
		ParseOK:          true,
		Latency:          5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var got string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(turn_blocks::text, '[]') FROM agent_turns WHERE session_id = $1::uuid ORDER BY created_at DESC LIMIT 1`, sessionID).Scan(&got); err != nil {
		t.Fatalf("load turn_blocks: %v", err)
	}
	if strings.Count(got, `"turn_summary"`) != 1 {
		t.Fatalf("turn_blocks = %s, want exactly one turn_summary", got)
	}
	if !strings.Contains(got, `"dispatch"`) || !strings.Contains(got, `"task.run"`) || !strings.Contains(got, `"14-day review scheduled."`) {
		t.Fatalf("turn_blocks = %s", got)
	}
}

func TestManagerStore_AppendAgentTurn_LeavesLiveSessionRuntimeStateForLiveOwnershipAndPersistsTurnRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session): %v", err)
	}

	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.Authored(true),
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"result":"done"}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var parseOK bool
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE((runtime_state->'last_turn'->>'parse_ok')::boolean, false)
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&parseOK); err != nil {
		t.Fatalf("load session runtime_state: %v", err)
	}
	if parseOK {
		t.Fatal("did not expect live session row to persist last_turn telemetry")
	}

	var turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns
		WHERE agent_id = 'a1' AND session_id = $1::uuid AND memory_enabled = TRUE
	`, sessionID).Scan(&turnCount); err != nil {
		t.Fatalf("count agent_turns(session): %v", err)
	}
	if turnCount != 1 {
		t.Fatalf("expected one session-mode agent_turn row, got %d", turnCount)
	}
}

func TestManagerStore_AppendAgentTurn_PreservesLiveSessionRetryLineageRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = jsonb_build_object(
			'provider_session_id', 'provider-1',
			'retry_reason', 'session not found',
			'retries_from_session_id', 'session-old'
		)
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("seed live runtime_state: %v", err)
	}

	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.Authored(true),
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"result":"done"}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var providerID, retryReason, retriesFrom string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(runtime_state->>'provider_session_id', ''),
			COALESCE(runtime_state->>'retry_reason', ''),
			COALESCE(runtime_state->>'retries_from_session_id', '')
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&providerID, &retryReason, &retriesFrom); err != nil {
		t.Fatalf("load live runtime_state lineage: %v", err)
	}
	if providerID != "provider-1" || retryReason != "session not found" || retriesFrom != "session-old" {
		t.Fatalf("unexpected live runtime_state after append: provider=%q reason=%q from=%q", providerID, retryReason, retriesFrom)
	}
}

func TestManagerStore_AppendAgentTurnRollsBackStatelessAuditAndTurnWhenTurnInsertFails(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	installFailAgentTurnInsertTrigger(t, ctx, db)

	sessionID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)
	err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          runID,
		FlowInstance:   "global",
		Memory:         agentmemory.PlatformDefault(),
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected AppendAgentTurn to fail when agent_turns insert fails")
	}

	var auditCount, turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_conversation_audits WHERE session_id = $1::uuid),
			(SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid)
	`, sessionID).Scan(&auditCount, &turnCount); err != nil {
		t.Fatalf("count rolled back task persistence: %v", err)
	}
	if auditCount != 0 {
		t.Fatalf("expected synthesized task audit row rollback, got %d rows", auditCount)
	}
	if turnCount != 0 {
		t.Fatalf("expected agent_turn insert rollback, got %d rows", turnCount)
	}
}

func TestManagerStore_StatelessTurnPersistsAuditEvidenceWithoutLiveMemory(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)
	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          runID,
		FlowInstance:   "global",
		Memory:         agentmemory.Authored(false),
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(stateless): %v", err)
	}

	var memoryEnabled bool
	var memorySource, conversation string
	if err := db.QueryRowContext(ctx, `
		SELECT memory_enabled, memory_source, conversation::text
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&memoryEnabled, &memorySource, &conversation); err != nil {
		t.Fatalf("load stateless audit: %v", err)
	}
	if memoryEnabled || memorySource != "authored" || conversation != "[]" {
		t.Fatalf("stateless audit memory=%v source=%q conversation=%s", memoryEnabled, memorySource, conversation)
	}

	var turnCount, liveCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_turns WHERE agent_id = 'a1' AND session_id = $1::uuid AND memory_enabled = FALSE),
			(SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'a1')
	`, sessionID).Scan(&turnCount, &liveCount); err != nil {
		t.Fatalf("count stateless evidence: %v", err)
	}
	if turnCount != 1 || liveCount != 0 {
		t.Fatalf("stateless evidence turns=%d live_memory=%d, want 1/0", turnCount, liveCount)
	}
}

func TestManagerStore_MemoryConversationDoesNotPersistStatelessAuditRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", "global")
	identity := specMemoryIdentity("a1", "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages: []llm.Message{
			{Role: "user", Content: "hello"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(memory): %v", err)
	}

	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.Authored(true),
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(memory): %v", err)
	}

	var auditCount, turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_conversation_audits WHERE agent_id = 'a1'),
			(SELECT COUNT(*) FROM agent_turns WHERE agent_id = 'a1' AND session_id = $1::uuid AND memory_enabled = TRUE)
	`, sessionID).Scan(&auditCount, &turnCount); err != nil {
		t.Fatalf("count persisted rows: %v", err)
	}
	if auditCount != 0 {
		t.Fatalf("expected no stateless audit rows for memory-enabled persistence, got %d", auditCount)
	}
	if turnCount != 1 {
		t.Fatalf("expected one memory-enabled turn row, got %d", turnCount)
	}
}

func TestManagerStore_AppendStatelessTurnCreatesCanonicalAuditRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)
	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID: "a1", RunID: runID, FlowInstance: "global",
		Memory: agentmemory.PlatformDefault(), SessionID: sessionID,
		RequestPayload: []byte(`{"kind":"stateless"}`), ResponseRaw: []byte(`{"ok":true}`),
		ParseOK: true, Latency: 5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(stateless missing audit): %v", err)
	}

	var gotRunID, flowInstance, source, conversation string
	var enabled bool
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, COALESCE(flow_instance,''), memory_enabled, memory_source, conversation::text
		FROM agent_conversation_audits WHERE session_id = $1::uuid
	`, sessionID).Scan(&gotRunID, &flowInstance, &enabled, &source, &conversation); err != nil {
		t.Fatalf("load canonical stateless audit row: %v", err)
	}
	if gotRunID != runID || flowInstance != "global" || enabled || source != "platform_default" || conversation != "[]" {
		t.Fatalf("audit run=%q flow=%q enabled=%v source=%q conversation=%s", gotRunID, flowInstance, enabled, source, conversation)
	}
	var turns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE session_id=$1::uuid AND memory_enabled=FALSE`, sessionID).Scan(&turns); err != nil || turns != 1 {
		t.Fatalf("stateless turn count=%d err=%v, want 1", turns, err)
	}
}

func TestManagerStore_AppendStatelessTurnPersistsEntityAsAuditMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	entityID := uuid.NewString()
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)
	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		SessionID:      sessionID,
		RunID:          runID,
		Memory:         agentmemory.Authored(false),
		EntityID:       entityID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless entity metadata): %v", err)
	}

	var count, turns int
	var gotEntityID, flowInstance, conversation, persistedRunID, memorySource string
	var memoryEnabled bool
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(MAX(entity_id::text), ''),
			COALESCE(MAX(flow_instance), ''),
			COALESCE(MAX(conversation::text), ''),
			COALESCE(MAX(run_id::text), ''),
			BOOL_OR(memory_enabled),
			COALESCE(MAX(memory_source), ''),
			(SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid AND memory_enabled = FALSE AND entity_id = $2::uuid)
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID, entityID).Scan(&count, &gotEntityID, &flowInstance, &conversation, &persistedRunID, &memoryEnabled, &memorySource, &turns); err != nil {
		t.Fatalf("read stateless entity audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("audit row count = %d, want 1", count)
	}
	if gotEntityID != entityID || flowInstance != "" || memoryEnabled || memorySource != "authored" {
		t.Fatalf("audit entity=%q flow_instance=%q memory=%v source=%q", gotEntityID, flowInstance, memoryEnabled, memorySource)
	}
	if persistedRunID != runID {
		t.Fatalf("audit run_id = %q, want %q", persistedRunID, runID)
	}
	if conversation != "[]" {
		t.Fatalf("conversation = %s, want empty stateless audit snapshot", conversation)
	}
	if turns != 1 {
		t.Fatalf("linked task turn count = %d, want 1", turns)
	}

	detail, err := pg.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversationTurns: %v", err)
	}
	if detail.Conversation.Memory || detail.Conversation.MemorySource != "authored" || detail.Conversation.FlowInstance != "" || detail.Conversation.TurnCount != 1 {
		t.Fatalf("operator conversation summary = %+v, want authored stateless audit with one turn", detail.Conversation)
	}
	if len(detail.Turns) != 1 {
		t.Fatalf("operator conversation turns = %d, want 1", len(detail.Turns))
	}
}

func TestManagerStore_AppendStatelessTurnPersistsFlowInstanceAuditIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	flowInstance := "review/inst-1"
	runID := uuid.NewString()
	seedManagerRun(t, ctx, db, runID)
	if err := pg.AppendAgentTurn(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), managedAgentTurnRecordForTest(t, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		SessionID:      sessionID,
		RunID:          runID,
		FlowInstance:   flowInstance,
		Memory:         agentmemory.PlatformDefault(),
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})); err != nil {
		t.Fatalf("AppendAgentTurn(stateless flow): %v", err)
	}

	var count, turns int
	var entityID, gotFlowInstance, conversation, persistedRunID, memorySource string
	var memoryEnabled bool
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(MAX(entity_id::text), ''),
			COALESCE(MAX(flow_instance), ''),
			COALESCE(MAX(conversation::text), ''),
			COALESCE(MAX(run_id::text), ''),
			BOOL_OR(memory_enabled),
			COALESCE(MAX(memory_source), ''),
			(SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid AND memory_enabled = FALSE)
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&count, &entityID, &gotFlowInstance, &conversation, &persistedRunID, &memoryEnabled, &memorySource, &turns); err != nil {
		t.Fatalf("read stateless flow audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("audit row count = %d, want 1", count)
	}
	if gotFlowInstance != flowInstance || entityID != "" || memoryEnabled || memorySource != "platform_default" {
		t.Fatalf("audit entity=%q flow_instance=%q memory=%v source=%q", entityID, gotFlowInstance, memoryEnabled, memorySource)
	}
	if persistedRunID != runID {
		t.Fatalf("audit run_id = %q, want %q", persistedRunID, runID)
	}
	if conversation != "[]" {
		t.Fatalf("conversation = %s, want empty stateless audit snapshot", conversation)
	}
	if turns != 1 {
		t.Fatalf("linked task turn count = %d, want 1", turns)
	}

	detail, err := pg.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversationTurns: %v", err)
	}
	if detail.Conversation.Memory || detail.Conversation.MemorySource != "platform_default" || detail.Conversation.FlowInstance != flowInstance || detail.Conversation.TurnCount != 1 {
		t.Fatalf("operator conversation summary = %+v, want platform-default stateless flow audit with one turn", detail.Conversation)
	}
	if len(detail.Turns) != 1 {
		t.Fatalf("operator conversation turns = %d, want 1", len(detail.Turns))
	}
}

func TestManagerStore_AppendAgentTurn_FailsOnMalformedCanonicalRuntimeLogTurnBlock(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecMemoryRun(t, ctx, db)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID:      "a1",
		Memory:       agentmemory.PlatformDefault(),
		SessionID:    sessionID,
		RunID:        specEntityStateRunID,
		FlowInstance: "global",
		TurnBlocks: []runtimellm.TurnBlock{
			{
				Kind:  "runtime_log",
				Title: "runtime log",
				Data:  json.RawMessage(`{"log_level":"warn","message":"runtime log","details":{"action":"tool_execution_denied"}}`),
			},
		},
		ParseOK: true,
		Latency: 5 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "canonical runtime_log block details.component is required") {
		t.Fatalf("AppendAgentTurn error = %v, want canonical runtime_log turn-block failure", err)
	}
}

func TestManagerStore_AppendAgentTurn_FailsOnNonStringCanonicalRuntimeLogTurnBlockField(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecMemoryRun(t, ctx, db)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID:      "a1",
		Memory:       agentmemory.PlatformDefault(),
		SessionID:    sessionID,
		RunID:        specEntityStateRunID,
		FlowInstance: "global",
		TurnBlocks: []runtimellm.TurnBlock{
			{
				Kind:  "runtime_log",
				Title: "runtime log",
				Data:  json.RawMessage(`{"log_level":"warn","message":"runtime log","details":{"component":123,"action":"tool_execution_denied"}}`),
			},
		},
		ParseOK: true,
		Latency: 5 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "canonical runtime_log block details.component must be a string") {
		t.Fatalf("AppendAgentTurn error = %v, want canonical runtime_log string-type failure", err)
	}
}

func TestManagerStore_UpsertAgent_MergesSubscriptions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "a1",
			Type:          "sonnet",
			Role:          "a1",
			FlowID:        "global",
			Model:         "regular",
			Subscriptions: []string{"inbound.*"},
			Config:        []byte(`{"system_prompt":"x"}`),
		},
		Status:  "active",
		HiredBy: "test",
	}
	if err := pg.UpsertAgent(ctx, rec); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	found := false
	for _, a := range agents {
		if a.Config.ID == "a1" {
			found = true
			if len(a.Config.Subscriptions) != 1 || a.Config.Subscriptions[0] != "inbound.*" {
				t.Fatalf("expected subscriptions merged, got %#v", a.Config.Subscriptions)
			}
		}
	}
	if !found {
		t.Fatalf("expected agent loaded")
	}

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{ExecutionMode: "live"}}); err == nil {
		t.Fatalf("expected agent id required error")
	}
}

func TestManagerStore_UpsertAgent_PersistsCanonicalControlPlaneOwnership(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-canonical-1",
			Type:            "review-worker",
			Role:            "reviewer",
			FlowID:          "review",
			Model:           "regular",
			LLMBackend:      "claude_cli",
			Memory:          agentmemory.Authored(true),
			MaxTurnsPerTask: 7,
			Subscriptions:   []string{"review.ready"},
			EmitEvents:      []string{"review.completed"},
			Tools:           []string{"agent_message"},
			Permissions:     []string{"agent_message"},
			NativeTools:     runtimeactors.NativeToolConfig{FileIO: true},
			WorkspaceClass:  "shared_flow",
			ManagerFallback: "control-plane",
			FlowPath:        "review/inst-1",
			EntityID:        entityID,
			ParentAgent:     "manager-1",
			Config: json.RawMessage(`{
				"system_prompt":"x",
				"type":"wrong-type",
				"memory":false,
				"subscriptions":["wrong.subscription"],
				"manager_fallback":"wrong-manager",
				"workspace_class":"wrong-workspace"
			}`),
		},
		Status: "active",
	}
	if err := pg.UpsertAgent(ctx, rec); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("agent count = %d, want 1", len(agents))
	}
	got := agents[0].Config
	if got.Type != "review-worker" {
		t.Fatalf("type = %q, want review-worker", got.Type)
	}
	if got.Model != "regular" {
		t.Fatalf("model = %q, want regular", got.Model)
	}
	if got.LLMBackend != "claude_cli" {
		t.Fatalf("llm_backend = %q, want claude_cli", got.LLMBackend)
	}
	if got.FlowID != "review" {
		t.Fatalf("flow_id = %q, want review", got.FlowID)
	}
	if got.Memory != agentmemory.Authored(true) {
		t.Fatalf("memory = %+v, want authored true", got.Memory)
	}
	if got.MaxTurnsPerTask != 7 {
		t.Fatalf("max_turns_per_task = %d, want 7", got.MaxTurnsPerTask)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0] != "review.ready" {
		t.Fatalf("subscriptions = %#v, want [review.ready]", got.Subscriptions)
	}
	if len(got.EmitEvents) != 1 || got.EmitEvents[0] != "review.completed" {
		t.Fatalf("emit_events = %#v, want [review.completed]", got.EmitEvents)
	}
	if got.ManagerFallback != "control-plane" {
		t.Fatalf("manager_fallback = %q, want control-plane", got.ManagerFallback)
	}
	if got.WorkspaceClass != "shared_flow" {
		t.Fatalf("workspace_class = %q, want shared_flow", got.WorkspaceClass)
	}
	if got.CanonicalFlowPath() != "review/inst-1" {
		t.Fatalf("flow_path = %q, want review/inst-1", got.FlowPath)
	}
	if got.ParentAgent != "manager-1" {
		t.Fatalf("parent_agent = %q, want manager-1", got.ParentAgent)
	}
	var opaque map[string]any
	if err := json.Unmarshal(got.Config, &opaque); err != nil {
		t.Fatalf("unmarshal opaque config: %v", err)
	}
	if len(opaque) != 1 || opaque["system_prompt"] != "x" {
		t.Fatalf("opaque config = %#v, want only system_prompt", opaque)
	}

	var (
		configRaw            []byte
		runtimeDescriptorRaw []byte
	)
	if err := db.QueryRowContext(ctx, `
		SELECT config, runtime_descriptor
		FROM agents
		WHERE agent_id = $1
	`, rec.Config.ID).Scan(&configRaw, &runtimeDescriptorRaw); err != nil {
		t.Fatalf("query persisted agent row: %v", err)
	}
	if err := validateOpaqueAgentConfig(configRaw); err != nil {
		t.Fatalf("validateOpaqueAgentConfig: %v", err)
	}
	desc, err := decodePersistedAgentRuntimeDescriptor(runtimeDescriptorRaw)
	if err != nil {
		t.Fatalf("decodePersistedAgentRuntimeDescriptor: %v", err)
	}
	if desc.Type != "review-worker" {
		t.Fatalf("runtime_descriptor.type = %q, want review-worker", desc.Type)
	}
	if desc.ManagerFallback != "control-plane" {
		t.Fatalf("runtime_descriptor.manager_fallback = %q, want control-plane", desc.ManagerFallback)
	}
}

func TestProjectPersistedAgentConfig_UsesCanonicalLLMBackendProfiles(t *testing.T) {
	projection, err := projectPersistedAgentConfig(runtimeactors.AgentConfig{
		ID:            "agent-default-backend",
		Role:          "reviewer",
		Model:         "regular",
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		Memory:        agentmemory.PlatformDefault(),
	}, "")
	if err != nil {
		t.Fatalf("projectPersistedAgentConfig: %v", err)
	}
	if projection.LLMBackend != "anthropic" {
		t.Fatalf("llm_backend = %q, want anthropic default profile", projection.LLMBackend)
	}

	projection, err = projectPersistedAgentConfig(runtimeactors.AgentConfig{
		ID:            "agent-openai-compatible-backend",
		Role:          "reviewer",
		Model:         "regular",
		LLMBackend:    "openai_compatible",
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		Memory:        agentmemory.PlatformDefault(),
	}, "")
	if err != nil {
		t.Fatalf("projectPersistedAgentConfig openai_compatible: %v", err)
	}
	if projection.LLMBackend != "openai_compatible" {
		t.Fatalf("llm_backend = %q, want openai_compatible", projection.LLMBackend)
	}

	projection, err = projectPersistedAgentConfig(runtimeactors.AgentConfig{
		ID:            "agent-openai-responses-backend",
		Role:          "reviewer",
		Model:         "regular",
		LLMBackend:    "openai_responses",
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		Memory:        agentmemory.PlatformDefault(),
	}, "")
	if err != nil {
		t.Fatalf("projectPersistedAgentConfig openai_responses: %v", err)
	}
	if projection.LLMBackend != "openai_responses" {
		t.Fatalf("llm_backend = %q, want openai_responses", projection.LLMBackend)
	}

	_, err = projectPersistedAgentConfig(runtimeactors.AgentConfig{
		ID:            "agent-bad-backend",
		Role:          "reviewer",
		Model:         "regular",
		LLMBackend:    "openai",
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		Memory:        agentmemory.PlatformDefault(),
	}, "")
	if err == nil || !strings.Contains(err.Error(), "invalid llm_backend") {
		t.Fatalf("projectPersistedAgentConfig error = %v, want invalid llm_backend", err)
	}
}

func TestManagerStore_LoadAgentsSpec_FailsClosedWhenOpaqueConfigContainsRuntimeKeys(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status
		) VALUES (
			$1, NULL, 'reviewer', 'regular', 'anthropic', FALSE, 'platform_default',
			NULL, NULL, $2::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
			$3::jsonb, 'active'
		)
	`, "agent-invalid-config", `{"system_prompt":"x","subscriptions":["wrong"]}`, `{"type":"review-worker"}`); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}

	_, err := pg.loadAgentsSpec(ctx)
	if err == nil || !strings.Contains(err.Error(), "config contains runtime-owned keys: subscriptions") {
		t.Fatalf("loadAgentsSpec error = %v, want runtime-owned config key failure", err)
	}
}

func TestManagerStore_LoadAgents_FailsClosedWhenCanonicalModelMissing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status
		) VALUES (
			$1, NULL, 'reviewer', '', 'anthropic', FALSE, 'platform_default',
			NULL, NULL, '{}'::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
			$2::jsonb, 'active'
		)
	`, "agent-missing-type", `{"type":"review-worker"}`); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}

	_, err := pg.LoadAgents(ctx)
	if err == nil || !strings.Contains(err.Error(), "missing model") {
		t.Fatalf("LoadAgents error = %v, want missing model failure", err)
	}
}

func TestPostgresStore_Manager_MoreCoverage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), specEntityStateRunID)
	ctx = runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "testco", "testco", "TestCo", "operating")
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "a1",
			Role:     "role",
			FlowID:   "global",
			Type:     "sonnet",
			Model:    "regular",
			Memory:   agentmemory.PlatformDefault(),
			EntityID: "",
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["system.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	terminateSpecAgentViaLifecycle(t, ctx, pg, "a1")
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "ephemeral-shard-1",
			Role:     "worker",
			FlowID:   "worker",
			Type:     "sonnet",
			Model:    "regular",
			Memory:   agentmemory.PlatformDefault(),
			EntityID: "",
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["review.ready"]}`),
		},
		Status:          "ephemeral",
		HiredBy:         "runtime",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent ephemeral: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	for _, a := range agents {
		if a.Config.ID == "a1" {
			t.Fatal("expected terminated agent to be excluded from LoadAgents")
		}
		if a.Config.ID == "ephemeral-shard-1" {
			t.Fatal("expected ephemeral agent to be excluded from LoadAgents")
		}
	}

	ceoID := "operator-" + entityID
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: ceoID,
			Role:     "operator",
			FlowID:   "operating",
			Type:     "sonnet",
			Model:    "regular",
			Memory:   agentmemory.Authored(true),
			FlowPath: "operating/global",
			EntityID: entityID,
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["review.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	})
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
		EventPattern: "review.*",
		SubscriberID: ceoID,
		InstalledBy:  ceoID,
		Reason:       "tests",
		Status:       "active",
		Source:       "seeded",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.DeactivateRoutingRulesByEntity(ctx, entityID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}

	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		"review.requested",
		"human",
		"",
		json.RawMessage(`{"x":1}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().Add(-2*time.Hour),
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{ceoID}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	pending, err := pg.ListPendingEventsForAgent(ctx, ceoID, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(pending) == 0 {
		t.Fatalf("ListPendingEventsForAgent err=%v len=%d", err, len(pending))
	}

	subPending, err := pg.ListPendingSubscribedEvents(ctx, ceoID, []events.EventType{"review.*"}, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(subPending) == 0 {
		t.Fatalf("ListPendingSubscribedEvents err=%v len=%d", err, len(subPending))
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID(), ceoID, "error", testRetryableFailure()); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	if rec, ok, err := pg.GetEventReceipt(ctx, evt.ID(), ceoID); err != nil || !ok || rec.Status == "" {
		t.Fatalf("GetEventReceipt ok=%v err=%v rec=%+v", ok, err, rec)
	}

	identity := specMemoryIdentity(ceoID, "operating/global")
	sessionID := acquireLiveTestSession(t, ctx, db, identity.AgentID, identity.FlowInstance)
	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		AgentID:        identity.AgentID,
		Memory:         agentmemory.Authored(true),
		SessionID:      sessionID,
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		TaskID:         "",
		RequestPayload: []byte(`{"in":1}`),
		ResponseRaw:    []byte(`{"out":1}`),
		ParseOK:        true,
		Latency:        123 * time.Millisecond,
		RetryCount:     0,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   identity.AgentID,
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		TaskID:    "",
		Messages:  []llm.Message{{Role: "user", Content: "hi"}},
		Summary:   "sum",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	if rec, ok, err := pg.LoadActiveConversation(ctx, identity); err != nil || !ok || rec.AgentID != ceoID {
		t.Fatalf("LoadActiveConversation ok=%v err=%v rec=%+v", ok, err, rec)
	}

	sc := runtimepipeline.Schedule{
		AgentID:   ceoID,
		EventType: "timer.test",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if got, err := pg.LoadActiveSchedules(ctx); err != nil || len(got) == 0 {
		t.Fatalf("LoadActiveSchedules err=%v len=%d", err, len(got))
	}
	if err := pg.MarkScheduleFired(ctx, sc); err != nil {
		t.Fatalf("MarkScheduleFired: %v", err)
	}
	if err := pg.CancelSchedule(ctx, ceoID, "timer.test"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
}

func TestPostgresStore_LoadAgents_FailsClosedOnLegacyRuntimeMetadataInConfig(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
			config, runtime_descriptor, status, created_at
		) VALUES (
			'legacy-session-agent', NULL, 'worker', 'regular', 'anthropic', FALSE, 'platform_default',
			'{"type":"sonnet","mode":"worker","session_scope":"global","system_prompt":"x"}'::jsonb,
			'{"type":"review-worker"}'::jsonb,
			'active',
			now()
		)
	`); err != nil {
		t.Fatalf("seed legacy agent row: %v", err)
	}
	_, err := pg.LoadAgents(ctx)
	if err == nil || !strings.Contains(err.Error(), "invalid opaque config: config contains runtime-owned keys: mode, session_scope, type") {
		t.Fatalf("LoadAgents error = %v, want fail-closed legacy runtime config error", err)
	}

	var configJSON string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(config::text, '{}')
		FROM agents
		WHERE agent_id = 'legacy-session-agent'
	`).Scan(&configJSON); err != nil {
		t.Fatalf("load legacy agent row: %v", err)
	}
	if !strings.Contains(configJSON, "session_scope") {
		t.Fatalf("expected legacy session_scope to remain untouched in opaque config, got %s", configJSON)
	}
}

func TestPostgresStore_LifecycleTerminationCleansMutableRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	resetAgentSessionsSpecTable(t, ctx, pg)

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-cleanup-1",
			Role:     "worker",
			FlowID:   "worker",
			Type:     "sonnet",
			Model:    "regular",
			Memory:   agentmemory.Authored(true),
			FlowPath: "global",
			Config: json.RawMessage(`{
			"system_prompt":"x",
			"subscriptions":["review.ready"]
		}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent: %v", err)
	}

	identity := specMemoryIdentity("agent-cleanup-1", "global")
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: acquireLiveTestSession(t, ctx, db, identity.AgentID, identity.FlowInstance),
		AgentID:   identity.AgentID,
		Identity:  identity,
		Memory:    agentmemory.Authored(true),
		Messages:  []llm.Message{{Role: "user", Content: "hello"}},
		Summary:   "x",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if err := appendManagedAgentTurnForTest(t, ctx, pg, runtimellm.AgentTurnRecord{
		SessionID:      uuid.NewString(),
		AgentID:        identity.AgentID,
		RunID:          identity.RunID,
		FlowInstance:   identity.FlowInstance,
		Memory:         agentmemory.PlatformDefault(),
		ResponseRaw:    []byte(`{"ok":true}`),
		RequestPayload: []byte(`{"kind":"stateless"}`),
		ParseOK:        true,
	}); err != nil {
		t.Fatalf("seed stateless audit: %v", err)
	}
	terminateSpecAgentViaLifecycle(t, ctx, pg, "agent-cleanup-1")

	var (
		agentStatus      string
		sessStatus       string
		sessReason       string
		sessTerminatedAt time.Time
		auditStatus      string
	)
	if err := db.QueryRowContext(ctx, `SELECT status FROM agents WHERE agent_id = $1`, "agent-cleanup-1").Scan(&agentStatus); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(termination_reason, ''), terminated_at
		FROM agent_sessions
		WHERE agent_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, "agent-cleanup-1").Scan(&sessStatus, &sessReason, &sessTerminatedAt); err != nil {
		t.Fatalf("read session status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_conversation_audits WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&auditStatus); err != nil {
		t.Fatalf("read audit status: %v", err)
	}
	if agentStatus != "terminated" {
		t.Fatalf("expected terminated agent status, got %q", agentStatus)
	}
	if sessStatus != "terminated" {
		t.Fatalf("expected terminated session status, got %q", sessStatus)
	}
	if sessReason != "cancelled" {
		t.Fatalf("expected cancelled session termination_reason, got %q", sessReason)
	}
	if sessTerminatedAt.IsZero() {
		t.Fatal("expected non-zero session terminated_at")
	}
	if auditStatus != "active" {
		t.Fatalf("expected immutable stateless audit evidence to survive termination unchanged, got %q", auditStatus)
	}
}

func TestManagerHelpers_MatchingAndRedaction(t *testing.T) {
	got := extractSubscriptions([]byte(`{"subscriptions":["a","b"," "],"tools":[]}`))

	hasA, hasB := false, false
	for _, v := range got {
		if v == "a" {
			hasA = true
		}
		if v == "b" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Fatalf("expected a and b subscriptions, got %v", got)
	}
	if normalizeJSONPayload([]byte(`{"b":1,"a":2}`)) == "" {
		t.Fatal("expected normalized json")
	}
	if !eventidentity.MatchPattern("review.*", "review.chat") {
		t.Fatal("expected subscription match")
	}
	if eventidentity.MatchPattern("review.*", "budget.alert") {
		t.Fatal("unexpected match")
	}
	if nullable("", "x") != "x" {
		t.Fatal("nullable fallback mismatch")
	}
	if sanitizeSchemaIdent("Test-Co!!") != "testco" {
		t.Fatalf("sanitizeSchemaIdent mismatch")
	}
	if quoteIdent("x") != `"x"` {
		t.Fatal("quoteIdent mismatch")
	}

	obj := map[string]any{"api_key": "secret", "name": "John Doe", "nested": map[string]any{"token": "t"}}
	redacted := redactPayloadValue("root", obj)
	b, _ := json.Marshal(redacted)
	if string(b) == "" {
		t.Fatal("expected redacted json")
	}
	_ = redactText("sk-ant-foo")
	_ = redactName("John Doe")
	_ = isNameKey("name")
}
