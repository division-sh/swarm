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

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/eventidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	llm "swarm/internal/runtime/llm"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimesessions "swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/testutil"
)

const specEntityStateRunID = "33333333-3333-3333-3333-333333333333"

func resetAgentSessionsSpecTable(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop legacy agent_sessions: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop legacy agent_turns: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    UNIQUE (agent_id, scope_key)\n);",
		},
		"agent_turns": {
			DDL: "CREATE TABLE agent_turns (\n    turn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    session_id UUID NOT NULL,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    scope_key TEXT,\n    entity_id UUID,\n    trigger_event_id UUID,\n    trigger_event_type TEXT,\n    task_id TEXT,\n    available_tools JSONB NOT NULL DEFAULT '[]',\n    tool_calls JSONB NOT NULL DEFAULT '[]',\n    emitted_events JSONB NOT NULL DEFAULT '[]',\n    mcp_servers JSONB NOT NULL DEFAULT '{}',\n    mcp_tools_listed JSONB NOT NULL DEFAULT '[]',\n    mcp_tools_visible JSONB NOT NULL DEFAULT '[]',\n    request_payload JSONB,\n    response_payload JSONB,\n    parse_ok BOOLEAN NOT NULL DEFAULT FALSE,\n    latency_ms INTEGER NOT NULL DEFAULT 0,\n    retry_count INTEGER NOT NULL DEFAULT 0,\n    error TEXT,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions): %v", err)
	}
}

func acquireLiveTestSession(t *testing.T, ctx context.Context, db *sql.DB, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, scopeKey string) string {
	t.Helper()
	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	lease, err := registry.Acquire(ctx, agentID, runtimeMode, sessionScope, "test-owner", scopeKey)
	if err != nil {
		t.Fatalf("Acquire(%s,%s,%s): %v", agentID, runtimeMode, scopeKey, err)
	}
	if err := registry.Release(ctx, lease); err != nil {
		t.Fatalf("Release(%s,%s): %v", agentID, lease.SessionID, err)
	}
	return lease.SessionID
}

func TestPostgresStore_AgentSessionTerminationMetadataMigrationBackfillsLegacyRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'entity',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'session',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			lease_holder TEXT,
			lease_expires_at TIMESTAMPTZ,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (agent_id, scope_key)
		)
	`); err != nil {
		t.Fatalf("create legacy agent_sessions: %v", err)
	}
	sessionID := uuid.NewString()
	createdAt := time.Now().UTC().Add(-2 * time.Hour).Round(time.Second)
	updatedAt := createdAt.Add(30 * time.Minute)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES ($1::uuid, 'a1', 'global', 'global', 'session', 'terminated', $2, $3)
	`, sessionID, createdAt, updatedAt); err != nil {
		t.Fatalf("insert legacy terminated session: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'session',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions): %v", err)
	}

	var reason string
	var terminatedAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(termination_reason, ''), terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&reason, &terminatedAt); err != nil {
		t.Fatalf("load migrated terminated row: %v", err)
	}
	if reason != "legacy" {
		t.Fatalf("termination_reason = %q, want legacy", reason)
	}
	if !terminatedAt.Equal(updatedAt) {
		t.Fatalf("terminated_at = %s, want %s", terminatedAt, updatedAt)
	}
}

func TestPostgresStore_MarkRunTerminal_UsesCanonicalCountersAndRejectsActiveDeliveries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'scan.requested', $3::uuid, 'entity', '{}'::jsonb, 'builder', 'platform', now()
		)
	`, eventID, runID, entityID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'pending', now()
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	for _, status := range []string{"completed", "failed"} {
		err := pg.MarkRunTerminal(ctx, runID, status, "quiescence timeout", time.Now().UTC())
		if err == nil || !strings.Contains(err.Error(), "active deliveries") {
			t.Fatalf("MarkRunTerminal(%s active delivery) error = %v, want active delivery rejection", status, err)
		}
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'delivered', delivered_at = now()
		WHERE run_id = $1::uuid
	`, runID); err != nil {
		t.Fatalf("deliver completion: %v", err)
	}

	if err := pg.MarkRunTerminal(ctx, runID, "completed", "", time.Now().UTC()); err != nil {
		t.Fatalf("MarkRunTerminal(completed): %v", err)
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

func TestPostgresStore_AppendEvent_ReopensPrematureCompletedRunAndSyncsCounters(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, ended_at, event_count, entity_count)
		VALUES ($1::uuid, 'completed', now(), 0, 0)
	`, runID); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}

	evt := events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("scan.completed"),
		SourceAgent: "agent-1",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
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
	if status != "running" {
		t.Fatalf("run status = %q, want running", status)
	}
	if eventCount != 1 {
		t.Fatalf("event_count = %d, want 1", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("entity_count = %d, want 1", entityCount)
	}
	if endedAt.Valid {
		t.Fatalf("ended_at valid = %v, want cleared after reopening", endedAt.Time)
	}
}

func TestPostgresStore_AppendEvent_DuplicateDoesNotReopenCompletedRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	completedAt := time.Now().UTC().Add(-time.Minute).Round(time.Second)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, ended_at, event_count, entity_count)
		VALUES ($1::uuid, 'completed', $2, 1, 1)
	`, runID, completedAt); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'scan.completed', $3::uuid, 'entity', '{}'::jsonb, 'agent-1', 'agent', now()
		)
	`, eventID, runID, entityID); err != nil {
		t.Fatalf("seed duplicate event: %v", err)
	}

	evt := events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("scan.completed"),
		SourceAgent: "agent-1",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
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

func TestPostgresStore_AppendEvent_AllowsRunsWithoutTriggerColumns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	for _, ddl := range []string{
		`DROP TABLE IF EXISTS event_deliveries CASCADE`,
		`DROP TABLE IF EXISTS events CASCADE`,
		`DROP TABLE IF EXISTS runs CASCADE`,
		`CREATE TABLE runs (
			run_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE events (
			event_id UUID PRIMARY KEY,
			run_id UUID REFERENCES runs(run_id),
			event_name TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope TEXT NOT NULL,
			payload JSONB NOT NULL,
			chain_depth INTEGER NOT NULL DEFAULT 0,
			produced_by TEXT,
			produced_by_type TEXT NOT NULL,
			source_event_id UUID,
			created_at TIMESTAMPTZ NOT NULL
		)`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("exec schema ddl %q: %v", ddl, err)
		}
	}

	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}

	runID := uuid.NewString()
	entityID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("scan.requested"),
		SourceAgent: "builder",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("run status = %q, want running", status)
	}
}

func TestPostgresStore_MarkRunTerminal_AllowsRunsWithoutCounterOrTerminalColumns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	for _, ddl := range []string{
		`DROP TABLE IF EXISTS event_deliveries CASCADE`,
		`DROP TABLE IF EXISTS runs CASCADE`,
		`CREATE TABLE runs (
			run_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE event_deliveries (
			delivery_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			run_id UUID,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("exec schema ddl %q: %v", ddl, err)
		}
	}

	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	if err := pg.MarkRunTerminal(ctx, runID, "completed", "", time.Now().UTC()); err != nil {
		t.Fatalf("MarkRunTerminal: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatalf("load run status: %v", err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}

	snap, err := pg.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRunLifecycleSnapshot: %v", err)
	}
	if snap.Status != "completed" {
		t.Fatalf("snapshot status = %q, want completed", snap.Status)
	}
	if snap.EventCount != 0 {
		t.Fatalf("snapshot event_count = %d, want 0", snap.EventCount)
	}
	if snap.EntityCount != 0 {
		t.Fatalf("snapshot entity_count = %d, want 0", snap.EntityCount)
	}
	if snap.ErrorSummary != "" {
		t.Fatalf("snapshot error_summary = %q, want empty", snap.ErrorSummary)
	}
	if snap.EndedAt != nil {
		t.Fatalf("snapshot ended_at = %#v, want nil without terminal columns", snap.EndedAt)
	}
}

func TestPostgresStore_EnsureSchemaTables_PhasesAgentSessionCompatibilityBeforeDependentDDL(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'entity',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'session',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			lease_holder TEXT,
			lease_expires_at TIMESTAMPTZ,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (agent_id, scope_key)
		)
	`); err != nil {
		t.Fatalf("create legacy agent_sessions: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE IF NOT EXISTS agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'session',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);\nCREATE INDEX IF NOT EXISTS idx_sessions_terminated_reason ON agent_sessions (termination_reason, terminated_at) WHERE status = 'terminated';",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions dependent ddl): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions dependent ddl): %v", err)
	}

	var (
		hasTerminationReason bool
		hasTerminatedAt      bool
		indexName            string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'agent_sessions' AND column_name = 'termination_reason'
		)
	`).Scan(&hasTerminationReason); err != nil {
		t.Fatalf("check termination_reason column: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'agent_sessions' AND column_name = 'terminated_at'
		)
	`).Scan(&hasTerminatedAt); err != nil {
		t.Fatalf("check terminated_at column: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(to_regclass('idx_sessions_terminated_reason')::text, '')`).Scan(&indexName); err != nil {
		t.Fatalf("check idx_sessions_terminated_reason: %v", err)
	}
	if !hasTerminationReason || !hasTerminatedAt {
		t.Fatalf("termination metadata columns missing: termination_reason=%v terminated_at=%v", hasTerminationReason, hasTerminatedAt)
	}
	if indexName != "idx_sessions_terminated_reason" {
		t.Fatalf("idx_sessions_terminated_reason regclass = %q, want idx_sessions_terminated_reason", indexName)
	}
}

func TestPostgresStore_EnsureSchemaTables_ReportsOutdatedSchemaForLegacyTimers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS timers CASCADE`); err != nil {
		t.Fatalf("drop timers: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE timers (
			timer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			timer_name TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			fire_event TEXT NOT NULL,
			fire_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			fire_at TIMESTAMPTZ NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("create legacy timers: %v", err)
	}

	err := pg.EnsureSchemaTables(ctx, []SchemaTableDDL{legacyTimerDiagnosticPlan()})
	if err == nil {
		t.Fatal("EnsureSchemaTables error = nil, want outdated schema diagnostic")
	}
	var outdated *OutdatedSchemaError
	if !errors.As(err, &outdated) {
		t.Fatalf("EnsureSchemaTables error = %T %[1]v, want OutdatedSchemaError", err)
	}
	if outdated.TableName != "timers" {
		t.Fatalf("outdated table = %q, want timers", outdated.TableName)
	}
	if !containsString(outdated.MissingColumns, "run_id") {
		t.Fatalf("missing columns = %#v, want run_id", outdated.MissingColumns)
	}
	got := err.Error()
	for _, want := range []string{
		"database schema is out of date",
		"platform_spec table timers",
		"run_id",
		"use a fresh database or run an approved schema migration",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error = %q, want substring %q", got, want)
		}
	}
	for _, disallowed := range []string{"pq:", "42703"} {
		if strings.Contains(got, disallowed) {
			t.Fatalf("error = %q, want no raw Postgres detail %q", got, disallowed)
		}
	}
}

func TestPostgresStore_EnsureSchemaTables_AllowsCurrentTimerSchema(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.EnsureSchemaTables(ctx, []SchemaTableDDL{legacyTimerDiagnosticPlan()}); err != nil {
		t.Fatalf("EnsureSchemaTables current timers: %v", err)
	}
}

func legacyTimerDiagnosticPlan() SchemaTableDDL {
	return SchemaTableDDL{
		TableName:   "timers",
		SchemaKind:  "platform_spec",
		ColumnCount: 21,
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS timers (
    timer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timer_name TEXT NOT NULL,
    run_id UUID REFERENCES runs(run_id),
    source_timer_id UUID REFERENCES timers(timer_id),
    forked_from_run_id UUID REFERENCES runs(run_id),
    forked_from_event_id UUID REFERENCES events(event_id),
    reconstruction_owner TEXT,
    entity_id UUID,
    flow_instance TEXT,
    fire_event TEXT NOT NULL,
    fire_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fire_at TIMESTAMPTZ NOT NULL,
    recurring BOOLEAN NOT NULL DEFAULT FALSE,
    recurrence_cron TEXT,
    recurrence_interval TEXT,
    owner_node TEXT,
    owner_agent TEXT,
    task_type TEXT NOT NULL DEFAULT 'timer' CHECK (task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'fired', 'cancelled', 'expired')),
    fired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`,
			`CREATE INDEX IF NOT EXISTS idx_timers_run ON timers(run_id, status, fire_at) WHERE run_id IS NOT NULL`,
		},
	}
}

func TestPostgresStore_EnsureSchemaTables_PhasesAgentRuntimeDescriptorCompatibilityBeforeDependentDDL(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agents CASCADE`); err != nil {
		t.Fatalf("drop agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agents (
			agent_id TEXT PRIMARY KEY,
			flow_instance TEXT,
			role TEXT NOT NULL,
			model_tier TEXT NOT NULL,
			llm_backend TEXT NOT NULL DEFAULT 'api',
			conversation_mode TEXT NOT NULL,
			parent_agent_id TEXT,
			entity_id UUID,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			subscriptions JSONB NOT NULL DEFAULT '[]'::jsonb,
			emit_events JSONB NOT NULL DEFAULT '[]'::jsonb,
			tools JSONB NOT NULL DEFAULT '[]'::jsonb,
			permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			turn_count INTEGER NOT NULL DEFAULT 0,
			last_active_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("create legacy agents: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agents": {
			DDL: "CREATE TABLE IF NOT EXISTS agents (\n    agent_id TEXT PRIMARY KEY,\n    flow_instance TEXT,\n    role TEXT NOT NULL,\n    model_tier TEXT NOT NULL,\n    llm_backend TEXT NOT NULL DEFAULT 'api',\n    conversation_mode TEXT NOT NULL,\n    parent_agent_id TEXT,\n    entity_id UUID,\n    config JSONB NOT NULL DEFAULT '{}',\n    subscriptions JSONB NOT NULL DEFAULT '[]',\n    emit_events JSONB NOT NULL DEFAULT '[]',\n    tools JSONB NOT NULL DEFAULT '[]',\n    permissions JSONB NOT NULL DEFAULT '[]',\n    runtime_descriptor JSONB NOT NULL DEFAULT '{}',\n    status TEXT NOT NULL DEFAULT 'active',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    last_active_at TIMESTAMPTZ,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);\nCREATE INDEX IF NOT EXISTS idx_agents_runtime_descriptor ON agents USING GIN (runtime_descriptor);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agents dependent ddl): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agents dependent ddl): %v", err)
	}

	var (
		hasRuntimeDescriptor bool
		indexName            string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'agents' AND column_name = 'runtime_descriptor'
		)
	`).Scan(&hasRuntimeDescriptor); err != nil {
		t.Fatalf("check runtime_descriptor column: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(to_regclass('idx_agents_runtime_descriptor')::text, '')`).Scan(&indexName); err != nil {
		t.Fatalf("check idx_agents_runtime_descriptor: %v", err)
	}
	if !hasRuntimeDescriptor {
		t.Fatal("runtime_descriptor column missing after EnsureSchemaTables")
	}
	if indexName != "idx_agents_runtime_descriptor" {
		t.Fatalf("idx_agents_runtime_descriptor regclass = %q, want idx_agents_runtime_descriptor", indexName)
	}
}

func TestPostgresStore_EnsureSchemaTables_PhasesConversationAuditRunIDCompatibilityBeforeDependentDDL(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_conversation_audits CASCADE`); err != nil {
		t.Fatalf("drop agent_conversation_audits: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS runs CASCADE`); err != nil {
		t.Fatalf("drop runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE runs (run_id UUID PRIMARY KEY DEFAULT gen_random_uuid())`); err != nil {
		t.Fatalf("create runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_conversation_audits (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT,
			scope TEXT NOT NULL DEFAULT 'global',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'task',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("create legacy agent_conversation_audits: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_conversation_audits": {
			DDL: "CREATE TABLE IF NOT EXISTS agent_conversation_audits (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT,\n    scope TEXT NOT NULL DEFAULT 'global',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    run_id UUID REFERENCES runs(run_id),\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);\nCREATE INDEX IF NOT EXISTS idx_audits_run ON agent_conversation_audits (run_id, created_at) WHERE run_id IS NOT NULL;",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_conversation_audits dependent ddl): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_conversation_audits dependent ddl): %v", err)
	}

	var (
		hasRunID  bool
		indexName string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'agent_conversation_audits' AND column_name = 'run_id'
		)
	`).Scan(&hasRunID); err != nil {
		t.Fatalf("check run_id column: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(to_regclass('idx_audits_run')::text, '')`).Scan(&indexName); err != nil {
		t.Fatalf("check idx_audits_run: %v", err)
	}
	if !hasRunID {
		t.Fatal("run_id column missing after EnsureSchemaTables")
	}
	if indexName != "idx_audits_run" {
		t.Fatalf("idx_audits_run regclass = %q, want idx_audits_run", indexName)
	}
}

func TestPostgresStore_EnsureSchemaTables_DropsDeprecatedEntitySubjectCompatibility(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS entity_state CASCADE`); err != nil {
		t.Fatalf("drop entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE entity_state (
			entity_id UUID PRIMARY KEY,
			subject_id UUID
		)
	`); err != nil {
		t.Fatalf("create legacy entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX idx_entity_subject ON entity_state(subject_id) WHERE subject_id IS NOT NULL`); err != nil {
		t.Fatalf("create legacy idx_entity_subject: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"entity_state": {
			DDL: "CREATE TABLE IF NOT EXISTS entity_state (\n    entity_id UUID PRIMARY KEY,\n    subject_id UUID,\n    flow_instance TEXT,\n    INDEX idx_entity_subject (subject_id) WHERE subject_id IS NOT NULL\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(entity_state ddl): %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
			t.Fatalf("EnsureSchemaTables(entity_state ddl) attempt %d: %v", attempt, err)
		}
	}

	var (
		hasSubjectID bool
		indexName    string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'entity_state' AND column_name = 'subject_id'
		)
	`).Scan(&hasSubjectID); err != nil {
		t.Fatalf("check subject_id column: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(to_regclass('idx_entity_subject')::text, '')`).Scan(&indexName); err != nil {
		t.Fatalf("check idx_entity_subject: %v", err)
	}
	if hasSubjectID {
		t.Fatal("subject_id column still exists after EnsureSchemaTables")
	}
	if indexName != "" {
		t.Fatalf("idx_entity_subject regclass = %q, want dropped", indexName)
	}
}

func TestPostgresStore_AgentSessionTerminationMetadataMigrationWithoutAgentTurnsSupportsResetAll(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'entity',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'session',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			lease_holder TEXT,
			lease_expires_at TIMESTAMPTZ,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (agent_id, scope_key)
		)
	`); err != nil {
		t.Fatalf("create legacy agent_sessions: %v", err)
	}
	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'active', now(), now()
		)
	`, sessionID); err != nil {
		t.Fatalf("insert legacy active session: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'session',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions): %v", err)
	}

	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	summary, err := registry.ResetAll(runtimesessions.RuntimeModeSession, runtimesessions.ResetMetadata{})
	if err != nil {
		t.Fatalf("ResetAll after agent_sessions-only migration: %v", err)
	}
	if got := summary.OrphanedCount(); got != 1 {
		t.Fatalf("ResetAll orphaned_count = %d, want 1", got)
	}

	var (
		status       string
		reason       string
		terminatedAt time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(termination_reason, ''), terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &reason, &terminatedAt); err != nil {
		t.Fatalf("load reset legacy session row: %v", err)
	}
	if status != "terminated" {
		t.Fatalf("status = %q, want terminated", status)
	}
	if reason != "orphaned" {
		t.Fatalf("termination_reason = %q, want orphaned", reason)
	}
	if terminatedAt.IsZero() {
		t.Fatal("terminated_at is zero")
	}
}

func TestPostgresStore_AgentSessionsPartialUniquenessAllowsTerminatedHistoryButRejectsSecondLiveOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)

	sessionA := uuid.NewString()
	sessionB := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'terminated',
			'failed', now(), now(), now()
		)
	`, sessionA); err != nil {
		t.Fatalf("insert terminated row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'active', now(), now()
		)
	`, sessionB); err != nil {
		t.Fatalf("insert active row after terminated history: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'suspended', now(), now()
		)
	`, uuid.NewString()); err == nil {
		t.Fatal("expected second non-terminated owner insert to fail")
	}
}

func TestPostgresRegistry_AcquireFailsClosedOnSuspendedResumableOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'suspended', now(), now()
		)
	`, uuid.NewString()); err != nil {
		t.Fatalf("insert suspended session: %v", err)
	}

	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	if _, err := registry.Acquire(ctx, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "worker-1", "global"); err != runtimesessions.ErrSessionSuspended {
		t.Fatalf("Acquire error = %v, want ErrSessionSuspended", err)
	}
}

func TestPostgresRegistry_ResetAllMarksActiveSessionsOrphaned(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")
	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	summary, err := registry.ResetAll(runtimesessions.RuntimeModeSession, runtimesessions.ResetMetadata{Source: "builder_api"})
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

func TestPostgresStore_AgentSessionSuccessorInvariantsRejectCrossScopeAndLegacyWrites(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)

	oldID := uuid.NewString()
	goodSuccessorID := uuid.NewString()
	badSuccessorID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'terminated',
			'failed', now(), now(), now()
		)
	`, oldID); err != nil {
		t.Fatalf("insert terminated session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'global', 'global', 'session', 'active', now(), now()
		)
	`, goodSuccessorID); err != nil {
		t.Fatalf("insert active successor candidate: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'flow-1', 'flow', 'session', 'terminated',
			'failed', now(), now(), now()
		)
	`, badSuccessorID); err != nil {
		t.Fatalf("insert terminated cross-scope candidate: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET successor_session_id = $2::uuid
		WHERE session_id = $1::uuid
	`, oldID, badSuccessorID); err == nil {
		t.Fatal("expected cross-scope successor assignment to fail")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status,
			termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'a1', 'legacy-new', 'global', 'session', 'terminated',
			'legacy', now(), now(), now()
		)
	`, uuid.NewString()); err == nil {
		t.Fatal("expected new legacy termination write to fail")
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
		Mode:          "global",
		Type:          "stub",
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
		if _, err := db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS fail_agent_turn_insert_trigger ON agent_turns`); err != nil {
			t.Fatalf("cleanup fail_agent_turn_insert trigger: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS fail_agent_turn_insert()`); err != nil {
			t.Fatalf("cleanup fail_agent_turn_insert function: %v", err)
		}
	})
}

func TestPostgresStore_AppendEvent_EntityIDBoundaryContract(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	validEntityID := uuid.NewString()
	validEventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          validEventID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "control-plane",
		TaskID:      "legacy-task-key",
		Payload:     []byte(`{"name":"Telemedicine Platform"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(validEntityID)); err != nil {
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
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          emptyEventID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "control-plane",
		Payload:     []byte(`{"name":"Telemedicine Platform"}`),
		CreatedAt:   time.Now(),
	}); err != nil {
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
	err := pg.AppendEvent(ctx, (events.Event{
		ID:          invalidEventID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "control-plane",
		Payload:     []byte(`{"name":"Telemedicine Platform"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID("pry_hc_telemedicine_001"))
	if err == nil {
		t.Fatal("expected AppendEvent to fail on non-UUID entity_id")
	}
	if !strings.Contains(err.Error(), "invalid entity_id") {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	eventID := uuid.NewString()
	err := pg.PersistEventWithDeliveries(ctx, (events.Event{
		ID:          eventID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"Telemedicine Platform"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID("pry_hc_telemedicine_001"), []string{"control-plane"})
	if err == nil {
		t.Fatal("expected PersistEventWithDeliveries to fail on non-UUID entity_id")
	}
	if !strings.Contains(err.Error(), "invalid entity_id") {
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
	pg := &PostgresStore{DB: db}
	pg.SetEventPayloadValidator(func(eventType string, payload []byte) error {
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
	ctx := context.Background()
	eventID := uuid.NewString()

	err := pg.AppendEvent(ctx, events.Event{
		ID:          eventID,
		Type:        events.EventType("task.completed"),
		SourceAgent: "control-plane",
		Payload:     []byte(`{"ok":"bad"}`),
		CreatedAt:   time.Now().UTC(),
	})
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

func TestPostgresStore_RecordInboundEvent_RejectsPayloadValidatorFailureBeforePersistence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	pg.SetEventPayloadValidator(func(eventType string, payload []byte) error {
		if strings.TrimSpace(eventType) != "platform.inbound_recorded" {
			t.Fatalf("unexpected event type %q", eventType)
		}
		return sql.ErrTxDone
	})
	ctx := context.Background()
	entityID := uuid.NewString()

	inserted, err := pg.RecordInboundEvent(ctx, "provider-evt-1", entityID, "github")
	if err == nil {
		t.Fatal("expected RecordInboundEvent to fail on payload validator rejection")
	}
	if inserted {
		t.Fatal("expected rejected inbound marker not to report insertion")
	}
	if !strings.Contains(err.Error(), "validate event payload") {
		t.Fatalf("RecordInboundEvent payload validator error = %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'platform.inbound_recorded'`).Scan(&count); err != nil {
		t.Fatalf("count inbound event markers: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rejected inbound marker not to persist, count=%d", count)
	}
}

func TestPostgresStore_GetEventReceipt_FallsBackToPersistedReceiptForNonTerminalDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "system.started",
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
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

	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(runtimemanager.ReceiptStatusDeadLetter, "retry_exhausted", 2, "boom"))
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
	if receipt.Status != runtimemanager.ReceiptStatusDeadLetter || receipt.RetryCount != 2 || receipt.Error != "boom" {
		t.Fatalf("receipt = %+v, want dead_letter retry_count=2 error=boom", receipt)
	}
}

func TestPostgresStore_AppendEvent_InheritsParentRunID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"runs": {
			DDL: "CREATE TABLE runs (\n    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    status TEXT NOT NULL DEFAULT 'running'\n);",
		},
		"events": {
			DDL: "CREATE TABLE events (\n    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    run_id UUID REFERENCES runs(run_id),\n    event_name TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope TEXT NOT NULL DEFAULT 'global',\n    payload JSONB NOT NULL DEFAULT '{}'::jsonb,\n    chain_depth INTEGER NOT NULL DEFAULT 0,\n    produced_by TEXT,\n    produced_by_type TEXT NOT NULL DEFAULT 'agent',\n    source_event_id UUID,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(events): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(events): %v", err)
	}

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	parentID := uuid.NewString()
	childID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          parentID,
		RunID:       runID,
		Type:        events.EventType("parent.event"),
		SourceAgent: "root",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.AppendEvent(context.Background(), events.Event{
		ID:            childID,
		Type:          events.EventType("child.event"),
		SourceAgent:   "child",
		ParentEventID: parentID,
		Payload:       []byte(`{}`),
		CreatedAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}

	var gotRunID, gotParent string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(run_id::text, ''), COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, childID).Scan(&gotRunID, &gotParent); err != nil {
		t.Fatalf("query child event: %v", err)
	}
	if gotRunID != runID {
		t.Fatalf("child run_id = %q, want %q", gotRunID, runID)
	}
	if gotParent != parentID {
		t.Fatalf("child source_event_id = %q, want %q", gotParent, parentID)
	}
}

func TestPostgresStore_EventReceiptsTypedIdentitySeparatesReceiptWriters(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, scope,
			payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'test.receipts.typed_identity', $3::uuid, 'flow-1', 'entity',
			'{}'::jsonb, 'test', 'platform', now()
		)
	`, eventID, runID, entityID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
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

	if err := pg.UpsertPipelineReceipt(ctx, eventID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, eventID, "pipeline", runtimemanager.ReceiptStatusProcessed, ""); err != nil {
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

func TestPostgresStore_EnsureSchemaTables_MigratesEventReceiptsTypedSubscriberIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	downgradeEventReceiptsToUntypedSubscriberIdentity(t, ctx, db)

	eventID := uuid.NewString()
	literalNodeEventID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		) VALUES
			($1::uuid, 'test.receipts.migration', $3::uuid, 'flow-1', 'entity', '{}'::jsonb, 'test', 'platform', now()),
			($2::uuid, 'test.receipts.literal_node_pipeline', $3::uuid, 'flow-1', 'entity', '{}'::jsonb, 'test', 'platform', now())
	`, eventID, literalNodeEventID, entityID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, idempotency_key)
		VALUES
			($1::uuid, 'platform', 'pipeline', 'success', '{}'::jsonb, NULL),
			($1::uuid, 'node', 'node:pipeline', 'no_op', '{}'::jsonb, $3),
			($2::uuid, 'node', 'node:pipeline', 'no_op', '{}'::jsonb, $4)
	`, eventID, literalNodeEventID,
		runtimepipeline.SystemNodeReceiptIdempotencyKey("pipeline", eventID),
		runtimepipeline.SystemNodeReceiptIdempotencyKey("node:pipeline", literalNodeEventID)); err != nil {
		t.Fatalf("seed legacy receipts: %v", err)
	}

	if err := pg.EnsureSchemaTables(ctx, eventReceiptsTypedIdentityPlans(t)); err != nil {
		t.Fatalf("EnsureSchemaTables(event_receipts typed identity): %v", err)
	}

	hasTypedKey, err := eventReceiptsTypedSubscriberIdentityKeyExists(ctx, db)
	if err != nil {
		t.Fatalf("eventReceiptsTypedSubscriberIdentityKeyExists: %v", err)
	}
	if !hasTypedKey {
		t.Fatal("typed event_receipts subscriber identity key missing after migration")
	}
	var rowsForPipeline int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_id = 'pipeline'
		  AND subscriber_type IN ('platform', 'node')
	`, eventID).Scan(&rowsForPipeline); err != nil {
		t.Fatalf("count migrated pipeline receipts: %v", err)
	}
	if rowsForPipeline != 2 {
		t.Fatalf("pipeline typed receipt rows = %d, want 2", rowsForPipeline)
	}
	var legacyRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node:pipeline'
	`, eventID).Scan(&legacyRows); err != nil {
		t.Fatalf("count legacy node:pipeline receipts: %v", err)
	}
	if legacyRows != 0 {
		t.Fatalf("legacy node:pipeline receipt rows = %d, want 0", legacyRows)
	}
	var literalRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node:pipeline'
		  AND idempotency_key = $2
	`, literalNodeEventID, runtimepipeline.SystemNodeReceiptIdempotencyKey("node:pipeline", literalNodeEventID)).Scan(&literalRows); err != nil {
		t.Fatalf("count literal node:pipeline receipts: %v", err)
	}
	if literalRows != 1 {
		t.Fatalf("literal node:pipeline receipt rows = %d, want 1", literalRows)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects)
		VALUES ($1::uuid, 'node', 'pipeline', 'success', '{}'::jsonb)
	`, eventID); err == nil {
		t.Fatal("duplicate typed event_receipts identity insert succeeded, want unique violation")
	}
}

func TestPostgresStore_EnsureSchemaTables_FailsClosedOnNodePipelineMigrationConflict(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	downgradeEventReceiptsToUntypedSubscriberIdentity(t, ctx, db)

	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, 'test.receipts.migration_conflict', 'global', '{}'::jsonb, 'test', 'platform', now())
	`, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, idempotency_key)
		VALUES
			($1::uuid, 'node', 'pipeline', 'success', '{}'::jsonb, $2),
			($1::uuid, 'node', 'node:pipeline', 'no_op', '{}'::jsonb, $3)
	`, eventID,
		runtimepipeline.SystemNodeReceiptIdempotencyKey("pipeline", eventID),
		runtimepipeline.SystemNodeReceiptIdempotencyKey("pipeline", eventID)); err != nil {
		t.Fatalf("seed conflicting legacy receipts: %v", err)
	}

	err := pg.EnsureSchemaTables(ctx, eventReceiptsTypedIdentityPlans(t))
	if err == nil {
		t.Fatal("EnsureSchemaTables error = nil, want node:pipeline fail-closed migration error")
	}
	if !strings.Contains(err.Error(), "node:pipeline") {
		t.Fatalf("EnsureSchemaTables error = %v, want node:pipeline conflict", err)
	}
}

func TestPostgresStore_EnsureSchemaTables_FailsClosedOnAmbiguousNodePipelineRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	downgradeEventReceiptsToUntypedSubscriberIdentity(t, ctx, db)

	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, 'test.receipts.ambiguous_node_pipeline', 'global', '{}'::jsonb, 'test', 'platform', now())
	`, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, idempotency_key)
		VALUES ($1::uuid, 'node', 'node:pipeline', 'no_op', '{}'::jsonb, NULL)
	`, eventID); err != nil {
		t.Fatalf("seed ambiguous legacy receipt: %v", err)
	}

	err := pg.EnsureSchemaTables(ctx, eventReceiptsTypedIdentityPlans(t))
	if err == nil {
		t.Fatal("EnsureSchemaTables error = nil, want ambiguous node:pipeline fail-closed migration error")
	}
	if !strings.Contains(err.Error(), "ambiguous node:pipeline") {
		t.Fatalf("EnsureSchemaTables error = %v, want ambiguous node:pipeline", err)
	}
}

func downgradeEventReceiptsToUntypedSubscriberIdentity(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		DO $$
		DECLARE rec RECORD;
		BEGIN
			FOR rec IN
				SELECT c.conname
				FROM pg_constraint c
				WHERE c.conrelid = 'event_receipts'::regclass
				  AND c.contype = 'u'
			LOOP
				EXECUTE format('ALTER TABLE event_receipts DROP CONSTRAINT %I', rec.conname);
			END LOOP;
		END
		$$;
		DROP INDEX IF EXISTS event_receipts_event_subscriber_identity_unique;
		ALTER TABLE event_receipts
			ADD CONSTRAINT event_receipts_event_id_subscriber_id_key UNIQUE (event_id, subscriber_id);
	`); err != nil {
		t.Fatalf("downgrade event_receipts identity: %v", err)
	}
	pg := &PostgresStore{DB: db}
	_, err := pg.BindSchemaCapabilities(ctx)
	if err != nil {
		t.Fatalf("bind downgraded schema capabilities: %v", err)
	}
}

func eventReceiptsTypedIdentityPlans(t *testing.T) []SchemaTableDDL {
	t.Helper()
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"event_receipts": {
			DDL: "CREATE TABLE event_receipts (\n    receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    event_id UUID NOT NULL REFERENCES events(event_id),\n    subscriber_type TEXT NOT NULL CHECK (subscriber_type IN ('node', 'agent', 'platform')),\n    subscriber_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    outcome TEXT NOT NULL CHECK (outcome IN ('success', 'reject', 'discard', 'kill', 'escalate', 'dead_letter', 'terminal_reject', 'waiting', 'fanned_out', 'no_op')),\n    reason_code TEXT,\n    state_before TEXT,\n    state_after TEXT,\n    side_effects JSONB NOT NULL DEFAULT '{}',\n    duration_ms INTEGER,\n    idempotency_key TEXT,\n    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    UNIQUE (event_id, subscriber_type, subscriber_id)\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(event_receipts): %v", err)
	}
	return plans
}

func TestPostgresStore_MarkRunTerminal_PersistsCanonicalLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	completedRunID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, completedRunID); err != nil {
		t.Fatalf("seed completed run: %v", err)
	}
	completedAt := time.Now().UTC().Round(time.Second)
	if err := pg.MarkRunTerminal(ctx, completedRunID, "completed", "", completedAt); err != nil {
		t.Fatalf("MarkRunTerminal(completed): %v", err)
	}

	var (
		completedStatus string
		completedErr    string
		completedEnded  time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(error_summary, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, completedRunID).Scan(&completedStatus, &completedErr, &completedEnded); err != nil {
		t.Fatalf("load completed run: %v", err)
	}
	if completedStatus != "completed" {
		t.Fatalf("completed run status = %q, want completed", completedStatus)
	}
	if completedErr != "" {
		t.Fatalf("completed run error_summary = %q, want empty", completedErr)
	}
	if completedEnded.IsZero() {
		t.Fatal("completed run ended_at not persisted")
	}

	failedRunID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, failedRunID); err != nil {
		t.Fatalf("seed failed run: %v", err)
	}
	failedAt := time.Now().UTC().Round(time.Second)
	if err := pg.MarkRunTerminal(ctx, failedRunID, "failed", "quiescence timeout", failedAt); err != nil {
		t.Fatalf("MarkRunTerminal(failed): %v", err)
	}

	var (
		failedStatus string
		failedErr    string
		failedEnded  time.Time
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(error_summary, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, failedRunID).Scan(&failedStatus, &failedErr, &failedEnded); err != nil {
		t.Fatalf("load failed run: %v", err)
	}
	if failedStatus != "failed" {
		t.Fatalf("failed run status = %q, want failed", failedStatus)
	}
	if failedErr != "quiescence timeout" {
		t.Fatalf("failed run error_summary = %q, want quiescence timeout", failedErr)
	}
	if failedEnded.IsZero() {
		t.Fatal("failed run ended_at not persisted")
	}
}

func TestPostgresStore_ListPendingEventsForAgent_PreservesRunID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	pg.schemaCaps = StoreSchemaCapabilities{
		Events: EventSchemaCapabilities{
			Log:        SchemaFlavorCanonical,
			Deliveries: SchemaFlavorCanonical,
			Receipts:   SchemaFlavorCanonical,
		},
	}
	pg.schemaCapsBound = true
	ctx := context.Background()

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
			last_error TEXT,
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'scoring/scoring.requested', $3::uuid, '{}'::jsonb, 'runtime', 'agent', now()
		)
	`, eventID, runID, entityID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
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
	if got[0].RunID != runID {
		t.Fatalf("pending event run_id = %q, want %q", got[0].RunID, runID)
	}
}

func TestPostgresStore_ListPendingEventsForAgent_UsesTypedEnvelopeMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	const flowInstance = "review/inst-1"
	seedSpecAgent(t, ctx, pg, "analysis-agent", "", "scoring.requested")

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEnvelope(events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
	})
	if err := pg.AppendEvent(ctx, evt); err != nil {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	parentID := uuid.NewString()
	eventProcessed := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}
	eventMissing := events.Event{
		ID:            uuid.NewString(),
		Type:          events.EventType("system.directive"),
		SourceAgent:   "human",
		Payload:       []byte(`{"directive":"x"}`),
		RunID:         runID,
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute),
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running') ON CONFLICT (run_id) DO NOTHING`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          parentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-3 * time.Minute),
	}); err != nil {
		t.Fatalf("append parent event: %v", err)
	}
	if err := pg.AppendEvent(ctx, eventProcessed); err != nil {
		t.Fatalf("append processed event: %v", err)
	}
	if err := pg.AppendEvent(ctx, eventMissing); err != nil {
		t.Fatalf("append missing event: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("upsert parent receipt: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventProcessed.ID, "processed", ""); err != nil {
		t.Fatalf("upsert processed receipt: %v", err)
	}

	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-1*time.Hour), 20)
	if err != nil {
		t.Fatalf("list missing pipeline receipts: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing event, got %d", len(missing))
	}
	if missing[0].Event.ID != eventMissing.ID {
		t.Fatalf("expected missing event id=%s got=%s", eventMissing.ID, missing[0].Event.ID)
	}
	if missing[0].Event.RunID != runID {
		t.Fatalf("missing event run_id = %q, want %q", missing[0].Event.RunID, runID)
	}
	if missing[0].Event.ParentEventID != parentID {
		t.Fatalf("missing event parent_event_id = %q, want %q", missing[0].Event.ParentEventID, parentID)
	}
	if missing[0].ReplayError != "" {
		t.Fatalf("missing event replay_error = %q, want empty", missing[0].ReplayError)
	}
}

func TestPostgresStore_PipelineReceipts_MissingEventsQuery_QuarantinesNoRunIDCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `ALTER TABLE events DROP COLUMN run_id`); err != nil {
		t.Fatalf("drop events.run_id: %v", err)
	}

	pg := &PostgresStore{DB: db}
	eventID := uuid.NewString()
	parentID := uuid.NewString()
	evt := events.Event{
		ID:            eventID,
		Type:          events.EventType("system.directive"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"directive":"x"}`),
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-time.Minute).UTC(),
	}
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("missing events = %d, want 1", len(missing))
	}
	if missing[0].Event.ID != eventID {
		t.Fatalf("missing event id = %q, want %q", missing[0].Event.ID, eventID)
	}
	if missing[0].Event.ParentEventID != parentID {
		t.Fatalf("missing event parent_event_id = %q, want %q", missing[0].Event.ParentEventID, parentID)
	}
	if missing[0].Event.RunID != "" {
		t.Fatalf("missing event run_id = %q, want empty", missing[0].Event.RunID)
	}
	if missing[0].ReplayError != "missing run_id schema capability" {
		t.Fatalf("missing event replay_error = %q, want missing run_id schema capability", missing[0].ReplayError)
	}
}

func TestPostgresStore_BeginEventTx_AppendAndDeliveriesTx(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	tx, err := pg.BeginEventTx(ctx)
	if err != nil {
		t.Fatalf("BeginEventTx: %v", err)
	}

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().UTC(),
	}
	if err := pg.AppendEventTx(ctx, tx, evt); err != nil {
		_ = tx.Rollback()
		t.Fatalf("AppendEventTx: %v", err)
	}
	if err := pg.InsertEventDeliveriesTx(ctx, tx, eventID, []string{"control-plane", "reviewer"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("InsertEventDeliveriesTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit event tx: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if nEvents != 1 || nDeliveries != 2 {
		t.Fatalf("expected event+2 deliveries persisted, got events=%d deliveries=%d", nEvents, nDeliveries)
	}
}

func TestPostgresStore_PersistEventWithDeliveries_SuccessAndRollbackOnFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	eventID := uuid.NewString()
	if err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          eventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"SaaS in Argentina"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{" control-plane ", "", "control-plane"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries success path: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events success: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count deliveries success: %v", err)
	}
	if nEvents != 1 || nDeliveries != 1 {
		t.Fatalf("expected deduped delivery insertion, got events=%d deliveries=%d", nEvents, nDeliveries)
	}

	failedEventID := uuid.NewString()
	err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          failedEventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"fail path"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{"missing-agent"})
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

func TestPostgresStore_Inbound_ValidationAndNotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := s.RecordInboundEvent(ctx, "", "v", "p"); err == nil {
		t.Fatal("expected provider_event_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "", "p"); err == nil {
		t.Fatal("expected entity_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "v", ""); err == nil {
		t.Fatal("expected provider required")
	}
}

func TestPostgresStore_Inbound_ResolveWithoutRunContextAllowsUnambiguousTarget(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state
		)
		VALUES ($1::uuid, $2::uuid, 'root', 'default', 'customer-a', 'Customer A', 'active')
	`, runID, entityID); err != nil {
		t.Fatalf("insert entity state: %v", err)
	}

	target, err := s.ResolveInboundTarget(ctx, "customer-a", "chat")
	if err != nil {
		t.Fatalf("ResolveInboundTarget: %v", err)
	}
	if target.EntityID != entityID || target.EntitySlug != "customer-a" {
		t.Fatalf("target = %+v, want entity=%s slug=customer-a", target, entityID)
	}
}

func TestPostgresStore_Inbound_ResolveWithoutRunContextRejectsAmbiguousTarget(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')`, runA, runB); err != nil {
		t.Fatalf("insert runs: %v", err)
	}
	for _, runID := range []string{runA, runB} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state
			)
			VALUES ($1::uuid, $2::uuid, 'root', 'default', 'customer-a', 'Customer A', 'active')
		`, runID, entityID); err != nil {
			t.Fatalf("insert entity state for run %s: %v", runID, err)
		}
	}

	_, err := s.ResolveInboundTarget(ctx, "customer-a", "chat")
	if err == nil || !strings.Contains(err.Error(), "ambiguous across runs") {
		t.Fatalf("ResolveInboundTarget error = %v, want ambiguous across runs", err)
	}
}

func TestPostgresStore_Inbound_PurgeDeletes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	if ok, err := s.RecordInboundEvent(ctx, "evt-old", entityID, "chat"); err != nil || !ok {
		t.Fatalf("record old ok=%v err=%v", ok, err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE events
		SET created_at = now() - interval '2 days'
		WHERE event_name = 'platform.inbound_recorded'
		  AND payload->>'provider_event_id' = 'evt-old'
	`); err != nil {
		t.Fatalf("age event: %v", err)
	}

	n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-24*time.Hour), 0)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 purged row, got %d", n)
	}
}

func TestPostgresStore_Inbound_RecordAndPurge(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	ok, err := s.RecordInboundEvent(ctx, "evt-1", entityID, "chat")
	if err != nil || !ok {
		t.Fatalf("record inbound ok=%v err=%v", ok, err)
	}
	ok, err = s.RecordInboundEvent(ctx, "evt-1", entityID, "chat")
	if err != nil || ok {
		t.Fatalf("expected duplicate record to be no-op ok=%v err=%v", ok, err)
	}

	if n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-1*time.Hour), 10); err != nil || n != 0 {
		t.Fatalf("purge n=%d err=%v", n, err)
	}
}

func TestPostgresStore_Mailbox_CRUD_Expire_Notify(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

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

	if err := s.DecideMailboxItem(ctx, id, "decided", "approve", "ok"); err != nil {
		t.Fatalf("decide: %v", err)
	}
	if err := s.DecideMailboxItem(ctx, id, "decided", "approve", "again"); err == nil {
		t.Fatal("expected decide on non-pending to fail")
	}
	if err := s.DecideMailboxItem(ctx, uuid.NewString(), "nope", "approve", ""); err == nil {
		t.Fatal("expected invalid status error")
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

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

func TestSchedules_ExactIdentityUsesTaskID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
	ctx := context.Background()
	pg1 := &PostgresStore{DB: db}
	pg2 := &PostgresStore{DB: db}

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
	ctx := context.Background()
	pg1 := &PostgresStore{DB: db}
	pg2 := &PostgresStore{DB: db}

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
	scheduler1 := runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		fired1.Add(1)
		if err := pg1.CompleteScheduleFireExact(context.Background(), sc); err != nil {
			t.Errorf("CompleteScheduleFireExact(pg1): %v", err)
		}
	})
	scheduler2 := runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		fired2.Add(1)
		if err := pg2.CompleteScheduleFireExact(context.Background(), sc); err != nil {
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
	ctx := context.Background()
	pgOwner := &PostgresStore{DB: db}
	pgSuccessor := &PostgresStore{DB: db}

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
	ctx := context.Background()
	pgOwner := &PostgresStore{DB: db}
	pgSuccessor := &PostgresStore{DB: db}

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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "inbound.*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "inbound.test",
		SourceAgent: "inbound",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
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
		if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "error", "boom"); err != nil {
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

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "processed", ""); err != nil {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	broadcastID := uuid.NewString()
	directOtherID := uuid.NewString()
	directSelfID := uuid.NewString()
	noDeliveryID := uuid.NewString()
	for idx, id := range []string{broadcastID, directOtherID, directSelfID, noDeliveryID} {
		if err := pg.AppendEvent(ctx, events.Event{
			ID:          id,
			Type:        "inbound.alert",
			SourceAgent: "runtime",
			Payload:     []byte(`{}`),
			CreatedAt:   time.Now().Add(time.Duration(-3+idx) * time.Minute),
		}); err != nil {
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
		gotSet[strings.TrimSpace(evt.ID)] = struct{}{}
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

func TestPendingEventQueries_PreserveParentCorrelation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	seedSpecAgent(t, ctx, pg, "a1", "", "inbound.*")

	parentID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          parentID,
		Type:        "inbound.root",
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}

	childID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            childID,
		Type:          "inbound.child",
		SourceAgent:   "runtime",
		Payload:       []byte(`{}`),
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute),
	}); err != nil {
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
	if got := strings.TrimSpace(direct[0].ParentEventID); got != parentID {
		t.Fatalf("direct pending parent_event_id = %q, want %q", got, parentID)
	}

	subscribed, err := pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	var child events.Event
	found := false
	for _, evt := range subscribed {
		if strings.TrimSpace(evt.ID) == childID {
			child = evt
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected child event %s in subscribed pending set", childID)
	}
	if got := strings.TrimSpace(child.ParentEventID); got != parentID {
		t.Fatalf("subscribed pending parent_event_id = %q, want %q", got, parentID)
	}
}

func TestManagerStore_EventReceiptBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "system.started",
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, "", "a1", "processed", ""); err == nil {
		t.Fatal("expected UpsertEventReceipt empty event to fail")
	}

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "", ""); err == nil {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "system.started",
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.MarkEventDeliveryInProgress(ctx, "", "a1", ""); err == nil {
		t.Fatal("expected empty eventID to fail")
	}
	if err := pg.MarkEventDeliveryInProgress(ctx, uuid.NewString(), "", ""); err == nil {
		t.Fatal("expected empty agentID to fail")
	}
}

func TestManagerStore_LoadRoutingRules_AndDeactivateValidation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := runtimecorrelation.WithRunID(context.Background(), specEntityStateRunID)

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

	if err := pg.MarkAgentTerminated(ctx, " "); err == nil {
		t.Fatalf("expected agent_id required")
	}

	if err := pg.CancelSchedule(ctx, "sub", "timer.recurring_digest"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	_ = time.Second
}

func TestManagerStore_LoadRoutingRules_DoesNotJoinRunScopedEntityState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

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

func TestManagerStore_EnsureEntitySchema(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := runtimecorrelation.WithRunID(context.Background(), specEntityStateRunID)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "entity-schema-flow", "vslug", "TestCo", "operating")
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}

}

func TestManagerStore_RoutingRules_DeactivateAndBootstrapVersion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := runtimecorrelation.WithRunID(context.Background(), specEntityStateRunID)

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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "a1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages: []llm.Message{
			{Role: "user", Content: "reach me at a@example.com"},
		},
		TurnCount: 2,
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global")
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.AgentID != "a1" || rec.Mode != "session" || rec.Status != "active" || rec.TurnCount != 2 {
		t.Fatalf("unexpected conversation: %+v", rec)
	}
	if len(rec.Messages) != 1 || strings.Contains(rec.Messages[0].Content, "a@example.com") || !strings.Contains(rec.Messages[0].Content, "[EMAIL]") {
		t.Fatalf("expected redacted email, got %#v", rec.Messages)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{AgentID: "a1", RuntimeMode: "session", SessionID: uuid.NewString()}); err == nil {
		t.Fatalf("expected missing session row error")
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "session",
		SessionID:      sessionID,
		TaskID:         uuid.NewString(),
		RequestPayload: []byte(`{"x":1}`),
		ResponseRaw:    []byte(`{"y":2}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var availableToolsJSON, toolCallsJSON, emittedEventsJSON, mcpServersJSON, mcpToolsListedJSON, mcpToolsVisibleJSON string
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(available_tools::text, '[]'),
			COALESCE(tool_calls::text, '[]'),
			COALESCE(emitted_events::text, '[]'),
			COALESCE(mcp_servers::text, '{}'),
			COALESCE(mcp_tools_listed::text, '[]'),
			COALESCE(mcp_tools_visible::text, '[]')
		FROM agent_turns
		WHERE agent_id = 'a1' AND session_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, sessionID).Scan(&availableToolsJSON, &toolCallsJSON, &emittedEventsJSON, &mcpServersJSON, &mcpToolsListedJSON, &mcpToolsVisibleJSON); err != nil {
		t.Fatalf("load agent_turns row: %v", err)
	}
	if availableToolsJSON != "[]" || toolCallsJSON != "[]" || emittedEventsJSON != "[]" || mcpServersJSON != "{}" || mcpToolsListedJSON != "[]" || mcpToolsVisibleJSON != "[]" {
		t.Fatalf("expected empty structured telemetry defaults, got tools=%s calls=%s emitted=%s mcp_servers=%s mcp_listed=%s mcp_visible=%s", availableToolsJSON, toolCallsJSON, emittedEventsJSON, mcpServersJSON, mcpToolsListedJSON, mcpToolsVisibleJSON)
	}
}

func TestManagerStore_ConversationPersistence_SessionPerEntityUsesActorContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	baseCtx := context.Background()
	resetAgentSessionsSpecTable(t, baseCtx, pg)
	entityID := uuid.NewString()
	seedSpecAgent(t, baseCtx, pg, "entity-agent", entityID, "")

	ctx := runtimeactors.WithActor(baseCtx, runtimeactors.AgentConfig{
		ID:       "entity-agent",
		FlowPath: "review/inst-1",
		EntityID: entityID,
	})
	sessionID := acquireLiveTestSession(t, ctx, db, "entity-agent", runtimesessions.RuntimeModeSessionPerEntity, runtimesessions.SessionScopeEntity, entityID)

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "entity-agent",
		Mode:         "session_per_entity",
		SessionScope: "entity",
		ScopeKey:     entityID,
		Messages:     []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session_per_entity): %v", err)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, "entity-agent", "session_per_entity", "entity", entityID)
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.SessionScope != "entity" || rec.ScopeKey != entityID {
		t.Fatalf("unexpected conversation record: %+v", rec)
	}

	var flowInstance string
	if err := db.QueryRowContext(baseCtx, `
		SELECT COALESCE(flow_instance, '')
		FROM agent_sessions
		WHERE agent_id = 'entity-agent' AND scope_key = $1
	`, entityID).Scan(&flowInstance); err != nil {
		t.Fatalf("load entity-scoped session row: %v", err)
	}
	if flowInstance != "review/inst-1" {
		t.Fatalf("flow_instance = %q, want review/inst-1", flowInstance)
	}
}

func TestManagerStore_AppendAgentTurn_PersistsObservedToolCalls(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "a1",
		RuntimeMode: "session",
		SessionID:   sessionID,
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    uuid.NewString(),
		AgentID:      "a1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages:     []llm.Message{{Role: "assistant", Content: "hello"}},
		TurnCount:    1,
		Status:       "active",
	})
	if err == nil {
		t.Fatal("expected live conversation persistence without a live session row to fail")
	}
	if !strings.Contains(err.Error(), "no active live session row found") {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	registry := runtimesessions.NewPostgresRegistry(db, 30*time.Second)
	lease, err := registry.Acquire(ctx, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "worker-1", "global")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rotated, err := registry.Rotate(ctx, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "worker-1", runtimesessions.RotationMetadata{
		CheckpointSummary: "rotation_reason=session not found",
		RetryReason:       "session not found",
	}, "global")
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if rotated.RetriesFromSessionID != lease.SessionID {
		t.Fatalf("RetriesFromSessionID = %q, want %q", rotated.RetriesFromSessionID, lease.SessionID)
	}

	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global")
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
		WHERE agent_id = 'a1' AND scope_key = 'global' AND runtime_mode = 'session' AND status = 'active'
	`).Scan(&gotReason, &gotFrom); err != nil {
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
		sameScope            bool
		sameScopeKey         bool
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
			old.scope = new.scope,
			old.scope_key = new.scope_key
		FROM agent_sessions old
		JOIN agent_sessions new ON new.session_id = $2::uuid
		WHERE old.session_id = $1::uuid
	`, lease.SessionID, rotated.SessionID).Scan(&sameAgent, &sameScope, &sameScopeKey); err != nil {
		t.Fatalf("compare rotated lineage scope: %v", err)
	}
	if !sameAgent || !sameScope || !sameScopeKey {
		t.Fatalf("rotated lineage mismatch: sameAgent=%v sameScope=%v sameScopeKey=%v", sameAgent, sameScope, sameScopeKey)
	}
}

func TestManagerStore_LoadActiveConversationFailsOnMalformedCanonicalRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = '{"summary":123}'::jsonb
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("seed malformed runtime_state: %v", err)
	}

	if _, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global"); err == nil || ok {
		t.Fatalf("expected malformed canonical runtime_state to fail, ok=%v err=%v", ok, err)
	} else if !strings.Contains(err.Error(), "decode conversation runtime_state") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerStore_LoadActiveConversationFailsOnMalformedCanonicalWatchdogRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if _, err := db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = '{"summary":"ok","watchdog":{"state":"mystery","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","last_output_at":"2026-04-10T12:00:00Z","recorded_at":"2026-04-10T12:00:30Z"}}'::jsonb
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("seed malformed watchdog runtime_state: %v", err)
	}

	if _, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global"); err == nil || ok {
		t.Fatalf("expected malformed canonical watchdog runtime_state to fail, ok=%v err=%v", ok, err)
	} else if !strings.Contains(err.Error(), "decode conversation runtime_state") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerStore_UpdateLiveSessionWatchdog_RoundTripsThroughLoadActiveConversation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID:    sessionID,
		AgentID:      "a1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
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

	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global")
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID:    sessionID,
		AgentID:      "a1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
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

	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global")
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.Watchdog != nil {
		t.Fatalf("malformed watchdog write poisoned runtime_state: %+v", rec.Watchdog)
	}
}

func TestManagerStore_UpdateLiveSessionWatchdog_PreservesCanonicalSummary(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "a1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
		Messages:     []runtimellm.Message{{Role: "assistant", Content: "still working"}},
		Summary:      "still working",
		TurnCount:    2,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}

	if err := pg.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID:    sessionID,
		AgentID:      "a1",
		SessionScope: "global",
		ScopeKey:     "global",
		Mode:         "session",
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

	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "session", "global", "global")
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

func TestManagerStore_Conversations_AndAgentTurns_PersistRunIDWhenColumnsExist(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"runs": {
			DDL: "CREATE TABLE runs (\n    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    status TEXT NOT NULL DEFAULT 'running'\n);",
		},
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    run_id UUID REFERENCES runs(run_id),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    UNIQUE (agent_id, scope_key)\n);",
		},
		"agent_turns": {
			DDL: "CREATE TABLE agent_turns (\n    turn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    run_id UUID REFERENCES runs(run_id),\n    agent_id TEXT NOT NULL,\n    session_id UUID NOT NULL,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    scope_key TEXT,\n    entity_id UUID,\n    trigger_event_id UUID,\n    trigger_event_type TEXT,\n    task_id TEXT,\n    available_tools JSONB NOT NULL DEFAULT '[]',\n    tool_calls JSONB NOT NULL DEFAULT '[]',\n    emitted_events JSONB NOT NULL DEFAULT '[]',\n    mcp_servers JSONB NOT NULL DEFAULT '{}',\n    mcp_tools_listed JSONB NOT NULL DEFAULT '[]',\n    mcp_tools_visible JSONB NOT NULL DEFAULT '[]',\n    request_payload JSONB,\n    response_payload JSONB,\n    parse_ok BOOLEAN NOT NULL DEFAULT FALSE,\n    latency_ms INTEGER NOT NULL DEFAULT 0,\n    retry_count INTEGER NOT NULL DEFAULT 0,\n    error TEXT,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions): %v", err)
	}
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		RunID:     runID,
		Mode:      "task",
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RunID:          runID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	var gotSessionRunID, gotTurnRunID string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(run_id::text, '') FROM agent_conversation_audits WHERE session_id = $1::uuid`, sessionID).Scan(&gotSessionRunID); err != nil {
		t.Fatalf("load audit run_id: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(run_id::text, '') FROM agent_turns WHERE session_id = $1::uuid ORDER BY created_at DESC LIMIT 1`, sessionID).Scan(&gotTurnRunID); err != nil {
		t.Fatalf("load turn run_id: %v", err)
	}
	if gotSessionRunID != runID {
		t.Fatalf("session run_id = %q, want %q", gotSessionRunID, runID)
	}
	if gotTurnRunID != runID {
		t.Fatalf("turn run_id = %q, want %q", gotTurnRunID, runID)
	}
}

func TestManagerStore_AppendAgentTurn_PersistsTurnBlocksWhenColumnExists(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `ALTER TABLE agent_turns ADD COLUMN IF NOT EXISTS turn_blocks JSONB NOT NULL DEFAULT '[]'::jsonb`); err != nil {
		t.Fatalf("add turn_blocks column: %v", err)
	}
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		ScopeKey:  "global",
		Mode:      "task",
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "a1",
		RuntimeMode: "task",
		SessionID:   sessionID,
		TurnBlocks: []runtimellm.TurnBlock{
			{Kind: "dispatch", Title: "scoring/vertical.marginal"},
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

func TestManagerStore_AppendAgentTurn_CanonicalizesTurnBlocksThroughSingleStoreAdapter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `ALTER TABLE agent_turns ADD COLUMN IF NOT EXISTS turn_blocks JSONB NOT NULL DEFAULT '[]'::jsonb`); err != nil {
		t.Fatalf("add turn_blocks column: %v", err)
	}
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		ScopeKey:  "global",
		Mode:      "task",
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:          "a1",
		RuntimeMode:      "task",
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "a1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages:     []llm.Message{{Role: "assistant", Content: "done"}},
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session): %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "session",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"result":"done"}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
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
		WHERE agent_id = 'a1' AND session_id = $1::uuid AND runtime_mode = 'session'
	`, sessionID).Scan(&turnCount); err != nil {
		t.Fatalf("count agent_turns(session): %v", err)
	}
	if turnCount != 1 {
		t.Fatalf("expected one session-mode agent_turn row, got %d", turnCount)
	}
}

func TestManagerStore_AppendAgentTurn_PreservesLiveSessionRetryLineageRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

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

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "session",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"result":"done"}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
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

func TestManagerStore_AppendAgentTurn_RollsBackTaskAuditAndTurnRowWhenTurnInsertFails(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	installFailAgentTurnInsertTrigger(t, ctx, db)

	sessionID := uuid.NewString()
	err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
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

func TestManagerStore_StatelessConversationPersistsAuditRowWithoutReload(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "user", Content: "one-shot"},
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	if rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "task", "", ""); err != nil || ok || rec.AgentID != "" {
		t.Fatalf("LoadActiveConversation(task) ok=%v err=%v rec=%+v", ok, err, rec)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_conversation_audits WHERE agent_id = 'a1' AND runtime_mode = 'task'`).Scan(&count); err != nil {
		t.Fatalf("count task audits: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one persisted task audit row, got %d", count)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task): %v", err)
	}

	var parseOK bool
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE((runtime_state->'last_turn'->>'parse_ok')::boolean, false)
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&parseOK); err != nil {
		t.Fatalf("load task runtime_state: %v", err)
	}
	if !parseOK {
		t.Fatal("expected task-mode last_turn telemetry to be persisted")
	}

	var turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns
		WHERE agent_id = 'a1' AND session_id = $1::uuid AND runtime_mode = 'task'
	`, sessionID).Scan(&turnCount); err != nil {
		t.Fatalf("count agent_turns(task): %v", err)
	}
	if turnCount != 1 {
		t.Fatalf("expected one task-mode agent_turn row, got %d", turnCount)
	}
}

func TestManagerStore_StatelessConversationFailsClosedWithoutAuditCapabilityAndDoesNotCreateTable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	resetAgentSessionsSpecTable(t, ctx, pg)
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_conversation_audits CASCADE`); err != nil {
		t.Fatalf("drop agent_conversation_audits: %v", err)
	}
	pg = &PostgresStore{DB: db}
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: uuid.NewString(),
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "user", Content: "one-shot"},
		},
		TurnCount: 1,
		Status:    "active",
	})
	if err == nil || !strings.Contains(err.Error(), "store: agent_conversation_audits schema is unavailable") {
		t.Fatalf("UpsertConversation(task) error = %v, want unavailable audit capability failure", err)
	}

	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.agent_conversation_audits') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check agent_conversation_audits existence: %v", err)
	}
	if exists {
		t.Fatal("expected task conversation hot path not to recreate agent_conversation_audits")
	}
}

func TestManagerStore_TaskAppendTurnFailsClosedWithoutAuditCapabilityAndDoesNotCreateTable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	resetAgentSessionsSpecTable(t, ctx, pg)
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_conversation_audits CASCADE`); err != nil {
		t.Fatalf("drop agent_conversation_audits: %v", err)
	}
	pg = &PostgresStore{DB: db}
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      uuid.NewString(),
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "store: agent_conversation_audits schema is unavailable") {
		t.Fatalf("AppendAgentTurn(task) error = %v, want unavailable audit capability failure", err)
	}

	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.agent_conversation_audits') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check agent_conversation_audits existence: %v", err)
	}
	if exists {
		t.Fatal("expected task turn hot path not to recreate agent_conversation_audits")
	}
}

func TestManagerStore_SessionConversationDoesNotPersistAuditRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	if err := pg.ensureConversationAuditTable(ctx); err != nil {
		t.Fatalf("ensureConversationAuditTable: %v", err)
	}
	seedSpecAgent(t, ctx, pg, "a1", "", "")
	sessionID := acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      "a1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages: []llm.Message{
			{Role: "user", Content: "hello"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(session): %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "session",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"session"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(session): %v", err)
	}

	var auditCount, turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_conversation_audits WHERE agent_id = 'a1'),
			(SELECT COUNT(*) FROM agent_turns WHERE agent_id = 'a1' AND session_id = $1::uuid AND runtime_mode = 'session')
	`, sessionID).Scan(&auditCount, &turnCount); err != nil {
		t.Fatalf("count persisted rows: %v", err)
	}
	if auditCount != 0 {
		t.Fatalf("expected no audit rows for session-mode persistence, got %d", auditCount)
	}
	if turnCount != 1 {
		t.Fatalf("expected one session-mode turn row, got %d", turnCount)
	}
}

func TestManagerStore_TaskConversationUpsertIsIdempotentBySessionID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	rec := runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "user", Content: "one-shot"},
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}
	if err := pg.UpsertConversation(ctx, rec); err != nil {
		t.Fatalf("UpsertConversation(task first): %v", err)
	}

	rec.Messages = append(rec.Messages, llm.Message{Role: "assistant", Content: "follow-up"})
	rec.TurnCount = 2
	if err := pg.UpsertConversation(ctx, rec); err != nil {
		t.Fatalf("UpsertConversation(task second): %v", err)
	}

	var count, turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(turn_count), 0)
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&count, &turnCount); err != nil {
		t.Fatalf("load task audit row: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one task audit row after repeated upserts, got %d", count)
	}
	if turnCount != 2 {
		t.Fatalf("expected turn_count to update on repeated task upsert, got %d", turnCount)
	}
}

func TestManagerStore_AppendAgentTurn_TaskCreatesAuditRowIfMissing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task missing row): %v", err)
	}

	var scope, scopeKey string
	var parseOK bool
	if err := db.QueryRowContext(ctx, `
		SELECT scope, COALESCE(scope_key, ''), COALESCE((runtime_state->'last_turn'->>'parse_ok')::boolean, false)
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&scope, &scopeKey, &parseOK); err != nil {
		t.Fatalf("load synthesized task audit row: %v", err)
	}
	if scope != "global" {
		t.Fatalf("expected synthesized task audit scope=global, got %q", scope)
	}
	if scopeKey != "" {
		t.Fatalf("expected synthesized task scope_key to stay empty, got %q", scopeKey)
	}
	if !parseOK {
		t.Fatal("expected synthesized task audit row to record last_turn telemetry")
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task after append): %v", err)
	}

	var count, turns int
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			(SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid AND runtime_mode = 'task')
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&count, &turns); err != nil {
		t.Fatalf("count synthesized task persistence: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one synthesized task audit row, got %d", count)
	}
	if turns != 1 {
		t.Fatalf("expected one task agent_turn row after synthesized append, got %d", turns)
	}
}

func TestManagerStore_AppendAgentTurn_TaskDoesNotAdoptLegacySessionRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			$1::uuid,
			'a1',
			'',
			'global',
			'[{"role":"assistant","content":"legacy task"}]'::jsonb,
			3,
			'task',
			'{"summary":"legacy task"}'::jsonb,
			'active',
			now() - interval '5 minutes',
			now() - interval '5 minutes'
		)
	`, sessionID); err != nil {
		t.Fatalf("seed legacy task session row: %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task legacy row): %v", err)
	}

	var turnCount int
	var conversation string
	var summary string
	if err := db.QueryRowContext(ctx, `
		SELECT
			turn_count,
			COALESCE(conversation::text, '[]'),
			COALESCE(runtime_state->>'summary', '')
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&turnCount, &conversation, &summary); err != nil {
		t.Fatalf("load synthesized task audit row: %v", err)
	}
	if turnCount != 0 {
		t.Fatalf("expected synthesized audit row to start with canonical turn_count 0, got %d", turnCount)
	}
	if conversation != "[]" {
		t.Fatalf("expected synthesized audit conversation to stay canonical empty array, got %s", conversation)
	}
	if summary != "" {
		t.Fatalf("expected synthesized audit summary to stay empty, got %q", summary)
	}
}

func TestPostgresStore_EnsureConversationAuditTable_DoesNotMigrateLegacyTaskSessionRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_conversation_audits CASCADE`); err != nil {
		t.Fatalf("drop agent_conversation_audits: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'global',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'task',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create legacy agent_sessions: %v", err)
	}

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			$1::uuid,
			'a1',
			'',
			'global',
			'[{"role":"assistant","content":"legacy task"}]'::jsonb,
			1,
			'task',
			'{"summary":"legacy task"}'::jsonb,
			'active',
			now() - interval '5 minutes',
			now() - interval '5 minutes'
		)
	`, sessionID); err != nil {
		t.Fatalf("seed legacy task session row: %v", err)
	}

	if err := pg.ensureConversationAuditTable(ctx); err != nil {
		t.Fatalf("ensureConversationAuditTable: %v", err)
	}

	var legacyCount, auditCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_sessions WHERE session_id = $1::uuid AND runtime_mode = 'task'),
			(SELECT COUNT(*) FROM agent_conversation_audits WHERE session_id = $1::uuid)
	`, sessionID).Scan(&legacyCount, &auditCount); err != nil {
		t.Fatalf("count task conversation rows after ensureConversationAuditTable: %v", err)
	}
	if legacyCount != 1 {
		t.Fatalf("expected legacy task session row to remain untouched, got %d", legacyCount)
	}
	if auditCount != 0 {
		t.Fatalf("expected ensureConversationAuditTable to stop migrating legacy task rows, got %d audit rows", auditCount)
	}
}

func TestPostgresStore_EnsureSchemaTables_NeutralizesLegacyTaskSessionRowsBeforeLiveSessionAcquire(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_conversation_audits CASCADE`); err != nil {
		t.Fatalf("drop agent_conversation_audits: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_turns CASCADE`); err != nil {
		t.Fatalf("drop agent_turns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'global',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'task',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			lease_holder TEXT,
			lease_expires_at TIMESTAMPTZ,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (agent_id, scope_key)
		)
	`); err != nil {
		t.Fatalf("create legacy agent_sessions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agent_turns (
			turn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			agent_id TEXT NOT NULL,
			session_id UUID NOT NULL,
			runtime_mode TEXT NOT NULL DEFAULT 'task',
			scope_key TEXT,
			entity_id UUID,
			trigger_event_id UUID,
			trigger_event_type TEXT,
			task_id TEXT,
			available_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
			tool_calls JSONB NOT NULL DEFAULT '[]'::jsonb,
			emitted_events JSONB NOT NULL DEFAULT '[]'::jsonb,
			mcp_servers JSONB NOT NULL DEFAULT '{}'::jsonb,
			mcp_tools_listed JSONB NOT NULL DEFAULT '[]'::jsonb,
			mcp_tools_visible JSONB NOT NULL DEFAULT '[]'::jsonb,
			request_payload JSONB,
			response_payload JSONB,
			parse_ok BOOLEAN NOT NULL DEFAULT FALSE,
			latency_ms INTEGER NOT NULL DEFAULT 0,
			retry_count INTEGER NOT NULL DEFAULT 0,
			error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create legacy agent_turns: %v", err)
	}

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, scope_key, scope, runtime_mode, status,
			lease_holder, lease_expires_at, created_at, updated_at
		) VALUES (
			$1::uuid,
			'a1',
			'global',
			'global',
			'task',
			'active',
			'legacy-worker',
			now() + interval '1 minute',
			now() - interval '5 minutes',
			now() - interval '2 minutes'
		)
	`, sessionID); err != nil {
		t.Fatalf("seed legacy task session row: %v", err)
	}

	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'global',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'session',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
		"agent_turns": {
			DDL: "CREATE TABLE agent_turns (\n    turn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    session_id UUID NOT NULL,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    scope_key TEXT,\n    entity_id UUID,\n    trigger_event_id UUID,\n    trigger_event_type TEXT,\n    task_id TEXT,\n    available_tools JSONB NOT NULL DEFAULT '[]',\n    tool_calls JSONB NOT NULL DEFAULT '[]',\n    emitted_events JSONB NOT NULL DEFAULT '[]',\n    mcp_servers JSONB NOT NULL DEFAULT '{}',\n    mcp_tools_listed JSONB NOT NULL DEFAULT '[]',\n    mcp_tools_visible JSONB NOT NULL DEFAULT '[]',\n    request_payload JSONB,\n    response_payload JSONB,\n    parse_ok BOOLEAN NOT NULL DEFAULT FALSE,\n    latency_ms INTEGER NOT NULL DEFAULT 0,\n    retry_count INTEGER NOT NULL DEFAULT 0,\n    error TEXT,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}

	var (
		status       string
		reason       string
		terminatedAt time.Time
		leaseHolder  sql.NullString
		leaseExpires sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(status, ''),
			COALESCE(termination_reason, ''),
			terminated_at,
			lease_holder,
			lease_expires_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &reason, &terminatedAt, &leaseHolder, &leaseExpires); err != nil {
		t.Fatalf("load neutralized legacy task session row: %v", err)
	}
	if status != "terminated" {
		t.Fatalf("status = %q, want terminated", status)
	}
	if reason != "orphaned" {
		t.Fatalf("termination_reason = %q, want orphaned", reason)
	}
	if terminatedAt.IsZero() {
		t.Fatal("terminated_at is zero")
	}
	if leaseHolder.Valid || leaseExpires.Valid {
		t.Fatalf("expected neutralized legacy task session row lease to be cleared, got holder=%q expires=%v", leaseHolder.String, leaseExpires)
	}

	seedSpecAgent(t, ctx, pg, "a1", "", "")
	acquireLiveTestSession(t, ctx, db, "a1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global")

	var activeSessionCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE agent_id = 'a1'
		  AND scope_key = 'global'
		  AND runtime_mode = 'session'
		  AND status = 'active'
	`).Scan(&activeSessionCount); err != nil {
		t.Fatalf("count live session rows after acquire: %v", err)
	}
	if activeSessionCount != 1 {
		t.Fatalf("expected one active live session row after acquire, got %d", activeSessionCount)
	}
}

func TestManagerStore_AppendAgentTurn_FailsOnMalformedCanonicalRuntimeLogTurnBlock(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "a1",
		RuntimeMode: "task",
		SessionID:   sessionID,
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:     "a1",
		RuntimeMode: "task",
		SessionID:   sessionID,
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

func TestManagerStore_AppendAgentTurn_TaskReactivatesExistingInactiveAuditRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE agent_conversation_audits
		SET status = 'terminated', updated_at = now() - interval '1 minute'
		WHERE session_id = $1::uuid
	`, sessionID); err != nil {
		t.Fatalf("terminate task audit row: %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task inactive audit): %v", err)
	}

	var status string
	var parseOK bool
	var turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			status,
			COALESCE((runtime_state->'last_turn'->>'parse_ok')::boolean, false),
			(SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid AND runtime_mode = 'task')
		FROM agent_conversation_audits
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &parseOK, &turnCount); err != nil {
		t.Fatalf("load reactivated task audit row: %v", err)
	}
	if status != "active" {
		t.Fatalf("expected inactive task audit row to be reactivated, got %q", status)
	}
	if !parseOK {
		t.Fatal("expected reactivated task audit row to record last_turn telemetry")
	}
	if turnCount != 1 {
		t.Fatalf("expected one task turn row after reactivation, got %d", turnCount)
	}
}

func TestManagerStore_TaskConversationDoesNotPersistLiveSessionRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task): %v", err)
	}

	var sessionCount, auditCount, turnCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'a1'),
			(SELECT COUNT(*) FROM agent_conversation_audits WHERE agent_id = 'a1' AND session_id = $1::uuid AND runtime_mode = 'task'),
			(SELECT COUNT(*) FROM agent_turns WHERE agent_id = 'a1' AND session_id = $1::uuid AND runtime_mode = 'task')
	`, sessionID).Scan(&sessionCount, &auditCount, &turnCount); err != nil {
		t.Fatalf("count persisted rows: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected no live session rows for task-mode persistence, got %d", sessionCount)
	}
	if auditCount != 1 {
		t.Fatalf("expected one task audit row, got %d", auditCount)
	}
	if turnCount != 1 {
		t.Fatalf("expected one task turn row, got %d", turnCount)
	}
}

func TestManagerStore_UpsertAgent_MergesSubscriptions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "a1",
			Type:          "sonnet",
			Role:          "a1",
			Mode:          "global",
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

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{}}); err == nil {
		t.Fatalf("expected agent id required error")
	}
}

func TestManagerStore_UpsertAgent_PersistsCanonicalControlPlaneOwnership(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:               "agent-canonical-1",
			Type:             "review-worker",
			Role:             "reviewer",
			Mode:             "review",
			ModelTier:        "sonnet",
			LLMBackend:       "cli_test",
			ConversationMode: "session_per_entity",
			SessionScope:     "entity",
			MaxTurnsPerTask:  7,
			Subscriptions:    []string{"review.ready"},
			EmitEvents:       []string{"review.completed"},
			Tools:            []string{"agent_message"},
			Permissions:      []string{"agent_message"},
			NativeTools:      runtimeactors.NativeToolConfig{FileIO: true},
			WorkspaceClass:   "shared_flow",
			ManagerFallback:  "control-plane",
			FlowPath:         "review/inst-1",
			EntityID:         entityID,
			ParentAgent:      "manager-1",
			Config: json.RawMessage(`{
				"system_prompt":"x",
				"type":"wrong-type",
				"conversation_mode":"task",
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
	if got.ModelTier != "sonnet" {
		t.Fatalf("model_tier = %q, want sonnet", got.ModelTier)
	}
	if got.LLMBackend != "cli_test" {
		t.Fatalf("llm_backend = %q, want cli_test", got.LLMBackend)
	}
	if got.ConversationMode != "session_per_entity" {
		t.Fatalf("conversation_mode = %q, want session_per_entity", got.ConversationMode)
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
		ID:               "agent-default-backend",
		Role:             "reviewer",
		ModelTier:        "sonnet",
		ConversationMode: "task",
	}, "")
	if err != nil {
		t.Fatalf("projectPersistedAgentConfig: %v", err)
	}
	if projection.LLMBackend != "api" {
		t.Fatalf("llm_backend = %q, want api default profile", projection.LLMBackend)
	}

	_, err = projectPersistedAgentConfig(runtimeactors.AgentConfig{
		ID:               "agent-bad-backend",
		Role:             "reviewer",
		ModelTier:        "sonnet",
		LLMBackend:       "openai",
		ConversationMode: "task",
	}, "")
	if err == nil || !strings.Contains(err.Error(), "invalid llm_backend") {
		t.Fatalf("projectPersistedAgentConfig error = %v, want invalid llm_backend", err)
	}
}

func TestManagerStore_UpsertAgent_RejectsInvalidConversationModeAtAdmission(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:               "agent-invalid-mode",
			Role:             "reviewer",
			Type:             "review-worker",
			ConversationMode: "nonsense",
		},
		Status: "active",
	})
	if err == nil {
		t.Fatal("expected invalid conversation mode to fail")
	}
	if !strings.Contains(err.Error(), `invalid agent session scope: invalid conversation mode "nonsense"`) {
		t.Fatalf("UpsertAgent error = %q", err.Error())
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE agent_id = 'agent-invalid-mode'`).Scan(&count); err != nil {
		t.Fatalf("count invalid-mode agent rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("persisted invalid-mode agent rows = %d, want 0", count)
	}
}

func TestManagerStore_UpsertAgent_FailsClosedWithoutHotPathSchemaRepair(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agents CASCADE`); err != nil {
		t.Fatalf("drop agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agents (
			id TEXT PRIMARY KEY,
			type TEXT,
			role TEXT NOT NULL,
			mode TEXT NOT NULL,
			entity_id UUID,
			parent_agent_id TEXT,
			status TEXT,
			coordinator_id TEXT,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			budget_envelope JSONB NOT NULL DEFAULT '{}'::jsonb,
			hired_by TEXT,
			template_version TEXT,
			started_at TIMESTAMPTZ,
			last_active_at TIMESTAMPTZ
		)
	`); err != nil {
		t.Fatalf("create legacy agents: %v", err)
	}

	err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:               "legacy-agent",
			Role:             "reviewer",
			Type:             "review-worker",
			ConversationMode: "task",
		},
		Status: "active",
	})
	if err == nil || !strings.Contains(err.Error(), "store: agents schema is unsupported by the explicit capability boundary") {
		t.Fatalf("UpsertAgent error = %v, want unsupported canonical schema failure", err)
	}

	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'agents'
			  AND column_name = 'runtime_descriptor'
		)
	`).Scan(&exists); err != nil {
		t.Fatalf("check agents.runtime_descriptor existence: %v", err)
	}
	if exists {
		t.Fatal("expected UpsertAgent not to backfill agents.runtime_descriptor on the hot path")
	}
}

func TestManagerStore_LoadAgentsSpec_FailsClosedWhenOpaqueConfigContainsRuntimeKeys(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.ensureSchemaCompatibilityColumns(ctx); err != nil {
		t.Fatalf("ensureSchemaCompatibilityColumns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model_tier, llm_backend, conversation_mode,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status
		) VALUES (
			$1, '', 'reviewer', 'sonnet', 'api', 'task',
			NULL, NULL, $2::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
			$3::jsonb, 'active'
		)
	`, "agent-invalid-config", `{"system_prompt":"x","subscriptions":["wrong"]}`, `{"type":"review-worker","mode":"review"}`); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}

	_, err := pg.loadAgentsSpec(ctx)
	if err == nil || !strings.Contains(err.Error(), "config contains runtime-owned keys: subscriptions") {
		t.Fatalf("loadAgentsSpec error = %v, want runtime-owned config key failure", err)
	}
}

func TestPostgresStore_EnsureSchemaCompatibilityColumnsAddsMultiBundleRunColumnsAsLegacy(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS runs CASCADE`); err != nil {
		t.Fatalf("drop canonical runs table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE runs (
			run_id UUID PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'running',
			bundle_fingerprint TEXT
		)
	`); err != nil {
		t.Fatalf("create legacy runs table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint)
		VALUES ($1::uuid, 'running', 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
	`, runID); err != nil {
		t.Fatalf("seed legacy run row: %v", err)
	}

	if err := pg.ensureSchemaCompatibilityColumns(ctx); err != nil {
		t.Fatalf("ensureSchemaCompatibilityColumns: %v", err)
	}

	var bundleHash sql.NullString
	var bundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&bundleHash, &bundleSource); err != nil {
		t.Fatalf("load migrated run row: %v", err)
	}
	if bundleHash.Valid {
		t.Fatalf("bundle_hash = %q, want NULL for legacy row", bundleHash.String)
	}
	if bundleSource != "legacy" {
		t.Fatalf("bundle_source = %q, want legacy", bundleSource)
	}
	var fkCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint
		WHERE conrelid = 'runs'::regclass
		  AND contype = 'f'
		  AND pg_get_constraintdef(oid) LIKE '%bundle_hash%'
	`).Scan(&fkCount); err != nil {
		t.Fatalf("inspect bundle_hash foreign keys: %v", err)
	}
	if fkCount != 0 {
		t.Fatalf("runs.bundle_hash foreign keys = %d, want none", fkCount)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, bundle_source) VALUES ($1::uuid, 'running', 'unsupported')`, uuid.NewString()); err == nil {
		t.Fatal("insert unsupported bundle_source succeeded, want check constraint failure")
	}
}

func TestManagerStore_LoadAgents_FailsClosedWhenCanonicalModelTierMissing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.ensureSchemaCompatibilityColumns(ctx); err != nil {
		t.Fatalf("ensureSchemaCompatibilityColumns: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model_tier, llm_backend, conversation_mode,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status
		) VALUES (
			$1, '', 'reviewer', '', 'api', 'task',
			NULL, NULL, '{}'::jsonb, '["review.ready"]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
			$2::jsonb, 'active'
		)
	`, "agent-missing-type", `{"mode":"review"}`); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}

	_, err := pg.LoadAgents(ctx)
	if err == nil || !strings.Contains(err.Error(), "missing model_tier") {
		t.Fatalf("LoadAgents error = %v, want missing model_tier failure", err)
	}
}

func TestManagerStore_LoadAgents_FailsClosedWithoutHotPathSchemaRepair(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agents CASCADE`); err != nil {
		t.Fatalf("drop agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agents (
			id TEXT PRIMARY KEY,
			type TEXT,
			role TEXT NOT NULL,
			mode TEXT NOT NULL,
			entity_id UUID,
			parent_agent_id TEXT,
			status TEXT,
			coordinator_id TEXT,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			budget_envelope JSONB NOT NULL DEFAULT '{}'::jsonb,
			hired_by TEXT,
			template_version TEXT,
			started_at TIMESTAMPTZ,
			last_active_at TIMESTAMPTZ
		)
	`); err != nil {
		t.Fatalf("create legacy agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ('legacy-agent', 'review-worker', 'reviewer', 'task', 'active', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("seed legacy agent row: %v", err)
	}

	_, err := pg.LoadAgents(ctx)
	if err == nil || !strings.Contains(err.Error(), "store: agents schema is unsupported by the explicit capability boundary") {
		t.Fatalf("LoadAgents error = %v, want unsupported canonical schema failure", err)
	}

	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'agents'
			  AND column_name = 'runtime_descriptor'
		)
	`).Scan(&exists); err != nil {
		t.Fatalf("check agents.runtime_descriptor existence: %v", err)
	}
	if exists {
		t.Fatal("expected LoadAgents not to backfill agents.runtime_descriptor on the hot path")
	}
}

func TestPostgresStore_Manager_MoreCoverage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := runtimecorrelation.WithRunID(context.Background(), specEntityStateRunID)
	resetAgentSessionsSpecTable(t, ctx, pg)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "testco", "testco", "TestCo", "operating")
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "a1",
			Role:     "role",
			Mode:     "global",
			Type:     "sonnet",
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
	if err := pg.MarkAgentTerminated(ctx, "a1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "ephemeral-shard-1",
			Role:     "worker",
			Mode:     "worker",
			Type:     "sonnet",
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
		Config: runtimeactors.AgentConfig{
			ID:       ceoID,
			Role:     "operator",
			Mode:     "operating",
			Type:     "sonnet",
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

	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "review.requested",
		SourceAgent: "human",
		Payload:     json.RawMessage(`{"x":1}`),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{ceoID}); err != nil {
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

	if err := pg.UpsertEventReceipt(ctx, evt.ID, ceoID, "error", "boom"); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	if rec, ok, err := pg.GetEventReceipt(ctx, evt.ID, ceoID); err != nil || !ok || rec.Status == "" {
		t.Fatalf("GetEventReceipt ok=%v err=%v rec=%+v", ok, err, rec)
	}

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, agent_id, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at)
		VALUES ($1::uuid, $2, 'global', 'global', '[]'::jsonb, 0, 'session', '{}'::jsonb, 'active', now(), now())
	`, sessionID, ceoID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        ceoID,
		RuntimeMode:    "session",
		SessionID:      sessionID,
		TaskID:         "",
		RequestPayload: []byte(`{"in":1}`),
		ResponseRaw:    []byte(`{"out":1}`),
		ParseOK:        true,
		Latency:        123 * time.Millisecond,
		RetryCount:     0,
		Error:          "",
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      ceoID,
		TaskID:       "",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages:     []llm.Message{{Role: "user", Content: "hi"}},
		Summary:      "sum",
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	if rec, ok, err := pg.LoadActiveConversation(ctx, ceoID, "session", "global", "global"); err != nil || !ok || rec.AgentID != ceoID {
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
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, role, model_tier, llm_backend, conversation_mode,
			config, runtime_descriptor, status, created_at
		) VALUES (
			'legacy-session-agent', 'worker', 'sonnet', 'api', 'session',
			'{"type":"sonnet","mode":"worker","session_scope":"global","system_prompt":"x"}'::jsonb,
			'{}'::jsonb,
			'active',
			now()
		)
	`); err != nil {
		t.Fatalf("seed legacy agent row: %v", err)
	}
	if err := pg.ensureAgentRuntimeDescriptorColumn(ctx); err != nil {
		t.Fatalf("ensureAgentRuntimeDescriptorColumn: %v", err)
	}

	_, err := pg.LoadAgents(ctx)
	if err == nil || !strings.Contains(err.Error(), "invalid opaque config: config contains runtime-owned keys: mode, session_scope, type") {
		t.Fatalf("LoadAgents error = %v, want fail-closed legacy runtime config error", err)
	}

	var (
		configJSON            string
		runtimeDescriptorJSON string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(config::text, '{}'), COALESCE(runtime_descriptor::text, '{}')
		FROM agents
		WHERE agent_id = 'legacy-session-agent'
	`).Scan(&configJSON, &runtimeDescriptorJSON); err != nil {
		t.Fatalf("load legacy agent row: %v", err)
	}
	if !strings.Contains(configJSON, "session_scope") {
		t.Fatalf("expected legacy session_scope to remain untouched in opaque config, got %s", configJSON)
	}
	if !strings.Contains(runtimeDescriptorJSON, `"type": "sonnet"`) && !strings.Contains(runtimeDescriptorJSON, `"type":"sonnet"`) {
		t.Fatalf("expected runtime_descriptor.type backfilled from model_tier, got %s", runtimeDescriptorJSON)
	}
}

func TestPostgresStore_LoadAgents_BackfillsRuntimeDescriptorTypeFromModelTierOnColumnUpgrade(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS agents CASCADE`); err != nil {
		t.Fatalf("drop agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE agents (
			agent_id TEXT PRIMARY KEY,
			flow_instance TEXT,
			role TEXT NOT NULL,
			model_tier TEXT NOT NULL,
			llm_backend TEXT NOT NULL DEFAULT 'api',
			conversation_mode TEXT NOT NULL,
			parent_agent_id TEXT,
			entity_id UUID,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			subscriptions JSONB NOT NULL DEFAULT '[]'::jsonb,
			emit_events JSONB NOT NULL DEFAULT '[]'::jsonb,
			tools JSONB NOT NULL DEFAULT '[]'::jsonb,
			permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			turn_count INTEGER NOT NULL DEFAULT 0,
			last_active_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("create legacy agents table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, role, model_tier, llm_backend, conversation_mode,
			config, status, created_at
		) VALUES (
			'precolumn-agent', 'worker', 'sonnet', 'api', 'task',
			'{"system_prompt":"x"}'::jsonb,
			'active',
			now()
		)
	`); err != nil {
		t.Fatalf("seed pre-column agent row: %v", err)
	}

	if err := pg.ensureAgentRuntimeDescriptorColumn(ctx); err != nil {
		t.Fatalf("ensureAgentRuntimeDescriptorColumn: %v", err)
	}

	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	found := false
	for _, agent := range agents {
		if agent.Config.ID != "precolumn-agent" {
			continue
		}
		found = true
		if agent.Config.Type != "sonnet" {
			t.Fatalf("Type = %q, want sonnet", agent.Config.Type)
		}
		if agent.Config.ModelTier != "sonnet" {
			t.Fatalf("ModelTier = %q, want sonnet", agent.Config.ModelTier)
		}
	}
	if !found {
		t.Fatal("expected precolumn-agent in LoadAgents result")
	}

	var runtimeDescriptorRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT runtime_descriptor
		FROM agents
		WHERE agent_id = 'precolumn-agent'
	`).Scan(&runtimeDescriptorRaw); err != nil {
		t.Fatalf("query upgraded runtime_descriptor: %v", err)
	}
	desc, err := decodePersistedAgentRuntimeDescriptor(runtimeDescriptorRaw)
	if err != nil {
		t.Fatalf("decodePersistedAgentRuntimeDescriptor: %v", err)
	}
	if desc.Type != "sonnet" {
		t.Fatalf("runtime_descriptor.type = %q, want sonnet", desc.Type)
	}
}

func TestPostgresStore_MarkAgentTerminated_CleansRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:   "agent-cleanup-1",
			Role: "worker",
			Mode: "worker",
			Type: "sonnet",
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

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID:    acquireLiveTestSession(t, ctx, db, "agent-cleanup-1", runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "global"),
		AgentID:      "agent-cleanup-1",
		Mode:         "session",
		SessionScope: "global",
		ScopeKey:     "global",
		Messages:     []llm.Message{{Role: "user", Content: "hello"}},
		Summary:      "x",
		TurnCount:    1,
		Status:       "active",
	}); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: uuid.NewString(),
		AgentID:   "agent-cleanup-1",
		Mode:      "task",
		Messages:  []llm.Message{{Role: "assistant", Content: "done"}},
		Summary:   "task",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("seed task audit: %v", err)
	}
	if err := pg.MarkAgentTerminated(ctx, "agent-cleanup-1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}

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
	if auditStatus != "terminated" {
		t.Fatalf("expected terminated audit status, got %q", auditStatus)
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
