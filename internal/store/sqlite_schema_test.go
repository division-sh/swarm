package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	stdruntime "runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestSQLiteSchemaStoreBootstrapsPlatformAndGeneratedTables(t *testing.T) {
	ctx := context.Background()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	platformPlans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(platformPlans) != 29 {
		t.Fatalf("platform table plan count = %d, want 29", len(platformPlans))
	}
	entityPlans, err := GenerateEntityTableDDLs(runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "products",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "slug", Type: "text", Indexed: true},
				{Name: "score", Type: "numeric(12,2)", Nullable: true},
				{Name: "metadata", Type: "jsonb"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("GenerateEntityTableDDLs: %v", err)
	}
	statePlans, err := GenerateNodeStateTableDDLs(map[string]runtimecontracts.SystemNodeContract{
		"planner": {
			StateTable: "planner_state",
			StateSchema: runtimecontracts.NodeStateSchema{
				Fields: []runtimecontracts.NodeStateField{
					{Name: "status", Type: "text"},
					{Name: "snapshot", Type: "jsonb"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("GenerateNodeStateTableDDLs: %v", err)
	}
	plans := append(platformPlans, entityPlans...)
	plans = append(plans, statePlans...)

	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	sqliteStore, err := NewSQLiteSchemaStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteSchemaStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if err := sqliteStore.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite bootstrap did not create file-backed db at %s: %v", dbPath, err)
	}

	for _, tableName := range append(platformTableNamesForSQLiteSchemaTest(), "products", "planner_state") {
		if !sqliteTableExists(t, sqliteStore.DB, tableName) {
			t.Fatalf("sqlite table %s missing after bootstrap", tableName)
		}
	}
	caps, err := sqliteStore.ResolveSchemaCapabilities(ctx)
	if err != nil {
		t.Fatalf("ResolveSchemaCapabilities: %v", err)
	}
	if caps.Agents != SchemaFlavorCanonical {
		t.Fatalf("agents capability = %s", caps.Agents)
	}
	if caps.EntityState != SchemaFlavorCanonical || !caps.EntityRunID {
		t.Fatalf("entity_state capability = %s run_id=%v", caps.EntityState, caps.EntityRunID)
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID || !caps.Events.LogIdempotencyKey || !caps.Events.LogRouteIdentity {
		t.Fatalf("event log capabilities = %+v", caps.Events)
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID || !caps.Events.DeliveryTargetRoute {
		t.Fatalf("event delivery capabilities = %+v", caps.Events)
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || !caps.Events.ReceiptTypedIdentity {
		t.Fatalf("event receipt capabilities = %+v", caps.Events)
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical || !caps.Conversations.SessionRunID {
		t.Fatalf("session capabilities = %+v", caps.Conversations)
	}
	if caps.Conversations.Audits != SchemaFlavorCanonical || !caps.Conversations.AuditRunID {
		t.Fatalf("audit capabilities = %+v", caps.Conversations)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical || !caps.Conversations.TurnRunID || !caps.Conversations.TurnBlocks {
		t.Fatalf("turn capabilities = %+v", caps.Conversations)
	}
	if caps.Conversations.Forks != SchemaFlavorCanonical || caps.Conversations.ForkSnapshots != SchemaFlavorCanonical || caps.Conversations.ForkTurns != SchemaFlavorCanonical {
		t.Fatalf("fork conversation capabilities = %+v", caps.Conversations)
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		t.Fatalf("mailbox capability = %s", caps.Mailbox)
	}
	if caps.Schedules != SchemaFlavorCanonical {
		t.Fatalf("schedule capability = %s", caps.Schedules)
	}
}

func TestSQLiteSchemaStoreMigratesLegacyAgentLLMBackendProfiles(t *testing.T) {
	ctx := context.Background()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	platformPlans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	agentPlans := []SchemaTableDDL{}
	for _, plan := range platformPlans {
		if plan.TableName == "agents" {
			agentPlans = append(agentPlans, plan)
		}
	}
	if len(agentPlans) != 1 {
		t.Fatalf("agents platform plan count = %d, want 1", len(agentPlans))
	}
	sqliteStore, err := NewSQLiteSchemaStore(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatalf("NewSQLiteSchemaStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		CREATE TABLE agents (
			agent_id TEXT PRIMARY KEY,
			flow_instance TEXT,
			role TEXT NOT NULL,
			model TEXT NOT NULL,
			llm_backend TEXT NOT NULL DEFAULT 'api' CHECK (llm_backend IN ('api', 'cli_test', 'openai_compatible', 'mock', 'local')),
			conversation_mode TEXT NOT NULL,
			parent_agent_id TEXT,
			entity_id TEXT,
			config TEXT NOT NULL DEFAULT '{}',
			subscriptions TEXT NOT NULL DEFAULT '[]',
			emit_events TEXT NOT NULL DEFAULT '[]',
			tools TEXT NOT NULL DEFAULT '[]',
			permissions TEXT NOT NULL DEFAULT '[]',
			runtime_descriptor TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL DEFAULT 'active'
		)
	`); err != nil {
		t.Fatalf("create legacy sqlite agents: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO agents (agent_id, role, model, llm_backend, conversation_mode)
			VALUES
				('agent-api', 'worker', 'sonnet', 'api', 'task'),
				('agent-cli', 'worker', 'sonnet', 'cli_test', 'task'),
				('agent-openai', 'worker', 'sonnet', 'openai_compatible', 'task')
		`); err != nil {
		t.Fatalf("seed legacy sqlite agents: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			CREATE TABLE agent_sessions (
				session_id TEXT PRIMARY KEY,
				agent_id TEXT NOT NULL REFERENCES agents(agent_id),
				scope_key TEXT NOT NULL,
				scope TEXT NOT NULL DEFAULT 'entity',
				runtime_mode TEXT NOT NULL DEFAULT 'task',
				status TEXT NOT NULL DEFAULT 'active'
			)
		`); err != nil {
		t.Fatalf("create legacy sqlite agent_sessions child: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, agent_id, scope_key)
			VALUES ('session-api', 'agent-api', 'scope-api')
		`); err != nil {
		t.Fatalf("seed legacy sqlite agent_sessions child: %v", err)
	}
	if err := sqliteStore.EnsureSchemaTables(ctx, agentPlans); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	rows, err := sqliteStore.DB.QueryContext(ctx, `SELECT agent_id, llm_backend FROM agents ORDER BY agent_id`)
	if err != nil {
		t.Fatalf("query migrated sqlite agents: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var id, backend string
		if err := rows.Scan(&id, &backend); err != nil {
			t.Fatalf("scan migrated sqlite agents: %v", err)
		}
		got[id] = backend
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read migrated sqlite agents: %v", err)
	}
	for id, want := range map[string]string{
		"agent-api":    "anthropic",
		"agent-cli":    "claude_cli",
		"agent-openai": "openai_compatible",
	} {
		if got[id] != want {
			t.Fatalf("%s llm_backend = %q, want %q (all rows %#v)", id, got[id], want, got)
		}
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model, llm_backend, conversation_mode) VALUES ('agent-legacy', 'worker', 'sonnet', 'api', 'task')`); err == nil {
		t.Fatal("insert legacy sqlite llm_backend api succeeded after migration")
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model, conversation_mode) VALUES ('agent-default', 'worker', 'sonnet', 'task')`); err != nil {
		t.Fatalf("insert default sqlite llm_backend after migration: %v", err)
	}
	var defaultBackend string
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT llm_backend FROM agents WHERE agent_id = 'agent-default'`).Scan(&defaultBackend); err != nil {
		t.Fatalf("query sqlite default backend: %v", err)
	}
	if defaultBackend != "anthropic" {
		t.Fatalf("sqlite default llm_backend = %q, want anthropic", defaultBackend)
	}
	conn, err := sqliteStore.DB.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve sqlite validation connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable sqlite foreign keys for validation: %v", err)
	}
	var childAgentID string
	if err := conn.QueryRowContext(ctx, `SELECT agent_id FROM agent_sessions WHERE session_id = 'session-api'`).Scan(&childAgentID); err != nil {
		t.Fatalf("query migrated sqlite child session: %v", err)
	}
	if childAgentID != "agent-api" {
		t.Fatalf("child session agent_id = %q, want agent-api", childAgentID)
	}
	fkRows, err := conn.QueryContext(ctx, `PRAGMA foreign_key_list(agent_sessions)`)
	if err != nil {
		t.Fatalf("inspect sqlite agent_sessions foreign keys: %v", err)
	}
	defer fkRows.Close()
	var referencesAgents bool
	for fkRows.Next() {
		var id, seq int
		var tableName, from, to, onUpdate, onDelete, match string
		if err := fkRows.Scan(&id, &seq, &tableName, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan sqlite agent_sessions foreign key: %v", err)
		}
		if tableName == "agents" && from == "agent_id" && to == "agent_id" {
			referencesAgents = true
		}
	}
	if err := fkRows.Err(); err != nil {
		t.Fatalf("read sqlite agent_sessions foreign keys: %v", err)
	}
	if !referencesAgents {
		t.Fatal("agent_sessions foreign key no longer references canonical agents(agent_id)")
	}
	checkRows, err := conn.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("run sqlite foreign_key_check: %v", err)
	}
	defer checkRows.Close()
	if checkRows.Next() {
		t.Fatal("sqlite foreign_key_check reported a violation after agents llm_backend migration")
	}
	if err := checkRows.Err(); err != nil {
		t.Fatalf("read sqlite foreign_key_check: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, agent_id, scope_key)
			VALUES ('session-missing', 'missing-agent', 'scope-missing')
		`); err == nil {
		t.Fatal("insert sqlite child session with missing agent succeeded after migration")
	}
}

func TestSQLiteSchemaStoreEnsureAgentModelAliasesMigratesLegacyModelTier(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := NewSQLiteSchemaStore(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatalf("NewSQLiteSchemaStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		CREATE TABLE agents (
			agent_id TEXT PRIMARY KEY,
			role TEXT NOT NULL,
			model_tier TEXT,
			conversation_mode TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create legacy sqlite agents: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model_tier, conversation_mode)
		VALUES
			('agent-haiku', 'worker', 'haiku', 'task'),
			('agent-low-cost', 'worker', 'low_cost', 'task'),
			('agent-sonnet', 'worker', 'sonnet', 'task'),
			('agent-general', 'worker', 'general', 'task'),
			('agent-generic', 'worker', 'generic', 'task')
	`); err != nil {
		t.Fatalf("seed legacy sqlite agents: %v", err)
	}

	if err := sqliteStore.ensureSQLiteAgentModelAliases(ctx); err != nil {
		t.Fatalf("ensureSQLiteAgentModelAliases: %v", err)
	}
	rows, err := sqliteStore.DB.QueryContext(ctx, `SELECT agent_id, model FROM agents ORDER BY agent_id`)
	if err != nil {
		t.Fatalf("query migrated sqlite agent models: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var id, model string
		if err := rows.Scan(&id, &model); err != nil {
			t.Fatalf("scan migrated sqlite agent model: %v", err)
		}
		got[id] = model
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read migrated sqlite agent models: %v", err)
	}
	want := map[string]string{
		"agent-haiku":    "cheap",
		"agent-low-cost": "cheap",
		"agent-sonnet":   "regular",
		"agent-general":  "regular",
		"agent-generic":  "regular",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("migrated sqlite models = %#v, want %#v", got, want)
	}
}

func TestSQLiteSchemaStoreEnsureAgentModelAliasesRejectsUnmappableLegacyModelTier(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := NewSQLiteSchemaStore(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatalf("NewSQLiteSchemaStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		CREATE TABLE agents (
			agent_id TEXT PRIMARY KEY,
			role TEXT NOT NULL,
			model_tier TEXT,
			conversation_mode TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create legacy sqlite agents: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model_tier, conversation_mode)
		VALUES ('agent-unknown', 'worker', 'opus', 'task')
	`); err != nil {
		t.Fatalf("seed legacy sqlite agent: %v", err)
	}

	err = sqliteStore.ensureSQLiteAgentModelAliases(ctx)
	if err == nil || !strings.Contains(err.Error(), "cannot map 1 legacy model_tier rows") {
		t.Fatalf("ensureSQLiteAgentModelAliases error = %v, want unmappable legacy model_tier failure", err)
	}
}

func TestSQLiteStatementsForPlanRejectsUnsupportedPostgresConstructs(t *testing.T) {
	_, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "bad_regex",
		SchemaKind: "test",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS bad_regex (\n    id TEXT CHECK (id ~ '^bad$')\n)",
		},
	})
	if err == nil {
		t.Fatal("SQLiteStatementsForPlan accepted unsupported regex construct")
	}
	if !strings.Contains(err.Error(), "unsupported SQLite schema construct") || !strings.Contains(err.Error(), "Postgres regex operator") {
		t.Fatalf("error = %v, want fail-closed unsupported regex diagnostic", err)
	}
}

func TestSQLiteSchemaStoreRendersExplicitUUIDDefaults(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := NewSQLiteSchemaStore(filepath.Join(t.TempDir(), "uuid-defaults.db"))
	if err != nil {
		t.Fatalf("NewSQLiteSchemaStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if err := sqliteStore.EnsureSchemaTables(ctx, []SchemaTableDDL{{
		TableName:  "uuid_defaults",
		SchemaKind: "test",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS uuid_defaults (\n    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n)",
		},
	}}); err != nil {
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO uuid_defaults DEFAULT VALUES`); err != nil {
		t.Fatalf("insert default row: %v", err)
	}
	var id string
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT id FROM uuid_defaults`).Scan(&id); err != nil {
		t.Fatalf("select generated uuid: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("generated id = %q, want SQLite-rendered UUID default", id)
	}
}

func TestSQLiteStatementsForPlanRejectsUnsupportedColumnType(t *testing.T) {
	_, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "bad_type",
		SchemaKind: "test",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS bad_type (\n    id SERIAL PRIMARY KEY\n)",
		},
	})
	if err == nil {
		t.Fatal("SQLiteStatementsForPlan accepted unsupported column type")
	}
	if !strings.Contains(err.Error(), "unsupported SQLite column type") {
		t.Fatalf("error = %v, want unsupported column type diagnostic", err)
	}
}

func TestNewSQLiteSchemaStoreRejectsInMemoryPaths(t *testing.T) {
	for _, path := range []string{":memory:", "file::memory:?cache=shared", "file:dev.db?mode=memory"} {
		t.Run(path, func(t *testing.T) {
			store, err := NewSQLiteSchemaStore(path)
			if err == nil {
				_ = store.Close()
				t.Fatal("NewSQLiteSchemaStore accepted in-memory path")
			}
			if !strings.Contains(err.Error(), "file-backed") {
				t.Fatalf("error = %v, want file-backed diagnostic", err)
			}
		})
	}
}

func loadPlatformSpecForSQLiteSchemaTest(t *testing.T) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	_, file, _, _ := stdruntime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	return spec
}

func platformTableNamesForSQLiteSchemaTest() []string {
	return []string{
		"bundles",
		"events",
		"run_fork_selected_contract_bindings",
		"run_fork_selected_contract_executions",
		"run_fork_selected_contract_branch_divergences",
		"run_fork_selected_contract_route_recoveries",
		"event_deliveries",
		"run_fork_delivery_event_replays",
		"event_receipts",
		"entity_state",
		"agents",
		"agent_sessions",
		"agent_turns",
		"agent_conversation_audits",
		"routing_rules",
		"mailbox",
		"api_idempotency",
		"conversation_forks",
		"conversation_fork_snapshots",
		"conversation_fork_turns",
		"runtime_ingress_state",
		"run_control_state",
		"spend_ledger",
		"flow_instances",
		"timers",
		"dead_letters",
		"runs",
		"entity_mutations",
		"schema_version",
	}
}

func sqliteTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, tableName).Scan(&exists); err != nil {
		t.Fatalf("query sqlite table %s: %v", tableName, err)
	}
	return exists == 1
}
