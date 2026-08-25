package schemastore

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSQLiteSchemaStoreBootstrapsPlatformAndGeneratedTables(t *testing.T) {
	ctx := context.Background()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	platformPlans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if len(platformPlans) != 76 {
		t.Fatalf("platform table plan count = %d, want 76", len(platformPlans))
	}
	statePlans, err := GenerateNodeStateTableDDLs([]runtimecontracts.ScopedNodeRecord{{
		LogicalID: "planner",
		Entry: runtimecontracts.SystemNodeContract{
			StateTable: "planner_state",
			StateSchema: runtimecontracts.NodeStateSchema{
				Fields: []runtimecontracts.NodeStateField{
					{Name: "status", Type: "text"},
					{Name: "snapshot", Type: "jsonb"},
				},
			},
		},
		Source: runtimecontracts.ContractItemSource{PackageKey: ".", Layer: "project"},
	}})
	if err != nil {
		t.Fatalf("GenerateNodeStateTableDDLs: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	sqliteStore, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	if err := sqliteStore.BootstrapSchema(ctx, SchemaBootstrapRequest{
		PlatformPlans: platformPlans,
		StatePlans:    statePlans,
		Origin: RuntimeStoreOrigin{
			SwarmVersion:    "sqlite-schema-test",
			PlatformVersion: spec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite bootstrap did not create file-backed db at %s: %v", dbPath, err)
	}

	for _, tableName := range append(platformTableNamesForSQLiteSchemaTest(), "planner_state") {
		if !sqliteTableExists(t, sqliteStore.backend.ConstructionHandle(), tableName) {
			t.Fatalf("sqlite table %s missing after bootstrap", tableName)
		}
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

func TestSQLiteStatementsForPlanTranslatesDeliveryAuthorityBundleHashExactly(t *testing.T) {
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "delivery_authority",
		SchemaKind: "test",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS delivery_authority (\n    authority_bundle_hash TEXT NOT NULL CHECK (authority_bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$')\n)",
		},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], "length(authority_bundle_hash) = 81") || strings.Contains(statements[0], "authority_(") {
		t.Fatalf("rendered statements = %#v, want exact authority bundle hash predicate", statements)
	}
}

func TestSQLiteStatementsForPlanTranslatesLifecycleBundleHashExactly(t *testing.T) {
	t.Parallel()

	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "agents",
		SchemaKind: "platform_spec",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS agents (\n    lifecycle_bundle_hash TEXT NOT NULL CHECK (lifecycle_bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$')\n)",
		},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 {
		t.Fatalf("statement count = %d, want 1", len(statements))
	}
	if strings.Contains(statements[0], "lifecycle_(") || !strings.Contains(statements[0], "length(lifecycle_bundle_hash) = 81") {
		t.Fatalf("translated lifecycle bundle hash predicate = %q", statements[0])
	}
}

func TestSQLiteStatementsForPlanAcceptsMultilineTableConstraint(t *testing.T) {
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "multiline_check",
		SchemaKind: "test",
		Statements: []string{`CREATE TABLE IF NOT EXISTS multiline_check (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    completed_at TIMESTAMPTZ,
    CHECK (
        (status = 'pending' AND completed_at IS NULL)
        OR (status = 'completed' AND completed_at IS NOT NULL)
    )
)`},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], "OR (status = 'completed'") {
		t.Fatalf("rendered statements = %#v, want intact multiline check", statements)
	}
}

func TestSQLiteStatementsForPlanAcceptsCompositeForeignKeyDeleteCascade(t *testing.T) {
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "child",
		SchemaKind: "test",
		Statements: []string{`CREATE TABLE IF NOT EXISTS child (
    run_id UUID NOT NULL,
    revision BIGINT NOT NULL,
    FOREIGN KEY (run_id, revision) REFERENCES parent(run_id, revision) ON DELETE CASCADE
)`},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], `FOREIGN KEY ("run_id", "revision") REFERENCES "parent"("run_id", "revision") ON DELETE CASCADE`) {
		t.Fatalf("rendered statements = %#v, want composite cascading foreign key", statements)
	}
}

func TestSQLiteStatementsForPlanAcceptsDeferredCompositeForeignKey(t *testing.T) {
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "child",
		SchemaKind: "platform_spec",
		Statements: []string{`CREATE TABLE IF NOT EXISTS child (
    delivery_id UUID NOT NULL,
    claim_version BIGINT NOT NULL,
    open_marker BOOLEAN NOT NULL,
    FOREIGN KEY (delivery_id, claim_version, open_marker) REFERENCES parent(delivery_id, current_attempt_version, current_attempt_open) DEFERRABLE INITIALLY DEFERRED
)`},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], `FOREIGN KEY ("delivery_id", "claim_version", "open_marker") REFERENCES "parent"("delivery_id", "current_attempt_version", "current_attempt_open") DEFERRABLE INITIALLY DEFERRED`) {
		t.Fatalf("deferred foreign key translation = %#v", statements)
	}
}

func TestSQLiteStatementsForPlanPreservesNonReusableBigserialIdentity(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := NewSQLite(filepath.Join(t.TempDir(), "bigserial.db"))
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "sequenced",
		SchemaKind: "test",
		Statements: []string{`CREATE TABLE IF NOT EXISTS sequenced (
    insertion_sequence BIGSERIAL PRIMARY KEY,
    insertion_transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id(),
    event_id UUID NOT NULL UNIQUE
)`},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], `"insertion_sequence" INTEGER PRIMARY KEY AUTOINCREMENT`) {
		t.Fatalf("rendered statements = %#v, want non-reusable SQLite sequence identity", statements)
	}
	if !strings.Contains(statements[0], `"insertion_transaction_id" INTEGER NOT NULL DEFAULT 0`) {
		t.Fatalf("rendered statements = %#v, want inert SQLite transaction identity", statements)
	}
	if _, err := sqliteStore.backend.ExecContext(ctx, statements[0]); err != nil {
		t.Fatalf("create sequenced table: %v", err)
	}
	firstID := "00000000-0000-4000-8000-000000000001"
	secondID := "00000000-0000-4000-8000-000000000002"
	thirdID := "00000000-0000-4000-8000-000000000003"
	for _, eventID := range []string{firstID, secondID} {
		if _, err := sqliteStore.backend.ExecContext(ctx, `INSERT INTO sequenced (event_id) VALUES (?)`, eventID); err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
	}
	var removedSequence int64
	if err := sqliteStore.backend.QueryRowContext(ctx, `SELECT insertion_sequence FROM sequenced WHERE event_id = ?`, secondID).Scan(&removedSequence); err != nil {
		t.Fatalf("read removed sequence: %v", err)
	}
	if _, err := sqliteStore.backend.ExecContext(ctx, `DELETE FROM sequenced WHERE event_id = ?`, secondID); err != nil {
		t.Fatalf("delete second row: %v", err)
	}
	if _, err := sqliteStore.backend.ExecContext(ctx, `INSERT INTO sequenced (event_id) VALUES (?)`, thirdID); err != nil {
		t.Fatalf("insert replacement row: %v", err)
	}
	var replacementSequence int64
	if err := sqliteStore.backend.QueryRowContext(ctx, `SELECT insertion_sequence FROM sequenced WHERE event_id = ?`, thirdID).Scan(&replacementSequence); err != nil {
		t.Fatalf("read replacement sequence: %v", err)
	}
	if replacementSequence <= removedSequence {
		t.Fatalf("replacement sequence = %d, want greater than deleted high-water %d", replacementSequence, removedSequence)
	}
}

func TestSQLiteSchemaStoreRendersExplicitUUIDDefaults(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := NewSQLite(filepath.Join(t.TempDir(), "uuid-defaults.db"))
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite schema store: %v", err)
		}
	})
	statements, err := SQLiteStatementsForPlan(SchemaTableDDL{
		TableName:  "uuid_defaults",
		SchemaKind: "test",
		Statements: []string{
			"CREATE TABLE IF NOT EXISTS uuid_defaults (\n    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n)",
		},
	})
	if err != nil {
		t.Fatalf("SQLiteStatementsForPlan: %v", err)
	}
	for _, statement := range statements {
		if _, err := sqliteStore.backend.ExecContext(ctx, statement); err != nil {
			t.Fatalf("execute rendered SQLite statement: %v", err)
		}
	}
	if _, err := sqliteStore.backend.ExecContext(ctx, `INSERT INTO uuid_defaults DEFAULT VALUES`); err != nil {
		t.Fatalf("insert default row: %v", err)
	}
	var id string
	if err := sqliteStore.backend.QueryRowContext(ctx, `SELECT id FROM uuid_defaults`).Scan(&id); err != nil {
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

func TestNewSQLiteRejectsInMemoryPaths(t *testing.T) {
	for _, path := range []string{":memory:", "file::memory:?cache=shared", "file:dev.db?mode=memory"} {
		t.Run(path, func(t *testing.T) {
			store, err := NewSQLite(path)
			if err == nil {
				_ = store.Close()
				t.Fatal("NewSQLite accepted in-memory path")
			}
			if !strings.Contains(err.Error(), "file-backed") {
				t.Fatalf("error = %v, want file-backed diagnostic", err)
			}
		})
	}
}

func loadPlatformSpecForSQLiteSchemaTest(t *testing.T) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	repoRoot := repoRootForRuntimeWriterGuard(t)
	return loadPlatformSpecDocumentForStoreTest(t, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}

func platformTableNamesForSQLiteSchemaTest() []string {
	return []string{
		"bundles",
		"events",
		"activity_attempts",
		"run_fork_selected_contract_bindings",
		"run_fork_selected_contract_runtime_executions",
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
		"conversation_fork_turn_completions",
		"runtime_external_effect_operations",
		"runtime_external_effect_attempts",
		"runtime_ingress_state",
		"run_control_state",
		"spend_ledger",
		"budget_admission_scopes",
		"runtime_effect_budget_reservations",
		"flow_instances",
		"timers",
		"dead_letters",
		"runs",
		"run_scenario_execution_profiles",
		"entity_mutations",
		"runtime_store_metadata",
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

// TestSQLiteSchemaStoreAcceptsRelativePath is a regression guard for the
// `sqliteFileDSN` path. Before this guard, the DSN was built with
// `url.URL{Scheme: "file", Path: path}`, which for a relative path like
// `.swarm/dev.db` serialized to `file://.swarm/dev.db` and bound the
// connection to a malformed URI; the first statement then failed with
// "SQL logic error: out of memory (1)". This test runs the open path with a
// relative path from a temp working directory and confirms a trivial query
// succeeds, which is only possible if the DSN is parsed correctly.
func TestSQLiteSchemaStoreAcceptsRelativePath(t *testing.T) {
	dir := t.TempDir()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(prev); err != nil {
			t.Fatalf("restore chdir: %v", err)
		}
	})

	store, err := NewSQLite(".swarm/dev.db")
	if err != nil {
		t.Fatalf("NewSQLite(relative): %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})

	var got int
	if err := store.backend.QueryRow(`SELECT 1`).Scan(&got); err != nil {
		t.Fatalf("SELECT 1 on relative-path store: %v", err)
	}
	if got != 1 {
		t.Fatalf("SELECT 1 returned %d, want 1", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".swarm", "dev.db")); err != nil {
		t.Fatalf("relative-path store did not create file at expected location: %v", err)
	}
}
