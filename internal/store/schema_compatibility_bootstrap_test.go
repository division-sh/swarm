package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

func TestSchemaBootstrapRejectsEveryRetiredPlatformTableUnchanged(t *testing.T) {
	for _, backend := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		for _, table := range platformschema.RetiredPlatformTables() {
			for _, populated := range []bool{false, true} {
				name := string(backend) + "/" + string(table) + "/empty"
				if populated {
					name = string(backend) + "/" + string(table) + "/populated"
				}
				t.Run(name, func(t *testing.T) {
					assertRetiredPlatformTableRejectedUnchanged(t, backend, table, populated)
				})
			}
		}
	}
}

func TestRetiredPlatformTableRegistryIsExact(t *testing.T) {
	want := []platformschema.RetiredPlatformTable{
		"schema_version",
		"decision_card_lifecycle_outbox",
		"agent_external_effect_operations",
		"agent_external_effect_attempts",
		"schedules",
	}
	if got := platformschema.RetiredPlatformTables(); !reflect.DeepEqual(got, want) {
		t.Fatalf("retired platform table registry = %v, want %v", got, want)
	}
}

func assertRetiredPlatformTableRejectedUnchanged(t *testing.T, backend SchemaDialect, table platformschema.RetiredPlatformTable, populated bool) {
	t.Helper()
	ctx := context.Background()
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	var db *sql.DB
	var bootstrap func(context.Context, SchemaBootstrapRequest) error
	var runtimeProbe func() error
	if backend == SchemaDialectSQLite {
		path := filepath.Join(t.TempDir(), "retired.db")
		accepted, err := NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := accepted.BootstrapSchema(ctx, SchemaBootstrapRequest{PlatformPlans: request.PlatformPlans, Origin: request.Origin}); err != nil {
			t.Fatalf("canonical bootstrap: %v", err)
		}
		t.Cleanup(func() { _ = accepted.Close() })
		db = accepted.DB
		unaccepted, err := NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = unaccepted.Close() })
		bootstrap = unaccepted.BootstrapSchema
		runtimeProbe = func() error {
			_, err := unaccepted.ListActiveAgentDescriptors(ctx)
			return err
		}
	} else {
		_, postgresDB, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		accepted := &PostgresStore{DB: postgresDB}
		if err := accepted.BootstrapSchema(ctx, SchemaBootstrapRequest{PlatformPlans: request.PlatformPlans, Origin: request.Origin}); err != nil {
			t.Fatalf("canonical bootstrap: %v", err)
		}
		db = postgresDB
		unaccepted := &PostgresStore{DB: postgresDB}
		bootstrap = unaccepted.BootstrapSchema
		runtimeProbe = func() error {
			_, err := unaccepted.ListActiveAgentDescriptors(ctx)
			return err
		}
	}

	identifier := string(table)
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+identifier+` (marker TEXT)`); err != nil {
		t.Fatal(err)
	}
	wantRows := 0
	if populated {
		if _, err := db.ExecContext(ctx, `INSERT INTO `+identifier+` (marker) VALUES ('preserve-me')`); err != nil {
			t.Fatal(err)
		}
		wantRows = 1
	}
	err := bootstrap(ctx, request)
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("bootstrap error = %v, want SchemaCompatibilityError", err)
	}
	if drift := strings.Join(incompatible.Drift, "; "); !strings.Contains(drift, "retired platform table "+identifier+" exists") {
		t.Fatalf("bootstrap drift = %v, want retired table evidence", incompatible.Drift)
	}
	if err := runtimeProbe(); err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
		t.Fatalf("runtime probe after rejected bootstrap = %v, want unaccepted admission failure", err)
	}
	assertSchemaTableExists(t, backend, db, identifier, true)
	var gotRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+identifier).Scan(&gotRows); err != nil {
		t.Fatal(err)
	}
	if gotRows != wantRows {
		t.Fatalf("retired table rows = %d, want %d", gotRows, wantRows)
	}
	assertSchemaTableExists(t, backend, db, "generated_probe_state", false)
}

func assertSchemaTableExists(t *testing.T, backend SchemaDialect, db *sql.DB, table string, want bool) {
	t.Helper()
	var exists bool
	if backend == SchemaDialectSQLite {
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
	} else if err := db.QueryRow(`SELECT to_regclass('public.' || $1) IS NOT NULL`, table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Fatalf("table %s exists = %t, want %t", table, exists, want)
	}
}

func TestSchemaBootstrapRejectsLegacyPlatformShapesUnchanged(t *testing.T) {
	type legacyShape struct {
		name      string
		table     string
		postgres  []string
		sqlite    []string
		populated bool
	}
	cases := []legacyShape{
		{name: "delivery_last_error", table: "event_deliveries", postgres: []string{`ALTER TABLE event_deliveries ADD COLUMN last_error TEXT`}, sqlite: []string{`ALTER TABLE event_deliveries ADD COLUMN last_error TEXT`}},
		{name: "receipt_without_failure", table: "event_receipts", postgres: []string{`ALTER TABLE event_receipts DROP COLUMN failure`}, sqlite: []string{`ALTER TABLE event_receipts DROP COLUMN failure`}},
		{name: "dead_letter_flat_failure", table: "dead_letters", postgres: []string{`ALTER TABLE dead_letters ADD COLUMN failure_type TEXT`}, sqlite: []string{`ALTER TABLE dead_letters ADD COLUMN failure_type TEXT`}},
		{name: "activity_error", table: "activity_attempts", postgres: []string{`ALTER TABLE activity_attempts ADD COLUMN error TEXT`}, sqlite: []string{`ALTER TABLE activity_attempts ADD COLUMN error TEXT`}},
		{name: "event_without_source_route", table: "events", postgres: []string{`DROP INDEX idx_events_source_route`, `ALTER TABLE events DROP COLUMN source_route`}, sqlite: []string{
			`DROP INDEX idx_events_source_route`,
			`CREATE TABLE events_without_source_route AS SELECT event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at, routing_source_kind, routing_source_authority, target_route, target_set, operator_reference_event_id, handler_node, idempotency_key FROM events`,
			`DROP TABLE events`,
			`ALTER TABLE events_without_source_route RENAME TO events`,
		}},
		{name: "run_error_summary", table: "runs", postgres: []string{`ALTER TABLE runs ADD COLUMN error_summary TEXT`}, sqlite: []string{`ALTER TABLE runs ADD COLUMN error_summary TEXT`}},
		{name: "directive_flat_error", table: "agent_directive_operations", postgres: []string{`ALTER TABLE agent_directive_operations ADD COLUMN error_code TEXT`}, sqlite: []string{`ALTER TABLE agent_directive_operations ADD COLUMN error_code TEXT`}},
		{name: "mailbox_without_deferred_until", table: "mailbox", postgres: []string{`ALTER TABLE mailbox DROP COLUMN deferred_until`}, sqlite: []string{`ALTER TABLE mailbox DROP COLUMN deferred_until`}},
		{name: "reply_without_activity_reference", table: "activity_attempts", postgres: []string{`ALTER TABLE activity_attempts DROP COLUMN reply_context_id`}, sqlite: []string{`ALTER TABLE activity_attempts DROP COLUMN reply_context_id`}},
		{name: "agents_empty", table: "agents", postgres: legacyPostgresAgentsShape(false), sqlite: legacySQLiteAgentsShape(false)},
		{name: "agents_populated", table: "agents", postgres: legacyPostgresAgentsShape(true), sqlite: legacySQLiteAgentsShape(true), populated: true},
		{name: "agent_turns_empty", table: "agent_turns", postgres: legacyPostgresAgentTurnsShape(false), sqlite: legacySQLiteAgentTurnsShape(false)},
		{name: "agent_turns_populated", table: "agent_turns", postgres: legacyPostgresAgentTurnsShape(true), sqlite: legacySQLiteAgentTurnsShape(true), populated: true},
		{name: "entity_state_empty", table: "entity_state", postgres: []string{`ALTER TABLE entity_state ADD COLUMN subject_id TEXT`}, sqlite: []string{`ALTER TABLE entity_state ADD COLUMN subject_id TEXT`}},
		{name: "entity_state_populated", table: "entity_state", postgres: legacyPostgresEntityStateShape(), sqlite: legacySQLiteEntityStateShape(), populated: true},
	}

	for _, backend := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		for _, tc := range cases {
			t.Run(string(backend)+"/"+tc.name, func(t *testing.T) {
				request := canonicalSchemaBootstrapTestRequest(t)
				db, bootstrap := legacyShapeTestStore(t, backend, request)
				statements := tc.postgres
				if backend == SchemaDialectSQLite {
					statements = tc.sqlite
				}
				for _, statement := range statements {
					if _, err := db.ExecContext(testAuthorActivityContext(), statement); err != nil {
						t.Fatalf("apply legacy shape statement %q: %v", statement, err)
					}
				}
				beforeColumns := selectedStoreTableColumns(t, backend, db, tc.table)
				beforeRows := selectedStoreTableRowCount(t, db, tc.table)
				if tc.populated && beforeRows == 0 {
					t.Fatalf("legacy shape %s was not populated", tc.name)
				}

				err := bootstrap(testAuthorActivityContext(), request)
				var incompatible *SchemaCompatibilityError
				if !errors.As(err, &incompatible) {
					t.Fatalf("bootstrap error = %v, want SchemaCompatibilityError", err)
				}
				afterColumns := selectedStoreTableColumns(t, backend, db, tc.table)
				if !reflect.DeepEqual(afterColumns, beforeColumns) {
					t.Fatalf("legacy %s columns changed during rejected bootstrap: before=%v after=%v", tc.table, beforeColumns, afterColumns)
				}
				if afterRows := selectedStoreTableRowCount(t, db, tc.table); afterRows != beforeRows {
					t.Fatalf("legacy %s rows changed during rejected bootstrap: before=%d after=%d", tc.table, beforeRows, afterRows)
				}
			})
		}
	}
}

func TestSchemaBootstrapAcceptsUnrelatedTablesUnchanged(t *testing.T) {
	for _, backend := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			request := canonicalSchemaBootstrapTestRequest(t)
			db, bootstrap := legacyShapeTestStore(t, backend, request)
			if _, err := db.ExecContext(testAuthorActivityContext(), `CREATE TABLE product_probe (id TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(testAuthorActivityContext(), `INSERT INTO product_probe (id, value) VALUES ('kept', 'unchanged')`); err != nil {
				t.Fatal(err)
			}
			if err := bootstrap(testAuthorActivityContext(), request); err != nil {
				t.Fatalf("bootstrap with unrelated table: %v", err)
			}
			var value string
			if err := db.QueryRowContext(testAuthorActivityContext(), `SELECT value FROM product_probe WHERE id = 'kept'`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != "unchanged" {
				t.Fatalf("unrelated table value = %q, want unchanged", value)
			}
		})
	}
}

func TestSchemaBootstrapRejectsUnstampedStoresUnchanged(t *testing.T) {
	for _, backend := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			request := canonicalSchemaBootstrapTestRequest(t)
			var db *sql.DB
			var bootstrap func(context.Context, SchemaBootstrapRequest) error
			if backend == SchemaDialectSQLite {
				store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "unstamped.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				db, bootstrap = store.DB, store.BootstrapSchema
			} else {
				_, postgresDB, cleanup := testutil.StartEmptyPostgres(t)
				t.Cleanup(cleanup)
				store := &PostgresStore{DB: postgresDB}
				db, bootstrap = postgresDB, store.BootstrapSchema
			}
			if _, err := db.ExecContext(testAuthorActivityContext(), `CREATE TABLE product_probe (id TEXT PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			if err := bootstrap(testAuthorActivityContext(), request); err == nil {
				t.Fatal("unstamped non-empty store bootstrap error = nil")
			}
			assertSchemaTableExists(t, backend, db, "product_probe", true)
			assertSchemaTableExists(t, backend, db, RuntimeStoreMetadataTable, false)
		})
	}
}

func legacyShapeTestStore(t *testing.T, backend SchemaDialect, request SchemaBootstrapRequest) (*sql.DB, func(context.Context, SchemaBootstrapRequest) error) {
	t.Helper()
	if backend == SchemaDialectSQLite {
		path := filepath.Join(t.TempDir(), "legacy-shape.db")
		accepted, err := NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = accepted.Close() })
		if err := accepted.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
			t.Fatalf("bootstrap canonical SQLite store: %v", err)
		}
		candidate, err := NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = candidate.Close() })
		return accepted.DB, candidate.BootstrapSchema
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	accepted := &PostgresStore{DB: db}
	if err := accepted.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("bootstrap canonical PostgreSQL store: %v", err)
	}
	candidate := &PostgresStore{DB: db}
	return db, candidate.BootstrapSchema
}

func selectedStoreTableColumns(t *testing.T, backend SchemaDialect, db *sql.DB, table string) map[string]bool {
	t.Helper()
	if backend == SchemaDialectSQLite {
		return sqliteColumnSet(t, testAuthorActivityContext(), db, table)
	}
	rows, err := db.QueryContext(testAuthorActivityContext(), `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
	`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
		columns[column] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}

func selectedStoreTableRowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(testAuthorActivityContext(), `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func legacyPostgresAgentsShape(populated bool) []string {
	statements := []string{
		`DROP TABLE agents CASCADE`,
		`CREATE TABLE agents (agent_id TEXT PRIMARY KEY, role TEXT NOT NULL, model_tier TEXT, llm_backend TEXT NOT NULL, config JSONB NOT NULL DEFAULT '{}'::jsonb, status TEXT)`,
	}
	if populated {
		statements = append(statements, `INSERT INTO agents (agent_id, role, model_tier, llm_backend, status) VALUES ('legacy-agent', 'reviewer', 'sonnet', 'api', 'active')`)
	}
	return statements
}

func legacySQLiteAgentsShape(populated bool) []string {
	statements := []string{
		`DROP TABLE agents`,
		`CREATE TABLE agents (agent_id TEXT PRIMARY KEY, role TEXT NOT NULL, model_tier TEXT, llm_backend TEXT NOT NULL, config TEXT NOT NULL DEFAULT '{}', status TEXT)`,
	}
	if populated {
		statements = append(statements, `INSERT INTO agents (agent_id, role, model_tier, llm_backend, status) VALUES ('legacy-agent', 'reviewer', 'sonnet', 'api', 'active')`)
	}
	return statements
}

func legacyPostgresAgentTurnsShape(populated bool) []string {
	statements := []string{`DROP TABLE agent_turns`, `CREATE TABLE agent_turns (turn_id UUID PRIMARY KEY, error TEXT)`}
	if populated {
		statements = append(statements, `INSERT INTO agent_turns (turn_id, error) VALUES ('00000000-0000-0000-0000-000000002055'::uuid, 'opaque provider failure')`)
	}
	return statements
}

func legacySQLiteAgentTurnsShape(populated bool) []string {
	statements := []string{`DROP TABLE agent_turns`, `CREATE TABLE agent_turns (turn_id TEXT PRIMARY KEY, error TEXT)`}
	if populated {
		statements = append(statements, `INSERT INTO agent_turns (turn_id, error) VALUES ('00000000-0000-0000-0000-000000002055', 'opaque provider failure')`)
	}
	return statements
}

func legacyPostgresEntityStateShape() []string {
	return []string{
		`ALTER TABLE entity_state ADD COLUMN subject_id TEXT`,
		`INSERT INTO runs (run_id, status) VALUES ('00000000-0000-0000-0000-000000002055'::uuid, 'running')`,
		`INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state, subject_id) VALUES ('00000000-0000-0000-0000-000000002055'::uuid, '00000000-0000-0000-0000-000000002056'::uuid, 'legacy/one', 'active', 'subject-1')`,
	}
}

func legacySQLiteEntityStateShape() []string {
	return []string{
		`ALTER TABLE entity_state ADD COLUMN subject_id TEXT`,
		`INSERT INTO runs (run_id, status) VALUES ('00000000-0000-0000-0000-000000002055', 'running')`,
		`INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state, subject_id) VALUES ('00000000-0000-0000-0000-000000002055', '00000000-0000-0000-0000-000000002056', 'legacy/one', 'active', 'subject-1')`,
	}
}

func TestSQLiteSchemaBootstrapFreshSecondBootAndDriftRejection(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "dev.db")
	first, err := NewSQLiteRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	if err := first.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("fresh bootstrap: %v", err)
	}
	var createdAt string
	if err := first.DB.QueryRow(`SELECT created_at FROM runtime_store_metadata WHERE id=1`).Scan(&createdAt); err != nil {
		t.Fatal(err)
	}
	request.Origin.SwarmVersion = "later-build"
	request.Origin.CreatedAt = request.Origin.CreatedAt.Add(time.Hour)
	if err := first.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("compatible second boot: %v", err)
	}
	var after string
	if err := first.DB.QueryRow(`SELECT created_at FROM runtime_store_metadata WHERE id=1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != createdAt {
		t.Fatalf("origin stamp changed on second boot: %q -> %q", createdAt, after)
	}
	if _, err := first.DB.Exec(`ALTER TABLE timers ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	err = first.BootstrapSchema(testAuthorActivityContext(), request)
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("drift bootstrap error = %v, want SchemaCompatibilityError", err)
	}
}

func TestSQLiteSchemaBootstrapRejectsUnstampedWithoutWALMutation(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "legacy.db")
	store, err := NewSQLiteRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.DB.Exec(`CREATE TABLE legacy_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err == nil {
		t.Fatal("unstamped non-empty store was accepted")
	}
	var mode string
	if err := store.DB.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode == "wal" {
		t.Fatal("incompatible preflight changed journal mode to WAL")
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
			t.Fatalf("incompatible preflight created sidecar %s", sidecar)
		}
	}
}

func TestSQLiteSchemaBootstrapConcurrentCreatorsConverge(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "concurrent.db")
	stores := make([]*SQLiteRuntimeStore, 2)
	for i := range stores {
		var err error
		stores[i], err = NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		current := stores[i]
		t.Cleanup(func() { _ = current.Close() })
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = stores[i].BootstrapSchema(testAuthorActivityContext(), request)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent bootstrap: %v", err)
		}
	}
	var count int
	if err := stores[0].DB.QueryRow(`SELECT COUNT(*) FROM runtime_store_metadata`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("origin rows = %d, err=%v", count, err)
	}
}

func TestPostgresSchemaBootstrapAcceptsCanonicalTemplateAndRejectsDrift(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("compatible template boot: %v", err)
	}
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("repeated compatible template boot: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE timers ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	err := store.BootstrapSchema(testAuthorActivityContext(), request)
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("drift bootstrap error = %v, want SchemaCompatibilityError", err)
	}
}

func TestSchemaBootstrapRejectsStageOnlyDecisionCardStoreBeforeMutation(t *testing.T) {
	current := canonicalSchemaBootstrapTestRequest(t)
	legacy := stageOnlyDecisionCardSchemaRequest(t)
	legacy.Origin.CreatedAt = legacy.Origin.CreatedAt.Truncate(time.Microsecond).Add(789 * time.Nanosecond)

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "stage-only.db")
		store, err := NewSQLiteRuntimeStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.BootstrapSchema(testAuthorActivityContext(), legacy); err != nil {
			t.Fatalf("bootstrap stage-only fixture: %v", err)
		}

		assertStageOnlyDecisionCardColumns(t, sqliteColumnSet(t, testAuthorActivityContext(), store.DB, "decision_cards"))
		assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(testAuthorActivityContext(), current), SchemaDialectSQLite, path, current.Origin, &legacy.Origin, "decision_cards", "anchor_kind", "human_task_continuations", "proposed_effect_continuations")
		assertStageOnlyDecisionCardColumns(t, sqliteColumnSet(t, testAuthorActivityContext(), store.DB, "decision_cards"))
		var continuations int
		if err := store.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='human_task_continuations'`).Scan(&continuations); err != nil {
			t.Fatal(err)
		}
		if continuations != 0 {
			t.Fatal("incompatible bootstrap created human_task_continuations")
		}
		if err := store.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proposed_effect_continuations'`).Scan(&continuations); err != nil {
			t.Fatal(err)
		}
		if continuations != 0 {
			t.Fatal("incompatible bootstrap created proposed_effect_continuations")
		}
	})

	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartEmptyPostgres(t)
		t.Cleanup(cleanup)
		store := &PostgresStore{DB: db}
		if err := store.BootstrapSchema(testAuthorActivityContext(), legacy); err != nil {
			t.Fatalf("bootstrap stage-only fixture: %v", err)
		}
		var target string
		if err := db.QueryRow(`SELECT current_database()`).Scan(&target); err != nil {
			t.Fatal(err)
		}
		if !postgresColumnExists(t, testAuthorActivityContext(), db, "decision_cards", "flow_instance") || postgresColumnExists(t, testAuthorActivityContext(), db, "decision_cards", "anchor_kind") {
			t.Fatal("stage-only PostgreSQL fixture has unexpected decision-card columns")
		}
		assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(testAuthorActivityContext(), current), SchemaDialectPostgres, target, current.Origin, &legacy.Origin, "decision_cards", "anchor_kind", "human_task_continuations", "proposed_effect_continuations")
		if !postgresColumnExists(t, testAuthorActivityContext(), db, "decision_cards", "flow_instance") || postgresColumnExists(t, testAuthorActivityContext(), db, "decision_cards", "anchor_kind") {
			t.Fatal("incompatible bootstrap mutated stage-only decision-card columns")
		}
		var continuations bool
		if err := db.QueryRow(`SELECT to_regclass('public.human_task_continuations') IS NOT NULL`).Scan(&continuations); err != nil {
			t.Fatal(err)
		}
		if continuations {
			t.Fatal("incompatible bootstrap created human_task_continuations")
		}
		if err := db.QueryRow(`SELECT to_regclass('public.proposed_effect_continuations') IS NOT NULL`).Scan(&continuations); err != nil {
			t.Fatal(err)
		}
		if continuations {
			t.Fatal("incompatible bootstrap created proposed_effect_continuations")
		}
	})
}

func assertStageOnlyDecisionCardColumns(t testing.TB, columns map[string]bool) {
	t.Helper()
	if !columns["flow_instance"] || !columns["stage_activation_id"] || columns["anchor_kind"] || columns["anchor"] {
		t.Fatalf("decision-card columns are not the stage-only fixture: %#v", columns)
	}
}

func sqliteColumnSet(t testing.TB, ctx context.Context, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		t.Fatalf("inspect SQLite table %s: %v", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan SQLite table %s: %v", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("inspect SQLite table %s: %v", table, err)
	}
	return columns
}

func postgresColumnExists(t testing.TB, ctx context.Context, db *sql.DB, table, column string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
		)
	`, table, column).Scan(&exists); err != nil {
		t.Fatalf("inspect PostgreSQL column %s.%s: %v", table, column, err)
	}
	return exists
}

func stageOnlyDecisionCardSchemaRequest(t testing.TB) SchemaBootstrapRequest {
	t.Helper()
	request := canonicalSchemaBootstrapTestRequest(t)
	request.Origin.SwarmVersion = "stage-only-card-schema"
	plans := make([]SchemaTableDDL, 0, len(request.PlatformPlans)-2)
	for _, plan := range request.PlatformPlans {
		switch plan.TableName {
		case "human_task_continuations", "proposed_effect_continuations":
			continue
		case "decision_cards":
			plan = stageOnlyDecisionCardTablePlan()
		}
		plans = append(plans, plan)
	}
	request.PlatformPlans = plans
	return request
}

func stageOnlyDecisionCardTablePlan() SchemaTableDDL {
	return SchemaTableDDL{
		TableName: "decision_cards", SchemaKind: "platform_spec", ColumnCount: 27,
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS decision_cards (
    card_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    flow_instance TEXT NOT NULL,
    flow_id TEXT,
    entity_id UUID NOT NULL,
    stage TEXT NOT NULL,
    stage_activation_id UUID NOT NULL,
    decision_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'decided', 'superseded')),
    snapshot JSONB NOT NULL,
    card_content_hash TEXT NOT NULL,
    decision_schema_hash TEXT NOT NULL,
    bundle_hash TEXT NOT NULL,
    workflow_version TEXT,
    effective_cadence JSONB NOT NULL DEFAULT '{}',
    provenance JSONB NOT NULL DEFAULT '{}',
    verdict TEXT,
    fields JSONB NOT NULL DEFAULT '{}',
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    deferred_until TIMESTAMPTZ,
    decision_event_id UUID,
    delivery_receipt_id TEXT,
    delivery_render_hash TEXT,
    superseded_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (run_id, flow_instance, entity_id, stage_activation_id, decision_id),
    CHECK ((status = 'pending' AND verdict IS NULL AND superseded_reason IS NULL) OR (status = 'decided' AND verdict IS NOT NULL AND decided_at IS NOT NULL AND decision_event_id IS NOT NULL) OR (status = 'superseded' AND superseded_reason IS NOT NULL))
)`,
			`CREATE INDEX IF NOT EXISTS idx_decision_cards_mailbox ON decision_cards (status, deferred_until, created_at)`,
			`CREATE INDEX IF NOT EXISTS idx_decision_cards_run ON decision_cards (run_id, created_at)`,
			`CREATE INDEX IF NOT EXISTS idx_decision_cards_entity ON decision_cards (run_id, entity_id, stage_activation_id)`,
		},
	}
}

func TestSchemaBootstrapRejectsUnexpectedIndex(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	const createUnexpectedIndex = `CREATE UNIQUE INDEX drift_probe_idx ON timers(status) WHERE status = 'pending'`

	t.Run("sqlite", func(t *testing.T) {
		store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "unexpected-index.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
			t.Fatalf("fresh bootstrap: %v", err)
		}
		if _, err := store.DB.Exec(createUnexpectedIndex); err != nil {
			t.Fatal(err)
		}
		assertUnexpectedIndexRejected(t, store.BootstrapSchema(testAuthorActivityContext(), request))
	})

	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		store := &PostgresStore{DB: db}
		if _, err := db.Exec(createUnexpectedIndex); err != nil {
			t.Fatal(err)
		}
		assertUnexpectedIndexRejected(t, store.BootstrapSchema(testAuthorActivityContext(), request))
	})
}

func assertUnexpectedIndexRejected(t *testing.T, err error) {
	t.Helper()
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("unexpected-index bootstrap error = %v, want SchemaCompatibilityError", err)
	}
	if !strings.Contains(err.Error(), "unexpected index drift_probe_idx") {
		t.Fatalf("unexpected-index bootstrap error = %v, want named index drift", err)
	}
}

func TestSQLiteSchemaBootstrapRejectsMalformedStoredOrigin(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	path := filepath.Join(t.TempDir(), "malformed-origin.db")
	store, err := NewSQLiteRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("fresh bootstrap: %v", err)
	}
	cases := []malformedStoredOriginCase{
		{name: "blank swarm version", update: `UPDATE runtime_store_metadata SET swarm_version = ' ' WHERE id = 1`, wantDrift: "stored Swarm version is required"},
		{name: "blank platform version", update: `UPDATE runtime_store_metadata SET platform_version = ' ' WHERE id = 1`, wantDrift: "stored platform version is required"},
		{name: "zero creation time", update: `UPDATE runtime_store_metadata SET created_at = '0001-01-01T00:00:00Z' WHERE id = 1`, wantDrift: "stored creation time must be non-zero"},
		{name: "invalid creation time", update: `UPDATE runtime_store_metadata SET created_at = 'not-a-time' WHERE id = 1`, wantDrift: "parse runtime store creation time"},
	}
	assertMalformedStoredOrigins(t, cases, func(statement string) error {
		_, err := store.DB.Exec(statement)
		return err
	}, func() error {
		_, err := store.DB.Exec(`UPDATE runtime_store_metadata SET swarm_version = ?, platform_version = ?, created_at = ? WHERE id = 1`, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt.UTC().Format(time.RFC3339Nano))
		return err
	}, func() error {
		return store.BootstrapSchema(testAuthorActivityContext(), request)
	}, SchemaDialectSQLite, path, request.Origin)
}

func TestPostgresSchemaBootstrapRejectsMalformedStoredOrigin(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	var target string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	cases := []malformedStoredOriginCase{
		{name: "blank swarm version", update: `UPDATE runtime_store_metadata SET swarm_version = ' ' WHERE id = 1`, wantDrift: "stored Swarm version is required"},
		{name: "blank platform version", update: `UPDATE runtime_store_metadata SET platform_version = ' ' WHERE id = 1`, wantDrift: "stored platform version is required"},
		{name: "zero creation time", update: `UPDATE runtime_store_metadata SET created_at = TIMESTAMPTZ '0001-01-01 00:00:00+00' WHERE id = 1`, wantDrift: "stored creation time must be non-zero"},
	}
	assertMalformedStoredOrigins(t, cases, func(statement string) error {
		_, err := db.Exec(statement)
		return err
	}, func() error {
		_, err := db.Exec(`UPDATE runtime_store_metadata SET swarm_version = $1, platform_version = $2, created_at = $3 WHERE id = 1`, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt)
		return err
	}, func() error {
		return store.BootstrapSchema(testAuthorActivityContext(), request)
	}, SchemaDialectPostgres, target, request.Origin)
}

type malformedStoredOriginCase struct {
	name      string
	update    string
	wantDrift string
}

func assertMalformedStoredOrigins(
	t *testing.T,
	cases []malformedStoredOriginCase,
	update func(string) error,
	restore func() error,
	bootstrap func() error,
	backend SchemaDialect,
	target string,
	current RuntimeStoreOrigin,
) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := update(tc.update); err != nil {
				t.Fatal(err)
			}
			bootstrapErr := bootstrap()
			if err := restore(); err != nil {
				t.Fatalf("restore canonical origin: %v", err)
			}
			assertSchemaCompatibilityDiagnostic(t, bootstrapErr, backend, target, current, nil, tc.wantDrift)
		})
	}
}

func TestPostgresSchemaBootstrapConcurrentFreshCreatorsConverge(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	dsn, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	second, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.DB.Close() })
	stores := []*PostgresStore{{DB: db}, second}
	errs := make([]error, len(stores))
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = stores[i].BootstrapSchema(testAuthorActivityContext(), request)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent postgres bootstrap: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_store_metadata`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("origin rows = %d, err=%v", count, err)
	}
}

func TestPostgresSchemaBootstrapRejectsBeforeExtensionOrSchemaMutation(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE legacy_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	store := &PostgresStore{DB: db}
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err == nil {
		t.Fatal("unstamped non-empty PostgreSQL store was accepted")
	}
	var extensionExists bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto')`).Scan(&extensionExists); err != nil {
		t.Fatal(err)
	}
	if extensionExists {
		t.Fatal("incompatible preflight created pgcrypto")
	}
	var columns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='legacy_state'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 1 {
		t.Fatalf("incompatible preflight mutated legacy_state columns: %d", columns)
	}
}

func TestSchemaBootstrapRollsBackFailedFreshCreation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			request := canonicalSchemaBootstrapTestRequest(t)
			for i := range request.PlatformPlans {
				if request.PlatformPlans[i].TableName != RuntimeStoreMetadataTable {
					continue
				}
				statements := append([]string(nil), request.PlatformPlans[i].Statements...)
				statements[0] = strings.TrimSuffix(strings.TrimSpace(statements[0]), ")") + ", CHECK (swarm_version = 'allowed'))"
				request.PlatformPlans[i].Statements = statements
				break
			}
			if backend == "sqlite" {
				store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "rollback.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				if err := store.BootstrapSchema(testAuthorActivityContext(), request); err == nil {
					t.Fatal("invalid fresh bootstrap succeeded")
				}
				tables, err := sqliteUserTables(testAuthorActivityContext(), store.DB)
				if err != nil || len(tables) != 0 {
					t.Fatalf("failed SQLite bootstrap left objects %v, err=%v", tables, err)
				}
				return
			}
			_, db, cleanup := testutil.StartEmptyPostgres(t)
			t.Cleanup(cleanup)
			store := &PostgresStore{DB: db}
			if err := store.BootstrapSchema(testAuthorActivityContext(), request); err == nil {
				t.Fatal("invalid fresh bootstrap succeeded")
			}
			tables, err := postgresPublicTables(testAuthorActivityContext(), db)
			if err != nil || len(tables) != 0 {
				t.Fatalf("failed PostgreSQL bootstrap left objects %v, err=%v", tables, err)
			}
		})
	}
}

func TestSQLiteSchemaBootstrapCreatesThenValidatesGeneratedState(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	storedOrigin, err := readRuntimeStoreOrigin(testAuthorActivityContext(), store.DB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(testAuthorActivityContext(), request), SchemaDialectSQLite, store.path, request.Origin, storedOrigin, "generated state table generated_probe_state:", "drift_probe")
	if !sqliteColumnSet(t, testAuthorActivityContext(), store.DB, "generated_probe_state")["drift_probe"] {
		t.Fatal("rejected SQLite generated-state bootstrap mutated the incompatible table")
	}
}

func TestPostgresSchemaBootstrapCreatesThenValidatesGeneratedState(t *testing.T) {
	request := canonicalSchemaBootstrapTestRequest(t)
	request.StatePlans = generatedProbeStatePlans()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	var target string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(testAuthorActivityContext(), request); err != nil {
		t.Fatalf("create generated state: %v", err)
	}
	storedOrigin, err := readRuntimeStoreOrigin(testAuthorActivityContext(), db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE generated_probe_state ADD COLUMN drift_probe TEXT`); err != nil {
		t.Fatal(err)
	}
	assertSchemaCompatibilityDiagnostic(t, store.BootstrapSchema(testAuthorActivityContext(), request), SchemaDialectPostgres, target, request.Origin, storedOrigin, "generated state table generated_probe_state:", "drift_probe")
	if !postgresColumnExists(t, testAuthorActivityContext(), db, "generated_probe_state", "drift_probe") {
		t.Fatal("rejected PostgreSQL generated-state bootstrap mutated the incompatible table")
	}
}

func assertSchemaCompatibilityDiagnostic(t *testing.T, err error, backend SchemaDialect, target string, current RuntimeStoreOrigin, wantOrigin *RuntimeStoreOrigin, wantDrift ...string) {
	t.Helper()
	current = current.canonical()
	if wantOrigin != nil {
		canonical := wantOrigin.canonical()
		wantOrigin = &canonical
	}
	var incompatible *SchemaCompatibilityError
	if !errors.As(err, &incompatible) {
		t.Fatalf("bootstrap error = %v (%T), want SchemaCompatibilityError", err, err)
	}
	if incompatible.Backend != backend || incompatible.Target != target {
		t.Fatalf("diagnostic backend/target = %s/%q, want %s/%q", incompatible.Backend, incompatible.Target, backend, target)
	}
	if incompatible.Current != current {
		t.Fatalf("diagnostic current origin = %#v, want %#v", incompatible.Current, current)
	}
	if wantOrigin != nil {
		if incompatible.Origin == nil || *incompatible.Origin != *wantOrigin {
			t.Fatalf("diagnostic stored origin = %#v, want %#v", incompatible.Origin, wantOrigin)
		}
	} else if incompatible.Origin != nil {
		t.Fatalf("malformed stored origin was exposed as valid evidence: %#v", incompatible.Origin)
	}
	drift := strings.Join(incompatible.Drift, "; ")
	for _, want := range wantDrift {
		if !strings.Contains(drift, want) {
			t.Fatalf("diagnostic drift = %v, want evidence %q", incompatible.Drift, want)
		}
	}
	wantRemediation := "create and select a fresh PostgreSQL database (for example: createdb swarm_fresh, then set database.name to swarm_fresh)"
	if backend == SchemaDialectSQLite {
		wantRemediation = "stop Swarm and remove the incompatible local store with: rm -f -- " + shellQuote(target) + " " + shellQuote(target+"-wal") + " " + shellQuote(target+"-shm")
	}
	if !strings.Contains(err.Error(), "remediation: "+wantRemediation) {
		t.Fatalf("diagnostic = %v, want remediation %q", err, wantRemediation)
	}
}

func generatedProbeStatePlans() []SchemaTableDDL {
	return []SchemaTableDDL{{
		TableName:  "generated_probe_state",
		SchemaKind: "state_schema",
		Statements: []string{`CREATE TABLE IF NOT EXISTS generated_probe_state (
    "entity_id" UUID NOT NULL,
    "node_id" TEXT NOT NULL,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "value" TEXT,
    PRIMARY KEY ("entity_id", "node_id")
)`},
	}}
}

func canonicalSchemaBootstrapTestRequest(t testing.TB) SchemaBootstrapRequest {
	t.Helper()
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(platform.PlatformSpecYAML(), &spec); err != nil {
		t.Fatal(err)
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	return SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        RuntimeStoreOrigin{SwarmVersion: "schema-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}
}
