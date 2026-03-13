package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func portFromDSN(t *testing.T, dsn string) int {
	t.Helper()
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "port=") {
			var n int
			fmtSscanf(strings.TrimPrefix(part, "port="), &n)
			if n > 0 {
				return n
			}
		}
	}
	t.Fatalf("port not found in dsn: %q", dsn)
	return 0
}

func dbNameFromDSN(t *testing.T, dsn string) string {
	t.Helper()
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			return strings.TrimPrefix(part, "dbname=")
		}
	}
	t.Fatalf("dbname not found in dsn: %q", dsn)
	return ""
}

// Small local helper to avoid importing fmt (keeps this file tiny in coverage terms).
func fmtSscanf(s string, out *int) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	*out = n
}

func TestPostgresStore_HelpersAndDigest(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := portFromDSN(t, dsn)
	dbName := dbNameFromDSN(t, dsn)

	cfg := config.DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     port,
		Name:     dbName,
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
		PoolSize: 5,
	}
	gotDSN := DSNFromConfig(cfg)
	if !strings.Contains(gotDSN, "host=127.0.0.1") || !strings.Contains(gotDSN, "dbname="+dbName) {
		t.Fatalf("unexpected dsn: %q", gotDSN)
	}
	pg, err := NewPostgresStore(gotDSN)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	ctx := context.Background()
	if err := pg.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// ApplyMigrationFile should execute SQL from disk.
	tmp := t.TempDir()
	path := tmp + "/x.sql"
	if err := osWriteFile(path, "CREATE TABLE IF NOT EXISTS t_cov (id INT);\n"); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	if err := pg.ApplyMigrationFile(ctx, path); err != nil {
		t.Fatalf("ApplyMigrationFile: %v", err)
	}

	// Seed entities for digest coverage.
	entityID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entities (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','approved','entity', now(), now())
	`, entityID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'active',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{"slug":"testco","name":"TestCo"}'::jsonb, now(), now()
		)
	`, entityID); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	// Active count includes active workflow instances.
	if n, err := pg.CountActiveInstances(ctx); err != nil || n < 1 {
		t.Fatalf("CountActiveInstances n=%d err=%v", n, err)
	}

	// Digest rows: metrics + spend.
	now := time.Now().UTC()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_metrics (id, entity_id, period_start, period_end, users_total, mrr_cents, api_cost_cents, infra_cost_cents, created_at)
		VALUES ($1::uuid,$2::uuid,$3,$4,10,1234,0,0, now())
	`, uuid.NewString(), entityID, now.Add(-24*time.Hour), now); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, entity_id, category, amount_cents, description, approved_by, created_at)
		VALUES ($1::uuid,$2::uuid,'api',500,'test','estimated', now())
	`, uuid.NewString(), entityID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}
	if rows, err := pg.ListInstanceDigestRows(ctx, 10); err != nil || len(rows) == 0 {
		t.Fatalf("ListInstanceDigestRows err=%v len=%d", err, len(rows))
	}

	// Active agent ids.
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a','stub','a','global','active','{}'::jsonb, now(), now()),
		       ('t','stub','t','global','terminated','{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}
	ids, err := pg.ListActiveAgentIDs(ctx)
	if err != nil || len(ids) == 0 {
		t.Fatalf("ListActiveAgentIDs err=%v ids=%v", err, ids)
	}

}

func TestPostgresStore_ApplyManagedMigrations(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := portFromDSN(t, dsn)
	dbName := dbNameFromDSN(t, dsn)
	cfg := config.DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     port,
		Name:     dbName,
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
	}
	pg, err := NewPostgresStore(DSNFromConfig(cfg))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	ctx := context.Background()

	tmp := t.TempDir()
	m1 := tmp + "/001.sql"
	m2 := tmp + "/002.sql"
	if err := osWriteFile(m1, "CREATE TABLE IF NOT EXISTS managed_m1 (id INT);\n"); err != nil {
		t.Fatalf("write m1: %v", err)
	}
	if err := osWriteFile(m2, "CREATE TABLE IF NOT EXISTS managed_m2 (id INT);\n"); err != nil {
		t.Fatalf("write m2: %v", err)
	}

	if err := pg.ApplyManagedMigrations(ctx, []MigrationSpec{
		{Version: 2, Name: "m2", Path: m2},
		{Version: 0, Name: "skip", Path: ""},
		{Version: 1, Name: "m1", Path: m1},
	}); err != nil {
		t.Fatalf("ApplyManagedMigrations: %v", err)
	}

	var n int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_version WHERE version IN (1,2)`).Scan(&n); err != nil {
		t.Fatalf("count schema_version: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 applied versions, got %d", n)
	}
	if err := pg.ApplyManagedMigrations(ctx, []MigrationSpec{
		{Version: 1, Name: "m1", Path: m1},
		{Version: 2, Name: "m2", Path: m2},
	}); err != nil {
		t.Fatalf("ApplyManagedMigrations second run: %v", err)
	}
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_version WHERE version IN (1,2)`).Scan(&n); err != nil {
		t.Fatalf("count schema_version second run: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected idempotent version count=2, got %d", n)
	}
}

// Minimal file writer helper (keeps imports down).
func osWriteFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
