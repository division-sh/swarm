package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestRunInitSubcommand_ConfigLoadFailure(t *testing.T) {
	err := runInitSubcommand([]string{"--config", filepath.Join(t.TempDir(), "missing.yaml")})
	if err == nil {
		t.Fatal("expected config load failure")
	}
}

func TestRunInitSubcommand_TemplateCompileFailure(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	// Force template compilation failure after stores are built/migrations applied.
	err := runInitSubcommand([]string{
		"--config", cfgPath,
		"--migration-file", filepath.Join(root, "contracts", "ddl-canonical.sql"),
		"--template-agents-dir", filepath.Join(t.TempDir(), "missing-template-agents"),
		"--template-routes-yaml", filepath.Join(t.TempDir(), "missing-routes.yaml"),
	})
	if err == nil {
		t.Fatal("expected template compile failure")
	}
}

func TestEnsureInitialTemplateCLI_PublishFromRepoYAML(t *testing.T) {
	root := repoRootFromCmd(t)
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	pg := &store.PostgresStore{DB: db}
	if err := ensureInitialTemplateCLI(
		ctx,
		db,
		pg,
		"test-init-template",
		filepath.Join(root, "configs", "agents", "templates"),
		filepath.Join(root, "configs", "agents", "templates", "routes.yaml"),
	); err != nil {
		t.Fatalf("ensureInitialTemplateCLI publish: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM org_templates WHERE version = 'test-init-template'`).Scan(&count); err != nil {
		t.Fatalf("query org_templates: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected published template row, got %d", count)
	}
}

func TestSeedGlobalAgentsFromYAML_PreservesExistingFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), []byte(`
agents:
  empire-coordinator:
    config_path: empire-coordinator.yaml
`), 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
if err := os.WriteFile(filepath.Join(dir, "empire-coordinator.yaml"), []byte(`
id: empire-coordinator
role: empire-coordinator
mode: holding
tools: ["mailbox_send"]
subscriptions: ["system.started"]
`), 0o644); err != nil {
		t.Fatalf("write agent yaml: %v", err)
	}

	startedAt := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			id, type, role, mode, status, hired_by, template_version, config, started_at, last_active_at
		) VALUES (
			'empire-coordinator', 'sonnet', 'empire-coordinator', 'holding', 'paused', 'human-seed', '2.0.1', '{}'::jsonb, $1, now()
		)
	`, startedAt); err != nil {
		t.Fatalf("seed existing agent: %v", err)
	}

	if err := seedGlobalAgentsFromYAML(ctx, pg, dir); err != nil {
		t.Fatalf("seedGlobalAgentsFromYAML: %v", err)
	}

	var status, hiredBy, templateVersion string
	var persistedStart time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT status, hired_by, COALESCE(template_version,''), started_at
		FROM agents
		WHERE id = 'empire-coordinator'
	`).Scan(&status, &hiredBy, &templateVersion, &persistedStart); err != nil {
		t.Fatalf("query agent: %v", err)
	}
	if status != "paused" {
		t.Fatalf("expected status preserved as paused, got %q", status)
	}
	if hiredBy != "human-seed" {
		t.Fatalf("expected hired_by preserved, got %q", hiredBy)
	}
	if templateVersion != "2.0.1" {
		t.Fatalf("expected template_version preserved, got %q", templateVersion)
	}
	if !persistedStart.Equal(startedAt) {
		t.Fatalf("expected started_at preserved, got %s want %s", persistedStart, startedAt)
	}
}

func TestInitHTTPServer_InvalidAddressBranch(t *testing.T) {
	initHTTPServer("127.0.0.1:bad-port", httpNoopHandler{}, "invalid")
	time.Sleep(20 * time.Millisecond)
}

type httpNoopHandler struct{}

func (httpNoopHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}
