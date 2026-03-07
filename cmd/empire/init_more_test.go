package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestEnsureInitialTemplateCLI_ExistsNoop(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('t1','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test', now())
	`); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}
	// Should short-circuit without attempting to compile template YAML.
	if err := ensureInitialTemplateCLI(ctx, db, nil, "t2", "does-not-exist", "does-not-exist"); err != nil {
		t.Fatalf("ensureInitialTemplateCLI noop: %v", err)
	}
}

func TestEnsureInitialTemplateCLI_RequiresDB(t *testing.T) {
	if err := ensureInitialTemplateCLI(context.Background(), nil, nil, "t1", "", ""); err == nil {
		t.Fatalf("expected db unavailable error")
	}
}

func TestSeedGlobalAgentsFromYAML_Persists(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), []byte(`
agents:
  empire-coordinator:
    config_path: empire-coordinator.yaml
  factory-cto:
    config_path: factory-cto.yaml
`), 0644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "empire-coordinator.yaml"), []byte(`
id: empire-coordinator
role: empire-coordinator
mode: holding
tools: ["agent_message"]
subscriptions: ["system.*"]
`), 0644); err != nil {
		t.Fatalf("write empire-coordinator: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "factory-cto.yaml"), []byte(`
id: factory-cto
role: factory-cto
mode: factory
tools: ["agent_message"]
subscriptions: ["factory.*"]
`), 0644); err != nil {
		t.Fatalf("write factory-cto: %v", err)
	}

	if err := seedGlobalAgentsFromYAML(ctx, pg, dir); err != nil {
		t.Fatalf("seedGlobalAgentsFromYAML: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE id IN ('empire-coordinator','factory-cto')`).Scan(&n); err != nil {
		t.Fatalf("count agents: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 agents, got %d", n)
	}
}

func TestPersistRuntimeConfig_Branches(t *testing.T) {
	ctx := context.Background()
	if err := persistRuntimeConfig(ctx, nil, "x"); err != nil {
		t.Fatalf("db nil should be noop: %v", err)
	}

	_, db, _ := testutil.StartPostgres(t)
	if err := persistRuntimeConfig(ctx, db, filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatalf("expected read error")
	}

	cfgPath := filepath.Join(t.TempDir(), "empire.yaml")
	if err := os.WriteFile(cfgPath, []byte("llm:\n  runtime_mode: cli_test\n"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := persistRuntimeConfig(ctx, db, cfgPath); err != nil {
		t.Fatalf("persistRuntimeConfig: %v", err)
	}
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_config`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected runtime_config row")
	}
}

func TestInitHTTPServer_Noops(t *testing.T) {
	initHTTPServer("", nil, "x")
	initHTTPServer("127.0.0.1:0", nil, "x")
	initHTTPServer("", http.NewServeMux(), "x")
}
