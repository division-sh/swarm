package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/config"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestTemplateCommands_ListCurrentDiff(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	ctx := context.Background()
	stores := buildStores(ctx, "postgres", cfg, true, filepath.Join(root, "contracts", "ddl-canonical.sql"))
	if stores.SQLDB == nil {
		t.Fatal("expected postgres db")
	}

	agentsV1 := filepath.Join(t.TempDir(), "agents_v1.json")
	bootstrapV1 := filepath.Join(t.TempDir(), "bootstrap_v1.json")
	seededV1 := filepath.Join(t.TempDir(), "seeded_v1.json")
	if err := os.WriteFile(agentsV1, []byte(`[{"role":"opco-ceo","type":"llm","system_prompt":"x","tools":[],"subscriptions":["board.*"]}]`), 0o644); err != nil {
		t.Fatalf("write agents v1: %v", err)
	}
	if err := os.WriteFile(bootstrapV1, []byte(`[{"event_pattern":"customer_message","subscriber_id":"support-agent","source":"bootstrap","status":"active"}]`), 0o644); err != nil {
		t.Fatalf("write bootstrap v1: %v", err)
	}
	if err := os.WriteFile(seededV1, []byte(`[{"event_pattern":"market_signals","subscriber_id":"chief-of-staff","source":"seeded","status":"active"}]`), 0o644); err != nil {
		t.Fatalf("write seeded v1: %v", err)
	}

	if err := runTemplateSubcommand([]string{
		"publish",
		"--config", cfgPath,
		"--store", "postgres",
		"--version", "t1",
		"--agents-file", agentsV1,
		"--bootstrap-routes-file", bootstrapV1,
		"--seeded-routes-file", seededV1,
	}); err != nil {
		t.Fatalf("template publish t1: %v", err)
	}

	agentsV2 := filepath.Join(t.TempDir(), "agents_v2.json")
	bootstrapV2 := filepath.Join(t.TempDir(), "bootstrap_v2.json")
	seededV2 := filepath.Join(t.TempDir(), "seeded_v2.json")
	if err := os.WriteFile(agentsV2, []byte(`[{"role":"opco-ceo","type":"llm","system_prompt":"x2","tools":[],"subscriptions":["board.*"]},{"role":"vp-product","type":"llm","system_prompt":"y","tools":[],"subscriptions":["product_report"]}]`), 0o644); err != nil {
		t.Fatalf("write agents v2: %v", err)
	}
	if err := os.WriteFile(bootstrapV2, []byte(`[{"event_pattern":"customer_message","subscriber_id":"support-agent","source":"bootstrap","status":"active"},{"event_pattern":"technical_spec_ready","subscriber_id":"cto-agent","source":"bootstrap","status":"active"}]`), 0o644); err != nil {
		t.Fatalf("write bootstrap v2: %v", err)
	}
	if err := os.WriteFile(seededV2, []byte(`[{"event_pattern":"market_signals","subscriber_id":"chief-of-staff","source":"seeded","status":"active"}]`), 0o644); err != nil {
		t.Fatalf("write seeded v2: %v", err)
	}
	if err := runTemplateSubcommand([]string{
		"publish",
		"--config", cfgPath,
		"--store", "postgres",
		"--version", "t2",
		"--agents-file", agentsV2,
		"--bootstrap-routes-file", bootstrapV2,
		"--seeded-routes-file", seededV2,
	}); err != nil {
		t.Fatalf("template publish t2: %v", err)
	}

	if err := runTemplateSubcommand([]string{"list", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("template list: %v", err)
	}
	if err := runTemplateSubcommand([]string{"current", "--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("template current global: %v", err)
	}

	verticalID := uuid.NewString()
	if _, err := stores.SQLDB.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid, 'x', 'x', 'us', 'operating', 'operating', 't1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := runTemplateSubcommand([]string{"current", "--config", cfgPath, "--store", "postgres", "--vertical-id", verticalID}); err != nil {
		t.Fatalf("template current vertical: %v", err)
	}
	if err := runTemplateSubcommand([]string{"diff", "--config", cfgPath, "--store", "postgres", "--from-version", "t1", "--to-version", "t2"}); err != nil {
		t.Fatalf("template diff: %v", err)
	}
}
