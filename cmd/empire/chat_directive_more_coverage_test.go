package main

import (
	"context"
	"testing"

	"empireai/internal/config"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestCLI_ChatAsync_AndResolveTargets(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	// Seed agents used for target resolution and FK constraints (deliveries -> agents.id).
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES
			('empire-coordinator', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now()),
			($1, 'stub', 'opco-ceo', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, "opco-ceo-"+verticalID, verticalID); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	// Async one-shot mode (no stdin loop).
	if err := runChatSubcommand([]string{
		"-config", cfgPath,
		"-store", "postgres",
		"-migrate=false",
		"--async",
		"empire-coordinator",
		"hello",
	}); err != nil {
		t.Fatalf("runChatSubcommand async: %v", err)
	}

	// Slash target resolution uses <vertical>/<agent-alias>.
	if err := runChatSubcommand([]string{
		"-config", cfgPath,
		"-store", "postgres",
		"-migrate=false",
		"--async",
		verticalID + "/ceo",
		"ping",
	}); err != nil {
		t.Fatalf("runChatSubcommand slash target: %v", err)
	}
}

func TestResolveTargetAgent_AmbiguousRoleErrors(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)
	_ = cfgPath

	// Seed two vp-product agents -> ambiguous resolution by role alias.
	v1 := uuid.NewString()
	v2 := uuid.NewString()
	for _, vid := range []string{v1, v2} {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
			VALUES ($1::uuid, 'V', 'v'||substr($1::text,1,6), 'us', 'operating', 'operating', now(), now())
		`, vid); err != nil {
			t.Fatalf("seed vertical: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', 'vp-product', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now())
		`, "vp-product-"+vid, vid); err != nil {
			t.Fatalf("seed agent: %v", err)
		}
	}

	cfg, err := loadConfigForTest(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	if _, err := resolveTargetAgent(context.Background(), stores, "vp-product"); err == nil {
		t.Fatalf("expected ambiguous target error")
	}
}

func TestResolveVerticalID_NotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if _, err := resolveVerticalID(context.Background(), db, "missing"); err == nil {
		t.Fatalf("expected not found error")
	}
}

func loadConfigForTest(path string) (*config.Config, error) {
	return config.Load(path)
}
