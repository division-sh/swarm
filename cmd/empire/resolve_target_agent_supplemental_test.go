package main

import (
	"context"
	"encoding/json"
	"testing"

	"empireai/internal/config"
	models "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestResolveTargetAgent_CoversIDRoleAmbiguityAndFallback(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	cfgPath := writeTempConfig(t, dsn)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	if stores.ManagerStore == nil || stores.SQLDB == nil {
		t.Fatalf("expected postgres stores")
	}

	v1 := uuid.NewString()
	if _, err := stores.SQLDB.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, v1); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Seed a few agents for resolution.
	if err := stores.ManagerStore.UpsertAgent(context.Background(), runtimemanager.PersistedAgent{
		Config: models.AgentConfig{
			ID:     "empire-coordinator",
			Role:   "empire-coordinator",
			Mode:   "holding",
			Type:   "stub",
			Config: json.RawMessage(`{"system_prompt":"x","subscriptions":["*"]}`),
		},
		Status:  "active",
		HiredBy: "test",
	}); err != nil {
		t.Fatalf("seed coordinator: %v", err)
	}
	// Two vp-growth agents makes "vp-growth" ambiguous.
	for _, id := range []string{"vp-growth-a", "vp-growth-b"} {
		if err := stores.ManagerStore.UpsertAgent(context.Background(), runtimemanager.PersistedAgent{
			Config: models.AgentConfig{
				ID:         id,
				Role:       "vp-growth",
				Mode:       "operating",
				Type:       "stub",
				VerticalID: v1,
				Config:     json.RawMessage(`{"system_prompt":"x","subscriptions":["*"]}`),
			},
			Status:  "active",
			HiredBy: "test",
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	// One opco-ceo in v1.
	if err := stores.ManagerStore.UpsertAgent(context.Background(), runtimemanager.PersistedAgent{
		Config: models.AgentConfig{
			ID:         "opco-ceo-" + v1,
			Role:       "opco-ceo",
			Mode:       "operating",
			Type:       "stub",
			VerticalID: v1,
			Config:     json.RawMessage(`{"system_prompt":"x","subscriptions":["*"]}`),
		},
		Status:  "active",
		HiredBy: "test",
	}); err != nil {
		t.Fatalf("seed ceo: %v", err)
	}

	// ID match.
	if tgt, err := resolveTargetAgent(context.Background(), stores, "empire-coordinator"); err != nil || tgt.ID != "empire-coordinator" {
		t.Fatalf("resolve by id: tgt=%+v err=%v", tgt, err)
	}
	// vertical/alias match (normalize ceo -> opco-ceo).
	if tgt, err := resolveTargetAgent(context.Background(), stores, v1+"/ceo"); err != nil || tgt.Role != "opco-ceo" {
		t.Fatalf("resolve by vertical/alias: tgt=%+v err=%v", tgt, err)
	}
	// Ambiguous role.
	if _, err := resolveTargetAgent(context.Background(), stores, "vp-growth"); err == nil {
		t.Fatal("expected ambiguous target error")
	}
	// Fallback: unknown raw should still resolve to a factory-style targetAgent.
	if tgt, err := resolveTargetAgent(context.Background(), stores, "nonexistent-agent"); err != nil || tgt.ID != "nonexistent-agent" {
		t.Fatalf("fallback: tgt=%+v err=%v", tgt, err)
	}
}
