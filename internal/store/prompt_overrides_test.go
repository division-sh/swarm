package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runtimeactors "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/testutil"
)

func TestPostgresStore_PromptOverridesCRUD(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS prompt_overrides (
			agent_id        TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
			prompt          TEXT NOT NULL,
			previous_prompt TEXT,
			source          TEXT NOT NULL DEFAULT 'dashboard',
			notes           TEXT,
			created_at      TIMESTAMPTZ DEFAULT now(),
			updated_at      TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("ensure prompt_overrides table: %v", err)
	}

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:     "coordinator",
			Role:   "coordinator",
			Mode:   "holding",
			Type:   "worker",
			Config: json.RawMessage(`{"system_prompt":"base prompt"}`),
		},
		Status:          "active",
		HiredBy:         "test",
		TemplateVersion: "2.0.4",
		StartedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	_, found, err := pg.GetPromptOverride(ctx, "coordinator")
	if err != nil {
		t.Fatalf("get initial override: %v", err)
	}
	if found {
		t.Fatal("expected no initial override")
	}

	if err := pg.UpsertPromptOverride(ctx, runtimemanager.PromptOverrideRecord{
		AgentID:        "coordinator",
		Prompt:         "override prompt",
		PreviousPrompt: "base prompt",
		Source:         "cli",
		Notes:          "test override",
	}); err != nil {
		t.Fatalf("upsert prompt override: %v", err)
	}

	rec, found, err := pg.GetPromptOverride(ctx, "coordinator")
	if err != nil {
		t.Fatalf("get override: %v", err)
	}
	if !found {
		t.Fatal("expected override row")
	}
	if rec.Prompt != "override prompt" || rec.PreviousPrompt != "base prompt" || rec.Source != "cli" {
		t.Fatalf("unexpected override record: %+v", rec)
	}

	if err := pg.DeletePromptOverride(ctx, "coordinator"); err != nil {
		t.Fatalf("delete override: %v", err)
	}
	_, found, err = pg.GetPromptOverride(ctx, "coordinator")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if found {
		t.Fatal("expected override to be deleted")
	}
}
