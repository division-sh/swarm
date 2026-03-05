package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/config"
	"empireai/internal/mailbox"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestMain_LoadAuditSpecInput_And_ManagedMigrations_And_MailboxSideEffects(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	ctx := context.Background()

	// loadAuditSpecInput: spec-file branch.
	specFile := filepath.Join(t.TempDir(), "spec.json")
	if err := writeFile(specFile, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("write spec file: %v", err)
	}
	if b, err := loadAuditSpecInput(ctx, db, "template", "", specFile); err != nil || string(b) == "" {
		t.Fatalf("loadAuditSpecInput(spec-file) b=%q err=%v", string(b), err)
	}
	// loadAuditSpecInput: template requires file.
	if _, err := loadAuditSpecInput(ctx, db, "template", "", ""); err == nil {
		t.Fatal("expected template audit to require spec-file")
	}
	// loadAuditSpecInput: vertical lookup.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, mvp_spec, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', '{"mvp":1}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := loadAuditSpecInput(ctx, nil, "vertical_spec", verticalID, ""); err == nil {
		t.Fatal("expected error when db is nil")
	}
	if _, err := loadAuditSpecInput(ctx, db, "vertical_spec", "", ""); err == nil {
		t.Fatal("expected error when vertical-id missing")
	}
	if b, err := loadAuditSpecInput(ctx, db, "vertical_spec", verticalID, ""); err != nil || !json.Valid(b) {
		t.Fatalf("loadAuditSpecInput(vertical) b=%q err=%v", string(b), err)
	}

	// applyManagedMigrations: covers schema_version flow and version skipping.
	pg := &store.PostgresStore{DB: db}
	mDir := filepath.Join(root, "migrations")
	if err := applyManagedMigrations(ctx, pg, []migrationSpec{
		{Version: 0, Name: "skip", Path: ""}, // no-op branch
		{Version: 1, Name: "001_initial", Path: filepath.Join(mDir, "001_initial.sql")},
		{Version: 2, Name: "002_v2_0", Path: filepath.Join(mDir, "002_v2_0.sql")},
	}); err != nil {
		t.Fatalf("applyManagedMigrations: %v", err)
	}
	// Calling again should see schema_version entries and skip.
	if err := applyManagedMigrations(ctx, pg, []migrationSpec{
		{Version: 1, Name: "001_initial", Path: filepath.Join(mDir, "001_initial.sql")},
	}); err != nil {
		t.Fatalf("applyManagedMigrations (second): %v", err)
	}

	// emitMailboxDecisionSideEffects: exercise multiple branches.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	stores := buildStores(ctx, "postgres", cfg, false, filepath.Join(root, "contracts", "ddl-canonical.sql"))
	if stores.SQLDB == nil || stores.EventStore == nil {
		t.Fatalf("expected postgres stores")
	}

	// Ensure recipient agents exist for delivery FK + filterExistingRecipients.
	for _, agentID := range []string{
		"empire-coordinator",
		"opco-ceo-" + verticalID,
		"holding-devops",
	} {
		if _, err := stores.SQLDB.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{}'::jsonb)
			ON CONFLICT (id) DO NOTHING
		`, agentID); err != nil {
			t.Fatalf("seed agent %s: %v", agentID, err)
		}
	}

	items := []runtime.MailboxItem{
		{ID: uuid.NewString(), Type: "spend_request", VerticalID: verticalID, FromAgent: "empire-coordinator", Context: []byte(`{}`)},
		{ID: uuid.NewString(), Type: "budget_increase", VerticalID: verticalID, FromAgent: "empire-coordinator", Context: []byte(`{}`)},
		{ID: uuid.NewString(), Type: "review", VerticalID: verticalID, FromAgent: "empire-coordinator", Context: []byte(`{"review_type":"founder_input"}`)},
		{ID: uuid.NewString(), Type: "cross_domain_escalation", VerticalID: verticalID, FromAgent: "empire-coordinator", Context: []byte(`{"q":"x"}`)},
		{ID: uuid.NewString(), Type: "devops.capacity_warning", VerticalID: "", FromAgent: "holding-devops", Context: []byte(`{}`)},
	}
	outcomes := []mailbox.DecisionOutcome{
		{Status: "approved", Decision: "approve"},
		{Status: "rejected", Decision: "reject"},
		{Status: "more_data", Decision: "more_data"},
	}
	for _, it := range items {
		for _, oc := range outcomes {
			notes := "do it"
			if oc.Status != "approved" {
				notes = ""
			}
			_ = emitMailboxDecisionSideEffects(ctx, stores, it, oc, notes)
		}
	}
}

func writeFile(path string, b []byte) error {
	return os.WriteFile(path, b, 0o644)
}
