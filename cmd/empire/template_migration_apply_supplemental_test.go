package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/models"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDecodeTemplateMigrationPlan_InvalidJSON(t *testing.T) {
	if _, err := decodeTemplateMigrationPlan([]byte("")); err == nil {
		t.Fatalf("expected error for empty")
	}
	if _, err := decodeTemplateMigrationPlan([]byte("{")); err == nil {
		t.Fatalf("expected error for invalid json")
	}
	// Wrong shape but valid json should still unmarshal.
	if p, err := decodeTemplateMigrationPlan([]byte(`{"vertical_id":"v","operations":[]}`)); err != nil || p.VerticalID != "v" {
		t.Fatalf("unexpected decode err=%v plan=%+v", err, p)
	}
}

func TestTemplateMigrationApply_EndToEnd_WithMailboxApproval(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	// routing_rules.installed_by and subscriber_id both FK into agents(id)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, hired_by, started_at, last_active_at)
		VALUES ('factory-cto', 'sonnet', 'factory-cto', 'factory', 'active', '{}'::jsonb, 'test', now(), now())
	`); err != nil {
		t.Fatalf("seed factory-cto agent: %v", err)
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, human_notes, business_brief, mvp_spec, brand, deploy_config, created_at, updated_at)
		VALUES ($1::uuid,'Vertical','vert','us','operating','operating','t1','notes','{}','{}','{}','{}', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	toVersion := "t2"
	agentsRaw, _ := json.Marshal([]map[string]any{
		{"role": "opco-ceo"},
		{"role": "vp-product"},
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ($1, $2::jsonb, '[]'::jsonb, '[]'::jsonb, 'test', now())
	`, toVersion, string(agentsRaw)); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}

	// Approved mailbox item required to execute.
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "factory-cto",
		Type:       "migration_approval",
		Priority:   "normal",
		Status:     "pending",
		Context:    []byte(`{"x":1}`),
		Summary:    "approve me",
	})
	if err != nil {
		t.Fatalf("InsertMailboxItem: %v", err)
	}
	if err := pg.DecideMailboxItem(ctx, mbID, "approved", "approve", ""); err != nil {
		t.Fatalf("DecideMailboxItem: %v", err)
	}

	agentID := "vp-product-" + verticalID
	plan := templateMigrationPlan{
		VerticalID:  verticalID,
		FromVersion: "t1",
		ToVersion:   toVersion,
		GeneratedBy: "factory-cto",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Operations: []templateMigrationOp{
			{
				Type:    "ADD_AGENT",
				AgentID: agentID,
				Config: models.AgentConfig{
					ID:            agentID,
					Type:          "worker",
					Role:          "", // exercise extractRoleFromAgentID
					Mode:          "operating",
					VerticalID:    verticalID,
					Subscriptions: []string{"product.*"},
					Config: mustJSON(map[string]any{
						"system_prompt": "hello {vertical_slug}\n{org_roster}\n{mandate_document}",
						"tools":         []string{"agent_message"},
					}),
				},
			},
			{
				Type:         "ADD_ROUTE",
				EventPattern: "product.*",
				SubscriberID: agentID,
				Source:       "seeded",
				InstalledBy:  "factory-cto",
				Reason:       "test",
			},
			{
				Type:         "REMOVE_ROUTE",
				EventPattern: "product.*",
				SubscriberID: agentID,
				Source:       "seeded",
				Reason:       "test",
			},
		},
	}
	planRaw, _ := json.Marshal(plan)

	migID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO template_migrations (id, vertical_id, from_version, to_version, plan, status, mailbox_id, created_at)
		VALUES ($1::uuid, $2::uuid, 't1', $3, $4::jsonb, 'pending', $5::uuid, now())
	`, migID, verticalID, toVersion, string(planRaw), mbID); err != nil {
		t.Fatalf("seed template_migrations: %v", err)
	}

	stores := storeBundle{
		SQLDB:           db,
		EventStore:      pg,
		ManagerStore:    pg,
		MailboxStore:    pg,
		SessionRegistry: sessions.NewInMemoryRegistry(0),
	}
	if err := applyTemplateMigrationWithPrimitives(ctx, "cli_test", stores, migID, "factory-cto"); err != nil {
		t.Fatalf("applyTemplateMigrationWithPrimitives: %v", err)
	}

	// Version bumped last write.
	var tmpl string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(template_version,'') FROM verticals WHERE id=$1::uuid`, verticalID).Scan(&tmpl); err != nil {
		t.Fatalf("load vertical template_version: %v", err)
	}
	if tmpl != toVersion {
		t.Fatalf("expected template_version=%s got=%s", toVersion, tmpl)
	}

	// Migration status completed.
	var st string
	if err := db.QueryRowContext(ctx, `SELECT status FROM template_migrations WHERE id=$1::uuid`, migID).Scan(&st); err != nil {
		t.Fatalf("load migration status: %v", err)
	}
	if st != "completed" {
		t.Fatalf("expected completed, got %s", st)
	}

	// Agent exists and has expanded prompt.
	var sp string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(config->>'system_prompt','')
		FROM agents
		WHERE id=$1
	`, agentID).Scan(&sp); err != nil {
		t.Fatalf("load agent prompt: %v", err)
	}
	if sp == "" || sp == "hello {vertical_slug}\n{org_roster}\n{mandate_document}" {
		t.Fatalf("expected placeholders expanded, got %q", sp)
	}

	// Events written.
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type IN ('template.migration_complete','template.migration_completed') AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 2 {
		t.Fatalf("expected migration complete events, got %d", n)
	}
}

func TestTemplateMigrationApply_RejectsUnapprovedMailbox(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','t1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "factory-cto",
		Type:       "migration_approval",
		Status:     "pending",
		Context:    []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}

	// to_version is FK -> org_templates(version)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('t2','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test', now())
	`); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}

	migID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO template_migrations (id, vertical_id, from_version, to_version, plan, status, mailbox_id, created_at)
		VALUES ($1::uuid, $2::uuid, 't1', 't2', $3::jsonb, 'pending', $4::uuid, now())
	`, migID, verticalID, `{"vertical_id":"`+verticalID+`","operations":[{"type":"ADD_AGENT","agent_id":"x","config":{"id":"x","type":"worker","vertical_id":"`+verticalID+`","config":{"system_prompt":"hi"}}}]}`, mbID); err != nil {
		t.Fatalf("seed migration: %v", err)
	}
	stores := storeBundle{SQLDB: db, EventStore: pg, ManagerStore: pg, MailboxStore: pg, SessionRegistry: sessions.NewInMemoryRegistry(0)}
	if err := applyTemplateMigrationWithPrimitives(ctx, "cli_test", stores, migID, "factory-cto"); err == nil {
		t.Fatalf("expected error for unapproved mailbox")
	}
}

func TestTemplateMigrationApply_InvalidPlanFailsAndEmits(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','t1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// to_version is FK -> org_templates(version)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('t2','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test', now())
	`); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}

	migID := uuid.NewString()
	// Valid JSON but empty operations triggers failure path.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO template_migrations (id, vertical_id, from_version, to_version, plan, status, created_at)
		VALUES ($1::uuid, $2::uuid, 't1', 't2', '{}'::jsonb, 'pending', now())
	`, migID, verticalID); err != nil {
		t.Fatalf("seed migration: %v", err)
	}
	stores := storeBundle{SQLDB: db, EventStore: pg, ManagerStore: pg, MailboxStore: pg, SessionRegistry: sessions.NewInMemoryRegistry(0)}
	if err := applyTemplateMigrationWithPrimitives(ctx, "cli_test", stores, migID, "factory-cto"); err == nil {
		t.Fatalf("expected failure")
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM template_migrations WHERE id=$1::uuid`, migID).Scan(&status); err != nil {
		t.Fatalf("load status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("expected failed, got %s", status)
	}
	// Fail emits event and mailbox item (best-effort).
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='template.migration_failed' AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected failure event")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE type='digest' AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected failure mailbox item")
	}
}

func TestTemplateMigrationHelpers(t *testing.T) {
	if got := extractRoleFromAgentID("vp-growth-123"); got != "vp-growth" {
		t.Fatalf("extractRoleFromAgentID got %q", got)
	}
	if got := extractRoleFromAgentID("x"); got != "" {
		t.Fatalf("expected empty for no dash, got %q", got)
	}
	if got := coalesce("  ", "f"); got != "f" {
		t.Fatalf("coalesce got %q", got)
	}

	mandate := renderMandateFromVertical("v", "notes", []byte(`{"a":1}`), []byte(`{}`), []byte(`null`), []byte(`{"d":2}`))
	if mandate == "" {
		t.Fatalf("expected mandate text")
	}
}
