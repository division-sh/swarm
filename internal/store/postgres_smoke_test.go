package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Smoke_ManagerEventsMailboxInboundScanCampaigns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	// Seed geography (required by some flows).
	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO geographies (id, name, country, region, created_at) VALUES ($1::uuid,'United States','US','', now())`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}

	// Seed entity.
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entities (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, entityID); err != nil {
		t.Fatalf("seed compatibility entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{"slug":"testco"}'::jsonb, now(), now()
		)
	`, entityID); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	// Ensure entity schema.
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Publish a minimal org template for template loader paths.
	agents := []byte(`[{"role":"operator","type":"llm","system_prompt":"x","tools":[],"subscriptions":["review.requested"]}]`)
	routes := []byte(`[{"event_pattern":"review.*","subscriber_role":"operator","reason":"tests"}]`)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES ('2.0.1', $1::jsonb, $2::jsonb, '[]'::jsonb, 'test', 'test', now())
	`, string(agents), string(routes)); err != nil {
		t.Fatalf("seed template: %v", err)
	}

	// Upsert agent + load agents.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "control-plane",
			Role:     "control-plane",
			Mode:     "global",
			EntityID: "",
			// Runtime-only JSON config; keep minimal but valid for prompt enforcement.
			Config: json.RawMessage(`{"system_prompt":"You are the control plane.","tools":[],"subscriptions":["system.started"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "2.0.1",
	}); err != nil {
		t.Fatalf("upsert agent: %v", err)
	}
	agentsOut, err := pg.LoadAgents(ctx)
	if err != nil || len(agentsOut) == 0 {
		t.Fatalf("load agents err=%v len=%d", err, len(agentsOut))
	}

	// Seed an operating agent id so routing_rules FK constraints are satisfied.
	ceoID := "operator-" + entityID
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       ceoID,
			Role:     "operator",
			Mode:     "operating",
			EntityID: entityID,
			Config:   json.RawMessage(`{"system_prompt":"You are an operator.","tools":[],"subscriptions":["review.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "2.0.1",
	}); err != nil {
		t.Fatalf("upsert ceo agent: %v", err)
	}

	// Routing rules.
	rule := runtimemanager.PersistedRoutingRule{
		EntityID:         entityID,
		EventPattern:     "review.*",
		SubscriberID:     ceoID,
		InstalledBy:      "control-plane",
		Reason:           "tests",
		Status:           "active",
		Source:           "bootstrap",
		BootstrapVersion: 1,
	}
	if err := pg.UpsertRoutingRule(ctx, rule); err != nil {
		t.Fatalf("upsert routing rule: %v", err)
	}
	if rules, err := pg.LoadRoutingRules(ctx); err != nil || len(rules) == 0 {
		t.Fatalf("load routing rules err=%v len=%d", err, len(rules))
	}

	// Events + deliveries + receipts.
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("review.requested"),
		SourceAgent: "dashboard",
		Payload:     json.RawMessage(`{"message":"hi"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{"control-plane"}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evt.ID, "control-plane", "processed", ""); err != nil {
		t.Fatalf("upsert receipt: %v", err)
	}

	// Mailbox.
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   evt.ID,
		EntityID:  entityID,
		FromAgent: "control-plane",
		Type:      "review",
		Priority:  "normal",
		Status:    "pending",
		Context:   []byte(`{"a":1}`),
		Summary:   "test mailbox",
		TimeoutAt: time.Now().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	if _, err := pg.GetMailboxItem(ctx, mbID); err != nil {
		t.Fatalf("get mailbox: %v", err)
	}
	if n, err := pg.CountMailboxItems(ctx, "pending"); err != nil || n < 1 {
		t.Fatalf("count mailbox err=%v n=%d", err, n)
	}
	if items, err := pg.ListMailboxItems(ctx, "pending", 10); err != nil || len(items) == 0 {
		t.Fatalf("list mailbox err=%v len=%d", err, len(items))
	}
	if err := pg.DecideMailboxItem(ctx, mbID, "approved", "approve", "ok"); err != nil {
		t.Fatalf("decide mailbox: %v", err)
	}

	// Inbound: accept unsigned email, reject unsigned chat.
	_, err = db.ExecContext(ctx, `
		UPDATE workflow_instances
		SET metadata = COALESCE(metadata, '{}'::jsonb) || '{"credentials":{"webhooks":{"chat":{"secret":"s3cr3t"}}}}'::jsonb
		WHERE instance_id = $1::uuid
	`, entityID)
	if err != nil {
		t.Fatalf("seed credentials: %v", err)
	}
	target, err := pg.ResolveInboundTarget(ctx, "testco", "chat")
	if err != nil || target.EntityID != entityID || target.WebhookSecret == "" {
		t.Fatalf("resolve inbound target err=%v target=%+v", err, target)
	}
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", entityID, "chat"); err != nil || !ok {
		t.Fatalf("record inbound err=%v ok=%v", err, ok)
	}
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", entityID, "chat"); err != nil || ok {
		t.Fatalf("record inbound duplicate err=%v ok=%v", err, ok)
	}

}
