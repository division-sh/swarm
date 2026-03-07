package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
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

	// Seed vertical.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Ensure vertical schema.
	if err := pg.EnsureVerticalSchema(ctx, verticalID); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Publish a minimal org template for template loader paths.
	agents := []byte(`[{"role":"opco-ceo","type":"llm","system_prompt":"x","tools":[],"subscriptions":["board.directive"]}]`)
	routes := []byte(`[{"event_pattern":"board.*","subscriber_role":"opco-ceo","reason":"tests"}]`)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES ('2.0.1', $1::jsonb, $2::jsonb, '[]'::jsonb, 'test', 'test', now())
	`, string(agents), string(routes)); err != nil {
		t.Fatalf("seed template: %v", err)
	}

	// Upsert agent + load agents.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: models.AgentConfig{
			ID:         "empire-coordinator",
			Role:       "empire-coordinator",
			Mode:       "holding",
			VerticalID: "",
			// Runtime-only JSON config; keep minimal but valid for prompt enforcement.
			Config: json.RawMessage(`{"system_prompt":"You are empire coordinator.","tools":[],"subscriptions":["system.started"]}`),
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

	// Seed an OpCo CEO agent id so routing_rules FK constraints are satisfied.
	ceoID := "opco-ceo-" + verticalID
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: models.AgentConfig{
			ID:         ceoID,
			Role:       "opco-ceo",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"You are an OpCo CEO.","tools":[],"subscriptions":["board.*"]}`),
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
		VerticalID:       verticalID,
		EventPattern:     "board.*",
		SubscriberID:     ceoID,
		InstalledBy:      "empire-coordinator",
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
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("board.directive"),
		SourceAgent: "dashboard",
		VerticalID:  verticalID,
		Payload:     json.RawMessage(`{"message":"hi"}`),
		CreatedAt:   time.Now(),
	}
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{"empire-coordinator"}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evt.ID, "empire-coordinator", "processed", ""); err != nil {
		t.Fatalf("upsert receipt: %v", err)
	}

	// Mailbox.
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:    evt.ID,
		VerticalID: verticalID,
		FromAgent:  "empire-coordinator",
		Type:       "review",
		Priority:   "normal",
		Status:     "pending",
		Context:    []byte(`{"a":1}`),
		Summary:    "test mailbox",
		TimeoutAt:  time.Now().Add(24 * time.Hour),
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

	// Inbound: accept unsigned email, reject unsigned whatsapp.
	_, err = db.ExecContext(ctx, `
		UPDATE verticals
		SET credentials = '{"webhooks":{"whatsapp":{"secret":"s3cr3t"}}}'::jsonb
		WHERE id = $1::uuid
	`, verticalID)
	if err != nil {
		t.Fatalf("seed credentials: %v", err)
	}
	target, err := pg.ResolveInboundTarget(ctx, "testco", "whatsapp")
	if err != nil || target.VerticalID != verticalID || target.WebhookSecret == "" {
		t.Fatalf("resolve inbound target err=%v target=%+v", err, target)
	}
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", verticalID, "whatsapp"); err != nil || !ok {
		t.Fatalf("record inbound err=%v ok=%v", err, ok)
	}
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", verticalID, "whatsapp"); err != nil || ok {
		t.Fatalf("record inbound duplicate err=%v ok=%v", err, ok)
	}

	// Scan campaigns.
	camp, err := pg.CreateScanCampaign(ctx, runtimepipeline.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        "seed",
		Categories:  []string{"local"},
		Priority:    "high",
		Status:      "queued",
	})
	if err != nil || camp.ID == "" {
		t.Fatalf("create scan campaign err=%v camp=%+v", err, camp)
	}
	if got, err := pg.ListScanCampaigns(ctx, runtimepipeline.ScanCampaignFilter{Status: "queued", Limit: 10}); err != nil || len(got) == 0 {
		t.Fatalf("list scan campaigns err=%v len=%d", err, len(got))
	}
	claimed, ok, err := pg.ClaimNextDueScanCampaign(ctx)
	if err != nil || !ok || claimed.ID != camp.ID {
		t.Fatalf("claim campaign err=%v ok=%v claimed=%+v", err, ok, claimed)
	}
}
