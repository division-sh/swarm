package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Smoke_ManagerEventsMailboxInboundScanCampaigns(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	pg := &PostgresStore{DB: db}
	const runID = "66666666-6666-6666-6666-666666666666"
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)

	// Seed entity.
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('testco', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'testco', 'default', 'testco', 'TestCo', 'operating',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}

	// Ensure entity schema.
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Upsert agent + load agents.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "control-plane",
			Role:     "control-plane",
			Mode:     "global",
			Model:    "regular",
			EntityID: "",
			// Runtime-only JSON config; keep minimal but valid for prompt enforcement.
			Config: json.RawMessage(`{"system_prompt":"You are the control plane.","tools":[],"subscriptions":["system.started"]}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
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
			Model:    "regular",
			EntityID: entityID,
			Config:   json.RawMessage(`{"system_prompt":"You are an operator.","tools":[],"subscriptions":["review.*"]}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
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
	evt := eventtest.PersistedProjection(
		uuid.NewString(),
		events.EventType("review.requested"),
		"dashboard",
		"",
		json.RawMessage(`{"message":"hi"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now(),
	)

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{"control-plane"}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}
	activeSessionID := uuid.NewString()
	if err := pg.MarkEventDeliveryInProgress(ctx, evt.ID(), "control-plane", activeSessionID); err != nil {
		t.Fatalf("mark delivery in progress: %v", err)
	}
	var inProgressStatus, inProgressReason, gotActiveSession string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_id = 'control-plane'
	`, evt.ID()).Scan(&inProgressStatus, &inProgressReason, &gotActiveSession); err != nil {
		t.Fatalf("load in-progress delivery: %v", err)
	}
	if inProgressStatus != "in_progress" {
		t.Fatalf("in-progress delivery status = %q, want in_progress", inProgressStatus)
	}
	if inProgressReason != "agent_processing" {
		t.Fatalf("in-progress delivery reason = %q, want agent_processing", inProgressReason)
	}
	if gotActiveSession != activeSessionID {
		t.Fatalf("active_session_id = %q, want %q", gotActiveSession, activeSessionID)
	}
	if err := pg.UpsertEventReceipt(ctx, evt.ID(), "control-plane", "processed", nil); err != nil {
		t.Fatalf("upsert receipt: %v", err)
	}
	var deliveryStatus, deliveryReason, receiptReason, clearedActiveSession string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(active_session_id::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_id = 'control-plane'
	`, evt.ID()).Scan(&deliveryStatus, &deliveryReason, &clearedActiveSession); err != nil {
		t.Fatalf("load delivery status/reason: %v", err)
	}
	if deliveryStatus != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", deliveryStatus)
	}
	if deliveryReason != "agent_processed" {
		t.Fatalf("delivery reason = %q, want agent_processed", deliveryReason)
	}
	if clearedActiveSession != "" {
		t.Fatalf("active_session_id = %q, want cleared", clearedActiveSession)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_id = 'control-plane'
	`, evt.ID()).Scan(&receiptReason); err != nil {
		t.Fatalf("load receipt reason: %v", err)
	}
	if receiptReason != "agent_processed" {
		t.Fatalf("receipt reason = %q, want agent_processed", receiptReason)
	}

	// Mailbox.
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   evt.ID(),
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
	if err := pg.DecideMailboxItem(ctx, mbID, "decided", "approve", "ok"); err != nil {
		t.Fatalf("decide mailbox: %v", err)
	}

	// Inbound dedupe record.
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", entityID, "chat"); err != nil || !ok {
		t.Fatalf("record inbound err=%v ok=%v", err, ok)
	}
	if ok, err := pg.RecordInboundEvent(ctx, "evt-1", entityID, "chat"); err != nil || ok {
		t.Fatalf("record inbound duplicate err=%v ok=%v", err, ok)
	}

}
