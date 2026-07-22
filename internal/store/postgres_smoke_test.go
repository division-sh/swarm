package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Smoke_ManagerEventsMailboxInboundScanCampaigns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	const runID = "66666666-6666-6666-6666-666666666666"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)

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

	// Upsert agent + load agents.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "control-plane",
			Role:          "control-plane",
			FlowID:        "global",
			Model:         "regular",
			ExecutionMode: "live",
			EntityID:      "",
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
			ID:            ceoID,
			Role:          "operator",
			FlowID:        "operating",
			Model:         "regular",
			ExecutionMode: "live",
			EntityID:      entityID,
			Config:        json.RawMessage(`{"system_prompt":"You are an operator.","tools":[],"subscriptions":["review.*"]}`),
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
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("review.requested"),
		"dashboard",
		"",
		json.RawMessage(`{"message":"hi"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now(),
	)

	if err := commitSemanticEventFixtureWithAgents(ctx, pg, evt, []string{"control-plane"}); err != nil {
		t.Fatalf("append event with exact delivery: %v", err)
	}
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "control-plane"}
	claimed, err := pg.ClaimAgentDelivery(ctx, evt, route)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	activeSessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status)
		VALUES ($1::uuid, $2::uuid, 'control-plane', 'global', TRUE, 'authored', 'active')
	`, activeSessionID, evt.RunID()); err != nil {
		t.Fatalf("seed delivery session: %v", err)
	}
	if _, err := pg.BindAgentSession(ctx, claimed.Claim, activeSessionID); err != nil {
		t.Fatalf("bind delivery session: %v", err)
	}
	var inProgressStatus, inProgressReason, gotActiveSession string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(d.status, ''), COALESCE(d.reason_code, ''), COALESCE(a.active_session_id::text, '')
		FROM event_deliveries d
		JOIN event_delivery_attempts a
		  ON a.delivery_id = d.delivery_id
		 AND a.claim_version = d.current_attempt_version
		 AND a.open_marker = TRUE
		WHERE d.event_id = $1::uuid AND d.subscriber_id = 'control-plane'
	`, evt.ID()).Scan(&inProgressStatus, &inProgressReason, &gotActiveSession); err != nil {
		t.Fatalf("load in-progress delivery: %v", err)
	}
	if inProgressStatus != "in_progress" {
		t.Fatalf("in-progress delivery status = %q, want in_progress", inProgressStatus)
	}
	if inProgressReason != "" {
		t.Fatalf("in-progress delivery reason = %q, want empty", inProgressReason)
	}
	if gotActiveSession != activeSessionID {
		t.Fatalf("active_session_id = %q, want %q", gotActiveSession, activeSessionID)
	}
	if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, 0); err != nil {
		t.Fatalf("settle delivery: %v", err)
	}
	var deliveryStatus, deliveryReason, receiptReason, clearedActiveSession string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(current_attempt_version::text, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_id = 'control-plane'
	`, evt.ID()).Scan(&deliveryStatus, &deliveryReason, &clearedActiveSession); err != nil {
		t.Fatalf("load delivery status/reason: %v", err)
	}
	if deliveryStatus != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", deliveryStatus)
	}
	if deliveryReason != "" {
		t.Fatalf("delivery reason = %q, want empty", deliveryReason)
	}
	if clearedActiveSession != "" {
		t.Fatalf("active_session_id = %q, want cleared", clearedActiveSession)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_delivery_outcomes
		WHERE delivery_id = $1::uuid
	`, claimed.Snapshot.DeliveryID).Scan(&receiptReason); err != nil {
		t.Fatalf("load delivery outcome reason: %v", err)
	}
	if receiptReason != "" {
		t.Fatalf("delivery outcome reason = %q, want empty", receiptReason)
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
}
