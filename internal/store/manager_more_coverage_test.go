package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestManagerStore_EnsureVerticalSchema_AndTemplates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vslug', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := pg.EnsureVerticalSchema(ctx, verticalID); err != nil {
		t.Fatalf("EnsureVerticalSchema: %v", err)
	}

	if _, err := pg.LoadOrgTemplate(ctx, ""); err == nil {
		t.Fatalf("expected template version required")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('t1','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test', now())
	`); err != nil {
		t.Fatalf("seed template: %v", err)
	}
	if rec, err := pg.LoadLatestOrgTemplate(ctx); err != nil || rec.Version != "t1" {
		t.Fatalf("LoadLatestOrgTemplate err=%v rec=%+v", err, rec)
	}
	if rec, err := pg.LoadOrgTemplate(ctx, "t1"); err != nil || rec.Version != "t1" {
		t.Fatalf("LoadOrgTemplate err=%v rec=%+v", err, rec)
	}

	// bootstrap_versions resolution by template bootstrap payload match.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bootstrap_versions (version, routes, proposed_by, approved_by, evidence, created_at)
		VALUES (3, '[]'::jsonb, 'initial', 'factory-cto', 'test', now())
	`); err != nil {
		t.Fatalf("seed bootstrap_versions: %v", err)
	}
	if v, err := pg.ResolveBootstrapVersion(ctx, "t1"); err != nil || v != 3 {
		t.Fatalf("ResolveBootstrapVersion err=%v v=%d", err, v)
	}
}

func TestManagerStore_RoutingRules_DeactivateAndBootstrapVersion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vslug', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// Seed agents for FK constraints.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('sub', 'stub', 'sub', 'operating', 'active', '{}'::jsonb, now(), now()),
		       ('inst', 'stub', 'inst', 'operating', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	r := runtime.PersistedRoutingRule{
		VerticalID:       verticalID,
		EventPattern:     "inbound.*",
		SubscriberID:     "sub",
		InstalledBy:      "inst",
		Reason:           "r",
		Status:           "active",
		Source:           "bootstrap",
		BootstrapVersion: 2,
	}
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	// Deactivate via upsert branch sets deactivated_at.
	r.Status = "deactivated"
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule deactivate: %v", err)
	}
	var deact *time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT deactivated_at
		FROM routing_rules
		WHERE vertical_id=$1::uuid AND event_pattern='inbound.*' AND subscriber_id='sub'
	`, verticalID).Scan(&deact); err != nil {
		t.Fatalf("load deactivated_at: %v", err)
	}
	if deact == nil {
		t.Fatalf("expected deactivated_at set")
	}

	// Deactivate by vertical (idempotent).
	if err := pg.DeactivateRoutingRulesByVertical(ctx, verticalID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByVertical: %v", err)
	}
}

func TestManagerStore_Conversations_AndAgentTurns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'a1', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	// Conversation upsert redacts and defaults mode/status.
	if err := pg.UpsertConversation(ctx, runtime.ConversationRecord{
		AgentID: "a1",
		Messages: []runtime.Message{
			{Role: "user", Content: "reach me at a@example.com"},
		},
		TurnCount: 2,
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "", "")
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.AgentID != "a1" || rec.Mode != "session" || rec.Status != "active" || rec.TurnCount != 2 {
		t.Fatalf("unexpected conversation: %+v", rec)
	}
	if len(rec.Messages) != 1 || strings.Contains(rec.Messages[0].Content, "a@example.com") || !strings.Contains(rec.Messages[0].Content, "[EMAIL]") {
		t.Fatalf("expected redacted email, got %#v", rec.Messages)
	}

	// AppendAgentTurn requires an active agent_sessions row.
	if err := pg.AppendAgentTurn(ctx, runtime.AgentTurnRecord{AgentID: "a1", RuntimeMode: "cli_test", SessionID: "s1"}); err == nil {
		t.Fatalf("expected missing session row error")
	}
	var sessionRowID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at)
		VALUES ('a1', 'cli_test', 'cli', 's1', 'active', 0, now())
		RETURNING id::text
	`).Scan(&sessionRowID); err != nil {
		t.Fatalf("seed agent_sessions: %v", err)
	}
	_ = sessionRowID
	if err := pg.AppendAgentTurn(ctx, runtime.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "cli_test",
		SessionID:      "s1",
		TaskID:         uuid.NewString(),
		RequestPayload: []byte(`{"x":1}`),
		ResponseRaw:    []byte(`{"y":2}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}
}

func TestManagerStore_UpsertAgent_MergesSubscriptions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	rec := runtime.PersistedAgent{
		Config: models.AgentConfig{
			ID:            "a1",
			Type:          "sonnet",
			Role:          "a1",
			Mode:          "holding",
			Subscriptions: []string{"inbound.*"},
			Config:        []byte(`{"system_prompt":"x"}`),
		},
		Status:  "active",
		HiredBy: "test",
	}
	if err := pg.UpsertAgent(ctx, rec); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	found := false
	for _, a := range agents {
		if a.Config.ID == "a1" {
			found = true
			if len(a.Config.Subscriptions) != 1 || a.Config.Subscriptions[0] != "inbound.*" {
				t.Fatalf("expected subscriptions merged, got %#v", a.Config.Subscriptions)
			}
		}
	}
	if !found {
		t.Fatalf("expected agent loaded")
	}

	// Guardrails: empty agent id errors.
	if err := pg.UpsertAgent(ctx, runtime.PersistedAgent{Config: models.AgentConfig{}}); err == nil {
		t.Fatalf("expected agent id required error")
	}

	// matchesAnySubscription helper is part of pending event filtering.
	if !matchesAnySubscription("inbound.a", []events.EventType{"inbound.*"}) {
		t.Fatalf("expected matchesAnySubscription true")
	}
}
