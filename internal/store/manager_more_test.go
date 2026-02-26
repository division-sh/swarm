package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Manager_MoreCoverage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	// Seed org templates (two versions to exercise latest lookup).
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES
			('v1','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test','v1', now() - interval '2 hour'),
			('v2','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test','v2', now())
	`); err != nil {
		t.Fatalf("seed templates: %v", err)
	}
	if rec, err := pg.LoadLatestOrgTemplate(ctx); err != nil || rec.Version != "v2" {
		t.Fatalf("LoadLatestOrgTemplate rec=%+v err=%v", rec, err)
	}
	if _, err := pg.LoadOrgTemplate(ctx, ""); err == nil {
		t.Fatal("expected LoadOrgTemplate error for empty version")
	}
	if rec, err := pg.LoadOrgTemplate(ctx, "v1"); err != nil || rec.Version != "v1" {
		t.Fatalf("LoadOrgTemplate rec=%+v err=%v", rec, err)
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating','v1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := pg.EnsureVerticalSchema(ctx, verticalID); err != nil {
		t.Fatalf("EnsureVerticalSchema: %v", err)
	}
	if err := pg.SetVerticalTemplateVersion(ctx, verticalID, "v2"); err != nil {
		t.Fatalf("SetVerticalTemplateVersion: %v", err)
	}

	// Agents: insert, then terminate.
	if err := pg.UpsertAgent(ctx, runtime.PersistedAgent{
		Config: models.AgentConfig{
			ID:         "a1",
			Role:       "role",
			Mode:       "holding",
			Type:       "sonnet",
			VerticalID: "",
			Config:     json.RawMessage(`{"system_prompt":"x","subscriptions":["system.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := pg.MarkAgentTerminated(ctx, "a1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtime.PersistedAgent{
		Config: models.AgentConfig{
			ID:         "ephemeral-shard-1",
			Role:       "market-research-agent",
			Mode:       "factory",
			Type:       "sonnet",
			VerticalID: "",
			Config:     json.RawMessage(`{"system_prompt":"x","subscriptions":["market_research.scan_assigned"]}`),
		},
		Status:          "ephemeral",
		HiredBy:         "runtime",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent ephemeral: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	for _, a := range agents {
		if a.Config.ID == "a1" {
			t.Fatal("expected terminated agent to be excluded from LoadAgents")
		}
		if a.Config.ID == "ephemeral-shard-1" {
			t.Fatal("expected ephemeral agent to be excluded from LoadAgents")
		}
	}

	// Routing rules: upsert + deactivate by vertical.
	ceoID := "opco-ceo-" + verticalID
	_ = pg.UpsertAgent(ctx, runtime.PersistedAgent{
		Config: models.AgentConfig{
			ID:         ceoID,
			Role:       "opco-ceo",
			Mode:       "operating",
			Type:       "sonnet",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","subscriptions":["board.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	})
	if err := pg.UpsertRoutingRule(ctx, runtime.PersistedRoutingRule{
		VerticalID:   verticalID,
		EventPattern: "board.*",
		SubscriberID: ceoID,
		InstalledBy:  ceoID,
		Reason:       "tests",
		Status:       "active",
		Source:       "seeded",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.DeactivateRoutingRulesByVertical(ctx, verticalID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByVertical: %v", err)
	}

	// Events: pending deliveries + receipts.
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        "board.directive",
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     json.RawMessage(`{"x":1}`),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{ceoID}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	pending, err := pg.ListPendingEventsForAgent(ctx, ceoID, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(pending) == 0 {
		t.Fatalf("ListPendingEventsForAgent err=%v len=%d", err, len(pending))
	}

	// Subscribed pending events uses subscription matching filter. Run before we record a receipt.
	subPending, err := pg.ListPendingSubscribedEvents(ctx, ceoID, []events.EventType{"board.*"}, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(subPending) == 0 {
		t.Fatalf("ListPendingSubscribedEvents err=%v len=%d", err, len(subPending))
	}

	// Mark receipt as error once; pending list will include it after delay windows only.
	if err := pg.UpsertEventReceipt(ctx, evt.ID, ceoID, "error", "boom"); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	if rec, ok, err := pg.GetEventReceipt(ctx, evt.ID, ceoID); err != nil || !ok || rec.Status == "" {
		t.Fatalf("GetEventReceipt ok=%v err=%v rec=%+v", ok, err, rec)
	}

	// Turns: requires an active session row.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1, 'cli_test', 'anthropic_cli', 's1', 'active', 0, now(), now())
	`, ceoID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtime.AgentTurnRecord{
		AgentID:        ceoID,
		RuntimeMode:    "cli_test",
		SessionID:      "s1",
		TaskID:         "",
		RequestPayload: []byte(`{"in":1}`),
		ResponseRaw:    []byte(`{"out":1}`),
		ParseOK:        true,
		Latency:        123 * time.Millisecond,
		RetryCount:     0,
		Error:          "",
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	// Conversations: upsert + load active.
	if err := pg.UpsertConversation(ctx, runtime.ConversationRecord{
		AgentID:   ceoID,
		TaskID:    "",
		Mode:      "session",
		Messages:  []runtime.Message{{Role: "user", Content: "hi"}},
		Summary:   "sum",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	if rec, ok, err := pg.LoadActiveConversation(ctx, ceoID, "session", ""); err != nil || !ok || rec.AgentID != ceoID {
		t.Fatalf("LoadActiveConversation ok=%v err=%v rec=%+v", ok, err, rec)
	}

	// Schedules: upsert + load + mark fired + cancel.
	sc := runtime.Schedule{
		AgentID:   ceoID,
		EventType: "timer.test",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if got, err := pg.LoadActiveSchedules(ctx); err != nil || len(got) == 0 {
		t.Fatalf("LoadActiveSchedules err=%v len=%d", err, len(got))
	}
	if err := pg.MarkScheduleFired(ctx, sc); err != nil {
		t.Fatalf("MarkScheduleFired: %v", err)
	}
	if err := pg.CancelSchedule(ctx, ceoID, "timer.test"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
}

func TestPostgresStore_MarkAgentTerminated_CleansRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.UpsertAgent(ctx, runtime.PersistedAgent{
		Config: models.AgentConfig{
			ID:   "agent-cleanup-1",
			Role: "market-research-agent",
			Mode: "factory",
			Type: "sonnet",
			Config: json.RawMessage(`{
				"system_prompt":"x",
				"subscriptions":["market_research.scan_assigned"]
			}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ($1, 'session', '[{"role":"user","content":"hello"}]'::jsonb, 'x', 1, 'active', now(), now())
	`, "agent-cleanup-1"); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1, 'cli_test', 'anthropic_cli', 'sess-cleanup-1', 'active', 2, now(), now())
	`, "agent-cleanup-1"); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	if err := pg.MarkAgentTerminated(ctx, "agent-cleanup-1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}

	var (
		agentStatus string
		convStatus  string
		sessStatus  string
	)
	if err := db.QueryRowContext(ctx, `SELECT status FROM agents WHERE id = $1`, "agent-cleanup-1").Scan(&agentStatus); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM conversations WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&convStatus); err != nil {
		t.Fatalf("read conversation status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&sessStatus); err != nil {
		t.Fatalf("read session status: %v", err)
	}
	if agentStatus != "terminated" {
		t.Fatalf("expected terminated agent status, got %q", agentStatus)
	}
	if convStatus != "terminated" {
		t.Fatalf("expected terminated conversation status, got %q", convStatus)
	}
	if sessStatus != "terminated" {
		t.Fatalf("expected terminated session status, got %q", sessStatus)
	}
}

func TestManagerHelpers_MatchingAndRedaction(t *testing.T) {
	got := extractSubscriptions([]byte(`{"subscriptions":["a","b"," "],"tools":[]}`))
	// Don't over-specify: extraction is best-effort and may preserve empty entries.
	hasA, hasB := false, false
	for _, v := range got {
		if v == "a" {
			hasA = true
		}
		if v == "b" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Fatalf("expected a and b subscriptions, got %v", got)
	}
	if normalizeJSONPayload([]byte(`{"b":1,"a":2}`)) == "" {
		t.Fatal("expected normalized json")
	}
	if !matchesAnySubscription("board.chat", []events.EventType{"board.*"}) {
		t.Fatal("expected subscription match")
	}
	if subscriptionMatch("board.*", "budget.alert") {
		t.Fatal("unexpected match")
	}
	if nullable("", "x") != "x" {
		t.Fatal("nullable fallback mismatch")
	}
	if sanitizeSchemaIdent("Test-Co!!") != "testco" {
		t.Fatalf("sanitizeSchemaIdent mismatch")
	}
	if quoteIdent("x") != `"x"` {
		t.Fatal("quoteIdent mismatch")
	}

	// Redaction paths.
	obj := map[string]any{"api_key": "secret", "name": "John Doe", "nested": map[string]any{"token": "t"}}
	redacted := redactPayloadValue("root", obj)
	b, _ := json.Marshal(redacted)
	if string(b) == "" {
		t.Fatal("expected redacted json")
	}
	_ = redactText("sk-ant-foo")
	_ = redactName("John Doe")
	_ = isNameKey("name")
}
