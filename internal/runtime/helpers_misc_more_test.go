package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

func TestRuntimeHelperFunctions_Misc(t *testing.T) {
	// eventbus helper
	filtered := filterOutVerticalScopedAgentIDs([]string{"opco-ceo-v1", "empire-coordinator", "vp-product-v1", "x"}, "v1")
	if len(filtered) != 2 || filtered[0] != "empire-coordinator" {
		t.Fatalf("filterOutVerticalScopedAgentIDs mismatch: %+v", filtered)
	}

	// llm cli helper
	deduped := dedupeToolCalls([]ToolCall{
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_trend"}},
		{Name: "", Arguments: map[string]any{}},
	})
	if len(deduped) != 2 {
		t.Fatalf("expected deduped tool calls length 2, got %d", len(deduped))
	}

	// mcp context pruning
	reg := newMCPTurnRegistry()
	reg.put("old", mcpTurnContext{CreatedAt: time.Now().Add(-20 * time.Minute)})
	reg.put("new", mcpTurnContext{CreatedAt: time.Now()})
	reg.mu.Lock()
	reg.pruneLocked(time.Now().Add(-10 * time.Minute))
	reg.mu.Unlock()
	if _, ok := reg.get("old"); ok {
		t.Fatal("expected old mcp turn context pruned")
	}
	if _, ok := reg.get("new"); !ok {
		t.Fatal("expected new mcp turn context retained")
	}

	// sharding helper
	if got := toStringList([]any{"a", " ", 3}); len(got) != 2 || got[0] != "a" || got[1] != "3" {
		t.Fatalf("toStringList []any mismatch: %+v", got)
	}
	if got := toStringList(`["x","y"]`); len(got) != 2 || got[0] != "x" {
		t.Fatalf("toStringList json string mismatch: %+v", got)
	}
	if got := toStringList("x, y"); len(got) != 2 || got[1] != "y" {
		t.Fatalf("toStringList csv mismatch: %+v", got)
	}

	// scan manager helper
	if name, country, _ := parseDirectiveGeography("SaaS in Paraguay for clinics"); name != "Paraguay" || country != "Paraguay" {
		t.Fatalf("parseDirectiveGeography known country mismatch: name=%q country=%q", name, country)
	}
	if name, _, _ := parseDirectiveGeography("SaaS in custom market where internet is high"); name != "Custom Market" {
		t.Fatalf("parseDirectiveGeography phrase extraction mismatch: %q", name)
	}
}

func TestToolGatewayHelpers_AuthorizeAndMCPError(t *testing.T) {
	g := NewToolGateway(nil, "secret-token")
	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", nil)
	if err := g.authorize(req); err == nil {
		t.Fatal("expected missing authorization error")
	}
	req.Header.Set("Authorization", "Bearer wrong")
	if err := g.authorize(req); err == nil {
		t.Fatal("expected invalid token error")
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	if err := g.authorize(req); err != nil {
		t.Fatalf("expected authorize success, got %v", err)
	}

	w := httptest.NewRecorder()
	g.writeMCPError(w, "id-1", -32600, "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected MCP error HTTP 200 envelope, got %d", w.Code)
	}
	var resp mcpRPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode mcp error response: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != -32600 || resp.Error.Message == "" {
		t.Fatalf("unexpected mcp error response: %+v", resp)
	}

	if got := toolResultText(nil); got != "ok" {
		t.Fatalf("toolResultText nil mismatch: %q", got)
	}
	if got := toolResultText(""); got != "ok" {
		t.Fatalf("toolResultText empty string mismatch: %q", got)
	}
	if got := toolResultText(map[string]any{"ok": true}); got == "" {
		t.Fatalf("toolResultText map should marshal, got empty")
	}
}

type heartbeatStoreStub struct {
	schedules []Schedule
}

func (s *heartbeatStoreStub) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}
func (s *heartbeatStoreStub) CancelSchedule(context.Context, string, string) error { return nil }
func (s *heartbeatStoreStub) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (s *heartbeatStoreStub) MarkScheduleFired(context.Context, Schedule) error { return nil }

func (s *heartbeatStoreStub) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *heartbeatStoreStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (s *heartbeatStoreStub) MarkAgentTerminated(context.Context, string) error  { return nil }
func (s *heartbeatStoreStub) EnsureVerticalSchema(context.Context, string) error { return nil }
func (s *heartbeatStoreStub) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, nil
}
func (s *heartbeatStoreStub) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, nil
}
func (s *heartbeatStoreStub) SetVerticalTemplateVersion(context.Context, string, string) error {
	return nil
}
func (s *heartbeatStoreStub) UpsertRoutingRule(context.Context, PersistedRoutingRule) error {
	return nil
}
func (s *heartbeatStoreStub) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error) {
	return nil, nil
}
func (s *heartbeatStoreStub) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}
func (s *heartbeatStoreStub) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}
func (s *heartbeatStoreStub) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *heartbeatStoreStub) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func TestAgentManager_InstallDefaultOpCoHeartbeats(t *testing.T) {
	ctx := context.Background()
	stub := &heartbeatStoreStub{}
	am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, stub)

	verticalID := "11111111-1111-1111-1111-111111111111"
	if err := am.installDefaultOpCoHeartbeats(ctx, verticalID); err != nil {
		t.Fatalf("installDefaultOpCoHeartbeats: %v", err)
	}
	if len(stub.schedules) != 3 {
		t.Fatalf("expected 3 heartbeat schedules, got %d", len(stub.schedules))
	}
	if stub.schedules[0].AgentID == "" || stub.schedules[0].VerticalID != verticalID {
		t.Fatalf("unexpected first heartbeat schedule: %+v", stub.schedules[0])
	}

	if err := am.installDefaultOpCoHeartbeats(ctx, ""); err != nil {
		t.Fatalf("empty vertical should be noop, got err=%v", err)
	}
}

func TestExtractCategoryList(t *testing.T) {
	got := extractCategoryList(map[string]any{
		"taxonomy_categories": []any{"a", "b"},
	})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("extractCategoryList mismatch: %+v", got)
	}

	got2 := extractCategoryList(map[string]any{
		"taxonomy_categories": []string{"x", "y"},
	})
	if len(got2) != 2 || got2[0] != "x" || got2[1] != "y" {
		t.Fatalf("extractCategoryList list mismatch: %+v", got2)
	}

	_ = models.AgentConfig{} // keep models import used for package-level compile stability in this file.
}
