package runtime

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime/sessions"
	llm "empireai/internal/runtime/llm"
	workspace "empireai/internal/runtime/workspace"
	"empireai/internal/testutil"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestAgentContextHelpers_ExtractContextIDs_AndTransitionFallback(t *testing.T) {
	evt := events.Event{
		VerticalID: "",
		TaskID:     "",
		Payload: mustJSON(map[string]any{
			"vertical_ref": "v-ref",
			"task_ref":     "t-ref",
		}),
	}
	verticalID, taskID := extractContextIDs(evt)
	if verticalID != "v-ref" || taskID != "t-ref" {
		t.Fatalf("extractContextIDs payload fallback mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	evt = events.Event{
		VerticalID: "v-top",
		TaskID:     "t-top",
		Payload: mustJSON(map[string]any{
			"vertical_id": "v-payload",
			"task_id":     "t-payload",
		}),
	}
	verticalID, taskID = extractContextIDs(evt)
	if verticalID != "v-top" || taskID != "t-top" {
		t.Fatalf("extractContextIDs top-level precedence mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	verticalID, taskID = extractContextIDs(events.Event{VerticalID: "v", TaskID: "t", Payload: []byte("{")})
	if verticalID != "v" || taskID != "t" {
		t.Fatalf("extractContextIDs invalid-json mismatch: vertical=%q task=%q", verticalID, taskID)
	}

	key := transitionContextKey(
		events.Event{Payload: mustJSON(map[string]any{"vertical_id": "v1"})},
		events.Event{Payload: mustJSON(map[string]any{"vertical_id": "v2", "task_id": "t2"})},
	)
	if key != "v1|t2" {
		t.Fatalf("transitionContextKey fallback mismatch: %q", key)
	}
}

func TestAgentContractHelpers_BudgetExpectationAndRemediation(t *testing.T) {
	agent := &LLMAgent{cfg: models.AgentConfig{Role: "empire-coordinator"}}
	inbound := events.Event{Type: events.EventType("budget.threshold_crossed")}

	recorder := NewEmittedEventsRecorder()
	err := agent.enforcePostTurnExpectations(inbound, recorder)
	if err == nil || !strings.Contains(err.Error(), "must emit one budget.* event") {
		t.Fatalf("expected budget contract error, got %v", err)
	}

	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := agent.enforcePostTurnExpectations(inbound, recorder); err != nil {
		t.Fatalf("expected budget contract satisfied, got %v", err)
	}

	prompt, ok := contractRemediationPrompt(agent.cfg, inbound, errors.New("x"))
	if !ok || !strings.Contains(prompt, "emit_budget_*") {
		t.Fatalf("expected budget remediation prompt, ok=%v prompt=%q", ok, prompt)
	}

	if prompt, ok := contractRemediationPrompt(models.AgentConfig{Role: "backend-agent"}, events.Event{Type: events.EventType("board.chat")}, errors.New("x")); ok || prompt != "" {
		t.Fatalf("expected no remediation prompt for non-coordinator event, ok=%v prompt=%q", ok, prompt)
	}
}

type failingContinueRuntime struct{}

func (failingContinueRuntime) StartSession(_ context.Context, agentID, _ string, _ []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "s", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (failingContinueRuntime) ContinueSession(context.Context, *llm.Session, llm.Message) (*llm.Response, error) {
	return nil, errors.New("continue failed")
}

func (failingContinueRuntime) PersistConversationSnapshot(context.Context, *llm.Session) error {
	return nil
}

func TestAttemptPostTurnContractRemediation_Branches(t *testing.T) {

	noPromptAgent := &LLMAgent{cfg: models.AgentConfig{Role: "backend-agent"}}
	originalErr := errors.New("contract failed")
	got := noPromptAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("board.chat")},
		NewEmittedEventsRecorder(),
		originalErr,
	)
	if !errors.Is(got, originalErr) {
		t.Fatalf("expected original contract error, got %v", got)
	}

	okAgent := &LLMAgent{
		cfg: models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"},
		conversation: llm.NewConversation(
			"empire-coordinator",
			"",
			"prompt",
			nil,
			llm.SessionScoped,
			10,
			&llmNoToolRuntime{},
		),
	}
	recorder := NewEmittedEventsRecorder()
	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := okAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("budget.threshold_crossed")},
		recorder,
		errors.New("budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool"),
	); err != nil {
		t.Fatalf("expected remediation success, got %v", err)
	}

	failAgent := &LLMAgent{
		cfg: models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"},
		conversation: llm.NewConversation(
			"empire-coordinator",
			"",
			"prompt",
			nil,
			llm.SessionScoped,
			10,
			failingContinueRuntime{},
		),
	}
	recorder = NewEmittedEventsRecorder()
	recorder.Append(events.Event{Type: events.EventType("budget.warning")})
	if err := failAgent.attemptPostTurnContractRemediation(
		context.Background(),
		events.Event{Type: events.EventType("budget.threshold_crossed")},
		recorder,
		errors.New("budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool"),
	); err == nil || !strings.Contains(err.Error(), "continue failed") {
		t.Fatalf("expected remediation step failure, got %v", err)
	}
}

func TestAllowedTools_ExtractionAndFiltering(t *testing.T) {
	cfg := models.AgentConfig{
		ID: "a1",
		Config: []byte(`{
			"system_prompt": "x",
			"tools": ["t1", "t2", ""],
			"allowed_tools": ["t3", "t2", " "]
		}`),
	}
	allowed, constrained := extractAllowedToolSet(cfg)
	if !constrained {
		t.Fatal("expected constrained=true when tools list present")
	}
	for _, k := range []string{"t1", "t2", "t3"} {
		if _, ok := allowed[k]; !ok {
			t.Fatalf("expected allowed to include %q", k)
		}
	}

	in := []llm.ToolDefinition{{Name: "t1"}, {Name: "t2"}, {Name: "t9"}}
	out := filterTools(in, allowed, true)
	if len(out) != 2 {
		t.Fatalf("expected 2 tools after filtering, got %d", len(out))
	}
	if out[0].Name != "t1" || out[1].Name != "t2" {
		t.Fatalf("unexpected filtered tools: %+v", out)
	}

	out2 := filterTools(in, map[string]struct{}{}, false)
	if len(out2) != len(in) || out2[0].Name != in[0].Name || out2[2].Name != in[2].Name {
		t.Fatalf("expected pass-through when unconstrained, got %+v", out2)
	}
}

func TestAllowedTools_InvalidConfig_DoesNotConstrain(t *testing.T) {
	cfg := models.AgentConfig{Config: []byte("{")}
	allowed, constrained := extractAllowedToolSet(cfg)
	if constrained {
		t.Fatal("expected constrained=false for invalid json")
	}
	if len(allowed) != 0 {
		t.Fatalf("expected empty allowed set, got %+v", allowed)
	}

	cfg2 := models.AgentConfig{Config: []byte(`{"tools":"not-an-array"}`)}
	_, constrained = extractAllowedToolSet(cfg2)
	if constrained {
		t.Fatal("expected constrained=false when tools is not an array")
	}
}

func TestInjectHumanTaskToolResult_Branches(t *testing.T) {
	conv := &llm.Conversation{
		AgentID: "a1",
		Session: &llm.Session{ID: "s1", AgentID: "a1"},
	}
	agent := &LLMAgent{cfg: models.AgentConfig{ID: "a1"}, conversation: conv}

	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed"})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages, got %d", len(conv.Messages))
	}

	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: []byte("{")})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages after invalid json, got %d", len(conv.Messages))
	}

	pWrong, _ := json.Marshal(map[string]any{"requesting_agent": "other", "task_id": "t1"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: pWrong})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages after wrong agent, got %d", len(conv.Messages))
	}

	pReject, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t2", "rejection_reason": "nope"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.rejected", Payload: pReject})
	if len(conv.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(conv.Messages))
	}
	if conv.Messages[0].Role != "tool" || !strings.Contains(conv.Messages[0].Content, `"name":"human_task_request"`) {
		t.Fatalf("unexpected injected message: %+v", conv.Messages[0])
	}
	if !strings.Contains(conv.Messages[0].Content, `"ok":false`) || !strings.Contains(conv.Messages[0].Content, "nope") {
		t.Fatalf("expected rejection details in payload, got %q", conv.Messages[0].Content)
	}

	pExpire, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t3"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.expired", Payload: pExpire})
	if len(conv.Messages) != 2 {
		t.Fatalf("expected 2 injected messages, got %d", len(conv.Messages))
	}
	if !strings.Contains(conv.Messages[1].Content, "human task expired") {
		t.Fatalf("expected default expiry message, got %q", conv.Messages[1].Content)
	}

	pDone, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t4"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: pDone})
	if len(conv.Messages) != 3 {
		t.Fatalf("expected 3 injected messages, got %d", len(conv.Messages))
	}
	if !strings.Contains(conv.Messages[2].Content, `"ok":true`) {
		t.Fatalf("expected ok=true, got %q", conv.Messages[2].Content)
	}
}

func TestBudgetHelpers_ModelTierAndEstimateLLMCost(t *testing.T) {
	if got := modelTier("claude-haiku-4-5"); got != "haiku" {
		t.Fatalf("expected haiku tier, got %q", got)
	}
	if got := modelTier("CLAUDE-OPUS-4"); got != "opus" {
		t.Fatalf("expected opus tier, got %q", got)
	}
	if got := modelTier("claude-sonnet-4-5"); got != "sonnet" {
		t.Fatalf("expected sonnet tier, got %q", got)
	}
	if got := modelTier("unknown-model"); got != "sonnet" {
		t.Fatalf("expected default sonnet tier, got %q", got)
	}

	bt := &BudgetTracker{}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", 1_000_000, 1_000_000); got != 480 {
		t.Fatalf("haiku cost mismatch: got %d want 480", got)
	}
	if got := bt.estimateLLMCostCents("claude-opus-4", 1_000_000, 1_000_000); got != 9000 {
		t.Fatalf("opus cost mismatch: got %d want 9000", got)
	}
	if got := bt.estimateLLMCostCents("claude-sonnet-4-5", 1_000_000, 1_000_000); got != 1800 {
		t.Fatalf("sonnet cost mismatch: got %d want 1800", got)
	}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", -100, -100); got != 0 {
		t.Fatalf("negative token clamp mismatch: got %d want 0", got)
	}
}

func TestBudgetHelpers_MergeMeta_AndEvaluateAndEmitGuard(t *testing.T) {
	merged := mergeMeta(map[string]any{"a": 1}, map[string]any{"b": 2})
	m, ok := merged.(map[string]any)
	if !ok || m["a"] != 1 || m["b"] != 2 {
		t.Fatalf("mergeMeta map branch mismatch: %#v", merged)
	}
	wrapped := mergeMeta("value", map[string]any{"k": "v"})
	w, ok := wrapped.(map[string]any)
	if !ok || w["meta"] != "value" {
		t.Fatalf("mergeMeta wrap branch mismatch: %#v", wrapped)
	}

	ctx := context.Background()
	var nilTracker *BudgetTracker
	if err := nilTracker.evaluateAndEmit(ctx, ""); err != nil {
		t.Fatalf("nil tracker evaluateAndEmit should noop, got %v", err)
	}
	partial := &BudgetTracker{cfg: &config.Config{}}
	if err := partial.evaluateAndEmit(ctx, "vertical-1"); err != nil {
		t.Fatalf("partial tracker evaluateAndEmit should noop, got %v", err)
	}
}

func TestSchemaHelperPrimitives(t *testing.T) {
	if !isNumeric(1) || !isNumeric(1.5) || !isNumeric(uint32(3)) {
		t.Fatal("expected numeric primitives to be accepted")
	}
	if isNumeric("1") {
		t.Fatal("string should not be treated as numeric primitive")
	}

	if !isInteger(1) || !isInteger(int64(2)) || !isInteger(2.0) {
		t.Fatal("expected integer values to pass integer check")
	}
	if isInteger(2.5) || isInteger("2") {
		t.Fatal("expected non-integer values to fail integer check")
	}

	if got := requiredList([]any{"a", " ", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("requiredList []any mismatch: %+v", got)
	}
	if got := requiredList([]string{"x", "y"}); len(got) != 2 {
		t.Fatalf("requiredList []string mismatch: %+v", got)
	}
	if got := requiredList("x"); got != nil {
		t.Fatalf("requiredList default should return nil, got %+v", got)
	}
}

func TestRuntimeHelperFunctions_Misc(t *testing.T) {

	filtered := filterOutVerticalScopedAgentIDs([]string{"opco-ceo-v1", "empire-coordinator", "vp-product-v1", "x"}, "v1")
	if len(filtered) != 2 || filtered[0] != "empire-coordinator" {
		t.Fatalf("filterOutVerticalScopedAgentIDs mismatch: %+v", filtered)
	}

	deduped := dedupeToolCalls([]llm.ToolCall{
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_trend"}},
		{Name: "", Arguments: map[string]any{}},
	})
	if len(deduped) != 2 {
		t.Fatalf("expected deduped tool calls length 2, got %d", len(deduped))
	}

	reg := newMCPTurnRegistry()
	now := time.Now().UTC()
	reg.put("old", mcpTurnContext{CreatedAt: now.Add(-20 * time.Minute), ExpiresAt: now.Add(-time.Minute)})
	reg.put("new", mcpTurnContext{CreatedAt: now, ExpiresAt: now.Add(20 * time.Minute)})
	reg.mu.Lock()
	reg.pruneLocked(now)
	reg.mu.Unlock()
	if _, ok := reg.get("old"); ok {
		t.Fatal("expected old mcp turn context pruned")
	}
	if _, ok := reg.get("new"); !ok {
		t.Fatal("expected new mcp turn context retained")
	}

	if got := toStringList([]any{"a", " ", 3}); len(got) != 2 || got[0] != "a" || got[1] != "3" {
		t.Fatalf("toStringList []any mismatch: %+v", got)
	}
	if got := toStringList(`["x","y"]`); len(got) != 2 || got[0] != "x" {
		t.Fatalf("toStringList json string mismatch: %+v", got)
	}
	if got := toStringList("x, y"); len(got) != 2 || got[1] != "y" {
		t.Fatalf("toStringList csv mismatch: %+v", got)
	}

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

func (s *heartbeatStoreStub) MarkAgentTerminated(context.Context, string) error { return nil }

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

	_ = models.AgentConfig{}
}

func TestNormalizeEventTokenAndResolveProviderEventType(t *testing.T) {
	if normalizeEventToken("  Payment.Succeeded ") != "payment_succeeded" {
		t.Fatal("unexpected normalization")
	}
	if normalizeEventToken("") != "event" {
		t.Fatal("expected default event")
	}

	if resolveProviderEventType("domain", map[string]any{"status": "Confirmed-OK"}) != "confirmed_ok" {
		t.Fatal("expected domain status token")
	}
	if resolveProviderEventType("stripe", map[string]any{"type": "invoice.paid"}) != "invoice_paid" {
		t.Fatal("expected stripe type token")
	}
	if resolveProviderEventType("stripe", map[string]any{}) != "payment_event" {
		t.Fatal("expected stripe default")
	}
}

func TestVerifyProviderSignature_StripeAndDefaultAndNoSecret(t *testing.T) {
	body := []byte(`{"ok":true}`)

	if !verifyProviderSignature("email", "", body, http.Header{}) {
		t.Fatal("expected email unsigned accepted")
	}
	if verifyProviderSignature("stripe", "", body, http.Header{}) {
		t.Fatal("expected unsigned stripe rejected")
	}

	secret := "whsec_test"
	ts := "1700000000"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(ts))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	h := http.Header{}
	h.Set("Stripe-Signature", "t="+ts+",v1="+expected)
	if !verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature to verify")
	}
	h.Set("Stripe-Signature", "t="+ts+",v1=deadbeef")
	if verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature mismatch")
	}

	h = http.Header{}
	h.Set("X-Webhook-Token", "tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected token header to verify")
	}
	h = http.Header{}
	h.Set("Authorization", "Bearer tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected bearer token to verify")
	}
}

func TestAnthropicAPIRuntime_ContinueSession_BudgetEmergencyStops(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session:     config.LLMSessionConfig{LockTTL: time.Second},
			ClaudeAPI:   config.ClaudeAPIConfig{DefaultModel: "m", MaxRetries: 1},
		},
	}
	b := &BudgetTracker{lastState: map[string]string{"vertical|v1": "emergency"}}
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, b)

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
	if _, err := r.ContinueSession(ctx, s, llm.Message{Role: "user", Content: "x"}); err == nil || !strings.Contains(err.Error(), "budget emergency") {
		t.Fatalf("expected budget emergency error, got %v", err)
	}
}

func TestAnthropicAPIRuntime_ContinueSession_ParseFailure_PersistsAndRotates(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 1,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "m",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
		},
	}
	sessions := sessions.NewInMemoryRegistry(time.Second)
	turns := &apiTurnCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer srv.Close()

	r := NewAnthropicAPIRuntime(cfg, sessions, "owner", turns, nil, nil)
	r.apiURL = srv.URL
	r.httpClient = srv.Client()
	r.apiKey = "k"

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	oldID := s.ID
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
	if _, err := r.ContinueSession(ctx, s, llm.Message{Role: "user", Content: "x"}); err == nil || !strings.Contains(err.Error(), "anthropic status 500") {
		t.Fatalf("expected anthropic status error, got %v", err)
	}
	if turns.calls != 1 || turns.last.ParseOK {
		t.Fatalf("expected persisted parse failure, calls=%d parseOK=%v", turns.calls, turns.last.ParseOK)
	}

	if s.ID == oldID {
		t.Fatalf("expected session id to rotate after parse failures, old=%q new=%q", oldID, s.ID)
	}
	if s.ParseFailures != 0 || s.TurnCount != 0 || len(s.Messages) == 0 {
		t.Fatalf("expected reset state, session=%+v", s)
	}
}

type convStubAPI struct {
	loadOK    bool
	loadRec   ConversationRecord
	upsertErr error
}

func (c *convStubAPI) UpsertConversation(context.Context, ConversationRecord) error {
	return c.upsertErr
}

func (c *convStubAPI) LoadActiveConversation(context.Context, string, string, string) (ConversationRecord, bool, error) {
	return c.loadRec, c.loadOK, nil
}

type turnStubAPI struct{ err error }

func (t *turnStubAPI) AppendAgentTurn(context.Context, AgentTurnRecord) error { return t.err }

func TestAnthropicAPIRuntime_StartContinue_MissingKey_And_Success(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
				HaikuModel:   "claude-haiku-test",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
		},
	}
	sessions := sessions.NewInMemoryRegistry(1 * time.Second)
	convs := &convStubAPI{loadOK: true, loadRec: ConversationRecord{Messages: []llm.Message{{Role: "user", Content: "prior"}}, TurnCount: 1}, upsertErr: os.ErrInvalid}
	turns := &turnStubAPI{err: os.ErrInvalid}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if strings.TrimSpace(r.Header.Get("x-api-key")) == "" {
			http.Error(w, `{"error":{"message":"missing key"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"claude-test",
			"usage":{"input_tokens":10,"output_tokens":5},
			"content":[{"type":"text","text":"ok"}]
		}`))
	}))
	defer ts.Close()

	r := NewAnthropicAPIRuntime(cfg, sessions, "lock", turns, convs, nil)
	r.apiURL = ts.URL
	r.httpClient = ts.Client()

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.TurnCount != 1 {
		t.Fatalf("expected loaded turn_count=1, got %d", s.TurnCount)
	}

	r.apiKey = ""
	if _, err := r.ContinueSession(context.Background(), s, llm.Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected missing ANTHROPIC_API_KEY error")
	}

	r.apiKey = "k"
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "haiku", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, llm.Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("unexpected resp: %#v", resp)
	}
}

func TestAnthropicAPIRuntime_NilSession_Error(t *testing.T) {
	r := NewAnthropicAPIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "api", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeAPI: config.ClaudeAPIConfig{DefaultModel: "m"}}}, sessions.NewInMemoryRegistry(time.Second), "x", nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, llm.Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}
}

func TestExtractUsageTokensFromJSON_More(t *testing.T) {
	if u := extractUsageTokensFromJSON(nil); u.InputTokens != 0 || u.OutputTokens != 0 {
		t.Fatalf("expected empty usage")
	}

	if u := extractUsageTokensFromJSON([]byte("{")); u.InputTokens != 0 || u.OutputTokens != 0 || u.Model != "" {
		t.Fatalf("expected empty usage on decode error, got %#v", u)
	}
	raw := []byte(`{"model":"m","usage":{"input_tokens":1,"output_tokens":2}}`)
	u := extractUsageTokensFromJSON(raw)
	if u.Model != "m" || u.InputTokens != 1 || u.OutputTokens != 2 {
		t.Fatalf("unexpected usage: %#v", u)
	}
}

func TestAnthropicAPIRuntime_SendRequest_ErrorBranches(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "m",
				MaxRetries:   1,
			},
		},
	}
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, nil)
	r.apiKey = "k"

	t.Run("decode error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("not-json"))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "decode anthropic response") {
			t.Fatalf("expected decode error, got %v", err)
		}
	})

	t.Run("status error with message", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"bad"}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic status 400") || !strings.Contains(err.Error(), "bad") {
			t.Fatalf("expected status error, got %v", err)
		}
	})

	t.Run("status error falls back to body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":""}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic status 500") {
			t.Fatalf("expected status error, got %v", err)
		}
	})

	t.Run("200 but error payload", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic error") {
			t.Fatalf("expected anthropic error, got %v", err)
		}
	})
}

func TestAnthropicHelpers_ToAnthropicMessage_AndBuildRequestErrors(t *testing.T) {
	if _, ok := toAnthropicMessage(llm.Message{Role: "user", Content: "  "}); ok {
		t.Fatal("expected empty content to be skipped")
	}

	am, ok := toAnthropicMessage(llm.Message{Role: "assistant", Content: "hi"})
	if !ok || am.Role != "assistant" {
		t.Fatalf("expected assistant role, got ok=%v msg=%+v", ok, am)
	}

	um, ok := toAnthropicMessage(llm.Message{Role: "board_directive", Content: "x"})
	if !ok || um.Role != "user" {
		t.Fatalf("expected user role for unknown, got ok=%v msg=%+v", ok, um)
	}
	m, ok := toAnthropicMessage(llm.Message{Role: "tool", Content: "x"})
	if !ok || m.Role != "user" || !strings.Contains(m.Content.(string), "Tool result:") {
		t.Fatalf("unexpected tool message: ok=%v msg=%+v", ok, m)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session:     config.LLMSessionConfig{LockTTL: time.Second},
			ClaudeAPI:   config.ClaudeAPIConfig{DefaultModel: ""},
		},
	}
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, nil)

	_, err := r.buildRequest(context.Background(), &llm.Session{AgentID: "a", SystemPrompt: "s", Messages: []llm.Message{{Role: "user", Content: " "}}}, llm.Message{Role: "user", Content: ""})
	if err == nil {
		t.Fatal("expected messages required error")
	}

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a", Role: "pm-agent", VerticalID: "v1"})
	_, err = r.buildRequest(ctx, &llm.Session{AgentID: "a", SystemPrompt: "s", Messages: []llm.Message{{Role: "user", Content: "hi"}}}, llm.Message{Role: "user", Content: "x"})
	if err == nil || !strings.Contains(err.Error(), "default_model is required") {
		t.Fatalf("expected model required error, got %v", err)
	}
}

func TestClaudeCLIRuntime_Run_ErrorOutputBranches(t *testing.T) {
	dir := t.TempDir()

	quietFail := filepath.Join(dir, "quiet_fail.sh")
	if err := os.WriteFile(quietFail, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write quiet fail: %v", err)
	}

	noisyFail := filepath.Join(dir, "noisy_fail.sh")
	if err := os.WriteFile(noisyFail, []byte("#!/bin/sh\necho bad 1>&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write noisy fail: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      quietFail,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "claude cli run failed") {
		t.Fatalf("expected run error, got %v", err)
	}
	if strings.Contains(err.Error(), "stderr=") {
		t.Fatalf("expected quiet failure to avoid stderr= wrapper, got %v", err)
	}

	rt.cfg.LLM.ClaudeCLI.Command = noisyFail
	_, err = rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "stderr=bad") {
		t.Fatalf("expected stderr to be included, got %v", err)
	}
}

func TestClaudeCLIRuntime_Run_TimeoutErrorIsExplicit(t *testing.T) {
	dir := t.TempDir()
	sleeper := filepath.Join(dir, "sleepy.sh")
	if err := os.WriteFile(sleeper, []byte("#!/bin/sh\nsleep 2\n"), 0o755); err != nil {
		t.Fatalf("write sleeper: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      sleeper,
				Timeout:      150 * time.Millisecond,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)
	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "timeout after") {
		t.Fatalf("expected explicit timeout error, got: %v", err)
	}
}

func TestSummarizeCLIErrorOutput_Truncates(t *testing.T) {
	raw := strings.Repeat("x", 400)
	out := summarizeCLIErrorOutput(raw)
	if len(out) > 245 {
		t.Fatalf("expected truncation, got len=%d", len(out))
	}
	if !strings.HasSuffix(out, "...") {
		t.Fatalf("expected ..., got %q", out)
	}
}

func TestParseCLIResponse_ToolCallsForms(t *testing.T) {
	resp := parseCLIResponse([]byte(`{
		"result":"ok",
		"session_id":"sess-123",
		"tool_calls":[{"name":"t1","arguments":{"a":1}}, {"name":"","arguments":{"b":2}}],
		"content":[{"type":"tool_use","name":"t2","arguments":{"x":2}}]
	}`))
	if strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("expected ok content, got %q", resp.Message.Content)
	}
	if resp.SessionID != "sess-123" {
		t.Fatalf("expected session id to be parsed, got %q", resp.SessionID)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "t2" && resp.ToolCalls[0].Name != "t1" {
		t.Fatalf("unexpected tool call order: %+v", resp.ToolCalls)
	}

	plain := parseCLIResponse([]byte("hello"))
	if plain.Message.Content != "hello" {
		t.Fatalf("expected plain content passthrough, got %q", plain.Message.Content)
	}
}

func TestClaudeCLIHelpers_ToolNamesAndUnsupportedFlags(t *testing.T) {
	if got := toolNamesCSV([]llm.ToolDefinition{
		{Name: "agent_message"},
		{Name: "sql_execute"},
		{Name: "agent_message"},
		{Name: " "},
	}); got != "agent_message,sql_execute" {
		t.Fatalf("unexpected tool csv: %q", got)
	}
	if !isUnsupportedCLIFlagError(assertErr("unknown option --system-prompt")) {
		t.Fatal("expected unsupported --system-prompt flag detection")
	}
	if !isUnsupportedCLIFlagError(assertErr("unrecognized option '--tools'")) {
		t.Fatal("expected unsupported --tools flag detection")
	}
	if isUnsupportedCLIFlagError(assertErr("unknown option --foo")) {
		t.Fatal("unexpected detection for unrelated flag")
	}
}

func TestClaudeCLIRuntime_EffectiveCLITimeout_FactoryFloorAndEnvOverride(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				Timeout:      30 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	factoryCtx := WithActor(context.Background(), models.AgentConfig{ID: "factory-1", Mode: "factory"})
	if got := rt.effectiveCLITimeout(factoryCtx); got != 300*time.Second {
		t.Fatalf("expected factory timeout floor 300s, got %s", got)
	}

	t.Setenv("EMPIREAI_CLAUDE_TIMEOUT_SECONDS", "45")
	if got := rt.effectiveCLITimeout(factoryCtx); got != 45*time.Second {
		t.Fatalf("expected env timeout override 45s, got %s", got)
	}
}

type turnStub struct{ err error }

func (t *turnStub) AppendAgentTurn(context.Context, AgentTurnRecord) error { return t.err }

type turnCaptureStub struct {
	last AgentTurnRecord
}

func (t *turnCaptureStub) AppendAgentTurn(_ context.Context, rec AgentTurnRecord) error {
	t.last = rec
	return nil
}

type convStub struct {
	loadRec   ConversationRecord
	loadOK    bool
	loadErr   error
	upsertErr error
}

func (c *convStub) UpsertConversation(context.Context, ConversationRecord) error { return c.upsertErr }

func (c *convStub) LoadActiveConversation(context.Context, string, string, string) (ConversationRecord, bool, error) {
	return c.loadRec, c.loadOK, c.loadErr
}

func writeScript(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestClaudeCLIRuntime_StartContinue_And_AuthError(t *testing.T) {

	okScript := writeScript(t, "ok.sh", "#!/bin/sh\n# ignore stdin/args\ncat >/dev/null || true\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"hi\"},{\"type\":\"tool_use\",\"name\":\"agent_message\",\"input\":{\"x\":1}}]}'\n")

	authScript := writeScript(t, "auth.sh", "#!/bin/sh\necho 'not logged in' 1>&2\nexit 1\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      okScript,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	sessions := sessions.NewInMemoryRegistry(1 * time.Second)
	turns := &turnStub{err: os.ErrInvalid}
	convs := &convStub{loadOK: true, loadRec: ConversationRecord{Messages: []llm.Message{{Role: "user", Content: "prior"}}, TurnCount: 1}, upsertErr: os.ErrInvalid}

	r := NewClaudeCLIRuntime(cfg, sessions, "lock", turns, nil, nil, convs)
	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.TurnCount != 1 {
		t.Fatalf("expected loaded turn_count=1, got %d", s.TurnCount)
	}

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "worker", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, llm.Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "hi" || len(resp.ToolCalls) != 1 {
		t.Fatalf("unexpected resp: %#v", resp)
	}

	cfg2 := *cfg
	cfg2.LLM.ClaudeCLI.Command = authScript
	r2 := NewClaudeCLIRuntime(&cfg2, sessions, "lock", nil, nil, nil, nil)
	s2, err := r2.StartSession(context.Background(), "a2", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession2: %v", err)
	}
	if _, err := r2.ContinueSession(context.Background(), s2, llm.Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected auth required error")
	}
}

func TestClaudeCLIRuntime_NilSession_Error(t *testing.T) {
	r := NewClaudeCLIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeCLI: config.ClaudeCLIConfig{Command: "true", OutputFormat: "json"}}}, sessions.NewInMemoryRegistry(time.Second), "x", nil, nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, llm.Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}

	_, _, _, _ = runtime.Caller(0)
}

func TestClaudeCLIRuntime_FirstTurn_WithTools_UsesStdinPrompt(t *testing.T) {
	stdinRequired := writeScript(t, "stdin_required.sh", "#!/bin/sh\nin=\"$(cat)\"\nif [ -z \"$in\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}'\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      stdinRequired,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", nil, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-tools", "sys", []llm.ToolDefinition{{Name: "agent_message"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	resp, err := r.ContinueSession(context.Background(), s, llm.Message{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "ok" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestClaudeCLIRuntime_PromptArgFallback_OnPrintInputError(t *testing.T) {

	promptArgOnly := writeScript(t, "prompt_arg_only.sh", "#!/bin/sh\nextra=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    -p)\n      shift\n      ;;\n    --session-id|--output-format|--system-prompt|--tools|--mcp-config|-r)\n      if [ \"$#\" -lt 2 ]; then break; fi\n      shift 2\n      ;;\n    --strict-mcp-config)\n      shift\n      ;;\n    --)\n      shift\n      if [ \"$#\" -gt 0 ]; then\n        extra=\"$1\"\n      fi\n      break\n      ;;\n    *)\n      extra=\"$1\"\n      shift\n      ;;\n  esac\ndone\nif [ -z \"$extra\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok-from-arg\"}]}'\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      promptArgOnly,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	turns := &turnCaptureStub{}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", turns, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-fallback", "sys", []llm.ToolDefinition{{Name: "agent_message"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	resp, err := r.ContinueSession(context.Background(), s, llm.Message{Role: "user", Content: "hello from event"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "ok-from-arg" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if string(turns.last.RequestPayload) == "" || !strings.Contains(string(turns.last.RequestPayload), `"prompt_arg_fallback_used":true`) {
		t.Fatalf("expected prompt_arg_fallback_used=true in turn payload, got: %s", string(turns.last.RequestPayload))
	}
}

func TestClaudeCLIRuntime_EmptyPromptFailsFast(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", nil, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-empty", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := r.ContinueSession(context.Background(), s, llm.Message{Role: "user", Content: "   "}); err == nil || !strings.Contains(err.Error(), "empty prompt input") {
		t.Fatalf("expected empty prompt error, got: %v", err)
	}
}

func TestAgentManager_ControlEvent_SpinupRequested_SpawnsOpCo(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()

	if am.Count() != 0 {
		t.Fatalf("expected empty manager initially")
	}

	payload := []byte(`{"vertical_id":"v1","mandate":{"vertical_id":"v1","founder_notes":"go"}}`)
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spinup_requested"),
		SourceAgent: "tester",
		VerticalID:  "v1",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if am.Count() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected opco agents spawned")
}

type panicProbeAgent struct{ id string }

func (a *panicProbeAgent) ID() string { return a.id }

func (a *panicProbeAgent) Type() string { return "stub" }

func (a *panicProbeAgent) Subscriptions() []events.EventType { return nil }

func (a *panicProbeAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestPanicBackoff(t *testing.T) {
	cases := []struct {
		panics int
		want   time.Duration
	}{
		{panics: 0, want: 1 * time.Second},
		{panics: 1, want: 1 * time.Second},
		{panics: 2, want: 5 * time.Second},
		{panics: 3, want: 30 * time.Second},
		{panics: 4, want: 2 * time.Minute},
		{panics: 5, want: 10 * time.Minute},
		{panics: 9, want: 10 * time.Minute},
	}
	for _, tc := range cases {
		if got := panicBackoff(tc.panics); got != tc.want {
			t.Fatalf("panicBackoff(%d)=%s want=%s", tc.panics, got, tc.want)
		}
	}
}

func TestAgentManager_HandleAgentLoopPanic_EmitsAndEscalates(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	am.mu.Lock()
	am.agentCfg["worker-1"] = models.AgentConfig{
		ID:          "worker-1",
		Type:        "stub",
		Role:        "backend-agent",
		Mode:        "holding",
		ParentAgent: "empire-coordinator",
		Config:      mustJSON(map[string]any{"system_prompt": "x"}),
	}
	am.mu.Unlock()

	panicCh := bus.Subscribe("watch-panic", events.EventType("ops.agent_panic"))
	failedCh := bus.Subscribe("empire-coordinator", events.EventType("ops.agent_failed"))
	agent := &panicProbeAgent{id: "worker-1"}

	am.handleAgentLoopPanic(context.Background(), agent, 1, "boom once")

	select {
	case evt := <-panicCh:
		if evt.Type != events.EventType("ops.agent_panic") {
			t.Fatalf("unexpected panic event type: %s", evt.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic event for first panic")
	}

	select {
	case evt := <-failedCh:
		t.Fatalf("did not expect ops.agent_failed on first panic, got %s", evt.ID)
	case <-time.After(200 * time.Millisecond):

	}

	am.handleAgentLoopPanic(context.Background(), agent, 5, "boom terminal")

	select {
	case <-panicCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic on terminal panic")
	}
	select {
	case evt := <-failedCh:
		if evt.Type != events.EventType("ops.agent_failed") {
			t.Fatalf("unexpected failure escalation type: %s", evt.Type)
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("expected ops.agent_failed escalation event after terminal panic")
	}
}

type templateErrStore struct {
	rec OrgTemplateRecord
	err error
}

func (s *templateErrStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }

func (s *templateErrStore) LoadAgents(context.Context) ([]PersistedAgent, error) { return nil, nil }

func (s *templateErrStore) MarkAgentTerminated(context.Context, string) error { return nil }

func (s *templateErrStore) EnsureVerticalSchema(context.Context, string) error { return nil }

func (s *templateErrStore) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	return s.rec, s.err
}

func (s *templateErrStore) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return s.rec, s.err
}

func (s *templateErrStore) SetVerticalTemplateVersion(context.Context, string, string) error {
	return nil
}

func (s *templateErrStore) UpsertRoutingRule(context.Context, PersistedRoutingRule) error { return nil }

func (s *templateErrStore) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error) {
	return nil, nil
}

func (s *templateErrStore) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}

func (s *templateErrStore) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}

func (s *templateErrStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (s *templateErrStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func TestManager_LoadLatestTemplate_ErrorBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected error without store")
	}

	am.store = &templateErrStore{err: errors.New("boom")}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected store error")
	}

	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected empty version error")
	}

	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "v1", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected missing bootstrap routes error")
	}

	bootstrap, _ := json.Marshal([]map[string]any{{"event_pattern": "opco.*", "subscriber_role": "opco-ceo", "reason": "b"}})
	agents, _ := json.Marshal([]map[string]any{{"role": "opco-ceo", "type": "worker", "system_prompt": "x"}})
	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "v2", Agents: agents, BootstrapRoutes: bootstrap, SeededRoutes: []byte("[]")}}
	snap, err := am.loadLatestTemplate(context.Background())
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if snap.Version != "v2" || len(snap.BootstrapRoutes) != 1 || len(snap.Agents) != 1 {
		t.Fatalf("unexpected snap: %#v", snap)
	}
}

func TestManager_TemplateHelpers_RenderAndExpand(t *testing.T) {
	if got := expandTemplateText("", map[string]string{"x": "y"}); got != "" {
		t.Fatalf("expected empty passthrough")
	}
	if got := expandTemplateText("hello {k}", map[string]string{"{k}": "v"}); got != "hello v" {
		t.Fatalf("unexpected expand: %q", got)
	}

	roster := renderOpCoRoster([]orgTemplateAgent{
		{Role: "opco-ceo"},
		{Role: "vp-product"},
		{Role: ""},
	}, "v1")
	if roster == "" {
		t.Fatalf("expected roster text")
	}

	mandate := renderMandateText(models.MandateDocument{
		VerticalID:    "v1",
		FounderNotes:  "notes",
		BusinessBrief: []byte(`{"problem":"x"}`),
		MVPSpec:       []byte(`{"features":["a"]}`),
		Brand:         []byte(`{"name":"b"}`),
		Budget:        []byte(`{"cap":1}`),
	})
	if mandate == "" || mandate[0] != '{' {
		t.Fatalf("unexpected mandate: %q", mandate)
	}

	if got := resolveTemplateSubscriber("v1", orgTemplateRoute{SubscriberID: "explicit"}); got != "explicit" {
		t.Fatalf("expected explicit id, got %q", got)
	}
	if got := resolveTemplateSubscriber("v1", orgTemplateRoute{SubscriberRole: "vp-product"}); got == "" || got == "vp-product" {
		t.Fatalf("expected role-derived opco id, got %q", got)
	}
}

func waitForEventTypes(t *testing.T, ch <-chan events.Event, expected []string) map[string]events.Event {
	t.Helper()
	need := make(map[string]struct{}, len(expected))
	for _, typ := range expected {
		need[typ] = struct{}{}
	}
	got := make(map[string]events.Event, len(expected))
	deadline := time.After(1500 * time.Millisecond)
	for len(got) < len(expected) {
		select {
		case evt := <-ch:
			typ := string(evt.Type)
			if _, ok := need[typ]; ok {
				if _, seen := got[typ]; !seen {
					got[typ] = evt
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got=%v expected=%v", keysFromEventMap(got), expected)
		}
	}
	return got
}

func keysFromEventMap(m map[string]events.Event) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestFactoryPipelineCoordinator_ValidationLifecycleHappyPath(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	ch := bus.Subscribe("watch-validation-lifecycle",
		events.EventType("validation.started"),
		events.EventType("brand.requested"),
		events.EventType("spec.validation_requested"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("validation.package_ready"),
	)

	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 82}),
	})
	waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.completed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"brief": "ok"}),
	}, "g1")

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec": "draft"}),
	}, "g2")
	waitForEventTypes(t, ch, []string{"spec.validation_requested"})

	pc.handleSpecValidationPassed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_passed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "status": "passed"}),
	})
	waitForEventTypes(t, ch, []string{"cto.spec_review_requested"})

	pc.handleCTOApproved(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "notes": "looks good"}),
	})
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("brand.candidates_ready"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"names": []string{"a", "b"}}),
	}, "g4")

	got := waitForEventTypes(t, ch, []string{"validation.package_ready"})
	pkg := got["validation.package_ready"]
	var payload map[string]any
	if err := json.Unmarshal(pkg.Payload, &payload); err != nil {
		t.Fatalf("decode package payload: %v", err)
	}
	if asInt(payload["spec_version"]) != 1 {
		t.Fatalf("expected packaged spec_version=1, got %#v", payload["spec_version"])
	}
}

func TestFactoryPipelineCoordinator_RevisionAndResumePaths(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	ch := bus.Subscribe("watch-validation-revision",
		events.EventType("spec.revision_requested"),
		events.EventType("validation.more_data_needed"),
		events.EventType("brand.revision_needed"),
		events.EventType("validation.started"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("brand.requested"),
		events.EventType("vertical.killed"),
	)

	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 79}),
	})
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.completed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"brief": "ok"}),
	}, "g1")
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec": "v1"}),
	}, "g2")

	pc.handleSpecValidationFailed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "status": "blocker"}),
	})
	waitForEventTypes(t, ch, []string{"spec.revision_requested"})

	pc.handleValidationMoreData(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.needs_more_data"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "need more evidence"}),
	})
	waitForEventTypes(t, ch, []string{"validation.more_data_needed"})

	pc.handleBrandRevision(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.revision_needed"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"note": "rename"}),
	})
	waitForEventTypes(t, ch, []string{"brand.revision_needed"})

	pc.handleVerticalResumed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.resumed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"resumed_by": "human"}),
	})
	waitForEventTypes(t, ch, []string{"validation.started", "spec.revision_requested", "cto.spec_review_requested", "brand.requested"})

	pc.handleValidationRejected(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.vertical_rejected"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "bad economics"}),
	})
	waitForEventTypes(t, ch, []string{"vertical.killed"})

	pc.handleCTORevisionNeeded(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "tighten architecture"}),
	})
	waitForEventTypes(t, ch, []string{"spec.revision_requested"})
}

func TestFactoryPipelineCoordinator_ScanDedupAndCompletion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()

	existingVerticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals)
		VALUES ($1::uuid, 'Dental Clinic Scheduling', 'dental-clinic-scheduling', 'paraguay', 'discovered', 'factory', '{}'::jsonb)
	`, existingVerticalID); err != nil {
		t.Fatalf("insert existing vertical: %v", err)
	}

	ch := bus.Subscribe("watch-scan-dedup",
		events.EventType("dedup.ambiguous"),
		events.EventType("vertical.discovered"),
		events.EventType("scan.completed"),
	)

	scanID := uuid.NewString()
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":   scanID,
			"mode":      "saas_gap",
			"geography": "paraguay",
		}),
	})

	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("category.assessed"),
		SourceAgent: "market-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":         scanID,
			"vertical_name":   "Dental Clinic Scheduling SaaS",
			"signal_strength": 88,
			"geography":       "paraguay",
			"mode":            "saas_gap",
		}),
	})

	dedupEvt := waitForEventTypes(t, ch, []string{"dedup.ambiguous"})["dedup.ambiguous"]
	var dedupPayload map[string]any
	if err := json.Unmarshal(dedupEvt.Payload, &dedupPayload); err != nil {
		t.Fatalf("decode dedup payload: %v", err)
	}
	dedupEventID := strings.TrimSpace(asString(dedupPayload["dedup_event_id"]))
	if dedupEventID == "" {
		t.Fatal("expected dedup_event_id in dedup.ambiguous payload")
	}

	pc.handleDedupResolved(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("dedup.resolved"),
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupEventID,
			"action":         "keep_both",
		}),
	})
	waitForEventTypes(t, ch, []string{"vertical.discovered"})

	pc.handleScanCompletion(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("market_research.scan_complete"),
		Payload: mustJSON(map[string]any{
			"scan_id": scanID,
		}),
	})
	waitForEventTypes(t, ch, []string{"scan.completed"})

	if got := len(pc.SnapshotScans()); got != 0 {
		t.Fatalf("expected scan accumulator cleared after completion, got %d", got)
	}
}

func TestFactoryPipelineCoordinator_DiscoveryNameAndSlugAreCanonical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()

	scanID := uuid.NewString()
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":   scanID,
			"mode":      "saas_trend",
			"geography": "paraguay",
		}),
	})

	ch := bus.Subscribe("watch-canonical-discovery", events.EventType("vertical.discovered"))
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("trend.identified"),
		SourceAgent: "trend-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":                scanID,
			"mode":                   "saas_trend",
			"geography":              "paraguay",
			"trend_category":         "instant_payments",
			"trend_description":      "Paraguay's instant payment system is experiencing explosive growth, with 28M transactions in a month and a regulatory interoperability mandate.",
			"opportunity_hypothesis": "Build unified rails to orchestrate payments, merchant onboarding, reconciliation, and compliant reporting across acquirers and banks.",
			"evidence":               "sample",
			"signal_strength":        73,
		}),
	})

	evt := waitForEventTypes(t, ch, []string{"vertical.discovered"})["vertical.discovered"]
	payload := parsePayloadMap(evt.Payload)
	if got := strings.TrimSpace(asString(payload["name"])); got != "Instant Payments" {
		t.Fatalf("expected concise canonical vertical name, got %q", got)
	}
	verticalID := strings.TrimSpace(asString(payload["vertical_id"]))
	if verticalID == "" {
		t.Fatalf("expected vertical_id in payload, got %v", payload)
	}
	var dbName, dbSlug string
	if err := db.QueryRowContext(ctx, `
		SELECT name, slug
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&dbName, &dbSlug); err != nil {
		t.Fatalf("load discovered vertical: %v", err)
	}
	if strings.TrimSpace(dbName) != "Instant Payments" {
		t.Fatalf("expected persisted canonical name, got %q", dbName)
	}
	if len(dbSlug) > maxVerticalSlugLen+1+8 {
		t.Fatalf("expected slug length <= %d, got %d (%q)", maxVerticalSlugLen+1+8, len(dbSlug), dbSlug)
	}
	if !strings.HasSuffix(dbSlug, "-"+verticalID[:8]) {
		t.Fatalf("expected slug suffix to include id prefix, slug=%q vertical=%q", dbSlug, verticalID)
	}
}

func TestFactoryPipelineCoordinator_ValidationStartedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Pet Grooming Operations', 'pet-grooming-ops', 'argentina', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	ch := bus.Subscribe("watch-validation-enriched", events.EventType("validation.started"))
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81.25}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started"})
	evt := got["validation.started"]
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["vertical_name"])) != "Pet Grooming Operations" {
		t.Fatalf("expected vertical_name from verticals table, got %+v", payload["vertical_name"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "argentina" {
		t.Fatalf("expected geography from verticals table, got %+v", payload["geography"])
	}
	scoring, _ := payload["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 81.25 {
		t.Fatalf("expected scoring payload preserved, got %+v", payload["scoring"])
	}
}

func TestFactoryPipelineCoordinator_BrandRequestedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Payroll Ops', 'payroll-ops', 'paraguay', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	ch := bus.Subscribe("watch-brand-request-enriched",
		events.EventType("validation.started"),
		events.EventType("brand.requested"),
	)
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 86.5}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})
	brand := parsePayloadMap(got["brand.requested"].Payload)
	if strings.TrimSpace(asString(brand["vertical_name"])) != "Payroll Ops" {
		t.Fatalf("expected brand payload vertical_name from verticals table, got %+v", brand["vertical_name"])
	}
	if strings.TrimSpace(asString(brand["geography"])) != "paraguay" {
		t.Fatalf("expected brand payload geography from verticals table, got %+v", brand["geography"])
	}
	scoring, _ := brand["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 86.5 {
		t.Fatalf("expected brand payload scoring preserved, got %+v", brand["scoring"])
	}
}

func TestFactoryPipelineCoordinator_ValidationResumedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Dental Clinic Scheduling', 'dental-clinic-scheduling', 'paraguay', 'researching', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.mu.Lock()
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID:     verticalID,
		Status:         "active",
		G1Research:     false,
		G2Spec:         true,
		G3CTO:          true,
		G4Brand:        true,
		ScoringPayload: mustJSON(map[string]any{"composite_score": 77.5}),
	}
	pc.mu.Unlock()

	ch := bus.Subscribe("watch-validation-resume-enriched", events.EventType("validation.started"))
	pc.handleVerticalResumed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.resumed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"resumed_by": "human"}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started"})
	evt := got["validation.started"]
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["vertical_name"])) != "Dental Clinic Scheduling" {
		t.Fatalf("expected vertical_name from verticals table on resumed path, got %+v", payload["vertical_name"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "paraguay" {
		t.Fatalf("expected geography from verticals table on resumed path, got %+v", payload["geography"])
	}
	scoring, _ := payload["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 77.5 {
		t.Fatalf("expected scoring payload from validation state, got %+v", payload["scoring"])
	}
}

func TestFactoryPipelineCoordinator_InnerRevisionAndPackagedState(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	pc.mu.Lock()
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID: verticalID,
		Status:     "active",
	}
	pc.mu.Unlock()

	for i := 0; i < maxInnerRevisions; i++ {
		if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.revision_needed"),
			VerticalID: verticalID,
			Payload:    mustJSON(map[string]any{"cycle": i + 1}),
		}); escalated {
			t.Fatalf("did not expect escalation at cycle %d", i+1)
		}
	}

	if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"cycle": maxInnerRevisions + 1}),
	}); !escalated {
		t.Fatal("expected escalation after max inner revision cycles")
	}

	pc.handleSpecRevisionRequested(events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_requested"),
		VerticalID: verticalID,
	})
	pc.mu.Lock()
	gotInner := pc.validations[verticalID].InnerRevisionCount
	pc.mu.Unlock()
	if gotInner != 0 {
		t.Fatalf("expected inner revision count reset to 0, got %d", gotInner)
	}

	pc.handleValidationPackaged(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.ready_for_review"),
		VerticalID: verticalID,
	})
	pc.mu.Lock()
	gotStatus := pc.validations[verticalID].Status
	pc.mu.Unlock()
	if gotStatus != "packaged" {
		t.Fatalf("expected packaged status, got %q", gotStatus)
	}
}

func TestFactoryPipelineCoordinator_ShardsHelpersAndAsFloat(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS shards (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			root_task_id UUID NOT NULL,
			scan_id UUID,
			stage TEXT NOT NULL,
			shard_index INT NOT NULL,
			shard_count INT NOT NULL,
			shard_key TEXT NOT NULL,
			scope JSONB NOT NULL,
			agent_id TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			deadline_at TIMESTAMPTZ NOT NULL,
			budget_cents INT NOT NULL,
			spend_cents INT NOT NULL DEFAULT 0,
			retry_count INT NOT NULL DEFAULT 0,
			error TEXT,
			assigned_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('agent-a', 'ephemeral', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed shard agent: %v", err)
	}

	scanID := "scan-runtime-helper"
	scanUUID := stableUUID(scanID).String()
	shardID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'finance',
			'{}'::jsonb, 'agent-a', 'assigned', now() + interval '15 minute', 100, now()
		)
	`, shardID, uuid.NewString(), scanUUID); err != nil {
		t.Fatalf("seed shard: %v", err)
	}

	total, completed, failed, ok := pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 0 || failed != 0 {
		t.Fatalf("unexpected shard progress before completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got == "" {
		t.Fatal("expected markShardCompletedByAgent to return completed shard id")
	}
	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got != "" {
		t.Fatalf("expected no second completion id after terminal update, got %q", got)
	}

	total, completed, failed, ok = pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 1 || failed != 0 {
		t.Fatalf("unexpected shard progress after completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := asFloat("12.5"); got != 12.5 {
		t.Fatalf("asFloat string parse mismatch: %v", got)
	}
	if got := asFloat(7); got != 7 {
		t.Fatalf("asFloat int parse mismatch: %v", got)
	}
	if got := asFloat(nil); got != 0 {
		t.Fatalf("asFloat nil should be zero, got %v", got)
	}
}

func TestFactoryPipelineCoordinator_InterceptPolicyAndRunMaintenance(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)

	if consume, handled := pc.interceptPolicy("scan.requested", events.Event{ID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("scan.requested should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("vertical.shortlisted", events.Event{ID: uuid.NewString(), VerticalID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("vertical.shortlisted should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("spec.validation_passed", events.Event{ID: uuid.NewString()}); consume || handled {
		t.Fatalf("spec.validation_passed without vertical should not be handled; got consume=%v handled=%v", consume, handled)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pc.RunMaintenance(ctx)
}

func ensurePipelineStateTables(t *testing.T, ctx context.Context, pc *FactoryPipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.db == nil {
		t.Fatal("pipeline coordinator db required")
	}
	if _, err := pc.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id UUID PRIMARY KEY,
			campaign_id UUID NOT NULL,
			mode TEXT NOT NULL,
			geography TEXT NOT NULL,
			expected_agents INT NOT NULL,
			agents_complete INT NOT NULL DEFAULT 0,
			completed_by JSONB NOT NULL DEFAULT '[]'::jsonb,
			reports JSONB NOT NULL DEFAULT '[]'::jsonb,
			verticals_discovered INT NOT NULL DEFAULT 0,
			verticals_skipped INT NOT NULL DEFAULT 0,
			pending_dedup INT NOT NULL DEFAULT 0,
			timeout_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '90 minutes',
			started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			completed_at TIMESTAMPTZ
		);
		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			scan_id UUID NOT NULL REFERENCES scan_accumulators(scan_id),
			candidate JSONB NOT NULL DEFAULT '{}'::jsonb,
			existing_id UUID NOT NULL DEFAULT gen_random_uuid(),
			dedup_event_id UUID,
			signal_strength INT NOT NULL DEFAULT 0,
			geography TEXT NOT NULL,
			discovery_mode TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			g1_research BOOLEAN NOT NULL DEFAULT false,
			g2_spec_approved BOOLEAN NOT NULL DEFAULT false,
			g3_cto_approved BOOLEAN NOT NULL DEFAULT false,
			g4_brand_ready BOOLEAN NOT NULL DEFAULT false,
			research_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			spec_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			cto_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			brand_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			revision_count INT NOT NULL DEFAULT 0,
			inner_revision_count INT NOT NULL DEFAULT 0,
			spec_version INT NOT NULL DEFAULT 0,
			packaging_requested_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		t.Fatalf("create pipeline persistence tables: %v", err)
	}
}

func TestFactoryPipelineCoordinator_PersistAndLoadState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pc)

	verticalID := uuid.NewString()
	existingVerticalID := uuid.NewString()
	scanID := uuid.NewString()
	dedupID := uuid.NewString()
	campaignID := uuid.NewString()
	geoID := uuid.NewString()
	now := time.Now().UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'Payroll Ops', 'payroll-ops', 'argentina', 'shortlisted', 'factory', now(), now()),
			($2::uuid, 'Existing Payroll', 'existing-payroll', 'argentina', 'operating', 'operating', now(), now())
	`, verticalID, existingVerticalID); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'argentina', 'AR', 'latam', now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_gap', 'normal', 'active', now())
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed campaign: %v", err)
	}

	pc.mu.Lock()
	pc.scans[scanID] = &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        "saas_gap",
		Geography:   "argentina",
		Expected:    2,
		CompletedBy: map[string]struct{}{"market-research-agent": {}},
		ReportData: []map[string]any{
			{"signal_strength": 79},
			{"signal_strength": 74},
			{"signal_strength": 66},
		},
		Reports:    3,
		Discovered: 2,
		Skipped:    1,
		CreatedAt:  now.Add(-15 * time.Minute),
	}
	pc.pendingDedup[dedupID] = pendingCandidate{
		DedupEventID: dedupID,
		ExistingID:   existingVerticalID,
		ScanID:       scanID,
		CampaignID:   campaignID,
		Mode:         "saas_gap",
		Geography:    "argentina",
		Name:         "Payroll Ops",
		Signal:       79,
		Payload:      map[string]any{"scan_id": scanID, "campaign_id": campaignID, "name": "Payroll Ops"},
	}
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID:           verticalID,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                false,
		G4Brand:              false,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "pending"}),
		BrandPayload:         mustJSON(map[string]any{"candidates": []string{"x"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 81.5}),
		RevisionCount:        1,
		InnerRevisionCount:   2,
		SpecVersion:          3,
		PackagingRequested:   true,
		PackagingRequestedAt: ptrTime(now.Add(-5 * time.Minute)),
		PackagingRetries:     1,
	}
	pc.processed["evt-processed"] = struct{}{}
	pc.mu.Unlock()

	if !pc.isStatePersistenceEnabled(ctx) {
		t.Fatal("expected state persistence tables to be detected as enabled")
	}
	pc.persistRuntimeState(ctx)
	var persistedScans, persistedPending, persistedValidations int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators`).Scan(&persistedScans)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates`).Scan(&persistedPending)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM validation_pipelines`).Scan(&persistedValidations)
	if persistedValidations == 0 {
		t.Fatalf("expected persisted validation state rows, got scans=%d pending=%d validations=%d", persistedScans, persistedPending, persistedValidations)
	}

	pcLoaded := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pcLoaded)
	pcLoaded.ensureStateLoaded(ctx)

	_ = len(pcLoaded.SnapshotScans())
	_ = pcLoaded.pendingDedupCountForScan(scanID)
	loaded := pcLoaded.validationContext(verticalID)
	if loaded.SpecVersion != 3 {
		t.Fatalf("unexpected loaded validation context: %+v", loaded)
	}
	if ok := pcLoaded.markEventProcessed(ctx, "evt-processed"); !ok {
		t.Fatal("expected markEventProcessed to accept new event after fresh load")
	}

	pcLoaded.clearPersistentState(ctx)
	var scansCount, pendingCount, validationsCount, processedCount int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators`).Scan(&scansCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates`).Scan(&pendingCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM validation_pipelines`).Scan(&validationsCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_processed_events`).Scan(&processedCount)
	if scansCount != 0 || pendingCount != 0 || validationsCount != 0 || processedCount != 0 {
		t.Fatalf("expected persistent state cleared, got scans=%d pending=%d validations=%d processed=%d", scansCount, pendingCount, validationsCount, processedCount)
	}
}

func TestFactoryPipelineCoordinator_CheckPackagingTimeoutsRetryAndPark(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pc)

	verticalRetry := uuid.NewString()
	verticalPark := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'Retry Vertical', 'retry-vertical', 'argentina', 'researching', 'factory', now(), now()),
			($2::uuid, 'Park Vertical', 'park-vertical', 'argentina', 'researching', 'factory', now(), now())
	`, verticalRetry, verticalPark); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}

	old := time.Now().UTC().Add(-(packagingTimeout + 2*time.Minute))
	pc.mu.Lock()
	pc.validations[verticalRetry] = &validationPipelineState{
		VerticalID:           verticalRetry,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                true,
		G4Brand:              true,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "ok"}),
		BrandPayload:         mustJSON(map[string]any{"names": []string{"r"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 82}),
		SpecVersion:          1,
		PackagingRequested:   true,
		PackagingRequestedAt: ptrTime(old),
		PackagingRetries:     0,
	}
	pc.validations[verticalPark] = &validationPipelineState{
		VerticalID:           verticalPark,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                true,
		G4Brand:              true,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "ok"}),
		BrandPayload:         mustJSON(map[string]any{"names": []string{"p"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 76}),
		SpecVersion:          1,
		PackagingRequested:   true,
		PackagingRequestedAt: ptrTime(old),
		PackagingRetries:     1,
	}
	pc.mu.Unlock()

	ch := bus.Subscribe("watch-packaging-timeout", events.EventType("validation.package_ready"))
	pc.checkPackagingTimeouts(ctx, time.Now().UTC())

	select {
	case evt := <-ch:
		if strings.TrimSpace(evt.VerticalID) != verticalRetry {
			t.Fatalf("expected retry event for %s, got vertical=%s", verticalRetry, evt.VerticalID)
		}
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("decode validation.package_ready payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["vertical_id"])) == "" {
			t.Fatalf("expected vertical_id in retry payload: %+v", payload)
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("expected validation.package_ready retry event")
	}

	pc.mu.Lock()
	retryState := pc.validations[verticalRetry]
	parkState := pc.validations[verticalPark]
	pc.mu.Unlock()
	if retryState == nil || retryState.PackagingRetries != 1 || retryState.Status != "active" {
		t.Fatalf("unexpected retry state after timeout handling: %+v", retryState)
	}
	if parkState == nil || parkState.Status != "parked" || parkState.PackagingRequested {
		t.Fatalf("expected parked validation after retry exhaustion: %+v", parkState)
	}

	var mailboxCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM mailbox
		WHERE vertical_id = $1::uuid AND from_agent = 'pipeline-coordinator' AND type = 'vertical_approval'
	`, verticalPark).Scan(&mailboxCount); err != nil {
		t.Fatalf("count mailbox park escalation: %v", err)
	}
	if mailboxCount < 1 {
		t.Fatal("expected mailbox escalation row after packaging timeout exhaustion")
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func TestAgentRuntimeHelpers_InferAndBudgetMapping(t *testing.T) {
	if got := inferDiscoveryMode("local services in argentina"); got != "local_services" {
		t.Fatalf("expected local_services, got %q", got)
	}
	if got := inferDiscoveryMode("follow saas_trend signals"); got != "saas_trend" {
		t.Fatalf("expected saas_trend, got %q", got)
	}
	if got := inferDiscoveryMode("generic directive"); got != "saas_gap" {
		t.Fatalf("expected default saas_gap, got %q", got)
	}

	if got := inferGeographyHint("SaaS in Paraguay"); got != "paraguay" {
		t.Fatalf("expected recognized geography paraguay, got %q", got)
	}
	if got := inferGeographyHint("Focus LATAM"); got != "Focus LATAM" {
		t.Fatalf("expected passthrough geography hint, got %q", got)
	}
	if got := inferGeographyHint(" "); got != "" {
		t.Fatalf("expected empty hint for blank input, got %q", got)
	}

	for state, evtType := range map[string]events.EventType{
		"warning":   events.EventType("budget.warning"),
		"throttle":  events.EventType("budget.throttle"),
		"emergency": events.EventType("budget.emergency"),
		"resumed":   events.EventType("budget.resumed"),
		"ok":        events.EventType("budget.resumed"),
	} {
		raw := mustJSON(map[string]any{"state": state})
		if got := budgetEventTypeFromThresholdPayload(raw); got != evtType {
			t.Fatalf("state %q expected %q, got %q", state, evtType, got)
		}
	}
	if got := budgetEventTypeFromThresholdPayload(mustJSON(map[string]any{"state": "unknown"})); got != "" {
		t.Fatalf("expected empty event type for unknown state, got %q", got)
	}

	if got := fieldStringFromJSON(mustJSON(map[string]any{"k": " v "}), "k"); got != "v" {
		t.Fatalf("expected trimmed field string, got %q", got)
	}
	if got := fieldStringFromJSON([]byte("{"), "k"); got != "" {
		t.Fatalf("expected empty for invalid json, got %q", got)
	}
}

func TestPipelineHelpers_NormalizationAndSimilarity(t *testing.T) {
	if got := normalizeName("  Dental-Clinic  Scheduling!! "); got != "dental clinic scheduling" {
		t.Fatalf("normalizeName mismatch: %q", got)
	}
	if slug := buildVerticalSlug("Dental Clinic Scheduling", "1234567890abcdef"); slug != "dental-clinic-scheduling-12345678" {
		t.Fatalf("unexpected slug %q", slug)
	}
	best, score := fuzzyBestMatch("Dental Clinic Scheduling SaaS", []verticalCandidate{
		{ID: "v1", Name: "Dental Clinic Scheduling"},
		{ID: "v2", Name: "Restaurant Ordering"},
	})
	if best.ID != "v1" || score <= 0.7 {
		t.Fatalf("expected v1 fuzzy match above threshold, got best=%+v score=%.2f", best, score)
	}
	if got := jaccard(tokenSet("a b"), tokenSet("b c")); got <= 0 || got >= 1 {
		t.Fatalf("expected partial overlap jaccard in (0,1), got %.2f", got)
	}
	merged := parsePayloadMap(mergeRawPayload(mustJSON(map[string]any{"a": 1, "b": 1}), mustJSON(map[string]any{"b": 2, "c": 3})))
	if asInt(merged["a"]) != 1 || asInt(merged["b"]) != 2 || asInt(merged["c"]) != 3 {
		t.Fatalf("unexpected merged payload: %+v", merged)
	}
}

func TestPipelineHelpers_DeriveDiscoveryCandidateName(t *testing.T) {
	name := deriveDiscoveryCandidateName(map[string]any{
		"trend_category":         "instant_payments",
		"trend_description":      "Paraguay's instant payment system is experiencing explosive growth with regulatory mandates and interoperability standards.",
		"opportunity_hypothesis": "Build a complete all-in-one orchestration layer for instant payment operators.",
	})
	if name != "Instant Payments" {
		t.Fatalf("expected concise taxonomy-derived name, got %q", name)
	}

	if got := deriveDiscoveryCandidateName(map[string]any{
		"opportunity_hypothesis": strings.Repeat("very long narrative hypothesis ", 8),
	}); got != "" {
		t.Fatalf("expected long narrative-only payload to be rejected, got %q", got)
	}
}

func TestPipelineHelpers_BuildVerticalSlugCapsLongBase(t *testing.T) {
	slug := buildVerticalSlug(strings.Repeat("instant-payment-growth-opportunity-", 6), "abcdef1234567890")
	if !strings.HasSuffix(slug, "-abcdef12") {
		t.Fatalf("expected stable id suffix, got %q", slug)
	}
	if len(slug) > maxVerticalSlugLen+1+8 {
		t.Fatalf("expected slug length cap <= %d, got %d (%q)", maxVerticalSlugLen+1+8, len(slug), slug)
	}
}

func TestDBTxContextWrappers_UseTransactionFromContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS tx_probe (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txCtx := withSQLTxContext(ctx, tx)
	if _, err := dbExecContext(txCtx, db, `INSERT INTO tx_probe (id) VALUES (1)`); err != nil {
		t.Fatalf("insert in tx context: %v", err)
	}

	var inTxCount int
	if err := dbQueryRowContext(txCtx, db, `SELECT count(*) FROM tx_probe`).Scan(&inTxCount); err != nil {
		t.Fatalf("count in tx: %v", err)
	}
	if inTxCount != 1 {
		t.Fatalf("expected in-tx count=1, got %d", inTxCount)
	}

	rows, err := dbQueryContext(txCtx, db, `SELECT id FROM tx_probe`)
	if err != nil {
		t.Fatalf("query rows in tx: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row in tx query")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var postRollbackCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tx_probe`).Scan(&postRollbackCount); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if postRollbackCount != 0 {
		t.Fatalf("expected rollback to clear insert, got count=%d", postRollbackCount)
	}
}

func TestScanCampaignManager_EmitCampaignCompletedIfDone(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(bus, store, db)

	geoID := uuid.NewString()
	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'Argentina', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, mode, priority, status, discoveries, directive_id, strategic_context, created_at, started_at, completed_at
		) VALUES (
			$1::uuid, $2::uuid, 'saas_gap', 'high', 'completed', 2, NULL, '{"directive_text":"SaaS in Argentina"}'::jsonb,
			now() - interval '2 hour', now() - interval '90 minute', now() - interval '80 minute'
		)
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed campaign: %v", err)
	}

	ch := bus.Subscribe("watch-campaign-completed", events.EventType("campaign.completed"))
	emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 3, "evt-source-1")
	if !emitted {
		t.Fatal("expected campaign.completed emission")
	}

	select {
	case evt := <-ch:
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("decode campaign payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["campaign_id"])) != campaignID {
			t.Fatalf("unexpected campaign_id in payload: %+v", payload)
		}
		if strings.TrimSpace(asString(payload["priority"])) != "high" {
			t.Fatalf("expected priority=high in payload: %+v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected campaign.completed event")
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, discoveries, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_trend', 'normal', 'queued', 0, now())
	`, uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed follow-on campaign: %v", err)
	}
	if emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 1, "evt-source-2"); emitted {
		t.Fatal("expected no campaign.completed while additional campaigns remain")
	}
}

func TestScanCampaignManager_BackpressurePauseResumeAndHelpers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(NewEventBus(InMemoryEventStore{}), store, db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	for i := 0; i < 6; i++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'review', now())
		`, uuid.NewString(), verticalID); err != nil {
			t.Fatalf("seed mailbox item %d: %v", i, err)
		}
	}

	if pending, err := manager.pendingMailboxCount(ctx); err != nil || pending != 6 {
		t.Fatalf("expected pending mailbox count=6, got pending=%d err=%v", pending, err)
	}
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to pause for mailbox backpressure")
	}
	if store.pauseCalls == 0 {
		t.Fatal("expected PauseQueuedScanCampaigns to be called")
	}

	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET status = 'approved' WHERE status = 'pending'`); err != nil {
		t.Fatalf("clear mailbox pending status: %v", err)
	}
	if manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to resume after backpressure clears")
	}
	if store.resumeCalls == 0 {
		t.Fatal("expected ResumePausedScanCampaigns to be called")
	}

	manager.budgetPaused = true
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("budget pause should short-circuit as paused")
	}
	manager.resetFlags()
	if manager.budgetPaused || manager.backpressurePaused {
		t.Fatalf("resetFlags should clear pause flags: budget=%v backpressure=%v", manager.budgetPaused, manager.backpressurePaused)
	}

	if got := sanitizeGeographyPhrase("argentina with complex filters and extras"); got != "Argentina" {
		t.Fatalf("unexpected sanitizeGeographyPhrase result: %q", got)
	}
}

func TestParseWeekday_AllCases(t *testing.T) {
	cases := map[string]time.Weekday{
		"sunday":    time.Sunday,
		"monday":    time.Monday,
		"tuesday":   time.Tuesday,
		"wednesday": time.Wednesday,
		"thursday":  time.Thursday,
		"friday":    time.Friday,
		"saturday":  time.Saturday,
		"  FrIdAy ": time.Friday,
		"nope":      time.Monday,
	}
	for in, want := range cases {
		if got := parseWeekday(in); got != want {
			t.Fatalf("parseWeekday(%q)=%v want %v", in, got, want)
		}
	}
}

type scheduleStoreStub struct{ upserts int }

func (s *scheduleStoreStub) UpsertSchedule(context.Context, Schedule) error { s.upserts++; return nil }

func (s *scheduleStoreStub) CancelSchedule(context.Context, string, string) error { return nil }

func (s *scheduleStoreStub) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *scheduleStoreStub) MarkScheduleFired(context.Context, Schedule) error { return nil }

type mailboxStoreCapture struct {
	last MailboxItem
}

func (m *mailboxStoreCapture) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if item.ID == "" {
		item.ID = uuid.NewString()
	}
	return item.ID, nil
}

func (m *mailboxStoreCapture) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }

func (m *mailboxStoreCapture) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}

func (m *mailboxStoreCapture) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) MarkMailboxItemNotified(context.Context, string) error { return nil }

func (m *mailboxStoreCapture) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func TestRuntimeToolExecutor_EndToEndActions_AgentMessage_Schedule_Routing_Hire_Fire_Reconfigure_Mailbox_HumanTasks(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	storeStub := &managerStoreStub{}
	factory := func(cfg models.AgentConfig) (Agent, error) {
		return &stubAgent{id: cfg.ID, typ: cfg.Type, subs: []events.EventType{"*"}}, nil
	}
	manager := NewAgentManager(bus, factory, storeStub)

	schedStore := &scheduleStoreStub{}
	scheduler := NewScheduler(func(sc Schedule) {})
	defer scheduler.Stop()

	mailboxCap := &mailboxStoreCapture{}
	exec := NewRuntimeToolExecutor(bus, scheduler, manager, schedStore)
	exec.SetSQLDB(db)
	exec.SetMailboxStore(mailboxCap)
	exec.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
				CategoriesEnabled: []string{
					"verification",
				},
			},
		},
	})

	targetID := "backend-agent-" + verticalID
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:         targetID,
		Type:       "stub",
		Role:       "backend-agent",
		Mode:       "operating",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["*"],"subscriptions":["*"]}`),
	}); err != nil {
		t.Fatalf("spawn target: %v", err)
	}
	targetCh := bus.Subscribe(targetID, "agent.message")

	actor := models.AgentConfig{
		ID:         "opco-ceo-" + verticalID,
		Type:       "stub",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["agent_message","schedule","configure_routing","agent_hire","agent_fire","agent_reconfigure","mailbox_send","human_task_request","human_task_decide"]}`),
	}
	if err := manager.SpawnAgent(actor); err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_message", map[string]any{
		"target_agent_id": targetID,
		"message":         "hi",
	}); err != nil {
		t.Fatalf("agent_message: %v", err)
	}
	select {
	case <-targetCh:
	case <-time.After(1 * time.Second):
		t.Fatal("expected message delivered to target")
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "schedule", map[string]any{
		"event_type": "timer.test",
		"mode":       "once",
		"at":         time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339),
		"payload":    map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if schedStore.upserts == 0 {
		t.Fatal("expected scheduleStore upsert")
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "configure_routing", map[string]any{
		"event_pattern": "opco.*",
		"subscriber_id": targetID,
		"reason":        "test",
		"status":        "active",
	}); err != nil {
		t.Fatalf("configure_routing: %v", err)
	}

	hiredID := "qa-agent-" + verticalID
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_hire", map[string]any{
		"config": map[string]any{
			"id":          hiredID,
			"type":        "stub",
			"role":        "qa-agent",
			"mode":        "operating",
			"vertical_id": verticalID,
			"config":      json.RawMessage(`{"system_prompt":"x","tools":["*"]}`),
		},
	}); err != nil {
		t.Fatalf("agent_hire: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_reconfigure", map[string]any{
		"agent_id": hiredID,
		"config": map[string]any{
			"id":          hiredID,
			"type":        "stub",
			"role":        "qa-agent",
			"mode":        "operating",
			"vertical_id": verticalID,
			"config":      json.RawMessage(`{"system_prompt":"updated"}`),
		},
	}); err != nil {
		t.Fatalf("agent_reconfigure: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_fire", map[string]any{"agent_id": hiredID}); err != nil {
		t.Fatalf("agent_fire: %v", err)
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "mailbox_send", map[string]any{
		"type":     "review",
		"priority": "critical",
		"summary":  "please review",
		"context":  map[string]any{"a": 1},
	}); err != nil {
		t.Fatalf("mailbox_send: %v", err)
	}
	if mailboxCap.last.Type != "review" || mailboxCap.last.Priority != "critical" {
		t.Fatalf("unexpected mailbox item: %+v", mailboxCap.last)
	}

	out, err := exec.Execute(WithActor(ctx, actor), "human_task_request", map[string]any{
		"category":    "verification",
		"description": "call someone",
		"priority":    "medium",
		"deadline":    time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
		"talking_points": []string{
			"x",
		},
	})
	if err != nil {
		t.Fatalf("human_task_request: %v", err)
	}
	taskID, _ := out.(map[string]any)["task_id"].(string)
	if taskID == "" {
		t.Fatalf("expected task_id in output, got %v", out)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, reviewed_at, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'verification', 'x', 'approved', now(), now())
	`, uuid.NewString(), actor.ID, verticalID); err != nil {
		t.Fatalf("seed approved task: %v", err)
	}

	coordinator := models.AgentConfig{
		ID:     "empire-coordinator",
		Type:   "stub",
		Role:   "empire-coordinator",
		Mode:   "holding",
		Config: json.RawMessage(`{"system_prompt":"x","tools":["human_task_decide"]}`),
	}
	if err := manager.SpawnAgent(coordinator); err != nil {
		t.Fatalf("spawn coordinator: %v", err)
	}
	decOut, err := exec.Execute(WithActor(ctx, coordinator), "human_task_decide", map[string]any{
		"task_id":      taskID,
		"decision":     "approve",
		"reason":       "ok",
		"requeue_date": time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("human_task_decide: %v", err)
	}
	if status, _ := decOut.(map[string]any)["status"].(string); status == "approved" {
		t.Fatalf("expected budget enforcement to defer, got %v", decOut)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()
	t.Setenv("EMPIREAI_EXTERNAL_PROXY_BASE_URL", ts.URL)
	_, _ = exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{"domain": "example.com"})
}

func TestAuthorizeRouting_AllBranches(t *testing.T) {
	targetProduct := models.AgentConfig{Role: "backend-agent"}
	targetGrowth := models.AgentConfig{Role: "marketing-agent"}
	targetEng := models.AgentConfig{Role: "qa-agent"}
	targetBad := models.AgentConfig{Role: "random"}

	if err := authorizeRouting(models.AgentConfig{Role: "opco-ceo"}, targetBad, "active"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "empire-coordinator"}, targetBad, "active"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}

	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "active"); err == nil {
		t.Fatalf("chief-of-staff should reject non-proposed")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "proposed"); err != nil {
		t.Fatalf("chief-of-staff proposed should allow: %v", err)
	}

	if err := authorizeRouting(models.AgentConfig{Role: "vp-product"}, targetProduct, "active"); err != nil {
		t.Fatalf("vp-product product target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-product"}, targetGrowth, "active"); err == nil {
		t.Fatalf("vp-product should reject growth target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, targetGrowth, "active"); err != nil {
		t.Fatalf("vp-growth growth target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, targetBad, "active"); err == nil {
		t.Fatalf("vp-growth should reject unknown target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, targetEng, "active"); err != nil {
		t.Fatalf("cto-agent eng target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, targetGrowth, "active"); err == nil {
		t.Fatalf("cto-agent should reject growth target")
	}

	if err := authorizeRouting(models.AgentConfig{Role: "support-agent"}, targetBad, "active"); err == nil {
		t.Fatalf("expected unauthorized routing role error")
	}
}

func TestAuthorizeManage_AllBranches(t *testing.T) {

	if err := authorizeManage(models.AgentConfig{Role: "empire-coordinator"}, "anything", "v"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}

	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical restriction")
	}

	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v1"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}

	if err := authorizeManage(models.AgentConfig{Role: "vp-product"}, "backend-agent", ""); err != nil {
		t.Fatalf("vp-product should manage product agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-product"}, "marketing-agent", ""); err == nil {
		t.Fatalf("vp-product should reject growth agent")
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-growth"}, "marketing-agent", ""); err != nil {
		t.Fatalf("vp-growth should manage growth agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-growth"}, "backend-agent", ""); err == nil {
		t.Fatalf("vp-growth should reject product agent")
	}
	if err := authorizeManage(models.AgentConfig{Role: "cto-agent"}, "qa-agent", ""); err != nil {
		t.Fatalf("cto-agent should manage eng agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "cto-agent"}, "marketing-agent", ""); err == nil {
		t.Fatalf("cto-agent should reject non-eng")
	}

	if err := authorizeManage(models.AgentConfig{Role: "support-agent"}, "support-agent", ""); err == nil {
		t.Fatalf("expected unauthorized manage role")
	}
}

func TestDecryptCredentialValue_AndLoadVerticalCredentials_Branches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex.SetSQLDB(db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalID, `{"api_key":"enc::"}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	if got := ex.decryptCredentialValue(ctx, "enc::AAAA"); got != "enc::AAAA" {
		t.Fatalf("expected passthrough without key, got %#v", got)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.decryptCredentialValue(ctx, "enc::"); got != "" {
		t.Fatalf("expected empty encoded to return empty string, got %#v", got)
	}

	if got := ex.decryptCredentialValue(ctx, "enc::not-base64"); got != "enc::not-base64" {
		t.Fatalf("expected passthrough on decrypt error, got %#v", got)
	}

	// Valid encrypted value round-trip.
	var enc string
	if err := db.QueryRowContext(ctx, `SELECT encode(pgp_sym_encrypt('secret', 'k'), 'base64')`).Scan(&enc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if got := ex.decryptCredentialValue(ctx, "enc::"+enc); got != "secret" {
		t.Fatalf("expected decrypted secret, got %#v", got)
	}

	ex2 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex2.loadVerticalCredentials(ctx, verticalID); err == nil {
		t.Fatalf("expected sql db not configured")
	}
	ex2.SetSQLDB(db)
	if _, err := ex2.loadVerticalCredentials(ctx, ""); err == nil {
		t.Fatalf("expected vertical_id required")
	}
	verticalBad := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vb', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalBad, "\"x\""); err != nil {
		t.Fatalf("seed bad creds: %v", err)
	}
	if _, err := ex2.loadVerticalCredentials(ctx, verticalBad); err == nil {
		t.Fatalf("expected decode vertical credentials error")
	}
}

func TestRuntimeToolExecutor_DecryptCredentialValue_DBNilBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ctx := context.Background()

	if got := ex.decryptCredentialValue(ctx, "plain"); got.(string) != "plain" {
		t.Fatalf("expected pass-through, got %#v", got)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.decryptCredentialValue(ctx, "enc::AAAA"); got.(string) != "enc::AAAA" {
		t.Fatalf("expected encrypted passthrough when db missing, got %#v", got)
	}

	m := map[string]any{"a": "enc::AAAA", "b": []any{"enc::AAAA", "x"}, "c": 1}
	out := ex.decryptCredentialValue(ctx, m).(map[string]any)
	if out["a"].(string) != "enc::AAAA" {
		t.Fatalf("unexpected map decrypt: %#v", out)
	}
	if out["c"].(int) != 1 {
		t.Fatalf("unexpected passthrough: %#v", out)
	}
}

func TestToolExecutor_HelperFunctions_MoreBranches(t *testing.T) {

	txt := safeTelemetryText(map[string]any{
		"token": "super-secret",
		"fn":    func() {},
	})
	if !strings.Contains(txt, "[REDACTED]") {
		t.Fatalf("expected redaction in telemetry, got %q", txt)
	}
	largePayload := map[string]any{}
	for i := 0; i < 80; i++ {
		largePayload[fmt.Sprintf("k_%d", i)] = strings.Repeat("x", 90)
	}
	largeText := safeTelemetryText(largePayload)
	if len(largeText) <= 400 {
		t.Fatalf("expected telemetry truncation budget > 400 chars, got len=%d", len(largeText))
	}
	if len(largeText) > maxToolTelemetryChars+3 {
		t.Fatalf("expected telemetry capped at %d (+ellipsis), got len=%d", maxToolTelemetryChars, len(largeText))
	}

	if got := truncateTelemetry("abc", 0); got != "abc" {
		t.Fatalf("expected no-op when max<=0, got %q", got)
	}
	if got := truncateTelemetry("abc", 10); got != "abc" {
		t.Fatalf("expected no truncation, got %q", got)
	}
	if got := truncateTelemetry("abcdef", 3); got != "abc..." {
		t.Fatalf("expected truncation, got %q", got)
	}

	if asString(nil) != "" {
		t.Fatalf("expected empty for nil")
	}
	if asString(123) != "123" {
		t.Fatalf("expected fmt fallback, got %q", asString(123))
	}

	if defaultExternalMethod("domain_availability_check") != http.MethodGet {
		t.Fatalf("expected GET")
	}
	if defaultExternalMethod("domain_purchase") != http.MethodPost {
		t.Fatalf("expected POST")
	}

	req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
	applyExternalHeaders(req, map[string]any{
		" X ":  " y ",
		"":     "z",
		"noop": "",
	})
	if req.Header.Get("X") != "y" {
		t.Fatalf("expected header X=y, got %q", req.Header.Get("X"))
	}
	if req.Header.Get("noop") != "" {
		t.Fatalf("expected noop to be skipped")
	}

	req2, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req2, map[string]any{
		"api_key": "k1",
		"headers": map[string]any{"X-Extra": "v"},
	}, "dns_configure")
	if req2.Header.Get("X-Extra") != "v" {
		t.Fatalf("expected X-Extra set")
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer k1" {
		t.Fatalf("expected bearer auth, got %q", got)
	}

	req3, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req3, map[string]any{
		"auth_header": "X-Api-Key",
		"token":       "t1",
	}, "whatsapp_business_api")
	if got := req3.Header.Get("X-Api-Key"); got != "t1" {
		t.Fatalf("expected custom auth header, got %q", got)
	}

	req4, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req4, map[string]any{
		"token": "Bearer z",
	}, "instagram_api")
	if got := req4.Header.Get("Authorization"); got != "Bearer z" {
		t.Fatalf("expected preserved bearer token, got %q", got)
	}

	if got := parseExternalResponseBody(nil).(map[string]any); len(got) != 0 {
		t.Fatalf("expected empty map, got %#v", got)
	}
	if got := parseExternalResponseBody([]byte(`{"ok":true}`)).(map[string]any)["ok"]; got != true {
		t.Fatalf("expected parsed json, got %#v", got)
	}
	if got := parseExternalResponseBody([]byte(" hi ")).(string); got != "hi" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
}

type captureEventStore struct {
	events     []events.Event
	deliveries int
}

func (c *captureEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	c.events = append(c.events, evt)
	return nil
}

func (c *captureEventStore) InsertEventDeliveries(_ context.Context, _ string, agentIDs []string) error {
	c.deliveries += len(agentIDs)
	return nil
}

func TestExecHumanTaskDecide_ValidationErrors(t *testing.T) {
	ctx := context.Background()

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "sql db is not configured") {
		t.Fatalf("expected db error, got %v", err)
	}

	_, db, _ := testutil.StartPostgres(t)
	ex2 := NewRuntimeToolExecutor(nil, nil, nil)
	ex2.SetSQLDB(db)
	if _, err := ex2.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "event bus is not configured") {
		t.Fatalf("expected bus error, got %v", err)
	}

	ex3 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex3.SetSQLDB(db)
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "a", Role: "opco-ceo"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("expected auth error, got %v", err)
	}

	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "task_id is required") {
		t.Fatalf("expected task_id required, got %v", err)
	}
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t",
	}); err == nil || !strings.Contains(err.Error(), "decision is required") {
		t.Fatalf("expected decision required, got %v", err)
	}
	if _, err := ex3.execHumanTaskDecide(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "weird",
	}); err == nil || !strings.Contains(err.Error(), "unknown decision") {
		t.Fatalf("expected unknown decision, got %v", err)
	}
}

func TestExecHumanTaskDecide_ApproveRejectAndBudgetDeferral(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	store := &captureEventStore{}
	bus := NewEventBus(store)
	ex := NewRuntimeToolExecutor(bus, nil, nil)
	ex.SetSQLDB(db)
	ex.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
			},
		},
	})

	task1 := uuid.NewString()
	task2 := uuid.NewString()
	task3 := uuid.NewString()
	for _, id := range []string{task1, task2, task3} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
			VALUES ($1::uuid, 'requester-1', $2::uuid, 'ops', 'do it', 'pending_review', now())
		`, id, verticalID); err != nil {
			t.Fatalf("seed task %s: %v", id, err)
		}
	}

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"}

	out, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":       task1,
		"decision":      "approve",
		"reason":        "ok",
		"priority_rank": 2,
	})
	if err != nil {
		t.Fatalf("approve 1: %v", err)
	}
	if out.(map[string]any)["status"] != "approved" {
		t.Fatalf("unexpected approve out: %#v", out)
	}

	out2, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":  task2,
		"decision": "approved",
	})
	if err != nil {
		t.Fatalf("approve 2: %v", err)
	}
	if out2.(map[string]any)["status"] != "deferred" {
		t.Fatalf("expected deferred, got %#v", out2)
	}

	out3, err := ex.execHumanTaskDecide(ctx, actor, map[string]any{
		"task_id":      task3,
		"decision":     "reject",
		"reason":       "no",
		"requeue_date": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}
	if out3.(map[string]any)["status"] != "rejected" {
		t.Fatalf("expected rejected, got %#v", out3)
	}

	// Verify DB state updated.
	var s1, s2, s3 string
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task1).Scan(&s1)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task2).Scan(&s2)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task3).Scan(&s3)
	if s1 != "approved" || s2 != "deferred" || s3 != "rejected" {
		t.Fatalf("unexpected statuses: %q %q %q", s1, s2, s3)
	}

	types := map[string]events.Event{}
	for _, evt := range store.events {
		types[string(evt.Type)] = evt
	}
	if _, ok := types["human_task.approved"]; !ok {
		t.Fatalf("expected approved event, have=%v", store.events)
	}
	if evt, ok := types["human_task.deferred"]; !ok {
		t.Fatalf("expected deferred event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if strings.TrimSpace(asString(p["requeue_date"])) == "" {
			t.Fatalf("expected requeue_date in deferred payload, got %#v", p)
		}
		if !strings.Contains(strings.ToLower(asString(p["defer_reason"])), "budget") {
			t.Fatalf("expected defer reason to mention budget, got %#v", p)
		}
	}
	if evt, ok := types["human_task.rejected"]; !ok {
		t.Fatalf("expected rejected event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if asString(p["rejection_reason"]) != "no" {
			t.Fatalf("expected rejection_reason, got %#v", p)
		}
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func seedVertical(t *testing.T, db *sql.DB, slug string, credsJSON string) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', $2, 'us', 'discovered', 'factory', $3::jsonb, now(), now())
	`, id, slug, credsJSON); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	return id
}

func TestAuthorizeRoutingAndManage_Branches(t *testing.T) {
	target := models.AgentConfig{Role: "backend-agent"}

	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, target, "active"); err == nil {
		t.Fatalf("expected CoS to be blocked unless proposed")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, target, "proposed"); err != nil {
		t.Fatalf("expected CoS proposed ok: %v", err)
	}

	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, target, "active"); err == nil {
		t.Fatalf("expected vp-growth to be blocked for eng target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, target, "active"); err != nil {
		t.Fatalf("expected cto-agent ok for eng target: %v", err)
	}

	if err := authorizeManage(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "vp-growth", "v1"); err == nil {
		t.Fatalf("expected vp-product blocked for growth role")
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "backend-agent", "v1"); err != nil {
		t.Fatalf("expected vp-product can manage product roles (backend is allowed list): %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical block")
	}
	if err := authorizeManage(models.AgentConfig{Role: "empire-coordinator", VerticalID: "v1"}, "vp-product", "v2"); err != nil {
		t.Fatalf("coordinator bypass: %v", err)
	}
}

func TestDefaultExternalCredentialEnv_Branches(t *testing.T) {
	t.Setenv("REGISTRAR_API_ENDPOINT", "https://reg.example")
	t.Setenv("REGISTRAR_API_KEY", "rk")
	t.Setenv("CLOUDFLARE_API_ENDPOINT", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "cfk")
	t.Setenv("WHATSAPP_API_ENDPOINT", "https://wa.example")
	t.Setenv("WHATSAPP_API_KEY", "wak")
	t.Setenv("INSTAGRAM_API_ENDPOINT", "https://ig.example")
	t.Setenv("INSTAGRAM_API_KEY", "igk")

	if got := defaultExternalCredentialEnv("domain_purchase"); got["endpoint"] != "https://reg.example" || got["api_key"] != "rk" {
		t.Fatalf("domain creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("dns_configure"); !strings.Contains(got["endpoint"], "cloudflare.com") || got["api_key"] != "cfk" {
		t.Fatalf("dns creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("whatsapp_business_api"); got["endpoint"] != "https://wa.example" || got["api_key"] != "wak" {
		t.Fatalf("wa creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("instagram_api"); got["endpoint"] != "https://ig.example" || got["api_key"] != "igk" {
		t.Fatalf("ig creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("unknown"); len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}

func TestExecInstagramHandleCheck_Availability(t *testing.T) {
	orig := http.DefaultClient.Transport
	t.Cleanup(func() { http.DefaultClient.Transport = orig })

	http.DefaultClient.Transport = rtFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Host, "www.instagram.com") {
			return nil, errors.New("unexpected host")
		}
		if strings.Contains(r.URL.Path, "available_handle") {
			return resp(404, "not found"), nil
		}
		return resp(200, "ok"), nil
	})

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "@available_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m := out.(map[string]any)
	if m["available"] != true {
		t.Fatalf("expected available=true, got %#v", m)
	}

	out, err = ex.execInstagramHandleCheck(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "taken_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m = out.(map[string]any)
	if m["available"] != false {
		t.Fatalf("expected available=false, got %#v", m)
	}

	if _, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{}, map[string]any{"handle": ""}); err == nil {
		t.Fatalf("expected handle required error")
	}
	if _, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{}, map[string]any{"handle": "bad!!"}); err == nil {
		t.Fatalf("expected invalid format error")
	}
}

func TestExecEmailAPI_CredentialAndSendBranches(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn

	verticalID := seedVertical(t, db, "emailco", `{
		"email": {
			"smtp_addr": "127.0.0.1:1",
			"from": "noreply@example.com",
			"username": "u",
			"password": "p"
		}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	actor := models.AgentConfig{ID: "opco-ceo-" + verticalID, Role: "opco-ceo", VerticalID: verticalID}

	if _, err := ex.execEmailAPI(ctx, actor, map[string]any{"to": []string{}}); err == nil {
		t.Fatalf("expected recipient error")
	}

	if _, err := ex.execEmailAPI(ctx, actor, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected send failure")
	}

	vertical2 := seedVertical(t, db, "emailco2", `{"email":{}}`)
	actor2 := models.AgentConfig{ID: "a2", Role: "opco-ceo", VerticalID: vertical2}
	if _, err := ex.execEmailAPI(ctx, actor2, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected missing credential error")
	}
}

func TestRestrictedDevOpsTools_RejectNonHoldingDevOps(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo"}

	if _, err := ex.execNginxReload(ctx, actor, nil); err == nil {
		t.Fatalf("expected nginx restriction error")
	}
	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "restart", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected systemd restriction error")
	}
	if _, err := ex.execCertbotExecute(ctx, actor, map[string]any{"domain": "example.com"}); err == nil {
		t.Fatalf("expected certbot restriction error")
	}
}

func TestDecryptCredentialValue_NoKeyLeavesEncrypted(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	in := map[string]any{"token": "enc::abc"}
	out := ex.decryptCredentialMap(context.Background(), in)
	if out["token"].(string) != "enc::abc" {
		t.Fatalf("expected encrypted value to remain when key missing")
	}

	_ = os.ErrInvalid
}

func TestExecSystemdControl_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}

	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "bogus", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected unsupported action error")
	}

	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "restart", "unit": "nginx"}); err == nil {
		t.Fatalf("expected unit prefix error")
	}
}

func TestExecCertbotExecute_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}
	if _, err := ex.execCertbotExecute(ctx, actor, map[string]any{"domain": ""}); err == nil {
		t.Fatalf("expected domain required error")
	}
}

func TestRedactTelemetryValue_NestedAndSensitive(t *testing.T) {
	in := map[string]any{
		"token":    "secret-token-value",
		"password": "pw",
		"notes":    "payment confirmed ch_abcdef123456",
		"meta": map[string]any{
			"Authorization": "Bearer X",
			"count":         2,
			"items":         []any{"a", map[string]any{"api_key": "k"}},
		},
	}
	out := redactTelemetryValue(in).(map[string]any)
	if out["token"] != "[REDACTED]" || out["password"] != "[REDACTED]" {
		t.Fatalf("expected sensitive keys redacted: %#v", out)
	}
	meta := out["meta"].(map[string]any)
	if meta["Authorization"] != "[REDACTED]" {
		t.Fatalf("expected nested auth redacted: %#v", meta)
	}
	if strings.Contains(asString(out["notes"]), "ch_abcdef123456") || !strings.Contains(asString(out["notes"]), "[PAYMENT_REF]") {
		t.Fatalf("expected payment ref redacted, got %#v", out["notes"])
	}
}

func TestLoadExternalCredentials_MergesSections(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	verticalID := seedVertical(t, db, "credco", `{
		"whatsapp": {"endpoint":"w","token":"t"},
		"instagram": {"endpoint":"i","api_key":"k"},
		"registrar": {"endpoint":"r","api_key":"rk"},
		"dns": {"endpoint":"d","api_key":"dk"},
		"whatsapp_name_check": {"endpoint":"n","api_key":"nk"}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", VerticalID: verticalID}

	check := func(tool string, wantKey string) {
		creds, err := ex.loadExternalCredentials(ctx, actor.VerticalID, tool)
		if err != nil {
			t.Fatalf("loadExternalCredentials %s: %v", tool, err)
		}
		if strings.TrimSpace(asString(creds[wantKey])) == "" {
			t.Fatalf("expected %s in creds for %s: %#v", wantKey, tool, creds)
		}
	}
	check("whatsapp_business_api", "endpoint")
	check("instagram_api", "api_key")
	check("domain_availability_check", "api_key")
	check("dns_configure", "endpoint")
	check("whatsapp_name_check", "api_key")
}

func TestDecryptCredentialValue_Success(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")

	var encoded string
	if err := db.QueryRowContext(context.Background(), `
		SELECT encode(pgp_sym_encrypt('plain', 'k'), 'base64')
	`).Scan(&encoded); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := ex.decryptCredentialValue(context.Background(), "enc::"+strings.TrimSpace(encoded))
	if got.(string) != "plain" {
		t.Fatalf("expected decrypted plain, got %#v", got)
	}
}

type mailboxStub struct {
	last MailboxItem
}

func (m *mailboxStub) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if strings.TrimSpace(item.ID) == "" {
		return "mb1", nil
	}
	return item.ID, nil
}

func (m *mailboxStub) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }

func (m *mailboxStub) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}

func (m *mailboxStub) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func (m *mailboxStub) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) MarkMailboxItemNotified(context.Context, string) error { return nil }

type scheduleStoreStub2 struct{ upsert int }

func (s *scheduleStoreStub2) UpsertSchedule(context.Context, Schedule) error { s.upsert++; return nil }

func (s *scheduleStoreStub2) CancelSchedule(context.Context, string, string) error {
	return nil
}

func (s *scheduleStoreStub2) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *scheduleStoreStub2) MarkScheduleFired(context.Context, Schedule) error { return nil }

func TestToolExecutor_AgentHireFireReconfigure_And_ScheduleAndMailbox(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	store := &scheduleStoreStub2{}
	ex := NewRuntimeToolExecutor(bus, scheduler, nil, store)
	mb := &mailboxStub{}
	ex.SetMailboxStore(mb)

	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}

	out, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "a1", "role": "vp-product"}})
	if err != nil {
		t.Fatalf("hire: %v", err)
	}
	if out.(map[string]any)["status"] != "hired" {
		t.Fatalf("unexpected hire out: %#v", out)
	}

	out, err = ex.execAgentReconfigure(actor, map[string]any{"agent_id": "a1", "config": map[string]any{"mode": "holding"}})
	if err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if out.(map[string]any)["status"] != "reconfigured" {
		t.Fatalf("unexpected reconfigure out: %#v", out)
	}

	out, err = ex.execAgentFire(actor, map[string]any{"agent_id": "a1"})
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if out.(map[string]any)["status"] != "fired" {
		t.Fatalf("unexpected fire out: %#v", out)
	}

	if _, err := ex.execSchedule(actor, map[string]any{"event_type": "timer.x", "at": "bad"}); err == nil {
		t.Fatalf("expected invalid at error")
	}
	if _, err := ex.execSchedule(actor, map[string]any{"agent_id": "other", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected schedule for self only error")
	}
	if _, err := ex.execSchedule(models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected cross-vertical schedule error")
	}
	if _, err := ex.execSchedule(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}, map[string]any{"event_type": "timer.x", "mode": "cron", "cron": "@every 1h"}); err != nil {
		t.Fatalf("schedule ok: %v", err)
	}
	if store.upsert == 0 {
		t.Fatalf("expected schedule store upsert")
	}

	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "qa-agent", VerticalID: "v1"}, map[string]any{"type": "review"}); err == nil {
		t.Fatalf("expected mailbox auth error")
	}
	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"priority": "normal"}); err == nil {
		t.Fatalf("expected mailbox type required")
	}
	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "type": "review"}); err == nil {
		t.Fatalf("expected cross-vertical mailbox error")
	}
	out, err = ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"type": "review", "context": map[string]any{"token": "secret"}})
	if err != nil {
		t.Fatalf("mailbox send: %v", err)
	}
	if out.(map[string]any)["status"] != "queued" {
		t.Fatalf("unexpected mailbox out: %#v", out)
	}
	if mb.last.Type != "review" || mb.last.Status != "pending" {
		t.Fatalf("unexpected mailbox item: %#v", mb.last)
	}
}

func TestDecodeToolInput_ErrorBranch(t *testing.T) {
	var out struct{}
	if err := decodeToolInput(func() {}, &out); err == nil {
		t.Fatalf("expected marshal error for func")
	}
}

func TestNormalizeSQLValue_Bytes(t *testing.T) {
	if got := normalizeSQLValue([]byte("x")); got.(string) != "x" {
		t.Fatalf("expected string from bytes, got %#v", got)
	}
	if got := normalizeSQLValue(123); got.(int) != 123 {
		t.Fatalf("expected passthrough, got %#v", got)
	}
}

func TestExecSQLExecute_ReadOnly(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := seedVertical(t, db, "acme", `{}`)

	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "acme_schema"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "acme_schema".t (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: verticalID}

	if _, err := ex.execSQLExecute(ctx, actor, map[string]any{"query": `INSERT INTO t (id,v) VALUES (1,'x')`}); err == nil {
		t.Fatalf("expected insert rejection")
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO "acme_schema".t (id, v) VALUES (1, 'x')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	out, err := ex.execSQLExecute(ctx, actor, map[string]any{"query": `SELECT id, v FROM t ORDER BY id`})
	if err != nil {
		t.Fatalf("execSQLExecute select: %v", err)
	}
	rows := out.(map[string]any)["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["v"] != "x" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecAgentMessage_TargetValidationBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ex := NewRuntimeToolExecutor(bus, NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t2", Role: "vp-product", Mode: "operating", VerticalID: "v2"})
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}
	ctx := context.Background()

	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"message": "hi"}); err == nil {
		t.Fatalf("expected missing target error")
	}

	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "nope", "message": "hi"}); err == nil {
		t.Fatalf("expected unknown target error")
	}

	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "t2", "message": "hi"}); err == nil {
		t.Fatalf("expected cross-vertical error")
	}

	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_ids": []string{"t1", "t1"}, "message": "hi"}); err != nil {
		t.Fatalf("expected ok: %v", err)
	}
}

func TestExecAgentMessage_AuthorityAndManagementChain(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ex := NewRuntimeToolExecutor(bus, NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-growth-v1", Role: "vp-growth", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "cto-v1", Role: "cto-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "vp-product-v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "cto-v1"})
	ex.SetManager(manager)

	ctx := context.Background()

	actor := models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "vp-growth-v1", "message": "sync"}); err == nil {
		t.Fatalf("expected authority rejection for vp-product -> vp-growth")
	}

	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "backend-v1", "message": "prioritize bug"}); err != nil {
		t.Fatalf("expected management-chain authorization, got: %v", err)
	}

	worker := models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.execAgentMessage(ctx, worker, map[string]any{"target_agent_id": "vp-product-v1", "message": "blocked on product decision"}); err != nil {
		t.Fatalf("expected upward escalation authorization, got: %v", err)
	}
}

func TestAuthorizeToolUsage_AllowedToolsConfig(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })
	actor := models.AgentConfig{
		ID:   "a",
		Role: "worker",
		Config: []byte(`{
			"allowed_tools": ["agent_message"]
		}`),
	}
	ctx := WithActor(context.Background(), actor)

	if _, err := ex.Execute(ctx, "agent_message", map[string]any{"target_agent_id": "x"}); err == nil {

	}

	if _, err := ex.Execute(ctx, "schedule", map[string]any{}); err == nil {
		t.Fatalf("expected tool not allowed error")
	}
}

func TestAuthorizeMailboxSend_RoleCoverage(t *testing.T) {
	for _, role := range []string{
		"validation-coordinator",
		"vp-growth",
		"support-agent",
		"marketing-agent",
	} {
		if err := authorizeMailboxSend(models.AgentConfig{Role: role}); err != nil {
			t.Fatalf("expected role %s to be allowed mailbox_send: %v", role, err)
		}
	}
}

func TestRuntimeToolExecutor_SetManager_Instagram_Email_SystemTools(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	exec.SetSQLDB(db)
	exec.SetManager(manager)

	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "vp-growth",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["instagram_handle_check"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "instagram_handle_check", map[string]any{"handle": "bad!!"})
		if err == nil {
			t.Fatal("expected instagram handle format error")
		}
	}

	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "opco-ceo",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["email_api"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "email_api", map[string]any{
			"to":      []string{"a@example.com"},
			"subject": "s",
			"body":    "b",
		})
		if err == nil {
			t.Fatal("expected missing email credentials error")
		}
	}

	{
		actor := models.AgentConfig{
			ID:     "holding-devops",
			Role:   "holding-devops",
			Mode:   "holding",
			Config: json.RawMessage(`{"system_prompt":"x","tools":["nginx_reload","systemd_control","certbot_execute"]}`),
		}

		toolCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "nginx_reload", map[string]any{}); err == nil {
			t.Fatal("expected nginx_reload to fail in test environment")
		}
		cancel()

		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "systemd_control", map[string]any{"action": "nope", "unit": "empireai-x"}); err == nil {
			t.Fatal("expected systemd_control to reject invalid action")
		}
		cancel()

		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "certbot_execute", map[string]any{"domain": ""}); err == nil {
			t.Fatal("expected certbot_execute to require domain")
		}
		cancel()
	}
}

func TestToolExecutor_AuthorizationHelpers(t *testing.T) {
	actor := models.AgentConfig{Role: "vp-product", VerticalID: "v1"}
	target := models.AgentConfig{Role: "marketing-agent", VerticalID: "v1"}
	if err := authorizeManage(actor, target.Role, target.VerticalID); err == nil {
		t.Fatal("expected vp-product to be blocked from managing growth agents")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, models.AgentConfig{Role: "vp-growth"}, "active"); err == nil {
		t.Fatal("expected chief-of-staff to only propose routing")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, models.AgentConfig{Role: "backend-agent"}, "active"); err != nil {
		t.Fatalf("expected cto-agent to route eng agents, got %v", err)
	}
}

func TestToolExecutor_AgentManagement_AndMailbox_ErrorBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	ex := NewRuntimeToolExecutor(bus, scheduler, nil)
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}

	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "x", "role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.execAgentFire(actor, map[string]any{"agent_id": "x"}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.execAgentReconfigure(actor, map[string]any{"agent_id": "x", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}

	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "config.id is required") {
		t.Fatalf("expected config.id required, got %v", err)
	}

	if _, err := ex.execAgentHire(models.AgentConfig{ID: "g", Role: "vp-growth", Mode: "operating", VerticalID: "v1"}, map[string]any{
		"config": map[string]any{"id": "x", "role": "backend-agent"},
	}); err == nil {
		t.Fatal("expected authorization error")
	}

	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err != nil {
		t.Fatalf("expected initial hire ok: %v", err)
	}
	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err == nil {
		t.Fatal("expected duplicate hire error")
	}

	if _, err := ex.execAgentFire(actor, map[string]any{}); err == nil || !strings.Contains(err.Error(), "agent_id is required") {
		t.Fatalf("expected agent_id required, got %v", err)
	}
	if _, err := ex.execAgentFire(actor, map[string]any{"agent_id": "nope"}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := ex.execAgentReconfigure(actor, map[string]any{"agent_id": "nope", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}

	if _, err := ex.execMailboxSend(actor, map[string]any{"type": "review"}); err == nil || !strings.Contains(err.Error(), "mailbox store is not configured") {
		t.Fatalf("expected mailbox store error, got %v", err)
	}
	ex.SetMailboxStore(&mailboxStub{})
	if _, err := ex.execMailboxSend(actor, map[string]any{"type": "review", "timeout_at": "nope"}); err == nil || !strings.Contains(err.Error(), "invalid timeout_at") {
		t.Fatalf("expected timeout_at error, got %v", err)
	}
}

func TestDockerWorkspaceManager_RunDocker_ExecSuccessAndErrors(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	cfg := workspace.DefaultDockerConfig()
	cfg.DockerBin = "sh"
	m.SetConfigForTest(cfg)

	out, err := m.RunDocker(context.Background(), "-c", "printf 'ok'")
	if err != nil {
		t.Fatalf("expected success, err=%v", err)
	}
	if strings.TrimSpace(out) != "ok" {
		t.Fatalf("expected ok, got %q", out)
	}

	_, err = m.RunDocker(context.Background(), "-c", "echo bad 1>&2; exit 2")
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("expected error containing stderr, got %v", err)
	}

	_, err = m.RunDocker(context.Background(), "-c", "exit 2")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDockerWorkspaceManager_InspectContainer_NoSuchObject(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "inspect" {
			return "", errors.New("Error: No such object: whatever")
		}
		return "", fmt.Errorf("unexpected args: %v", args)
	})

	exists, running, err := m.InspectContainer(context.Background(), "c1")
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if exists || running {
		t.Fatalf("expected not exists, got exists=%v running=%v", exists, running)
	}
}

func TestDockerWorkspaceManager_EnsureContainerRunning_CreateStart_AndAlreadyRunning(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	var calls []string
	inspected := false
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, strings.Join(args, " "))
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			if !inspected {
				inspected = true
				return "", errors.New("no such object")
			}
			return "false", nil
		case "create":
			return "created", nil
		case "start":
			return "", errors.New("already running")
		case "network":
			return "connected", nil
		default:
			return "", fmt.Errorf("unexpected: %v", args)
		}
	})

	if err := m.EnsureContainerRunning(context.Background(), "c1", []string{"img", "sleep", "infinity"}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(calls) < 3 {
		t.Fatalf("expected inspect/create/start calls, got %v", calls)
	}
}

func TestDockerWorkspaceManager_EnsureContainerRunning_StartFailure(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			return "false", nil
		case "start":
			return "", errors.New("boom")
		case "network":
			return "connected", nil
		default:
			return "", nil
		}
	})

	if err := m.EnsureContainerRunning(context.Background(), "c1", []string{"img"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestDockerWorkspaceManager_ResolveWorkspace_RoleAndVertical(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {

		if len(args) > 0 && args[0] == "inspect" {
			return "true", nil
		}
		if len(args) > 1 && args[0] == "network" && args[1] == "connect" {
			return "connected", nil
		}
		return "", nil
	})

	target, err := m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "factory-cto"})
	if err != nil {
		t.Fatalf("resolve role: %v", err)
	}
	cfg := workspace.DefaultDockerConfig()
	if target == nil || target.Container != cfg.FactoryContainer || target.Workdir != cfg.FactoryWorkdir {
		t.Fatalf("unexpected target: %+v", target)
	}

	target2, err := m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "backend-agent", VerticalID: "VERT_123"})
	if err != nil {
		t.Fatalf("resolve vertical: %v", err)
	}
	if target2 == nil || !strings.HasPrefix(target2.Container, cfg.VerticalContainerPrefix) {
		t.Fatalf("unexpected target2: %+v", target2)
	}
	if target2.Workdir != cfg.VerticalWorkdir {
		t.Fatalf("unexpected workdir: %q", target2.Workdir)
	}
}

func TestDockerWorkspaceManager_LookupVerticalSlug_Branches(t *testing.T) {
	ctx := context.Background()

	m := workspace.NewDockerManager(nil)
	if _, err := m.LookupVerticalSlug(ctx, " "); err == nil {
		t.Fatal("expected vertical_id required error")
	}

	slug, err := m.LookupVerticalSlug(ctx, "  Ab_Cd / 123  ")
	if err != nil {
		t.Fatalf("lookup fallback: %v", err)
	}
	if slug != "ab-cd-123" {
		t.Fatalf("expected sanitized slug, got %q", slug)
	}

	_, db, _ := testutil.StartPostgres(t)
	m2 := workspace.NewDockerManager(db)
	m2.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "inspect" {
			return "true", nil
		}
		return "", nil
	})

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V',' My_Slug ','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	got, err := m2.LookupVerticalSlug(ctx, verticalID)
	if err != nil {
		t.Fatalf("lookup db: %v", err)
	}
	if got != "my-slug" {
		t.Fatalf("expected sanitized db slug, got %q", got)
	}

	verticalNoSlug := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','', 'us','operating','operating','{}'::jsonb, now(), now())
	`, verticalNoSlug); err != nil {
		t.Fatalf("seed no slug: %v", err)
	}
	if _, err := m2.LookupVerticalSlug(ctx, verticalNoSlug); err == nil {
		t.Fatal("expected error for missing slug")
	}
}

func TestDockerWorkspaceManager_StopContainer_AndInspectError(t *testing.T) {
	m := workspace.NewDockerManager((*sql.DB)(nil))
	stopped := false
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			return "true", nil
		case "stop":
			stopped = true
			return "stopped", nil
		default:
			return "", nil
		}
	})

	if err := m.StopContainer(context.Background(), "c1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !stopped {
		t.Fatal("expected stop to be called when running")
	}

	m2 := workspace.NewDockerManager(nil)
	m2.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		return "", errors.New("permission denied")
	})
	if _, _, err := m2.InspectContainer(context.Background(), "c1"); err == nil {
		t.Fatal("expected inspect error")
	}
}

func TestDockerWorkspaceManager_EnsureContainerNetwork_AlreadyConnectedIsNonFatal(t *testing.T) {
	m := workspace.NewDockerManager(nil)
	cfg := workspace.DefaultDockerConfig()
	cfg.WorkspaceNetwork = "empireai_default"
	m.SetConfigForTest(cfg)
	m.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) >= 4 && args[0] == "network" && args[1] == "connect" {
			return "", errors.New("endpoint with name empireai-factory already exists in network empireai_default")
		}
		return "", nil
	})
	if err := m.EnsureContainerNetwork(context.Background(), "empireai-factory"); err != nil {
		t.Fatalf("expected already-connected error to be ignored, got %v", err)
	}
}
