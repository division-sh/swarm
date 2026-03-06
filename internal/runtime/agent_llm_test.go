package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

type llmToolCallRuntime struct {
	firstResponse Response
	secondText    string
}

func (r *llmToolCallRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-1", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *llmToolCallRuntime) ContinueSession(_ context.Context, _ *Session, msg Message) (*Response, error) {
	if msg.Role == "tool" {
		return &Response{Message: Message{Role: "assistant", Content: strings.TrimSpace(r.secondText)}}, nil
	}
	resp := r.firstResponse
	if strings.TrimSpace(resp.Message.Role) == "" {
		resp.Message.Role = "assistant"
	}
	return &resp, nil
}

type llmNoToolRuntime struct{}

func (r *llmNoToolRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-2", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *llmNoToolRuntime) ContinueSession(_ context.Context, _ *Session, _ Message) (*Response, error) {
	return &Response{Message: Message{Role: "assistant", Content: "acknowledged"}}, nil
}

func TestLLMAgentOnEvent_EmitViaToolCall(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)

	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        mustJSON(map[string]any{"system_prompt": "coordinator", "tools": []string{"agent_message"}}),
	}
	rt := &llmToolCallRuntime{
		firstResponse: Response{
			Message: Message{Role: "assistant", Content: "calling emit tool"},
			ToolCalls: []ToolCall{{
				Name: "emit_scan_requested",
				Arguments: map[string]any{
					"mode":      "saas_gap",
					"geography": "paraguay",
				},
			}},
		},
		secondText: "done",
	}
	agent := NewLLMAgent(cfg, rt, exec, exec.ToolDefinitions())

	out, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	if err != nil {
		t.Fatalf("OnEvent error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no direct returned events, got %d", len(out))
	}
	if len(store.events) == 0 {
		t.Fatal("expected emitted event persisted through event bus")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if strings.TrimSpace(asString(payload["mode"])) != "saas_gap" {
		t.Fatalf("expected normalized mode, got %+v", payload["mode"])
	}
}

func TestLLMAgentOnEvent_RequiresDirectiveEmission(t *testing.T) {
	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        mustJSON(map[string]any{"system_prompt": "coordinator"}),
	}
	agent := NewLLMAgent(cfg, &llmNoToolRuntime{}, noopToolExec{}, nil)
	_, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-2",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	if err == nil || !strings.Contains(err.Error(), "must emit scan.requested") {
		t.Fatalf("expected missing scan emission error, got %v", err)
	}
}

func TestNewLLMAgent_AutoInjectsEmitToolsWhenConstrained(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "empire-coordinator",
		Type: "worker",
		Role: "empire-coordinator",
		Mode: "holding",
		Config: mustJSON(map[string]any{
			"system_prompt": "coordinator",
			"tools":         []string{"agent_message"},
		}),
	}
	agent := NewLLMAgent(cfg, &llmNoToolRuntime{}, noopToolExec{}, []ToolDefinition{
		{Name: "agent_message"},
		{Name: "mailbox_send"},
	})
	foundEmit := false
	foundMailbox := false
	for _, tool := range agent.conversation.Tools {
		if tool.Name == "emit_scan_requested" {
			foundEmit = true
		}
		if tool.Name == "mailbox_send" {
			foundMailbox = true
		}
	}
	if !foundEmit {
		t.Fatalf("expected emit_scan_requested auto-injected, got %+v", agent.conversation.Tools)
	}
	if !foundMailbox {
		t.Fatalf("expected constrained static tools filtering to retain universal mailbox_send, got %+v", agent.conversation.Tools)
	}
}

func TestFormatEventForAgent_UsesEmitToolContractText(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "empire-coordinator",
		Role: "empire-coordinator",
		Mode: "holding",
	}
	text := formatEventForAgent(cfg, events.Event{
		ID:          "evt-3",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "x"}),
	})
	if !strings.Contains(text, "Emit events by calling emit_* tools only") {
		t.Fatalf("expected emit tool contract text, got: %s", text)
	}
	if strings.Contains(text, "emit_events") {
		t.Fatalf("did not expect legacy emit_events envelope contract, got: %s", text)
	}
	if !strings.Contains(text, "REQUIRED for this turn: call emit_scan_requested exactly once") {
		t.Fatalf("expected strict directive tool requirement, got: %s", text)
	}
}

func TestFormatEventForAgent_BudgetThresholdAddsStrictBudgetEmitRequirement(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "empire-coordinator",
		Role: "empire-coordinator",
		Mode: "holding",
	}
	text := formatEventForAgent(cfg, events.Event{
		ID:          "evt-budget",
		Type:        events.EventType("budget.threshold_crossed"),
		SourceAgent: "runtime",
		Payload:     mustJSON(map[string]any{"status": "throttle"}),
	})
	if !strings.Contains(text, "REQUIRED for this turn: call exactly one emit_budget_* tool") {
		t.Fatalf("expected strict budget emit requirement, got: %s", text)
	}
}

func TestContractRemediationPrompt_EmpireCoordinatorDirective(t *testing.T) {
	prompt, ok := contractRemediationPrompt(
		models.AgentConfig{Role: "empire-coordinator"},
		events.Event{Type: events.EventType("system.directive")},
		errors.New("system.directive handling must emit scan.requested via emit_scan_requested"),
	)
	if !ok {
		t.Fatal("expected remediation prompt for coordinator directive")
	}
	if !strings.Contains(prompt, "emit_scan_requested") {
		t.Fatalf("expected remediation prompt to reference emit_scan_requested, got: %s", prompt)
	}
}

func TestNormalizeScanMode_AcceptsV2038Aliases(t *testing.T) {
	cases := map[string]string{
		"local_underserved":    "local_services",
		"trend_opportunity":    "saas_trend",
		"adjacent_opportunity": "saas_trend",
	}
	for in, want := range cases {
		if got := normalizeScanMode(in); got != want {
			t.Fatalf("normalizeScanMode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewLLMAgent_ConstraintsOverrideConversationDefaults(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "pipeline-coordinator",
		Type: "worker",
		Role: "pipeline-coordinator",
		Mode: "factory",
		Config: mustJSON(map[string]any{
			"system_prompt": "score",
			"constraints": map[string]any{
				"max_turns_per_task": 40,
				"conversation_mode":  "session",
			},
		}),
	}
	agent := NewLLMAgent(cfg, &llmNoToolRuntime{}, noopToolExec{}, nil)
	if agent.conversation.MaxTurns != 40 {
		t.Fatalf("expected max turns override=40, got %d", agent.conversation.MaxTurns)
	}
	if agent.conversation.Mode != SessionScoped {
		t.Fatalf("expected conversation mode override=session, got %v", agent.conversation.Mode)
	}
}

func TestExtractConversationConstraints_TopLevelFallback(t *testing.T) {
	mode, maxTurns := extractConversationConstraints(mustJSON(map[string]any{
		"conversation_mode":  "task",
		"max_turns_per_task": 12,
	}))
	if mode == nil || *mode != TaskScoped {
		t.Fatalf("expected task mode from top-level fallback, got %v", mode)
	}
	if maxTurns != 12 {
		t.Fatalf("expected max turns=12 from top-level fallback, got %d", maxTurns)
	}
}

type noopToolExec struct{}

func (noopToolExec) Execute(context.Context, string, any) (any, error) { return nil, nil }

type countingSessionRuntime struct {
	startCalls int
}

func (r *countingSessionRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	r.startCalls++
	return &Session{ID: "sess-count", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *countingSessionRuntime) ContinueSession(_ context.Context, _ *Session, _ Message) (*Response, error) {
	return &Response{Message: Message{Role: "assistant", Content: "ok"}}, nil
}

func TestLLMAgentOnEvent_TaskScopedResetsAcrossVerticalContexts(t *testing.T) {
	rt := &countingSessionRuntime{}
	cfg := models.AgentConfig{
		ID:   "pipeline-coordinator",
		Type: "worker",
		Role: "pipeline-coordinator",
		Mode: "factory",
		Config: mustJSON(map[string]any{
			"system_prompt": "score",
		}),
	}
	agent := NewLLMAgent(cfg, rt, noopToolExec{}, nil)
	if agent.conversation.Mode != TaskScoped {
		t.Fatalf("expected task scoped conversation for factory mode, got %v", agent.conversation.Mode)
	}

	evtA := events.Event{
		ID:         "evt-a",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-a",
		Payload:    mustJSON(map[string]any{"vertical_id": "vertical-a", "dimension": "pain_severity", "score": 72}),
	}
	if _, err := agent.OnEvent(context.Background(), evtA); err != nil {
		t.Fatalf("OnEvent first: %v", err)
	}
	if got := agent.conversation.TurnCount; got != 1 {
		t.Fatalf("expected turn_count=1 after first scoped event, got %d", got)
	}

	evtB := events.Event{
		ID:         "evt-b",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-b",
		Payload:    mustJSON(map[string]any{"vertical_id": "vertical-b", "dimension": "market_size", "score": 68}),
	}
	if _, err := agent.OnEvent(context.Background(), evtB); err != nil {
		t.Fatalf("OnEvent second: %v", err)
	}
	// Cross-vertical event must use a fresh task-scoped conversation.
	if got := agent.conversation.TurnCount; got != 1 {
		t.Fatalf("expected turn_count reset to 1 after scope change, got %d", got)
	}
	if rt.startCalls != 2 {
		t.Fatalf("expected StartSession called twice across scope change, got %d", rt.startCalls)
	}
}

func TestLLMAgentOnEvent_TaskScopedRetriesAfterMaxTurns(t *testing.T) {
	rt := &countingSessionRuntime{}
	cfg := models.AgentConfig{
		ID:   "pipeline-coordinator",
		Type: "worker",
		Role: "pipeline-coordinator",
		Mode: "factory",
		Config: mustJSON(map[string]any{
			"system_prompt": "score",
			"constraints": map[string]any{
				"max_turns_per_task": 1,
				"conversation_mode":  "task",
			},
		}),
	}
	agent := NewLLMAgent(cfg, rt, noopToolExec{}, nil)

	evt := events.Event{
		ID:         "evt-retry-1",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-retry",
		Payload:    mustJSON(map[string]any{"vertical_id": "vertical-retry", "dimension": "pain_severity", "score": 70}),
	}
	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("first OnEvent: %v", err)
	}

	evt2 := events.Event{
		ID:         "evt-retry-2",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-retry",
		Payload:    mustJSON(map[string]any{"vertical_id": "vertical-retry", "dimension": "market_size", "score": 66}),
	}
	if _, err := agent.OnEvent(context.Background(), evt2); err != nil {
		t.Fatalf("expected retry after max-turn reset, got error: %v", err)
	}
	if rt.startCalls < 2 {
		t.Fatalf("expected second StartSession after max-turn reset, got %d", rt.startCalls)
	}
}
