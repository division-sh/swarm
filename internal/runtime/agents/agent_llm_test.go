package agents

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sharedjson"
	runtimetools "empireai/internal/runtime/tools"
)

type captureStore struct {
	events []events.Event
}

func (c *captureStore) AppendEvent(ctx context.Context, evt events.Event) error {
	c.events = append(c.events, evt)
	return nil
}

func (c *captureStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

type llmToolCallRuntime struct {
	firstResponse llm.Response
	secondText    string
}

func (r *llmToolCallRuntime) StartSession(_ context.Context, agentID, _ string, _ []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "sess-1", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *llmToolCallRuntime) ContinueSession(_ context.Context, _ *llm.Session, msg llm.Message) (*llm.Response, error) {
	if msg.Role == "tool" {
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: strings.TrimSpace(r.secondText)}}, nil
	}
	resp := r.firstResponse
	if strings.TrimSpace(resp.Message.Role) == "" {
		resp.Message.Role = "assistant"
	}
	return &resp, nil
}

type llmNoToolRuntime struct{}

func (r *llmNoToolRuntime) StartSession(_ context.Context, agentID, _ string, _ []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "sess-2", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *llmNoToolRuntime) ContinueSession(_ context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "acknowledged"}}, nil
}

func TestLLMAgentOnEvent_EmitViaToolCall(t *testing.T) {
	store := &captureStore{}
	bus := runtimebus.NewEventBus(store)
	exec := runtimetools.NewExecutor(bus, nil, nil)

	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        sharedjson.MustJSON(map[string]any{"system_prompt": "coordinator", "tools": []string{"agent_message"}}),
	}
	rt := &llmToolCallRuntime{
		firstResponse: llm.Response{
			Message: llm.Message{Role: "assistant", Content: "calling emit tool"},
			ToolCalls: []llm.ToolCall{{
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
		Payload:     sharedjson.MustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
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
	if strings.TrimSpace(sharedjson.AsString(payload["mode"])) != "saas_gap" {
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
		Config:        sharedjson.MustJSON(map[string]any{"system_prompt": "coordinator"}),
	}
	agent := NewLLMAgent(cfg, &llmNoToolRuntime{}, noopToolExec{}, nil)
	_, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-2",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     sharedjson.MustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
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
		Config: sharedjson.MustJSON(map[string]any{
			"system_prompt": "coordinator",
			"tools":         []string{"agent_message"},
		}),
	}
	agent := NewLLMAgent(cfg, &llmNoToolRuntime{}, noopToolExec{}, []llm.ToolDefinition{
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
		Payload:     sharedjson.MustJSON(map[string]any{"directive_text": "x"}),
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
		Payload:     sharedjson.MustJSON(map[string]any{"status": "throttle"}),
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
		Config: sharedjson.MustJSON(map[string]any{
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
	if agent.conversation.Mode != llm.SessionScoped {
		t.Fatalf("expected conversation mode override=session, got %v", agent.conversation.Mode)
	}
}

func TestExtractConversationConstraints_TopLevelFallback(t *testing.T) {
	mode, maxTurns := extractConversationConstraints(sharedjson.MustJSON(map[string]any{
		"conversation_mode":  "task",
		"max_turns_per_task": 12,
	}))
	if mode == nil || *mode != llm.TaskScoped {
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

func (r *countingSessionRuntime) StartSession(_ context.Context, agentID, _ string, _ []llm.ToolDefinition) (*llm.Session, error) {
	r.startCalls++
	return &llm.Session{ID: "sess-count", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *countingSessionRuntime) ContinueSession(_ context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}}, nil
}

func TestLLMAgentOnEvent_TaskScopedResetsAcrossVerticalContexts(t *testing.T) {
	rt := &countingSessionRuntime{}
	cfg := models.AgentConfig{
		ID:   "pipeline-coordinator",
		Type: "worker",
		Role: "pipeline-coordinator",
		Mode: "factory",
		Config: sharedjson.MustJSON(map[string]any{
			"system_prompt": "score",
		}),
	}
	agent := NewLLMAgent(cfg, rt, noopToolExec{}, nil)
	if agent.conversation.Mode != llm.TaskScoped {
		t.Fatalf("expected task scoped conversation for factory mode, got %v", agent.conversation.Mode)
	}

	evtA := events.Event{
		ID:         "evt-a",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-a",
		Payload:    sharedjson.MustJSON(map[string]any{"vertical_id": "vertical-a", "dimension": "pain_severity", "score": 72}),
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
		Payload:    sharedjson.MustJSON(map[string]any{"vertical_id": "vertical-b", "dimension": "market_size", "score": 68}),
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
		Config: sharedjson.MustJSON(map[string]any{
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
		Payload:    sharedjson.MustJSON(map[string]any{"vertical_id": "vertical-retry", "dimension": "pain_severity", "score": 70}),
	}
	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("first OnEvent: %v", err)
	}

	evt2 := events.Event{
		ID:         "evt-retry-2",
		Type:       events.EventType("score.dimension_complete"),
		VerticalID: "vertical-retry",
		Payload:    sharedjson.MustJSON(map[string]any{"vertical_id": "vertical-retry", "dimension": "market_size", "score": 66}),
	}
	if _, err := agent.OnEvent(context.Background(), evt2); err != nil {
		t.Fatalf("expected retry after max-turn reset, got error: %v", err)
	}
	if rt.startCalls < 2 {
		t.Fatalf("expected second StartSession after max-turn reset, got %d", rt.startCalls)
	}
}

func TestAgentContextHelpers_ExtractContextIDs_AndTransitionFallback(t *testing.T) {
	evt := events.Event{
		VerticalID: "",
		TaskID:     "",
		Payload: sharedjson.MustJSON(map[string]any{
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
		Payload: sharedjson.MustJSON(map[string]any{
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
		events.Event{Payload: sharedjson.MustJSON(map[string]any{"vertical_id": "v1"})},
		events.Event{Payload: sharedjson.MustJSON(map[string]any{"vertical_id": "v2", "task_id": "t2"})},
	)
	if key != "v1|t2" {
		t.Fatalf("transitionContextKey fallback mismatch: %q", key)
	}
}

func TestAgentContractHelpers_BudgetExpectationAndRemediation(t *testing.T) {
	agent := &LLMAgent{cfg: models.AgentConfig{Role: "empire-coordinator"}}
	inbound := events.Event{Type: events.EventType("budget.threshold_crossed")}

	recorder := runtimebus.NewEmittedEventsRecorder()
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
		runtimebus.NewEmittedEventsRecorder(),
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
	recorder := runtimebus.NewEmittedEventsRecorder()
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
	recorder = runtimebus.NewEmittedEventsRecorder()
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
