package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
)

type fakeRuntime struct {
	calls    int
	seenMsgs []Message
}

func (f *fakeRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-1", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (f *fakeRuntime) ContinueSession(_ context.Context, s *Session, message Message) (*Response, error) {
	f.calls++
	f.seenMsgs = append(f.seenMsgs, message)
	switch f.calls {
	case 1:
		return &Response{
			Message: Message{Role: "assistant", Content: "running tool"},
			ToolCalls: []ToolCall{
				{Name: "echo", Arguments: map[string]any{"text": "hello"}},
			},
		}, nil
	case 2:
		s.TurnCount++
		return &Response{Message: Message{Role: "assistant", Content: "done"}}, nil
	default:
		return &Response{Message: Message{Role: "assistant", Content: "extra"}}, nil
	}
}

type scriptedRuntime struct {
	responses []*Response
	calls     int
	seenMsgs  []Message
}

func (s *scriptedRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-scripted", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (s *scriptedRuntime) ContinueSession(_ context.Context, _ *Session, message Message) (*Response, error) {
	s.calls++
	s.seenMsgs = append(s.seenMsgs, message)
	if len(s.responses) == 0 {
		return &Response{Message: Message{Role: "assistant", Content: "done"}}, nil
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

type fakeToolExec struct {
	calls int
}

func (f *fakeToolExec) Execute(_ context.Context, name string, input any) (any, error) {
	f.calls++
	return map[string]any{"name": name, "input": input}, nil
}

func (f *fakeToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{Name: name, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

type selectiveToolExec struct {
	results map[string]any
	errors  map[string]error
	calls   []string
}

func (s *selectiveToolExec) Execute(_ context.Context, name string, input any) (any, error) {
	s.calls = append(s.calls, name)
	if err := s.errors[name]; err != nil {
		return nil, err
	}
	if out, ok := s.results[name]; ok {
		return out, nil
	}
	return map[string]any{"name": name, "input": input}, nil
}

func (s *selectiveToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") || strings.HasPrefix(strings.TrimSpace(name), "mcp__runtime-tools__emit_") {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Kind:     kind,
			Visible:  true,
			Callable: true,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type panicToolExec struct{}

func (panicToolExec) Execute(context.Context, string, any) (any, error) {
	panic("boom")
}

func (panicToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{Name: name, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

type largeToolExec struct {
	payload any
}

func (l largeToolExec) Execute(context.Context, string, any) (any, error) {
	return l.payload, nil
}

func (l largeToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{Name: name, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

type relayRuntime struct {
	relayPath string
	relayErr  error
	toolName  string
	raw       []byte
}

func (r *relayRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-relay", AgentID: agentID, RuntimeMode: "cli_test"}, nil
}

func (r *relayRuntime) ContinueSession(_ context.Context, _ *Session, message Message) (*Response, error) {
	return &Response{Message: Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func (r *relayRuntime) PersistOversizedToolResultRelay(_ context.Context, _ *Session, toolName string, rawJSON []byte) (toolResultRelayRef, error) {
	r.toolName = toolName
	r.raw = append([]byte(nil), rawJSON...)
	if r.relayErr != nil {
		return toolResultRelayRef{}, r.relayErr
	}
	return toolResultRelayRef{
		Path:       r.relayPath,
		ReadTool:   "read_file",
		Format:     "json",
		Visibility: "workspace_mount",
	}, nil
}

type capabilityAwareToolExec struct {
	captured toolcapabilities.Set
}

func (c *capabilityAwareToolExec) Execute(ctx context.Context, name string, input any) (any, error) {
	set, _ := toolcapabilities.FromContext(ctx)
	c.captured = set
	return map[string]any{"name": name, "input": input}, nil
}

func (c *capabilityAwareToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") || strings.HasPrefix(strings.TrimSpace(name), "mcp__runtime-tools__emit_") {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Kind:     kind,
			Visible:  true,
			Callable: true,
		})
	}
	return toolcapabilities.NewSet(caps)
}

func testAsString(v any) string {
	s, _ := v.(string)
	return s
}

func TestConversationStep_ResolvesToolCalls(t *testing.T) {
	rt := &fakeRuntime{}
	te := &fakeToolExec{}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{{Name: "emit_scan_requested"}}, SessionScoped, 10, rt)
	c.SetToolExecutor(te)

	resp, err := c.Step(models.WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "analysis"}), "start")
	if err != nil {
		t.Fatalf("step error: %v", err)
	}
	if resp.Message.Content != "done" {
		t.Fatalf("expected final assistant reply, got: %q", resp.Message.Content)
	}
	if te.calls != 1 {
		t.Fatalf("expected 1 tool execution, got %d", te.calls)
	}
	if rt.calls != 2 {
		t.Fatalf("expected 2 runtime calls, got %d", rt.calls)
	}
	if len(rt.seenMsgs) != 2 {
		t.Fatalf("expected 2 seen messages, got %d", len(rt.seenMsgs))
	}
	if rt.seenMsgs[1].Role != "tool" {
		t.Fatalf("expected second message role tool, got %s", rt.seenMsgs[1].Role)
	}
	var payload []map[string]any
	if err := json.Unmarshal([]byte(rt.seenMsgs[1].Content), &payload); err != nil {
		t.Fatalf("tool payload not json: %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("expected one tool payload entry, got %d", len(payload))
	}
}

func TestConversationStep_AttachesCanonicalToolCapabilitySetToExecutionContext(t *testing.T) {
	rt := &fakeRuntime{}
	te := &capabilityAwareToolExec{}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{
		{Name: "echo"},
		{Name: "emit_scan_requested"},
	}, SessionScoped, 10, rt)
	c.SetToolExecutor(te)

	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:   "analysis-agent",
		Role: "analysis",
	})
	if _, err := c.Step(ctx, "start"); err != nil {
		t.Fatalf("step error: %v", err)
	}
	if _, ok := te.captured.Capability("echo"); !ok {
		t.Fatalf("expected capability set to include echo, got %#v", te.captured.ByName)
	}
	if _, ok := te.captured.Capability("emit_scan_requested"); !ok {
		t.Fatalf("expected capability set to include emit_scan_requested, got %#v", te.captured.ByName)
	}
}

func TestConversationStep_NoExecutorReturnsInitialToolCall(t *testing.T) {
	rt := &fakeRuntime{}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{{Name: "emit_scan_requested"}, {Name: "echo"}}, SessionScoped, 10, rt)

	resp, err := c.Step(models.WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "analysis"}), "start")
	if err != nil {
		t.Fatalf("step error: %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected tool call passthrough, got %d", len(resp.ToolCalls))
	}
	if rt.calls != 1 {
		t.Fatalf("expected one runtime call, got %d", rt.calls)
	}
}

func TestConversation_MiscHelpers(t *testing.T) {
	rt := &fakeRuntime{}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)

	c.SetMaxToolRounds(-1)
	if c.maxToolRounds <= 0 {
		t.Fatalf("expected positive maxToolRounds, got %d", c.maxToolRounds)
	}

	c.AppendFeedback("  hello  ")
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "system" {
		t.Fatalf("expected system feedback message, got %+v", c.Messages)
	}

	c.AppendResult(ToolResult{Name: "t", Payload: "{\"ok\":true}"})
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "tool" {
		t.Fatalf("expected tool message after AppendResult, got %+v", c.Messages)
	}

	c.Reset()
	if c.Session != nil || c.TurnCount != 0 || len(c.Messages) != 0 {
		t.Fatalf("expected reset to clear session/messages/turns, got session=%v turns=%d msgs=%d", c.Session != nil, c.TurnCount, len(c.Messages))
	}

	if err := c.InjectAsyncToolResult(context.Background(), "x", true, map[string]any{"ok": true}, ""); err != nil {
		t.Fatalf("InjectAsyncToolResult: %v", err)
	}
	if len(c.Messages) != 1 || c.Messages[0].Role != "tool" {
		t.Fatalf("expected tool message, got %+v", c.Messages)
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(c.Messages[0].Content), &arr); err != nil || len(arr) != 1 {
		t.Fatalf("expected json array tool result, err=%v content=%q", err, c.Messages[0].Content)
	}
}

func TestConversation_ExecuteToolCalls_RecoversPanic(t *testing.T) {
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(panicToolExec{})

	raw, _ := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != false {
		t.Fatalf("expected panic to be captured as error payload, got %#v", arr)
	}
	if !strings.Contains(strings.ToLower(testAsString(arr[0]["error"])), "tool panic") {
		t.Fatalf("expected tool panic text, got %#v", arr[0]["error"])
	}
}

func TestConversation_ExecuteToolCalls_TruncatesLargeResult(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(largeToolExec{payload: map[string]any{"blob": huge}})

	raw, _ := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	if resultMap == nil || resultMap["truncated"] != true {
		t.Fatalf("expected truncated result metadata, got %#v", arr[0]["result"])
	}
}

func TestConversation_ExecuteToolCalls_PreservesLargeReadFileResultOnSupportedRelayPath(t *testing.T) {
	huge := strings.Repeat("x", maxToolMessageBytes+8*1024)
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(largeToolExec{payload: map[string]any{
		"content":    huge,
		"size_bytes": len(huge),
	}})

	raw, _ := c.executeToolCalls(context.Background(), []ToolCall{{Name: "read_file", Arguments: map[string]any{"path": "/workspace/corpus.json"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	if resultMap == nil {
		t.Fatalf("expected result map, got %#v", arr[0]["result"])
	}
	if truncated, _ := resultMap["truncated"].(bool); truncated {
		t.Fatalf("expected supported read_file relay to preserve full content, got %#v", resultMap)
	}
	content, _ := resultMap["content"].(string)
	if len(content) != len(huge) {
		t.Fatalf("content length = %d, want %d", len(content), len(huge))
	}
}

func TestConversation_ExecuteToolCalls_RelaysOversizedResultsForHelperRuntime(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	rt := &relayRuntime{relayPath: "/workspace/.swarm/tool-results/sess-relay/sql-execute-1.json"}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, rt)
	c.Session = &Session{ID: "sess-relay", AgentID: "a1", RuntimeMode: "cli_test"}
	c.SetToolExecutor(largeToolExec{payload: map[string]any{"blob": huge}})

	raw, _ := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	if rt.toolName != "sql_execute" {
		t.Fatalf("relay tool name = %q, want sql_execute", rt.toolName)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	if resultMap == nil || resultMap["truncated"] != true {
		t.Fatalf("expected relayed oversized result metadata, got %#v", arr[0]["result"])
	}
	followUp, _ := resultMap["follow_up"].(map[string]any)
	if followUp == nil {
		t.Fatalf("expected follow_up metadata, got %#v", resultMap)
	}
	if followUp["path"] != rt.relayPath || followUp["tool"] != "read_file" {
		t.Fatalf("follow_up = %#v, want relay path/tool", followUp)
	}
	if strings.Contains(string(rt.raw), "...(truncated)") {
		t.Fatalf("relay payload should keep full raw json, got %q", string(rt.raw))
	}
}

func TestConversation_ExecuteToolCalls_RelaysOversizedReadFileResultsForHelperRuntime(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	rt := &relayRuntime{relayPath: "/workspace/.swarm/tool-results/sess-relay/read-file-1.json"}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, rt)
	c.Session = &Session{ID: "sess-relay", AgentID: "a1", RuntimeMode: "cli_test"}
	c.SetToolExecutor(largeToolExec{payload: map[string]any{
		"content":    huge,
		"size_bytes": len(huge),
	}})

	raw, _ := c.executeToolCalls(context.Background(), []ToolCall{{Name: "read_file", Arguments: map[string]any{"path": "/data/test-signals-25.jsonl"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	followUp, _ := resultMap["follow_up"].(map[string]any)
	if followUp == nil || followUp["path"] != rt.relayPath {
		t.Fatalf("follow_up = %#v, want relay path", followUp)
	}
}

func TestConversation_ExecuteToolCalls_FailsClosedWhenHelperRelayWriteFails(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	rt := &relayRuntime{relayErr: errors.New("workspace boom")}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, rt)
	c.Session = &Session{ID: "sess-relay", AgentID: "a1", RuntimeMode: "cli_test"}
	c.SetToolExecutor(largeToolExec{payload: map[string]any{"blob": huge}})

	raw, executed := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != false {
		t.Fatalf("expected failed tool payload, got %#v", arr)
	}
	if !strings.Contains(testAsString(arr[0]["error"]), "workspace boom") {
		t.Fatalf("tool relay error = %#v, want workspace boom", arr[0]["error"])
	}
	if len(executed) != 1 || executed[0].OK {
		t.Fatalf("executed = %#v, want failed tool execution record", executed)
	}
}

func TestConversationStep_TerminatesAfterSuccessfulEmitToolCalls(t *testing.T) {
	rt := &scriptedRuntime{
		responses: []*Response{{
			Message: Message{Role: "assistant", Content: "emitting"},
			ToolCalls: []ToolCall{
				{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "corpus"}},
			},
		}},
	}
	te := &selectiveToolExec{
		results: map[string]any{
			"emit_scan_requested": map[string]any{"event_id": "evt-1", "status": "published"},
		},
	}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{{Name: "emit_scan_requested"}}, SessionScoped, 10, rt)
	c.SetToolExecutor(te)

	resp, err := c.Step(models.WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "analysis"}), "start")
	if err != nil {
		t.Fatalf("step error: %v", err)
	}
	if rt.calls != 1 {
		t.Fatalf("expected one runtime call, got %d", rt.calls)
	}
	if len(rt.seenMsgs) != 1 {
		t.Fatalf("expected one seen message, got %d", len(rt.seenMsgs))
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected terminal emit response to clear tool calls, got %+v", resp.ToolCalls)
	}
	if len(te.calls) != 1 || te.calls[0] != "emit_scan_requested" {
		t.Fatalf("unexpected tool execution calls: %#v", te.calls)
	}
}

func TestConversationStep_ContinuesWhenAnyNonEmitToolIsPresent(t *testing.T) {
	rt := &scriptedRuntime{
		responses: []*Response{{
			Message: Message{Role: "assistant", Content: "continued"},
		}},
	}
	te := &selectiveToolExec{
		results: map[string]any{
			"emit_scan_requested": map[string]any{"event_id": "evt-1", "status": "published"},
			"echo":                map[string]any{"ok": true},
		},
	}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{{Name: "emit_scan_requested"}, {Name: "echo"}}, SessionScoped, 10, rt)
	c.SetToolExecutor(te)
	c.Session = &Session{ID: "sess-1", AgentID: "a1", RuntimeMode: "api"}

	initial := &Response{
		Message: Message{Role: "assistant", Content: "doing work"},
		ToolCalls: []ToolCall{
			{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "corpus"}},
			{Name: "echo", Arguments: map[string]any{"text": "hello"}},
		},
	}
	resp, err := c.resolveToolCalls(models.WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "analysis"}), initial)
	if err != nil {
		t.Fatalf("resolveToolCalls error: %v", err)
	}
	if rt.calls != 1 {
		t.Fatalf("expected continuation runtime call, got %d", rt.calls)
	}
	if len(rt.seenMsgs) != 1 || rt.seenMsgs[0].Role != "tool" {
		t.Fatalf("expected tool continuation message, got %+v", rt.seenMsgs)
	}
	if resp.Message.Content != "continued" {
		t.Fatalf("unexpected response content: %q", resp.Message.Content)
	}
}

func TestConversationStep_ContinuesWhenEmitToolFails(t *testing.T) {
	rt := &scriptedRuntime{
		responses: []*Response{{
			Message: Message{Role: "assistant", Content: "continued after error"},
		}},
	}
	te := &selectiveToolExec{
		errors: map[string]error{
			"emit_scan_requested": errors.New("publish failed"),
		},
	}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)
	c.SetToolExecutor(te)
	c.Session = &Session{ID: "sess-1", AgentID: "a1", RuntimeMode: "api"}

	initial := &Response{
		Message: Message{Role: "assistant", Content: "doing work"},
		ToolCalls: []ToolCall{
			{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "corpus"}},
		},
	}
	if _, err := c.resolveToolCalls(context.Background(), initial); err != nil {
		t.Fatalf("resolveToolCalls error: %v", err)
	}
	if rt.calls != 1 {
		t.Fatalf("expected continuation runtime call after emit failure, got %d", rt.calls)
	}
}

func TestConversationStep_DoesNotExecuteEmitAfterSaveFailureInSameRound(t *testing.T) {
	rt := &scriptedRuntime{
		responses: []*Response{{
			Message: Message{Role: "assistant", Content: "should have stopped after save failure"},
		}},
	}
	te := &selectiveToolExec{
		results: map[string]any{
			"emit_spec_draft_ready": map[string]any{"event_id": "evt-1", "status": "published"},
		},
		errors: map[string]error{
			"save_entity_field": errors.New("cross_flow_write_forbidden"),
		},
	}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)
	c.SetToolExecutor(te)
	c.Session = &Session{ID: "sess-1", AgentID: "a1", RuntimeMode: "api"}

	initial := &Response{
		Message: Message{Role: "assistant", Content: "doing work"},
		ToolCalls: []ToolCall{
			{Name: "save_entity_field", Arguments: map[string]any{"field": "mvp_spec"}},
			{Name: "emit_spec_draft_ready", Arguments: map[string]any{"entity_id": "validation-1"}},
		},
	}
	if _, err := c.resolveToolCalls(context.Background(), initial); err != nil {
		t.Fatalf("resolveToolCalls error: %v", err)
	}
	if len(te.calls) != 1 || te.calls[0] != "save_entity_field" {
		t.Fatalf("tool calls = %#v, want only save_entity_field", te.calls)
	}
}

func TestExecuteToolCalls_UsesCapabilityKindForTerminalBehavior(t *testing.T) {
	te := &capabilityAwareToolExec{}
	c := NewConversation("a1", "t1", "sys", []ToolDefinition{{Name: "emit_scan_requested"}}, SessionScoped, 10, &fakeRuntime{})
	c.SetToolExecutor(te)

	ctx := models.WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "analysis"})
	_, executed := c.executeToolCalls(ctx, []ToolCall{{Name: "emit_scan_requested", Arguments: map[string]any{"x": 1}}})
	if len(executed) != 1 {
		t.Fatalf("executed length = %d, want 1", len(executed))
	}
	if !executed[0].Terminal {
		t.Fatalf("executed[0].Terminal = false, want true")
	}
	if !shouldTerminateAfterToolCalls(executed) {
		t.Fatal("shouldTerminateAfterToolCalls = false, want true")
	}
}
