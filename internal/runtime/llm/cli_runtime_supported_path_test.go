package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

type firstTurnWorkflowToolExec struct {
	readPayload any
	calls       []string
}

func (e *firstTurnWorkflowToolExec) Execute(ctx context.Context, name string, input any) (any, error) {
	e.calls = append(e.calls, strings.TrimSpace(name))
	switch strings.TrimSpace(name) {
	case "read_file":
		return e.readPayload, nil
	case "emit_category_assessed":
		if recorder, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok {
			recorder.Append(events.Event{Type: events.EventType("discovery/category.assessed")})
		}
		return map[string]any{"emitted": true, "input": input}, nil
	default:
		return map[string]any{"name": name, "input": input}, nil
	}
}

func (e *firstTurnWorkflowToolExec) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
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

func TestConversationStep_ClaudeCLIFirstTurnPreservesSupportedReadFileSurface(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "docker-state")
	captureDir := filepath.Join(tempDir, "captures")
	if err := os.MkdirAll(captureDir, 0o755); err != nil {
		t.Fatalf("mkdir capture dir: %v", err)
	}
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
state_file="${FAKE_DOCKER_STATE_FILE}"
capture_dir="${FAKE_DOCKER_CAPTURE_DIR}"
count=0
if [ -f "$state_file" ]; then
  count=$(cat "$state_file")
fi
count=$((count + 1))
printf '%s' "$count" > "$state_file"
cat > "$capture_dir/$count.stdin"
if grep -q '"name":"read_file"' "$capture_dir/$count.stdin" && grep -q '"ok":true' "$capture_dir/$count.stdin"; then
  printf '%s\n' '{"type":"result","result":"done"}'
  exit 0
fi
  printf '%s\n' '{"type":"system","subtype":"init","session_id":"provider-sess-1","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["mcp__runtime-tools__emit_category_assessed","Read","Write","Edit"]}'
  printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-read-1","name":"Read","input":{"path":"/workspace/corpus.json"}}},"session_id":"provider-sess-1"}'
  printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"provider-sess-1"}'
  printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-emit-1","name":"emit_category_assessed","input":{"category":"payments"}}},"session_id":"provider-sess-1"}'
  printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_stop","index":1},"session_id":"provider-sess-1"}'
  exit 0
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}
	t.Setenv("SWARM_DOCKER_BIN", scriptPath)
	t.Setenv("FAKE_DOCKER_STATE_FILE", stateFile)
	t.Setenv("FAKE_DOCKER_CAPTURE_DIR", captureDir)

	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	var allowedTools []string
	turns := &turnCapture{}
	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",
		turns,
		nil,
		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register: func(_ context.Context, _ time.Duration, got []string) string {
					allowedTools = append([]string(nil), got...)
					return "ctx-token-368"
				},
				unregister: func(string) {},
			},
		},
	)
	relayWrites := 0
	runtime.execWorkspaceFn = func(context.Context, *workspace.Target, string, ...string) ([]byte, []byte, int, error) {
		relayWrites++
		return nil, nil, 0, nil
	}

	huge := strings.Repeat("x", maxToolResultBytes+1024)
	exec := &firstTurnWorkflowToolExec{
		readPayload: map[string]any{
			"content":    huge,
			"size_bytes": len(huge),
		},
	}
	conv := NewConversation(
		"market-research-agent",
		"task-1",
		"system prompt",
		[]ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
		},
		SessionScoped,
		4,
		runtime,
	)
	conv.SetToolExecutor(exec)

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(
		sessions.WithScope(
			runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
				ID: "market-research-agent",
				NativeTools: runtimeactors.NativeToolConfig{
					FileIO: true,
				},
				SessionScope: sessions.SessionScopeGlobal.String(),
			}),
			sessions.RuntimeModeSession.String(),
			sessions.SessionScopeGlobal.String(),
			"global",
		),
		recorder,
	)

	resp, err := conv.Step(ctx, "scan the file")
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp == nil {
		t.Fatal("expected final response")
	}
	if relayWrites != 0 {
		t.Fatalf("relay writes = %d, want 0 on supported read_file turn", relayWrites)
	}
	if !slices.Equal(allowedTools, []string{"emit_category_assessed"}) {
		t.Fatalf("allowed tools = %#v", allowedTools)
	}
	if len(turns.records) == 0 {
		t.Fatal("expected persisted turn records")
	}
	first := turns.records[0]
	if !slices.Equal(first.AvailableTools, []string{"emit_category_assessed", "read_file", "write_file"}) {
		t.Fatalf("first turn available_tools = %#v", first.AvailableTools)
	}
	if !slices.Equal(first.MCPToolsListed, []string{"mcp__runtime-tools__emit_category_assessed"}) {
		t.Fatalf("first turn mcp_tools_listed = %#v", first.MCPToolsListed)
	}
	if !slices.Equal(exec.calls, []string{"read_file", "emit_category_assessed"}) {
		t.Fatalf("tool exec calls = %#v", exec.calls)
	}
	if len(recorder.Snapshot()) != 1 || recorder.Snapshot()[0].Type != events.EventType("discovery/category.assessed") {
		t.Fatalf("emitted events = %#v", recorder.Snapshot())
	}

	secondInput, err := capturedToolResultInput(captureDir)
	if err != nil {
		t.Fatalf("read tool-result stdin: %v", err)
	}
	var payload []map[string]any
	if err := json.Unmarshal(secondInput, &payload); err != nil {
		t.Fatalf("unmarshal second stdin: %v", err)
	}
	if len(payload) != 2 {
		t.Fatalf("tool payload entries = %d, want 2 (%#v)", len(payload), payload)
	}
	readEntry := payload[0]
	if readEntry["name"] != "read_file" || readEntry["ok"] != true {
		t.Fatalf("read entry = %#v", readEntry)
	}
	readResult, _ := readEntry["result"].(map[string]any)
	if readResult == nil {
		t.Fatalf("read result = %#v", readEntry["result"])
	}
	if _, ok := readResult["follow_up"]; ok {
		t.Fatalf("read result follow_up = %#v, want absent on supported turn", readResult["follow_up"])
	}
	if truncated, _ := readResult["truncated"].(bool); truncated {
		t.Fatalf("read result = %#v, want full inline content", readResult)
	}
	content, _ := readResult["content"].(string)
	if len(content) != len(huge) {
		t.Fatalf("read result content len = %d, want %d", len(content), len(huge))
	}
}

func capturedToolResultInput(captureDir string) ([]byte, error) {
	matches, err := filepath.Glob(filepath.Join(captureDir, "*.stdin"))
	if err != nil {
		return nil, err
	}
	slices.Sort(matches)
	for _, match := range matches {
		data, err := os.ReadFile(match)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(data, []byte(`"name":"read_file"`)) && bytes.Contains(data, []byte(`"ok":true`)) {
			return data, nil
		}
	}
	return nil, os.ErrNotExist
}
