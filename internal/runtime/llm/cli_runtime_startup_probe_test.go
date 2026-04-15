package llm

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_CapturesProviderInitVisibleTools(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' '{"type":"system","subtype":"init","session_id":"provider-startup-1","tools":["Read","Write","Edit"]}'
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}
	t.Setenv("SWARM_DOCKER_BIN", scriptPath)

	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",
		nil,
		nil,
		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-startup" },
				unregister: func(string) {},
			},
		},
	)

	actor := runtimeactors.AgentConfig{
		ID: "market-research-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	}
	tools := []ToolDefinition{
		{Name: "read_file"},
		{Name: "write_file"},
	}

	resp, err := runtime.ProbeStartupVisibleToolSurface(runtimeactors.WithActor(context.Background(), actor), actor, "system prompt", tools)
	if err != nil {
		t.Fatalf("ProbeStartupVisibleToolSurface: %v", err)
	}
	if got := resp.SessionID; got != "provider-startup-1" {
		t.Fatalf("session_id = %q, want provider-startup-1", got)
	}
	got := ObservedCanonicalVisibleToolsForActor(actor, tools, resp)
	want := PlannedCanonicalVisibleToolsForActor(actor, tools)
	if !slices.Equal(got, want) {
		t.Fatalf("observed visible tools = %#v, want %#v", got, want)
	}
}
