package llm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_CapturesProviderInitVisibleTools(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
session_id=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--session-id" ]; then
    shift
    session_id="${1:-}"
    break
  fi
  shift
done
if ! printf '%s' "$session_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
  printf '%s\n' 'Error: Invalid session ID. Must be a valid UUID.' >&2
  exit 1
fi
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
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
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

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_UsesUUIDSessionID(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	capturePath := filepath.Join(tempDir, "captured-session-id")
	script := `#!/bin/sh
set -eu
capture_path="${CAPTURE_PATH}"
session_id=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--session-id" ]; then
    shift
    session_id="${1:-}"
    break
  fi
  shift
done
printf '%s' "$session_id" > "$capture_path"
if ! printf '%s' "$session_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
  printf '%s\n' 'Error: Invalid session ID. Must be a valid UUID.' >&2
  exit 1
fi
cat >/dev/null
printf '%s\n' '{"type":"system","subtype":"init","session_id":"provider-startup-uuid","tools":["Read","Write","Edit"]}'
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}
	t.Setenv("SWARM_DOCKER_BIN", scriptPath)
	t.Setenv("CAPTURE_PATH", capturePath)

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
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
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

	if _, err := runtime.ProbeStartupVisibleToolSurface(runtimeactors.WithActor(context.Background(), actor), actor, "system prompt", tools); err != nil {
		t.Fatalf("ProbeStartupVisibleToolSurface: %v", err)
	}

	sessionIDBytes, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatalf("read captured session id: %v", err)
	}
	sessionID := string(sessionIDBytes)
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(sessionID) {
		t.Fatalf("captured session id = %q, want UUID", sessionID)
	}
}

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_MissingWorkspaceCLIIsActionable(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("SWARM_WORKSPACE_IMAGE", "swarm-workspace:test")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' 'OCI runtime exec failed: exec failed: unable to start container process: exec: "claude": executable file not found in $PATH: unknown' >&2
exit 127
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
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-missing-cli" },
				unregister: func(string) {},
			},
		},
	)

	actor := runtimeactors.AgentConfig{ID: "market-research-agent"}
	_, err := runtime.ProbeStartupVisibleToolSurface(runtimeactors.WithActor(context.Background(), actor), actor, "system prompt", []ToolDefinition{{Name: "emit_event"}})
	if !errors.Is(err, ErrClaudeWorkspaceCLIUnavailable) {
		t.Fatalf("ProbeStartupVisibleToolSurface error = %v, want ErrClaudeWorkspaceCLIUnavailable", err)
	}
	for _, want := range []string{
		"local cli_test workspace cannot execute configured Claude CLI command",
		`"claude"`,
		`"swarm-agent-market-research"`,
		`"swarm-workspace:test"`,
		"remove stale workspace containers",
		"set SWARM_WORKSPACE_IMAGE",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ProbeStartupVisibleToolSurface error missing %q:\n%v", want, err)
		}
	}
}
