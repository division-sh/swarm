package llm

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",

		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			ToolGateway:         testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-startup" },
				unregister: func(string) {},
			},
		})

	actor := runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	}
	tools := []ToolDefinition{
		{Name: "read_file"},
		{Name: "write_file"},
	}

	resp, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, tools), actor, "system prompt", tools)
	if err != nil {
		t.Fatalf("ProbeStartupVisibleToolSurface: %v", err)
	}
	if got := resp.SessionID; got != "provider-startup-1" {
		t.Fatalf("session_id = %q, want provider-startup-1", got)
	}
	if resp.CapabilitySurface == nil {
		t.Fatal("startup response is missing canonical capability surface")
	}
	got := resp.CapabilitySurface.EffectiveNames()
	if len(got) != 2 || got[0] != "read_file" || got[1] != "write_file" {
		t.Fatalf("effective startup tools = %#v, want [read_file write_file]", got)
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
	t.Setenv("CAPTURE_PATH", capturePath)

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",

		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-startup" },
				unregister: func(string) {},
			},
		})

	actor := runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	}
	tools := []ToolDefinition{
		{Name: "read_file"},
		{Name: "write_file"},
	}

	if _, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, tools), actor, "system prompt", tools); err != nil {
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

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.Workspace.Image = "swarm-workspace:test"
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",

		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			ToolGateway:         testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-missing-cli" },
				unregister: func(string) {},
			},
		})

	actor := runtimeactors.AgentConfig{ID: "market-research-agent"}
	tools := []ToolDefinition{{Name: "emit_event"}}
	_, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, tools), actor, "system prompt", tools)
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "claude_cli_startup_probe_failed" {
		t.Fatalf("ProbeStartupVisibleToolSurface failure = %#v, want generic connector failure", failure)
	}
}

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_AuthenticationFailureIsCanonical(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' 'OAuth token expired' >&2
exit 1
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",

		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			ToolGateway:         testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				register:   func(context.Context, time.Duration, []string) string { return "ctx-token-auth" },
				unregister: func(string) {},
			},
		})

	actor := runtimeactors.AgentConfig{ID: "market-research-agent"}
	tools := []ToolDefinition{{Name: "emit_event"}}
	_, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, tools), actor, "system prompt", tools)
	assertClaudeAuthenticationFailure(t, err)
}

func managedStartupProbeTestContext(t *testing.T, actor runtimeactors.AgentConfig, tools []ToolDefinition) context.Context {
	t.Helper()
	ctx, surface := testManagedCLISurfaceContext(t, actor, tools)
	return effecttest.New().StartupProbeContext(ctx, surface)
}
