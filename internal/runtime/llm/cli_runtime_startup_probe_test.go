package llm

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestStartupProbeConsumesSessionContractWithoutExecutionFrame(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	capturePath := filepath.Join(tempDir, "startup-input")
	t.Setenv("STARTUP_INPUT_CAPTURE", capturePath)
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
session_id=""
tools_arg=""
allowed_arg=""
system_prompt_arg=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-id)
      shift
      session_id="${1:-}"
      ;;
    --tools)
      shift
      tools_arg="${1:-}"
      ;;
    --allowedTools)
      shift
      allowed_arg="${1:-}"
      ;;
    --system-prompt)
      shift
      system_prompt_arg="${1:-}"
      ;;
    --disallowedTools)
      printf '%s\n' 'unexpected --disallowedTools' >&2
      exit 1
      ;;
  esac
  shift || true
done
if ! printf '%s' "$session_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
  printf '%s\n' 'Error: Invalid session ID. Must be a valid UUID.' >&2
  exit 1
fi
if [ "$tools_arg" != "Edit,ExitPlanMode,Read,Write" ]; then
  printf '%s\n' "unexpected --tools: $tools_arg" >&2
  exit 1
fi
if [ "$allowed_arg" != "Edit,ExitPlanMode,Read,Write" ]; then
  printf '%s\n' "unexpected --allowedTools: $allowed_arg" >&2
  exit 1
fi
if [ "$system_prompt_arg" != "system prompt" ]; then
  printf '%s\n' "unexpected --system-prompt: $system_prompt_arg" >&2
  exit 1
fi
cat >"$STARTUP_INPUT_CAPTURE"
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
	actor.Identity = testAgentIdentity(actor.ID, "")
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
	input, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatalf("read startup input: %v", err)
	}
	if string(input) != "Startup validation probe. Do not call any tools. Reply with the exact text ok." || strings.Contains(string(input), agentframe.Version) || strings.Contains(string(input), `"kind":"initial"`) {
		t.Fatalf("startup input acquired managed execution-frame semantics: %q", input)
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
	actor.Identity = testAgentIdentity(actor.ID, "")
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
	actor.Identity = testAgentIdentity(actor.ID, "")
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
	actor.Identity = testAgentIdentity(actor.ID, "")
	tools := []ToolDefinition{{Name: "emit_event"}}
	_, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, tools), actor, "system prompt", tools)
	assertClaudeAuthenticationFailure(t, err)
}

func TestClaudeCLIRuntimeProbeStartupVisibleToolSurface_IncompatiblePositiveSelectorFailsWithoutRetry(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	countPath := filepath.Join(tempDir, "count")
	script := `#!/bin/sh
set -eu
count_path="${CLAUDE_SELECTOR_COUNT_PATH}"
count=0
if [ -f "$count_path" ]; then count="$(cat "$count_path")"; fi
count=$((count + 1))
printf '%s' "$count" > "$count_path"
case " $* " in
  *" --tools "*) ;;
  *) printf '%s\n' 'missing required --tools selector' >&2; exit 2 ;;
esac
cat >/dev/null
printf '%s\n' 'error: unknown option --tools' >&2
exit 2
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake Docker script: %v", err)
	}
	t.Setenv("CLAUDE_SELECTOR_COUNT_PATH", countPath)

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"
	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",
		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-selector", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{ProviderCredentials: testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")},
	)
	actor := runtimeactors.AgentConfig{ID: "selector-agent", NativeTools: runtimeactors.NativeToolConfig{WebSearch: true}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	_, err := runtime.ProbeStartupVisibleToolSurface(managedStartupProbeTestContext(t, actor, nil), actor, "system prompt", nil)
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "claude_cli_startup_probe_failed" {
		t.Fatalf("ProbeStartupVisibleToolSurface failure = %#v, want canonical incompatible-selector connector failure", failure)
	}
	raw, readErr := os.ReadFile(countPath)
	if readErr != nil {
		t.Fatalf("read invocation count: %v", readErr)
	}
	if got := strings.TrimSpace(string(raw)); got != "1" {
		t.Fatalf("selector attempts = %q, want exactly one with no negative-catalog retry", got)
	}
}

func managedStartupProbeTestContext(t *testing.T, actor runtimeactors.AgentConfig, tools []ToolDefinition) context.Context {
	t.Helper()
	ctx, surface := testManagedCLISurfaceContext(t, actor, tools)
	return effecttest.New().StartupProbeContext(ctx, surface)
}
