package llm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

type workspaceResolverStub struct {
	target *workspace.Target
	err    error
}

func (s workspaceResolverStub) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.target, nil
}

func TestClaudeCLIRuntimeResolveWorkspace_RequiresResolver(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "campaign-coordinator",
	})

	_, err := runtime.resolveWorkspace(ctx)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}
}

func TestClaudeCLIRuntimeResolveWorkspace_RequiresContainerTarget(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, workspaceResolverStub{}, nil, nil)
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "campaign-coordinator",
	})

	_, err := runtime.resolveWorkspace(ctx)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}
}

func TestClaudeCLIRuntimeContinueSession_RejectsHostFallbackWhenTargetMissing(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)
	session := &Session{
		ID:      "sess-1",
		AgentID: "campaign-coordinator",
	}

	_, err := runtime.runWithInput(context.Background(), nil, nil, "hello", MonitorTurnMeta{})
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}

	_ = session
}

func TestClaudeCLIRuntimeBuildCommand_UsesContainerReachableMCPGatewayURL(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)
	runtime.cfg.LLM.ClaudeCLI.Command = "claude"

	cmd := runtime.buildCommand(context.Background(), []string{"--print", "hello"}, &workspace.Target{
		Container: "swarm-agent-market-research",
		Workdir:   "/workspace",
	})
	got := strings.Join(cmd.Args, " ")
	if !strings.Contains(got, "SWARM_TOOL_GATEWAY_URL=http://host.docker.internal:8081/mcp") {
		t.Fatalf("docker args = %q, want explicit container MCP gateway URL", got)
	}
	if !strings.Contains(got, "SWARM_TOOL_GATEWAY_TOKEN=gateway-token") {
		t.Fatalf("docker args = %q, want MCP gateway token propagated into cli_test container exec", got)
	}
}

func TestClaudeCLIRuntimeRunWithInput_MissingWorkspaceCLIUsesActionableDiagnostic(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_WORKSPACE_IMAGE", "swarm-workspace:test")

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
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.OutputFormat = "json"
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)

	_, err := runtime.runWithInput(context.Background(), nil, &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}, "hello", MonitorTurnMeta{})
	if !errors.Is(err, ErrClaudeWorkspaceCLIUnavailable) {
		t.Fatalf("runWithInput error = %v, want ErrClaudeWorkspaceCLIUnavailable", err)
	}
	for _, want := range []string{
		"local cli_test workspace cannot execute configured Claude CLI command",
		`"swarm-agent-market-research"`,
		`"swarm-workspace:test"`,
		"build or pull a workspace image",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("runWithInput error missing %q:\n%v", want, err)
		}
	}
}

func TestWorkspaceCLIDiagnosticError_MatchesAbsolutePathNoSuchFile(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_IMAGE", "swarm-workspace:absolute")
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.Command = "/usr/local/bin/claude"

	err := workspaceCLIDiagnosticError(cfg, &workspace.Target{Container: "swarm-agent-market-research"}, `OCI runtime exec failed: exec failed: unable to start container process: exec: "/usr/local/bin/claude": stat /usr/local/bin/claude: no such file or directory: unknown`)
	if !errors.Is(err, ErrClaudeWorkspaceCLIUnavailable) {
		t.Fatalf("workspaceCLIDiagnosticError error = %v, want ErrClaudeWorkspaceCLIUnavailable", err)
	}
	for _, want := range []string{`"/usr/local/bin/claude"`, `"swarm-agent-market-research"`, `"swarm-workspace:absolute"`, "set SWARM_WORKSPACE_IMAGE"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("workspaceCLIDiagnosticError error missing %q:\n%v", want, err)
		}
	}
}

func TestClaudeCLIRuntimePersistOversizedToolResultRelay_WritesWorkspaceVisibleFile(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, workspaceResolverStub{
		target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"},
	}, nil, nil)
	var gotTarget *workspace.Target
	var gotStdin string
	var gotArgs []string
	runtime.execWorkspaceFn = func(_ context.Context, target *workspace.Target, stdin string, args ...string) ([]byte, []byte, int, error) {
		gotTarget = target
		gotStdin = stdin
		gotArgs = append([]string(nil), args...)
		return nil, nil, 0, nil
	}
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "market-research-agent"})

	relay, err := runtime.PersistOversizedToolResultRelay(ctx, &Session{ID: "sess-1"}, "sql_execute", []byte(`{"blob":"hello"}`))
	if err != nil {
		t.Fatalf("PersistOversizedToolResultRelay err = %v", err)
	}
	if gotTarget == nil || gotTarget.Container != "swarm-agent-market-research" {
		t.Fatalf("got target = %#v", gotTarget)
	}
	if relay.ReadTool != "read_file" || relay.Format != "json" {
		t.Fatalf("relay = %#v", relay)
	}
	if !strings.HasPrefix(relay.Path, "/workspace/.swarm/tool-results/sess-1/sql_execute-") || !strings.HasSuffix(relay.Path, ".json") {
		t.Fatalf("relay path = %q", relay.Path)
	}
	if gotStdin != `{"blob":"hello"}` {
		t.Fatalf("relay stdin = %q", gotStdin)
	}
	if len(gotArgs) == 0 || gotArgs[len(gotArgs)-1] != relay.Path {
		t.Fatalf("workspace args = %#v, want relay path suffix", gotArgs)
	}
}

func TestClaudeCLIRuntimePersistOversizedToolResultRelay_PropagatesWorkspaceWriteFailure(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, workspaceResolverStub{
		target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"},
	}, nil, nil)
	runtime.execWorkspaceFn = func(context.Context, *workspace.Target, string, ...string) ([]byte, []byte, int, error) {
		return nil, []byte("permission denied"), 1, errors.New("exit 1")
	}
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "market-research-agent"})

	_, err := runtime.PersistOversizedToolResultRelay(ctx, &Session{ID: "sess-1"}, "sql_execute", []byte(`{"blob":"hello"}`))
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("PersistOversizedToolResultRelay err = %v, want permission denied", err)
	}
}
