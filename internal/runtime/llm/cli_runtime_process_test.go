package llm

import (
	"context"
	"errors"
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
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
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
		t.Fatalf("docker args = %q, want container-reachable MCP gateway URL", got)
	}
}
