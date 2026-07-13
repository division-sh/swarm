package llm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type workspaceResolverStub struct {
	target *workspace.Target
	err    error
}

func beginClaudeTestCompletion(t *testing.T, parent context.Context, request string) (*effecttest.Harness, context.Context, *runtimeeffects.Handle) {
	t.Helper()
	harness := effecttest.New()
	ctx := harness.CompletionContext(t.Name())
	if actor, ok := runtimeactors.ActorFromContext(parent); ok {
		ctx = runtimeactors.WithActor(ctx, actor)
	}
	attempt, err := runtimeeffects.BeginCompletion(ctx, "claude_cli", []byte(request), nil)
	if err != nil {
		t.Fatalf("authorize claude completion: %v", err)
	}
	return harness, ctx, attempt
}

func settleClaudeTestCompletionFailure(t *testing.T, harness *effecttest.Harness, ctx context.Context, attempt *runtimeeffects.Handle, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("claude completion unexpectedly succeeded")
	}
	state, ok := harness.StateForAdapter("claude_cli")
	if !ok || state == runtimeeffects.StateSettled || state == runtimeeffects.StateTerminalFailure || state == runtimeeffects.StateOutcomeUncertain {
		t.Fatalf("low-level completion primitive settled independently: state=%q present=%t", state, ok)
	}
	settleEffectTestCompletionFailure(t, ctx, &completionDispatch{handle: attempt}, err, claudeCompletionFailureState(err))
}

func (s workspaceResolverStub) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.target, nil
}

func TestClaudeCLIRuntimeResolveWorkspace_RequiresResolver(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{
		ID: "campaign-coordinator",
	})

	_, err := runtime.resolveWorkspace(ctx)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}
}

func TestClaudeCLIRuntimeResolveWorkspace_RequiresContainerTarget(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", workspaceResolverStub{}, nil, nil)
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{
		ID: "campaign-coordinator",
	})

	_, err := runtime.resolveWorkspace(ctx)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}
}

func TestClaudeCLIRuntimeContinueSession_RejectsHostFallbackWhenTargetMissing(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	session := &Session{
		ID:      "sess-1",
		AgentID: "campaign-coordinator",
	}

	harness, ctx, attempt := beginClaudeTestCompletion(t, unmanagedLLMTestContext(), "hello")
	_, err := runtime.runWithPreparedInput(ctx, nil, nil, "hello", MonitorTurnMeta{}, attempt)
	settleClaudeTestCompletionFailure(t, harness, ctx, attempt, err)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("expected ErrClaudeWorkspaceRequired, got %v", err)
	}

	_ = session
}

func TestClaudeCLIRuntimeRejectsHostWorkspaceBackend(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	target := &workspace.Target{
		Workdir: t.TempDir(),
		Backend: workspace.BackendHost,
	}

	harness, ctx, attempt := beginClaudeTestCompletion(t, unmanagedLLMTestContext(), "hello")
	_, err := runtime.runWithPreparedInput(ctx, nil, target, "hello", MonitorTurnMeta{}, attempt)
	settleClaudeTestCompletionFailure(t, harness, ctx, attempt, err)
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("runWithInput error = %v, want ErrClaudeWorkspaceRequired", err)
	}
	if !strings.Contains(err.Error(), "host workspace backend does not support Claude CLI execution yet") {
		t.Fatalf("runWithInput error = %v, want host backend fail-closed diagnostic", err)
	}
}

func TestClaudeCLIRuntimeWorkspaceCommandRejectsHostWorkspaceBackend(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	target := &workspace.Target{
		Workdir: t.TempDir(),
		Backend: workspace.BackendHost,
	}

	_, _, exitCode, err := runtime.runWorkspaceCommand(unmanagedLLMTestContext(), target, "", "sh", "-lc", "true")
	if !errors.Is(err, ErrClaudeWorkspaceRequired) {
		t.Fatalf("runWorkspaceCommand error = %v, want ErrClaudeWorkspaceRequired", err)
	}
	if !strings.Contains(err.Error(), "host workspace backend does not support Claude CLI execution yet") {
		t.Fatalf("runWorkspaceCommand error = %v, want host backend fail-closed diagnostic", err)
	}
	if exitCode != 0 {
		t.Fatalf("runWorkspaceCommand exit code = %d, want zero for pre-exec fail-closed path", exitCode)
	}
}

func TestClaudeCLIRuntimeBuildCommand_UsesContainerReachableMCPGatewayURL(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://stale.example.invalid:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "stale-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	runtime.cfg.LLM.ClaudeCLI.Command = "claude"
	runtime.toolGateway = testToolGatewayBinding("http://127.0.0.1:8082", "http://host.docker.internal:8082", "gateway-token")
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "stored-oauth-token")

	cmd, err := runtime.buildCommand(unmanagedLLMTestContext(), []string{"--print", "hello"}, &workspace.Target{
		Backend:   workspace.BackendDocker,
		Container: "swarm-agent-market-research",
		Workdir:   "/workspace",
	})
	if err != nil {
		t.Fatalf("buildCommand: %v", err)
	}
	got := strings.Join(cmd.Args, " ")
	if !strings.Contains(got, "SWARM_TOOL_GATEWAY_URL=http://host.docker.internal:8082/mcp") {
		t.Fatalf("docker args = %q, want explicit container MCP gateway URL", got)
	}
	if strings.Contains(got, "SWARM_TOOL_GATEWAY_TOKEN=") {
		t.Fatalf("docker args = %q, want no gateway token env propagated into cli_test container exec", got)
	}
	if !strings.Contains(got, "CLAUDE_CODE_OAUTH_TOKEN=stored-oauth-token") {
		t.Fatalf("docker args = %q, want provider credential from swarm secrets", got)
	}
	if strings.Contains(got, "stale.example.invalid") || strings.Contains(got, "stale-token") {
		t.Fatalf("docker args = %q, stale operator gateway env leaked into launch", got)
	}
	if strings.Contains(got, "stale-oauth-token") {
		t.Fatalf("docker args = %q, stale provider env leaked into launch", got)
	}
}

func TestClaudeCLIRuntimeRunWithInput_UnstructuredMissingBinaryOutputStaysGeneric(t *testing.T) {
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
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.OutputFormat = "json"
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	harness, ctx, attempt := beginClaudeTestCompletion(t, unmanagedLLMTestContext(), "hello")
	_, err := runtime.runWithPreparedInput(ctx, nil, &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}, "hello", MonitorTurnMeta{}, attempt)
	settleClaudeTestCompletionFailure(t, harness, ctx, attempt, err)
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "claude_cli_process_failed" {
		t.Fatalf("runWithInput failure = %#v, want generic connector failure", failure)
	}
}

func TestClaudeCLIRuntimeRunWithInput_AuthenticationFailureIsCanonical(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tests := []struct {
		name         string
		outputFormat string
		script       string
	}{
		{
			name:         "buffered stderr",
			outputFormat: "json",
			script: `#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' 'Not logged in. Please run /login.' >&2
exit 1
`,
		},
		{
			name:         "streaming stdout",
			outputFormat: "stream-json",
			script: `#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' 'OAuth token expired'
exit 1
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			scriptPath := filepath.Join(tempDir, "fake-docker.sh")
			if err := os.WriteFile(scriptPath, []byte(tt.script), 0o755); err != nil {
				t.Fatalf("write fake docker script: %v", err)
			}

			cfg := &config.Config{}
			cfg.Workspace.DockerBin = scriptPath
			cfg.LLM.ClaudeCLI.Command = "claude"
			cfg.LLM.ClaudeCLI.OutputFormat = tt.outputFormat
			runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
			runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

			harness, ctx, attempt := beginClaudeTestCompletion(t, unmanagedLLMTestContext(), "hello")
			_, err := runtime.runWithPreparedInput(ctx, nil, &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}, "hello", MonitorTurnMeta{}, attempt)
			settleClaudeTestCompletionFailure(t, harness, ctx, attempt, err)
			assertClaudeAuthenticationFailure(t, err)
		})
	}
}

func assertClaudeAuthenticationFailure(t *testing.T, err error) {
	t.Helper()
	failure, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("failure = %v, want canonical failure envelope", err)
	}
	if failure.Failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Failure.Detail.Code != "provider_unauthorized" {
		t.Fatalf("failure = %#v, want authentication_required/provider_unauthorized", failure.Failure)
	}
	if got := failure.Failure.Detail.Attributes["auth_kind"]; got != "provider_credential" {
		t.Fatalf("auth_kind = %#v, want provider_credential", got)
	}
	if got := failure.Failure.Detail.Attributes["provider"]; got != "claude" {
		t.Fatalf("provider = %#v, want claude", got)
	}
	if strings.Contains(strings.ToLower(err.Error()), "not logged in") || strings.Contains(strings.ToLower(err.Error()), "oauth token") {
		t.Fatalf("failure presentation leaks raw provider output: %v", err)
	}
}

func TestClaudeCLIRuntimePersistOversizedToolResultRelay_WritesWorkspaceVisibleFile(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", workspaceResolverStub{
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
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{ID: "market-research-agent"})

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
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", workspaceResolverStub{
		target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"},
	}, nil, nil)

	runtime.execWorkspaceFn = func(context.Context, *workspace.Target, string, ...string) ([]byte, []byte, int, error) {
		return nil, []byte("permission denied"), 1, errors.New("exit 1")
	}
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{ID: "market-research-agent"})

	_, err := runtime.PersistOversizedToolResultRelay(ctx, &Session{ID: "sess-1"}, "sql_execute", []byte(`{"blob":"hello"}`))
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "workspace_tool_result_relay_write_failed" {
		t.Fatalf("PersistOversizedToolResultRelay failure = %#v, want typed connector failure", failure)
	}
}

func TestEffectiveCLITimeoutForConfigIgnoresRetiredEnvAndPreservesActorFloor(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_TIMEOUT_SECONDS", "1")
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.Timeout = 45 * time.Second

	if got := effectiveCLITimeoutForConfig(unmanagedLLMTestContext(), cfg); got != 45*time.Second {
		t.Fatalf("timeout without actor = %v, want config timeout", got)
	}

	globalActorCtx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{ID: "global-agent"})
	if got := effectiveCLITimeoutForConfig(globalActorCtx, cfg); got != 300*time.Second {
		t.Fatalf("timeout for global/no-entity actor = %v, want floor", got)
	}

	entityActorCtx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{ID: "entity-agent", EntityID: "customer-1"})
	if got := effectiveCLITimeoutForConfig(entityActorCtx, cfg); got != 45*time.Second {
		t.Fatalf("timeout for entity actor = %v, want config timeout", got)
	}
}
