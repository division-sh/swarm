package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	workspace "swarm/internal/runtime/workspace"
)

func TestNativeWorkspaceCommandFailsClosedForHostBackend(t *testing.T) {
	exec := &Executor{}
	_, _, exitCode, err := exec.runWorkspaceCommand(context.Background(), &workspace.Target{
		Workdir: t.TempDir(),
		Backend: workspace.BackendHost,
	}, time.Second, "", "sh", "-lc", "true")
	if err == nil || !strings.Contains(err.Error(), "host workspace backend does not support native tool execution yet") {
		t.Fatalf("runWorkspaceCommand error = %v, want host backend fail-closed error", err)
	}
	if exitCode != -1 {
		t.Fatalf("exit code = %d, want -1 for fail-closed host backend", exitCode)
	}
}

func TestNativeFileToolsFailClosedForHostBackendThroughExecutionTarget(t *testing.T) {
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
		execWorkspaceFn: func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
			t.Fatalf("host native file tools must fail closed before workspace command execution")
			return nil, nil, 0, nil
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:          "writer",
		NativeTools: models.NativeToolConfig{FileIO: true},
	})

	_, err := exec.execNativeReadFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/workspace/input.txt"})
	if err == nil || !strings.Contains(err.Error(), "host workspace backend does not support native tool execution yet") {
		t.Fatalf("execNativeReadFile error = %v, want host fail-closed diagnostic", err)
	}

	_, err = exec.execNativeWriteFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/workspace/output.txt", "content": "hello"})
	if err == nil || !strings.Contains(err.Error(), "host workspace backend does not support native tool execution yet") {
		t.Fatalf("execNativeWriteFile error = %v, want host fail-closed diagnostic", err)
	}
}

func TestNativeFallbackToolSurfaceConsumesWorkspaceExecutionTarget(t *testing.T) {
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	}
	actor := models.AgentConfig{
		ID: "host-agent",
		NativeTools: models.NativeToolConfig{
			Bash:      true,
			FileIO:    true,
			WebSearch: true,
		},
	}

	defs := exec.ToolDefinitionsForActorInContext(context.Background(), actor)
	for _, denied := range []string{"bash", "read_file", "write_file"} {
		if containsToolDefinition(defs, denied) {
			t.Fatalf("context definitions contain %q for host backend: %#v", denied, defs)
		}
	}
	if !containsToolDefinition(defs, "web_search") {
		t.Fatalf("context definitions missing web_search, which is not workspace-execution backed: %#v", defs)
	}

	caps := exec.ToolCapabilitiesForActorInContext(context.Background(), actor, []string{"bash", "read_file", "write_file", "web_search"}, nil)
	for _, denied := range []string{"bash", "read_file", "write_file"} {
		cap := caps.ByName[denied]
		if cap.Visible || cap.Callable || !strings.Contains(cap.DenialReason, "host workspace backend does not support native tool execution yet") {
			t.Fatalf("capability %s = %#v, want host workspace execution denial", denied, cap)
		}
	}
	web := caps.ByName["web_search"]
	if !web.Visible || !web.Callable {
		t.Fatalf("web_search capability = %#v, want visible/callable", web)
	}
}

func containsToolDefinition(defs []llm.ToolDefinition, name string) bool {
	for _, def := range defs {
		if strings.TrimSpace(def.Name) == name {
			return true
		}
	}
	return false
}
