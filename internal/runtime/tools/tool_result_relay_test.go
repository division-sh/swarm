package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type relayWorkspaceResolverStub struct {
	target *workspace.Target
}

func (s relayWorkspaceResolverStub) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return s.target, nil
}

func TestExecutorPersistOversizedToolResultRelay_DockerChunksLargeReadFileResultsThroughWorkspaceCommand(t *testing.T) {
	tmpDir := t.TempDir()
	var commandCalls int
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendDocker, Container: "swarm-agent", Workdir: "/workspace"},
		},
		execWorkspaceFn: func(_ context.Context, execTarget workspace.ExecutionTarget, _ time.Duration, stdin string, args ...string) ([]byte, []byte, int, error) {
			commandCalls++
			if execTarget.Mode != workspace.ExecutionModeDockerContainer {
				t.Fatalf("exec target mode = %s, want docker_container", execTarget.Mode)
			}
			if len(args) == 0 {
				return nil, []byte("missing args"), 1, nil
			}
			relayPath := args[len(args)-1]
			hostPath := filepath.Join(tmpDir, strings.TrimPrefix(relayPath, "/workspace/"))
			if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
				return nil, []byte(err.Error()), 1, nil
			}
			if err := os.WriteFile(hostPath, []byte(stdin), 0o644); err != nil {
				return nil, []byte(err.Error()), 1, nil
			}
			return nil, nil, 0, nil
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{ID: "market-research-agent"})
	raw, err := json.Marshal(map[string]any{
		"content":    strings.Repeat("x", toolResultRelayChunkBytes+512),
		"size_bytes": toolResultRelayChunkBytes + 512,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	relay, err := exec.PersistOversizedToolResultRelay(ctx, "read_file", raw)
	if err != nil {
		t.Fatalf("PersistOversizedToolResultRelay: %v", err)
	}
	if len(relay.Chunks) < 2 {
		t.Fatalf("relay chunks = %#v, want multiple chunk paths", relay.Chunks)
	}
	if relay.Path != "" {
		t.Fatalf("relay path = %q, want empty for chunked read_file relay", relay.Path)
	}
	for _, chunk := range relay.Chunks {
		if !strings.HasPrefix(chunk, "/workspace/"+workspaceToolResultRelayDir+"/") {
			t.Fatalf("chunk path = %q, want logical workspace relay path", chunk)
		}
		hostChunk := filepath.Join(tmpDir, strings.TrimPrefix(chunk, "/workspace/"))
		data, err := os.ReadFile(hostChunk)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", hostChunk, err)
		}
		if len(data) == 0 {
			t.Fatalf("chunk %q is empty", chunk)
		}
		if len(data) > toolResultRelayChunkBytes {
			t.Fatalf("chunk %q length = %d, want <= %d", chunk, len(data), toolResultRelayChunkBytes)
		}
	}
	if commandCalls == 0 {
		t.Fatalf("docker relay did not use workspace command path")
	}
}

func TestExecutorPersistOversizedToolResultRelay_HostUsesExecutionTargetFileIO(t *testing.T) {
	exec, actor, target := newHostRelayExecutor(t)
	exec.execWorkspaceFn = func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
		t.Fatalf("host relay must use direct platform file I/O, not workspace command execution")
		return nil, nil, 0, nil
	}
	ctx := models.WithActor(context.Background(), actor)

	relay, err := exec.PersistOversizedToolResultRelay(ctx, "sql_execute", []byte(`{"blob":"hello"}`))
	if err != nil {
		t.Fatalf("PersistOversizedToolResultRelay: %v", err)
	}
	if relay.ReadTool != "read_file" || relay.Format != "json" || relay.Visibility != "workspace_mount" {
		t.Fatalf("relay = %#v, want read_file json workspace_mount", relay)
	}
	if !strings.HasPrefix(relay.Path, "/workspace/"+workspaceToolResultRelayDir+"/") || !strings.HasSuffix(relay.Path, ".json") {
		t.Fatalf("relay path = %q, want logical workspace relay path", relay.Path)
	}
	if strings.Contains(relay.Path, target.Workdir) {
		t.Fatalf("relay path leaked host workdir: %q", relay.Path)
	}
	resolved, err := target.ExecutionTarget().ResolveHostPath(relay.Path, workspace.PathAccessRead)
	if err != nil {
		t.Fatalf("ResolveHostPath relay: %v", err)
	}
	if data, err := os.ReadFile(resolved.HostPath); err != nil || string(data) != `{"blob":"hello"}` {
		t.Fatalf("host relay file = %q err=%v", data, err)
	}
	read, err := exec.Execute(ctx, "read_file", map[string]any{"path": relay.Path})
	if err != nil {
		t.Fatalf("Execute read_file relay path: %v", err)
	}
	if got := read.(map[string]any)["content"]; got != `{"blob":"hello"}` {
		t.Fatalf("read relay content = %#v", got)
	}
}

func TestExecutorPersistOversizedToolResultRelay_HostChunksLargeReadFileResults(t *testing.T) {
	exec, actor, target := newHostRelayExecutor(t)
	exec.execWorkspaceFn = func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
		t.Fatalf("host chunk relay must use direct platform file I/O, not workspace command execution")
		return nil, nil, 0, nil
	}
	ctx := models.WithActor(context.Background(), actor)
	raw, err := json.Marshal(map[string]any{
		"content":    strings.Repeat("x", toolResultRelayChunkBytes+512),
		"size_bytes": toolResultRelayChunkBytes + 512,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	relay, err := exec.PersistOversizedToolResultRelay(ctx, "read_file", raw)
	if err != nil {
		t.Fatalf("PersistOversizedToolResultRelay: %v", err)
	}
	if len(relay.Chunks) < 2 {
		t.Fatalf("relay chunks = %#v, want multiple chunk paths", relay.Chunks)
	}
	for _, chunk := range relay.Chunks {
		if !strings.HasPrefix(chunk, "/workspace/"+workspaceToolResultRelayDir+"/") || strings.Contains(chunk, target.Workdir) {
			t.Fatalf("chunk path = %q, want logical workspace path without host backing path", chunk)
		}
		resolved, err := target.ExecutionTarget().ResolveHostPath(chunk, workspace.PathAccessRead)
		if err != nil {
			t.Fatalf("ResolveHostPath(%q): %v", chunk, err)
		}
		data, err := os.ReadFile(resolved.HostPath)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", chunk, err)
		}
		if len(data) == 0 {
			t.Fatalf("chunk %q is empty", chunk)
		}
		if len(data) > toolResultRelayChunkBytes {
			t.Fatalf("chunk %q length = %d, want <= %d", chunk, len(data), toolResultRelayChunkBytes)
		}
	}
	read, err := exec.Execute(ctx, "read_file", map[string]any{"path": relay.Chunks[0]})
	if err != nil {
		t.Fatalf("Execute read_file first chunk: %v", err)
	}
	if got := read.(map[string]any)["size_bytes"]; got == 0 {
		t.Fatalf("read first chunk result = %#v, want non-empty", read)
	}
}

func TestExecutorPersistOversizedToolResultRelay_HostWithoutBackingMountFailsClosed(t *testing.T) {
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
		execWorkspaceFn: func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
			t.Fatalf("host relay must not shell through workspace command execution")
			return nil, nil, 0, nil
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{ID: "market-research-agent"})

	_, err := exec.PersistOversizedToolResultRelay(ctx, "sql_execute", []byte(`{"blob":"hello"}`))
	if err == nil || !strings.Contains(err.Error(), "host backing path for /workspace is unavailable") {
		t.Fatalf("PersistOversizedToolResultRelay error = %v, want host backing path fail-closed diagnostic", err)
	}
}

func newHostRelayExecutor(t *testing.T) (*Executor, models.AgentConfig, *workspace.Target) {
	t.Helper()
	ctx := context.Background()
	workspaceRoot := filepath.Join(t.TempDir(), "host-workspaces")
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := workspace.NewHostManager(nil)
	manager.SetConfig(workspace.HostConfig{
		WorkspaceRoot:       workspaceRoot,
		SharedDataSource:    dataDir,
		DataMountPoint:      workspace.LogicalDataMount,
		ContractsSource:     contractsDir,
		ContractsMountPoint: workspace.LogicalContractsMount,
	})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := manager.ValidateSource(ctx, source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsurePrereqs(ctx); err != nil {
		t.Fatalf("EnsurePrereqs: %v", err)
	}
	actor := models.AgentConfig{
		ID:          "market-research-agent",
		NativeTools: models.NativeToolConfig{FileIO: true, Bash: true},
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		ModelRuntime:      nativeCapabilityRuntimeStub{},
		WorkspaceResolver: manager,
	})
	return exec, actor, target
}
