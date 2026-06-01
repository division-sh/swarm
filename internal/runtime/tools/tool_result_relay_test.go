package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type relayWorkspaceResolverStub struct {
	target *workspace.Target
}

func (s relayWorkspaceResolverStub) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return s.target, nil
}

func TestExecutorPersistOversizedToolResultRelay_ChunksLargeReadFileResults(t *testing.T) {
	tmpDir := t.TempDir()
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendDocker, Container: "swarm-agent", Workdir: "/workspace"},
		},
		execWorkspaceFn: func(_ context.Context, _ workspace.ExecutionTarget, _ time.Duration, stdin string, args ...string) ([]byte, []byte, int, error) {
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
}

func TestExecutorPersistOversizedToolResultRelay_FailsClosedForHostBackend(t *testing.T) {
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
		execWorkspaceFn: func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
			t.Fatalf("host relay must fail closed before workspace command execution")
			return nil, nil, 0, nil
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{ID: "market-research-agent"})

	_, err := exec.PersistOversizedToolResultRelay(ctx, "sql_execute", []byte(`{"blob":"hello"}`))
	if err == nil || !strings.Contains(err.Error(), "host workspace backend does not support tool result relay yet") {
		t.Fatalf("PersistOversizedToolResultRelay error = %v, want host relay fail-closed diagnostic", err)
	}
}
