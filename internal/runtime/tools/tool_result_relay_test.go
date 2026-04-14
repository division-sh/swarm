package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	workspace "swarm/internal/runtime/workspace"
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
			target: &workspace.Target{Workdir: tmpDir},
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
		if !strings.HasPrefix(chunk, filepath.Join(tmpDir, workspaceToolResultRelayDir)+"/") {
			t.Fatalf("chunk path = %q, want workspace relay path", chunk)
		}
		data, err := os.ReadFile(chunk)
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
}
