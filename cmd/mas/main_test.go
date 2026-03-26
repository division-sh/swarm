package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimebus "empireai/internal/runtime/bus"
	"empireai/internal/runtime/sessions"
)

func TestDefaultContractsPathExists(t *testing.T) {
	path := filepath.Join(repoRoot(), defaultContractsPath, "package.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default contracts path missing package.yaml: %v", err)
	}
}

func TestRuntimeProjectSupervisor_RejectsInvalidBuilderProjectContracts(t *testing.T) {
	ctx := context.Background()
	cfg, err := defaultRuntimeConfig()
	if err != nil {
		t.Fatalf("defaultRuntimeConfig: %v", err)
	}
	stores := storeBundle{
		EventStore:      runtimebus.InMemoryEventStore{},
		SessionRegistry: sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL),
	}

	projectRoot := t.TempDir()
	writeTempProjectFile(t, projectRoot, ".builder-config/graph.json", "{}\n")
	writeTempProjectFile(t, projectRoot, "package.yaml", "name: invalid-builder\nversion: 0.1.0\nplatform_version: 0.1.0\nflows: []\n")
	writeTempProjectFile(t, projectRoot, "schema.yaml", "initial_state: idle\nterminal_states: []\nstates: [idle]\npins:\n  inputs:\n    events: [loop.event]\n  outputs:\n    events: [loop.event]\n")
	writeTempProjectFile(t, projectRoot, "agents.yaml", "{}\n")
	writeTempProjectFile(t, projectRoot, "tools.yaml", "{}\n")
	writeTempProjectFile(t, projectRoot, "policy.yaml", "{}\n")
	writeTempProjectFile(t, projectRoot, "events.yaml", "loop.event:\n  payload: {}\n")
	writeTempProjectFile(t, projectRoot, "nodes.yaml", strings.Join([]string{
		"loop-node:",
		"  execution_type: system_node",
		"  subscribes_to:",
		"    - loop.event",
		"  produces:",
		"    - loop.event",
		"  event_handlers:",
		"    loop.event:",
		"      emits: loop.event",
		"",
	}, "\n"))

	supervisor := newRuntimeProjectSupervisor(
		repoRoot(),
		filepath.Join(repoRoot(), defaultPlatformSpecPath),
		cfg,
		stores,
		nil,
		"",
		nil,
		nil,
		nil,
	)

	_, err = supervisor.OpenProject(ctx, projectRoot)
	if err == nil {
		t.Fatal("expected invalid builder project to be rejected")
	}
	if !strings.Contains(err.Error(), "emits its own trigger event") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func writeTempProjectFile(t *testing.T, root string, relativePath string, contents string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(fullPath), err)
	}
	if err := os.WriteFile(fullPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", fullPath, err)
	}
}
