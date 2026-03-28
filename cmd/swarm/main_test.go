package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/events"
	"swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/sessions"
)

func TestResolveContractsPath_RequiresExplicitWorkflowContracts(t *testing.T) {
	path := resolveContractsPath(repoRoot(), "")
	if strings.TrimSpace(path) != "" {
		t.Fatalf("expected no implicit contracts path, got %q", path)
	}
}

func TestResolveContractsPath_UsesMASContractsDirOverride(t *testing.T) {
	t.Setenv("SWARM_CONTRACTS_DIR", filepath.Join(repoRoot(), "internal", "runtime", "testdata", "generic-swarm-bundle"))
	path := resolveContractsPath(repoRoot(), "")
	if strings.TrimSpace(path) == "" {
		t.Fatal("expected contracts path from SWARM_CONTRACTS_DIR")
	}
	if _, err := os.Stat(filepath.Join(path, "package.yaml")); err != nil {
		t.Fatalf("contracts path missing package.yaml: %v", err)
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

func TestDashboardDynamicRuntimeControl_ResetStatePublishesPlatformReset(t *testing.T) {
	bus := &recordingManagerBus{}
	control := dashboardDynamicRuntimeControl{
		supervisor: &runtimeProjectSupervisor{
			currentRT: &runtime.Runtime{
				Manager: runtimemanager.NewAgentManager(bus, nil),
			},
		},
	}

	if err := control.ResetState(); err != nil {
		t.Fatalf("ResetState: %v", err)
	}

	if len(bus.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(bus.published))
	}
	if evt := bus.published[0]; evt.Type != events.EventType("platform.reset") {
		t.Fatalf("event type = %s, want platform.reset", evt.Type)
	}
}

type recordingManagerBus struct {
	published []events.Event
}

func (b *recordingManagerBus) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return nil
}

func (*recordingManagerBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (*recordingManagerBus) Subscribe(string, ...events.EventType) <-chan events.Event   { return make(chan events.Event) }
func (*recordingManagerBus) Unsubscribe(string)                                           {}
func (*recordingManagerBus) Store() runtimebus.EventStore                                 { return runtimebus.InMemoryEventStore{} }
func (*recordingManagerBus) ResetInMemoryState() error                                    { return nil }
func (*recordingManagerBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry)   {}

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
