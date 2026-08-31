package workspace

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestDurableWorkspaceBackingKeySeparatesExactOwnerTuple(t *testing.T) {
	firstHash := sourceartifact.HashPrefix + "123456789abc" + strings.Repeat("0", 52)
	secondHash := sourceartifact.HashPrefix + "123456789abc" + strings.Repeat("1", 52)
	if firstHash == secondHash {
		t.Fatal("collision fixture hashes must differ")
	}

	fixtures := []struct {
		kind     durableWorkspaceKind
		identity string
	}{
		{durableWorkspaceScaffold, ""},
		{durableWorkspaceSystem, ""},
		{durableWorkspaceSystemEntity, ""},
		{durableWorkspaceSystemNginx, ""},
		{durableWorkspaceSystemSystemd, ""},
		{durableWorkspaceAgent, strings.Repeat("a", 64)},
		{durableWorkspaceFlow, "a/b"},
	}
	for _, fixture := range fixtures {
		first := durableWorkspaceKeyForTest(t, firstHash, fixture.kind, fixture.identity)
		second := durableWorkspaceKeyForTest(t, secondHash, fixture.kind, fixture.identity)
		if first == second {
			t.Fatalf("distinct full bundle hashes collapsed for %s: %q", fixture.kind, first)
		}
	}

	firstScope, err := durableBundleScopeKey(firstHash)
	if err != nil {
		t.Fatal(err)
	}
	secondScope, err := durableBundleScopeKey(secondHash)
	if err != nil {
		t.Fatal(err)
	}
	if firstScope == secondScope || !strings.Contains(firstScope, strings.TrimPrefix(firstHash, sourceartifact.HashPrefix)) {
		t.Fatalf("durable bundle scopes are not lossless: first=%q second=%q", firstScope, secondScope)
	}
	root := t.TempDir()
	firstHost := NewHostManager()
	firstHost.SetConfig(HostConfig{WorkspaceRoot: root, BundleScope: firstScope})
	secondHost := NewHostManager()
	secondHost.SetConfig(HostConfig{WorkspaceRoot: root, BundleScope: secondScope})
	firstRoot, err := firstHost.hostRoot()
	if err != nil {
		t.Fatal(err)
	}
	secondRoot, err := secondHost.hostRoot()
	if err != nil {
		t.Fatal(err)
	}
	if firstRoot == secondRoot {
		t.Fatalf("distinct full bundle hashes share host workspace root %q", firstRoot)
	}
}

func TestFlowWorkspaceBackingKeysAreInjectiveAndBackendNeutral(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {Value: map[string]any{
				"shared": map[string]any{"workspace_scope": "per-flow-instance"},
			}},
		}},
	})

	docker := NewDockerManager()
	dockerConfig := DefaultDockerConfig()
	dockerConfig.SourceProjection = projection
	dockerConfig.WorkspaceNetwork = ""
	docker.SetConfig(dockerConfig)
	docker.SetSemanticSource(source)
	bindTestDockerProjection(t, docker, projection)
	docker.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if args[0] == "inspect" {
			return "", fmt.Errorf("no such object")
		}
		return "", nil
	})

	host := NewHostManager()
	host.SetConfig(HostConfig{
		WorkspaceRoot: t.TempDir(), SourceProjection: projection, SourceMountPoint: LogicalSourceMount,
	})
	host.SetSemanticSource(source)
	bindTestHostProjection(t, host, projection)

	seen := map[string]string{}
	for index, flowPath := range []string{"a/b", "a-b", "a_b"} {
		identity := runtimeagentidentitytest.Declared(t, "worker", "test/agents.yaml", flowPath, fmt.Sprintf("i-%d", index), flowPath)
		actor := models.AgentConfig{
			ExecutionMode: "live", ID: "worker", Identity: identity, FlowPath: flowPath, WorkspaceClass: "shared",
		}
		_, dockerVolume, err := docker.workspaceContainerAndVolume("per-flow-instance", flowPath, actor)
		if err != nil {
			t.Fatalf("docker workspace %q: %v", flowPath, err)
		}
		hostTarget, err := host.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
		if err != nil {
			t.Fatalf("host workspace %q: %v", flowPath, err)
		}
		if got := filepath.Base(hostTarget.Workdir); got != dockerVolume {
			t.Fatalf("backend backing-key drift for %q: host=%q docker=%q", flowPath, got, dockerVolume)
		}
		if previous, exists := seen[dockerVolume]; exists {
			t.Fatalf("flow identities %q and %q share backing key %q", previous, flowPath, dockerVolume)
		}
		seen[dockerVolume] = flowPath
	}
}

func TestRuntimeWorkspaceContainersUseCurrentProjectionInventory(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	manager := NewDockerManager()
	cfg := DefaultDockerConfig()
	cfg.SourceProjection = projection
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	bindTestDockerProjection(t, manager, projection)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if args[0] == "inspect" {
			return "", fmt.Errorf("no such object")
		}
		return "", nil
	})

	actor := models.AgentConfig{
		ExecutionMode: "live", ID: "worker", Identity: runtimeagentidentitytest.RootDeclared(t, "worker", "test/agents.yaml"),
	}
	target, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatal(err)
	}
	containers, err := manager.RuntimeWorkspaceContainers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		manager.cfg.ScaffoldContainer: false,
		manager.cfg.SystemContainer:   false,
		target.Container:              false,
	}
	for _, container := range containers {
		if _, ok := want[container]; ok {
			want[container] = true
		}
	}
	for container, found := range want {
		if !found {
			t.Fatalf("current projection container %q missing from inventory %v", container, containers)
		}
	}
}
