package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func testRuntimeSourceProjection(t *testing.T) (*sourceartifact.RuntimeProjection, string) {
	return testRuntimeSourceProjectionNamed(t, "workspace-test")
}

func testRuntimeSourceProjectionNamed(t *testing.T, name string) (*sourceartifact.RuntimeProjection, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: "+name+"\n"), 0o644); err != nil {
		t.Fatalf("write source fixture: %v", err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit source fixture: %v", err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatalf("materialize source fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := projection.Release(); err != nil {
			t.Errorf("release source fixture: %v", err)
		}
	})
	return projection, projection.PrivateRoot()
}

func TestBundleScopedSystemWorkspacesDoNotReusePriorSourceMount(t *testing.T) {
	first, firstRoot := testRuntimeSourceProjectionNamed(t, "first-source")
	second, secondRoot := testRuntimeSourceProjectionNamed(t, "second-source")
	if first.BundleHash() == second.BundleHash() {
		t.Fatal("distinct source fixtures produced the same bundle hash")
	}

	created := map[string]string{}
	runDocker := func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created[args[2]] = strings.Join(args, " ")
		}
		return "", nil
	}
	start := func(projection *sourceartifact.RuntimeProjection) *DockerManager {
		manager := NewDockerManager(nil)
		cfg := DefaultDockerConfig()
		cfg.SourceProjection = projection
		cfg.WorkspaceNetwork = ""
		manager.SetConfig(cfg)
		manager.SetBundleScope(projection.BundleHash())
		manager.SetRunDockerFnForTest(runDocker)
		if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
			t.Fatalf("EnsureSystemWorkspaces(%s): %v", projection.BundleHash(), err)
		}
		return manager
	}

	firstManager := start(first)
	secondManager := start(second)
	for _, pair := range [][3]string{
		{firstManager.cfg.ScaffoldContainer, firstRoot, first.BundleHash()},
		{firstManager.cfg.SystemContainer, firstRoot, first.BundleHash()},
		{secondManager.cfg.ScaffoldContainer, secondRoot, second.BundleHash()},
		{secondManager.cfg.SystemContainer, secondRoot, second.BundleHash()},
	} {
		call := created[pair[0]]
		if call == "" {
			t.Fatalf("bundle-scoped container %q was not created", pair[0])
		}
		if !strings.Contains(call, pair[1]+":"+LogicalSourceMount+":ro") || !strings.Contains(call, pair[2]) {
			t.Fatalf("container %q is not bound to its source projection and hash:\n%s", pair[0], call)
		}
	}
	if firstManager.cfg.ScaffoldContainer == secondManager.cfg.ScaffoldContainer || firstManager.cfg.SystemContainer == secondManager.cfg.SystemContainer {
		t.Fatalf("different source artifacts reused system container names: first=%#v second=%#v", firstManager.cfg, secondManager.cfg)
	}
}

func TestReboundDockerManagerLazySystemWorkspacesMountSelectedSource(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	rebound, err := manager.RebindSourceProjection(projection, source)
	if err != nil {
		t.Fatalf("RebindSourceProjection: %v", err)
	}
	selected := rebound.(*DockerManager)
	var creates [][]string
	selected.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			creates = append(creates, append([]string(nil), args...))
		}
		return "", nil
	})

	for _, workspaceClass := range []string{"scaffold", "system"} {
		creates = nil
		target, err := selected.ResolveWorkspace(context.Background(), models.AgentConfig{WorkspaceClass: workspaceClass})
		if err != nil {
			t.Fatalf("ResolveWorkspace(%s): %v", workspaceClass, err)
		}
		if target == nil || target.Backend != BackendDocker {
			t.Fatalf("ResolveWorkspace(%s) target = %#v", workspaceClass, target)
		}
		joined := flattenDockerCalls(creates)
		wantMount := projectionRoot + ":" + LogicalSourceMount + ":ro"
		if !strings.Contains(joined, wantMount) {
			t.Fatalf("ResolveWorkspace(%s) create args missing selected source mount %q:\n%s", workspaceClass, wantMount, joined)
		}
	}
}

func TestWorkspaceManagersRebindSelectedSourceWithoutMutatingBootLifecycle(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})

	t.Run("host", func(t *testing.T) {
		manager := NewHostManager()
		bootCfg := DefaultHostConfig()
		bootCfg.WorkspaceRoot = t.TempDir()
		manager.SetConfig(bootCfg)

		rebound, err := RebindSourceProjection(manager, projection, source)
		if err != nil {
			t.Fatalf("RebindSourceProjection: %v", err)
		}
		selected, ok := rebound.(*HostManager)
		if !ok {
			t.Fatalf("rebound lifecycle = %T", rebound)
		}
		if manager.cfg.SourceProjection != nil || manager.cfg.BundleHash != "" {
			t.Fatalf("boot manager was mutated: %#v", manager.cfg)
		}
		if selected.cfg.SourceProjection != projection || selected.cfg.BundleHash != projection.BundleHash() {
			t.Fatalf("selected host source binding = %#v", selected.cfg)
		}
		mounts := selected.hostExecutionMounts(t.TempDir(), "")
		if len(mounts) != 2 || mounts[1].LogicalPath != LogicalSourceMount || mounts[1].HostPath != projectionRoot || mounts[1].Access != MountAccessReadOnly {
			t.Fatalf("selected host mounts = %#v", mounts)
		}
	})

	t.Run("docker", func(t *testing.T) {
		manager := NewDockerManager(nil)
		bootCfg := DefaultDockerConfig()
		manager.SetConfig(bootCfg)

		rebound, err := RebindSourceProjection(manager, projection, source)
		if err != nil {
			t.Fatalf("RebindSourceProjection: %v", err)
		}
		selected, ok := rebound.(*DockerManager)
		if !ok {
			t.Fatalf("rebound lifecycle = %T", rebound)
		}
		if manager.cfg.SourceProjection != nil || manager.cfg.BundleHash != "" {
			t.Fatalf("boot manager was mutated: %#v", manager.cfg)
		}
		if selected.cfg.SourceProjection != projection || selected.cfg.BundleHash != projection.BundleHash() {
			t.Fatalf("selected Docker source binding = %#v", selected.cfg)
		}
		if selected.cfg.BundleScope == "" || !strings.Contains(selected.cfg.ScaffoldContainer, selected.cfg.BundleScope) {
			t.Fatalf("selected Docker names are not bundle scoped: %#v", selected.cfg)
		}
		mountArgs := strings.Join(selected.standardMountArgs(), " ")
		if !strings.Contains(mountArgs, projectionRoot+":"+LogicalSourceMount+":ro") {
			t.Fatalf("selected Docker source mount = %q", mountArgs)
		}
	})
}
