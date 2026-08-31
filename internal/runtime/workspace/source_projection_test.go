package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	var removed []string
	runDocker := func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created[args[2]] = strings.Join(args, " ")
		case "rm":
			removed = append(removed, args[len(args)-1])
		}
		return "", nil
	}
	start := func(projection *sourceartifact.RuntimeProjection) *DockerManager {
		manager := NewDockerManager(nil)
		cfg := DefaultDockerConfig()
		cfg.SourceProjection = projection
		cfg.WorkspaceNetwork = ""
		manager.SetConfig(cfg)
		if err := manager.BindSourceProjection(projection); err != nil {
			t.Fatalf("BindSourceProjection(%s): %v", projection.BundleHash(), err)
		}
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
	if err := firstManager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := secondManager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 4 {
		t.Fatalf("released containers = %#v, want both system containers for both projections", removed)
	}
}

func TestSameBundleHashUsesDistinctProcessProjectionContainersForRunningAndStoppedPredecessors(t *testing.T) {
	for _, predecessorRunning := range []bool{true, false} {
		t.Run(fmt.Sprintf("predecessor_running_%t", predecessorRunning), func(t *testing.T) {
			first, _ := testRuntimeSourceProjectionNamed(t, "same-source")
			second, _ := testRuntimeSourceProjectionNamed(t, "same-source")
			if first.BundleHash() != second.BundleHash() || first.Identity() == second.Identity() {
				t.Fatalf("same artifact projection identities = first %s/%s second %s/%s", first.BundleHash(), first.Identity(), second.BundleHash(), second.Identity())
			}
			bind := func(projection *sourceartifact.RuntimeProjection) *DockerManager {
				manager := NewDockerManager(nil)
				cfg := DefaultDockerConfig()
				cfg.SourceProjection = projection
				cfg.WorkspaceNetwork = ""
				manager.SetConfig(cfg)
				if err := manager.BindSourceProjection(projection); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = manager.ReleaseSourceProjection(context.Background()) })
				return manager
			}
			firstManager := bind(first)
			secondManager := bind(second)
			if firstManager.cfg.ScaffoldContainer == secondManager.cfg.ScaffoldContainer || firstManager.cfg.SystemContainer == secondManager.cfg.SystemContainer {
				t.Fatalf("same-hash projections reused process container names: first=%#v second=%#v", firstManager.cfg, secondManager.cfg)
			}
			oldNames := map[string]bool{
				firstManager.cfg.ScaffoldContainer: predecessorRunning,
				firstManager.cfg.SystemContainer:   predecessorRunning,
			}
			var created []string
			secondManager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
				switch args[0] {
				case "inspect":
					if running, ok := oldNames[args[len(args)-1]]; ok {
						return fmt.Sprintf("%t", running), nil
					}
					return "", fmt.Errorf("no such object")
				case "create":
					created = append(created, args[2])
				}
				return "", nil
			})
			if err := secondManager.EnsureSystemWorkspaces(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(created) != 2 || created[0] == firstManager.cfg.ScaffoldContainer || created[1] == firstManager.cfg.SystemContainer {
				t.Fatalf("created containers = %#v, want only fresh process projection names", created)
			}
		})
	}
}

func TestDockerManagerRejectsRepeatedSourceProjectionBinding(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	manager := NewDockerManager(nil)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatalf("first BindSourceProjection: %v", err)
	}
	if err := manager.BindSourceProjection(projection); err == nil {
		t.Fatal("second BindSourceProjection error = nil")
	}
	if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatalf("ReleaseSourceProjection: %v", err)
	}
	if _, err := os.Stat(projectionRoot); err != nil {
		t.Fatalf("caller projection root removed before caller release: %v", err)
	}
}

func TestReleaseSourceProjectionRemovesOwnedContainersAfterPartialLaunch(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SourceProjection = projection
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatal(err)
	}
	var calls []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, strings.Join(args, " "))
		if args[0] == "inspect" {
			return "", fmt.Errorf("no such object")
		}
		if args[0] == "start" {
			return "", fmt.Errorf("start failed")
		}
		return "", nil
	})
	if err := manager.EnsureSystemWorkspaces(context.Background()); err == nil || !strings.Contains(err.Error(), "start failed") {
		t.Fatalf("EnsureSystemWorkspaces error = %v, want partial-launch failure", err)
	}
	if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(calls, "\n")
	if !strings.Contains(joined, "rm --force "+manager.cfg.ScaffoldContainer) {
		t.Fatalf("partial launch container was not removed before source release:\n%s", joined)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err == nil || !strings.Contains(err.Error(), "is released") {
		t.Fatalf("post-release restart error = %v, want fail-closed projection release", err)
	}
}

func TestReleaseSourceProjectionWaitsForAdmittedContainerLaunchBeforeTeardown(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SourceProjection = projection
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatal(err)
	}
	createStarted := make(chan struct{})
	allowCreate := make(chan struct{})
	var removed []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			close(createStarted)
			<-allowCreate
		case "rm":
			removed = append(removed, args[len(args)-1])
		}
		return "", nil
	})
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- manager.EnsureSystemWorkspaces(context.Background()) }()
	<-createStarted
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- manager.ReleaseSourceProjection(context.Background()) }()
	releaseStartDeadline := time.After(time.Second)
	for {
		manager.projectionMu.Lock()
		releaseStarted := manager.projectionReleased
		manager.projectionMu.Unlock()
		if releaseStarted {
			break
		}
		select {
		case <-releaseStartDeadline:
			t.Fatal("release did not install its projection fence")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-releaseDone:
		t.Fatalf("release completed before admitted launch settled: %v", err)
	default:
	}
	close(allowCreate)
	if err := <-ensureDone; err == nil || !strings.Contains(err.Error(), "is released") {
		t.Fatalf("EnsureSystemWorkspaces error = %v, want remaining launch rejected after release began", err)
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("ReleaseSourceProjection: %v", err)
	}
	if len(removed) != 1 || removed[0] != manager.cfg.ScaffoldContainer {
		t.Fatalf("removed containers = %#v, want the admitted partial launch", removed)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err == nil || !strings.Contains(err.Error(), "is released") {
		t.Fatalf("post-release launch error = %v, want released projection rejection", err)
	}
}

func TestReleaseSourceProjectionReconcilesCreateAndRemoveAcknowledgmentLoss(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SourceProjection = projection
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatal(err)
	}
	createAttempted := false
	removeAttempted := false
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			createAttempted = true
			return "", fmt.Errorf("create acknowledgment lost")
		case "rm":
			removeAttempted = true
			return "", fmt.Errorf("remove acknowledgment lost")
		}
		return "", nil
	})
	if err := manager.EnsureSystemWorkspaces(context.Background()); err == nil || !strings.Contains(err.Error(), "acknowledgment lost") {
		t.Fatalf("EnsureSystemWorkspaces error = %v, want create acknowledgment loss", err)
	}
	if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatalf("ReleaseSourceProjection reconciled error = %v", err)
	}
	if !createAttempted || !removeAttempted {
		t.Fatalf("acknowledgment proof attempts = create:%t remove:%t", createAttempted, removeAttempted)
	}
	if _, err := os.Stat(projectionRoot); err != nil {
		t.Fatalf("caller-owned projection disappeared before caller release: %v", err)
	}
}

func TestReleaseSourceProjectionRetainsFilesystemWhileContainerRemovalIsUncertain(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SourceProjection = projection
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatal(err)
	}
	containers := map[string]bool{}
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			if containers[args[len(args)-1]] {
				return "true", nil
			}
			return "", fmt.Errorf("no such object")
		case "create":
			containers[args[2]] = true
		case "rm":
			return "", fmt.Errorf("remove state uncertain")
		}
		return "", nil
	})
	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseSourceProjection(context.Background()); err == nil || !strings.Contains(err.Error(), "remove state uncertain") {
		t.Fatalf("ReleaseSourceProjection error = %v, want unresolved remove", err)
	}
	if err := projection.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(projectionRoot); err != nil {
		t.Fatalf("uncertain container lost its retained source projection: %v", err)
	}
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if args[0] == "rm" {
			delete(containers, args[len(args)-1])
		}
		return "", nil
	})
	if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(projectionRoot); !os.IsNotExist(err) {
		t.Fatalf("reconciled projection still exists: %v", err)
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
	t.Cleanup(func() { _ = selected.ReleaseSourceProjection(context.Background()) })
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
		t.Cleanup(func() { _ = selected.ReleaseSourceProjection(context.Background()) })
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
		t.Cleanup(func() { _ = selected.ReleaseSourceProjection(context.Background()) })
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
