package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type projectionDockerCall struct {
	manager string
	args    []string
}

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

func bindTestDockerProjection(t *testing.T, manager *DockerManager, projection *sourceartifact.RuntimeProjection) {
	t.Helper()
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatalf("BindSourceProjection: %v", err)
	}
	t.Cleanup(func() { _ = manager.ReleaseSourceProjection(context.Background()) })
}

func bindTestHostProjection(t *testing.T, manager *HostManager, projection *sourceartifact.RuntimeProjection) {
	t.Helper()
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatalf("BindSourceProjection: %v", err)
	}
	t.Cleanup(func() { _ = manager.ReleaseSourceProjection(context.Background()) })
}

func durableWorkspaceKeyForTest(t *testing.T, bundleHash string, kind durableWorkspaceKind, semanticIdentity string) string {
	t.Helper()
	key, err := durableWorkspaceBackingKey(bundleHash, kind, semanticIdentity)
	if err != nil {
		t.Fatal(err)
	}
	return key
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
		manager := NewDockerManager()
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
			ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")
			first, _ := testRuntimeSourceProjectionNamed(t, "same-source")
			second, _ := testRuntimeSourceProjectionNamed(t, "same-source")
			if first.BundleHash() != second.BundleHash() || first.Identity() == second.Identity() {
				t.Fatalf("same artifact projection identities = first %s/%s second %s/%s", first.BundleHash(), first.Identity(), second.BundleHash(), second.Identity())
			}
			semanticSource := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
					"workspace_classes": {Value: map[string]any{
						"dedicated": map[string]any{"workspace_scope": "per-agent"},
						"shared":    map[string]any{"workspace_scope": "per-flow-instance"},
					}},
				}},
			})
			var calls []projectionDockerCall
			bind := func(name string, projection *sourceartifact.RuntimeProjection) *DockerManager {
				manager := NewDockerManager()
				cfg := DefaultDockerConfig()
				cfg.SourceProjection = projection
				cfg.WorkspaceNetwork = ""
				cfg.WorkspaceImage = "test-image"
				manager.SetConfig(cfg)
				if err := manager.BindSourceProjection(projection); err != nil {
					t.Fatal(err)
				}
				manager.SetSemanticSource(semanticSource)
				manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
					calls = append(calls, projectionDockerCall{manager: name, args: append([]string(nil), args...)})
					if args[0] == "inspect" {
						return "", fmt.Errorf("no such object")
					}
					return "", nil
				})
				return manager
			}
			firstManager := bind("first", first)
			secondManager := bind("second", second)
			if firstManager.cfg.ScaffoldContainer == secondManager.cfg.ScaffoldContainer || firstManager.cfg.SystemContainer == secondManager.cfg.SystemContainer {
				t.Fatalf("same-hash projections reused process container names: first=%#v second=%#v", firstManager.cfg, secondManager.cfg)
			}
			for _, volumes := range [][2]string{
				{firstManager.cfg.ScaffoldVolume, secondManager.cfg.ScaffoldVolume},
				{firstManager.cfg.SystemEntitiesVolume, secondManager.cfg.SystemEntitiesVolume},
				{firstManager.cfg.SystemNginxVolume, secondManager.cfg.SystemNginxVolume},
				{firstManager.cfg.SystemSystemdVolume, secondManager.cfg.SystemSystemdVolume},
			} {
				if volumes[0] == "" || volumes[0] != volumes[1] {
					t.Fatalf("same-hash process replacement changed bundle-owned system volume: first=%q second=%q", volumes[0], volumes[1])
				}
			}
			if firstManager.cfg.ProcessScope == secondManager.cfg.ProcessScope || firstManager.cfg.BundleScope != secondManager.cfg.BundleScope {
				t.Fatalf("projection and bundle scopes = first %q/%q second %q/%q", firstManager.cfg.ProcessScope, firstManager.cfg.BundleScope, secondManager.cfg.ProcessScope, secondManager.cfg.BundleScope)
			}

			flowActor := models.AgentConfig{
				ID:             "reviewer",
				FlowPath:       "review/inst-1",
				Identity:       runtimeagentidentitytest.Runtime(t, "reviewer", "workspace-process-replacement", "review", "inst-1", "review/inst-1"),
				WorkspaceClass: "shared",
			}
			agentActor := models.AgentConfig{
				ID:             "dedicated-agent",
				Identity:       runtimeagentidentitytest.RootDeclared(t, "dedicated-agent", "test/agents.yaml"),
				WorkspaceClass: "dedicated",
			}
			for _, workspace := range []struct {
				name  string
				actor models.AgentConfig
			}{
				{name: "per-agent", actor: agentActor},
				{name: "per-flow-instance", actor: flowActor},
			} {
				firstTarget, err := firstManager.ResolveWorkspaceForCapabilityAdmission(ctx, workspace.actor)
				if err != nil {
					t.Fatalf("first %s workspace: %v", workspace.name, err)
				}
				secondTarget, err := secondManager.ResolveWorkspaceForCapabilityAdmission(ctx, workspace.actor)
				if err != nil {
					t.Fatalf("second %s workspace: %v", workspace.name, err)
				}
				assertSameDurableVolumeAttachment(t, calls, workspace.name, firstTarget.Container, secondTarget.Container)
			}
			if err := firstManager.EnsureSystemWorkspaces(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := secondManager.EnsureSystemWorkspaces(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, call := range calls {
				if call.manager != "second" || len(call.args) == 0 || call.args[0] != "inspect" {
					continue
				}
				inspected := call.args[len(call.args)-1]
				if inspected == firstManager.cfg.ScaffoldContainer || inspected == firstManager.cfg.SystemContainer {
					t.Fatalf("replacement process inspected %s predecessor container %q instead of its fresh projection identity", map[bool]string{true: "running", false: "stopped"}[predecessorRunning], inspected)
				}
			}
			assertSameDurableVolumeAttachment(t, calls, "scaffold", firstManager.cfg.ScaffoldContainer, secondManager.cfg.ScaffoldContainer)
			assertSameDurableVolumeAttachment(t, calls, "system", firstManager.cfg.SystemContainer, secondManager.cfg.SystemContainer)

			if err := firstManager.ReleaseSourceProjection(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := secondManager.ReleaseSourceProjection(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, call := range calls {
				if len(call.args) > 0 && call.args[0] == "volume" {
					t.Fatalf("projection release attempted durable volume mutation: manager=%s args=%#v", call.manager, call.args)
				}
			}
		})
	}
}

func assertSameDurableVolumeAttachment(t *testing.T, calls []projectionDockerCall, workspace, firstContainer, secondContainer string) {
	t.Helper()
	if firstContainer == "" || firstContainer == secondContainer {
		t.Fatalf("%s process containers = first %q second %q, want distinct non-empty names", workspace, firstContainer, secondContainer)
	}
	createdVolumes := func(container string) []string {
		for _, call := range calls {
			if len(call.args) < 3 || call.args[0] != "create" || call.args[1] != "--name" || call.args[2] != container {
				continue
			}
			var volumes []string
			for i := 3; i+1 < len(call.args); i++ {
				if call.args[i] == "-v" && !strings.HasSuffix(call.args[i+1], ":ro") {
					volumes = append(volumes, call.args[i+1])
				}
			}
			sort.Strings(volumes)
			return volumes
		}
		return nil
	}
	firstVolumes := createdVolumes(firstContainer)
	secondVolumes := createdVolumes(secondContainer)
	if len(firstVolumes) == 0 || strings.Join(firstVolumes, "\n") != strings.Join(secondVolumes, "\n") {
		t.Fatalf("%s durable attachments changed across process replacement: first=%q second=%q", workspace, firstVolumes, secondVolumes)
	}
}

func TestDockerManagerRejectsRepeatedSourceProjectionBinding(t *testing.T) {
	projection, projectionRoot := testRuntimeSourceProjection(t)
	manager := NewDockerManager()
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
	manager := NewDockerManager()
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
	manager := NewDockerManager()
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
	manager := NewDockerManager()
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
	manager := NewDockerManager()
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
	manager := NewDockerManager()
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
		manager := NewDockerManager()
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
		if selected.cfg.ProcessScope == "" || !strings.Contains(selected.cfg.ScaffoldContainer, selected.cfg.ProcessScope) {
			t.Fatalf("selected Docker names are not bundle scoped: %#v", selected.cfg)
		}
		mountArgs := strings.Join(selected.standardMountArgs(), " ")
		if !strings.Contains(mountArgs, projectionRoot+":"+LogicalSourceMount+":ro") {
			t.Fatalf("selected Docker source mount = %q", mountArgs)
		}
	})
}
