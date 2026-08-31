package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontaineridentity "github.com/division-sh/swarm/internal/runtime/containeridentity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/google/uuid"
)

func TestWorkspacePrerequisiteRecoveryUsesConfiguredDockerBinaryAndLocalBuild(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.DockerBin = "/opt/docker cli"
	cfg.WorkspaceImage = "custom-workspace:test"
	manager.SetConfig(cfg)

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		return "", fmt.Errorf("daemon offline")
	})
	err := manager.CheckDockerAvailable(context.Background())
	var prerequisiteErr *PrerequisiteError
	if !errors.As(err, &prerequisiteErr) {
		t.Fatalf("CheckDockerAvailable error type = %T, want *PrerequisiteError", err)
	}
	if !strings.Contains(prerequisiteErr.Problem, cfg.DockerBin) || prerequisiteErr.Remediation != "Start the Docker daemon, then verify with `'/opt/docker cli' info`" {
		t.Fatalf("Docker diagnostic = %#v, want configured binary recovery", prerequisiteErr)
	}

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) >= 2 && args[0] == "image" && args[1] == "inspect" {
			return "", fmt.Errorf("image absent")
		}
		return "", nil
	})
	err = manager.CheckWorkspaceImageAvailable(context.Background())
	prerequisiteErr = nil
	if !errors.As(err, &prerequisiteErr) {
		t.Fatalf("CheckWorkspaceImageAvailable error type = %T, want *PrerequisiteError", err)
	}
	if !strings.Contains(prerequisiteErr.Problem, cfg.WorkspaceImage) || prerequisiteErr.Remediation != "Run `swarm workspace build --backend claude_cli` before startup" || strings.Contains(prerequisiteErr.Error(), "pull") {
		t.Fatalf("image diagnostic = %#v, want exact local build recovery without pull", prerequisiteErr)
	}
}

func TestWorkspaceClassesForSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated":   map[string]any{"workspace_scope": "per-agent"},
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	})
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		t.Fatalf("workspaceClassesForSource: %v", err)
	}
	if got := classes["dedicated"]; got != "per-agent" {
		t.Fatalf("dedicated scope = %q, want per-agent", got)
	}
	if got := classes["shared_flow"]; got != "per-flow-instance" {
		t.Fatalf("shared_flow scope = %q, want per-flow-instance", got)
	}
}

func TestValidateSource_RejectsUndefinedWorkspaceClass(t *testing.T) {
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"coordinator": {ID: "coordinator", WorkspaceClass: "missing"},
		},
	})
	err := manager.ValidateSource(context.Background(), source)
	if err == nil || !strings.Contains(err.Error(), `undefined workspace_class "missing"`) {
		t.Fatalf("expected undefined workspace_class error, got %v", err)
	}
}

func TestValidateAgentWorkspaceClassesCensusesAmbiguousScopedDeclarations(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	writeWorkspaceValidationFile(t, filepath.Join(root, "package.yaml"), `
name: scoped-workspace-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/project-a
  - path: packages/project-b
`)
	writeWorkspaceValidationFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-workspace-validation\n")
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(root, "packages", project)
		writeWorkspaceValidationFile(t, filepath.Join(dir, "package.yaml"), "name: "+project+"\nversion: \"1.0.0\"\nflows: []\n")
		writeWorkspaceValidationFile(t, filepath.Join(dir, "agents.yaml"), "shared-worker:\n  id: shared-worker\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise scoped workspace-class validation.\n  workspace_class: missing\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	err = validateAgentWorkspaceClasses(source, map[string]string{"dedicated": "per-agent"})
	if err == nil || !strings.Contains(err.Error(), `project packages/project-a agent shared-worker references undefined workspace_class "missing"`) {
		t.Fatalf("validateAgentWorkspaceClasses error = %v, want qualified scoped-agent failure", err)
	}
}

func writeWorkspaceValidationFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimSpace(content)+"\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestResolveWorkspace_PerAgentMountsStandardPaths(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetDataProjectionProvider(workspaceDataProjectionStub{root: dataDir})
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
	}))

	var created []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created = append([]string{}, args...)
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")
	actor := models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "dedicated-agent",
		Identity:       runtimeagentidentitytest.RootDeclaredForRun(t, "11111111-1111-1111-1111-111111111111", "dedicated-agent", "test/agents.yaml"),
		WorkspaceClass: "dedicated",
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	if target == nil || target.Container != "swarm-agent-"+fingerprint {
		t.Fatalf("target = %#v, want exact-identity agent container", target)
	}
	joined := strings.Join(created, " ")
	for _, expected := range []string{
		dataDir + ":/data:ro",
		contractsDir + ":/opt/swarm/contracts:ro",
		"workspaces_agent_" + fingerprint + ":/workspace",
		"--label dev.swarm.container.kind=agent",
		"--label dev.swarm.reset.eligible=true",
		"--label dev.swarm.agent_id=dedicated-agent",
		"--label dev.swarm.agent_name_owner=test/agents.yaml",
		"--label dev.swarm.agent_name_source=declared",
		"--label dev.swarm.agent_route_presence=root",
		"--label dev.swarm.run_id=11111111-1111-1111-1111-111111111111",
		"--label dev.swarm.data_projection_id=data-projection-v1:sha256:",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("create args missing %q: %v", expected, created)
		}
	}
}

func TestEnsureWorkspaceContainerReplacesDifferentRunProjectionIdentity(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	name := "swarm-flow-shared-work"
	existing := runtimecontaineridentity.Identity{
		Owner: runtimecontaineridentity.OwnerRuntime, Kind: runtimecontaineridentity.KindFlow,
		ResetEligible: true, CreationSource: "workspace.ResolveWorkspace", ContainerName: name,
		WorkspaceScope: "per-flow-instance", RunID: "11111111-1111-1111-1111-111111111111", FlowInstance: "shared/work",
		AgentIdentity:  runtimeagentidentitytest.DeclaredForRun(t, "11111111-1111-1111-1111-111111111111", "worker", "test/agents.yaml", "shared", "work", "shared/work"),
		DataProjection: testDataProjectionID("a"),
	}
	requested := existing
	requested.RunID = "22222222-2222-2222-2222-222222222222"
	requested.AgentIdentity = runtimeagentidentitytest.DeclaredForRun(t, requested.RunID, "worker", "test/agents.yaml", "shared", "work", "shared/work")
	requested.DataProjection = testDataProjectionID("b")
	labels, err := json.Marshal(existing.Labels())
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch {
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{.State.Running}}":
			return "true", nil
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{json .Config.Labels}}":
			return string(labels), nil
		default:
			return "", nil
		}
	})
	if err := manager.EnsureContainerRunningWithIdentity(context.Background(), name, requested, []string{
		"-v", "/projection/run-b:/data:ro", "image", "sleep", "infinity",
	}); err != nil {
		t.Fatalf("EnsureContainerRunningWithIdentity: %v", err)
	}
	joined := flattenDockerCalls(calls)
	for _, required := range []string{
		"rm --force " + name,
		"create --name " + name,
		"--label dev.swarm.run_id=22222222-2222-2222-2222-222222222222",
		"--label dev.swarm.data_projection_id=" + string(testDataProjectionID("b")),
		"/projection/run-b:/data:ro",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("replacement calls missing %q:\n%s", required, joined)
		}
	}
	if strings.Index(joined, "rm --force "+name) > strings.Index(joined, "create --name "+name) {
		t.Fatalf("stale container was not removed before replacement:\n%s", joined)
	}
}

func TestEnsureWorkspaceContainerReusesExactRunProjectionIdentity(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	identity := runtimecontaineridentity.Identity{
		Owner: runtimecontaineridentity.OwnerRuntime, Kind: runtimecontaineridentity.KindFlow,
		ResetEligible: true, CreationSource: "workspace.ResolveWorkspace", ContainerName: "swarm-flow-shared-work",
		WorkspaceScope: "per-flow-instance", RunID: "22222222-2222-2222-2222-222222222222", FlowInstance: "shared/work",
		AgentIdentity:  runtimeagentidentitytest.DeclaredForRun(t, "22222222-2222-2222-2222-222222222222", "worker", "test/agents.yaml", "shared", "work", "shared/work"),
		DataProjection: testDataProjectionID("b"),
	}
	labels, err := json.Marshal(identity.Labels())
	if err != nil {
		t.Fatal(err)
	}
	var mutatingCall bool
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch {
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{.State.Running}}":
			return "true", nil
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{json .Config.Labels}}":
			return string(labels), nil
		case args[0] == "rm" || args[0] == "create" || args[0] == "start":
			mutatingCall = true
		}
		return "", nil
	})
	if err := manager.EnsureContainerRunningWithIdentity(context.Background(), identity.ContainerName, identity, []string{
		"-v", "/projection/run-b:/data:ro",
	}); err != nil {
		t.Fatalf("EnsureContainerRunningWithIdentity: %v", err)
	}
	if mutatingCall {
		t.Fatal("exact container identity was needlessly replaced")
	}
}

func TestEnsureWorkspaceContainerRejectsUnownedNameCollision(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	requested := runtimecontaineridentity.Identity{
		Owner: runtimecontaineridentity.OwnerRuntime, Kind: runtimecontaineridentity.KindFlow,
		ResetEligible: true, CreationSource: "workspace.ResolveWorkspace", ContainerName: "swarm-flow-shared-work",
		WorkspaceScope: "per-flow-instance", RunID: "22222222-2222-2222-2222-222222222222", FlowInstance: "shared/work",
		AgentIdentity:  runtimeagentidentitytest.DeclaredForRun(t, "22222222-2222-2222-2222-222222222222", "worker", "test/agents.yaml", "shared", "work", "shared/work"),
		DataProjection: testDataProjectionID("b"),
	}
	var mutatingCall bool
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch {
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{.State.Running}}":
			return "true", nil
		case len(args) >= 3 && args[0] == "inspect" && args[2] == "{{json .Config.Labels}}":
			return `{}`, nil
		case args[0] == "rm" || args[0] == "create" || args[0] == "start":
			mutatingCall = true
		}
		return "", nil
	})
	err := manager.EnsureContainerRunningWithIdentity(context.Background(), requested.ContainerName, requested, nil)
	if err == nil || !strings.Contains(err.Error(), "without the required runtime identity") {
		t.Fatalf("identity collision error = %v", err)
	}
	if mutatingCall {
		t.Fatal("unowned container name collision triggered mutation")
	}
}

func TestResolveWorkspace_BundleScopeDisambiguatesContainersVolumesAndLabels(t *testing.T) {
	const bundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const entityID = "22222222-2222-2222-2222-222222222222"
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(workspaceLookupStub{entity: WorkspaceEntityLookup{Slug: "acme"}})
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetBundleScope(bundleHash)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
	}))

	var creates [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			creates = append(creates, append([]string{}, args...))
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")
	actor := models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "dedicated-agent",
		Identity:       runtimeagentidentitytest.RootDeclaredForRun(t, "11111111-1111-1111-1111-111111111111", "dedicated-agent", "test/agents.yaml"),
		WorkspaceClass: "dedicated",
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	if target == nil || target.Container != "swarm-bundle-aaaaaaaaaaaa-agent-"+fingerprint {
		t.Fatalf("target = %#v, want bundle-scoped agent container", target)
	}
	joined := flattenDockerCalls(creates)
	for _, expected := range []string{
		"workspaces_" + volumeScopeKey("swarm-bundle-aaaaaaaaaaaa-agent_"+fingerprint) + ":/workspace",
		"--label dev.swarm.bundle_hash=" + bundleHash,
		"--label dev.swarm.container.name=swarm-bundle-aaaaaaaaaaaa-agent-" + fingerprint,
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("bundle-scoped agent workspace create args missing %q:\n%s", expected, joined)
		}
	}

	creates = nil
	if err := manager.EnsureEntityWorkspace(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntityWorkspace: %v", err)
	}
	joined = flattenDockerCalls(creates)
	for _, expected := range []string{
		"create --name swarm-bundle-aaaaaaaaaaaa-acme",
		"entities_swarm_bundle_aaaaaaaaaaaa_entity_acme:/workspace",
		"--label dev.swarm.bundle_hash=" + bundleHash,
		"--label dev.swarm.container.name=swarm-bundle-aaaaaaaaaaaa-acme",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("bundle-scoped entity workspace create args missing %q:\n%s", expected, joined)
		}
	}
}

func TestResolveWorkspace_PerFlowInstanceSharesByFlowPath(t *testing.T) {
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	}))

	var created []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created = append([]string{}, args...)
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "22222222-2222-2222-2222-222222222222")
	actor := models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "shared-work-lead",
		Identity:       runtimeagentidentitytest.DeclaredForRun(t, "22222222-2222-2222-2222-222222222222", "shared-work-lead", "test/agents.yaml", "shared", "work-001", "shared/work-001"),
		WorkspaceClass: "shared_flow",
		FlowPath:       "shared/work-001",
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	wantContainer := "swarm-flow-shared-work-001-agent-" + fingerprint
	if target == nil || target.Container != wantContainer {
		t.Fatalf("target = %#v, want %s", target, wantContainer)
	}
	joined := strings.Join(created, " ")
	if !strings.Contains(joined, "workspaces_flow_shared-work-001:/workspace") {
		t.Fatalf("expected shared flow workspace volume, got %v", created)
	}
	for _, expected := range []string{
		"--label dev.swarm.container.kind=flow",
		"--label dev.swarm.reset.eligible=true",
		"--label dev.swarm.flow_instance=shared/work-001",
		"--label dev.swarm.agent_id=shared-work-lead",
		"--label dev.swarm.run_id=22222222-2222-2222-2222-222222222222",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("create args missing %q: %v", expected, created)
		}
	}
}

func TestResolveWorkspace_PerFlowInstanceIsolatesActorDataAndSharesWorkspaceVolume(t *testing.T) {
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	rootA := filepath.Clean(t.TempDir())
	rootB := filepath.Clean(t.TempDir())
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetDataProjectionProvider(workspaceDataProjectionStub{roots: map[string]string{
		"reader-a": rootA,
		"reader-b": rootB,
	}})
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {Value: map[string]any{
				"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
			}},
		}},
	}))

	var creates [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			creates = append(creates, append([]string(nil), args...))
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "33333333-3333-3333-3333-333333333333")
	actorA := models.AgentConfig{
		ExecutionMode: "live", ID: "reader-a", WorkspaceClass: "shared_flow", FlowPath: "shared/work-002",
		Identity: runtimeagentidentitytest.DeclaredForRun(t, "33333333-3333-3333-3333-333333333333", "reader-a", "test/agents.yaml", "shared", "work-002", "shared/work-002"),
	}
	actorB := models.AgentConfig{
		ExecutionMode: "live", ID: "reader-b", WorkspaceClass: "shared_flow", FlowPath: "shared/work-002",
		Identity: runtimeagentidentitytest.DeclaredForRun(t, "33333333-3333-3333-3333-333333333333", "reader-b", "test/agents.yaml", "shared", "work-002", "shared/work-002"),
	}
	targetA, err := manager.ResolveWorkspace(ctx, actorA)
	if err != nil {
		t.Fatalf("ResolveWorkspace actor A: %v", err)
	}
	targetB, err := manager.ResolveWorkspace(ctx, actorB)
	if err != nil {
		t.Fatalf("ResolveWorkspace actor B: %v", err)
	}
	if targetA.Container == targetB.Container {
		t.Fatalf("distinct actors reused container %q", targetA.Container)
	}
	if len(creates) != 2 {
		t.Fatalf("create calls = %d, want one isolated container per actor", len(creates))
	}
	joinedA := strings.Join(creates[0], " ")
	joinedB := strings.Join(creates[1], " ")
	sharedVolume := "workspaces_flow_shared-work-002:/workspace"
	for label, joined := range map[string]string{"actor A": joinedA, "actor B": joinedB} {
		if !strings.Contains(joined, sharedVolume) {
			t.Fatalf("%s container does not share exact flow workspace volume: %s", label, joined)
		}
	}
	if !strings.Contains(joinedA, rootA+":/data:ro") || strings.Contains(joinedA, rootB+":/data:ro") {
		t.Fatalf("actor A data mount is not isolated: %s", joinedA)
	}
	if !strings.Contains(joinedB, rootB+":/data:ro") || strings.Contains(joinedB, rootA+":/data:ro") {
		t.Fatalf("actor B data mount is not isolated: %s", joinedB)
	}
	if !strings.Contains(joinedA, "--label dev.swarm.agent_id=reader-a") || !strings.Contains(joinedB, "--label dev.swarm.agent_id=reader-b") {
		t.Fatalf("per-flow container identities do not carry exact actors:\nA: %s\nB: %s", joinedA, joinedB)
	}
}

func TestResolveWorkspaceRejectsMalformedProjectionBeforeDockerMutation(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	manager.SetDataProjectionProvider(workspaceProjectionProviderFunc(func(context.Context, models.AgentConfig) (runtimedataaccess.Projection, error) {
		return runtimedataaccess.Projection{
			ID:   runtimedataaccess.ProjectionID("data-projection-v1:sha256:deadbeef"),
			Root: filepath.Clean(t.TempDir()),
		}, nil
	}))
	mutated := false
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		mutated = true
		return "", nil
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "44444444-4444-4444-4444-444444444444")
	_, err := manager.ResolveWorkspace(ctx, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "reader",
		Identity:      runtimeagentidentitytest.RootDeclaredForRun(t, "44444444-4444-4444-4444-444444444444", "reader", "test/agents.yaml"),
	})
	if err == nil || !strings.Contains(err.Error(), "data projection identity requires one canonical SHA-256 digest") {
		t.Fatalf("ResolveWorkspace error = %v, want malformed projection rejection", err)
	}
	if mutated {
		t.Fatal("malformed projection reached Docker mutation")
	}
}

func TestResolveWorkspaceForCapabilityAdmissionDoesNotMaterializeRunBoundData(t *testing.T) {
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceImage = "test-image"
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	materializeCalls := 0
	manager.SetDataProjectionProvider(workspaceProjectionProviderFunc(func(context.Context, models.AgentConfig) (runtimedataaccess.Projection, error) {
		materializeCalls++
		return runtimedataaccess.Projection{}, errors.New("run-bound projection requested")
	}))
	var created []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created = append([]string(nil), args...)
		}
		return "", nil
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "preflight-reader",
		Identity:      runtimeagentidentitytest.RootDeclared(t, "preflight-reader", "test/agents.yaml"),
	}

	target, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
	if err != nil {
		t.Fatalf("ResolveWorkspaceForCapabilityAdmission: %v", err)
	}
	if materializeCalls != 0 {
		t.Fatalf("capability admission materialized run-bound data %d times", materializeCalls)
	}
	if !target.ExecutionTarget().Supports(ExecutionCapabilityClaudeCLI) {
		t.Fatalf("capability-admission target = %#v, want Claude CLI support", target)
	}
	if target.Container != cfg.SystemContainer || target.Workdir != cfg.SystemWorkdir {
		t.Fatalf("capability-admission target = %#v, want existing runless system workspace", target)
	}
	if len(created) != 0 {
		t.Fatalf("capability admission created execution workspace: %v", created)
	}
	if strings.Contains(strings.Join(created, " "), ":/data:ro") {
		t.Fatalf("capability-admission container received a run-bound data mount: %v", created)
	}
	if _, err := manager.ResolveWorkspace(context.Background(), actor); err == nil || !strings.Contains(err.Error(), "run-bound projection requested") {
		t.Fatalf("execution ResolveWorkspace error = %v, want strict run-bound materialization", err)
	}
	if materializeCalls != 1 {
		t.Fatalf("execution materialization calls = %d, want 1", materializeCalls)
	}
}

type executionOnlyWorkspaceResolver struct{}

func (executionOnlyWorkspaceResolver) ResolveWorkspace(context.Context, models.AgentConfig) (*Target, error) {
	return &Target{Backend: BackendHost, Workdir: "/workspace"}, nil
}

func TestResolveForCapabilityAdmissionRejectsExecutionOnlyResolver(t *testing.T) {
	_, err := ResolveForCapabilityAdmission(context.Background(), executionOnlyWorkspaceResolver{}, models.AgentConfig{})
	if err == nil || err.Error() != "workspace resolver does not expose capability-admission resolution" {
		t.Fatalf("ResolveForCapabilityAdmission error = %v", err)
	}
}

func TestRuntimeWorkspaceContainersWithoutRunContextReturnsStaticContainers(t *testing.T) {
	manager := NewDockerManager(nil)
	containers, err := manager.RuntimeWorkspaceContainers(context.Background())
	if err != nil {
		t.Fatalf("RuntimeWorkspaceContainers: %v", err)
	}
	got := strings.Join(containers, ",")
	for _, expected := range []string{"swarm-scaffold", "swarm-system"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("containers = %v, want %s", containers, expected)
		}
	}
}

func TestRuntimeWorkspaceLookupFailsClosed(t *testing.T) {
	runID := uuid.NewString()
	entityID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)

	manager := NewDockerManager(nil)
	if _, err := manager.RuntimeWorkspaceContainers(ctx); err == nil || !strings.Contains(err.Error(), "lookup is required") {
		t.Fatalf("RuntimeWorkspaceContainers error = %v, want missing lookup failure", err)
	}
	if _, err := manager.LookupEntitySlug(ctx, entityID); err == nil || !strings.Contains(err.Error(), "lookup is required") {
		t.Fatalf("LookupEntitySlug error = %v, want missing lookup failure", err)
	}

	manager = NewDockerManager(workspaceLookupStub{
		entity: WorkspaceEntityLookup{},
		set:    RuntimeWorkspaceContainerSet{EntitySlugs: []string{""}},
	})
	if _, err := manager.RuntimeWorkspaceContainers(ctx); err == nil || !strings.Contains(err.Error(), "empty entity slug") {
		t.Fatalf("RuntimeWorkspaceContainers empty slug error = %v", err)
	}
	if _, err := manager.LookupEntitySlug(ctx, entityID); err == nil || !strings.Contains(err.Error(), "empty slug") {
		t.Fatalf("LookupEntitySlug empty slug error = %v", err)
	}
}

type workspaceLookupStub struct {
	entity WorkspaceEntityLookup
	set    RuntimeWorkspaceContainerSet
}

type workspaceDataProjectionStub struct {
	root  string
	roots map[string]string
}

type workspaceProjectionProviderFunc func(context.Context, models.AgentConfig) (runtimedataaccess.Projection, error)

func (f workspaceProjectionProviderFunc) Materialize(ctx context.Context, actor models.AgentConfig) (runtimedataaccess.Projection, error) {
	return f(ctx, actor)
}

func testDataProjectionID(hexDigit string) runtimedataaccess.ProjectionID {
	return runtimedataaccess.ProjectionID("data-projection-v1:sha256:" + strings.Repeat(hexDigit, 64))
}

func (s workspaceDataProjectionStub) Materialize(_ context.Context, actor models.AgentConfig) (runtimedataaccess.Projection, error) {
	root := s.root
	if s.roots != nil {
		root = s.roots[strings.TrimSpace(actor.ID)]
	}
	digest := sha256.Sum256([]byte(root))
	return runtimedataaccess.Projection{
		ID:   runtimedataaccess.ProjectionID("data-projection-v1:sha256:" + hex.EncodeToString(digest[:])),
		Root: filepath.Clean(root),
	}, nil
}

func (s workspaceLookupStub) LookupWorkspaceEntity(context.Context, runtimecurrentstate.Identity) (WorkspaceEntityLookup, error) {
	return s.entity, nil
}

func (s workspaceLookupStub) ListRuntimeWorkspaceContainers(context.Context, string) (RuntimeWorkspaceContainerSet, error) {
	return s.set, nil
}

func TestResolveWorkspace_UsesInjectedSemanticSourceForRoleLookup(t *testing.T) {
	const owner = "test://workspace/ops/worker"
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				owner: {Kind: "agent", FlowID: "ops", LocalID: "worker", Full: owner},
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{
				owner: {Kind: "agent", FlowID: "ops", LocalID: "worker", Full: owner},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {ID: "worker-1", Role: "worker", WorkspaceClass: "shared_flow"},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{{
				Paths: runtimecontracts.FlowContractPaths{ID: "ops", Flow: "ops"},
				Path:  "ops",
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"worker": {ID: "worker-1", Role: "worker", WorkspaceClass: "shared_flow"},
				},
				AgentURIs: map[string]string{"worker": owner},
			}}},
			ByID: map[string]*runtimecontracts.FlowContractView{},
		},
	})
	if err := manager.ValidateSource(context.Background(), source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create", "start":
			return "", nil
		default:
			return "", nil
		}
	})

	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "worker-1",
		Identity:      runtimeagentidentitytest.Declared(t, "worker-1", owner, "ops", "instance-1", "ops/instance-1"),
		Role:          "worker",
		FlowPath:      "ops/instance-1",
	}
	target, err := manager.ResolveWorkspace(context.Background(), actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	wantContainer := "swarm-flow-ops-instance-1-agent-" + fingerprint
	if target == nil || target.Container != wantContainer {
		t.Fatalf("target = %#v, want %s", target, wantContainer)
	}
}

func TestResolveWorkspace_FailsClosedWithoutInjectedSourceForWorkspaceClassScope(t *testing.T) {
	manager := NewDockerManager(nil)
	_, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "worker-1",
		WorkspaceClass: "dedicated",
	})
	if err == nil || !strings.Contains(err.Error(), `semantic source is required for workspace_class "dedicated"`) {
		t.Fatalf("expected missing semantic source error, got %v", err)
	}
}

func TestDefaultDockerConfigDoesNotDeriveSourceRootMounts(t *testing.T) {
	cfg := DefaultDockerConfig()
	if cfg.SharedDataSource != "" {
		t.Fatalf("SharedDataSource = %q, want no source-root default", cfg.SharedDataSource)
	}
	if cfg.ContractsSource != "" {
		t.Fatalf("ContractsSource = %q, want no source-root default", cfg.ContractsSource)
	}
}

func TestDefaultDockerConfigHasNoAmbientDataSource(t *testing.T) {
	cfg := DefaultDockerConfig()
	if cfg.SharedDataSource != "" {
		t.Fatalf("SharedDataSource = %q, want retired ambient authority to remain absent", cfg.SharedDataSource)
	}
	if cfg.ContractsSource != "" {
		t.Fatalf("ContractsSource = %q, want command-level resolver to own contracts source", cfg.ContractsSource)
	}
}

func TestEnsurePrereqs_CreatesMissingNetworkAndFailsClosedForMissingImage(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = "test-network"
	cfg.WorkspaceImage = "test-image:latest"
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		switch {
		case len(args) >= 3 && args[0] == "version":
			return "26.1.0", nil
		case len(args) >= 3 && args[0] == "network" && args[1] == "inspect":
			return "", fmt.Errorf("no such network")
		case len(args) >= 3 && args[0] == "network" && args[1] == "create":
			return "created", nil
		case len(args) >= 3 && args[0] == "image" && args[1] == "inspect":
			return "", fmt.Errorf("no such image")
		case len(args) >= 6 && args[0] == "build":
			return "built", nil
		default:
			return "", nil
		}
	})

	err := manager.EnsurePrereqs(context.Background())
	if err == nil {
		t.Fatal("EnsurePrereqs unexpectedly succeeded with missing workspace image")
	}
	if !strings.Contains(err.Error(), `workspace image "test-image:latest" is not available`) {
		t.Fatalf("EnsurePrereqs error = %v, want missing image diagnostic", err)
	}
	if !strings.Contains(err.Error(), "swarm workspace build --backend claude_cli") || strings.Contains(err.Error(), "pull") {
		t.Fatalf("EnsurePrereqs error = %v, want exact local build remediation without pull", err)
	}

	joined := flattenDockerCalls(calls)
	for _, expected := range []string{
		"version --format {{.Server.Version}}",
		"network inspect test-network",
		"network create test-network",
		"image inspect test-image:latest",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsurePrereqs calls missing %q: %s", expected, joined)
		}
	}
	if strings.Contains(joined, "build ") || strings.Contains(joined, "Dockerfile.workspace") {
		t.Fatalf("EnsurePrereqs still attempted source-root image build:\n%s", joined)
	}
}

func TestEnsureSystemWorkspaces_CreatesScaffoldAndSystemContainers(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create", "start":
			return "", nil
		default:
			return "", nil
		}
	})

	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}

	joined := flattenDockerCalls(calls)
	for _, expected := range []string{
		"create --name swarm-scaffold",
		"create --name swarm-system",
		"--label dev.swarm.container.kind=scaffold",
		"--label dev.swarm.container.kind=system",
		"--label dev.swarm.reset.eligible=false",
		"test-image sleep infinity",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsureSystemWorkspaces calls missing %q: %s", expected, joined)
		}
	}
}

func TestSystemWorkspaceContainersUsesConfiguredNames(t *testing.T) {
	manager := NewDockerManager(nil)
	manager.SetConfigForTest(DockerConfig{
		ScaffoldContainer: "custom-scaffold",
		SystemContainer:   "custom-system",
	})

	got := manager.SystemWorkspaceContainers()
	want := []string{"custom-scaffold", "custom-system"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SystemWorkspaceContainers = %#v, want %#v", got, want)
	}
}

func TestManagedResetContainerInventoryConsumesTypedLabels(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return "swarm-agent-agent-a\nswarm-system\nswarm-malformed\nswarm-stale-name\n", nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-agent-agent-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":                    "runtime",
				"dev.swarm.container.kind":           "agent",
				"dev.swarm.reset.eligible":           "true",
				"dev.swarm.creation_source":          "workspace.ResolveWorkspace",
				"dev.swarm.container.name":           "swarm-agent-agent-a",
				"dev.swarm.workspace.scope":          "per-agent",
				"dev.swarm.run_id":                   "33333333-3333-3333-3333-333333333333",
				"dev.swarm.agent_id":                 "agent-a",
				"dev.swarm.agent_name_owner":         "test/agents.yaml",
				"dev.swarm.agent_name_source":        "declared",
				"dev.swarm.agent_route_presence":     "present",
				"dev.swarm.agent_flow_scope_key":     "flow",
				"dev.swarm.agent_flow_instance_id":   "a",
				"dev.swarm.agent_flow_instance_path": "flow/a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-system":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "system",
				"dev.swarm.reset.eligible":  "false",
				"dev.swarm.creation_source": "workspace.EnsureSystemWorkspaces",
				"dev.swarm.container.name":  "swarm-system",
				"dev.swarm.workspace.scope": "system",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-malformed":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":          "runtime",
				"dev.swarm.container.kind": "agent",
				"dev.swarm.reset.eligible": "true",
				"dev.swarm.container.name": "different-container-name",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-stale-name":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":                    "runtime",
				"dev.swarm.container.kind":           "agent",
				"dev.swarm.reset.eligible":           "true",
				"dev.swarm.creation_source":          "workspace.ResolveWorkspace",
				"dev.swarm.container.name":           "old-valid-container-name",
				"dev.swarm.workspace.scope":          "per-agent",
				"dev.swarm.run_id":                   "44444444-4444-4444-4444-444444444444",
				"dev.swarm.agent_id":                 "agent-stale",
				"dev.swarm.agent_name_owner":         "test/agents.yaml",
				"dev.swarm.agent_name_source":        "declared",
				"dev.swarm.agent_route_presence":     "present",
				"dev.swarm.agent_flow_scope_key":     "flow",
				"dev.swarm.agent_flow_instance_id":   "stale",
				"dev.swarm.agent_flow_instance_path": "flow/stale",
			}, true), nil
		default:
			return "", nil
		}
	})

	refs, err := manager.ManagedResetContainerInventory(context.Background())
	if err != nil {
		t.Fatalf("ManagedResetContainerInventory: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %#v, want one reset-eligible managed container", refs)
	}
	ref := refs[0]
	if ref.Name != "swarm-agent-agent-a" || ref.Kind != "agent" || !ref.ResetEligible || ref.AgentIdentity.AgentID() != "agent-a" || ref.RunID == "" {
		t.Fatalf("ref = %#v, want agent identity with run lineage", ref)
	}
}

func TestCleanupDevEntityContainersStopsOnlyIdentityProvenEntityContainers(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return strings.Join([]string{
				"swarm-entity-acme",
				"swarm-agent-agent-a",
				"swarm-flow-flow-a",
				"swarm-system",
				"swarm-unlabeled",
				"swarm-operator",
				"swarm-stale-name",
			}, "\n"), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-entity-acme":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.EnsureEntityWorkspace",
				"dev.swarm.container.name":  "swarm-entity-acme",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "entity-1",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-agent-agent-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":                    "runtime",
				"dev.swarm.container.kind":           "agent",
				"dev.swarm.reset.eligible":           "true",
				"dev.swarm.creation_source":          "workspace.ResolveWorkspace",
				"dev.swarm.container.name":           "swarm-agent-agent-a",
				"dev.swarm.workspace.scope":          "per-agent",
				"dev.swarm.run_id":                   runtimeagentidentitytest.DefaultRunID,
				"dev.swarm.agent_id":                 "agent-a",
				"dev.swarm.agent_name_owner":         "test/agents.yaml",
				"dev.swarm.agent_name_source":        "declared",
				"dev.swarm.agent_route_presence":     "present",
				"dev.swarm.agent_flow_scope_key":     "flow",
				"dev.swarm.agent_flow_instance_id":   "a",
				"dev.swarm.agent_flow_instance_path": "flow/a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-flow-flow-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":                    "runtime",
				"dev.swarm.container.kind":           "flow",
				"dev.swarm.reset.eligible":           "true",
				"dev.swarm.creation_source":          "workspace.ResolveWorkspace",
				"dev.swarm.container.name":           "swarm-flow-flow-a",
				"dev.swarm.workspace.scope":          "per-flow-instance",
				"dev.swarm.run_id":                   runtimeagentidentitytest.DefaultRunID,
				"dev.swarm.flow_instance":            "flow-a",
				"dev.swarm.agent_id":                 "agent-a",
				"dev.swarm.agent_name_owner":         "test/agents.yaml",
				"dev.swarm.agent_name_source":        "declared",
				"dev.swarm.agent_route_presence":     "present",
				"dev.swarm.agent_flow_scope_key":     "flow",
				"dev.swarm.agent_flow_instance_id":   "a",
				"dev.swarm.agent_flow_instance_path": "flow/a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-system":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "system",
				"dev.swarm.reset.eligible":  "false",
				"dev.swarm.creation_source": "workspace.EnsureSystemWorkspaces",
				"dev.swarm.container.name":  "swarm-system",
				"dev.swarm.workspace.scope": "system",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-unlabeled":
			return managedContainerInspectJSON(nil, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-operator":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "operator",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.container.name":  "swarm-operator",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "operator-entity",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-stale-name":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":          "runtime",
				"dev.swarm.container.kind": "entity",
				"dev.swarm.reset.eligible": "true",
				"dev.swarm.container.name": "different-container-name",
				"dev.swarm.entity_id":      "stale-entity",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{.State.Running}}" && args[len(args)-1] == "swarm-entity-acme":
			return "true", nil
		case joined == "stop swarm-entity-acme":
			return "", nil
		default:
			return "", nil
		}
	})

	result, err := manager.CleanupDevEntityContainers(context.Background())
	if err != nil {
		t.Fatalf("CleanupDevEntityContainers: %v", err)
	}
	if result.OperationName != DevEntityCleanupOperationName {
		t.Fatalf("operation = %q, want %q", result.OperationName, DevEntityCleanupOperationName)
	}
	if len(result.Stopped) != 1 || result.Stopped[0].Name != "swarm-entity-acme" || result.Stopped[0].Kind != "entity" {
		t.Fatalf("stopped = %#v, want only entity container", result.Stopped)
	}
	if len(result.Preserved) != 2 {
		t.Fatalf("preserved = %#v, want agent and flow reset-eligible containers preserved", result.Preserved)
	}
	joined := flattenDockerCalls(calls)
	for _, forbidden := range []string{"stop swarm-agent-agent-a", "stop swarm-flow-flow-a", "stop swarm-system", "stop swarm-unlabeled", "stop swarm-operator", "stop swarm-stale-name"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("dev cleanup stopped forbidden container %q:\n%s", forbidden, joined)
		}
	}
}

func TestCleanupDevEntityContainersReportsStopFailures(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return "swarm-entity-acme", nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-entity-acme":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.EnsureEntityWorkspace",
				"dev.swarm.container.name":  "swarm-entity-acme",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "entity-1",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{.State.Running}}" && args[len(args)-1] == "swarm-entity-acme":
			return "true", nil
		case joined == "stop swarm-entity-acme":
			return "", fmt.Errorf("docker stop failed")
		default:
			return "", nil
		}
	})

	result, err := manager.CleanupDevEntityContainers(context.Background())
	if err == nil || !strings.Contains(err.Error(), "dev entity container cleanup failed: 1 container(s)") {
		t.Fatalf("CleanupDevEntityContainers err = %v, want stop failure", err)
	}
	if len(result.Selected) != 1 || result.Selected[0].Name != "swarm-entity-acme" {
		t.Fatalf("selected = %#v, want failed entity selected", result.Selected)
	}
	if len(result.Stopped) != 0 {
		t.Fatalf("stopped = %#v, want none after failure", result.Stopped)
	}
	if len(result.Failed) != 1 || result.Failed[0].Container.Name != "swarm-entity-acme" || !strings.Contains(result.Failed[0].Error, "docker stop failed") {
		t.Fatalf("failed = %#v, want entity stop failure", result.Failed)
	}
}

func flattenDockerCalls(calls [][]string) string {
	lines := make([]string, 0, len(calls))
	for _, call := range calls {
		lines = append(lines, strings.Join(call, " "))
	}
	return strings.Join(lines, "\n")
}

func managedContainerInspectJSON(labels map[string]string, running bool) string {
	labelParts := make([]string, 0, len(labels))
	for key, value := range labels {
		labelParts = append(labelParts, fmt.Sprintf("%q:%q", key, value))
	}
	return fmt.Sprintf(`{"State":{"Running":%t},"Config":{"Labels":{%s}}}`, running, strings.Join(labelParts, ","))
}
