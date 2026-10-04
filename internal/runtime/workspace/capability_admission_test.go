package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCapabilityInspectionMatchesBoundDockerCapabilitiesWithoutCreatingWorkspaces(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	cfg := DefaultDockerConfig()
	inspection, err := NewDockerCapabilityInspection(cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	cfg.SourceProjection = projection
	manager := NewDockerManager()
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	bindTestDockerProjection(t, manager, projection)
	manager.SetRunDockerFnForTest(func(context.Context, ...string) (string, error) {
		t.Fatal("capability admission invoked Docker")
		return "", nil
	})
	for _, class := range []string{"", "system", "scaffold"} {
		actor := models.AgentConfig{ID: "runless-actor", WorkspaceClass: class}
		observed, err := ResolveForCapabilityAdmission(context.Background(), inspection, actor)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
		if err != nil {
			t.Fatal(err)
		}
		if bound.Workdir != observed.Workdir || !reflect.DeepEqual(bound.Mounts, observed.Mounts) {
			t.Fatalf("class %q: bound=%#v, %v; inspection=%#v", class, bound, err, observed)
		}
		for _, capability := range []ExecutionCapability{ExecutionCapabilityNativeCommand, ExecutionCapabilityFileRead, ExecutionCapabilityFileWrite, ExecutionCapabilityToolResultRelay, ExecutionCapabilityClaudeCLI} {
			if observed.ExecutionTarget().Supports(capability) != bound.ExecutionTarget().Supports(capability) {
				t.Fatalf("Docker capability %s diverged", capability)
			}
		}
		effective, err := DockerCapabilityTarget(manager.cfg, source, actor)
		if err != nil || !reflect.DeepEqual(bound, effective) {
			t.Fatalf("bound owner projection diverged: %#v, %v; bound=%#v", effective, err, bound)
		}
	}
	if _, err := inspection.ResolveWorkspace(context.Background(), models.AgentConfig{}); err == nil {
		t.Fatal("inspection exposed execution workspace resolution")
	}
	if _, ok := inspection.(Lifecycle); ok {
		t.Fatal("inspection exposes a runtime workspace lifecycle")
	}
	manager.SetRunDockerFnForTest(func(context.Context, ...string) (string, error) {
		return "", nil
	})
	if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), models.AgentConfig{}); err == nil || !strings.Contains(err.Error(), "released") {
		t.Fatalf("boot lost source revocation: %v", err)
	}
}

func TestCapabilityInspectionMatchesHostCapabilitiesWithoutSourceBindingOrDirectoryCreation(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	root := filepath.Join(t.TempDir(), "not-created", "workspaces")
	cfg := DefaultHostConfig()
	cfg.WorkspaceRoot = root
	inspection, err := NewHostCapabilityInspection(cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	cfg.SourceProjection = projection
	manager := NewHostManager()
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	bindTestHostProjection(t, manager, projection)
	actor := models.AgentConfig{ID: "runless-actor"}
	observed, err := ResolveForCapabilityAdmission(context.Background(), inspection, actor)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []ExecutionCapability{ExecutionCapabilityNativeCommand, ExecutionCapabilityFileRead, ExecutionCapabilityFileWrite, ExecutionCapabilityToolResultRelay, ExecutionCapabilityClaudeCLI} {
		if observed.ExecutionTarget().Supports(capability) != bound.ExecutionTarget().Supports(capability) {
			t.Fatalf("host capability %s diverged", capability)
		}
	}
	if observed.Workdir != root || !strings.HasPrefix(bound.Workdir, root+string(filepath.Separator)) {
		t.Fatalf("host root/bound scope diverged: inspection=%q, bound=%q", observed.Workdir, bound.Workdir)
	}
	effective, err := HostCapabilityTarget(manager.cfg, source, actor)
	if err != nil || effective.Workdir != bound.Workdir {
		t.Fatalf("bound host owner projection diverged: %#v, %v; bound=%#v", effective, err, bound)
	}
	for _, mount := range observed.Mounts {
		if mount.LogicalPath == LogicalSourceMount && mount.HostPath != "" {
			t.Fatalf("inspection fabricated a source binding: %#v", mount)
		}
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("capability admission created host root: %v", err)
	}
	if _, err := inspection.ResolveWorkspace(context.Background(), actor); err == nil {
		t.Fatal("host inspection permits execution resolution")
	}
}

func TestCapabilityInspectionPreservesScopedRouteRejectionAndCancellation(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {Value: map[string]any{"per-instance": map[string]any{"workspace_scope": "per-flow-instance"}}},
		}},
	})
	host := DefaultHostConfig()
	host.WorkspaceRoot = filepath.Join(t.TempDir(), "not-created")
	docker, err := NewDockerCapabilityInspection(DefaultDockerConfig(), source)
	if err != nil {
		t.Fatal(err)
	}
	local, err := NewHostCapabilityInspection(host, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, inspection := range []CapabilityAdmissionResolver{docker, local} {
		actor := models.AgentConfig{ID: "actor", WorkspaceClass: "per-instance"}
		if _, err := ResolveForCapabilityAdmission(context.Background(), inspection, actor); err == nil || !strings.Contains(err.Error(), "requires flow_path") {
			t.Fatalf("missing scoped route was admitted: %v", err)
		}
		actor.FlowPath = "project/flow"
		if _, err := ResolveForCapabilityAdmission(context.Background(), inspection, actor); err != nil {
			t.Fatalf("supported route was rejected: %v", err)
		}
		actor.WorkspaceClass = "undefined"
		if _, err := ResolveForCapabilityAdmission(context.Background(), inspection, actor); err == nil || !strings.Contains(err.Error(), "not defined") {
			t.Fatalf("undefined workspace class was admitted: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ResolveForCapabilityAdmission(ctx, inspection, models.AgentConfig{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation was lost: %v", err)
		}
	}
	if _, err := NewHostCapabilityInspection(host, nil); err == nil {
		t.Fatal("missing host source was admitted")
	}
	if _, err := NewDockerCapabilityInspection(DefaultDockerConfig(), nil); err == nil {
		t.Fatal("missing Docker source was admitted")
	}
}

func TestCapabilityInspectionDoesNotWeakenHostOverlapRejection(t *testing.T) {
	projection, root := testRuntimeSourceProjection(t)
	manager := NewHostManager()
	cfg := DefaultHostConfig()
	cfg.WorkspaceRoot = root
	cfg.SourceProjection = projection
	manager.SetConfig(cfg)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	bindTestHostProjection(t, manager, projection)
	if _, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), models.AgentConfig{}); err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("boot admitted overlapping source/workspace roots: %v", err)
	}
}
