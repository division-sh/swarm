package workspace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestHostManagerValidatesSourcesAndCreatesSystemWorkspacesWithoutDocker(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	root := filepath.Join(t.TempDir(), "host-workspaces")
	manager := NewHostManager(nil)
	manager.SetConfig(HostConfig{
		WorkspaceRoot:       root,
		SharedDataSource:    dataDir,
		DataMountPoint:      "/data",
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := manager.ValidateSource(context.Background(), source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}
	for _, rel := range []string{"scaffold", "system"} {
		if info, err := os.Stat(filepath.Join(root, rel)); err != nil || !info.IsDir() {
			t.Fatalf("host workspace %s stat = info:%#v err:%v, want directory", rel, info, err)
		}
	}
}

func TestHostManagerResolveWorkspaceCreatesScopedHostTargets(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	root := filepath.Join(t.TempDir(), "host-workspaces")
	canonicalRoot := canonicalTestPath(t, root)
	manager := NewHostManager(nil)
	manager.SetConfig(HostConfig{
		WorkspaceRoot:       root,
		SharedDataSource:    dataDir,
		DataMountPoint:      "/data",
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated":   map[string]any{"workspace_scope": "per-agent"},
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	}))

	dedicated, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "Dedicated Agent",
		WorkspaceClass: "dedicated",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace dedicated: %v", err)
	}
	if dedicated == nil || dedicated.Enabled() || !dedicated.HostBackend() {
		t.Fatalf("dedicated target = %#v, want host target without container", dedicated)
	}
	if !strings.HasPrefix(filepath.Clean(dedicated.Workdir), filepath.Join(canonicalRoot, "agents")) {
		t.Fatalf("dedicated workdir = %q, want under agents root %q", dedicated.Workdir, filepath.Join(canonicalRoot, "agents"))
	}

	shared, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "shared-agent",
		FlowPath:       "flows/acme/review",
		WorkspaceClass: "shared_flow",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace shared: %v", err)
	}
	if shared == nil || shared.Enabled() || !shared.HostBackend() {
		t.Fatalf("shared target = %#v, want host target without container", shared)
	}
	if !strings.HasPrefix(filepath.Clean(shared.Workdir), filepath.Join(canonicalRoot, "flows")) {
		t.Fatalf("shared workdir = %q, want under flows root %q", shared.Workdir, filepath.Join(canonicalRoot, "flows"))
	}
}

func TestHostManagerRejectsWorkspaceRootOverlappingReadOnlySources(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	contractsDir := t.TempDir()
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewHostManager(nil)
	manager.SetConfig(HostConfig{
		WorkspaceRoot:       root,
		SharedDataSource:    dataDir,
		DataMountPoint:      "/data",
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "must not overlap /data source") {
		t.Fatalf("ValidateSource error = %v, want overlap rejection", err)
	}
}

func TestHostManagerRejectsSymlinkedWorkspaceRootIntoReadOnlySources(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	for _, tt := range []struct {
		name       string
		target     string
		wantSource string
	}{
		{name: "data", target: dataDir, wantSource: "/data source"},
		{name: "contracts", target: contractsDir, wantSource: "/opt/swarm/contracts source"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rootLink := filepath.Join(t.TempDir(), "host-workspaces")
			if err := os.Symlink(tt.target, rootLink); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
			manager := NewHostManager(nil)
			manager.SetConfig(HostConfig{
				WorkspaceRoot:       rootLink,
				SharedDataSource:    dataDir,
				DataMountPoint:      "/data",
				ContractsSource:     contractsDir,
				ContractsMountPoint: "/opt/swarm/contracts",
			})
			err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
			if err == nil || !strings.Contains(err.Error(), "must not overlap "+tt.wantSource) {
				t.Fatalf("ValidateSource error = %v, want symlink overlap rejection for %s", err, tt.wantSource)
			}
		})
	}
}

func TestHostManagerRejectsSymlinkedWorkspaceChildEscape(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	root := filepath.Join(t.TempDir(), "host-workspaces")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := os.Symlink(dataDir, filepath.Join(root, "agents")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	manager := NewHostManager(nil)
	manager.SetConfig(HostConfig{
		WorkspaceRoot:       root,
		SharedDataSource:    dataDir,
		DataMountPoint:      "/data",
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := manager.ValidateSource(context.Background(), source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	manager.SetSemanticSource(source)
	_, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1"})
	if err == nil || !strings.Contains(err.Error(), "escapes root") {
		t.Fatalf("ResolveWorkspace error = %v, want symlink child escape rejection", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "agent-1")); !os.IsNotExist(err) {
		t.Fatalf("symlinked workspace child created under /data: %v", err)
	}
}

func TestHostManagerResolveWorkspaceValidatesRootBeforeCreate(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	rootLink := filepath.Join(t.TempDir(), "host-workspaces")
	if err := os.Symlink(dataDir, rootLink); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	manager := NewHostManager(nil)
	manager.SetConfig(HostConfig{
		WorkspaceRoot:       rootLink,
		SharedDataSource:    dataDir,
		DataMountPoint:      "/data",
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))

	_, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1"})
	if err == nil || !strings.Contains(err.Error(), "must not overlap /data source") {
		t.Fatalf("ResolveWorkspace error = %v, want validation-before-create overlap rejection", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "agents")); !os.IsNotExist(err) {
		t.Fatalf("ResolveWorkspace created workspace directory through symlinked host root: %v", err)
	}
}

func TestHostManagerContainerSurfacesAreNoop(t *testing.T) {
	manager := NewHostManager(nil)
	inventory, err := manager.ManagedResetContainerInventory(context.Background())
	if err != nil {
		t.Fatalf("ManagedResetContainerInventory: %v", err)
	}
	if len(inventory) != 0 {
		t.Fatalf("inventory = %#v, want empty host container inventory", inventory)
	}
	result, err := manager.CleanupDevEntityContainers(context.Background())
	if err != nil {
		t.Fatalf("CleanupDevEntityContainers: %v", err)
	}
	if result.OperationName != DevEntityCleanupOperationName {
		t.Fatalf("cleanup operation = %q, want %q", result.OperationName, DevEntityCleanupOperationName)
	}
}

func canonicalTestPath(t *testing.T, path string) string {
	t.Helper()
	canonical, err := canonicalPathForOverlap(path, "test path")
	if err != nil {
		t.Fatalf("canonical test path %s: %v", path, err)
	}
	return canonical
}
