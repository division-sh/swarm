package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestConfigureWorkspaceDataProjectionMaterializesCanonicalEmptyDataForGrantFreeBundle(t *testing.T) {
	contractsRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsRoot, "package.yaml"), []byte("name: data-free\n"), 0o600); err != nil {
		t.Fatalf("write package marker: %v", err)
	}
	manager := workspace.NewHostManager()
	cfg := workspace.DefaultHostConfig()
	cfg.WorkspaceRoot = t.TempDir()
	cfg.ContractsSource = contractsRoot
	manager.SetConfigForTest(cfg)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager.SetSemanticSource(source)

	if err := configureWorkspaceDataProjection(manager, source, serveRuntimePersistence{}); err != nil {
		t.Fatalf("configureWorkspaceDataProjection: %v", err)
	}
	actor := models.AgentConfig{
		ID:       "data-free-reader",
		Identity: runtimeagentidentitytest.RootDeclared(t, "data-free-reader", "test/agents.yaml"),
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "81818181-8181-8181-8181-818181818181")
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}

	var dataMount workspace.ExecutionMount
	for _, mount := range target.Mounts {
		if mount.LogicalPath == workspace.LogicalDataMount {
			dataMount = mount
			break
		}
	}
	if dataMount.HostPath == "" || dataMount.Access != workspace.MountAccessReadOnly {
		t.Fatalf("data mount = %#v, want canonical read-only empty projection", dataMount)
	}
	manifestPath := strings.TrimPrefix(runtimedataaccess.AccessManifestPath, workspace.LogicalDataMount+"/")
	manifestBytes, err := os.ReadFile(filepath.Join(dataMount.HostPath, filepath.FromSlash(manifestPath)))
	if err != nil {
		t.Fatalf("read empty projection manifest: %v", err)
	}
	var manifest durabledata.AccessList
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode empty projection manifest: %v", err)
	}
	if len(manifest.Items) != 0 || manifest.RunID != "81818181-8181-8181-8181-818181818181" || manifest.AgentIdentity.AgentID() != actor.ID {
		t.Fatalf("empty projection manifest = %#v", manifest)
	}
}

func TestConfigureWorkspaceDataProjectionRejectsMissingStoreForDataBearingBundle(t *testing.T) {
	repo := repoRootForTest()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, "tests", "tier12-runtime-tools", "test-flow-data-access"),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load data-bearing bundle: %v", err)
	}
	source := semanticview.Wrap(bundle)
	if !source.DataProjectionRequired() {
		t.Fatal("data-bearing fixture does not require a projection")
	}
	err = configureWorkspaceDataProjection(workspace.NewHostManager(), source, serveRuntimePersistence{})
	if err == nil || err.Error() != "selected store does not expose durable data access projection" {
		t.Fatalf("configureWorkspaceDataProjection error = %v", err)
	}
}

func TestConfigureWorkspaceDataProjectionMountsCanonicalEmptyDataInDocker(t *testing.T) {
	contractsRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsRoot, "package.yaml"), []byte("name: data-free\n"), 0o600); err != nil {
		t.Fatalf("write package marker: %v", err)
	}
	manager := workspace.NewDockerManager(nil)
	cfg := workspace.DefaultDockerConfig()
	cfg.ContractsSource = contractsRoot
	cfg.WorkspaceImage = "test-image"
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager.SetSemanticSource(source)
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

	if err := configureWorkspaceDataProjection(manager, source, serveRuntimePersistence{}); err != nil {
		t.Fatalf("configureWorkspaceDataProjection: %v", err)
	}
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "docker-data-free-reader",
		Identity:      runtimeagentidentitytest.RootDeclared(t, "docker-data-free-reader", "test/agents.yaml"),
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "82828282-8282-8282-8282-828282828282")
	if _, err := manager.ResolveWorkspace(ctx, actor); err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	joined := strings.Join(created, " ")
	if !strings.Contains(joined, ":/data:ro") {
		t.Fatalf("Docker create args lack canonical empty /data projection: %v", created)
	}
	if !strings.Contains(joined, "--label dev.swarm.data_projection_id=data-projection-v1:sha256:") {
		t.Fatalf("Docker create args lack typed empty projection identity: %v", created)
	}
}
