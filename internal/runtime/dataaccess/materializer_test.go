package dataaccess

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestMaterializerCreatesImmutableContentAddressedProjectionAndReusesIt(t *testing.T) {
	root := t.TempDir()
	registerProjectionCleanup(t, root)
	materializer, err := NewMaterializer(root, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), nil)
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	actor := models.AgentConfig{
		ID:       "reader",
		Identity: runtimeagentidentitytest.RootDeclared(t, "reader", "test/agents.yaml"),
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")

	first, err := materializer.Materialize(ctx, actor)
	if err != nil {
		t.Fatalf("first Materialize: %v", err)
	}
	second, err := materializer.Materialize(ctx, actor)
	if err != nil {
		t.Fatalf("replayed Materialize: %v", err)
	}
	if first.Root != second.Root || !strings.HasPrefix(first.Root, filepath.Join(root, "a_")) {
		t.Fatalf("projection roots = %q and %q, want one content-addressed root under %q", first.Root, second.Root, root)
	}
	manifestPath := projectionHostPath(first.Root, AccessManifestPath)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read projection manifest: %v", err)
	}
	if !strings.Contains(string(manifest), `"run_id":"11111111-1111-1111-1111-111111111111"`) || !strings.HasSuffix(string(manifest), "\n") {
		t.Fatalf("projection manifest = %q, want exact run identity and final newline", manifest)
	}
	if info, err := os.Stat(manifestPath); err != nil || info.Mode().Perm() != 0o400 {
		t.Fatalf("manifest mode = %#v, %v; want 0400", info, err)
	}
	if info, err := os.Stat(first.Root); err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("projection root mode = %#v, %v; want 0500", info, err)
	}
}

func TestMaterializerRejectsContradictoryExistingProjection(t *testing.T) {
	root := t.TempDir()
	registerProjectionCleanup(t, root)
	materializer, err := NewMaterializer(root, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	actor := models.AgentConfig{ID: "reader", Identity: runtimeagentidentitytest.RootDeclared(t, "reader", "test/agents.yaml")}
	ctx := runtimecorrelation.WithRunID(context.Background(), "22222222-2222-2222-2222-222222222222")
	projection, err := materializer.Materialize(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := projectionHostPath(projection.Root, AccessManifestPath)
	if err := os.Chmod(manifestPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := materializer.Materialize(ctx, actor); err == nil || !strings.Contains(err.Error(), "contradictory manifest bytes") {
		t.Fatalf("Materialize error = %v, want contradictory projection rejection", err)
	}
}

func TestMaterializerRejectsUnexpectedFileOnReuse(t *testing.T) {
	root := t.TempDir()
	registerProjectionCleanup(t, root)
	materializer, err := NewMaterializer(root, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	actor := models.AgentConfig{ID: "reader", Identity: runtimeagentidentitytest.RootDeclared(t, "reader", "test/agents.yaml")}
	ctx := runtimecorrelation.WithRunID(context.Background(), "33333333-3333-3333-3333-333333333333")
	projection, err := materializer.Materialize(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(projection.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projection.Root, "unexpected"), []byte("hostile"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(projection.Root, 0o500); err != nil {
		t.Fatal(err)
	}
	if _, err := materializer.Materialize(ctx, actor); err == nil || !strings.Contains(err.Error(), "unexpected entry") {
		t.Fatalf("Materialize reuse error = %v", err)
	}
}

func TestProjectionReuseRejectsMissingModifiedAndUnexpectedFiles(t *testing.T) {
	staticPath := "/data/.swarm/static/s_proof.data"
	access := durabledata.AccessList{
		SchemaVersion: durabledata.AccessSchemaVersion,
		RunID:         "11111111-1111-1111-1111-111111111111",
		Items: []durabledata.AccessItem{{
			Kind: "static_file",
			Static: &durabledata.StaticAccessItem{
				Kind: "static_file", MountPath: staticPath, Content: []byte("canonical\n"),
			},
		}},
	}
	manifest, err := json.Marshal(access)
	if err != nil {
		t.Fatal(err)
	}
	manifest = append(manifest, '\n')
	build := func(t *testing.T) string {
		t.Helper()
		root := filepath.Join(t.TempDir(), "projection")
		registerProjectionCleanup(t, root)
		if err := writeProjectionFile(root, AccessManifestPath, manifest); err != nil {
			t.Fatal(err)
		}
		if err := writeProjectionFile(root, staticPath, []byte("canonical\n")); err != nil {
			t.Fatal(err)
		}
		if err := makeProjectionReadOnly(root); err != nil {
			t.Fatal(err)
		}
		return root
	}

	t.Run("valid", func(t *testing.T) {
		if err := verifyProjection(build(t), access, manifest); err != nil {
			t.Fatalf("verifyProjection: %v", err)
		}
	})
	t.Run("modified", func(t *testing.T) {
		root := build(t)
		path := projectionHostPath(root, staticPath)
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("tampered\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o400); err != nil {
			t.Fatal(err)
		}
		if err := verifyProjection(root, access, manifest); err == nil || !strings.Contains(err.Error(), "contradict canonical content") {
			t.Fatalf("verifyProjection error = %v", err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		root := build(t)
		parent := filepath.Dir(projectionHostPath(root, staticPath))
		if err := os.Chmod(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(projectionHostPath(root, staticPath)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(parent, 0o500); err != nil {
			t.Fatal(err)
		}
		if err := verifyProjection(root, access, manifest); err == nil || !strings.Contains(err.Error(), "missing canonical file") {
			t.Fatalf("verifyProjection error = %v", err)
		}
	})
	t.Run("unexpected", func(t *testing.T) {
		root := build(t)
		if err := os.Chmod(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "unexpected"), []byte("hostile"), 0o400); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(root, 0o500); err != nil {
			t.Fatal(err)
		}
		if err := verifyProjection(root, access, manifest); err == nil || !strings.Contains(err.Error(), "unexpected entry") {
			t.Fatalf("verifyProjection error = %v", err)
		}
	})
}

func TestProjectionHostPathRejectsEscapesAndNonDataPaths(t *testing.T) {
	root := t.TempDir()
	for _, logical := range []string{"", "/", "/workspace/file", "/data", "/data/../escape", "data/file", "/data/../../escape"} {
		if got := projectionHostPath(root, logical); got != "" {
			t.Fatalf("projectionHostPath(%q) = %q, want rejection", logical, got)
		}
	}
	if got := projectionHostPath(root, "/data/.swarm/static/s_deadbeef.data"); got != filepath.Join(root, ".swarm", "static", "s_deadbeef.data") {
		t.Fatalf("valid projection path = %q", got)
	}
}

func registerProjectionCleanup(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if info.IsDir() {
					_ = os.Chmod(path, 0o700)
				} else {
					_ = os.Chmod(path, 0o600)
				}
			}
			return nil
		})
	})
}
