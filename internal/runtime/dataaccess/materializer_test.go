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
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("make hostile projection parent permissive: %v", err)
	}
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
	if first.ID != second.ID {
		t.Fatalf("projection identities = %q and %q, want one exact content identity", first.ID, second.ID)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("projection validation: %v", err)
	}
	manifestPath := projectionHostPath(first.Root, AccessManifestPath)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read projection manifest: %v", err)
	}
	if !strings.Contains(string(manifest), `"run_id":"11111111-1111-1111-1111-111111111111"`) || !strings.HasSuffix(string(manifest), "\n") {
		t.Fatalf("projection manifest = %q, want exact run identity and final newline", manifest)
	}
	if info, err := os.Stat(manifestPath); err != nil || info.Mode().Perm() != 0o444 {
		t.Fatalf("manifest mode = %#v, %v; want 0444", info, err)
	}
	if info, err := os.Stat(first.Root); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("projection root mode = %#v, %v; want 0555", info, err)
	}
	if info, err := os.Stat(root); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("projection parent mode = %#v, %v; want private 0700", info, err)
	}
}

func TestMaterializerSeparatesConcreteActorsWithIdenticalEmptyGrants(t *testing.T) {
	root := t.TempDir()
	registerProjectionCleanup(t, root)
	materializer, err := NewMaterializer(root, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), nil)
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "12121212-1212-1212-1212-121212121212")
	actorA := models.AgentConfig{ID: "reader-a", Identity: runtimeagentidentitytest.RootDeclared(t, "reader-a", "test/agents.yaml")}
	actorB := models.AgentConfig{ID: "reader-b", Identity: runtimeagentidentitytest.RootDeclared(t, "reader-b", "test/agents.yaml")}
	projectionA, err := materializer.Materialize(ctx, actorA)
	if err != nil {
		t.Fatalf("Materialize actor A: %v", err)
	}
	projectionB, err := materializer.Materialize(ctx, actorB)
	if err != nil {
		t.Fatalf("Materialize actor B: %v", err)
	}
	if projectionA.ID == projectionB.ID || projectionA.Root == projectionB.Root {
		t.Fatalf("distinct actors share projection: A=%#v B=%#v", projectionA, projectionB)
	}
	if projectionA.AccessList.AgentIdentity.AgentID() != "reader-a" || projectionB.AccessList.AgentIdentity.AgentID() != "reader-b" {
		t.Fatalf("projection actor ownership drifted: A=%#v B=%#v", projectionA.AccessList.AgentIdentity, projectionB.AccessList.AgentIdentity)
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
	if err := os.Chmod(projection.Root, 0o555); err != nil {
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
		root := build(t)
		if err := verifyProjection(root, access, manifest); err != nil {
			t.Fatalf("verifyProjection: %v", err)
		}
		if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().Perm()&0o222 != 0 {
				t.Fatalf("projection path %s is writable: %04o", path, info.Mode().Perm())
			}
			if info.IsDir() && info.Mode().Perm()&0o005 != 0o005 {
				t.Fatalf("projection directory %s is not traversable/readable by Docker UID 10001: %04o", path, info.Mode().Perm())
			}
			if !info.IsDir() && info.Mode().Perm()&0o004 != 0o004 {
				t.Fatalf("projection file %s is not readable by Docker UID 10001: %04o", path, info.Mode().Perm())
			}
			return nil
		}); err != nil {
			t.Fatalf("walk readable projection: %v", err)
		}
	})
	t.Run("writable", func(t *testing.T) {
		root := build(t)
		path := projectionHostPath(root, staticPath)
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := verifyProjection(root, access, manifest); err == nil || !strings.Contains(err.Error(), "mode is mutable") {
			t.Fatalf("verifyProjection error = %v, want writable-mode rejection", err)
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
		if err := os.Chmod(path, 0o444); err != nil {
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
		if err := os.Chmod(parent, 0o555); err != nil {
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
		if err := os.Chmod(root, 0o555); err != nil {
			t.Fatal(err)
		}
		if err := verifyProjection(root, access, manifest); err == nil || !strings.Contains(err.Error(), "unexpected entry") {
			t.Fatalf("verifyProjection error = %v", err)
		}
	})
}

func TestProjectionValidateRejectsMissingOrMalformedIdentity(t *testing.T) {
	root := filepath.Clean(t.TempDir())
	for _, projection := range []Projection{
		{Root: root},
		{ID: ProjectionID("data-projection-v1:sha256:deadbeef"), Root: root},
		{ID: ProjectionID("data-projection-v1:sha256:" + strings.Repeat("A", 64)), Root: root},
		{ID: ProjectionID("data-projection-v1:sha256:" + strings.Repeat("a", 64)), Root: "relative"},
	} {
		if err := projection.Validate(); err == nil {
			t.Fatalf("Projection.Validate(%#v) error = nil, want fail-closed rejection", projection)
		}
	}
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
