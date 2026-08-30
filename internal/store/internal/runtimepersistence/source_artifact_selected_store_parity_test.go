package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
)

type selectedSourceArtifactStore interface {
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
}

func TestSourceArtifactSelectedStoreParity(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: source-artifact-parity\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit source artifact: %v", err)
	}

	for _, fixture := range []struct {
		name        string
		open        func(*testing.T) (selectedSourceArtifactStore, *sql.DB)
		placeholder string
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (selectedSourceArtifactStore, *sql.DB) {
				selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return selected, selected.backend.ConstructionHandle()
			},
			placeholder: "?",
		},
		{
			name: "postgres",
			open: func(t *testing.T) (selectedSourceArtifactStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return newTestPostgresStore(t, db), db
			},
			placeholder: "$1",
		},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := fixture.open(t)

			first, err := selected.EnsureSourceArtifact(ctx, artifact)
			if err != nil || !first.Created || first.Artifact.BundleHash != artifact.BundleHash() {
				t.Fatalf("first ensure = %#v, %v", first, err)
			}
			stored, err := selected.GetSourceArtifact(ctx, artifact.BundleHash())
			if err != nil {
				t.Fatalf("get source artifact: %v", err)
			}
			decoded, err := stored.Decode()
			if err != nil {
				t.Fatalf("decode source artifact: %v", err)
			}
			if decoded.BundleHash() != artifact.BundleHash() || !bytes.Equal(decoded.LogicalBlob(), artifact.LogicalBlob()) {
				t.Fatalf("decoded artifact = hash:%q blob_equal:%t", decoded.BundleHash(), bytes.Equal(decoded.LogicalBlob(), artifact.LogicalBlob()))
			}
			second, err := selected.EnsureSourceArtifact(ctx, artifact)
			if err != nil || second.Created || !bytes.Equal(second.Artifact.SourceBlob, artifact.LogicalBlob()) {
				t.Fatalf("reconciled ensure = %#v, %v", second, err)
			}

			if _, err := db.ExecContext(ctx, "UPDATE source_artifacts SET source_blob = "+sourceArtifactCorruptBlobSQL(fixture.name)+" WHERE bundle_hash = "+fixture.placeholder, artifact.BundleHash()); err != nil {
				t.Fatalf("corrupt source artifact: %v", err)
			}
			if _, err := selected.GetSourceArtifact(ctx, artifact.BundleHash()); err == nil {
				t.Fatal("corrupt source artifact readback succeeded")
			}
		})
	}
}

func sourceArtifactCorruptBlobSQL(backend string) string {
	if backend == "postgres" {
		return "decode('00', 'hex')"
	}
	return "X'00'"
}
