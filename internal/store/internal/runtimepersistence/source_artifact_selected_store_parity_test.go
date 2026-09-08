package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type selectedSourceArtifactStore interface {
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
}

func TestSourceArtifactStartupIntegrityParityPreservesRunHistory(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				selectedSourceArtifactStore
				startuprecovery.AvailabilityReader
			}
			var db *sql.DB
			placeholder := "?"
			if backend == "sqlite" {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = s, s.backend.ConstructionHandle()
			} else {
				_, database, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, db = newTestPostgresStore(t, database), database
				placeholder = "$1"
			}
			ctx := testAuthorActivityContext()
			for i, condition := range []string{"valid", "missing", "corrupt", "hash mismatch"} {
				t.Run(condition, func(t *testing.T) {
					root := t.TempDir()
					if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte(fmt.Sprintf("name: startup-integrity-%d\n", i)), 0o600); err != nil {
						t.Fatal(err)
					}
					artifact, err := sourceartifact.AdmitDirectory(root)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := selected.EnsureSourceArtifact(ctx, artifact); err != nil {
						t.Fatal(err)
					}
					runID := uuid.NewString()
					fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: artifact.BundleHash()}
					if backend == "sqlite" {
						runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
					} else {
						runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
					}
					setBlob := func(blob []byte) {
						t.Helper()
						query := "UPDATE source_artifacts SET source_blob = ? WHERE bundle_hash = ?"
						if backend == "postgres" {
							query = "UPDATE source_artifacts SET source_blob = $1::bytea WHERE bundle_hash = $2"
						}
						if _, err := db.ExecContext(ctx, query, blob, artifact.BundleHash()); err != nil {
							t.Fatal(err)
						}
					}
					switch condition {
					case "missing":
						if _, err := db.ExecContext(ctx, "DELETE FROM source_artifacts WHERE bundle_hash = "+placeholder, artifact.BundleHash()); err != nil {
							t.Fatal(err)
						}
					case "corrupt":
						setBlob([]byte{0})
					case "hash mismatch":
						if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: different\n"), 0o600); err != nil {
							t.Fatal(err)
						}
						other, err := sourceartifact.AdmitDirectory(root)
						if err != nil {
							t.Fatal(err)
						}
						setBlob(other.LogicalBlob())
					}
					_, err = startuprecovery.Recover(ctx, startuprecovery.Request{AvailabilityReader: selected, ArtifactReader: selected})
					if (err == nil) != (condition == "valid") {
						t.Fatalf("recovery = %v", err)
					}
					var status string
					if err := db.QueryRowContext(ctx, "SELECT status FROM runs WHERE run_id = "+placeholder, runID).Scan(&status); err != nil || status != "running" {
						t.Fatalf("recovery altered run history: status=%s, err=%v", status, err)
					}
					// Remove only this test's injected corruption before the next census.
					if condition == "missing" {
						if _, err := selected.EnsureSourceArtifact(ctx, artifact); err != nil {
							t.Fatal(err)
						}
					} else {
						setBlob(artifact.LogicalBlob())
					}
				})
			}
		})
	}
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
