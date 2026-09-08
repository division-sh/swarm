package serveapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, condition := range []string{"missing", "corrupt", "hash mismatch"} {
			t.Run(backend+"/"+condition, func(t *testing.T) {
				ctx := context.Background()
				var db *sql.DB
				var artifacts interface {
					EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
					BootstrapSchema(context.Context, store.SchemaBootstrapRequest) error
				}
				var sqliteStore *store.SQLiteRuntimeStore
				var configPath, sqlitePath string
				if backend == "postgres" {
					_, database, _ := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
					db, artifacts = database, storetest.NewPostgresStoreForTest(database)
					configPath = writeServeRuntimeTestConfig(t)
				} else {
					isolateCLIAPIConfigEnv(t)
					sqlitePath = filepath.Join(t.TempDir(), "runtime.db")
					configPath = writeStoreBackendRuntimeConfig(t, "sqlite", sqlitePath)
					selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
					if err != nil {
						t.Fatal(err)
					}
					sqliteStore = selected
					t.Cleanup(func() { _ = selected.Close() })
					db, artifacts = storetest.DatabaseForTest(selected), selected
				}
				spec, err := loadServePlatformSpecDocument(filepath.Join(repoRootForTest(), defaultPlatformSpecPath))
				if err != nil {
					t.Fatal(err)
				}
				plans, err := store.GeneratePlatformTableDDLs(spec)
				if err != nil {
					t.Fatal(err)
				}
				if err := artifacts.BootstrapSchema(ctx, store.SchemaBootstrapRequest{
					PlatformPlans: plans,
					Origin:        store.RuntimeStoreOrigin{SwarmVersion: "startup-integrity-test", PlatformVersion: "test", CreatedAt: time.Now().UTC()},
				}); err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: historical-source\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := artifacts.EnsureSourceArtifact(ctx, artifact); err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: artifact.BundleHash()}
				placeholder := "?"
				if backend == "sqlite" {
					runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
				} else {
					placeholder = "$1"
					runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
				}
				// Deliberately corrupt a valid historical record after canonical admission.
				// Serve loads a different authored source and must not repair this one.
				if condition == "missing" {
					_, err = db.ExecContext(ctx, "DELETE FROM source_artifacts WHERE bundle_hash = "+placeholder, artifact.BundleHash())
				} else {
					blob := []byte{0}
					if condition == "hash mismatch" {
						if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: replacement-source\n"), 0o600); err != nil {
							t.Fatal(err)
						}
						other, err := sourceartifact.AdmitDirectory(root)
						if err != nil {
							t.Fatal(err)
						}
						blob = other.LogicalBlob()
					}
					query := "UPDATE source_artifacts SET source_blob = ? WHERE bundle_hash = ?"
					if backend == "postgres" {
						query = "UPDATE source_artifacts SET source_blob = $1::bytea WHERE bundle_hash = $2"
					}
					_, err = db.ExecContext(ctx, query, blob, artifact.BundleHash())
				}
				if err != nil {
					t.Fatal(err)
				}
				if sqliteStore != nil {
					if err := sqliteStore.Close(); err != nil {
						t.Fatal(err)
					}
				}
				var out lockedBuffer
				published := false
				code := runFrom(ctx, repoRootForTest(), cliapp.ServeOptions{
					ConfigPath: configPath, SourceRoot: filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
					PlatformSpecPath: defaultPlatformSpecPath, StoreMode: backend, StoreModeSet: true,
					APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
					Output: &out, TestLLMRuntime: servedNoopLLMRuntime{},
					TestRuntimeContextsReadyHook: func(*runtime.RuntimeContextManager) { published = true },
				})
				if code == 0 || published || strings.Contains(out.String(), "ready in ") {
					t.Fatalf("invalid historical source reached readiness: code=%d published=%v\n%s", code, published, out.String())
				}
				if !strings.Contains(out.String(), runID) || !strings.Contains(out.String(), artifact.BundleHash()) {
					t.Fatalf("boot did not identify the invalid run and source: %s", out.String())
				}
				if backend == "sqlite" {
					selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
					if err != nil {
						t.Fatal(err)
					}
					defer selected.Close()
					db = storetest.DatabaseForTest(selected)
				}
				var status string
				if err := db.QueryRowContext(ctx, "SELECT status FROM runs WHERE run_id = "+placeholder, runID).Scan(&status); err != nil || status != "running" {
					t.Fatalf("startup changed historical run: status=%s err=%v", status, err)
				}
			})
		}
	}
}
