package serveapp

import (
	"github.com/division-sh/swarm/tests/support/channelfixture"

	"bytes"
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, condition := range []string{"missing", "corrupt", "hash mismatch", "project default"} {
			if backend == "postgres" && condition == "project default" {
				continue
			}
			t.Run(backend+"/"+condition, func(t *testing.T) {
				ctx := context.Background()
				repo := repoRootForTest()
				sourceRoot := filepath.Join("tests", "tier8-boot-verification", "test-boot-success")
				var db *sql.DB
				var artifacts interface {
					EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
					EnsureRootedSource(context.Context, sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error)
					BootstrapSchema(context.Context, store.SchemaBootstrapRequest) error
				}
				var sqliteStore *store.SQLiteRuntimeStore
				var configPath, sqlitePath string
				if backend == "postgres" {
					_, database, _ := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
					db, artifacts = database, storetest.NewPostgresStoreForTest(database, channelfixture.Facts)
					configPath = writeServeRuntimeTestConfig(t)
				} else {
					isolateCLIAPIConfigEnv(t)
					sqlitePath = filepath.Join(t.TempDir(), "runtime.db")
					configPath = writeStoreBackendRuntimeConfig(t, "sqlite", sqlitePath)
					if condition == "project default" {
						repo = t.TempDir()
						sourceRoot = repo
						sqlitePath = filepath.Join(repo, ".swarm", "stores", "dev.db")
						configPath = writeStoreBackendRuntimeConfig(t, "", "")
						if err := os.MkdirAll(filepath.Dir(sqlitePath), 0o700); err != nil {
							t.Fatal(err)
						}
						for name, body := range map[string]string{"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "schema.yaml": "# current project, not the missing historical source\nstages: []\n"} {
							if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o600); err != nil {
								t.Fatal(err)
							}
						}
					}
					selected, err := store.NewSQLiteRuntimeStore(sqlitePath, channelfixture.Facts)
					if err != nil {
						t.Fatal(err)
					}
					sqliteStore = selected
					t.Cleanup(func() { _ = selected.Close() })
					db, artifacts = storetest.DatabaseForTest(selected), selected
				}
				spec, err := loadServePlatformSpecDocument()
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
				if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("version: 1.0.0\nplatform_version: '*'\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("stages: []\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				rooted, err := artifact.RootedSource()
				if err != nil {
					t.Fatal(err)
				}
				setupCtx := runtimecorrelation.WithRootAdmission(runtimecorrelation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(artifact)), rooted.Admission())
				setupCtx = serveFixtureProcessContext(t, setupCtx, artifacts)
				if _, err := artifacts.EnsureRootedSource(setupCtx, rooted); err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				storetest.RequireRun(t, setupCtx, artifacts.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: artifact.BundleHash(), RootAdmission: rooted.Admission()})
				releaseServeTestAgentFixtureCapability(t, artifacts.(storetest.AgentFixtureStore))
				placeholder := "?"
				if backend != "sqlite" {
					placeholder = "$1"
				}
				// Deliberately corrupt a valid historical record after canonical admission.
				// Serve loads a different authored source and must not repair this one.
				if condition == "missing" || condition == "project default" {
					_, err = db.ExecContext(ctx, "DELETE FROM source_artifacts WHERE bundle_hash = "+placeholder, artifact.BundleHash())
				} else {
					blob := []byte{0}
					if condition == "hash mismatch" {
						if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("# distinct retained bytes\nstages: []\n"), 0o600); err != nil {
							t.Fatal(err)
						}
						other, err := sourceartifact.AdmitDirectory(root)
						if err != nil {
							t.Fatal(err)
						}
						if other.BundleHash() == artifact.BundleHash() {
							t.Fatal("hash-mismatch fixture must have distinct admitted bytes")
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
				code := runFrom(ctx, repo, cliapp.ServeOptions{
					ConfigPath: configPath, SourceRoot: sourceRoot,
					StoreMode: backend, StoreModeSet: true,
					APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
					Output: &out, TestLLMRuntime: servedNoopLLMRuntime{},
					TestRuntimeContextsReadyHook: func(*runtime.RuntimeContextManager) { published = true },
				})
				wantCode := 3
				if condition == "missing" || condition == "project default" {
					wantCode = serveExitDataIntegrity
				}
				if code != wantCode || published || strings.Contains(out.String(), "ready in ") {
					t.Fatalf("invalid historical source reached readiness: code=%d published=%v\n%s", code, published, out.String())
				}
				if !strings.Contains(out.String(), runID) || !strings.Contains(out.String(), artifact.BundleHash()) {
					t.Fatalf("boot did not identify the invalid run and source: %s", out.String())
				}
				for _, want := range []string{
					"ERROR: startup recovery refused",
					"step 5 (startup recovery)",
					"doctor --config '" + configPath + "'",
				} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("recovery diagnostic missing %q:\n%s", want, out.String())
					}
				}
				if condition == "project default" {
					if !strings.Contains(out.String(), "store status") || !strings.Contains(out.String(), sqlitePath) {
						t.Fatalf("default-store refusal omitted the reproducible inspection or actual path:\n%s", out.String())
					}
					var command string
					for _, line := range strings.Split(out.String(), "\n") {
						if after, ok := strings.CutPrefix(strings.TrimSpace(line), "Inspect the selected store: "); ok {
							command = after
						}
					}
					binaryDir := t.TempDir()
					if err := testplanning.BuildGoProduct(t.Context(), repoRootForTest(), os.Getenv("SWARM_TEST_BUILD_CACHE"), filepath.Join(binaryDir, "swarm"), os.Getenv("SWARM_TEST_PROOF_PROFILE"), "build", "./cmd/swarm"); err != nil {
						t.Fatalf("build public CLI: %v", err)
					}
					before, err := os.ReadFile(sqlitePath)
					if err != nil {
						t.Fatal(err)
					}
					inspect := exec.Command("sh", "-c", command)
					inspect.Env = append(releaseProviderTriggerProcessEnv(), "PATH="+binaryDir+string(os.PathListSeparator)+os.Getenv("PATH"))
					if output, err := inspect.CombinedOutput(); err != nil || !strings.Contains(string(output), "selected-store lease is not held") || !strings.Contains(string(output), "last admitted serve") {
						t.Fatalf("emitted command did not inspect the real project store: %v\n%s", err, output)
					}
					after, err := os.ReadFile(sqlitePath)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("emitted read-only command changed the project database: %v", err)
					}
				} else if !strings.Contains(out.String(), "store status --store '"+backend+"' --config '"+configPath+"'") {
					t.Fatalf("reproducible store inspection lost its configuration:\n%s", out.String())
				}
				if backend == "sqlite" {
					selected, err := store.NewSQLiteRuntimeStore(sqlitePath, channelfixture.Facts)
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
