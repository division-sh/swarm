package cliapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/google/uuid"
)

type closeErrorAuthorityInspectionStore struct {
	inspection runtimestartupownership.AuthorityInspection
	inspectErr error
	closeErr   error
	closeCalls int
}

func (s *closeErrorAuthorityInspectionStore) InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error) {
	return s.inspection, s.inspectErr
}

func (s *closeErrorAuthorityInspectionStore) Close() error {
	s.closeCalls++
	return s.closeErr
}

func TestAuthorityInspectionPropagatesOperationAndCloseErrors(t *testing.T) {
	store := &closeErrorAuthorityInspectionStore{
		inspectErr: errors.New("inspect failed"), closeErr: errors.New("inspection close failed"),
	}
	_, err := inspectAuthorityAndClose(context.Background(), store)
	for _, want := range []string{"inspect failed", "inspection close failed"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("inspect error = %v, want %q", err, want)
		}
	}
	if store.closeCalls != 1 {
		t.Fatalf("inspection close calls = %d, want 1", store.closeCalls)
	}
}

func TestStoreStatusReadsSelectedStoreOwnershipWithoutContextInference(t *testing.T) {
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "missing", "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)

	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 || stderr != "" {
		t.Fatalf("store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	if stdout != "Project owner: selected store has not been created.\n" {
		t.Fatalf("store status = %q", stdout)
	}
	if strings.Contains(strings.ToLower(stdout), "context") {
		t.Fatalf("store status inferred ownership from a context: %q", stdout)
	}
	if _, err := os.Stat(filepath.Dir(sqlitePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("store status created selected SQLite parent: %v", err)
	}
}

func TestStoreStatusReadsCurrentSQLiteStoreWithoutMutationBootstrap(t *testing.T) {
	unsetStoreSelectorEnv(t)
	path := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, path)
	bootstrapStoreAuthoritySQLite(t, configPath, path, "durable-status-owner")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	coordinate, err := os.Stat(path + ".possession")
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 || stderr != "" || stdout != "Project owner: selected-store lease is available (last admitted generation 1, owner durable-status-owner).\n" {
		t.Fatalf("read-only status: code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("status mutated selected database: before=%#v after=%#v", before, after)
	}
	afterCoordinate, err := os.Stat(path + ".possession")
	if err != nil || !os.SameFile(coordinate, afterCoordinate) {
		t.Fatalf("status replaced possession coordinate: %v", err)
	}
}

func TestStoreStatusRejectsMissingCoordinateWithoutRecreatingIt(t *testing.T) {
	unsetStoreSelectorEnv(t)
	path := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, path)
	bootstrapStoreAuthoritySQLite(t, configPath, path, "inactive-owner")
	if err := os.Remove(path + ".possession"); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code == 0 || !strings.Contains(stdout+stderr, "possession coordinate was missing") {
		t.Fatalf("status inferred live ownership from a stamp without its coordinate: code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if _, err := os.Stat(path + ".possession"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only status recreated coordinate: %v", err)
	}
}

func TestStoreStatusDoesNotBootstrapEmptyPostgres(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.SourceEnv)) == "" {
		t.Skip("requires canonical PostgreSQL test harness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	manager, err := testpostgres.ManagerFromEnvironment(ctx)
	if err != nil {
		t.Fatalf("construct PostgreSQL test manager: %v", err)
	}
	sandbox, err := manager.Acquire(ctx, false)
	if err != nil {
		t.Fatalf("acquire empty PostgreSQL sandbox: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := sandbox.Release(cleanupCtx); err != nil {
			t.Errorf("release PostgreSQL sandbox: %v", err)
		}
	})
	dsn, err := sandbox.Connection.String()
	if err != nil {
		t.Fatalf("serialize PostgreSQL sandbox connection: %v", err)
	}
	setPostgresEnvFromDSN(t, dsn)
	configPath := os.Getenv("SWARM_CONFIG")

	stdout, stderr, code := runStoreAuthorityCommandWithStore(t, configPath, "postgres", "status")
	if code != 0 || stderr != "" || stdout != "Project owner: selected-store lease is available; no process generation has been admitted.\n" {
		t.Fatalf("PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	var publicObjects int
	if err := sandbox.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')`).Scan(&publicObjects); err != nil {
		t.Fatalf("inspect PostgreSQL catalog: %v", err)
	}
	if publicObjects != 0 {
		t.Fatalf("store status created %d PostgreSQL public objects", publicObjects)
	}

	selected := storetest.AdmitPostgresRuntimeStore(t, sandbox.DB)
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "durable-postgres-status-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("construct PostgreSQL authority: %v", err)
	}
	before, err := selectedtest.InspectProcessLeaseSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code = runStoreAuthorityCommandWithStore(t, configPath, "postgres", "status")
	if code != 0 || stderr != "" || stdout != "Project owner: selected-store lease is held (last admitted generation 1, owner durable-postgres-status-owner).\n" {
		t.Fatalf("held PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	after, err := selectedtest.InspectProcessLeaseSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("status changed held process snapshot: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := capability.ProveCurrent(ctx); err != nil {
		t.Fatalf("status released the actual PostgreSQL lease: %v", err)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code = runStoreAuthorityCommandWithStore(t, configPath, "postgres", "status")
	if code != 0 || stderr != "" || stdout != "Project owner: selected-store lease is available (last admitted generation 1, owner durable-postgres-status-owner).\n" {
		t.Fatalf("current PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
}

func TestPlatformSpecBindsOwnershipLossDeadlinesAndReadOnlyStatus(t *testing.T) {
	var spec struct {
		PlatformTables struct {
			Tables map[string]struct {
				PossessionLossContract       string `yaml:"possession_loss_contract"`
				DiagnosticInspectionContract string `yaml:"diagnostic_inspection_contract"`
			} `yaml:"tables"`
		} `yaml:"platform_tables"`
		AgentTopologyAuthority struct {
			ProcessCapability struct {
				Acquisition             string `yaml:"acquisition"`
				Grants                  string `yaml:"grants"`
				LifecycleReconciliation string `yaml:"lifecycle_reconciliation"`
				Inspection              struct {
					Rule string `yaml:"rule"`
				} `yaml:"inspection"`
			} `yaml:"process_capability"`
		} `yaml:"agent_topology_authority"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	for _, retired := range []string{"runtime_startup_authority_facts", "runtime_startup_authority_repairs"} {
		if _, exists := spec.PlatformTables.Tables[retired]; exists {
			t.Fatalf("retired process journal remains in fresh DDL: %s", retired)
		}
	}
	generation := spec.PlatformTables.Tables["runtime_process_generation"]
	if !strings.Contains(generation.PossessionLossContract, "monitor deadline") ||
		!strings.Contains(generation.PossessionLossContract, "while accepted work survives") {
		t.Fatalf("loss contract omitted bounded proof or retained possession: %q", generation.PossessionLossContract)
	}
	if !strings.Contains(generation.DiagnosticInspectionContract, "without mutation") ||
		!strings.Contains(generation.DiagnosticInspectionContract, "MVCC admission snapshot") ||
		!strings.Contains(generation.DiagnosticInspectionContract, "No inspector creates") {
		t.Fatalf("read-only inspection contract is incomplete: %q", generation.DiagnosticInspectionContract)
	}
	process := spec.AgentTopologyAuthority.ProcessCapability
	for _, required := range []string{"backend-file identity", "pool bound to one inode", "<canonical database path>.possession",
		"another SQLite-managed file are prohibited", "complete pair", "unsupported filesystem tampering",
		"acknowledged native COMMIT", "there is no process journal"} {
		if !strings.Contains(strings.ToLower(process.Acquisition), strings.ToLower(required)) {
			t.Fatalf("acquisition contract omitted %q", required)
		}
	}
	if !strings.Contains(process.Grants, "error can never publish successful release") || !strings.Contains(process.Grants, "configured monitor deadline") {
		t.Fatalf("grant contract does not bind failed release classification: %q", process.Grants)
	}
	for _, required := range []string{"topology_authority_kind", "execution_lifetime", "fails closed"} {
		if !strings.Contains(process.LifecycleReconciliation, required) {
			t.Fatalf("lifecycle census omitted %q", required)
		}
	}
	if !strings.Contains(process.Inspection.Rule, "cannot release") || !strings.Contains(process.Inspection.Rule, "before accepted work and exact disposal join") {
		t.Fatalf("inspection creates early unlock permission: %q", process.Inspection.Rule)
	}
}

func TestStoreHasNoRepairMutationCommand(t *testing.T) {
	unsetStoreSelectorEnv(t)
	path := filepath.Join(t.TempDir(), "uncreated", "selected.db")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"store", "repair-authority"}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 || !strings.Contains(stdout.String()+stderr.String(), "unknown command") {
		t.Fatalf("retired repair command remains admitted: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unknown command touched selected-store infrastructure: %v", err)
	}
}

func runStoreAuthorityCommand(t *testing.T, configPath string, args ...string) (string, string, int) {
	t.Helper()
	return runStoreAuthorityCommandWithStore(t, configPath, "sqlite", args...)
}

func runStoreAuthorityCommandWithStore(t *testing.T, configPath, backend string, args ...string) (string, string, int) {
	t.Helper()
	commandArgs := append([]string{"store"}, args...)
	commandArgs = append(commandArgs, "--config", configPath, "--store", backend)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), commandArgs, &stdout, &stderr, defaultRootCommandOptions())
	return stdout.String(), stderr.String(), code
}

func bootstrapStoreAuthoritySQLite(t *testing.T, configPath, sqlitePath, ownerID string) {
	t.Helper()
	paths, err := ResolveCLISourcePaths(RepoRoot(), CLISourcePathOptions{
		ConfigPath: configPath,
		SourceRoot: filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success"),
	})
	if err != nil {
		t.Fatalf("resolve selected-store schema paths: %v", err)
	}
	_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), paths.SourceRoot)
	if err != nil {
		t.Fatalf("load selected-store schema source: %v", err)
	}
	plans, err := StateStoreSchemaPlans(bundle)
	if err != nil {
		t.Fatalf("generate selected-store schema plans: %v", err)
	}
	selected, err := selectedtest.OpenProcessLeaseFixture(context.Background(), backendselection.Selection{Backend: backendselection.BackendSQLite, SQLitePath: sqlitePath}, "")
	if err != nil {
		t.Fatalf("open selected SQLite store: %v", err)
	}
	defer selected.Close()
	if err := selected.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans.Platform,
		StatePlans:    plans.State,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: "store-authority-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("bootstrap selected SQLite store: %v", err)
	}
	capability, err := selected.AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
		OwnerID: ownerID, BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := selectedtest.InspectProcessLeaseSnapshot(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 || stderr != "" || stdout != fmt.Sprintf("Project owner: selected-store lease is held (last admitted generation 1, owner %s).\n", ownerID) {
		t.Fatalf("held SQLite store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	after, err := selectedtest.InspectProcessLeaseSnapshot(context.Background(), selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("status changed held process snapshot: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("status released the actual SQLite lease: %v", err)
	}
	if err := capability.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func writeStoreAuthorityConfig(t *testing.T, sqlitePath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, path, fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %q\n", sqlitePath))
	return path
}
