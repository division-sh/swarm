package cliapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
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

func (s *closeErrorAuthorityInspectionStore) InspectStartupStatus(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	return s.InspectAuthority(ctx)
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
	for _, projectDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("project_default_%t", projectDefault), func(t *testing.T) {
			unsetStoreSelectorEnv(t)
			repo := RepoRoot()
			path := filepath.Join(t.TempDir(), "selected.db")
			configPath := writeStoreAuthorityConfig(t, path)
			if projectDefault {
				repo = t.TempDir()
				path = filepath.Join(repo, ".swarm", "stores", "dev.db")
				configPath = writeStoreAuthorityConfig(t, "")
				stdout, stderr, code := runStoreAuthorityCommandFrom(t, repo, configPath, "sqlite", "status")
				if code != 0 || stderr != "" || !strings.Contains(stdout, "has not been created") {
					t.Fatalf("absent project store: code=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("status created the absent project store parent: %v", err)
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			bootstrapStoreAuthoritySQLite(t, configPath, path, "durable-status-owner", repo)
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			coordinate, err := os.Stat(path + ".possession")
			if err != nil {
				t.Fatal(err)
			}
			stdout, stderr, code := runStoreAuthorityCommandFrom(t, repo, configPath, "sqlite", "status")
			if code != 0 || stderr != "" {
				t.Fatalf("read-only status: code=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
			requireStoreReadout(t, stdout, "not held")
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
		})
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
	if code != 0 || stderr != "" || stdout != "Project owner: selected-store lease is not held; no process generation has been admitted.\n" {
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
	if code != 0 || stderr != "" {
		t.Fatalf("held PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	requireStoreReadout(t, stdout, "held")
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
	if code != 0 || stderr != "" {
		t.Fatalf("current PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	requireStoreReadout(t, stdout, "not held")
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
	return runStoreAuthorityCommandFrom(t, RepoRoot(), configPath, backend, args...)
}

func runStoreAuthorityCommandFrom(t *testing.T, repo, configPath, backend string, args ...string) (string, string, int) {
	t.Helper()
	commandArgs := append([]string{"store"}, args...)
	commandArgs = append(commandArgs, "--config", configPath, "--store", backend)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, commandArgs, &stdout, &stderr, defaultRootCommandOptions())
	return stdout.String(), stderr.String(), code
}

func bootstrapStoreAuthoritySQLite(t *testing.T, configPath, sqlitePath, ownerID string, invocationRoots ...string) {
	t.Helper()
	repo := RepoRoot()
	if len(invocationRoots) != 0 {
		repo = invocationRoots[0]
	}
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
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: "store-authority-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("bootstrap selected SQLite store: %v", err)
	}
	runtimeID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
		OwnerID: ownerID, BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
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
	stdout, stderr, code := runStoreAuthorityCommandFrom(t, repo, configPath, "sqlite", "status")
	if code != 0 || stderr != "" || !strings.Contains(stdout, "last admitted serve "+runtimeID+" (generation 1)") {
		t.Fatalf("held SQLite store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	requireStoreReadout(t, stdout, "held")
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

func requireStoreReadout(t *testing.T, output, held string) {
	t.Helper()
	pattern := `^Project owner: selected-store lease is ` + held + `; last admitted serve ([0-9a-f-]{36}) \(generation 1\); interrupted: 0 provider turns to resume, 0 provider turns to dead-letter, 0 HTTP tool calls outcome unknown\n$`
	match := regexp.MustCompile(pattern).FindStringSubmatch(output)
	if len(match) != 2 {
		t.Fatalf("store readout = %q", output)
	}
	if id, err := uuid.Parse(match[1]); err != nil || id == uuid.Nil || id.String() != match[1] {
		t.Fatalf("invalid last-admitted instance: %q", match[1])
	}
}

func writeStoreAuthorityConfig(t *testing.T, sqlitePath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	body := "store:\n  backend: sqlite\n"
	if sqlitePath != "" {
		body += fmt.Sprintf("  sqlite:\n    path: %q\n", sqlitePath)
	}
	writeRuntimeConfigText(t, path, body)
	return path
}
