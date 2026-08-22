package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/google/uuid"
)

func TestStoreStatusReadsSelectedStoreOwnershipWithoutContextInference(t *testing.T) {
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "missing", "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)

	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 || stderr != "" {
		t.Fatalf("store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	if stdout != "Project owner: no previous session is recorded.\n" {
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
	sqlitePath := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)
	bootstrapStoreAuthoritySQLite(t, configPath, sqlitePath)
	seedValidAuthorityHead(t, sqlitePath, "durable-status-owner")
	beforeInfo, err := os.Stat(sqlitePath)
	if err != nil {
		t.Fatalf("stat selected SQLite store before status: %v", err)
	}

	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 || stderr != "" {
		t.Fatalf("store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "session durable-status-owner is recorded as active") {
		t.Fatalf("store status = %q", stdout)
	}
	afterInfo, err := os.Stat(sqlitePath)
	if err != nil {
		t.Fatalf("stat selected SQLite store after status: %v", err)
	}
	if beforeInfo.Size() != afterInfo.Size() || !beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
		t.Fatalf("store status mutated selected SQLite database: before=(%d,%s) after=(%d,%s)", beforeInfo.Size(), beforeInfo.ModTime(), afterInfo.Size(), afterInfo.ModTime())
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
	if code != 0 || stderr != "" || stdout != "Project owner: no previous session is recorded.\n" {
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
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "durable-postgres-status-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "postgres_retained_session")
	if err != nil {
		t.Fatalf("construct PostgreSQL authority: %v", err)
	}
	snapshot, err := canonicaljson.Bytes(authority)
	if err != nil {
		t.Fatalf("encode PostgreSQL authority: %v", err)
	}
	if _, err := storetest.DatabaseForTest(selected).ExecContext(ctx, `
		INSERT INTO runtime_startup_authority_facts (
			fact_id, authority_id, authority_generation, transition_ordinal,
			state_version, state, owner_id, boot_id, runtime_instance_id,
			backend, acquisition_id, acquisition_request_hash, acquisition_kind,
			predecessor_authority_id, successor_authority_id, snapshot, created_at
		) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8::uuid,$9::uuid,$10,$11::uuid,$12,$13,NULL,NULL,$14::jsonb,$15)
	`, uuid.NewString(), authority.AuthorityID, authority.AuthorityGeneration, authority.TransitionOrdinal,
		authority.StateVersion, authority.State, authority.OwnerID, authority.BootID, authority.RuntimeInstanceID,
		authority.Backend, authority.AcquisitionID, authority.AcquisitionRequestHash, authority.AcquisitionKind,
		string(snapshot), authority.RecordedAt.UTC()); err != nil {
		t.Fatalf("seed PostgreSQL authority: %v", err)
	}
	stdout, stderr, code = runStoreAuthorityCommandWithStore(t, configPath, "postgres", "status")
	if code != 0 || stderr != "" || !strings.Contains(stdout, "session durable-postgres-status-owner is recorded as active") {
		t.Fatalf("current PostgreSQL store status = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
}

func TestPlatformSpecBindsOwnershipLossDeadlinesAndReadOnlyStatus(t *testing.T) {
	var spec struct {
		PlatformTables struct {
			Tables map[string]struct {
				PossessionLossContract struct {
					OperationErrorRecheck string `yaml:"operation_error_recheck"`
					TerminalReadback      string `yaml:"terminal_readback"`
				} `yaml:"possession_loss_contract"`
				DiagnosticInspectionContract string `yaml:"diagnostic_inspection_contract"`
			} `yaml:"tables"`
		} `yaml:"platform_tables"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	authority := spec.PlatformTables.Tables["runtime_startup_authority_facts"]
	for name, value := range map[string]string{
		"operation error recheck": authority.PossessionLossContract.OperationErrorRecheck,
		"terminal readback":       authority.PossessionLossContract.TerminalReadback,
		"diagnostic inspection":   authority.DiagnosticInspectionContract,
	} {
		if !strings.Contains(value, "monitor deadline") && name != "diagnostic inspection" {
			t.Fatalf("%s contract does not bind the configured monitor deadline: %q", name, value)
		}
		if name == "diagnostic inspection" && (!strings.Contains(value, "read-only") || !strings.Contains(value, "never creates")) {
			t.Fatalf("diagnostic inspection contract does not prohibit bootstrap mutation: %q", value)
		}
	}
}

func TestDoctorSeparatesContextDiscoveryFromProjectOwnership(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)
	bootstrapStoreAuthoritySQLite(t, configPath, sqlitePath)
	seedValidAuthorityHead(t, sqlitePath, "durable-selected-store-owner")

	swarmDir := filepath.Join(t.TempDir(), "state")
	contractsPath := filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success")
	projectRoot, canonicalStatus := canonicalizeDoctorTargetPath(contractsPath)
	if canonicalStatus != "resolved" {
		t.Fatalf("canonical project status = %q", canonicalStatus)
	}
	registry := newLocalContextRegistry(swarmDir)
	first := startCLIAPIRuntimeIdentityServer(t, "context-runtime-one")
	second := startCLIAPIRuntimeIdentityServer(t, "context-runtime-two")
	writeCLIAPITestContext(t, registry, "one", "context-runtime-one", first.URL, projectRoot)
	writeCLIAPITestContext(t, registry, "two", "context-runtime-two", second.URL, projectRoot)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"--swarm-dir", swarmDir,
		"doctor", "--target", "--json",
		"--api-server", "http://127.0.0.1:19009",
		"--contracts", contractsPath,
		"--config", configPath,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK || stderr.String() != "" {
		t.Fatalf("doctor target = code:%d stdout:%s stderr:%s", code, stdout.String(), stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor target: %v\n%s", err, stdout.String())
	}
	if report.Context.ProjectScoped.Status != "multiple_live" || report.Context.ProjectScoped.Owner != localContextRegistryOwner {
		t.Fatalf("project context = %#v, want distinct multiple-live discovery fact", report.Context.ProjectScoped)
	}
	if report.ProjectOwner.Status != string(runtimestartupownership.StateActive) ||
		report.ProjectOwner.Owner != selectedStoreOwnerReader ||
		!strings.Contains(report.ProjectOwner.Detail, "durable-selected-store-owner") {
		t.Fatalf("project owner = %#v, want canonical selected-store fact", report.ProjectOwner)
	}
	if strings.Contains(report.ProjectOwner.Detail, "context-runtime") {
		t.Fatalf("project owner inferred from endpoint context: %#v", report.ProjectOwner)
	}
}

func TestStoreRepairAuthorityRequiresExactFindingsAndJournalsRepair(t *testing.T) {
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)
	bootstrapStoreAuthoritySQLite(t, configPath, sqlitePath)
	seedCorruptAuthorityHead(t, sqlitePath)

	stdout, stderr, code := runStoreAuthorityCommand(t, configPath, "repair-authority")
	if code != CLIExitValidation || stderr != "" {
		t.Fatalf("repair inspection = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	for _, want := range []string{
		"The recorded project session is inconsistent.",
		"Plan: replace only the broken session record with a clean stopped session.",
		"Your flows, runs, and data are untouched.",
		"Re-run with --confirm sha256:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("repair inspection omitted %q:\n%s", want, stdout)
		}
	}
	digest := regexp.MustCompile(`--confirm (sha256:[0-9a-f]{64})`).FindStringSubmatch(stdout)
	if len(digest) != 2 {
		t.Fatalf("repair inspection omitted exact findings digest:\n%s", stdout)
	}

	stdout, stderr, code = runStoreAuthorityCommand(t, configPath, "repair-authority", "--confirm", digest[1])
	if code != 0 || stderr != "" {
		t.Fatalf("confirmed repair = code:%d stdout:%q stderr:%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "✓ Project ownership repaired. Your flows, runs, and data were untouched.") {
		t.Fatalf("confirmed repair transcript:\n%s", stdout)
	}

	selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open repaired store: %v", err)
	}
	defer selected.Close()
	var repairs int
	if err := storetest.DatabaseForTest(selected).QueryRow(`SELECT COUNT(*) FROM runtime_startup_authority_repairs`).Scan(&repairs); err != nil {
		t.Fatalf("read authority repair journal: %v", err)
	}
	if repairs != 1 {
		t.Fatalf("authority repair journal rows = %d, want 1", repairs)
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

func bootstrapStoreAuthoritySQLite(t *testing.T, configPath, sqlitePath string) {
	t.Helper()
	paths, err := ResolveCLIContractPlatformSpecPaths(RepoRoot(), CLIContractPlatformSpecPathOptions{ConfigPath: configPath})
	if err != nil {
		t.Fatalf("resolve selected-store schema paths: %v", err)
	}
	_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), paths.ContractsPath, paths.PlatformSpecPath)
	if err != nil {
		t.Fatalf("load selected-store schema source: %v", err)
	}
	plans, err := StateStoreSchemaPlans(bundle)
	if err != nil {
		t.Fatalf("generate selected-store schema plans: %v", err)
	}
	selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
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
}

func writeStoreAuthorityConfig(t *testing.T, sqlitePath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, path, fmt.Sprintf("paths:\n  contracts_path: tests/tier8-boot-verification/test-boot-success\nstore:\n  backend: sqlite\n  sqlite:\n    path: %q\n", sqlitePath))
	return path
}

func seedCorruptAuthorityHead(t *testing.T, sqlitePath string) {
	t.Helper()
	selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open selected store: %v", err)
	}
	defer selected.Close()
	authorityID := uuid.NewString()
	_, err = storetest.DatabaseForTest(selected).Exec(`
		INSERT INTO runtime_startup_authority_facts (
			fact_id, authority_id, authority_generation, transition_ordinal,
			state_version, state, owner_id, boot_id, runtime_instance_id,
			backend, acquisition_id, acquisition_request_hash, acquisition_kind,
			predecessor_authority_id, successor_authority_id, snapshot
		) VALUES (?, ?, 1, 1, 1, 'active', 'broken-owner', ?, ?,
			'sqlite_retained_owner', ?, ?, 'cold', NULL, NULL, '{}')
	`, uuid.NewString(), authorityID, uuid.NewString(), uuid.NewString(), uuid.NewString(), strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("seed corrupt authority head: %v", err)
	}
}

func seedValidAuthorityHead(t *testing.T, sqlitePath, ownerID string) {
	t.Helper()
	selected, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open selected store: %v", err)
	}
	defer selected.Close()
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: ownerID, BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "sqlite_retained_owner")
	if err != nil {
		t.Fatalf("construct valid authority: %v", err)
	}
	snapshot, err := canonicaljson.Bytes(authority)
	if err != nil {
		t.Fatalf("encode valid authority: %v", err)
	}
	_, err = storetest.DatabaseForTest(selected).Exec(`
		INSERT INTO runtime_startup_authority_facts (
			fact_id, authority_id, authority_generation, transition_ordinal,
			state_version, state, owner_id, boot_id, runtime_instance_id,
			backend, acquisition_id, acquisition_request_hash, acquisition_kind,
			predecessor_authority_id, successor_authority_id, snapshot, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
	`, uuid.NewString(), authority.AuthorityID, authority.AuthorityGeneration, authority.TransitionOrdinal,
		authority.StateVersion, authority.State, authority.OwnerID, authority.BootID, authority.RuntimeInstanceID,
		authority.Backend, authority.AcquisitionID, authority.AcquisitionRequestHash, authority.AcquisitionKind,
		string(snapshot), authority.RecordedAt.UTC())
	if err != nil {
		t.Fatalf("seed valid authority head: %v", err)
	}
}
