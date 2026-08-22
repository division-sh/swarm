package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestStoreStatusReadsSelectedStoreOwnershipWithoutContextInference(t *testing.T) {
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "selected.db")
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
}

func TestDoctorSeparatesContextDiscoveryFromProjectOwnership(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), "selected.db")
	configPath := writeStoreAuthorityConfig(t, sqlitePath)
	if _, _, code := runStoreAuthorityCommand(t, configPath, "status"); code != 0 {
		t.Fatalf("bootstrap selected store status code = %d", code)
	}
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
	_, _, code := runStoreAuthorityCommand(t, configPath, "status")
	if code != 0 {
		t.Fatalf("bootstrap selected store status code = %d", code)
	}
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
	commandArgs := append([]string{"store"}, args...)
	commandArgs = append(commandArgs, "--config", configPath, "--store", "sqlite")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), commandArgs, &stdout, &stderr, defaultRootCommandOptions())
	return stdout.String(), stderr.String(), code
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
