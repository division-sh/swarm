package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestResolveLocalRuntimeStateUsesProjectLocalStoreAndData(t *testing.T) {
	projectRoot, contractsPath := writeLocalRuntimeStateProject(t)
	swarmDir := cliSwarmDirResolution{Path: t.TempDir(), Source: "test"}

	state, err := resolveLocalRuntimeState(localRuntimeStateOptions{
		RepoRoot:                projectRoot,
		ResolvedPaths:           cliContractPlatformSpecPaths{ContractsPath: contractsPath},
		SwarmDir:                swarmDir,
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err != nil {
		t.Fatalf("resolveLocalRuntimeState: %v", err)
	}
	wantStore := filepath.Join(state.Project.CanonicalProjectRoot, ".swarm", "stores", "dev.db")
	if state.StoreSelection.SQLitePath != wantStore || state.StoreSelection.SQLitePathSource != storebackend.SourceProjectDefault {
		t.Fatalf("sqlite path = %q source %q, want %q from project_default", state.StoreSelection.SQLitePath, state.StoreSelection.SQLitePathSource, wantStore)
	}
	wantData := filepath.Join(state.Project.CanonicalProjectRoot, ".swarm", "data")
	if state.MountSources.DataSource != wantData || state.MountSources.DataSourceSource != defaultWorkspaceDataSourceSource {
		t.Fatalf("data source = %#v, want %q from %s", state.MountSources, wantData, defaultWorkspaceDataSourceSource)
	}
	if info, err := os.Stat(wantData); err != nil || !info.IsDir() {
		t.Fatalf("project data default stat = (%v, %v), want created directory", info, err)
	}
}

func TestResolveLocalRuntimeStateBorrowedContractsRequireExplicitData(t *testing.T) {
	repoRoot := t.TempDir()
	_, contractsPath := writeLocalRuntimeStateProject(t)

	_, err := resolveLocalRuntimeState(localRuntimeStateOptions{
		RepoRoot:                repoRoot,
		ResolvedPaths:           cliContractPlatformSpecPaths{ContractsPath: contractsPath},
		SwarmDir:                cliSwarmDirResolution{Path: t.TempDir(), Source: "test"},
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
	})
	if err == nil || !strings.Contains(err.Error(), "workspace data source is required") {
		t.Fatalf("resolveLocalRuntimeState error = %v, want explicit data requirement", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(contractsPath), ".swarm", "data")); !os.IsNotExist(err) {
		t.Fatalf("borrowed contracts data stat error = %v, want no .swarm/data created", err)
	}
}

func TestResolveLocalRuntimeStateRejectsLegacySQLiteOrphan(t *testing.T) {
	projectRoot, contractsPath := writeLocalRuntimeStateProject(t)
	legacyPath := filepath.Join(projectRoot, storebackend.LegacySQLiteRelativePath)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o755); err != nil {
		t.Fatalf("mkdir legacy sqlite dir: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("legacy"), 0o644); err != nil {
		t.Fatalf("write legacy sqlite: %v", err)
	}

	_, err := resolveLocalRuntimeState(localRuntimeStateOptions{
		RepoRoot:                projectRoot,
		ResolvedPaths:           cliContractPlatformSpecPaths{ContractsPath: contractsPath},
		SwarmDir:                cliSwarmDirResolution{Path: t.TempDir(), Source: "test"},
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "legacy project SQLite store exists") || !strings.Contains(err.Error(), ".swarm/stores/dev.db") {
		t.Fatalf("resolveLocalRuntimeState error = %v, want legacy sqlite orphan rejection", err)
	}
}

func TestPrepareLocalRunProjectClaimRejectsLiveProjectContext(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	projectRoot, contractsPath := writeLocalRuntimeStateProject(t)
	canonicalProjectRoot, status := canonicalizeDoctorTargetPath(projectRoot)
	if status != "resolved" {
		t.Fatalf("canonicalize project root status = %s", status)
	}
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
	writeCLIAPITestContext(t, registry, localProjectContextName(canonicalProjectRoot), "runtime-live", server.URL, canonicalProjectRoot)

	apiOptions := defaultRootCommandOptions()
	apiOptions.rootFlags = &rootCommandFlagState{swarmDir: swarmDir, swarmDirSet: true}
	_, err := prepareLocalRunProjectClaim(context.Background(), projectRoot, runCommandOptions{
		apiOptions:    apiOptions,
		contractsPath: contractsPath,
	}, cliContractPlatformSpecPaths{ContractsPath: contractsPath})
	if err == nil || !strings.Contains(err.Error(), "local swarm run requires exclusive project runtime") || !strings.Contains(err.Error(), "--connect") {
		t.Fatalf("prepareLocalRunProjectClaim error = %v, want live project runtime conflict", err)
	}
}

func writeLocalRuntimeStateProject(t *testing.T) (string, string) {
	t.Helper()
	projectRoot := t.TempDir()
	contractsPath := filepath.Join(projectRoot, "contracts")
	if err := os.MkdirAll(contractsPath, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsPath, "package.yaml"), []byte("name: local-runtime-state-test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	return projectRoot, contractsPath
}
