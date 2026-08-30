package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestResolveLocalRuntimeStateUsesProjectLocalStoreWithoutAmbientData(t *testing.T) {
	projectRoot, sourceRoot := writeLocalRuntimeStateProject(t)
	swarmDir := CLISwarmDirResolution{Path: t.TempDir(), Source: "test"}

	state, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot:                projectRoot,
		ResolvedPaths:           CLISourcePlatformSpecPaths{SourceRoot: sourceRoot},
		SwarmDir:                swarmDir,
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err != nil {
		t.Fatalf("ResolveLocalRuntimeState: %v", err)
	}
	wantStore := filepath.Join(state.Project.CanonicalProjectRoot, ".swarm", "stores", "dev.db")
	if state.StoreSelection.SQLitePath != wantStore || state.StoreSelection.SQLitePathSource != storebackend.SourceProjectDefault {
		t.Fatalf("sqlite path = %q source %q, want %q from project_default", state.StoreSelection.SQLitePath, state.StoreSelection.SQLitePathSource, wantStore)
	}
	if state.MountSources.DataSource != "" || state.MountSources.DataSourceSource != "" {
		t.Fatalf("data source = %#v, want no ambient data source", state.MountSources)
	}
	if _, err := os.Stat(filepath.Join(state.Project.CanonicalProjectRoot, ".swarm", "data")); !os.IsNotExist(err) {
		t.Fatalf("retired project data default exists: %v", err)
	}
}

func TestResolveLocalRuntimeStateBorrowedContractsNeedsNoAmbientData(t *testing.T) {
	RepoRoot := t.TempDir()
	_, sourceRoot := writeLocalRuntimeStateProject(t)

	state, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot:                RepoRoot,
		ResolvedPaths:           CLISourcePlatformSpecPaths{SourceRoot: sourceRoot},
		SwarmDir:                CLISwarmDirResolution{Path: t.TempDir(), Source: "test"},
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
	})
	if err != nil {
		t.Fatalf("ResolveLocalRuntimeState: %v", err)
	}
	if state.MountSources != (WorkspaceMountSources{}) {
		t.Fatalf("mount sources = %#v, want no ambient data", state.MountSources)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(sourceRoot), ".swarm", "data")); !os.IsNotExist(err) {
		t.Fatalf("borrowed contracts data stat error = %v, want no .swarm/data created", err)
	}
}

func TestResolveLocalRuntimeStateRejectsLegacySQLiteOrphan(t *testing.T) {
	projectRoot, sourceRoot := writeLocalRuntimeStateProject(t)
	legacyPath := filepath.Join(projectRoot, storebackend.LegacySQLiteRelativePath)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o755); err != nil {
		t.Fatalf("mkdir legacy sqlite dir: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("legacy"), 0o644); err != nil {
		t.Fatalf("write legacy sqlite: %v", err)
	}

	_, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot:                projectRoot,
		ResolvedPaths:           CLISourcePlatformSpecPaths{SourceRoot: sourceRoot},
		SwarmDir:                CLISwarmDirResolution{Path: t.TempDir(), Source: "test"},
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "legacy project SQLite store exists") || !strings.Contains(err.Error(), ".swarm/stores/dev.db") {
		t.Fatalf("ResolveLocalRuntimeState error = %v, want legacy sqlite orphan rejection", err)
	}
}

func TestRunStartLocalLiveProjectRefusal(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	projectRoot, sourceRoot := writeLocalRuntimeStateProject(t)
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
		apiOptions: apiOptions,
		sourceRoot: sourceRoot,
	}, CLISourcePlatformSpecPaths{SourceRoot: sourceRoot})
	if err == nil || !strings.Contains(err.Error(), "local swarm run start requires exclusive project runtime") || !strings.Contains(err.Error(), "--connect") {
		t.Fatalf("prepareLocalRunProjectClaim error = %v, want live project runtime conflict", err)
	}
}

func writeLocalRuntimeStateProject(t *testing.T) (string, string) {
	t.Helper()
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "schema.yaml"), []byte("name: local-runtime-state-test\n"), 0o644); err != nil {
		t.Fatalf("write root schema: %v", err)
	}
	return projectRoot, projectRoot
}
