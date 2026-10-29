package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestChannelAndServeUseSameDeploymentProjectStore(t *testing.T) {
	for _, tc := range []struct {
		name               string
		absolute, explicit bool
	}{
		{name: "unset"}, {name: "absolute", absolute: true},
		{name: "unset_explicit_store", explicit: true}, {name: "absolute_explicit_store", absolute: true, explicit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root, source := writeLocalRuntimeStateProject(t)
			t.Setenv("HOME", t.TempDir())
			path := filepath.Join(root, ".swarm", "swarm.yaml")
			if err := os.Mkdir(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			body := "store:\n  backend: sqlite\n"
			explicitStore := filepath.Join(t.TempDir(), "authored.sqlite")
			if tc.explicit {
				body += fmt.Sprintf("  sqlite:\n    path: %q\n", explicitStore)
			}
			if tc.absolute {
				body += fmt.Sprintf("paths:\n  swarm_dir: %q\n", t.TempDir())
			}
			writeRuntimeConfigText(t, path, body)
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
			if err != nil {
				t.Fatal(err)
			}
			swarmDir, err := resolveCLISwarmDirFromConfig(mustInvocationRootForTest(root), cliSwarmDirOptions{}, loaded.cli)
			if err != nil {
				t.Fatal(err)
			}
			channel, err := ResolveRuntimeStoreSelection(root, "", false, loaded)
			if err != nil {
				t.Fatal(err)
			}
			serve, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
				RepoRoot: root, ResolvedPaths: CLISourcePaths{SourceRoot: source}, SwarmDir: swarmDir, Config: loaded.Config,
			})
			if err != nil {
				t.Fatal(err)
			}
			want := filepath.Join(serve.Project.CanonicalProjectRoot, projectSQLiteStoreRelativePath)
			wantSource := storebackend.SourceProjectDefault
			if tc.explicit {
				want, wantSource = explicitStore, storebackend.SourceRuntimeConfig
			}
			if channel != serve.StoreSelection || channel.SQLitePath != want || channel.SQLitePathSource != wantSource {
				t.Fatalf("channel and serve selected different defaults: channel=%#v serve=%#v", channel, serve.StoreSelection)
			}
			if _, err := os.Stat(want); !os.IsNotExist(err) {
				t.Fatal("store selection must remain read-only", err)
			}
		})
	}
}

func TestResolveLocalRuntimeStateUsesProjectLocalStoreWithoutAmbientData(t *testing.T) {
	projectRoot, sourceRoot := writeLocalRuntimeStateProject(t)
	swarmDir := CLISwarmDirResolution{Path: t.TempDir(), Source: "test"}

	state, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot:                projectRoot,
		ResolvedPaths:           CLISourcePaths{SourceRoot: sourceRoot},
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
		ResolvedPaths:           CLISourcePaths{SourceRoot: sourceRoot},
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
		ResolvedPaths:           CLISourcePaths{SourceRoot: sourceRoot},
		SwarmDir:                CLISwarmDirResolution{Path: t.TempDir(), Source: "test"},
		Config:                  &config.Config{},
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported project SQLite store exists") || !strings.Contains(err.Error(), ".swarm/stores/dev.db") {
		t.Fatalf("ResolveLocalRuntimeState error = %v, want legacy sqlite orphan rejection", err)
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
