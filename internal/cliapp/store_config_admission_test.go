package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/spf13/cobra"
)

func TestRuntimeConfigStoreSelectionRetainsSelectedFile(t *testing.T) {
	for _, cut := range []string{"explicit selector", "replaced file", "removed file"} {
		t.Run(cut, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			path := filepath.Join(t.TempDir(), "operator.yaml")
			writeRuntimeConfigText(t, path, "paths:\n  swarm_dir: admitted-state\nstore:\n  backend: sqlite\n")
			if cut == "explicit selector" {
				if err := os.MkdirAll(filepath.Join(repo, ".swarm"), 0o700); err != nil {
					t.Fatal(err)
				}
				writeRuntimeConfigText(t, filepath.Join(repo, ".swarm", "swarm.yaml"), "paths:\n  swarm_dir: fallback-state\n")
			} else {
				t.Setenv("SWARM_CONFIG", path)
			}
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			if cut == "replaced file" {
				writeRuntimeConfigText(t, path, "paths:\n  swarm_dir: redirected-state\n")
			} else if cut == "removed file" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			selection, err := ResolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, loaded)
			if err != nil {
				t.Fatalf("captured store selection re-read configuration: %v", err)
			}
			want := filepath.Join(repo, "admitted-state", "stores", "default", "dev.db")
			if selection.Backend != storebackend.BackendSQLite || selection.BackendSource != storebackend.SourceRuntimeConfig || selection.SQLitePath != want || selection.SQLitePathSource != storebackend.SourceSwarmDirDefault || loaded.Path != path {
				t.Fatalf("selection = %#v, selected file=%q, want %s from %s", selection, loaded.Path, want, path)
			}
			if _, err := os.Stat(filepath.Dir(want)); !os.IsNotExist(err) {
				t.Fatalf("pure selection created store state: %v", err)
			}
		})
	}
}

func TestRuntimeConfigAuthorityStoreRetainsAdmissionAndFlags(t *testing.T) {
	for _, mutation := range []string{"redirect", "invalid"} {
		for _, flagSet := range []bool{false, true} {
			name := mutation + "/config-dir"
			if flagSet {
				name = mutation + "/flag-dir"
			}
			t.Run(name, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				repo := t.TempDir()
				path := filepath.Join(t.TempDir(), "operator.yaml")
				writeRuntimeConfigText(t, path, "paths:\n  swarm_dir: admitted-state\nstore:\n  backend: postgres\n")
				loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
				if err != nil {
					t.Fatal(err)
				}
				cmd := &cobra.Command{}
				cmd.PersistentFlags().String("swarm-dir", "unselected-flag-default", "")
				wantDir := "admitted-state"
				if flagSet {
					if err := cmd.PersistentFlags().Set("swarm-dir", "flag-state"); err != nil {
						t.Fatal(err)
					}
					wantDir = "flag-state"
				}
				if mutation == "redirect" {
					writeRuntimeConfigText(t, path, "paths:\n  swarm_dir: redirected-state\n")
				} else {
					writeRuntimeConfigText(t, path, "unknown_after_admission: true\n")
				}
				selection, err := resolveAuthorityStoreSelection(mustInvocationRootForTest(repo), cmd, loaded, CLISourcePlatformSpecPaths{}, "sqlite", true)
				if err != nil {
					t.Fatalf("authority store selection re-read admitted file: %v", err)
				}
				want := filepath.Join(repo, wantDir, "stores", "default", "dev.db")
				if selection.Backend != storebackend.BackendSQLite || selection.BackendSource != storebackend.SourceFlag || selection.SQLitePath != want || selection.SQLitePathSource != storebackend.SourceSwarmDirDefault {
					t.Fatalf("flag/config store selection = %#v, want %s", selection, want)
				}
				if _, err := os.Stat(filepath.Dir(want)); !os.IsNotExist(err) {
					t.Fatalf("pure inspection selection created store state: %v", err)
				}
			})
		}
	}
}

func TestRuntimeConfigStoreSelectionRefusesUnadmittedResults(t *testing.T) {
	for _, cut := range []string{"zero result", "projected only", "missing projection", "source refused", "ambient blocker"} {
		t.Run(cut, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			path := filepath.Join(repo, "operator.yaml")
			writeRuntimeConfigText(t, path, "paths:\n  swarm_dir: uninspected-state\n")
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			wantCause := "admitted runtime config is required"
			switch cut {
			case "zero result":
				loaded = RuntimeConfigLoadResult{}
			case "projected only":
				loaded = RuntimeConfigLoadResult{Config: loaded.Config}
			case "missing projection":
				loaded.Config = nil
			case "source refused":
				writeRuntimeConfigText(t, path, "unknown_before_admission: true\n")
				loaded, err = LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
				wantCause = "unknown_before_admission"
				if err == nil || loaded.validated {
					t.Fatal("source-refusal control did not reach refusal")
				}
			case "ambient blocker":
				t.Setenv("SWARM_UNADMITTED_STORE_SELECTION", "not-a-delegation")
				loaded, err = LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
				wantCause = "env/unknown_stale @ SWARM_UNADMITTED_STORE_SELECTION"
				if err == nil || !loaded.validated {
					t.Fatal("ambient control did not retain valid source with an env blocker")
				}
			}
			for _, flagSet := range []bool{false, true} {
				selection, err := ResolveRuntimeStoreSelection(repo, "sqlite", flagSet, loaded)
				if err == nil || !strings.Contains(err.Error(), wantCause) || selection != (storebackend.Selection{}) {
					t.Fatalf("unadmitted runtime selection: result=%#v error=%v", selection, err)
				}
				selection, err = resolveAuthorityStoreSelection(mustInvocationRootForTest(repo), nil, loaded, CLISourcePlatformSpecPaths{}, "sqlite", flagSet)
				if err == nil || !strings.Contains(err.Error(), wantCause) || selection != (storebackend.Selection{}) {
					t.Fatalf("unadmitted authority selection: result=%#v error=%v", selection, err)
				}
			}
			if _, err := os.Stat(filepath.Join(repo, "uninspected-state")); !os.IsNotExist(err) {
				t.Fatalf("refused admission created state: %v", err)
			}
		})
	}
}

func TestRuntimeConfigDoctorOwnerInspectsTheAdmittedStore(t *testing.T) {
	for _, cut := range []string{"replaced source", "removed source", "swarm-dir default"} {
		t.Run(cut, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			root := mustInvocationRootForTest(repo)
			sqlitePath := filepath.Join(t.TempDir(), "selected.db")
			stateDir := t.TempDir()
			if cut == "swarm-dir default" {
				sqlitePath = filepath.Join(stateDir, "stores", "default", "dev.db")
				if err := os.MkdirAll(filepath.Dir(sqlitePath), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			path := writeStoreAuthorityConfig(t, sqlitePath)
			bootstrapStoreAuthoritySQLite(t, path, sqlitePath, "captured-doctor-owner")
			if cut == "swarm-dir default" {
				writeRuntimeConfigText(t, path, fmt.Sprintf("paths:\n  swarm_dir: %q\nstore:\n  backend: sqlite\n", stateDir))
			}
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			swarmDir, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{}, loaded.cli)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(sqlitePath)
			if err != nil {
				t.Fatal(err)
			}
			if cut == "replaced source" {
				writeRuntimeConfigText(t, path, "unknown_after_admission: true\n")
			} else if cut == "removed source" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			opts := doctorOptions{configPath: path}
			opts.apiOptions.invocationRoot = root
			report, err := buildDoctorTargetReport(context.Background(), repo, opts, loaded.cli, swarmDir, loaded.Config)
			if err != nil || report.Store == nil || report.Store.Path != sqlitePath || report.ProjectOwner == nil || report.ProjectOwner.Status != "available" || report.ProjectOwner.Detail != "Project owner: selected-store lease is available (last admitted generation 1, owner captured-doctor-owner)." {
				t.Fatalf("doctor inspected a different or reloaded store: store=%#v owner=%#v error=%v", report.Store, report.ProjectOwner, err)
			}
			after, err := os.ReadFile(sqlitePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("doctor owner inspection changed the selected database: %v", err)
			}
		})
	}
}
