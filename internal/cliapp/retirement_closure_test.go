package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestRetirementClosureDetachIsUnknownBeforeEffects(t *testing.T) {
	for _, flag := range []string{"--detach", "--detach=true", "--detach=false"} {
		t.Run(flag, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			var out, errOut bytes.Buffer
			rootPath := t.TempDir()
			root := newRootCommand(context.Background(), rootPath, &out, &errOut)
			cmd, _, err := root.Find([]string{"run", "start"})
			if err != nil {
				t.Fatal(err)
			}
			if cmd.Flags().Lookup("detach") != nil {
				t.Fatal("self-rejecting flag is still registered")
			}
			code := executeRootCommand(context.Background(), rootPath, []string{"run", "start", flag}, &out, &errOut)
			if code != 2 || !strings.Contains(errOut.String(), "unknown flag: --detach") {
				t.Fatalf("code=%d stderr=%s", code, &errOut)
			}
			entries, err := os.ReadDir(rootPath)
			if err != nil || len(entries) != 0 {
				t.Fatalf("unknown flag created state: %v, %v", entries, err)
			}
		})
	}
}

func TestRetirementClosureSuggestionsExcludeUnsupportedSettings(t *testing.T) {
	for _, tc := range []struct {
		body, key           string
		excluded, supported []string
	}{
		{"runtime: {max_concurrent_agent: 2}\n", "runtime.max_concurrent_agent", []string{"max_concurrent_agents", "event_poll_interval"}, []string{"fan_out_workers", "recovery_on_startup"}},
		{"llm: {claude_cli: {retrie: 2}}\n", "llm.claude_cli.retrie", []string{"retries", "use_tmux", "no_session_persistence"}, []string{"command", "timeout"}},
		{"database: {passwor: secret}\n", "database.passwor", []string{"password"}, []string{"password_env", "password_file"}},
	} {
		t.Run(tc.key, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			path := filepath.Join(t.TempDir(), "swarm.yaml")
			writeRuntimeConfigText(t, path, tc.body)
			_, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{ExplicitPath: path})
			if err == nil {
				t.Fatal("typo accepted")
			}
			found := false
			for _, d := range unifiedConfigDiagnosticsFromError(err) {
				if d.Kind != unifiedConfigDiagnosticUnknownKey || d.Key != tc.key {
					continue
				}
				found = true
				for _, key := range tc.excluded {
					if slices.Contains(d.ValidOptions, key) {
						t.Fatalf("unsupported setting advertised: %s", d.Message)
					}
				}
				for _, key := range tc.supported {
					if !slices.Contains(d.ValidOptions, key) {
						t.Fatalf("supported setting missing: %s", d.Message)
					}
				}
				if d.Line == 0 || d.Column == 0 {
					t.Fatalf("source coordinates missing: %#v", d)
				}
			}
			if !found {
				t.Fatalf("missing diagnostic for %s: %v", tc.key, err)
			}
		})
	}
}

func TestRetirementClosureMergedEnvDelegation(t *testing.T) {
	for _, body := range []string{
		"database: {password_env: SWARM_REVIEW_DB_SECRET}\n",
		"database: {<<: {password_env: SWARM_REVIEW_DB_SECRET}}\n",
		"database: {user: &name SWARM_REVIEW_DB_SECRET, <<: {password_env: *name}}\n",
	} {
		t.Run(body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo, path := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
			writeRuntimeConfigText(t, path, body)
			t.Setenv("SWARM_REVIEW_DB_SECRET", "dummy-not-a-credential")
			got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			if got.Config.Database.PasswordEnv != "SWARM_REVIEW_DB_SECRET" {
				t.Fatalf("delegation lost: %#v", got.Config.Database)
			}
		})
	}
}

func TestRetirementClosureSelectedDocumentMergeIsolation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo, global := t.TempDir(), ignoredUserGlobalConfigPath(t)
	writeSingleDeploymentConfig(t, global, "runtime: {<<: {recovery_on_startup: true}}\nllm: {session: {rotate_after_turns: 7}}\n")
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), "runtime: {recovery_on_startup: false}\n")
	selected := filepath.Join(repo, ".swarm", "swarm.yaml")
	writeSingleDeploymentConfig(t, selected, "runtime: {<<: {recovery_on_startup: false}}\n")
	got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatal(err)
	}
	if got.Config.Runtime.RecoveryOnStartup || got.Config.LLM.Session.RotateAfterTurns != 40 || got.Path != selected {
		t.Fatalf("single-document merge/default isolation lost: path=%q recovery=%t rotation=%d", got.Path, got.Config.Runtime.RecoveryOnStartup, got.Config.LLM.Session.RotateAfterTurns)
	}
}

// USER #1801/6170905244 removes source trust tiers, not typed/secret refusals.
func TestRetirementClosureSelectedTypedValues(t *testing.T) {
	for _, row := range []struct {
		body, want string
	}{
		{"serve: {<<: {api_listen_addr: 0.0.0.0:8090}}\n", ""},
		{"store: {sqlite: {path: ../operator-state.db}}\n", ""},
		{"serve: {api_listen_addr: 127.0.0.1:8090}\n", ""},
		{"serve: {api_listen_addr: ''}\n", ""},
		{"database: {<<: {password: synthetic-inline-secret}}\n", "plaintext secret"},
		{"runtime: {<<: {max_concurrent_agents: 2}}\n", "recognized but not yet supported"},
		{"serve: null\n", "must be a YAML mapping"},
		{"serve: {<<: {unknown: true}}\n", "unknown config key"},
	} {
		t.Run(row.body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			path := filepath.Join(repo, ".swarm", "swarm.yaml")
			writeSingleDeploymentConfig(t, path, row.body)
			loaded, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo})
			if row.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if loaded.Path != path {
					t.Fatalf("selected source lost: %q", loaded.Path)
				}
			} else if err == nil || !strings.Contains(err.Error(), row.want) {
				t.Fatalf("error = %v, want %q", err, row.want)
			}
		})
	}
}

func TestRetirementClosureSingleDocumentExpansionAndIsolation(t *testing.T) {
	forms := []string{
		"runtime: {recovery_on_startup: %s}\n",
		"runtime: {<<: {recovery_on_startup: %s}}\n",
		"llm: {models: {default: {anthropic: &value %s}}}\nruntime: {recovery_on_startup: *value}\n",
	}
	for _, selection := range []string{"deployment", "locator", "flag"} {
		for from, ignored := range forms {
			for to, selected := range forms {
				t.Run(fmt.Sprintf("%s/forms=%d-%d", selection, from, to), func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					repo := t.TempDir()
					for _, path := range []string{ignoredUserGlobalConfigPath(t), filepath.Join(repo, "swarm.yaml"), filepath.Join(repo, ".swarm", "swarm.yaml")} {
						writeSingleDeploymentConfig(t, path, fmt.Sprintf(ignored, "true")+"database: {pool_size: 17}\n")
					}
					path := filepath.Join(repo, ".swarm", "swarm.yaml")
					opts := unifiedConfigLoadOptions{RepoRoot: repo}
					if selection != "deployment" {
						path = filepath.Join(t.TempDir(), "operator.yaml")
						if selection == "locator" {
							t.Setenv("SWARM_CONFIG", path)
						} else {
							opts.ExplicitPath = path
						}
					}
					writeSingleDeploymentConfig(t, path, fmt.Sprintf(selected, "false"))
					got, err := loadUnifiedConfigForTest(t, opts)
					if err != nil {
						t.Fatal(err)
					}
					if got.Config.Runtime.RecoveryOnStartup || got.Config.Database.PoolSize != 5 || got.Path != path {
						t.Fatalf("selected expansion/isolation lost: path=%q recovery=%t pool=%d", got.Path, got.Config.Runtime.RecoveryOnStartup, got.Config.Database.PoolSize)
					}
				})
			}
		}
	}
}

func TestRetirementClosureSelectedEnvDelegationAndReplacement(t *testing.T) {
	for _, selection := range []string{"deployment", "locator", "flag"} {
		t.Run(selection, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo := t.TempDir()
			path := filepath.Join(repo, ".swarm", "swarm.yaml")
			t.Setenv("SWARM_REVIEW_DB_SECRET", "dummy-not-a-credential")
			opts := unifiedConfigLoadOptions{RepoRoot: repo}
			if selection != "deployment" {
				path = filepath.Join(t.TempDir(), "operator.yaml")
				if selection == "locator" {
					t.Setenv("SWARM_CONFIG", path)
				} else {
					opts.ExplicitPath = path
				}
			}
			writeSingleDeploymentConfig(t, path, "database: {<<: {password_env: SWARM_REVIEW_DB_SECRET}}\n")
			got, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.ExplicitPath})
			if err != nil {
				t.Fatal(err)
			}
			if got.Path != path || got.Config.Database.PasswordEnv != "SWARM_REVIEW_DB_SECRET" {
				t.Fatalf("wrong selected delegation: path=%q env=%q", got.Path, got.Config.Database.PasswordEnv)
			}
			findings := doctorSwarmEnvFindings(got)
			found := false
			for _, finding := range findings {
				if finding.Name == "SWARM_REVIEW_DB_SECRET" {
					found = finding.AcceptedBy == "database.password_env"
				}
			}
			if !found {
				t.Fatalf("doctor disagrees: %#v", findings)
			}
		})
	}
	for _, clearing := range []string{"database: {password_env: ''}\n", "database: {password_env: null}\n", "database: null\n", "database: {<<: {password_env: OTHER_DB_SECRET}}\n"} {
		t.Run(clearing, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
			writeSingleDeploymentConfig(t, ignoredUserGlobalConfigPath(t), "database: {<<: {password_env: SWARM_REVIEW_DB_SECRET}}\n")
			writeRuntimeConfigText(t, explicit, clearing)
			t.Setenv("SWARM_REVIEW_DB_SECRET", "dummy-not-a-credential")
			got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
			if err == nil || !strings.Contains(err.Error(), "unknown SWARM_* env") {
				t.Fatalf("cleared delegation resurrected: %v", err)
			}
			if got.Config.Database.PasswordEnv == "SWARM_REVIEW_DB_SECRET" {
				t.Fatal("old secret reference survived")
			}
			if got.Path != explicit {
				t.Fatal("clear/replacement lost selected source")
			}
		})
	}
}

func TestRetirementClosureDuplicatesRemainDocumentLocal(t *testing.T) {
	for _, body := range []string{
		"runtime: {recovery_on_startup: true, recovery_on_startup: false}\n",
		"runtime: {<<: {recovery_on_startup: true}, recovery_on_startup: false}\n",
		"runtime: {<<: [&r {recovery_on_startup: true}, *r]}\n",
	} {
		t.Run(body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
			writeSingleDeploymentConfig(t, ignoredUserGlobalConfigPath(t), body)
			writeRuntimeConfigText(t, explicit, body)
			_, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
			if err == nil || !strings.Contains(err.Error(), "duplicate") {
				t.Fatalf("selected duplicate was admitted: %v", err)
			}
			writeRuntimeConfigText(t, explicit, "runtime: {recovery_on_startup: false}\n")
			loaded, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
			if err != nil || loaded.Config.Runtime.RecoveryOnStartup {
				t.Fatalf("ignored duplicate affected valid selected document: %v", err)
			}
		})
	}
}

func TestRetirementClosureExpandedPresenceAndEmptyMap(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
	writeSingleDeploymentConfig(t, ignoredUserGlobalConfigPath(t), "paths: {<<: {swarm_dir: /tmp/operator-state}}\nruntime: {<<: {recovery_on_startup: false}}\nllm: {session: {rotate_after_turns: 7}}\n")
	writeRuntimeConfigText(t, explicit, "paths: {<<: {swarm_dir: ''}}\nruntime: {}\n")
	got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
	if err != nil {
		t.Fatal(err)
	}
	if !got.CLI.Paths.SwarmDirSet || got.CLI.Paths.SwarmDir != "" {
		t.Fatalf("explicit empty presence lost: %#v", got.CLI.Paths)
	}
	if !got.Config.Runtime.RecoveryOnStartup || got.Config.LLM.Session.RotateAfterTurns != 40 || got.Path != explicit {
		t.Fatal("empty mapping did not preserve built-in defaults or selected source")
	}
}

func TestRetirementClosureStoreRefusalHasNoMigrationAdvice(t *testing.T) {
	projectRoot, sourceRoot := writeLocalRuntimeStateProject(t)
	legacyPath := filepath.Join(projectRoot, storebackend.LegacySQLiteRelativePath)
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyPath, []byte("unsupported-store-placeholder"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot: projectRoot, ResolvedPaths: CLISourcePaths{SourceRoot: sourceRoot},
		SwarmDir: CLISwarmDirResolution{Path: t.TempDir(), Source: "test"}, Config: &config.Config{}, EnforceLegacySQLite: true,
	})
	if err == nil {
		t.Fatal("unsupported store must still be rejected")
	}
	for _, instruction := range []string{"move the file", "remove the legacy file", "confirming the old data"} {
		if strings.Contains(err.Error(), instruction) {
			t.Fatalf("migration advice remains: %v", err)
		}
	}
	contents, readErr := os.ReadFile(legacyPath)
	if readErr != nil || string(contents) != "unsupported-store-placeholder" {
		t.Fatalf("refusal changed old store: %q %v", contents, readErr)
	}
}

func TestRetirementClosureStoreConflictPredicateIsUnchanged(t *testing.T) {
	for _, row := range []struct {
		name                                                                   string
		local, oldExists, selectedExists, samePath, explicit, postgres, reject bool
	}{
		{name: "orphan", local: true, oldExists: true, reject: true},
		{name: "canonical_exists", local: true, oldExists: true, selectedExists: true},
		{name: "no_old_store", local: true},
		{name: "nonproject", oldExists: true},
		{name: "explicit_source", local: true, oldExists: true, explicit: true},
		{name: "postgres", local: true, oldExists: true, postgres: true},
		{name: "same_path", local: true, oldExists: true, samePath: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			root := t.TempDir()
			oldPath, selectedPath := filepath.Join(root, storebackend.LegacySQLiteRelativePath), filepath.Join(root, projectSQLiteStoreRelativePath)
			for _, path := range []string{oldPath, selectedPath} {
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if row.oldExists {
				if err := os.WriteFile(oldPath, []byte("old"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if row.selectedExists {
				if err := os.WriteFile(selectedPath, []byte("selected"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			selection := storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: selectedPath, SQLitePathSource: storebackend.SourceProjectDefault}
			if row.samePath {
				selection.SQLitePath = oldPath
			}
			if row.explicit {
				selection.SQLitePathSource = storebackend.SourceFlag
			}
			if row.postgres {
				selection.Backend = storebackend.BackendPostgres
			}
			err := legacyProjectSQLiteStoreError(localRuntimeStateProject{ProjectLocal: row.local, CanonicalProjectRoot: root}, selection)
			if (err != nil) != row.reject {
				t.Fatalf("reject=%v error=%v", row.reject, err)
			}
			if err != nil && (!strings.Contains(err.Error(), oldPath) || !strings.Contains(err.Error(), selectedPath) || !strings.Contains(err.Error(), "source authority conflict")) {
				t.Fatalf("incomplete authority diagnostic: %v", err)
			}
			if !row.selectedExists {
				if _, err := os.Stat(selectedPath); !os.IsNotExist(err) {
					t.Fatalf("refusal created selected store: %v", err)
				}
			}
		})
	}
}
