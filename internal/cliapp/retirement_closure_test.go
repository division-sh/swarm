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

func TestRetirementClosureMergedLayerOverride(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo, global := t.TempDir(), userGlobalUnifiedConfigPath()
	writeRuntimeConfigText(t, global, "runtime: {<<: {recovery_on_startup: true}}\n")
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), "runtime: {recovery_on_startup: false}\n")
	got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatal(err)
	}
	if got.Config.Runtime.RecoveryOnStartup {
		t.Fatal("later override lost")
	}
	if origin := got.KeyOrigins["runtime.recovery_on_startup"]; origin.Layer != unifiedLayerProject {
		t.Fatalf("wrong origin: %#v", origin)
	}
}

func TestRetirementClosureLayerExpansionPrecedenceAndOrigins(t *testing.T) {
	forms := []string{
		"runtime: {recovery_on_startup: %s}\n",
		"runtime: {<<: {recovery_on_startup: %s}}\n",
		"llm: {models: {default: {anthropic: &value %s}}}\nruntime: {recovery_on_startup: *value}\n",
	}
	for first := 0; first < 3; first++ {
		for last := first + 1; last < 4; last++ {
			for from, lower := range forms {
				for to, upper := range forms {
					t.Run(fmt.Sprintf("layers=%d-%d/forms=%d-%d", first, last, from, to), func(t *testing.T) {
						isolateCLIAPIConfigEnv(t)
						repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
						paths := []string{userGlobalUnifiedConfigPath(), filepath.Join(repo, "swarm.yaml"), filepath.Join(repo, ".swarm", "swarm.yaml"), explicit}
						for _, path := range paths {
							if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
								t.Fatal(err)
							}
						}
						names := []unifiedConfigLayerName{unifiedLayerUserGlobal, unifiedLayerProject, unifiedLayerLocalOperator, unifiedLayerExplicit}
						writeRuntimeConfigText(t, paths[first], fmt.Sprintf(lower, "true"))
						writeRuntimeConfigText(t, paths[last], fmt.Sprintf(upper, "false"))
						opts := unifiedConfigLoadOptions{RepoRoot: repo}
						if last == 3 {
							opts.ExplicitPath = explicit
						}
						got, err := loadUnifiedConfigForTest(t, opts)
						if err != nil {
							t.Fatal(err)
						}
						if got.Config.Runtime.RecoveryOnStartup {
							t.Fatal("precedence lost")
						}
						origin := got.KeyOrigins["runtime.recovery_on_startup"]
						if origin.Layer != names[last] || origin.Path != paths[last] {
							t.Fatalf("wrong origin: %#v", origin)
						}
						for key := range got.KeyOrigins {
							if strings.Contains(key, "<<") {
								t.Fatalf("raw merge origin survived: %s", key)
							}
						}
					})
				}
			}
		}
	}
}

func TestRetirementClosureEnvDelegationTrustClearsAndReplacement(t *testing.T) {
	for _, layer := range []unifiedConfigLayerName{unifiedLayerUserGlobal, unifiedLayerProject, unifiedLayerLocalOperator, unifiedLayerExplicit} {
		t.Run(string(layer), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
			paths := map[unifiedConfigLayerName]string{unifiedLayerUserGlobal: userGlobalUnifiedConfigPath(), unifiedLayerProject: filepath.Join(repo, "swarm.yaml"), unifiedLayerLocalOperator: filepath.Join(repo, ".swarm", "swarm.yaml"), unifiedLayerExplicit: explicit}
			if err := os.MkdirAll(filepath.Dir(paths[layer]), 0700); err != nil {
				t.Fatal(err)
			}
			writeRuntimeConfigText(t, paths[layer], "database: {<<: {password_env: SWARM_REVIEW_DB_SECRET}}\n")
			t.Setenv("SWARM_REVIEW_DB_SECRET", "dummy-not-a-credential")
			opts := unifiedConfigLoadOptions{RepoRoot: repo}
			if layer == unifiedLayerExplicit {
				opts.ExplicitPath = explicit
			}
			got, err := loadUnifiedConfigForTest(t, opts)
			if layer == unifiedLayerProject {
				if err == nil || !strings.Contains(err.Error(), "not allowed in project_config") || !strings.Contains(err.Error(), "SWARM_REVIEW_DB_SECRET") {
					t.Fatalf("project delegation not blocked: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.KeyOrigins["database.password_env"].Layer != layer {
				t.Fatalf("wrong delegation origin: %#v", got.KeyOrigins)
			}
			findings := doctorSwarmEnvFindings(repo, opts.ExplicitPath)
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
			writeRuntimeConfigText(t, userGlobalUnifiedConfigPath(), "database: {<<: {password_env: SWARM_REVIEW_DB_SECRET}}\n")
			writeRuntimeConfigText(t, explicit, clearing)
			t.Setenv("SWARM_REVIEW_DB_SECRET", "dummy-not-a-credential")
			got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
			if err == nil || !strings.Contains(err.Error(), "unknown SWARM_* env") {
				t.Fatalf("cleared delegation resurrected: %v", err)
			}
			if got.Config.Database.PasswordEnv == "SWARM_REVIEW_DB_SECRET" {
				t.Fatal("old secret reference survived")
			}
			if clearing == "database: null\n" {
				if _, ok := got.KeyOrigins["database.password_env"]; ok {
					t.Fatalf("stale descendant origin: %#v", got.KeyOrigins)
				}
			} else if got.KeyOrigins["database.password_env"].Layer != unifiedLayerExplicit {
				t.Fatal("clear/replacement lost origin")
			}
		})
	}
}

func TestRetirementClosureDuplicatesRemainLayerLocal(t *testing.T) {
	for _, body := range []string{
		"runtime: {recovery_on_startup: true, recovery_on_startup: false}\n",
		"runtime: {<<: {recovery_on_startup: true}, recovery_on_startup: false}\n",
		"runtime: {<<: [&r {recovery_on_startup: true}, *r]}\n",
	} {
		t.Run(body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
			writeRuntimeConfigText(t, userGlobalUnifiedConfigPath(), body)
			writeRuntimeConfigText(t, explicit, "runtime: {recovery_on_startup: false}\n")
			_, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
			if err == nil || !strings.Contains(err.Error(), "duplicate") {
				t.Fatalf("later layer hid duplicate: %v", err)
			}
		})
	}
}

func TestRetirementClosureExpandedPresenceAndEmptyMap(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo, explicit := t.TempDir(), filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, userGlobalUnifiedConfigPath(), "paths: {<<: {swarm_dir: /tmp/operator-state}}\nruntime: {<<: {recovery_on_startup: true}}\n")
	writeRuntimeConfigText(t, explicit, "paths: {<<: {swarm_dir: ''}}\nruntime: {}\n")
	got, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicit})
	if err != nil {
		t.Fatal(err)
	}
	if !got.CLI.Paths.SwarmDirSet || got.CLI.Paths.SwarmDir != "" {
		t.Fatalf("explicit empty presence lost: %#v", got.CLI.Paths)
	}
	if !got.Config.Runtime.RecoveryOnStartup || got.KeyOrigins["runtime.recovery_on_startup"].Layer != unifiedLayerUserGlobal {
		t.Fatal("empty mapping incorrectly cleared prior fields")
	}
	if got.KeyOrigins["paths.swarm_dir"].Layer != unifiedLayerExplicit {
		t.Fatal("empty leaf override lost origin")
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
		RepoRoot: projectRoot, ResolvedPaths: CLISourcePlatformSpecPaths{SourceRoot: sourceRoot},
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
