package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestResolveRuntimeStoreSelectionConsumesCanonicalSources(t *testing.T) {
	t.Run("rollout default is sqlite", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		swarmDir := t.TempDir()
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{"swarm_dir": swarmDir}))
		got, err := ResolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
			t.Fatalf("selection = %#v, want sqlite rollout default", got)
		}
		if want := filepath.Join(swarmDir, "stores", "default", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceSwarmDirDefault {
			t.Fatalf("sqlite path = %q source %q, want %q from swarm-dir default", got.SQLitePath, got.SQLitePathSource, want)
		}
	})

	t.Run("runtime config ignores retired env", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		t.Setenv("SWARM_STORE_BACKEND", storebackend.BackendPostgres.String())
		got, err := ResolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{
			Store: config.StoreConfig{Backend: storebackend.BackendSQLite.String()},
		})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRuntimeConfig {
			t.Fatalf("selection = %#v, want config-selected sqlite", got)
		}
	})

	t.Run("retired env does not affect rollout default", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		t.Setenv("SWARM_STORE_BACKEND", storebackend.BackendPostgres.String())
		got, err := ResolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
			t.Fatalf("selection = %#v, want retired env to leave rollout default sqlite", got)
		}
	})

	t.Run("runtime config sqlite path ignores retired env", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv("SWARM_SQLITE_PATH", "env/dev.db")
		got, err := ResolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{
			Store: config.StoreConfig{
				Backend: storebackend.BackendSQLite.String(),
				SQLite:  config.StoreSQLiteConfig{Path: "config/dev.db"},
			},
		})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if want := filepath.Join(repo, "config", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceRuntimeConfig {
			t.Fatalf("sqlite path = %q source %q, want %q from runtime config", got.SQLitePath, got.SQLitePathSource, want)
		}
	})

	t.Run("retired sqlite path env does not affect default", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv("SWARM_SQLITE_PATH", "env/dev.db")
		got, err := ResolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if !strings.HasSuffix(got.SQLitePath, filepath.Join("stores", "default", "dev.db")) || got.SQLitePathSource != storebackend.SourceSwarmDirDefault {
			t.Fatalf("sqlite path = %q source %q, want swarm-dir default not retired env", got.SQLitePath, got.SQLitePathSource)
		}
	})

	t.Run("flag beats config and ignores retired env", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv("SWARM_STORE_BACKEND", storebackend.BackendPostgres.String())
		got, err := ResolveRuntimeStoreSelection(repo, storebackend.BackendSQLite.String(), true, &config.Config{
			Store: config.StoreConfig{
				Backend: storebackend.BackendPostgres.String(),
				SQLite:  config.StoreSQLiteConfig{Path: "config/dev.db"},
			},
		})
		if err != nil {
			t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceFlag {
			t.Fatalf("selection = %#v, want flag-selected sqlite", got)
		}
		if want := filepath.Join(repo, "config", "dev.db"); got.SQLitePath != want {
			t.Fatalf("sqlite path = %q, want %q", got.SQLitePath, want)
		}
	})
}

func TestDatabaseEnvDetailsDoNotImplyPostgresSelection(t *testing.T) {
	unsetStoreSelectorEnv(t)
	t.Setenv("SWARM_DB_HOST", "db-env-host")
	t.Setenv("SWARM_DB_PORT", "15432")
	t.Setenv("SWARM_DB_NAME", "db_env_name")
	t.Setenv("SWARM_DB_USER", "db_env_user")
	t.Setenv("SWARM_DB_SSLMODE", "require")
	t.Setenv("SWARM_DB_POOL_SIZE", "9")
	t.Setenv("PGHOST", "pg-env-host")
	t.Setenv("PGPORT", "25432")
	t.Setenv("PGDATABASE", "pg_env_name")
	t.Setenv("PGUSER", "pg_env_user")

	cfg, err := defaultRuntimeConfig()
	if err != nil {
		t.Fatalf("defaultRuntimeConfig: %v", err)
	}
	if cfg.Database.Host != "127.0.0.1" || cfg.Database.Port != 5432 || cfg.Database.Name != "swarm" || cfg.Database.User != "postgres" || cfg.Database.SSLMode != "disable" || cfg.Database.PoolSize != 5 {
		t.Fatalf("database env fallback affected built-in default config: %#v", cfg.Database)
	}
	got, err := ResolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, cfg)
	if err != nil {
		t.Fatalf("ResolveRuntimeStoreSelection: %v", err)
	}
	if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
		t.Fatalf("selection = %#v, want DB env details to leave backend at rollout default sqlite", got)
	}
}

func TestExplicitRuntimeConfigDatabaseBeatsDatabaseEnv(t *testing.T) {
	t.Setenv("PGHOST", "pg-env-host")
	t.Setenv("PGPASSWORD", "pg-env-password")

	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{
		RepoRoot:     t.TempDir(),
		ExplicitPath: writeStoreDatabaseRuntimeConfig(t),
	})
	if err != nil {
		t.Fatalf("LoadRuntimeConfigWithOptions: %v", err)
	}
	got := cfgResult.Config.Database
	if got.Host != "db-config-host" || got.Port != 15433 || got.Name != "db_config_name" || got.User != "db_config_user" || got.SSLMode != "verify-full" || got.PoolSize != 11 {
		t.Fatalf("database config = %#v, want explicit runtime config values", got)
	}
	if got.Password != "" {
		t.Fatalf("database password = %q, want absent password to remain unset rather than sourced from env", got.Password)
	}
}

func TestSwarmExampleDoesNotPromoteDatabasePassword(t *testing.T) {
	runtimeConfig, err := os.ReadFile(filepath.Join(RepoRoot(), "swarm.example.yaml"))
	if err != nil {
		t.Fatalf("read swarm.example.yaml: %v", err)
	}
	text := string(runtimeConfig)
	if strings.Contains(text, "\n#   password:") || strings.Contains(text, "\npassword:") {
		t.Fatalf("swarm.example.yaml must not promote plaintext DB password config:\n%s", text)
	}
	for _, want := range []string{"Secret references", "password_secret_key", "password_file", "password_env", "never store plaintext secrets"} {
		if !strings.Contains(text, want) {
			t.Fatalf("swarm.example.yaml missing credential guidance %q:\n%s", want, text)
		}
	}
}

func TestServeCommandCapturesStoreFlagForCanonicalResolver(t *testing.T) {
	unsetStoreSelectorEnv(t)

	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--store", "sqlite"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d, want 0 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if captured.StoreMode != storebackend.BackendSQLite.String() || !captured.StoreModeSet {
		t.Fatalf("serve store opts = mode %q set %v, want sqlite set=true", captured.StoreMode, captured.StoreModeSet)
	}
}

func TestServeHelpDocumentsSQLiteDefaultAndPostgresOptIn(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--help"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("serve --help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Runtime store backend", "sqlite (local/dev default)", "postgres (explicit opt-in production/external backend)"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("serve help missing %q:\n%s", want, stdout.String())
		}
	}
}

func writeStoreDatabaseRuntimeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	contents := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"database:",
		"  host: db-config-host",
		"  port: 15433",
		"  name: db_config_name",
		"  user: db_config_user",
		"  sslmode: verify-full",
		"  pool_size: 11",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}
