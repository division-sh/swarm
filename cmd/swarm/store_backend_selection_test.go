package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestResolveRuntimeStoreSelectionConsumesCanonicalSources(t *testing.T) {
	t.Run("rollout default is sqlite", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		swarmDir := t.TempDir()
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{"swarm_dir": swarmDir}))
		got, err := resolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
			t.Fatalf("selection = %#v, want sqlite rollout default", got)
		}
		if want := filepath.Join(swarmDir, "stores", "default", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceSwarmDirDefault {
			t.Fatalf("sqlite path = %q source %q, want %q from swarm-dir default", got.SQLitePath, got.SQLitePathSource, want)
		}
	})

	t.Run("runtime config beats env", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		t.Setenv(storebackend.EnvStoreBackend, storebackend.BackendPostgres.String())
		got, err := resolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{
			Store: config.StoreConfig{Backend: storebackend.BackendSQLite.String()},
		})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRuntimeConfig {
			t.Fatalf("selection = %#v, want config-selected sqlite", got)
		}
	})

	t.Run("env fallback remains visible", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		t.Setenv(storebackend.EnvStoreBackend, storebackend.BackendPostgres.String())
		got, err := resolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendPostgres || got.BackendSource != storebackend.SourceEnvironment {
			t.Fatalf("selection = %#v, want env fallback postgres", got)
		}
	})

	t.Run("runtime config sqlite path beats env", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv(storebackend.EnvSQLitePath, "env/dev.db")
		got, err := resolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{
			Store: config.StoreConfig{
				Backend: storebackend.BackendSQLite.String(),
				SQLite:  config.StoreSQLiteConfig{Path: "config/dev.db"},
			},
		})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if want := filepath.Join(repo, "config", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceRuntimeConfig {
			t.Fatalf("sqlite path = %q source %q, want %q from runtime config", got.SQLitePath, got.SQLitePathSource, want)
		}
	})

	t.Run("env sqlite path fallback remains visible", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv(storebackend.EnvSQLitePath, "env/dev.db")
		got, err := resolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if want := filepath.Join(repo, "env", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceEnvironment {
			t.Fatalf("sqlite path = %q source %q, want %q from env fallback", got.SQLitePath, got.SQLitePathSource, want)
		}
	})

	t.Run("flag beats env and config", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		t.Setenv(storebackend.EnvStoreBackend, storebackend.BackendPostgres.String())
		got, err := resolveRuntimeStoreSelection(repo, storebackend.BackendSQLite.String(), true, &config.Config{
			Store: config.StoreConfig{
				Backend: storebackend.BackendPostgres.String(),
				SQLite:  config.StoreSQLiteConfig{Path: "config/dev.db"},
			},
		})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceFlag {
			t.Fatalf("selection = %#v, want flag-selected sqlite", got)
		}
		if want := filepath.Join(repo, "config", "dev.db"); got.SQLitePath != want {
			t.Fatalf("sqlite path = %q, want %q", got.SQLitePath, want)
		}
	})
}

func TestRunServeRuntimeConsumesCanonicalStoreSelectionBeforeStoreConstruction(t *testing.T) {
	tests := []struct {
		name        string
		storeMode   string
		storeFlag   bool
		envBackend  string
		envPath     string
		configStore string
		configPath  string
		wantBackend storebackend.Backend
		wantSource  storebackend.Source
		wantPathSrc storebackend.Source
	}{
		{
			name:        "rollout default sqlite reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			wantBackend: storebackend.BackendSQLite,
			wantSource:  storebackend.SourceRolloutDefault,
			wantPathSrc: storebackend.SourceSwarmDirDefault,
		},
		{
			name:        "flag postgres reaches store construction",
			storeMode:   storebackend.BackendPostgres.String(),
			storeFlag:   true,
			envBackend:  storebackend.BackendSQLite.String(),
			configStore: storebackend.BackendSQLite.String(),
			configPath:  "config/dev.db",
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceFlag,
		},
		{
			name:        "env postgres fallback reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			envBackend:  storebackend.BackendPostgres.String(),
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceEnvironment,
		},
		{
			name:        "config sqlite beats env selectors before store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			envBackend:  storebackend.BackendPostgres.String(),
			envPath:     "env/dev.db",
			configStore: storebackend.BackendSQLite.String(),
			configPath:  "config/dev.db",
			wantBackend: storebackend.BackendSQLite,
			wantSource:  storebackend.SourceRuntimeConfig,
			wantPathSrc: storebackend.SourceRuntimeConfig,
		},
		{
			name:        "config postgres reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			configStore: storebackend.BackendPostgres.String(),
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceRuntimeConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetStoreSelectorEnv(t)
			if tt.envBackend != "" {
				t.Setenv(storebackend.EnvStoreBackend, tt.envBackend)
			}
			if tt.envPath != "" {
				t.Setenv(storebackend.EnvSQLitePath, tt.envPath)
			}
			oldBuildStores := buildStoresForServe
			var captured storebackend.Selection
			buildStoresForServe = func(_ context.Context, selection storebackend.Selection, _ *config.Config) (storeBundle, error) {
				captured = selection
				return storeBundle{}, errors.New("stop after selector proof")
			}
			t.Cleanup(func() {
				buildStoresForServe = oldBuildStores
			})

			var out bytes.Buffer
			code := runServeRuntime(context.Background(), t.TempDir(), serveOptions{
				ConfigPath:         writeStoreBackendRuntimeConfig(t, tt.configStore, tt.configPath),
				StoreMode:          tt.storeMode,
				StoreModeSet:       tt.storeFlag,
				APIListenAddr:      defaultAPIListenAddr,
				MCPListenAddr:      defaultMCPListenAddr,
				ShutdownGrace:      runtime.DefaultShutdownGrace,
				SelfCheck:          true,
				RequireBundleMatch: true,
				Verbose:            true,
				Output:             &out,
			})
			if code != 1 {
				t.Fatalf("runServeRuntime code = %d, want selector proof failure 1; output=%s", code, out.String())
			}
			if captured.Backend != tt.wantBackend || captured.BackendSource != tt.wantSource {
				t.Fatalf("selection = %#v, want %s from %s", captured, tt.wantBackend, tt.wantSource)
			}
			if tt.wantPathSrc != "" && captured.SQLitePathSource != tt.wantPathSrc {
				t.Fatalf("sqlite path source = %q, want %q in selection %#v", captured.SQLitePathSource, tt.wantPathSrc, captured)
			}
		})
	}
}

func TestRunServeRuntimeStoreFlagCanOverrideConfigPostgresBeforePasswordRequirement(t *testing.T) {
	unsetStoreSelectorEnv(t)
	configPath := writeStoreBackendRuntimeConfigWithoutPasswordSource(t, storebackend.BackendPostgres.String())

	oldBuildStores := buildStoresForServe
	var captured storebackend.Selection
	buildStoresForServe = func(_ context.Context, selection storebackend.Selection, _ *config.Config) (storeBundle, error) {
		captured = selection
		return storeBundle{}, errors.New("stop after selector proof")
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})

	var out bytes.Buffer
	code := runServeRuntime(context.Background(), t.TempDir(), serveOptions{
		ConfigPath:         configPath,
		StoreMode:          storebackend.BackendSQLite.String(),
		StoreModeSet:       true,
		APIListenAddr:      defaultAPIListenAddr,
		MCPListenAddr:      defaultMCPListenAddr,
		ShutdownGrace:      runtime.DefaultShutdownGrace,
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
		Output:             &out,
	})
	if code != 1 {
		t.Fatalf("runServeRuntime code = %d, want selector proof failure 1; output=%s", code, out.String())
	}
	if captured.Backend != storebackend.BackendSQLite || captured.BackendSource != storebackend.SourceFlag {
		t.Fatalf("selection = %#v, want flag-selected sqlite before postgres password requirement", captured)
	}
	if strings.Contains(out.String(), "postgres store requires exactly one database password source") {
		t.Fatalf("output = %q, want no config-load postgres password rejection before effective store selection", out.String())
	}
}

func TestDatabaseEnvDetailsDoNotImplyPostgresSelection(t *testing.T) {
	unsetStoreSelectorEnv(t)
	t.Setenv("SWARM_DB_HOST", "db-env-host")
	t.Setenv("SWARM_DB_PORT", "15432")
	t.Setenv("SWARM_DB_NAME", "db_env_name")
	t.Setenv("SWARM_DB_USER", "db_env_user")
	t.Setenv("SWARM_DB_SSLMODE", "require")
	t.Setenv("SWARM_DB_POOL_SIZE", "9")

	cfg, err := defaultRuntimeConfig()
	if err != nil {
		t.Fatalf("defaultRuntimeConfig: %v", err)
	}
	if cfg.Database.Host != "db-env-host" || cfg.Database.Port != 15432 || cfg.Database.Name != "db_env_name" || cfg.Database.User != "db_env_user" || cfg.Database.SSLMode != "require" || cfg.Database.PoolSize != 9 {
		t.Fatalf("database env fallback not reflected in built-in default config: %#v", cfg.Database)
	}
	got, err := resolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, cfg)
	if err != nil {
		t.Fatalf("resolveRuntimeStoreSelection: %v", err)
	}
	if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
		t.Fatalf("selection = %#v, want DB env details to leave backend at rollout default sqlite", got)
	}
}

func TestExplicitRuntimeConfigDatabaseBeatsDatabaseEnv(t *testing.T) {
	t.Setenv("SWARM_DB_HOST", "db-env-host")
	t.Setenv("PGHOST", "pg-env-host")
	t.Setenv("SWARM_DB_PASSWORD", "env-password")
	t.Setenv("PGPASSWORD", "pg-env-password")

	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:     t.TempDir(),
		ExplicitPath: writeStoreDatabaseRuntimeConfig(t),
	})
	if err != nil {
		t.Fatalf("loadRuntimeConfigWithOptions: %v", err)
	}
	got := cfgResult.Config.Database
	if got.Host != "db-config-host" || got.Port != 15433 || got.Name != "db_config_name" || got.User != "db_config_user" || got.SSLMode != "verify-full" || got.PoolSize != 11 {
		t.Fatalf("database config = %#v, want explicit runtime config values", got)
	}
	if got.Password != "" {
		t.Fatalf("database password = %q, want absent password to remain unset rather than sourced from env", got.Password)
	}
}

func TestPostgresDSNFromConfigRejectsImplicitPasswordEnv(t *testing.T) {
	t.Setenv("SWARM_DB_PASSWORD", "env-password")
	t.Setenv("PGPASSWORD", "pg-env-password")

	_, err := postgresDSNFromConfig(context.Background(), config.DatabaseConfig{
		Host:    "127.0.0.1",
		Port:    5432,
		Name:    "swarm",
		User:    "postgres",
		SSLMode: "disable",
	})
	if err == nil || !strings.Contains(err.Error(), "postgres store requires exactly one database password source") {
		t.Fatalf("postgresDSNFromConfig error = %v, want implicit env fail-closed guidance", err)
	}
}

func TestPostgresDSNFromConfigSecretKeyUsesFileStoreNotEnvOverlay(t *testing.T) {
	ctx := context.Background()
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialsPath)
	t.Setenv("POSTGRES_PASSWORD", "env-shadow")

	fileStore, err := credentialFileStore()
	if err != nil {
		t.Fatalf("credentialFileStore: %v", err)
	}
	if err := fileStore.Set(ctx, "postgres_password", "file-secret"); err != nil {
		t.Fatalf("seed credential file: %v", err)
	}

	dsn, err := postgresDSNFromConfig(ctx, config.DatabaseConfig{
		Host:              "127.0.0.1",
		Port:              5432,
		Name:              "swarm",
		User:              "postgres",
		PasswordSecretKey: "postgres_password",
		SSLMode:           "disable",
	})
	if err != nil {
		t.Fatalf("postgresDSNFromConfig: %v", err)
	}
	if !strings.Contains(dsn, "password='file-secret'") {
		t.Fatalf("dsn = %q, want file-backed password", dsn)
	}
	if strings.Contains(dsn, "env-shadow") {
		t.Fatalf("dsn = %q, password_secret_key must not use env overlay", dsn)
	}
}

func TestRuntimeConfigExampleDoesNotPromoteDatabasePassword(t *testing.T) {
	runtimeConfig, err := os.ReadFile(filepath.Join(repoRoot(), "runtime-config.example.yaml"))
	if err != nil {
		t.Fatalf("read runtime-config.example.yaml: %v", err)
	}
	text := string(runtimeConfig)
	if strings.Contains(text, "password:") {
		t.Fatalf("runtime-config.example.yaml must not promote plaintext DB password config:\n%s", text)
	}
	if !strings.Contains(text, "Keep credentials") || !strings.Contains(text, "swarm secrets") {
		t.Fatalf("runtime-config.example.yaml must keep credential guidance out of plaintext config:\n%s", text)
	}
}

func TestServeCommandCapturesStoreFlagForCanonicalResolver(t *testing.T) {
	unsetStoreSelectorEnv(t)

	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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

func TestBuildStoresAcceptsSQLiteSelectedCoreRuntimeStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.db")
	stores, err := buildStores(context.Background(), storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       path,
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, &config.Config{})
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeDB(stores.SQLDB) })
	if stores.SQLDB == nil || stores.RuntimeLogStore == nil || stores.SchemaBootstrapper == nil || stores.EventStore == nil || stores.PipelineStore == nil || stores.SessionRegistry == nil || stores.ConversationStore == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil || stores.MailboxMaterializer == nil || stores.MailboxStore == nil || stores.BudgetSpendStore == nil || stores.InboundStore == nil || stores.MailboxAPIStore == nil || stores.ObservabilityStore == nil || stores.AgentUsageStore == nil || stores.AgentDeliveryLifecycleStore == nil || stores.RuntimeIngressStore == nil || stores.IdempotencyStore == nil || stores.TurnStore == nil || stores.StartupOwnership == nil || stores.AgentConversationReadStore == nil {
		t.Fatalf("sqlite store bundle missing selected core owners: %#v", stores)
	}
	if stores.Postgres != nil {
		t.Fatalf("sqlite store bundle Postgres = %#v, want nil", stores.Postgres)
	}
	if _, ok := stores.ObservabilityStore.(apiv1.RunReadStore); !ok {
		t.Fatalf("sqlite ObservabilityStore = %T, want selected run read store for run.get/list", stores.ObservabilityStore)
	}
	if _, ok := stores.ObservabilityStore.(apiv1.EntityReadStore); !ok {
		t.Fatalf("sqlite ObservabilityStore = %T, want selected entity read store for entity.*", stores.ObservabilityStore)
	}
	apiCaps, err := stores.facade().apiCapabilities(selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("sqlite apiCapabilities: %v", err)
	}
	if apiCaps.AgentConversations == nil {
		t.Fatal("sqlite apiCapabilities missing AgentConversations pure operator-read owner")
	}
	if apiCaps.BundleCatalog == nil {
		t.Fatal("sqlite apiCapabilities missing BundleCatalog pure operator-read owner")
	}
	classifiedOut := map[string]any{
		"ConversationForks":   apiCaps.ConversationForks,
		"BundleDelete":        apiCaps.BundleDelete,
		"RunForkAvailability": apiCaps.RunForkAvailability,
		"RunFork":             apiCaps.RunFork,
		"ResetCoordinator":    apiCaps.ResetCoordinator,
		"ResetQuiescer":       apiCaps.ResetQuiescer,
		"ResetCleaner":        apiCaps.ResetCleaner,
	}
	for name, capability := range classifiedOut {
		if capability != nil {
			t.Fatalf("sqlite optional capability %s = %T, want nil classified split/postgres-only capability", name, capability)
		}
	}
	runtimeStores := stores.runtimeStores()
	if runtimeStores.SQLDB != nil {
		t.Fatalf("sqlite runtimeStores SQLDB = %#v, want nil raw runtime SQL handle", runtimeStores.SQLDB)
	}
	if runtimeStores.RuntimeLogStore == nil {
		t.Fatal("sqlite runtimeStores RuntimeLogStore missing backend-neutral runtime diagnostics owner")
	}
	if runtimeStores.MailboxMaterializer == nil {
		t.Fatal("sqlite runtimeStores MailboxMaterializer missing backend-neutral mailbox_write owner")
	}
	if runtimeStores.ConstructionBlocker != "" {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want explicit sqlite construction unblocked after mailbox_write owner", runtimeStores.ConstructionBlocker)
	}
	if strings.Contains(runtimeStores.ConstructionBlocker, "pipeline coordination/background nodes") {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want #1147 pipeline/background owner removed from residual blocker", runtimeStores.ConstructionBlocker)
	}
	if strings.Contains(runtimeStores.ConstructionBlocker, "budget tracking/spend ledger") {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want #1148 budget/spend owner removed from residual blocker", runtimeStores.ConstructionBlocker)
	}
	if runtimeStores.BudgetSpendStore == nil {
		t.Fatal("sqlite runtimeStores BudgetSpendStore missing backend-neutral budget/spend owner")
	}
	if runtimeStores.InboundStore == nil {
		t.Fatal("sqlite runtimeStores InboundStore missing backend-neutral inbound webhook owner")
	}
	if runtimeStores.ToolEntityStore == nil {
		t.Fatal("sqlite runtimeStores ToolEntityStore missing backend-neutral entity tool owner")
	}
	if runtimeStores.HumanTaskStore == nil {
		t.Fatal("sqlite runtimeStores HumanTaskStore missing backend-neutral human-task owner")
	}
	if runtimeStores.PipelineStore == nil || !runtimeStores.PipelineStore.Enabled() {
		t.Fatal("sqlite runtimeStores PipelineStore missing enabled backend-neutral pipeline owner")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", path, err)
	}
}

func TestBuildStoresSQLiteSelectsRunBundleContextForServedEventPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.db")
	stores, err := buildStores(context.Background(), storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       path,
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, &config.Config{})
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeDB(stores.SQLDB) })
	runBundleContext, ok := stores.ObservabilityStore.(apiv1.RunBundleContextStore)
	if !ok {
		t.Fatalf("sqlite ObservabilityStore = %T, want selected run bundle context store for event.publish --run-id", stores.ObservabilityStore)
	}
	if got := stores.facade().apiRunBundleContextStore(); got == nil || got != runBundleContext {
		t.Fatalf("selected API run bundle context = %T, want sqlite selected owner %T", got, runBundleContext)
	}
}

func TestSelectedOperatorReadConstructionParityClassifiesSQLitePostgresDelta(t *testing.T) {
	ctx := context.Background()
	sqliteStores, err := buildStores(ctx, storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       filepath.Join(t.TempDir(), "dev.db"),
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, &config.Config{})
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeDB(sqliteStores.SQLDB) })

	postgresStores := selectedPostgresStoreBundle(&store.PostgresStore{}, &config.Config{})
	postgresCaps, err := postgresStores.facade().apiCapabilities(selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("postgres apiCapabilities: %v", err)
	}
	sqliteCaps, err := sqliteStores.facade().apiCapabilities(selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("sqlite apiCapabilities: %v", err)
	}

	pureOperatorReads := map[string]struct {
		postgres any
		sqlite   any
	}{
		"AgentConversations": {postgres: postgresCaps.AgentConversations, sqlite: sqliteCaps.AgentConversations},
		"BundleCatalog":      {postgres: postgresCaps.BundleCatalog, sqlite: sqliteCaps.BundleCatalog},
	}
	for name, caps := range pureOperatorReads {
		if caps.postgres == nil {
			t.Fatalf("postgres pure operator-read capability %s unexpectedly nil; parity guard lost its baseline", name)
		}
		if caps.sqlite == nil {
			t.Fatalf("sqlite pure operator-read capability %s nil while postgres wires it; wire SQLite or classify explicitly", name)
		}
	}

	classifiedSQLiteOmissions := map[string]struct {
		postgres any
		sqlite   any
		reason   string
	}{
		"ConversationForks": {
			postgres: postgresCaps.ConversationForks,
			sqlite:   sqliteCaps.ConversationForks,
			reason:   "mixed read/write lifecycle capability; split to #1783 before SQLite promotion",
		},
		"BundleDelete": {
			postgres: postgresCaps.BundleDelete,
			sqlite:   sqliteCaps.BundleDelete,
			reason:   "mutating/destructive bundle capability, not pure operator read",
		},
		"RunForkAvailability": {
			postgres: postgresCaps.RunForkAvailability,
			sqlite:   sqliteCaps.RunForkAvailability,
			reason:   "run.fork availability/execution product seam split from operator reads",
		},
		"RunFork": {
			postgres: postgresCaps.RunFork,
			sqlite:   sqliteCaps.RunFork,
			reason:   "run.fork execution product seam split from operator reads",
		},
		"ResetCoordinator": {
			postgres: postgresCaps.ResetCoordinator,
			sqlite:   sqliteCaps.ResetCoordinator,
			reason:   "destructive reset capability split from operator reads",
		},
		"ResetQuiescer": {
			postgres: postgresCaps.ResetQuiescer,
			sqlite:   sqliteCaps.ResetQuiescer,
			reason:   "destructive reset capability split from operator reads",
		},
		"ResetCleaner": {
			postgres: postgresCaps.ResetCleaner,
			sqlite:   sqliteCaps.ResetCleaner,
			reason:   "destructive reset capability split from operator reads",
		},
	}
	for name, caps := range classifiedSQLiteOmissions {
		if caps.postgres == nil {
			t.Fatalf("classified optional capability %s no longer has a postgres baseline; update #1782 construction-parity guard classification: %s", name, caps.reason)
		}
		if caps.sqlite != nil {
			t.Fatalf("sqlite optional capability %s = %T, want nil until separately gated: %s", name, caps.sqlite, caps.reason)
		}
	}
}

func TestBuildStoresSQLiteRuntimeNoLongerFailsClosedOnMailboxMaterializationOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dev.db")
	stores, err := buildStores(ctx, storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       path,
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, &config.Config{})
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeDB(stores.SQLDB) })
	runtimeStores := stores.runtimeStores()
	if runtimeStores.SQLDB != nil {
		t.Fatalf("sqlite runtimeStores SQLDB = %#v, want nil raw runtime SQL handle", runtimeStores.SQLDB)
	}
	if runtimeStores.RuntimeLogStore == nil {
		t.Fatal("sqlite runtimeStores RuntimeLogStore missing backend-neutral runtime diagnostics owner")
	}
	if runtimeStores.MailboxMaterializer == nil {
		t.Fatal("sqlite runtimeStores MailboxMaterializer missing backend-neutral mailbox_write owner")
	}
	if runtimeStores.ConstructionBlocker != "" {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want construction blocker removed after mailbox_write owner", runtimeStores.ConstructionBlocker)
	}
	bundle := loadStoreBackendSelectionWorkflowBundle(t)
	if _, err := initializeStateStores(ctx, stores, bundle, false); err != nil {
		t.Fatalf("initializeStateStores(sqlite): %v", err)
	}
	rt, err := runtime.NewRuntime(ctx, runtime.RuntimeDeps{
		Config: &config.Config{},
		Stores: runtimeStores,
		Options: runtime.RuntimeOptions{
			SelfCheck:      true,
			WorkflowModule: stubWorkflowModule{source: semanticview.Wrap(bundle)},
			LLMRuntime:     storeBackendSelectionNoopLLMRuntime{},
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime(sqlite): %v", err)
	}
	if rt.Pipeline == nil {
		t.Fatal("NewRuntime(sqlite) Pipeline = nil, want runtime construction to consume SQLite pipeline store")
	}
	if rt.Stores.SQLDB != nil {
		t.Fatalf("NewRuntime(sqlite) raw SQLDB = %#v, want nil", rt.Stores.SQLDB)
	}
	if rt.Stores.MailboxMaterializer == nil {
		t.Fatal("NewRuntime(sqlite) MailboxMaterializer missing")
	}
}

type storeBackendSelectionNoopLLMRuntime struct{}

func (storeBackendSelectionNoopLLMRuntime) StartSession(context.Context, string, string, []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{}, nil
}

func (storeBackendSelectionNoopLLMRuntime) ContinueSession(context.Context, *runtimellm.Session, runtimellm.Message) (*runtimellm.Response, error) {
	return &runtimellm.Response{}, nil
}

func loadStoreBackendSelectionWorkflowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: store-backend-selection
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: store-backend-selection
initial_state: idle
states:
  - idle
terminal_states:
  - idle
`)
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeStoreBackendSelectionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writeStoreBackendSelectionFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeStoreBackendRuntimeConfig(t *testing.T, backend string, sqlitePath string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
	}
	if strings.TrimSpace(backend) != "" || strings.TrimSpace(sqlitePath) != "" {
		lines = append(lines,
			"store:",
			"  backend: "+backend,
			"  sqlite:",
			"    path: "+sqlitePath,
		)
	}
	if strings.TrimSpace(backend) == storebackend.BackendPostgres.String() {
		lines = append(lines,
			"database:",
			"  password_env: PGPASSWORD",
		)
	}
	lines = append(lines,
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}

func writeStoreBackendRuntimeConfigWithoutPasswordSource(t *testing.T, backend string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	contents := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
		"store:",
		"  backend: " + backend,
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

func unsetStoreSelectorEnv(t *testing.T) {
	t.Helper()
	unsetEnvForTest(t, storebackend.EnvStoreBackend)
	unsetEnvForTest(t, storebackend.EnvSQLitePath)
}

func unsetEnvForTest(t *testing.T, key string) {
	t.Helper()
	previous, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, previous)
			return
		}
		_ = os.Unsetenv(key)
	})
}
