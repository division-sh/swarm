package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/config"
	"swarm/internal/runtime"
	storebackend "swarm/internal/store/backendselection"
)

func TestResolveRuntimeStoreSelectionConsumesCanonicalSources(t *testing.T) {
	t.Run("staged default remains postgres", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		got, err := resolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendPostgres || got.BackendSource != storebackend.SourceRolloutDefault {
			t.Fatalf("selection = %#v, want staged postgres rollout default", got)
		}
	})

	t.Run("env beats runtime config", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		t.Setenv(storebackend.EnvStoreBackend, storebackend.BackendPostgres.String())
		got, err := resolveRuntimeStoreSelection(t.TempDir(), storebackend.ActiveDefaultBackend().String(), false, &config.Config{
			Store: config.StoreConfig{Backend: storebackend.BackendSQLite.String()},
		})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendPostgres || got.BackendSource != storebackend.SourceEnvironment {
			t.Fatalf("selection = %#v, want env-selected postgres", got)
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
		configStore string
		wantBackend storebackend.Backend
		wantSource  storebackend.Source
	}{
		{
			name:        "flag postgres reaches store construction",
			storeMode:   storebackend.BackendPostgres.String(),
			storeFlag:   true,
			envBackend:  storebackend.BackendSQLite.String(),
			configStore: storebackend.BackendSQLite.String(),
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceFlag,
		},
		{
			name:        "env postgres reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			envBackend:  storebackend.BackendPostgres.String(),
			configStore: storebackend.BackendSQLite.String(),
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceEnvironment,
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
				ConfigPath:         writeStoreBackendRuntimeConfig(t, tt.configStore, "config/dev.db"),
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
		})
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

func TestServeHelpDocumentsStagedStoreBackends(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--help"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("serve --help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Runtime store backend", "postgres (active default)", "sqlite (selected core stores; default flip after #1088)"} {
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
	if stores.SQLDB == nil || stores.SchemaBootstrapper == nil || stores.EventStore == nil || stores.PipelineStore == nil || stores.SessionRegistry == nil || stores.ConversationStore == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil || stores.MailboxStore == nil || stores.BudgetSpendStore == nil || stores.MailboxAPIStore == nil || stores.ObservabilityStore == nil || stores.RuntimeIngressStore == nil || stores.IdempotencyStore == nil || stores.TurnStore == nil || stores.StartupOwnership == nil {
		t.Fatalf("sqlite store bundle missing selected core owners: %#v", stores)
	}
	if stores.Postgres != nil {
		t.Fatalf("sqlite store bundle Postgres = %#v, want nil", stores.Postgres)
	}
	runtimeStores := stores.runtimeStores()
	if runtimeStores.SQLDB != nil {
		t.Fatalf("sqlite runtimeStores SQLDB = %#v, want nil raw runtime SQL handle", runtimeStores.SQLDB)
	}
	if !strings.Contains(runtimeStores.ConstructionBlocker, "#1150 runtime diagnostics/logging") {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want #1150 diagnostics fail-closed blocker", runtimeStores.ConstructionBlocker)
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

func TestBuildStoresSQLiteRuntimeFailsClosedUntilRawSQLConsumersSplit(t *testing.T) {
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
	_, err = runtime.NewRuntime(ctx, runtime.RuntimeDeps{
		Config:  &config.Config{},
		Stores:  stores.runtimeStores(),
		Options: runtime.RuntimeOptions{SelfCheck: true},
	})
	if err == nil {
		t.Fatal("NewRuntime(sqlite) succeeded, want fail-closed blocker while #1150 diagnostics remains split")
	}
	if !strings.Contains(err.Error(), "#1150 runtime diagnostics/logging") {
		t.Fatalf("NewRuntime(sqlite) error = %v, want #1150 diagnostics blocker", err)
	}
}

func writeStoreBackendRuntimeConfig(t *testing.T, backend string, sqlitePath string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: false",
	}
	if strings.TrimSpace(backend) != "" || strings.TrimSpace(sqlitePath) != "" {
		lines = append(lines,
			"store:",
			"  backend: "+backend,
			"  sqlite:",
			"    path: "+sqlitePath,
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
