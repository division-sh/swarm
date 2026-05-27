package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	platformcontracts "swarm/docs/specs/swarm-platform/platform/contracts"
	"swarm/internal/config"
	"swarm/internal/runtime"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimellm "swarm/internal/runtime/llm"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
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
	if stores.SQLDB == nil || stores.SchemaBootstrapper == nil || stores.EventStore == nil || stores.SessionRegistry == nil || stores.ConversationStore == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil || stores.MailboxStore == nil || stores.MailboxAPIStore == nil || stores.ObservabilityStore == nil || stores.RuntimeIngressStore == nil || stores.IdempotencyStore == nil || stores.TurnStore == nil || stores.StartupOwnership == nil {
		t.Fatalf("sqlite store bundle missing selected core owners: %#v", stores)
	}
	if stores.Postgres != nil {
		t.Fatalf("sqlite store bundle Postgres = %#v, want nil", stores.Postgres)
	}
	runtimeStores := stores.runtimeStores()
	if runtimeStores.SQLDB != nil {
		t.Fatalf("sqlite runtimeStores SQLDB = %#v, want nil raw runtime SQL handle", runtimeStores.SQLDB)
	}
	if runtimeStores.ConstructionBlocker != "" {
		t.Fatalf("sqlite runtimeStores ConstructionBlocker = %q, want construction-ready typed SQLite owners", runtimeStores.ConstructionBlocker)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", path, err)
	}
}

func TestBuildStoresSQLiteRuntimeStartsThroughTypedStoreOwners(t *testing.T) {
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
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	var platformSpec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(platformcontracts.PlatformSpecYAML(), &platformSpec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	if err := stores.SchemaBootstrapper.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(sqlite): %v", err)
	}
	if _, err := stores.SchemaBootstrapper.ResolveSchemaCapabilities(ctx); err != nil {
		t.Fatalf("ResolveSchemaCapabilities(sqlite): %v", err)
	}
	rt, err := runtime.NewRuntime(ctx, runtime.RuntimeDeps{
		Config: &config.Config{},
		Stores: stores.runtimeStores(),
		Options: runtime.RuntimeOptions{
			WorkflowModule: sqliteRuntimeStartupTestModule{source: semanticview.Wrap(bundle)},
			LLMRuntime:     runtimellm.NoopRuntime{},
			SelfCheck:      true,
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime(sqlite): %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Runtime.Start(sqlite): %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Runtime.Shutdown(sqlite): %v", err)
	}
}

type sqliteRuntimeStartupTestModule struct {
	source semanticview.Source
}

func (s sqliteRuntimeStartupTestModule) SemanticSource() semanticview.Source { return s.source }
func (sqliteRuntimeStartupTestModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (sqliteRuntimeStartupTestModule) WorkflowNodes() []runtimepipeline.WorkflowNode { return nil }
func (sqliteRuntimeStartupTestModule) GuardRegistry() runtimepipeline.GuardRegistry  { return nil }
func (sqliteRuntimeStartupTestModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
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
