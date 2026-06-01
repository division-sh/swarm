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
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestResolveRuntimeStoreSelectionConsumesCanonicalSources(t *testing.T) {
	t.Run("rollout default is sqlite", func(t *testing.T) {
		unsetStoreSelectorEnv(t)
		repo := t.TempDir()
		got, err := resolveRuntimeStoreSelection(repo, storebackend.ActiveDefaultBackend().String(), false, &config.Config{})
		if err != nil {
			t.Fatalf("resolveRuntimeStoreSelection: %v", err)
		}
		if got.Backend != storebackend.BackendSQLite || got.BackendSource != storebackend.SourceRolloutDefault {
			t.Fatalf("selection = %#v, want sqlite rollout default", got)
		}
		if want := filepath.Join(repo, ".swarm", "dev.db"); got.SQLitePath != want || got.SQLitePathSource != storebackend.SourceRolloutDefault {
			t.Fatalf("sqlite path = %q source %q, want %q from rollout default", got.SQLitePath, got.SQLitePathSource, want)
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
			name:        "rollout default sqlite reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
			wantBackend: storebackend.BackendSQLite,
			wantSource:  storebackend.SourceRolloutDefault,
		},
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
	if stores.SQLDB == nil || stores.RuntimeLogStore == nil || stores.SchemaBootstrapper == nil || stores.EventStore == nil || stores.PipelineStore == nil || stores.SessionRegistry == nil || stores.ConversationStore == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil || stores.MailboxMaterializer == nil || stores.MailboxStore == nil || stores.BudgetSpendStore == nil || stores.MailboxAPIStore == nil || stores.ObservabilityStore == nil || stores.AgentUsageStore == nil || stores.RuntimeIngressStore == nil || stores.IdempotencyStore == nil || stores.TurnStore == nil || stores.StartupOwnership == nil {
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
	if _, err := initializeStateStores(ctx, stores, bundle); err != nil {
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
platform_version: ">=1.0.0"
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
