package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestRunServeRuntimeConsumesCanonicalStoreSelectionBeforeStoreConstruction(t *testing.T) {
	tests := []struct {
		name        string
		storeMode   string
		storeFlag   bool
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
			configStore: storebackend.BackendSQLite.String(),
			configPath:  "config/dev.db",
			wantBackend: storebackend.BackendPostgres,
			wantSource:  storebackend.SourceFlag,
		},
		{
			name:        "config sqlite reaches store construction",
			storeMode:   storebackend.ActiveDefaultBackend().String(),
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
			code := Run(context.Background(), t.TempDir(), cliapp.ServeOptions{
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
				t.Fatalf("Run code = %d, want selector proof failure 1; output=%s", code, out.String())
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
	code := Run(context.Background(), t.TempDir(), cliapp.ServeOptions{
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
		t.Fatalf("Run code = %d, want selector proof failure 1; output=%s", code, out.String())
	}
	if captured.Backend != storebackend.BackendSQLite || captured.BackendSource != storebackend.SourceFlag {
		t.Fatalf("selection = %#v, want flag-selected sqlite before postgres password requirement", captured)
	}
	if strings.Contains(out.String(), "postgres store requires exactly one database password source") {
		t.Fatalf("output = %q, want no config-load postgres password rejection before effective store selection", out.String())
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

	fileStore, err := cliapp.CredentialFileStore()
	if err != nil {
		t.Fatalf("cliapp.CredentialFileStore: %v", err)
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
	if stores.SQLDB == nil || stores.RuntimeLogStore == nil || stores.SchemaBootstrapper == nil || stores.EventStore == nil || stores.PipelineStore == nil || stores.SessionRegistry == nil || stores.ConversationStore == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil || stores.MailboxMaterializer == nil || stores.MailboxStore == nil || stores.BudgetSpendStore == nil || stores.InboundStore == nil || stores.MailboxAPIStore == nil || stores.ObservabilityStore == nil || stores.AgentUsageStore == nil || stores.AgentDeliveryLifecycleStore == nil || stores.RuntimeIngressStore == nil || stores.IdempotencyStore == nil || stores.StartupOwnership == nil || stores.AgentConversationReadStore == nil {
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
	if apiCaps.ConversationForks == nil {
		t.Fatal("sqlite apiCapabilities missing ConversationForks read owner")
	}
	if apiCaps.ConversationForkLifecycle == nil {
		t.Fatal("sqlite apiCapabilities missing ConversationForkLifecycle mutation owner")
	}
	classifiedOut := map[string]any{
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
	if apiCaps.RuntimeContexts != nil {
		t.Fatalf("sqlite optional capability RuntimeContexts = %T, want nil classified split/postgres-only capability", apiCaps.RuntimeContexts)
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

	ledger := selectedOperatorReadConstructionCapabilityLedger()
	seen := map[string]struct{}{}
	for _, entry := range ledger {
		if entry.Reason == "" {
			t.Fatalf("selected operator-read construction capability %s missing classification reason", entry.Name)
		}
		if _, exists := seen[entry.Name]; exists {
			t.Fatalf("selected operator-read construction capability %s appears more than once in classification ledger", entry.Name)
		}
		seen[entry.Name] = struct{}{}
		postgresValue, ok := selectedAPICapabilityField(postgresCaps, entry.Name)
		if !ok {
			t.Fatalf("selected operator-read construction capability ledger names unknown field %s", entry.Name)
		}
		sqliteValue, _ := selectedAPICapabilityField(sqliteCaps, entry.Name)
		postgresConfigured := selectedAPICapabilityConfigured(postgresValue)
		sqliteConfigured := selectedAPICapabilityConfigured(sqliteValue)
		switch entry.Classification {
		case "wired_both":
			if !postgresConfigured {
				t.Fatalf("postgres selected operator-read capability %s unexpectedly nil; parity guard lost its baseline", entry.Name)
			}
			if !sqliteConfigured {
				t.Fatalf("sqlite selected operator-read capability %s nil while postgres wires it; wire SQLite or classify explicitly: %s", entry.Name, entry.Reason)
			}
		case "split_with_issue_ref", "different_semantic_concept_with_proof", "postgres_only_with_spec_ref":
			if entry.Issue == 0 && strings.TrimSpace(entry.SpecRef) == "" {
				t.Fatalf("selected operator-read construction capability %s classification %s missing issue or governing spec ref", entry.Name, entry.Classification)
			}
			if entry.RequiresPostgresBaseline && !postgresConfigured {
				t.Fatalf("classified optional capability %s no longer has a postgres baseline; update construction-parity classification: %s", entry.Name, entry.Reason)
			}
			if sqliteConfigured {
				t.Fatalf("sqlite optional capability %s is configured; keep it classified until separately gated: %s", entry.Name, entry.Reason)
			}
		default:
			t.Fatalf("selected operator-read construction capability %s has unsupported classification %q", entry.Name, entry.Classification)
		}
	}
	for _, field := range selectedAPICapabilityFieldNames() {
		if _, ok := seen[field]; !ok {
			t.Fatalf("selected operator-read construction capability ledger missing field %s", field)
		}
	}
}

type selectedOperatorReadConstructionCapabilityEntry struct {
	Name                     string
	Classification           string
	Issue                    int
	SpecRef                  string
	RequiresPostgresBaseline bool
	Reason                   string
}

func selectedOperatorReadConstructionCapabilityLedger() []selectedOperatorReadConstructionCapabilityEntry {
	return []selectedOperatorReadConstructionCapabilityEntry{
		{Name: "Database", Classification: "wired_both", Reason: "health.check/readiness pinger is selected on SQLite and Postgres"},
		{Name: "Runs", Classification: "wired_both", Reason: "run.get/list/diagnose read owner is backend-neutral selected-store surface"},
		{Name: "Entities", Classification: "wired_both", Reason: "entity.get/list/aggregate read owner is backend-neutral selected-store surface"},
		{Name: "AgentConversations", Classification: "wired_both", Reason: "agent and conversation read owner was promoted by #1782/#1805"},
		{Name: "Observability", Classification: "wired_both", Reason: "event/runtime log/run trace read owner is backend-neutral selected-store surface"},
		{Name: "RunBundleContext", Classification: "wired_both", Reason: "served event.publish follow-up read context is required on both selected stores"},
		{Name: "TestSetup", Classification: "wired_both", Reason: "test.setup_entities capability is selected through entity owner and remains mutating-ledger classified separately"},
		{Name: "BundleCatalog", Classification: "wired_both", Reason: "bundle.list/get/agents read owner was promoted by #1782/#1805"},
		{Name: "BundleDelete", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.bundle_delete", RequiresPostgresBaseline: true, Reason: "bundle.delete is a spec-classified Postgres-only mutating/destructive bundle lifecycle capability, not operator-read parity"},
		{Name: "ConversationForks", Classification: "wired_both", Reason: "conversation.fork_list/view consume the shared fork semantic owner on SQLite and Postgres"},
		{Name: "ConversationForkLifecycle", Classification: "wired_both", Reason: "conversation.fork/fork_chat/fork_delete consume the shared fork semantic owner with backend-local mutation adapters"},
		{Name: "RunForkAvailability", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork", RequiresPostgresBaseline: true, Reason: "run.fork availability is a spec-classified Postgres-only product/mutating lifecycle seam"},
		{Name: "RunFork", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork", RequiresPostgresBaseline: true, Reason: "run.fork execution is a spec-classified Postgres-only product/mutating lifecycle seam"},
		{Name: "RuntimeContexts", Classification: "different_semantic_concept_with_proof", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.selected_runtime_store_facade", Reason: "multi-bundle DB-loaded runtime context routing is conditional product/runtime support, not core operator-read parity"},
		{Name: "ResetCoordinator", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.runtime_nuke", RequiresPostgresBaseline: true, Reason: "destructive reset coordinator is a spec-classified Postgres-only product capability"},
		{Name: "ResetQuiescer", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.runtime_nuke", RequiresPostgresBaseline: true, Reason: "destructive reset quiescer is a spec-classified Postgres-only product capability"},
		{Name: "ResetCleaner", Classification: "postgres_only_with_spec_ref", SpecRef: "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.runtime_nuke", RequiresPostgresBaseline: true, Reason: "destructive reset cleaner is a spec-classified Postgres-only product capability"},
	}
}

func selectedAPICapabilityFieldNames() []string {
	typ := reflect.TypeOf(selectedAPICapabilities{})
	out := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		out = append(out, typ.Field(i).Name)
	}
	sort.Strings(out)
	return out
}

func selectedAPICapabilityField(caps selectedAPICapabilities, name string) (reflect.Value, bool) {
	value := reflect.ValueOf(caps).FieldByName(name)
	if !value.IsValid() {
		return reflect.Value{}, false
	}
	return value, true
}

func selectedAPICapabilityConfigured(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return !value.IsZero()
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
			SelfCheck:              true,
			WorkflowModule:         stubWorkflowModule{source: semanticview.Wrap(bundle)},
			LLMRuntime:             storeBackendSelectionNoopLLMRuntime{},
			ProviderTriggerCatalog: testProviderTriggerCatalog(t),
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
	RepoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot, root, runtimecontracts.DefaultPlatformSpecFile(RepoRoot))
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
	configText := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}

func writeStoreBackendRuntimeConfigWithoutPasswordSource(t *testing.T, backend string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	contents := withTestProviderTriggerPlatformInventory(t, strings.Join([]string{
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
	}, "\n")+"\n")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}

func unsetStoreSelectorEnv(t *testing.T) {
	t.Helper()
	unsetEnvForTest(t, "SWARM_STORE_BACKEND")
	unsetEnvForTest(t, "SWARM_SQLITE_PATH")
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
