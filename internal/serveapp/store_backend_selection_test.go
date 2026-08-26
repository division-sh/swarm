package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
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
			buildStoresForServe = func(_ context.Context, selection storebackend.Selection, _ *config.Config) (*selectedStoreOwner, error) {
				captured = selection
				return nil, errors.New("stop after selector proof")
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
	buildStoresForServe = func(_ context.Context, selection storebackend.Selection, _ *config.Config) (*selectedStoreOwner, error) {
		captured = selection
		return nil, errors.New("stop after selector proof")
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
	stores := openSelectedSQLiteOwner(t, path, &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	runtimeDeps := stores.RuntimeDeps()
	if stores.Schema() == nil || stores.Pinger() == nil || stores.StartupOwnership() == nil ||
		stores.OperatorChannels() == nil || stores.MailboxAPI() == nil || stores.Observability() == nil ||
		stores.AgentUsage() == nil || stores.AgentDeliveryLifecycle() == nil || stores.Idempotency() == nil ||
		stores.Runs() == nil || stores.Entities() == nil || stores.Agents() == nil || stores.Conversations() == nil ||
		runtimeDeps.EventStore == nil || !runtimeDeps.WorkflowPersistence.Valid() || runtimeDeps.SessionRegistry == nil ||
		runtimeDeps.ConversationStore == nil || runtimeDeps.ManagerStore == nil || runtimeDeps.GenericScheduleStore == nil ||
		runtimeDeps.MailboxMaterializer == nil || runtimeDeps.MailboxStore == nil || runtimeDeps.BudgetSpendStore == nil ||
		runtimeDeps.InboundStore == nil || runtimeDeps.RuntimeIngressStore == nil {
		t.Fatal("SQLite selected owner has an incomplete required projection")
	}
	apiCaps, err := buildSelectedAPICapabilities(stores, selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("sqlite apiCapabilities: %v", err)
	}
	if apiCaps.Agents == nil || apiCaps.Conversations == nil {
		t.Fatal("sqlite apiCapabilities missing exact agent/conversation read owners")
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
	}
	for name, capability := range classifiedOut {
		if capability != nil {
			t.Fatalf("sqlite optional capability %s = %T, want nil classified split/postgres-only capability", name, capability)
		}
	}
	if apiCaps.RuntimeContexts != nil {
		t.Fatalf("sqlite optional capability RuntimeContexts = %T, want nil classified split/postgres-only capability", apiCaps.RuntimeContexts)
	}
	if _, ok := reflect.TypeOf(runtimeDeps).FieldByName("SQLDB"); ok {
		t.Fatal("sqlite RuntimeDeps exposes raw SQLDB field")
	}
	if runtimeDeps.RuntimeLogStore == nil {
		t.Fatal("sqlite runtimeDeps RuntimeLogStore missing backend-neutral runtime diagnostics owner")
	}
	if runtimeDeps.MailboxMaterializer == nil {
		t.Fatal("sqlite runtimeDeps MailboxMaterializer missing backend-neutral mailbox_write owner")
	}
	if runtimeDeps.BudgetSpendStore == nil {
		t.Fatal("sqlite runtimeDeps BudgetSpendStore missing backend-neutral budget/spend owner")
	}
	if runtimeDeps.InboundStore == nil {
		t.Fatal("sqlite runtimeDeps InboundStore missing backend-neutral inbound webhook owner")
	}
	if runtimeDeps.ToolEntityStore == nil {
		t.Fatal("sqlite runtimeDeps ToolEntityStore missing backend-neutral entity tool owner")
	}
	if runtimeDeps.HumanTaskStore == nil {
		t.Fatal("sqlite runtimeDeps HumanTaskStore missing backend-neutral human-task owner")
	}
	if !runtimeDeps.WorkflowPersistence.Valid() {
		t.Fatal("sqlite runtimeDeps WorkflowPersistence missing backend-neutral pipeline owner")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", path, err)
	}
}

func TestBuildStoresSQLiteBindsOwnershipToConstructedBackendIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	stores := openSelectedSQLiteOwner(t, path, &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	if _, err := initializeServePlatformStateStores(ctx, stores.Schema(), filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath)); err != nil {
		t.Fatalf("bootstrap SQLite selected store: %v", err)
	}

	retiredPath := path + ".retired"
	if err := os.Rename(path, retiredPath); err != nil {
		t.Fatalf("replace constructed SQLite identity: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create replacement SQLite identity: %v", err)
	}
	contender, _, contenderErr := storeconstruction.OpenSQLiteRuntimeWithOwnershipBinding(retiredPath)
	if contender != nil {
		_ = contender.Close()
		t.Fatal("renamed constructed SQLite backend admitted a concurrent process constructor")
	}
	var contenderAcquisitionErr *runtimestartupownership.AcquisitionError
	if !errors.As(contenderErr, &contenderAcquisitionErr) || contenderAcquisitionErr.Failure != runtimestartupownership.AcquisitionTakeoverRequired {
		t.Fatalf("renamed backend contender error=%v, want takeover_required", contenderErr)
	}

	capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "backend-identity-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if capability != nil {
		_ = capability.Release(ctx)
		t.Fatal("SQLite acquisition returned a capability for a different backend inode")
	}
	requireSQLiteBackendIdentityRefusal(t, err)

	var authorityRows int
	if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&authorityRows); err != nil {
		t.Fatalf("read constructed SQLite authority rows: %v", err)
	}
	if authorityRows != 0 {
		t.Fatalf("constructed SQLite backend gained %d authority rows after identity replacement", authorityRows)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("replacement SQLite identity was mutated: info=%#v err=%v", info, err)
	}
}

func requireSQLiteBackendIdentityRefusal(t testing.TB, err error) {
	t.Helper()
	var possessionErr *runtimestartupownership.PossessionError
	if !errors.As(err, &possessionErr) || possessionErr.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("SQLite backend identity error=%v, want ownership_unprovable", err)
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
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	runBundleContext := stores.RunBundleContext()
	apiCaps, err := buildSelectedAPICapabilities(stores, selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("build API capabilities: %v", err)
	}
	if runBundleContext == nil || apiCaps.RunBundleContext != runBundleContext {
		t.Fatalf("selected API run bundle context = %T, want selected owner %T", apiCaps.RunBundleContext, runBundleContext)
	}
}

func TestSelectedOwnerAPICapabilityMatrixIsExplicitAcrossBackends(t *testing.T) {
	ctx := context.Background()
	sqlite := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "dev.db"), &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, sqlite) })
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	postgres := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, postgres) })

	sqliteCaps, err := buildSelectedAPICapabilities(sqlite, selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("build SQLite API capabilities: %v", err)
	}
	postgresCaps, err := buildSelectedAPICapabilities(postgres, selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatalf("build PostgreSQL API capabilities: %v", err)
	}
	for name, available := range map[string]bool{
		"sqlite required serve ingest":       sqlite.ServeBundleIngestWriter() != nil,
		"sqlite required run availability":   sqlite.RunBundleAvailability() != nil,
		"sqlite database":                    sqliteCaps.Database != nil,
		"sqlite runs":                        sqliteCaps.Runs != nil,
		"sqlite entities":                    sqliteCaps.Entities != nil,
		"sqlite agents":                      sqliteCaps.Agents != nil,
		"sqlite conversations":               sqliteCaps.Conversations != nil,
		"sqlite observability":               sqliteCaps.Observability != nil,
		"sqlite run bundle context":          sqliteCaps.RunBundleContext != nil,
		"sqlite test setup":                  sqliteCaps.TestSetup != nil,
		"sqlite bundle catalog":              sqliteCaps.BundleCatalog != nil,
		"sqlite bundle register":             sqliteCaps.BundleRegister != nil,
		"sqlite conversation reads":          sqliteCaps.ConversationForks != nil,
		"sqlite conversation lifecycle":      sqliteCaps.ConversationForkLifecycle != nil,
		"postgres bundle register":           postgresCaps.BundleRegister != nil,
		"postgres required serve ingest":     postgres.ServeBundleIngestWriter() != nil,
		"postgres required run availability": postgres.RunBundleAvailability() != nil,
		"postgres bundle delete":             postgresCaps.BundleDelete != nil,
		"postgres run fork":                  postgresCaps.RunFork != nil,
		"postgres run fork selector":         postgresCaps.RunForkSelector != nil,
		"postgres reset":                     postgresCaps.ResetCoordinator != nil,
	} {
		if !available {
			t.Fatalf("%s capability is unavailable", name)
		}
	}
	for name, available := range map[string]bool{
		"sqlite bundle delete":     sqliteCaps.BundleDelete != nil,
		"sqlite run fork":          sqliteCaps.RunFork != nil,
		"sqlite run fork selector": sqliteCaps.RunForkSelector != nil,
		"sqlite reset":             sqliteCaps.ResetCoordinator != nil,
	} {
		if available {
			t.Fatalf("%s capability unexpectedly available", name)
		}
	}
	if err := sqlite.Pinger().Ping(ctx); err != nil {
		t.Fatalf("SQLite selected owner ping: %v", err)
	}
	if err := postgres.Pinger().Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL selected owner ping: %v", err)
	}
}

func TestBuildStoresSQLiteRuntimeNoLongerFailsClosedOnMailboxMaterializationOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dev.db")
	cfg := &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}}
	stores, err := buildStores(ctx, storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       path,
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, cfg)
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	runtimeDeps := stores.RuntimeDeps()
	if _, ok := reflect.TypeOf(runtimeDeps).FieldByName("SQLDB"); ok {
		t.Fatal("sqlite RuntimeDeps exposes raw SQLDB field")
	}
	if runtimeDeps.RuntimeLogStore == nil {
		t.Fatal("sqlite runtimeDeps RuntimeLogStore missing backend-neutral runtime diagnostics owner")
	}
	if runtimeDeps.MailboxMaterializer == nil {
		t.Fatal("sqlite runtimeDeps MailboxMaterializer missing backend-neutral mailbox_write owner")
	}
	bundle := loadStoreBackendSelectionWorkflowBundle(t)
	if _, err := initializeStateStores(ctx, stores.Schema(), bundle); err != nil {
		t.Fatalf("initializeStateStores(sqlite): %v", err)
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}
	processWorkOwner := newSupervisorTestProcessOwner(t)
	runtimeDeps.Config = cfg
	runtimeDeps.Options = runtime.RuntimeOptions{
		SelfCheck:              true,
		ProcessWorkOwner:       processWorkOwner,
		RuntimeInstanceID:      "11111111-1111-4111-8111-111111111111",
		BundleSourceFact:       mustServeTestEphemeralBundleSourceFact(bundleHash),
		WorkflowModule:         stubWorkflowModule{source: semanticview.Wrap(bundle)},
		LLMRuntime:             storeBackendSelectionNoopLLMRuntime{},
		ProviderTriggerCatalog: testProviderTriggerCatalog(t),
		ProviderCredentials:    processIngressCredentialStore{},
	}
	rt, err := runtime.NewRuntime(ctx, runtimeDeps)
	if err != nil {
		t.Fatalf("NewRuntime(sqlite): %v", err)
	}
	t.Cleanup(func() {
		if err := rt.ShutdownWithOptions(runtime.DefaultShutdownOptions()); err != nil {
			t.Errorf("shutdown sqlite runtime: %v", err)
		}
	})
	if rt.Pipeline == nil {
		t.Fatal("NewRuntime(sqlite) Pipeline = nil, want runtime construction to consume SQLite pipeline store")
	}
}

type storeBackendSelectionNoopLLMRuntime struct{ runtimellm.NoopRuntime }

func (storeBackendSelectionNoopLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
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
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		RepoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(RepoRoot),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
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
		"  execution_posture: live",
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
		"  execution_posture: live",
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
