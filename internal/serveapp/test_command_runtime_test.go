package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testsql"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestSwarmTestServedSQLiteNoLiveLLMProof(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := writeScenarioRunnerFixture(t)
	configPath := writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              configPath,
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"test",
		"--config", configPath,
		"--contracts", contractsPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "10s",
		"--poll-interval", "25ms",
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
		t.Fatalf("stdout missing success:\n%s", stdout.String())
	}
}

func TestServedParityHarnessGeneratedInputFixtureLifecycle(t *testing.T) {
	servedparity.Run(t, servedparity.MustScenario(servedparity.ScenarioGeneratedInputFixtureLifecycle), runServedGeneratedInputFixtureBackendProof)
}

func TestServedParityHarnessDerivedScenarioLifecycle(t *testing.T) {
	servedparity.Run(t, servedparity.MustScenario(servedparity.ScenarioDerivedScenarioLifecycle), runServedDerivedScenarioBackendProof)
}

func TestServeRuntimeConfiguredChannelBindingProjectsOnce(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	contractsPath := canonicalrouting.WriteNovelDerivedScenarioBundle(t)
	configPath := writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "channel-projection.sqlite"))
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	channelPackDir := filepath.Join(cliapp.RepoRoot(), "packs", "channels", "telegram")
	configured := string(rawConfig) + fmt.Sprintf(`
channels:
  packs:
    platform_dirs: [%q]
  bindings:
    ops:
      pack: provider.telegram.hitl_channel
      destination: "42"
`, channelPackDir)
	if err := os.WriteFile(configPath, []byte(configured), 0o644); err != nil {
		t.Fatal(err)
	}
	process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: configPath, ContractsPath: contractsPath,
		PlatformSpecPath: filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath),
		APIListenAddr:    "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, NoRequireBundleMatch: true,
	})
	process.waitForReadyLine()
	rt := servedTestProcessRuntime(t, process)
	if rt == nil || rt.Options.WorkflowModule == nil {
		t.Fatal("configured channel runtime is incomplete")
	}
	if _, ok := rt.Options.WorkflowModule.SemanticSource().ToolEntries()["channel.ops.deliver"]; !ok {
		t.Fatal("configured channel runtime omitted projected channel.ops.deliver tool")
	}
}

func TestScaffoldAdmittedArchetypesRunUneditedZeroCredentialSQLite(t *testing.T) {
	for _, archetype := range []string{"zero-agent-automation", "webhook-responder"} {
		archetype := archetype
		t.Run(archetype, func(t *testing.T) {
			runScaffoldArchetypeSQLiteProof(t, archetype)
		})
	}
}

func TestServedParityHarnessPublicMockApprovalLifecycle(t *testing.T) {
	servedparity.Run(t, servedparity.MustScenario(servedparity.ScenarioPublicMockApprovalLifecycle), runServedPublicMockApprovalBackendProof)
}

func TestPublicTemplateInputPublicationRollbackIsAtomicAcrossSelectedStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			endpoint, db, bundleHash := startPublicInputRollbackRuntime(t, backend)
			claim := testsql.EventCorruptionClaim{
				Invariant: "public_input.selected_store_atomicity",
				Reason:    "prove public template lifecycle and publication facts roll back on late delivery failure",
			}
			if backend == servedparity.BackendExplicitPostgres {
				testsql.InstallPostgresEventDeliveryFailureAfterFlowMaterialization(t, context.Background(), db, claim, "telegram-chat")
			} else {
				testsql.InstallSQLiteEventDeliveryFailureAfterFlowMaterialization(t, context.Background(), db, claim, "telegram-chat")
			}

			idempotencyKey := "public-input-rollback-" + string(backend)
			rpcErr := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
				"bundle_hash": bundleHash,
				"event_name":  "inbound.telegram.text_message",
				"payload": map[string]any{
					"conversation_reference": "chat-42", "external_account_reference": "account-42",
					"provider_message_reference": 1, "text": "rollback proof",
				},
				"idempotency_key": idempotencyKey,
			})
			if code, _ := rpcErr.Data["code"].(string); code != apiv1.EventPublishFailedCode {
				t.Fatalf("%s event.publish error = %#v, want %s", backend, rpcErr, apiv1.EventPublishFailedCode)
			}
			eventID, runID := publicInputFailureIdentity(t, rpcErr)
			requirePublicInputRollbackNoResidue(t, db, backend, idempotencyKey, eventID, runID)
		})
	}
}

func TestPublicTemplateInputPublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores(t *testing.T) {
	stages := []struct {
		name    string
		install func(testing.TB, context.Context, *sql.DB, testsql.EventCorruptionClaim, string, servedparity.Backend)
	}{
		{name: "replay_scope", install: func(t testing.TB, ctx context.Context, db *sql.DB, claim testsql.EventCorruptionClaim, flow string, backend servedparity.Backend) {
			if backend == servedparity.BackendExplicitPostgres {
				testsql.InstallPostgresReplayScopeFailureAfterDelivery(t, ctx, db, claim, flow)
			} else {
				testsql.InstallSQLiteReplayScopeFailureAfterDelivery(t, ctx, db, claim, flow)
			}
		}},
		{name: "api_completion", install: func(t testing.TB, ctx context.Context, db *sql.DB, claim testsql.EventCorruptionClaim, flow string, backend servedparity.Backend) {
			if backend == servedparity.BackendExplicitPostgres {
				testsql.InstallPostgresAPICompletionFailureAfterPublication(t, ctx, db, claim, flow)
			} else {
				testsql.InstallSQLiteAPICompletionFailureAfterPublication(t, ctx, db, claim, flow)
			}
		}},
	}
	for _, backend := range servedparity.RequiredBackends {
		for _, stage := range stages {
			backend, stage := backend, stage
			t.Run(string(backend)+"/"+stage.name, func(t *testing.T) {
				endpoint, db, bundleHash := startPublicInputRollbackRuntime(t, backend)
				claim := testsql.EventCorruptionClaim{Invariant: "public_input.selected_store_atomicity", Reason: "prove rollback at " + stage.name + " after prior publication facts exist"}
				stage.install(t, context.Background(), db, claim, "telegram-chat", backend)
				idempotencyKey := "public-input-" + stage.name + "-" + string(backend)
				rpcErr := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
					"bundle_hash": bundleHash, "event_name": "inbound.telegram.text_message",
					"payload":         map[string]any{"conversation_reference": "chat-42", "external_account_reference": "account-42", "provider_message_reference": 1, "text": "late rollback proof"},
					"idempotency_key": idempotencyKey,
				})
				if code, _ := rpcErr.Data["code"].(string); code != apiv1.EventPublishFailedCode {
					t.Fatalf("%s/%s event.publish error = %#v, want %s", backend, stage.name, rpcErr, apiv1.EventPublishFailedCode)
				}
				eventID, runID := publicInputFailureIdentity(t, rpcErr)
				requirePublicInputRollbackNoResidue(t, db, backend, idempotencyKey, eventID, runID)
			})
		}
	}
}

func startPublicInputRollbackRuntime(t *testing.T, backend servedparity.Backend) (string, *sql.DB, string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	t.Setenv("PATH", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	contractsPath := writePublicTelegramMockApprovalScenarioFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	var db *sql.DB
	opts := cliapp.ServeOptions{
		ContractsPath: contractsPath, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, NoRequireBundleMatch: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		stubServeRuntimeWorkspaceLifecycle(t)
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "public-input-rollback.sqlite"))
	case servedparity.BackendExplicitPostgres:
		_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		opts.StoreMode = storebackend.BackendPostgres.String()
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, opts)
	if db == nil {
		t.Fatalf("%s selected database is required", backend)
	}
	return endpoint, db, bundleHash
}

func publicInputFailureIdentity(t *testing.T, rpcErr *servedJSONRPCError) (string, string) {
	t.Helper()
	details, ok := rpcErr.Data["details"].(map[string]any)
	if !ok {
		t.Fatalf("event.publish failure has no details: %#v", rpcErr)
	}
	eventID, _ := details["event_id"].(string)
	runID, _ := details["run_id"].(string)
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(runID) == "" {
		t.Fatalf("event.publish failure identity = event:%q run:%q", eventID, runID)
	}
	return eventID, runID
}

func requirePublicInputRollbackNoResidue(t *testing.T, db *sql.DB, backend servedparity.Backend, idempotencyKey, eventID, runID string) {
	t.Helper()
	queries := []struct {
		label string
		sql   string
		args  []any
	}{
		{label: "run", sql: `SELECT COUNT(*) FROM runs WHERE run_id = ?`, args: []any{runID}},
		{label: "flow instance", sql: `SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`},
		{label: "entity", sql: `SELECT COUNT(*) FROM entity_state WHERE run_id = ?`, args: []any{runID}},
		{label: "route", sql: `SELECT COUNT(*) FROM routing_rules WHERE flow_instance LIKE 'telegram-chat/%'`},
		{label: "event", sql: `SELECT COUNT(*) FROM events WHERE event_id = ?`, args: []any{eventID}},
		{label: "delivery", sql: `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, args: []any{eventID}},
		{label: "replay scope", sql: `SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id = ?`, args: []any{eventID}},
	}
	for _, query := range queries {
		var count int
		statement := query.sql
		if backend == servedparity.BackendExplicitPostgres && len(query.args) > 0 {
			statement = strings.Replace(statement, "?", "$1::uuid", 1)
		}
		if err := db.QueryRowContext(context.Background(), statement, query.args...).Scan(&count); err != nil {
			t.Fatalf("%s count %s rollback rows: %v", backend, query.label, err)
		}
		if count != 0 {
			t.Fatalf("%s %s rows after public-input rollback = %d, want 0", backend, query.label, count)
		}
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, servedBackendLabel(backend), "event.publish", idempotencyKey); got != 0 {
		t.Fatalf("%s API idempotency completion rows after public-input rollback = %d, want 0", backend, got)
	}
}

func runServedPublicMockApprovalBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	t.Setenv("PATH", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	contractsPath := writePublicTelegramMockApprovalScenarioFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)

	var db *sql.DB
	var configPath string
	opts := cliapp.ServeOptions{
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), "public-mock-approval.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		configPath = writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath)
	case servedparity.BackendExplicitPostgres:
		_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
			return serveRuntimeWorkspaceStub{}
		})
		configPath = writeMockAgentRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		opts.StoreMode = storebackend.BackendPostgres.String()
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	opts.ConfigPath = configPath
	var verifyOut, verifyErr bytes.Buffer
	if code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"verify", "--contracts", contractsPath, "--config", configPath,
	}, &verifyOut, &verifyErr, nil); code != 0 {
		t.Fatalf("%s verify code = %d stderr=%s stdout=%s", backend, code, verifyErr.String(), verifyOut.String())
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, opts)
	requireSchemaOnlyProviderTriggerHasNoWebhookRoute(t, strings.TrimSuffix(endpoint, "/v1/rpc"))

	var stdout, stderr bytes.Buffer
	started := time.Now()
	scenario := filepath.ToSlash(filepath.Join("flows", "telegram-chat", "tests", "public-mock-approval.yaml"))
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--config", configPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "45s",
		"--poll-interval", "25ms",
		scenario,
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("%s code = %d stderr=%s stdout=%s", backend, code, stderr.String(), stdout.String())
	}
	if elapsed := time.Since(started); elapsed >= 60*time.Second {
		t.Fatalf("%s public mock approval journey took %s, want under 60s", backend, elapsed)
	}
	if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("%s stdout=%q stderr=%q", backend, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(credentialPath); !os.IsNotExist(err) {
		t.Fatalf("%s zero-credential journey created credential store %s: %v", backend, credentialPath, err)
	}

	runID := requireSingleServedBundleRun(t, endpoint, bundleHash)
	requirePublicMockApprovalEvents(t, endpoint, runID)
	requireServedParitySettlementPostconditions(t, endpoint, db, servedBackendLabel(backend), runID, servedparity.MustScenario(servedparity.ScenarioPublicMockApprovalLifecycle))
}

func requireSchemaOnlyProviderTriggerHasNoWebhookRoute(t *testing.T, serverURL string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(serverURL, "/")+"/webhooks/chat/telegram", strings.NewReader(`{"update_id":1}`))
	if err != nil {
		t.Fatalf("build schema-only webhook probe: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("schema-only webhook probe: %v", err)
	}
	defer resp.Body.Close()
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read schema-only webhook response: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(body.String(), "no ingress target") || !strings.Contains(body.String(), "add ingress") {
		t.Fatalf("schema-only webhook response = %d %q, want teaching 404 with no route activation", resp.StatusCode, body.String())
	}
}

func runServedDerivedScenarioBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	t.Setenv("PATH", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	contractsPath := canonicalrouting.WriteNovelDerivedScenarioBundle(t)

	var db *sql.DB
	var configPath string
	var postgresDSN string
	platformSpecPath := filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath)
	opts := cliapp.ServeOptions{
		ContractsPath: contractsPath, PlatformSpecPath: platformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, NoRequireBundleMatch: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		stubServeRuntimeWorkspaceLifecycle(t)
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		configPath = writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "derived-scenario.sqlite"))
	case servedparity.BackendExplicitPostgres:
		postgresDSN, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		configPath = writeMockAgentRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		opts.StoreMode = storebackend.BackendPostgres.String()
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	opts.ConfigPath = configPath
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	rt := servedTestProcessRuntime(t, process)
	if db == nil || rt == nil || rt.Options.WorkflowModule == nil {
		t.Fatalf("%s derived scenario runtime is incomplete", backend)
	}

	plans, err := scenarioderivation.Compile(rt.Options.WorkflowModule.SemanticSource(), rt.EffectiveSourceIdentity, scenarioderivation.Request{FlowID: "fulfillment", Input: "request"})
	if err != nil || len(plans) != 1 {
		t.Fatalf("%s compile negative selector plan: plans=%d err=%v", backend, len(plans), err)
	}
	selector, err := scenarioexecution.NewSelector(plans[0].Profile)
	if err != nil {
		t.Fatal(err)
	}
	selector.EffectiveSourceDigest = "sha256:" + strings.Repeat("0", 64)
	bundleHash, err := runtimecontracts.BundleHash(mustBundleFromSource(t, rt.Options.WorkflowModule.SemanticSource()))
	if err != nil {
		t.Fatal(err)
	}
	beforeMismatch := loadScenarioMutationCounts(t, db, backend)
	rpcErr := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
		"bundle_hash": bundleHash, "event_name": "fulfillment.requested",
		"payload":         map[string]any{"order_id": "must-not-publish"},
		"idempotency_key": "derived-effective-source-mismatch-" + string(backend),
		"scenario_execution": map[string]any{
			"profile_id": selector.ProfileID, "profile_digest": selector.ProfileDigest,
			"effective_source_digest": selector.EffectiveSourceDigest,
		},
	})
	if code, _ := rpcErr.Data["code"].(string); code != apiv1.BundleMismatchCode {
		t.Fatalf("%s mismatched effective source error = %#v", backend, rpcErr)
	}
	if afterPublish := loadScenarioMutationCounts(t, db, backend); afterPublish != beforeMismatch {
		t.Fatalf("%s mismatched event.publish mutated state: before=%+v after=%+v", backend, beforeMismatch, afterPublish)
	}
	setupRunID := "00000000-0000-4000-8000-000000000201"
	setupEntityID := "00000000-0000-4000-8000-000000000202"
	rpcErr = requireServedJSONRPCError(t, endpoint, "test.setup_entities", map[string]any{
		"bundle_hash": bundleHash, "run_id": setupRunID,
		"idempotency_key": "derived-setup-effective-source-mismatch-" + string(backend),
		"entities": []any{map[string]any{
			"alias": "subject", "entity_id": setupEntityID, "flow_instance": "fulfillment",
			"entity_type": "subject", "current_state": "ready", "fields": map[string]any{},
		}},
		"scenario_execution": map[string]any{
			"profile_id": selector.ProfileID, "profile_digest": selector.ProfileDigest,
			"effective_source_digest": selector.EffectiveSourceDigest,
		},
	})
	if code, _ := rpcErr.Data["code"].(string); code != apiv1.BundleMismatchCode {
		t.Fatalf("%s mismatched setup effective source error = %#v", backend, rpcErr)
	}
	if afterSetup := loadScenarioMutationCounts(t, db, backend); afterSetup != beforeMismatch {
		t.Fatalf("%s mismatched test.setup_entities mutated state: before=%+v after=%+v", backend, beforeMismatch, afterSetup)
	}

	var stdout, stderr bytes.Buffer
	started := time.Now()
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"test", "--derive", "fulfillment", "--input", "request",
		"--contracts", contractsPath, "--config", configPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "20s", "--poll-interval", "25ms",
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("%s derived scenario code=%d stdout=%s stderr=%s", backend, code, stdout.String(), stderr.String())
	}
	if elapsed := time.Since(started); elapsed >= 60*time.Second {
		t.Fatalf("%s derived scenario took %s, want under 60s", backend, elapsed)
	}
	if !strings.Contains(stdout.String(), "scenario ok: derived:fulfillment/request") || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("%s derived output stdout=%q stderr=%q", backend, stdout.String(), stderr.String())
	}
	requireExactScenarioExecutionProfile(t, db, backend, rt.EffectiveSourceIdentity)
	beforeRestart := loadExactScenarioExecutionProfileRecord(t, db, backend)
	if _, err := os.Stat(credentialPath); !os.IsNotExist(err) {
		t.Fatalf("%s zero-credential journey created credential store %s: %v", backend, credentialPath, err)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("%s first derived runtime stop code = %d\n%s", backend, code, process.outputString())
	}
	if backend == servedparity.BackendExplicitPostgres {
		reopened, err := store.NewPostgresStore(postgresDSN)
		if err != nil {
			t.Fatalf("reopen PostgreSQL derived runtime store: %v", err)
		}
		t.Cleanup(func() { _ = storetest.DatabaseForTest(reopened).Close() })
		priorBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			storetest.BootstrapPostgresRuntimeStore(t, reopened)
			return selectedPostgresStoreBundle(reopened, storetest.DatabaseForTest(reopened), cfg), nil
		}
		t.Cleanup(func() { buildStoresForServe = priorBuildStores })
		db = storetest.DatabaseForTest(reopened)
	}
	restarted := startServeRuntimeTestProcess(t, opts)
	restarted.waitForReadyLine()
	restartedEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, restarted.outputString()) + "/v1/rpc"
	restartedRuntime := servedTestProcessRuntime(t, restarted)
	if !restartedRuntime.EffectiveSourceIdentity.Equal(rt.EffectiveSourceIdentity) {
		t.Fatalf("%s restart effective identity changed: before=%s after=%s", backend, rt.EffectiveSourceIdentity.Digest(), restartedRuntime.EffectiveSourceIdentity.Digest())
	}
	afterRestart := loadExactScenarioExecutionProfileRecord(t, db, backend)
	if beforeRestart.runID != afterRestart.runID || beforeRestart.profileID != afterRestart.profileID || beforeRestart.profileDigest != afterRestart.profileDigest || beforeRestart.effectiveDigest != afterRestart.effectiveDigest || !bytes.Equal(beforeRestart.raw, afterRestart.raw) {
		t.Fatalf("%s restart changed exact scenario execution profile", backend)
	}
	var resumed map[string]any
	requireServedJSONRPCResult(t, restartedEndpoint, "run.get", map[string]any{"run_id": beforeRestart.runID}, &resumed)
	run, _ := resumed["run"].(map[string]any)
	if run["run_id"] != beforeRestart.runID {
		t.Fatalf("%s restarted run readback = %#v, want original run", backend, resumed)
	}
	finalRecord := loadExactScenarioExecutionProfileRecord(t, db, backend)
	if !bytes.Equal(beforeRestart.raw, finalRecord.raw) || beforeRestart.profileDigest != finalRecord.profileDigest || scenarioExecutionProfileCount(t, db, backend) != 1 {
		t.Fatalf("%s restarted execution changed or duplicated durable scenario profile", backend)
	}
}

func runScaffoldArchetypeSQLiteProof(t *testing.T, archetype string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	t.Setenv("PATH", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	scaffoldRoot := scaffoldConformanceArchetype(t, archetype)
	contractsPath := scaffoldRoot
	if archetype == "webhook-responder" {
		contractsPath = filepath.Join(scaffoldRoot, "bot")
	}
	configPath := filepath.Join(contractsPath, "swarm.yaml")

	var verifyOut, verifyErr bytes.Buffer
	if code := cliapp.Execute(context.Background(), contractsPath, []string{"verify", "--config", configPath, "--contracts", contractsPath}, &verifyOut, &verifyErr, nil); code != 0 {
		t.Fatalf("%s generated verify code=%d stdout=%s stderr=%s", archetype, code, verifyOut.String(), verifyErr.String())
	}
	oldBuildStores := buildStoresForServe
	var db *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			db = stores.SQLDB
		}
		return stores, err
	}
	t.Cleanup(func() { buildStoresForServe = oldBuildStores })
	endpoint, rt := startServedEventPublishFollowUpRuntimeAtRepo(t, contractsPath, cliapp.ServeOptions{
		ConfigPath: configPath, ContractsPath: contractsPath, PlatformSpecPath: filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath),
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, NoRequireBundleMatch: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	if db == nil || rt == nil {
		t.Fatalf("%s generated runtime is incomplete", archetype)
	}
	var stdout, stderr bytes.Buffer
	started := time.Now()
	code := cliapp.Execute(context.Background(), contractsPath, []string{
		"test", "--config", configPath, "--contracts", contractsPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "30s", "--poll-interval", "25ms", filepath.Join("tests", "smoke.yaml"),
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("%s generated scenario code=%d stdout=%s stderr=%s", archetype, code, stdout.String(), stderr.String())
	}
	if elapsed := time.Since(started); elapsed >= 60*time.Second {
		t.Fatalf("%s generated journey took %s, want under 60s", archetype, elapsed)
	}
	requireExactScenarioExecutionProfile(t, db, servedparity.BackendDefaultSQLite, rt.EffectiveSourceIdentity)
	if _, err := os.Stat(credentialPath); !os.IsNotExist(err) {
		t.Fatalf("%s generated journey created credential store %s: %v", archetype, credentialPath, err)
	}
}

func scaffoldConformanceArchetype(t *testing.T, archetype string) string {
	t.Helper()
	destination := filepath.Join(t.TempDir(), archetype)
	var stdout, stderr bytes.Buffer
	if code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"new", archetype, "--output", destination}, &stdout, &stderr, nil); code != 0 {
		t.Fatalf("scaffold %s code=%d stdout=%s stderr=%s", archetype, code, stdout.String(), stderr.String())
	}
	return destination
}

func scenarioExecutionProfileCount(t *testing.T, db *sql.DB, backend servedparity.Backend) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM run_scenario_execution_profiles`).Scan(&count); err != nil {
		t.Fatalf("%s count scenario execution profiles: %v", backend, err)
	}
	return count
}

type scenarioMutationCounts struct {
	Runs, Events, EntityState, Profiles int
}

func loadScenarioMutationCounts(t *testing.T, db *sql.DB, backend servedparity.Backend) scenarioMutationCounts {
	t.Helper()
	var counts scenarioMutationCounts
	queries := []struct {
		name string
		out  *int
	}{
		{name: "runs", out: &counts.Runs},
		{name: "events", out: &counts.Events},
		{name: "entity_state", out: &counts.EntityState},
		{name: "run_scenario_execution_profiles", out: &counts.Profiles},
	}
	for _, query := range queries {
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+query.name).Scan(query.out); err != nil {
			t.Fatalf("%s count %s: %v", backend, query.name, err)
		}
	}
	return counts
}

func requireExactScenarioExecutionProfile(t *testing.T, db *sql.DB, backend servedparity.Backend, identity scenarioexecution.EffectiveSourceIdentity) {
	t.Helper()
	record := loadExactScenarioExecutionProfileRecord(t, db, backend)
	profile, err := scenarioexecution.DecodeProfile(record.raw, record.profileDigest)
	if err != nil {
		t.Fatalf("%s decode exact scenario profile: %v", backend, err)
	}
	if profile.ID() != record.profileID || record.effectiveDigest != identity.Digest() || !profile.EffectiveSourceIdentity().Equal(identity) {
		t.Fatalf("%s persisted profile identity mismatch: id=%s digest=%s effective=%s", backend, record.profileID, record.profileDigest, record.effectiveDigest)
	}
}

type scenarioExecutionProfileRecord struct {
	runID, profileID, profileDigest, effectiveDigest string
	raw                                              []byte
}

func loadExactScenarioExecutionProfileRecord(t *testing.T, db *sql.DB, backend servedparity.Backend) scenarioExecutionProfileRecord {
	t.Helper()
	var record scenarioExecutionProfileRecord
	if err := db.QueryRowContext(context.Background(), `SELECT run_id, profile_id, profile_digest, effective_source_digest, profile_bytes FROM run_scenario_execution_profiles`).Scan(&record.runID, &record.profileID, &record.profileDigest, &record.effectiveDigest, &record.raw); err != nil {
		t.Fatalf("%s load scenario execution profile: %v", backend, err)
	}
	return record
}

func servedTestProcessRuntime(t *testing.T, process *serveRuntimeTestProcess) *runtimepkg.Runtime {
	t.Helper()
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.runtime == nil {
		t.Fatal("served test process runtime is not ready")
	}
	return process.runtime
}

func mustBundleFromSource(t *testing.T, source semanticview.Source) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("effective source is not bundle-backed")
	}
	return bundle
}

func startServedEventPublishFollowUpRuntimeAtRepo(t *testing.T, repoRoot string, opts cliapp.ServeOptions) (string, *runtimepkg.Runtime) {
	t.Helper()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	var out lockedBuffer
	done := make(chan int, 1)
	runtimeReady := make(chan *runtimepkg.Runtime, 1)
	priorRuntimeReadyHook := opts.TestRuntimeReadyHook
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
		if priorRuntimeReadyHook != nil {
			priorRuntimeReadyHook(rt)
		}
		select {
		case runtimeReady <- rt:
		default:
		}
	}
	opts.Output = &out
	go func() { done <- Run(serveCtx, repoRoot, opts) }()
	waitForServeReadyLine(t, &out, done)
	var rt *runtimepkg.Runtime
	select {
	case rt = <-runtimeReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for generated serve runtime\noutput:\n%s", out.String())
	}
	t.Cleanup(func() {
		cancelServe()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("generated Run exit code = %d\noutput:\n%s", code, out.String())
			}
		case <-time.After(servedProofPollDeadline):
			t.Errorf("timed out stopping generated Run\noutput:\n%s", out.String())
		}
	})
	return "http://" + serveRuntimeAPIListenerFromOutput(t, out.String()) + "/v1/rpc", rt
}

func runServedGeneratedInputFixtureBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	configureStandingLifecycleCredentials(t)
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		http.Error(w, "generated input proof must not call provider transport", http.StatusInternalServerError)
	}))
	t.Cleanup(provider.Close)
	contractsPath := writeGeneratedTelegramScenarioFixture(t, provider.URL)

	var db *sql.DB
	var configPath string
	var runtimeContexts *runtimepkg.RuntimeContextManager
	opts := cliapp.ServeOptions{
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestRuntimeContextsReadyHook: func(manager *runtimepkg.RuntimeContextManager) {
			runtimeContexts = manager
		},
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), "generated-input.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		configPath = writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath)
	case servedparity.BackendExplicitPostgres:
		_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
			return serveRuntimeWorkspaceStub{}
		})
		configPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode = storebackend.BackendPostgres.String()
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	opts.ConfigPath = configPath
	endpoint, rt := startServedEventPublishFollowUpRuntime(t, opts)
	if db == nil {
		t.Fatalf("%s served database is required", backend)
	}
	if rt == nil || rt.Options.WorkflowModule == nil {
		t.Fatalf("%s admitted runtime workflow module is required", backend)
	}
	runtimeSource := rt.Options.WorkflowModule.SemanticSource()
	runtimeGeneration := requireProviderTriggerEventSource(t, runtimeSource, "inbound.telegram.text_message")
	authoredBundle, ok := semanticview.Bundle(runtimeSource)
	if !ok || authoredBundle == nil {
		t.Fatalf("%s effective runtime source is not bundle-backed", backend)
	}
	if _, declared := authoredBundle.EventEntry("inbound.telegram.text_message"); declared {
		t.Fatalf("%s authored bundle unexpectedly owns imported Telegram event", backend)
	}
	if runtimeContexts == nil {
		t.Fatalf("%s runtime context manager was not published", backend)
	}
	loadedContexts := runtimeContexts.LoadedContexts()
	if len(loadedContexts) != 1 {
		t.Fatalf("%s loaded runtime contexts = %d, want 1", backend, len(loadedContexts))
	}
	contextGeneration := requireProviderTriggerEventSource(t, loadedContexts[0].Source, "inbound.telegram.text_message")
	if !contextGeneration.Equal(runtimeGeneration) {
		t.Fatalf("%s runtime/context provider-trigger generations differ: runtime=%s context=%s", backend, runtimeGeneration.Diagnostic(), contextGeneration.Diagnostic())
	}

	var stdout, stderr bytes.Buffer
	scenario := filepath.ToSlash(filepath.Join("flows", "telegram-chat", "tests", "generated-input.yaml"))
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--config", configPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "20s",
		"--poll-interval", "25ms",
		scenario,
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("%s code = %d stderr=%s stdout=%s", backend, code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("%s stdout=%q stderr=%q", backend, stdout.String(), stderr.String())
	}
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("%s provider transport calls = %d, want zero", backend, got)
	}

	eventID, runID, payload := loadGeneratedTelegramEvent(t, db, backend)
	wantPayload := map[string]any{
		"conversation_reference":     "0",
		"external_account_reference": "0",
		"provider_message_reference": float64(1),
		"text":                       "0",
	}
	if !reflect.DeepEqual(payload, wantPayload) {
		t.Fatalf("%s persisted event %s payload = %#v, want %#v", backend, eventID, payload, wantPayload)
	}
	requireServedParitySettlementPostconditions(t, endpoint, db, servedBackendLabel(backend), runID, servedparity.MustScenario(servedparity.ScenarioGeneratedInputFixtureLifecycle))
}

func writeGeneratedTelegramScenarioFixture(t *testing.T, providerURL string) string {
	t.Helper()
	_ = providerURL
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	for _, name := range []string{
		"bot/flows/telegram-chat/agents.yaml",
		"bot/flows/telegram-chat/events.yaml",
		"bot/flows/telegram-chat/tools.yaml",
	} {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, filepath.FromSlash(name)), "{}\n")
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "bot", "flows", "telegram-chat", "nodes.yaml"), `
telegram-input-observer:
  id: telegram-input-observer
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "bot", "flows", "telegram-chat", "tests", "generated-input.yaml"), `
name: generated Telegram normalized input
steps:
  - publish: telegram_text_message
    payload: generate
`)
	return root
}

func writePublicTelegramMockApprovalScenarioFixture(t *testing.T) string {
	t.Helper()
	exampleRoot := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	root := filepath.Join(exampleRoot, "bot")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "telegram-chat", "nodes.yaml"), `
telegram-responder:
  id: telegram-responder
  execution_type: system_node
  subscribes_to: [telegram.reply_requested]
  event_handlers:
    telegram.reply_requested:
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        approval: {decision: send_telegram_message}
        input:
          chat_id: {cel: payload.chat_id}
          text: {cel: payload.text}
telegram-revision:
  id: telegram-revision
  execution_type: system_node
  subscribes_to: [telegram_send_message.revision_requested]
  event_handlers:
    telegram_send_message.revision_requested: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "telegram-chat", "tests", "public-mock-approval.yaml"), `
name: public generated Telegram mock approval
steps:
  - publish: telegram_text_message
    payload: generate
  - mailbox.decide:
      match:
        anchor_kind: proposed_effect
        decision: send_telegram_message
        activity_id: telegram_send_message
      verdict: approve
      fields: {}
expect:
  events:
    ordered:
      - inbound.telegram.text_message
      - platform.activity_requested
      - telegram-chat/telegram_send_message.succeeded
  no_dead_letters: true
`)
	return root
}

func requireSingleServedBundleRun(t *testing.T, endpoint, bundleHash string) string {
	t.Helper()
	var result struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"bundle_hash": bundleHash, "limit": 500}, &result)
	var matches []operatorread.RunHeader
	for _, run := range result.Runs {
		if run.Origin.EventType() == "inbound.telegram.text_message" {
			matches = append(matches, run)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("run.list for bundle %s returned %d normalized-input runs, want one: %#v", bundleHash, len(matches), result.Runs)
	}
	return matches[0].RunID
}

func requirePublicMockApprovalEvents(t *testing.T, endpoint, runID string) {
	t.Helper()
	var result operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
		"filter": map[string]any{"run_id": runID},
		"limit":  500,
	}, &result)
	wantCounts := map[string]int{
		"inbound.telegram.text_message":                 1,
		"platform.activity_requested":                   1,
		"telegram-chat/telegram_send_message.succeeded": 1,
	}
	counts := make(map[string]int, len(wantCounts))
	replyRequested := 0
	for _, event := range result.Events {
		if strings.HasPrefix(event.EventName, "telegram-chat/") && strings.HasSuffix(event.EventName, "/telegram.reply_requested") {
			replyRequested++
			if event.ExecutionMode != executionmode.Mock {
				t.Fatalf("event %s execution mode = %q, want mock", event.EventName, event.ExecutionMode)
			}
			if len(event.DeadLetters) != 0 {
				t.Fatalf("event %s dead letters = %#v", event.EventName, event.DeadLetters)
			}
			continue
		}
		if _, tracked := wantCounts[event.EventName]; !tracked {
			continue
		}
		counts[event.EventName]++
		if event.ExecutionMode != executionmode.Mock {
			t.Fatalf("event %s execution mode = %q, want mock", event.EventName, event.ExecutionMode)
		}
		if len(event.DeadLetters) != 0 {
			t.Fatalf("event %s dead letters = %#v", event.EventName, event.DeadLetters)
		}
	}
	if replyRequested != 1 {
		t.Fatalf("template-scoped telegram.reply_requested count = %d, want one; events=%#v", replyRequested, result.Events)
	}
	for eventName, want := range wantCounts {
		if got := counts[eventName]; got != want {
			t.Fatalf("event.list %s count = %d, want %d; events=%#v", eventName, got, want, result.Events)
		}
	}
	for _, event := range result.Events {
		if event.EventName != "inbound.telegram.text_message" {
			continue
		}
		agentDeliveries := 0
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "agent" || !strings.HasPrefix(delivery.SubscriberID, "phrase-bot") {
				continue
			}
			agentDeliveries++
			if delivery.Target.FlowID != "telegram-chat" || !strings.HasPrefix(delivery.Target.FlowInstance, "telegram-chat/") || delivery.Target.EntityID == "" {
				t.Fatalf("public input agent delivery target = %#v", delivery.Target)
			}
		}
		if agentDeliveries != 1 {
			t.Fatalf("public input agent deliveries = %d, want one: %#v", agentDeliveries, event.Deliveries)
		}
	}
}

func loadGeneratedTelegramEvent(t *testing.T, db *sql.DB, backend servedparity.Backend) (eventID, runID string, payload map[string]any) {
	t.Helper()
	query := `SELECT event_id, run_id, payload FROM events WHERE event_name = ? ORDER BY created_at DESC LIMIT 1`
	if backend == servedparity.BackendExplicitPostgres {
		query = `SELECT event_id::text, run_id::text, payload::text FROM events WHERE event_name = $1 ORDER BY created_at DESC LIMIT 1`
	}
	var raw string
	if err := db.QueryRowContext(context.Background(), query, "inbound.telegram.text_message").Scan(&eventID, &runID, &raw); err != nil {
		t.Fatalf("%s load generated Telegram event: %v", backend, err)
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("%s decode generated Telegram payload: %v", backend, err)
	}
	return eventID, runID, payload
}

func servedBackendLabel(backend servedparity.Backend) string {
	if backend == servedparity.BackendExplicitPostgres {
		return "postgres"
	}
	return "sqlite"
}

func TestSwarmTestCanonicalRoutingExamplesRunFullAuthoredPathsOnServedSQLite(t *testing.T) {
	rootNode := func(localID string) string {
		return identitytest.RootNode(t, localID).Key()
	}
	flowNode := func(flowID, localID string) string {
		return identitytest.FlowNode(t, flowID, localID).Key()
	}
	tests := []struct {
		example        canonicalrouting.ArtifactID
		deliveredNodes map[string]int
	}{
		{canonicalrouting.RootIngress, map[string]int{rootNode("item-handler"): 1, rootNode("item-observer"): 1}},
		{canonicalrouting.ParentConnect, map[string]int{flowNode("producer", "producer-node"): 1, flowNode("consumer", "consumer-node"): 1}},
		{canonicalrouting.TemplateSelectExisting, map[string]int{flowNode("producer", "producer-node"): 2, flowNode("account", "account-node"): 2}},
		{canonicalrouting.TemplateSelectOrCreate, map[string]int{flowNode("producer", "producer-node"): 1, flowNode("account", "account-node"): 1}},
		{canonicalrouting.TemplateReply, map[string]int{flowNode("initiator", "initiator-node"): 2, flowNode("requester", "requester-node"): 3, flowNode("provider", "provider-node"): 1}},
		{canonicalrouting.TemplateCreateMintedKey, map[string]int{flowNode("producer", "producer-node"): 1, flowNode("validator", "validator-node"): 1}},
	}
	for _, test := range tests {
		t.Run(string(test.example), func(t *testing.T) {
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			contractsPath := canonicalrouting.ExampleRoot(t, test.example)
			sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
			oldBuildStores := buildStoresForServe
			t.Cleanup(func() { buildStoresForServe = oldBuildStores })
			var servedDB *sql.DB
			replyContextObserved := make(chan string, 1)
			buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
				stores, err := oldBuildStores(ctx, selection, cfg)
				if err == nil {
					servedDB = stores.SQLDB
				}
				return stores, err
			}
			configPath := writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath)
			options := cliapp.ServeOptions{
				ConfigPath:              configPath,
				ContractsPath:           contractsPath,
				PlatformSpecPath:        defaultPlatformSpecPath,
				APIListenAddr:           "127.0.0.1:0",
				MCPListenAddr:           "127.0.0.1:0",
				SelfCheck:               true,
				RequireBundleMatch:      false,
				NoRequireBundleMatch:    true,
				Verbose:                 true,
				TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
			}
			if test.example == canonicalrouting.TemplateReply {
				options.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, nodeID string, _ events.Event) error {
					if nodeID != flowNode("provider", "provider-node") {
						return nil
					}
					select {
					case replyContextObserved <- events.DeliveryContextFromContext(ctx).ReplyContextID():
					default:
					}
					return nil
				}
			}
			endpoint, _ := startServedEventPublishFollowUpRuntime(t, options)

			var stdout, stderr bytes.Buffer
			code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
				"test",
				"--config", configPath,
				"--contracts", contractsPath,
				"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
				"--timeout", "20s",
				"--poll-interval", "25ms",
			}, &stdout, &stderr, nil)
			observedReplyContext := ""
			select {
			case observedReplyContext = <-replyContextObserved:
			default:
			}
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s provider_reply_context=%q\n%s", code, stderr.String(), stdout.String(), observedReplyContext, canonicalRoutingSQLiteDebug(t, servedDB))
			}
			if servedDB == nil {
				t.Fatal("served SQLite database is required for canonical routing proof")
			}
			if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
				t.Fatalf("stdout missing supported scenario success:\n%s", stdout.String())
			}
			if test.example == canonicalrouting.TemplateReply && observedReplyContext == "" {
				t.Fatal("provider handler did not receive route-scoped reply context")
			}
			for nodeID, minimum := range test.deliveredNodes {
				var count int
				if err := servedDB.QueryRowContext(context.Background(), `
					SELECT COUNT(*)
					FROM event_deliveries
					WHERE subscriber_type = 'node' AND subscriber_id = ? AND status = 'delivered'
				`, nodeID).Scan(&count); err != nil {
					t.Fatalf("count delivered node/%s: %v", nodeID, err)
				}
				if count < minimum {
					t.Fatalf("delivered node/%s rows = %d, want at least %d\n%s", nodeID, count, minimum, canonicalRoutingSQLiteDebug(t, servedDB))
				}
			}
		})
	}
}

func canonicalRoutingSQLiteDebug(t *testing.T, db *sql.DB) string {
	t.Helper()
	if db == nil {
		return "served SQLite database unavailable"
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT e.event_name,
		       COALESCE(e.flow_instance, ''),
		       COALESCE((SELECT r.outcome FROM event_receipts r
		                 WHERE r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline'), ''),
		       COALESCE((SELECT r.reason_code || ':' || r.side_effects FROM event_receipts r
		                 WHERE r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline'), ''),
		       COALESCE((SELECT group_concat(d.subscriber_type || '/' || d.subscriber_id || '=' || d.status || '@' || d.delivery_context, ',')
		                 FROM event_deliveries d WHERE d.event_id = e.event_id), '')
		FROM events e
		WHERE e.event_name <> 'platform.runtime_log'
		ORDER BY e.created_at, e.event_id
	`)
	if err != nil {
		return "query canonical routing debug: " + err.Error()
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var eventName, flowInstance, pipelineOutcome, pipelineDetail, deliveries string
		if err := rows.Scan(&eventName, &flowInstance, &pipelineOutcome, &pipelineDetail, &deliveries); err != nil {
			return "scan canonical routing debug: " + err.Error()
		}
		lines = append(lines, fmt.Sprintf("event=%s flow=%s pipeline=%s detail=%s deliveries=%s", eventName, flowInstance, pipelineOutcome, pipelineDetail, deliveries))
	}
	if err := rows.Err(); err != nil {
		return "read canonical routing debug: " + err.Error()
	}
	deadRows, err := db.QueryContext(context.Background(), `SELECT original_event, failure FROM dead_letters ORDER BY created_at`)
	if err != nil {
		return strings.Join(lines, "\n") + "\nquery dead letters: " + err.Error()
	}
	defer deadRows.Close()
	for deadRows.Next() {
		var eventName, failure string
		if err := deadRows.Scan(&eventName, &failure); err != nil {
			return strings.Join(lines, "\n") + "\nscan dead letters: " + err.Error()
		}
		lines = append(lines, fmt.Sprintf("dead_letter event=%s failure=%s", eventName, failure))
	}
	return strings.Join(lines, "\n")
}

func writeScenarioRunnerFixture(t *testing.T) string {
	t.Helper()
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	if err := os.RemoveAll(filepath.Join(contractsPath, "tests")); err != nil {
		t.Fatalf("remove inherited canonical scenarios: %v", err)
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "fixtures", "item-received.yaml"), `
item_id: fixture
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "empire-routing.yaml"), `
name: empire-style deterministic routing
steps:
  - publish: item.received
    idempotency_key: ${scenario.sha40("empire-cost-router")}
    payload:
      from: fixtures/item-received.yaml
      set:
        item_id: initial
  - publish: item.processed
    payload:
      item_id: review
invalid:
  base:
    publish: item.received
    payload:
      from: fixtures/item-received.yaml
  cases:
    - name: invalid-item-id
      set:
        payload.item_id: [not, text]
      expect: reject
expect:
  events:
    include: [item.received, item.processed]
  no_dead_letters: true
  entities:
    - type: default
      current_state: done
`)
	return contractsPath
}
