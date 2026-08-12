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

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"

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

func TestServedParityHarnessPublicMockApprovalLifecycle(t *testing.T) {
	servedparity.Run(t, servedparity.MustScenario(servedparity.ScenarioPublicMockApprovalLifecycle), runServedPublicMockApprovalBackendProof)
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
	root := canonicalrouting.CopyStandingTelegramServe(t, providerURL)
	for _, name := range []string{
		"flows/telegram-chat/agents.yaml",
		"flows/telegram-chat/events.yaml",
		"flows/telegram-chat/tools.yaml",
	} {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, filepath.FromSlash(name)), "{}\n")
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "telegram-chat", "nodes.yaml"), `
telegram-input-observer:
  id: telegram-input-observer
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "telegram-chat", "tests", "generated-input.yaml"), `
name: generated Telegram normalized input
steps:
  - publish: telegram_text_message
    payload: generate
`)
	return root
}

func writePublicTelegramMockApprovalScenarioFixture(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyStandingTelegramMockServe(t, "https://example.invalid")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: public-telegram-mock-approval
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
connector_packs:
  imports:
    - {provider: telegram, tool: telegram.send_message}
provider_trigger_events:
  imports:
    - {provider: telegram, event: inbound.telegram.text_message}
flows:
  - {id: telegram-chat, flow: telegram-chat, mode: template}
`)
	for _, flow := range []string{"telegram-ingress", "memory-singleton"} {
		if err := os.RemoveAll(filepath.Join(root, "flows", flow)); err != nil {
			t.Fatalf("remove unrelated %s fixture flow: %v", flow, err)
		}
	}
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
	tests := []struct {
		example        canonicalrouting.ArtifactID
		deliveredNodes map[string]int
	}{
		{canonicalrouting.RootIngress, map[string]int{"item-handler": 1, "item-observer": 1}},
		{canonicalrouting.ParentConnect, map[string]int{"producer-node": 1, "consumer-node": 1}},
		{canonicalrouting.TemplateSelectExisting, map[string]int{"producer-node": 2, "account-node": 2}},
		{canonicalrouting.TemplateSelectOrCreate, map[string]int{"producer-node": 1, "account-node": 1}},
		{canonicalrouting.TemplateReply, map[string]int{"initiator-node": 2, "requester-node": 3, "provider-node": 1}},
		{canonicalrouting.TemplateCreateMintedKey, map[string]int{"producer-node": 1, "validator-node": 1}},
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
					if nodeID != "provider-node" {
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
