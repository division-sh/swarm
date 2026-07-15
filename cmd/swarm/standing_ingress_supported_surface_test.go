package main

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
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type telegramPhraseBotLLMRuntime struct{}

func (telegramPhraseBotLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

func (telegramPhraseBotLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.PlatformDefault()
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID: uuid.NewString(), AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...), Memory: memory, MemoryIdentity: execution.Identity,
	}, nil
}

func (telegramPhraseBotLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("phrase-bot requires managed capability surface")
	}
	observed, err := runtimellm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	if message.Role == "tool" {
		return &runtimellm.Response{
			Message:   runtimellm.Message{Role: "assistant", Content: "Telegram reply sent."},
			SessionID: session.ID, CapabilitySurface: &observed,
		}, nil
	}
	const payloadPrefix = "- payload: "
	start := strings.Index(message.Content, payloadPrefix)
	if start < 0 {
		return nil, fmt.Errorf("phrase-bot input has no event payload")
	}
	raw := message.Content[start+len(payloadPrefix):]
	if end := strings.IndexByte(raw, '\n'); end >= 0 {
		raw = raw[:end]
	}
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode phrase-bot event payload: %w", err)
	}
	chatID := strings.TrimSpace(fmt.Sprint(payload["chat_id"]))
	messageText := strings.TrimSpace(fmt.Sprint(payload["text"]))
	if chatID == "" || messageText == "" {
		return nil, fmt.Errorf("phrase-bot requires chat_id and text")
	}
	call := runtimellm.ToolCall{
		ID:   "reply-" + chatID,
		Name: "emit_telegram_reply_requested",
		Arguments: map[string]any{
			"chat_id": chatID,
			"text":    "Swarm heard: " + messageText,
		},
	}
	return &runtimellm.Response{
		Message:   runtimellm.Message{Role: "assistant", ToolCalls: []runtimellm.ToolCall{call}},
		ToolCalls: []runtimellm.ToolCall{call}, SessionID: session.ID, CapabilitySurface: &observed,
	}, nil
}

func (telegramPhraseBotLLMRuntime) PersistConversationSnapshot(context.Context, *runtimellm.Session) error {
	return nil
}
func TestServedParityHarnessStandingServiceLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioStandingServiceSuspendLifecycle),
		servedparity.MustScenario(servedparity.ScenarioStandingServiceResumeLifecycle),
		servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedStandingServiceLifecycleBackendProof)
}

func runServedStandingServiceLifecycleBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	telegramCalls := make(chan struct{}, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		telegramCalls <- struct{}{}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(telegram.Close)
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	configureStandingLifecycleCredentials(t)

	var db *sql.DB
	var opts serveOptions
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "standing-lifecycle.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = stores.SQLDB
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		opts = serveOptions{
			ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
			ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
			StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	case servedparity.BackendExplicitPostgres:
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		oldBuildStores := buildStoresForServe
		oldWorkspace := configuredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return storeBundle{}, err
			}
			if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
				_ = pg.DB.Close()
				return storeBundle{}, err
			}
			db = pg.DB
			return selectedPostgresStoreBundle(pg, cfg), nil
		}
		configuredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			configuredWorkspaceLifecycleForServe = oldWorkspace
		})
		opts = serveOptions{
			ConfigPath: writeServeRuntimeTestConfig(t), ContractsPath: contractsRoot,
			PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
			TestLLMRuntime:          telegramPhraseBotLLMRuntime{},
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}
	default:
		t.Fatalf("unknown standing served parity backend %q", backend)
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	if db == nil {
		t.Fatal("standing served parity SQLDB is required")
	}
	firstEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
	serviceID, firstRunID, firstGeneration := loadServedStandingOwner(t, db, string(backend))
	suspendKey := "standing-suspend-" + string(backend)
	suspended := invokeServedStandingOperation(t, firstEndpoint, "standing.suspend", serviceID, suspendKey)
	if suspended.EffectiveState != "suspended" || suspended.Transition != "suspended" || suspended.RunID != firstRunID {
		t.Fatalf("%s suspend result = %#v", backend, suspended)
	}
	replayedSuspend := invokeServedStandingOperation(t, firstEndpoint, "standing.suspend", serviceID, suspendKey)
	if replayedSuspend != suspended {
		t.Fatalf("%s suspend replay = %#v, want %#v", backend, replayedSuspend, suspended)
	}
	requireStandingTelegramUnavailable(t, strings.TrimSuffix(firstEndpoint, "/v1/rpc"), 9001)
	assertServedStandingState(t, db, string(backend), serviceID, firstRunID, firstGeneration, "suspended", "paused")
	if code := first.stop(); code != 0 {
		t.Fatalf("%s first standing serve exit = %d", backend, code)
	}

	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondOutput := second.outputString()
	if !strings.Contains(secondOutput, "suspended") || !strings.Contains(secondOutput, "swarm standing resume "+serviceID) {
		t.Fatalf("%s restart readiness omitted suspended standing story:\n%s", backend, secondOutput)
	}
	secondEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, secondOutput) + "/v1/rpc"
	resumed := invokeServedStandingOperation(t, secondEndpoint, "standing.resume", serviceID, "standing-resume-"+string(backend))
	if resumed.EffectiveState != "active" || resumed.Transition != "operator_resumed" || resumed.RunID != firstRunID {
		t.Fatalf("%s resume result = %#v", backend, resumed)
	}
	if entity := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9002, 42); entity == "" {
		t.Fatalf("%s resumed standing service returned empty entity", backend)
	}
	requireStandingLifecycleTelegramCall(t, telegramCalls, backend, "resume")
	assertServedStandingState(t, db, string(backend), serviceID, firstRunID, firstGeneration, "active", "running")

	resetKey := "standing-reset-" + string(backend)
	reset := invokeServedStandingOperation(t, secondEndpoint, "standing.reset", serviceID, resetKey)
	if reset.EffectiveState != "active" || reset.Transition != "reset" || reset.Generation != firstGeneration+1 || reset.RunID == firstRunID {
		t.Fatalf("%s reset result = %#v", backend, reset)
	}
	if entity := sendStandingTelegramUpdate(t, strings.TrimSuffix(secondEndpoint, "/v1/rpc"), 9003, 42); entity == "" {
		t.Fatalf("%s reset standing service returned empty entity", backend)
	}
	requireStandingLifecycleTelegramCall(t, telegramCalls, backend, "reset")
	replayedReset := invokeServedStandingOperation(t, secondEndpoint, "standing.reset", serviceID, resetKey)
	if replayedReset != reset {
		t.Fatalf("%s reset replay = %#v, want %#v", backend, replayedReset, reset)
	}
	assertServedStandingState(t, db, string(backend), serviceID, reset.RunID, reset.Generation, "active", "running")
	requireServedParitySettlementPostconditions(t, secondEndpoint, db, string(backend), firstRunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle))
	requireServedParitySettlementPostconditions(t, secondEndpoint, db, string(backend), reset.RunID, servedparity.MustScenario(servedparity.ScenarioStandingServiceResetLifecycle))
	if code := second.stop(); code != 0 {
		t.Fatalf("%s second standing serve exit = %d", backend, code)
	}
}

type servedStandingOperationResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
	Transition     string `json:"transition"`
}

func invokeServedStandingOperation(t *testing.T, endpoint, method, serviceID, idempotencyKey string) servedStandingOperationResult {
	t.Helper()
	var result servedStandingOperationResult
	response := requestServedJSONRPCWithTimeout(t, endpoint, method, map[string]any{
		"service_id": serviceID, "reason": "served parity proof", "idempotency_key": idempotencyKey,
	}, 15*time.Second)
	if response.Error != nil {
		t.Fatalf("%s error = %#v", method, response.Error)
	}
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode %s result: %v\n%s", method, err, string(response.Result))
	}
	return result
}

func configureStandingLifecycleCredentials(t *testing.T) {
	t.Helper()
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentials, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentials.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
}

func loadServedStandingOwner(t *testing.T, db *sql.DB, backend string) (serviceID, runID string, generation int64) {
	t.Helper()
	query := `SELECT service_id, current_run_id, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	if backend == string(servedparity.BackendExplicitPostgres) {
		query = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	}
	if err := db.QueryRowContext(context.Background(), query).Scan(&serviceID, &runID, &generation); err != nil {
		t.Fatalf("%s load standing owner: %v", backend, err)
	}
	return serviceID, runID, generation
}

func assertServedStandingState(t *testing.T, db *sql.DB, backend, serviceID, runID string, generation int64, effectiveState, runStatus string) {
	t.Helper()
	query := `
		SELECT ss.current_run_id, ss.current_generation, ss.effective_state, r.status
		FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id
		WHERE ss.service_id = ?`
	args := []any{serviceID}
	if backend == string(servedparity.BackendExplicitPostgres) {
		query = `
			SELECT ss.current_run_id::text, ss.current_generation, ss.effective_state, r.status
			FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id
			WHERE ss.service_id = $1::uuid`
	}
	var gotRunID, gotState, gotRunStatus string
	var gotGeneration int64
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&gotRunID, &gotGeneration, &gotState, &gotRunStatus); err != nil {
		t.Fatalf("%s load standing state: %v", backend, err)
	}
	if gotRunID != runID || gotGeneration != generation || gotState != effectiveState || gotRunStatus != runStatus {
		t.Fatalf("%s standing state = run:%s generation:%d state:%s run_status:%s", backend, gotRunID, gotGeneration, gotState, gotRunStatus)
	}
}

func TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	calls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode Telegram call: %v", err)
		}
		calls <- body
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer telegram.Close()

	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	dataRoot := t.TempDir()
	sqlitePath := filepath.Join(dataRoot, "standing.sqlite")
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
	opts := serveOptions{
		ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstEntity := sendStandingTelegramUpdate(t, firstURL, 101, 42)
	secondEntity := sendStandingTelegramUpdate(t, firstURL, 102, 42)
	if firstEntity == "" || firstEntity != secondEntity {
		t.Fatalf("delivery entities = %q and %q, want one standing entity", firstEntity, secondEntity)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 42, 42)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	firstOutput := first.outputString()
	for _, want := range []string{
		"swarm serve --dev · ",
		"store                      sqlite · " + sqlitePath,
		"workspace                  host · agent work runs on this machine",
		"listeners                  api 127.0.0.1:",
		"ready in ",
		"telegram webhook",
		"webhook_signing.telegram bound",
		"shutdown · complete",
	} {
		if !strings.Contains(firstOutput, want) {
			t.Fatalf("concise supported serve output missing %q:\n%s", want, firstOutput)
		}
	}
	if strings.Contains(firstOutput, "[1/22]") || strings.Contains(firstOutput, "telegram-secret") || strings.Contains(firstOutput, "\x1b[") {
		t.Fatalf("concise supported serve output leaked verbose, secret, or terminal decoration:\n%s", firstOutput)
	}
	if strings.Count(firstOutput, "workspace                  host · agent work runs on this machine") != 1 || strings.Count(firstOutput, "ready in ") != 1 {
		t.Fatalf("concise supported serve output retained parallel lifecycle writers:\n%s", firstOutput)
	}
	for _, forbidden := range []string{"request_authentication=", "catalog_generation=", "manifest_hash=", "policy_source=", "provenance=", "source_path=", "standing ingress admitted:"} {
		if strings.Contains(firstOutput, forbidden) {
			t.Fatalf("concise supported serve output leaked diagnostic field %q:\n%s", forbidden, firstOutput)
		}
	}

	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	restartedEntity := sendStandingTelegramUpdate(t, secondURL, 103, 84)
	if restartedEntity != firstEntity {
		t.Fatalf("restart entity = %q, want %q", restartedEntity, firstEntity)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 84)
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}

	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite runtime store: %v", err)
	}
	defer sqliteStore.Close()
	var runs, instances, entities int
	var standingRunID string
	if err := sqliteStore.DB.QueryRow(`
		SELECT current_run_id
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-ingress'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`, firstEntity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND json_extract(anchor, '$.entity_id') = ? AND status = 'pending' AND json_extract(snapshot, '$.decision') = 'standing_review'`, firstEntity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	var entityEvents, wrongRunEvents int
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events
		WHERE entity_id = ?
	`, standingRunID, firstEntity).Scan(&entityEvents, &wrongRunEvents); err != nil {
		t.Fatalf("inspect standing event lineage: %v", err)
	}
	if entityEvents == 0 || wrongRunEvents != 0 {
		t.Fatalf("standing entity event lineage = events:%d wrong_run:%d, want events>0/wrong_run:0", entityEvents, wrongRunEvents)
	}
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, standingRunID, nil)
}

func TestStandingIngressUnsupportedAliasFailsBeforeServeReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
	packagePath := filepath.Join(contractsRoot, "package.yaml")
	raw, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read package: %v", err)
	}
	writeStandingCandidateFile(t, packagePath, strings.Replace(string(raw), "alias: chat", "alias: chat/support", 1))
	sqlitePath := filepath.Join(t.TempDir(), "invalid-alias.sqlite")
	process := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Dev: true, Verbose: true,
	})
	code, exited := process.waitForExit(15 * time.Second)
	if !exited {
		process.cleanup()
		t.Fatal("serve reached a live process with an unreachable standing ingress alias")
	}
	process.recordStopped(code)
	output := process.outputString()
	if code == 0 || !strings.Contains(output, "one URL-safe path segment") || strings.Contains(output, "swarm runtime ready") {
		t.Fatalf("invalid alias exit/output = %d\n%s", code, output)
	}
}

func TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	oldBuildStores := buildStoresForServe
	oldWorkspace := configuredWorkspaceLifecycleForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspace
	})

	calls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		calls <- body
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"webhook_signing.telegram": "telegram-secret", "telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
	opts := serveOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "postgres", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	entity := sendStandingTelegramUpdate(t, baseURL, 201, 42)
	if got := sendStandingTelegramUpdate(t, baseURL, 202, 42); got != entity {
		t.Fatalf("second entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 42, 42)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore: %v", err)
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	if got := sendStandingTelegramUpdate(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), 203, 84); got != entity {
		t.Fatalf("restart entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 84)
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open Postgres: %v", err)
	}
	defer db.Close()
	var runs, instances, entities int
	var standingRunID string
	if err := db.QueryRow(`
		SELECT current_run_id::text
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
		  AND declaration_present = TRUE
		  AND effective_state = 'active'
	`).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM standing_services
		WHERE flow_id = 'telegram-ingress'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-ingress'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = $1::uuid`, entity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = $1::uuid THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND anchor->>'entity_id' = $1 AND status = 'pending' AND snapshot->>'decision' = 'standing_review'`, entity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	var entityEvents, wrongRunEvents int
	if err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = $1::uuid THEN 0 ELSE 1 END), 0)
		FROM events
		WHERE entity_id = $2::uuid
	`, standingRunID, entity).Scan(&entityEvents, &wrongRunEvents); err != nil {
		t.Fatalf("inspect standing event lineage: %v", err)
	}
	if entityEvents == 0 || wrongRunEvents != 0 {
		t.Fatalf("standing entity event lineage = events:%d wrong_run:%d, want events>0/wrong_run:0", entityEvents, wrongRunEvents)
	}
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, standingRunID, func(t *testing.T) {
		var reopenErr error
		runtimePG, reopenErr = store.NewPostgresStore(dsn)
		if reopenErr != nil {
			t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", reopenErr)
		}
	})
}

func requireChangedStandingColdStartMatrix(t *testing.T, opts serveOptions, contractsRoot, originalRunID string, prepare func(*testing.T)) {
	t.Helper()
	packagePath := filepath.Join(contractsRoot, "package.yaml")
	basePackage, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read standing package: %v", err)
	}
	flowDir := filepath.Join(contractsRoot, "flows", "telegram-ingress")
	flowSchemaPath := filepath.Join(flowDir, "schema.yaml")
	baseFlowSchema, err := os.ReadFile(flowSchemaPath)
	if err != nil {
		t.Fatalf("read standing flow schema: %v", err)
	}
	type candidateMutation struct {
		name       string
		apply      func(*testing.T)
		wantOutput []string
	}
	mutations := []candidateMutation{
		{name: "source revision one", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.1"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision two", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.2"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "source revision three", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), `version: "1.0.0"`, `version: "1.0.3"`, 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "package manifest name revised", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), "name: standing-telegram-proof", "name: renamed-standing-proof", 1))
		}, wantOutput: []string{" revised run=" + originalRunID}},
		{name: "standing declaration removed", apply: func(t *testing.T) {
			before := string(basePackage)
			start := strings.Index(before, "flows:\n")
			if start < 0 {
				t.Fatal("flows declaration not found")
			}
			writeStandingCandidateFile(t, packagePath, before[:start]+"flows: []\n")
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "standing changed to non-standing", apply: func(t *testing.T) {
			before := string(basePackage)
			standingBlock := `    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`
			changed := strings.Replace(before, standingBlock, "", 1)
			if changed == before {
				t.Fatal("standing activation block not found")
			}
			chatDeclaration := "  - {id: telegram-chat, flow: telegram-chat, mode: template}\n"
			withoutChat := strings.Replace(changed, chatDeclaration, "", 1)
			if withoutChat == changed {
				t.Fatal("telegram-chat declaration not found")
			}
			changed = withoutChat
			writeStandingCandidateFile(t, packagePath, changed)
			nonStandingSchema := canonicalrouting.WithoutStandingIngressPins(t, string(baseFlowSchema))
			writeStandingCandidateFile(t, flowSchemaPath, nonStandingSchema)
		}, wantOutput: []string{" orphaned declaration_removed=true"}},
		{name: "flow identity renamed", apply: func(t *testing.T) {
			renamedDir := filepath.Join(contractsRoot, "flows", "telegram-ingress-v2")
			if err := os.Rename(flowDir, renamedDir); err != nil {
				t.Fatalf("rename flow directory: %v", err)
			}
			t.Cleanup(func() {
				_ = os.Rename(renamedDir, flowDir)
				_ = os.WriteFile(flowSchemaPath, baseFlowSchema, 0o600)
			})
			writeStandingCandidateFile(t, filepath.Join(renamedDir, "schema.yaml"), strings.Replace(string(baseFlowSchema), "name: telegram-ingress", "name: telegram-ingress-v2", 1))
			writeStandingCandidateFile(t, packagePath, strings.ReplaceAll(string(basePackage), "telegram-ingress", "telegram-ingress-v2"))
		}, wantOutput: []string{" created run=", " orphaned declaration_removed=true"}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, string(basePackage))
			writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
			if _, err := os.Stat(flowDir); err != nil {
				t.Fatalf("standing flow directory unavailable before mutation: %v", err)
			}
			mutation.apply(t)
			if prepare != nil {
				prepare(t)
			}
			requireChangedStandingColdStartReconciled(t, opts, mutation.wantOutput...)
		})
	}
	writeStandingCandidateFile(t, packagePath, string(basePackage))
	writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
}

func writeStandingCandidateFile(t testing.TB, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write standing candidate %s: %v", path, err)
	}
}

func requireChangedStandingColdStartReconciled(t *testing.T, opts serveOptions, wantOutput ...string) {
	t.Helper()
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	if code := process.stop(); code != 0 {
		t.Fatalf("changed standing bundle exit = %d\n%s", code, process.outputString())
	}
	output := process.outputString()
	for _, want := range wantOutput {
		if !strings.Contains(output, want) {
			t.Fatalf("changed standing bundle output omitted %q:\n%s", want, output)
		}
	}
}

func requireStandingTelegramUnavailable(t testing.TB, baseURL string, updateID int) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"chat":{"id":42},"text":"hello %d"}}`, updateID, updateID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new suspended webhook request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send suspended webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		var payload any
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		t.Fatalf("suspended webhook status=%d payload=%v, want %d", resp.StatusCode, payload, http.StatusServiceUnavailable)
	}
}

func requireStandingLifecycleTelegramCall(t testing.TB, calls <-chan struct{}, backend servedparity.Backend, phase string) {
	t.Helper()
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out waiting for standing Telegram side effect after %s", backend, phase)
	}
}

func sendStandingTelegramUpdate(t testing.TB, baseURL string, updateID, chatID int) string {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"chat":{"id":%d},"text":"hello %d"}}`, updateID, updateID, chatID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new webhook request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send webhook: %v", err)
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode webhook response status=%d: %v", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status=%d payload=%v", resp.StatusCode, payload)
	}
	return strings.TrimSpace(fmt.Sprint(payload["entity_id"]))
}

func requireStandingTelegramCalls(t testing.TB, calls <-chan map[string]any, sqlitePath string, chatIDs ...int) {
	t.Helper()
	for i, chatID := range chatIDs {
		select {
		case call := <-calls:
			if got := strings.TrimSpace(fmt.Sprint(call["chat_id"])); got != fmt.Sprint(chatID) {
				t.Fatalf("Telegram chat_id = %v, want %d", call["chat_id"], chatID)
			}
		case <-time.After(30 * time.Second):
			diagnostics := "unavailable"
			if strings.HasPrefix(sqlitePath, "postgres:") {
				diagnostics = standingPostgresDiagnostics(strings.TrimPrefix(sqlitePath, "postgres:"))
			} else if strings.TrimSpace(sqlitePath) != "" {
				diagnostics = standingSQLiteDiagnostics(sqlitePath)
			}
			t.Fatalf("timed out waiting for Telegram reply %d/%d; diagnostics: %s", i+1, len(chatIDs), diagnostics)
		}
	}
}

func standingPostgresDiagnostics(dsn string) string {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err.Error()
	}
	defer db.Close()
	rows, err := db.Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	parts := []string{}
	for rows.Next() {
		var name string
		var count int
		if rows.Scan(&name, &count) == nil {
			parts = append(parts, fmt.Sprintf("%s=%d", name, count))
		}
	}
	logRows, err := db.Query(`SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := db.Query(`SELECT event_id::text, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := db.Query(`SELECT event_id::text, status, COALESCE(failure::text, '') FROM pipeline_receipts ORDER BY updated_at`)
	if err == nil {
		defer receiptRows.Close()
		for receiptRows.Next() {
			var eventID, status, failure string
			if receiptRows.Scan(&eventID, &status, &failure) == nil {
				parts = append(parts, fmt.Sprintf("receipt:%s:%s:%s", eventID, status, failure))
			}
		}
	}
	return strings.Join(parts, ",")
}

func standingSQLiteDiagnostics(path string) string {
	store, err := store.NewSQLiteRuntimeStore(path)
	if err != nil {
		return err.Error()
	}
	defer store.Close()
	rows, err := store.DB.Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	parts := []string{}
	for rows.Next() {
		var name string
		var count int
		if err := rows.Scan(&name, &count); err != nil {
			return err.Error()
		}
		parts = append(parts, fmt.Sprintf("%s=%d", name, count))
	}
	logRows, err := store.DB.Query(`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := store.DB.Query(`SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := store.DB.Query(`SELECT event_id, status, COALESCE(failure, '') FROM pipeline_receipts ORDER BY updated_at`)
	if err == nil {
		defer receiptRows.Close()
		for receiptRows.Next() {
			var eventID, status, failure string
			if receiptRows.Scan(&eventID, &status, &failure) == nil {
				parts = append(parts, fmt.Sprintf("receipt:%s:%s:%s", eventID, status, failure))
			}
		}
	}
	return strings.Join(parts, ",")
}

func writeStandingTelegramServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	return canonicalrouting.CopyStandingTelegramServe(t, telegramBaseURL)
}
