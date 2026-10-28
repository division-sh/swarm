package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type telegramPhraseBotLLMRuntime struct {
	onContinue func(context.Context, *runtimellm.Session, runtimellm.Message) error
}

func (telegramPhraseBotLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

func (telegramPhraseBotLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.Plan{}
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID: uuid.NewString(), AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...), Memory: memory, MemoryIdentity: execution.Identity,
	}, nil
}

func (telegramPhraseBotLLMRuntime) PrepareManagedSession(context.Context, *runtimellm.Session) error {
	return nil
}

func (r telegramPhraseBotLLMRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, managedCall runtimellm.ManagedCall) (*runtimellm.Response, error) {
	message, err := managedCall.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if r.onContinue != nil {
		if err := r.onContinue(ctx, session, message); err != nil {
			return nil, err
		}
	}
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
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(managedCall.Frame().Turn.Event.Payload)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode phrase-bot event payload: %w", err)
	}
	chatID := strings.TrimSpace(fmt.Sprint(payload["conversation_reference"]))
	messageText := strings.TrimSpace(fmt.Sprint(payload["text"]))
	if chatID == "" || messageText == "" {
		return nil, fmt.Errorf("phrase-bot requires conversation_reference and text")
	}
	call := runtimellm.ToolCall{
		ID:   "reply-" + chatID,
		Name: "emit_telegram_reply_requested",
		Arguments: map[string]any{
			"chat_id": chatID,
			"text":    "Swarm heard: " + messageText,
		},
	}
	toolOutputAuthority := runtimellm.ToolOutputAuthority{
		ProviderOperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("standing-phrase-bot-provider-operation:"+managedCall.Frame().FrameID)).String(),
		SettledAt:           time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
	}
	return &runtimellm.Response{
		Message:             runtimellm.Message{Role: "assistant", ToolCalls: []runtimellm.ToolCall{call}},
		ToolCalls:           []runtimellm.ToolCall{call},
		SessionID:           session.ID,
		CapabilitySurface:   &observed,
		ToolOutputAuthority: &toolOutputAuthority,
	}, nil
}

func (telegramPhraseBotLLMRuntime) ContinueForkChatSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ForkChatCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func (telegramPhraseBotLLMRuntime) PersistConversationSnapshot(context.Context, *sessions.Lease, *runtimellm.Session) error {
	return nil
}

func configureStandingLifecycleCredentials(t *testing.T) {
	t.Helper()
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentials, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"telegram.webhook_signing_secret": "telegram-secret", "telegram.telegram_bot_token": "bot-token"} {
		if err := credentials.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
}

func assertServedFlowRunState(t *testing.T, db *sql.DB, backend, runID, runStatus string) {
	t.Helper()
	query := `SELECT status FROM runs WHERE run_id = ? AND landing_run=TRUE`
	if backend == "postgres" || backend == string(servedparity.BackendExplicitPostgres) {
		query = `SELECT status FROM runs WHERE run_id = $1::uuid AND landing_run=TRUE`
	}
	var gotRunStatus string
	if err := db.QueryRowContext(context.Background(), query, runID).Scan(&gotRunStatus); err != nil {
		t.Fatalf("%s load landing run: %v", backend, err)
	}
	if gotRunStatus != runStatus {
		t.Fatalf("%s landing run %s status=%s, want %s", backend, runID, gotRunStatus, runStatus)
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

	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	dataRoot := t.TempDir()
	sqlitePath := filepath.Join(dataRoot, "standing.sqlite")
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"telegram.webhook_signing_secret": "telegram-secret", "telegram.telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
	opts := cliapp.ServeOptions{
		ConfigPath: configPath, SourceRoot: sourceRoot, StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck:      true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstBinding := sendStandingTelegramUpdate(t, firstURL, 101, 42)
	secondBinding := sendStandingTelegramUpdate(t, firstURL, 102, 42)
	if firstBinding != secondBinding {
		t.Fatalf("delivery bindings = %+v and %+v, want one standing generation", firstBinding, secondBinding)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 42, 42)
	waitForStandingDeliveryQuiescence(t, sqlitePath)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	firstOutput := first.outputString()
	for _, want := range []string{
		"swarm serve · ",
		"store                      sqlite · " + sqlitePath,
		"workspace                  host · agent work runs on this machine",
		"listeners                  api 127.0.0.1:",
		"ready in ",
		"telegram webhook",
		"telegram.webhook_signing_secret bound",
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

	enableServeRuntimeRecovery(t, configPath)
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	restartedBinding := sendStandingTelegramUpdate(t, secondURL, 103, 84)
	if restartedBinding != firstBinding {
		t.Fatalf("restart binding = %+v, want %+v", restartedBinding, firstBinding)
	}
	requireStandingTelegramCalls(t, calls, sqlitePath, 84)
	waitForStandingDeliveryQuiescence(t, sqlitePath)
	requireStandingDeclarationPublicationReadback(t, secondURL, firstBinding, []string{"101", "102", "103"})
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}

	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite runtime store: %v", err)
	}
	defer func() {
		if sqliteStore != nil {
			_ = sqliteStore.Close()
		}
	}()
	var runs, instances, entities int
	var standingRunID string
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT run_id
		FROM runs
		WHERE flow_path = '.'
		  AND landing_run = TRUE
	`).Scan(&standingRunID); err != nil || standingRunID != firstBinding.RunID {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	_, receiver := requireSingleReceiverTargetState(t, requireReceiverProofStateReader(t, sqliteStore), standingRunID, ".")
	standingEntityID := receiver.EntityID
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT COUNT(*)
		FROM runs
		WHERE flow_path = '.'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow("SELECT COUNT(*) FROM flow_instances WHERE flow_template = '.'").Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`, standingEntityID).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
	var chatInstances, normalizedEvents, wrongNormalizedRuns int
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&chatInstances); err != nil {
		t.Fatalf("count per-chat instances: %v", err)
	}
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'
	`, standingRunID).Scan(&normalizedEvents, &wrongNormalizedRuns); err != nil {
		t.Fatalf("inspect normalized event lineage: %v", err)
	}
	if chatInstances != 2 || normalizedEvents != 3 || wrongNormalizedRuns != 0 {
		t.Fatalf("normalized routing = chat_instances:%d events:%d wrong_run:%d, want 2/3/0", chatInstances, normalizedEvents, wrongNormalizedRuns)
	}
	var pendingCards int
	if err := storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND json_extract(anchor, '$.entity_id') = ? AND status = 'pending' AND json_extract(snapshot, '$.decision') = 'retire_service'`, standingEntityID).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	if _, err := initializeServePlatformStateStores(t.Context(), sqliteStore); err != nil {
		t.Fatal(err)
	}
	disableServeRuntimeRecovery(t, opts.ConfigPath)
	requireChangedStandingColdStartMatrix(t, opts, sourceRoot, standingRunID, func(t *testing.T) standingRetainedState {
		return readStandingRetainedState(t, sqliteStore, standingRunID)
	})
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close SQLite inspection store after restart matrix: %v", err)
	}
	sqliteStore = nil
}

func TestServeAuthorActivityAttachmentFailureKeepsRuntimeHealthy(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()

	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	dataRoot := t.TempDir()
	sqlitePath := filepath.Join(dataRoot, "standing.sqlite")
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"telegram.webhook_signing_secret": "telegram-secret", "telegram.telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		SourceRoot: sourceRoot, StoreMode: "sqlite", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Dev: true, LocalRun: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
		TestAfterAuthorActivityHead: func() error { return errors.New("author activity head unavailable") },
	})
	process.waitForReadyLine()
	output := process.outputString()
	for _, want := range []string{"author activity head unavailable", "inspect with swarm logs --follow"} {
		if !strings.Contains(output, want) {
			t.Fatalf("attachment failure output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "ready — waiting for events") {
		t.Fatalf("failed attachment claimed to be waiting for events:\n%s", output)
	}
	response, err := http.Get("http://" + serveRuntimeAPIListenerFromOutput(t, output) + "/healthz")
	if err != nil {
		t.Fatalf("healthy runtime after feed attachment failure: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status after feed attachment failure = %d", response.StatusCode)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exit after feed attachment failure = %d\n%s", code, process.outputString())
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
	t.Cleanup(func() {
		if err := runtimePG.Close(); err != nil {
			t.Error(err)
		}
	})
	oldBuildStores := buildStoresForServe
	oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		storetest.BootstrapPostgresRuntimeStore(t, runtimePG)
		return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(runtimePG), cfg), nil
	}
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
	})

	calls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		calls <- body
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"telegram.webhook_signing_secret": "telegram-secret", "telegram.telegram_bot_token": "bot-token"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}
	opts := cliapp.ServeOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: sourceRoot, StoreMode: "postgres", APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Verbose: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	binding := sendStandingTelegramUpdate(t, baseURL, 201, 42)
	if got := sendStandingTelegramUpdate(t, baseURL, 202, 42); got != binding {
		t.Fatalf("second binding = %+v, want %+v", got, binding)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 42, 42)
	waitForStandingDeliveryQuiescence(t, "postgres:"+dsn)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore: %v", err)
	}
	enableServeRuntimeRecovery(t, opts.ConfigPath)
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	if got := sendStandingTelegramUpdate(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), 203, 84); got != binding {
		t.Fatalf("restart binding = %+v, want %+v", got, binding)
	}
	requireStandingTelegramCalls(t, calls, "postgres:"+dsn, 84)
	waitForStandingDeliveryQuiescence(t, "postgres:"+dsn)
	requireStandingDeclarationPublicationReadback(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), binding, []string{"201", "202", "203"})
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
		SELECT run_id::text
		FROM runs
		WHERE flow_path = '.'
		  AND landing_run = TRUE
	`).Scan(&standingRunID); err != nil || standingRunID != binding.RunID {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	observer, err := storetest.OpenReleaseProcessReadOnlyInspection("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, receiver := requireSingleReceiverTargetState(t, requireReceiverProofStateReader(t, observer), standingRunID, ".")
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	entity := receiver.EntityID
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM runs
		WHERE flow_path = '.'
	`).Scan(&runs); err != nil {
		t.Fatalf("count standing run authorities: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM flow_instances WHERE flow_template = '.'").Scan(&instances); err != nil {
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
	if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE anchor_kind = 'stage_gate' AND anchor->>'entity_id' = $1 AND status = 'pending' AND snapshot->>'decision' = 'retire_service'`, entity).Scan(&pendingCards); err != nil || pendingCards != 1 {
		t.Fatalf("standing initial gate cards = %d, %v, want one persisted card across restart", pendingCards, err)
	}
	disableServeRuntimeRecovery(t, opts.ConfigPath)
	requireChangedStandingColdStartMatrix(t, opts, sourceRoot, standingRunID, func(t *testing.T) standingRetainedState {
		if err := runtimePG.Close(); err != nil {
			t.Fatal(err)
		}
		var reopenErr error
		runtimePG, reopenErr = store.NewPostgresStore(dsn)
		if reopenErr != nil {
			t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", reopenErr)
		}
		storetest.BootstrapPostgresRuntimeStore(t, runtimePG)
		return readStandingRetainedState(t, runtimePG, standingRunID)
	})
}

func TestStandingRestartMixedHealthyAndTerminalProcessParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
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
			t.Cleanup(telegram.Close)
			sourceRoot := writeMixedStandingTelegramServeFixture(t, telegram.URL)
			configureStandingLifecycleCredentials(t)

			var (
				db            *sql.DB
				postgresStore *store.PostgresStore
				sqliteStore   *store.SQLiteRuntimeStore
			)
			captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
				db, postgresStore, sqliteStore = selectedRuntimeStoreForTest(t, persistence)
			})
			runtimes := make(chan *runtimepkg.Runtime, 2)
			opts := cliapp.ServeOptions{
				SourceRoot: sourceRoot, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SelfCheck: true, Verbose: true,
				TestLLMRuntime: telegramPhraseBotLLMRuntime{}, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
				TestRuntimeReadyHook: func(rt *runtimepkg.Runtime) { runtimes <- rt },
			}
			selectedStore := ""
			if backend == "sqlite" {
				sqlitePath := filepath.Join(t.TempDir(), "mixed-standing.sqlite")
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
				opts.StoreMode = "sqlite"
				selectedStore = sqlitePath
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				var opened *store.PostgresStore
				oldBuildStores := buildStoresForServe
				oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
				buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
					var err error
					opened, err = store.NewPostgresStore(dsn)
					if err != nil {
						return nil, err
					}
					storetest.BootstrapPostgresRuntimeStore(t, opened)
					return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(opened), cfg), nil
				}
				cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
					return serveRuntimeWorkspaceStub{}, nil
				}
				t.Cleanup(func() {
					buildStoresForServe = oldBuildStores
					cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
				})
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode = "postgres"
				opts.StoreModeSet = true
				selectedStore = "postgres:" + dsn
			}

			first := startServeRuntimeTestProcess(t, opts)
			first.waitForReadyLine()
			firstRuntime := waitForStandingRuntime(t, runtimes, backend, "first boot")
			if db == nil {
				t.Fatalf("%s mixed standing database was not captured", backend)
			}
			healthyRun := loadServedLandingRunByFlow(t, db, backend, ".")
			terminalRun := loadServedLandingRunByFlow(t, db, backend, "telegram-stopped")
			firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
			if binding := sendStandingTelegramUpdate(t, firstURL, 301, 42); binding.RunID != healthyRun {
				t.Fatalf("%s healthy standing service returned wrong binding before restart: %+v", backend, binding)
			}
			requireStandingTelegramCalls(t, calls, selectedStore, 42)
			waitForStandingDeliveryQuiescence(t, selectedStore)
			terminalizeStandingRunFromRuntime(t, firstRuntime, postgresStore, sqliteStore, terminalRun)
			assertServedFlowRunState(t, db, backend, terminalRun, "cancelled")
			if code := first.stop(); code != 0 {
				t.Fatalf("%s first mixed standing serve exit = %d", backend, code)
			}

			enableServeRuntimeRecovery(t, opts.ConfigPath)
			second := startServeRuntimeTestProcess(t, opts)
			second.waitForReadyLine()
			_ = waitForStandingRuntime(t, runtimes, backend, "restart")
			secondOutput := second.outputString()
			secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, secondOutput)
			if binding := sendStandingTelegramUpdate(t, secondURL, 302, 84); binding.RunID != healthyRun {
				t.Fatalf("%s healthy standing service returned wrong binding after restart: %+v", backend, binding)
			}
			requireStandingTelegramCalls(t, calls, selectedStore, 84)
			waitForStandingDeliveryQuiescence(t, selectedStore)
			assertNoStandingTelegramCall(t, calls, backend)
			assertServedFlowRunState(t, db, backend, healthyRun, "live")
			assertServedFlowRunState(t, db, backend, terminalRun, "cancelled")
			if code := second.stop(); code != 0 {
				t.Fatalf("%s second mixed standing serve exit = %d", backend, code)
			}
		})
	}
}

func waitForStandingRuntime(t testing.TB, runtimes <-chan *runtimepkg.Runtime, backend, phase string) *runtimepkg.Runtime {
	t.Helper()
	select {
	case rt := <-runtimes:
		return rt
	case <-time.After(15 * time.Second):
		t.Fatalf("%s timed out waiting for runtime during %s", backend, phase)
		return nil
	}
}

func terminalizeStandingRunFromRuntime(t testing.TB, rt *runtimepkg.Runtime, postgresStore *store.PostgresStore, sqliteStore *store.SQLiteRuntimeStore, runID string) {
	t.Helper()
	if rt == nil || rt.WorkOccurrence() == nil {
		t.Fatal("standing terminalization requires the live runtime occurrence")
	}
	fact := rt.Options.SourceArtifactFact
	runtimeInstanceID := strings.TrimSpace(rt.Options.RuntimeInstanceID)
	if runtimeInstanceID == "" || fact.BundleHash() == "" {
		t.Fatalf("standing terminalization runtime identity = %q/%q", runtimeInstanceID, fact.BundleHash())
	}
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeInstanceID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash()))
	ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
	ctx, err := rt.Bus.AdmitSourceArtifactFact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := runtimerunlifecycle.TerminalRequest{
		RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
	}
	if postgresStore != nil {
		_, _, err = postgresStore.MarkTerminalRun(ctx, request)
	} else if sqliteStore != nil {
		_, _, err = sqliteStore.MarkTerminalRun(ctx, request)
	} else {
		t.Fatal("standing terminalization requires a selected-store lifecycle owner")
	}
	if err != nil {
		t.Fatalf("terminalize standing run %s: %v", runID, err)
	}
}

func loadServedLandingRunByFlow(t testing.TB, db *sql.DB, backend, flowID string) string {
	t.Helper()
	query := `SELECT run_id FROM runs WHERE flow_path = ? AND landing_run=TRUE`
	args := []any{flowID}
	if backend == "postgres" {
		query = `SELECT run_id::text FROM runs WHERE flow_path = $1 AND landing_run=TRUE`
	}
	var runID string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&runID); err != nil {
		t.Fatalf("%s load standing owner for flow %s: %v", backend, flowID, err)
	}
	return runID
}

func assertNoStandingTelegramCall(t testing.TB, calls <-chan map[string]any, backend string) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("%s emitted an unexpected duplicate Telegram call: %#v", backend, call)
	case <-time.After(250 * time.Millisecond):
	}
}

func enableServeRuntimeRecovery(t *testing.T, configPath string) {
	t.Helper()
	setServeRuntimeRecovery(t, configPath, false, true)
}

func disableServeRuntimeRecovery(t *testing.T, configPath string) {
	t.Helper()
	setServeRuntimeRecovery(t, configPath, true, false)
}

func setServeRuntimeRecovery(t *testing.T, configPath string, from, to bool) {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read serve runtime config: %v", err)
	}
	fromLine := fmt.Sprintf("recovery_on_startup: %t", from)
	toLine := fmt.Sprintf("recovery_on_startup: %t", to)
	if count := strings.Count(string(raw), fromLine); count != 1 {
		t.Fatalf("%s count = %d, want 1", fromLine, count)
	}
	updated := strings.Replace(string(raw), fromLine, toLine, 1)
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("set serve runtime recovery to %t: %v", to, err)
	}
}

type standingRetainedState struct {
	run    runtimerunlifecycle.Snapshot
	source sourceartifact.Persisted
	work   map[string]storetest.SelectedForkStorageTableSnapshot
}

func readStandingRetainedState(t *testing.T, selected sourceArtifactReader, runID string) standingRetainedState {
	t.Helper()
	run, err := storetest.ReadRuntimeLogRunSnapshot(t.Context(), selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := selected.GetSourceArtifact(t.Context(), run.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	work, err := storetest.ReadSelectedForkSourceDomain(t.Context(), selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	return standingRetainedState{run: run, source: source, work: work}
}

func requireChangedStandingColdStartMatrix(t *testing.T, opts cliapp.ServeOptions, sourceRoot, originalRunID string, prepare func(*testing.T) standingRetainedState) {
	t.Helper()
	manifestPath := filepath.Join(sourceRoot, "manifest.yaml")
	baseManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read standing manifest: %v", err)
	}
	rootSchemaPath := filepath.Join(sourceRoot, "schema.yaml")
	baseRootSchema, err := os.ReadFile(rootSchemaPath)
	if err != nil {
		t.Fatalf("read root connection schema: %v", err)
	}
	removeConnectedReceiver := func(t *testing.T) {
		t.Helper()
		// The removed chat flow owns the tool used by the root smoke scenario.
		// Retire its executable test declaration too, rather than leave a dangling double.
		for _, name := range []string{"telegram-chat", "tests"} {
			original := filepath.Join(sourceRoot, name)
			removed := filepath.Join(t.TempDir(), name)
			if err := os.Rename(original, removed); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Rename(removed, original); err != nil {
					t.Error(err)
				}
			})
		}
		var schema map[string]any
		if err := yaml.Unmarshal(baseRootSchema, &schema); err != nil {
			t.Fatal(err)
		}
		delete(schema, "connect")
		raw, err := yaml.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, rootSchemaPath, string(raw))
	}
	flowDir := filepath.Join(sourceRoot, ".")
	flowSchemaPath := filepath.Join(flowDir, "schema.yaml")
	baseFlowSchema, err := os.ReadFile(flowSchemaPath)
	if err != nil {
		t.Fatalf("read standing flow schema: %v", err)
	}
	type candidateMutation struct {
		name       string
		apply      func(*testing.T)
		wantStrand []string
	}
	mutations := []candidateMutation{
		{name: "source revision one", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.1"`, 1))
		}},
		{name: "source revision two", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.2"`, 1))
		}},
		{name: "source revision three", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, manifestPath, strings.Replace(string(baseManifest), `version: "1.0.0"`, `version: "1.0.3"`, 1))
		}},
		{name: "standing changed to non-standing", wantStrand: []string{"instance telegram-chat/", "stage_removed (pending)", "field_value_incompatible"}, apply: func(t *testing.T) {
			removeConnectedReceiver(t)
			var schema map[string]any
			if err := yaml.Unmarshal(baseFlowSchema, &schema); err != nil {
				t.Fatal(err)
			}
			delete(schema, "pins")
			delete(schema, "activation")
			delete(schema, "ingress")
			delete(schema, "connect")
			raw, err := yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, flowSchemaPath, string(raw))
		}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			writeStandingCandidateFile(t, rootSchemaPath, string(baseRootSchema))
			writeStandingCandidateFile(t, manifestPath, string(baseManifest))
			writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
			if _, err := os.Stat(flowDir); err != nil {
				t.Fatalf("standing flow directory unavailable before mutation: %v", err)
			}
			mutation.apply(t)
			before := prepare(t)
			if len(mutation.wantStrand) != 0 {
				ready := make(chan struct{}, 1)
				refused := opts
				refused.TestRuntimeReadyHook = func(*runtimepkg.Runtime) { ready <- struct{}{} }
				process := startServeRuntimeTestProcess(t, refused)
				code, exited := process.waitForExit(serveRuntimeStopTimeout)
				if !exited {
					process.cleanup()
					t.Fatal("destructive bundle edit did not refuse before readiness")
				}
				output := process.outputString()
				if code != 3 || len(ready) != 0 || strings.Contains(output, "swarm runtime ready") {
					t.Fatalf("destructive bundle edit reached readiness or lost refusal: exit=%d\n%s", code, output)
				}
				for _, want := range append([]string{"pinned bundle admission", "bundle upgrade would strand retained work"}, mutation.wantStrand...) {
					if !strings.Contains(output, want) {
						t.Fatalf("destructive bundle refusal missing %q:\n%s", want, output)
					}
				}
				if after := prepare(t); !reflect.DeepEqual(before, after) {
					t.Fatalf("refused bundle edit changed retained run, source or work: before=%+v after=%+v", before, after)
				}
				return
			}
			requireChangedStandingColdStartReconciled(t, opts, originalRunID)
		})
	}
	writeStandingCandidateFile(t, rootSchemaPath, string(baseRootSchema))
	writeStandingCandidateFile(t, manifestPath, string(baseManifest))
	writeStandingCandidateFile(t, flowSchemaPath, string(baseFlowSchema))
}

func writeStandingCandidateFile(t testing.TB, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write standing candidate %s: %v", path, err)
	}
}

func requireChangedStandingColdStartReconciled(t *testing.T, opts cliapp.ServeOptions, originalRunID string) {
	t.Helper()
	ready := make(chan *runtimepkg.Runtime, 1)
	previousHook := opts.TestRuntimeReadyHook
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
		if previousHook != nil {
			previousHook(rt)
		}
		ready <- rt
	}
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	rt := <-ready
	reader, ok := rt.Bus.Store().(runtimerunlifecycle.LandingRunReader)
	if !ok {
		t.Fatal("served store lacks its native landing run reader")
	}
	landing, found, err := reader.LoadLandingRun(t.Context(), runtimerunlifecycle.LiveRunScope{FlowPath: "."})
	if err != nil || !found || landing.RunID != originalRunID || landing.State != runtimerunlifecycle.StateLive {
		t.Fatalf("source change discarded or replaced the retained flow run: %+v found=%t err=%v", landing, found, err)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("changed standing bundle exit = %d\n%s", code, process.outputString())
	}
	output := process.outputString()
	if !strings.Contains(output, "shutdown · complete") {
		t.Fatalf("changed standing bundle did not release its selected-store authority:\n%s", output)
	}
}

func sendStandingTelegramUpdate(t testing.TB, baseURL string, updateID, chatID int, diagnostics ...func() string) standingTelegramBinding {
	t.Helper()
	return sendStandingTelegramUpdatePublication(t, baseURL, updateID, chatID, diagnostics...).standingTelegramBinding
}

func standingWebhookDiagnostics(diagnostics []func() string) string {
	if len(diagnostics) == 0 || diagnostics[0] == nil {
		return ""
	}
	return "\nserve output:\n" + diagnostics[0]()
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

func waitForStandingDeliveryQuiescence(t testing.TB, selectedStore string) {
	t.Helper()
	driver, dsn := "sqlite", selectedStore
	if strings.HasPrefix(selectedStore, "postgres:") {
		driver, dsn = "postgres", strings.TrimPrefix(selectedStore, "postgres:")
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s delivery observation: %v", driver, err)
	}
	defer db.Close()

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var unfinished int
		err := db.QueryRow(`
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE status IN ('pending', 'failed', 'in_progress')`).Scan(&unfinished)
		if err == nil && unfinished == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("wait for %s delivery quiescence: unfinished=%d err=%v", driver, unfinished, err)
		case <-ticker.C:
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
	selected, err := store.NewSQLiteRuntimeStore(path)
	if err != nil {
		return err.Error()
	}
	defer selected.Close()
	rows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
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
	logRows, err := storetest.DatabaseForTest(selected).Query(`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`)
	if err == nil {
		defer logRows.Close()
		for logRows.Next() {
			var payload string
			if logRows.Scan(&payload) == nil {
				parts = append(parts, payload)
			}
		}
	}
	deliveryRows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`)
	if err == nil {
		defer deliveryRows.Close()
		for deliveryRows.Next() {
			var eventID, subscriber, status string
			if deliveryRows.Scan(&eventID, &subscriber, &status) == nil {
				parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", eventID, subscriber, status))
			}
		}
	}
	receiptRows, err := storetest.DatabaseForTest(selected).Query(`SELECT event_id, status, COALESCE(failure, '') FROM pipeline_receipts ORDER BY updated_at`)
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
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	selected := filepath.Join(filepath.Dir(root), "chat")
	if err := os.Rename(root, selected); err != nil {
		t.Fatal(err)
	}
	redirectExternalHosts(t, map[string]string{"api.telegram.org": telegramBaseURL})
	return selected
}

func writeMixedStandingTelegramServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	root := writeStandingTelegramServeFixture(t, telegramBaseURL)
	flowDir := filepath.Join(root, "telegram-stopped")
	if err := os.MkdirAll(flowDir, 0o755); err != nil {
		t.Fatalf("create terminal standing flow directory: %v", err)
	}
	baseSchema, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
	if err != nil {
		t.Fatalf("read healthy standing flow schema: %v", err)
	}
	stoppedSchema := canonicalrouting.WithoutStandingIngressPins(t, string(baseSchema))
	var declaration map[string]any
	if err := yaml.Unmarshal([]byte(stoppedSchema), &declaration); err != nil {
		t.Fatal(err)
	}
	delete(declaration, "connect")
	encoded, err := yaml.Marshal(declaration)
	if err != nil {
		t.Fatal(err)
	}
	stoppedSchema = string(encoded)
	stoppedSchema += "schedules:\n  retained: {every: 24h, emit: service.tick}\n"
	writeStandingCandidateFile(t, filepath.Join(flowDir, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeStandingCandidateFile(t, filepath.Join(flowDir, "schema.yaml"), stoppedSchema)
	writeStandingCandidateFile(t, filepath.Join(flowDir, "events.yaml"), "service.tick:\n")
	writeStandingCandidateFile(t, filepath.Join(flowDir, "nodes.yaml"), "hold:\n  execution_type: system_node\n  subscribes_to: [service.tick]\n  event_handlers:\n    service.tick:\n      guard: {id: hold, check: true}\n")
	baseEntities, err := os.ReadFile(filepath.Join(root, "entities.yaml"))
	if err != nil {
		t.Fatalf("read healthy standing flow entities: %v", err)
	}
	writeStandingCandidateFile(t, filepath.Join(flowDir, "entities.yaml"), string(baseEntities))
	return root
}
