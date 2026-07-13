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
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
)

type telegramPhraseBotLLMRuntime struct{}

func (telegramPhraseBotLLMRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{
		ID: agentID + "-session", AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...),
	}, nil
}

func (telegramPhraseBotLLMRuntime) ContinueSession(_ context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	if message.Role == "tool" {
		return &runtimellm.Response{
			Message:   runtimellm.Message{Role: "assistant", Content: "Telegram reply sent."},
			SessionID: session.ID,
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
		ToolCalls: []runtimellm.ToolCall{call}, SessionID: session.ID,
	}, nil
}

func (telegramPhraseBotLLMRuntime) PersistConversationSnapshot(context.Context, *runtimellm.Session) error {
	return nil
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
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, nil)

	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite runtime store: %v", err)
	}
	defer sqliteStore.Close()
	var runs, instances, entities int
	var standingRunID string
	if err := sqliteStore.DB.QueryRow(`
		SELECT es.run_id
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		JOIN runs r ON r.run_id = es.run_id
		WHERE es.entity_id = ?
		  AND fi.flow_template = 'telegram-ingress'
		  AND json_extract(es.fields, '$.activation') = 'standing'
		  AND r.status = 'running'
		  AND r.bundle_hash IS NOT NULL
	`, firstEntity).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`
		SELECT COUNT(DISTINCT es.run_id)
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE fi.flow_template = 'telegram-ingress'
		  AND json_extract(es.fields, '$.activation') = 'standing'
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
	requireChangedStandingColdStartMatrix(t, opts, contractsRoot, func(t *testing.T) {
		var reopenErr error
		runtimePG, reopenErr = store.NewPostgresStore(dsn)
		if reopenErr != nil {
			t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", reopenErr)
		}
	})

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open Postgres: %v", err)
	}
	defer db.Close()
	var runs, instances, entities int
	var standingRunID string
	if err := db.QueryRow(`
		SELECT es.run_id::text
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		JOIN runs r ON r.run_id = es.run_id
		WHERE es.entity_id = $1::uuid
		  AND fi.flow_template = 'telegram-ingress'
		  AND es.fields->>'activation' = 'standing'
		  AND r.status = 'running'
		  AND r.bundle_hash IS NOT NULL
	`, entity).Scan(&standingRunID); err != nil {
		t.Fatalf("resolve standing run authority: %v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(DISTINCT es.run_id)
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE fi.flow_template = 'telegram-ingress'
		  AND es.fields->>'activation' = 'standing'
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
}

func requireChangedStandingColdStartMatrix(t *testing.T, opts serveOptions, contractsRoot string, prepare func(*testing.T)) {
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
		name  string
		apply func(*testing.T)
	}
	mutations := []candidateMutation{
		{name: "package identity renamed", apply: func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, strings.Replace(string(basePackage), "name: standing-telegram-proof", "name: renamed-standing-proof", 1))
		}},
		{name: "standing declaration removed", apply: func(t *testing.T) {
			before := string(basePackage)
			start := strings.Index(before, "flows:\n")
			if start < 0 {
				t.Fatal("flows declaration not found")
			}
			writeStandingCandidateFile(t, packagePath, before[:start]+"flows: []\n")
		}},
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
		}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			writeStandingCandidateFile(t, packagePath, string(basePackage))
			if _, err := os.Stat(flowDir); err != nil {
				t.Fatalf("standing flow directory unavailable before mutation: %v", err)
			}
			mutation.apply(t)
			if prepare != nil {
				prepare(t)
			}
			requireChangedStandingColdStartRejected(t, opts)
		})
	}
	writeStandingCandidateFile(t, packagePath, string(basePackage))
}

func writeStandingCandidateFile(t testing.TB, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write standing candidate %s: %v", path, err)
	}
}

func requireChangedStandingColdStartRejected(t *testing.T, opts serveOptions) {
	t.Helper()
	process := startServeRuntimeTestProcess(t, opts)
	code, exited := process.waitForExit(15 * time.Second)
	if !exited {
		process.cleanup()
		t.Fatal("changed standing bundle did not fail startup")
	}
	process.recordStopped(code)
	output := process.outputString()
	if code == 0 || !strings.Contains(output, "outside candidate bundle set") || !strings.Contains(output, "serve the admitted bundle or perform an explicit reset/migration") {
		t.Fatalf("changed standing bundle exit/output = %d\n%s", code, output)
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
		case <-time.After(5 * time.Second):
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
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `name: standing-telegram-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: telegram-ingress
    flow: telegram-ingress
    mode: singleton
    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
  - id: telegram-chat
    flow: telegram-chat
    mode: template
`,
		"schema.yaml": "name: standing-telegram-proof\n",
		"policy.yaml": "{}\n",
		"tools.yaml":  "{}\n",
		"agents.yaml": "{}\n",
		"events.yaml": "{}\n",
		"nodes.yaml":  "{}\n",
		"flows/telegram-ingress/schema.yaml": `name: telegram-ingress
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: telegram_update
        event: inbound.telegram
        source: external
  outputs:
    events: []
`,
		"flows/telegram-ingress/types.yaml": "{}\n",
		"flows/telegram-ingress/entities.yaml": `telegram_service:
  service_id:
    type: text
    initial: standing
  active_chats:
    type: map[text]json
    initial: {}
`,
		"flows/telegram-ingress/events.yaml": "{}\n",
		"flows/telegram-ingress/nodes.yaml":  "{}\n",
		"flows/telegram-ingress/tools.yaml":  "{}\n",
		"flows/telegram-ingress/policy.yaml": "{}\n",
		"flows/telegram-ingress/agents.yaml": "{}\n",
		"flows/telegram-chat/schema.yaml": `name: telegram-chat
mode: template
instance:
  by: chat_id
  on_missing: create
  on_conflict: reuse
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: telegram_text_message
        event: inbound.telegram.text_message
        source: external
        resolution:
          mode: select-or-create
          instance_key: chat_id
        carries:
          chat_id:
            from: payload.chat_id
            type: text
  outputs:
    events: []
`,
		"flows/telegram-chat/types.yaml": "{}\n",
		"flows/telegram-chat/entities.yaml": `chat:
  chat_id:
    type: text
    indexed: true
    _unused_reason: populated from the normalized input resolution carry
  last_message:
    type: text
    initial: ""
`,
		"flows/telegram-chat/events.yaml": `telegram.reply_requested:
  chat_id: text
  text: text
`,
		"flows/telegram-chat/nodes.yaml": `telegram-responder:
  id: telegram-responder
  execution_type: system_node
  subscribes_to: [telegram.reply_requested]
  event_handlers:
    telegram.reply_requested:
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        input:
          chat_id:
            cel: payload.chat_id
          text:
            cel: payload.text
`,
		"flows/telegram-chat/tools.yaml": fmt.Sprintf(`telegram.send_message:
  category: provider_connector
  description: send Telegram messages
  handler_type: http
  effect_class: non_idempotent_write
  credentials: [telegram_bot_token]
  input_schema:
    type: object
    properties:
      chat_id: {type: string}
      text: {type: string}
    required: [chat_id, text]
  output_schema: {type: object}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: %s/bot{{credentials.telegram_bot_token}}/sendMessage
    body:
      chat_id: "{{input.chat_id}}"
      text: "{{input.text}}"
`, strings.TrimRight(telegramBaseURL, "/")),
		"flows/telegram-chat/policy.yaml": "{}\n",
		"flows/telegram-chat/agents.yaml": `phrase-bot:
  id: phrase-bot-{instance_id}
  role: phrase_bot
  prompt_ref: phrase-bot
  model: regular
  mode: session_per_entity
  subscriptions:
    - inbound.telegram.text_message
  emit_events:
    - telegram.reply_requested
`,
		"flows/telegram-chat/prompts/phrase-bot.md": `Reply to each Telegram message by emitting telegram.reply_requested with the same chat_id.
`,
	}
	for name, contents := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return root
}
