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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
)

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
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstEntity := sendStandingTelegramUpdate(t, firstURL, 101)
	secondEntity := sendStandingTelegramUpdate(t, firstURL, 102)
	if firstEntity == "" || firstEntity != secondEntity {
		t.Fatalf("delivery entities = %q and %q, want one standing entity", firstEntity, secondEntity)
	}
	requireStandingTelegramCalls(t, calls, 2, sqlitePath)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}

	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	restartedEntity := sendStandingTelegramUpdate(t, secondURL, 103)
	if restartedEntity != firstEntity {
		t.Fatalf("restart entity = %q, want %q", restartedEntity, firstEntity)
	}
	requireStandingTelegramCalls(t, calls, 1, sqlitePath)
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}
	mutateStandingFixtureBundleVersion(t, contractsRoot)
	requireChangedStandingColdStartRejected(t, opts)

	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite runtime store: %v", err)
	}
	defer sqliteStore.Close()
	var runs, instances, entities int
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE status = 'running' AND bundle_hash IS NOT NULL`).Scan(&runs); err != nil {
		t.Fatalf("count standing runs: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`, firstEntity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
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
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	entity := sendStandingTelegramUpdate(t, baseURL, 201)
	if got := sendStandingTelegramUpdate(t, baseURL, 202); got != entity {
		t.Fatalf("second entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, 2, "postgres:"+dsn)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore: %v", err)
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	if got := sendStandingTelegramUpdate(t, "http://"+serveRuntimeAPIListenerFromOutput(t, second.outputString()), 203); got != entity {
		t.Fatalf("restart entity = %q, want %q", got, entity)
	}
	requireStandingTelegramCalls(t, calls, 1, "postgres:"+dsn)
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}
	mutateStandingFixtureBundleVersion(t, contractsRoot)
	runtimePG, err = store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("reopen PostgresStore for changed-bundle probe: %v", err)
	}
	requireChangedStandingColdStartRejected(t, opts)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open Postgres: %v", err)
	}
	defer db.Close()
	var runs, instances, entities int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE status = 'running' AND bundle_hash IS NOT NULL`).Scan(&runs); err != nil {
		t.Fatalf("count standing runs: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`).Scan(&instances); err != nil {
		t.Fatalf("count standing instances: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE entity_id = $1::uuid`, entity).Scan(&entities); err != nil {
		t.Fatalf("count standing entities: %v", err)
	}
	if runs != 1 || instances != 1 || entities != 1 {
		t.Fatalf("standing authority counts = runs:%d instances:%d entities:%d, want 1/1/1", runs, instances, entities)
	}
}

func mutateStandingFixtureBundleVersion(t testing.TB, contractsRoot string) {
	t.Helper()
	path := filepath.Join(contractsRoot, "package.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read standing package for changed-bundle probe: %v", err)
	}
	changed := strings.Replace(string(body), `version: "1.0.0"`, `version: "1.0.1"`, 1)
	if changed == string(body) {
		t.Fatal("standing package version marker was not found")
	}
	if err := os.WriteFile(path, []byte(changed), 0o600); err != nil {
		t.Fatalf("write changed standing package: %v", err)
	}
}

func requireChangedStandingColdStartRejected(t *testing.T, opts serveOptions) {
	t.Helper()
	process := startServeRuntimeTestProcess(t, opts)
	code, exited := process.waitForExit(5 * time.Second)
	if !exited {
		process.cleanup()
		t.Fatal("changed standing bundle did not fail startup")
	}
	process.recordStopped(code)
	output := process.outputString()
	if code == 0 || !strings.Contains(output, "serve the admitted bundle or perform an explicit reset/migration") {
		t.Fatalf("changed standing bundle exit/output = %d\n%s", code, output)
	}
}

func sendStandingTelegramUpdate(t testing.TB, baseURL string, updateID int) string {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"chat":{"id":42},"text":"hello %d"}}`, updateID, updateID, updateID))
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

func requireStandingTelegramCalls(t testing.TB, calls <-chan map[string]any, count int, sqlitePath string) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case call := <-calls:
			if strings.TrimSpace(fmt.Sprint(call["chat_id"])) != "42" {
				t.Fatalf("Telegram chat_id = %v", call["chat_id"])
			}
		case <-time.After(5 * time.Second):
			diagnostics := "unavailable"
			if strings.HasPrefix(sqlitePath, "postgres:") {
				diagnostics = standingPostgresDiagnostics(strings.TrimPrefix(sqlitePath, "postgres:"))
			} else if strings.TrimSpace(sqlitePath) != "" {
				diagnostics = standingSQLiteDiagnostics(sqlitePath)
			}
			t.Fatalf("timed out waiting for Telegram reply %d/%d; diagnostics: %s", i+1, count, diagnostics)
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
  - id: telegram-chat
    flow: telegram-chat
    mode: singleton
    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`,
		"schema.yaml": "name: standing-telegram-proof\n",
		"policy.yaml": "{}\n",
		"tools.yaml":  "{}\n",
		"agents.yaml": "{}\n",
		"events.yaml": "{}\n",
		"nodes.yaml":  "{}\n",
		"flows/telegram-chat/schema.yaml": `name: telegram-chat
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
		"flows/telegram-chat/types.yaml": "{}\n",
		"flows/telegram-chat/entities.yaml": `chat_service:
  service_id:
    type: text
    initial: standing
  chats:
    type: map[text]json
    initial: {}
`,
		"flows/telegram-chat/events.yaml": `inbound.telegram:
  entity_id: text
  provider: text
  event_type: text
  provider_event_type: text
  provider_event_id: text
  provider_delivery_id: text
  headers: json
  received_at: text
`,
		"flows/telegram-chat/nodes.yaml": `telegram-responder:
  id: telegram-responder
  execution_type: system_node
  subscribes_to: [inbound.telegram]
  event_handlers:
    inbound.telegram:
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        input:
          chat_id:
            cel: payload.payload.message.chat.id
          text:
            cel: payload.payload.message.text
`,
		"flows/telegram-chat/tools.yaml": fmt.Sprintf(`telegram.send_message:
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
		"flows/telegram-chat/agents.yaml": "{}\n",
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
