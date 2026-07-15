package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const standingMemoryAsyncProofTimeout = 30 * time.Second

type standingMemoryProviderMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type standingMemoryProviderRequest struct {
	Messages []standingMemoryProviderMessage `json:"messages"`
}

type standingMemoryProviderRecorder struct {
	t        testing.TB
	mu       sync.Mutex
	requests []standingMemoryProviderRequest
}

func (r *standingMemoryProviderRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/chat/completions" {
		r.t.Errorf("OpenAI-compatible path = %q, want /v1/chat/completions", req.URL.Path)
		http.Error(w, "unexpected path", http.StatusNotFound)
		return
	}
	if got := req.Header.Get("Authorization"); got != "Bearer compatible-key" {
		r.t.Errorf("OpenAI-compatible authorization = %q, want stored credential", got)
		http.Error(w, "bad credential", http.StatusUnauthorized)
		return
	}
	var recorded standingMemoryProviderRequest
	if err := json.NewDecoder(req.Body).Decode(&recorded); err != nil {
		r.t.Errorf("decode OpenAI-compatible request: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.requests = append(r.requests, recorded)
	r.mu.Unlock()

	w.Header().Set("content-type", "application/json")
	last := standingMemoryLastMessage(recorded.Messages)
	if standingMemoryRequestContains(recorded, "Remember each singleton ping") ||
		standingMemoryRequestContains(recorded, "Observe every raw Telegram update") ||
		last.Role == "tool" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model":   "gpt-compatible",
			"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "observed"}}},
			"usage":   map[string]any{"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
		})
		return
	}
	payload, err := standingMemoryLatestEventPayload(recorded.Messages)
	if err != nil {
		r.t.Errorf("resolve Telegram event payload from provider request: %v", err)
		http.Error(w, "missing event payload", http.StatusBadRequest)
		return
	}
	chatID := strings.TrimSpace(fmt.Sprint(payload["conversation_reference"]))
	text := strings.TrimSpace(fmt.Sprint(payload["text"]))
	arguments, _ := json.Marshal(map[string]any{"chat_id": chatID, "text": "Swarm heard: " + text})
	_ = json.NewEncoder(w).Encode(map[string]any{
		"model": "gpt-compatible",
		"choices": []any{map[string]any{"message": map[string]any{
			"role": "assistant",
			"tool_calls": []any{map[string]any{
				"id": "reply-" + chatID, "type": "function",
				"function": map[string]any{"name": "emit_telegram_reply_requested", "arguments": string(arguments)},
			}},
		}}},
		"usage": map[string]any{"prompt_tokens": 12, "completion_tokens": 4, "total_tokens": 16},
	})
}

func (r *standingMemoryProviderRecorder) snapshot() []standingMemoryProviderRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]standingMemoryProviderRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func (r *standingMemoryProviderRecorder) waitForCount(t testing.TB, want int) {
	t.Helper()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		if len(r.snapshot()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("OpenAI-compatible requests = %d, want at least %d", len(r.snapshot()), want)
}

func TestStandingTelegramMemorySupportedSurfaceSQLitePostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runStandingTelegramMemorySupportedSurface(t, backend)
		})
	}
}

func runStandingTelegramMemorySupportedSurface(t *testing.T, backend string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	recorder := &standingMemoryProviderRecorder{t: t}
	provider := httptest.NewServer(recorder)
	defer provider.Close()

	telegramCalls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode Telegram connector request: %v", err)
		}
		telegramCalls <- body
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer telegram.Close()

	contractsRoot := writeStandingMemoryServeFixture(t, telegram.URL)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsRoot)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{
		"webhook_signing.telegram":  "telegram-secret",
		"telegram_bot_token":        "bot-token",
		"OPENAI_COMPATIBLE_API_KEY": "compatible-key",
	} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	var storeLocation string
	var prepareRestart func()
	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case "sqlite":
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		storeLocation = filepath.Join(t.TempDir(), "memory.sqlite")
		opts.ConfigPath = writeStandingMemoryRuntimeConfig(t, "sqlite", storeLocation, provider.URL)
		opts.StoreMode = "sqlite"
	case "postgres":
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		storeLocation = dsn
		var runtimePG *store.PostgresStore
		openStore := func() {
			var openErr error
			runtimePG, openErr = store.NewPostgresStore(dsn)
			if openErr != nil {
				t.Fatalf("NewPostgresStore: %v", openErr)
			}
		}
		openStore()
		oldBuildStores := buildStoresForServe
		oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			if _, bindErr := runtimePG.BindSchemaCapabilities(ctx); bindErr != nil {
				return storeBundle{}, bindErr
			}
			return selectedPostgresStoreBundle(runtimePG, cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		prepareRestart = openStore
		opts.ConfigPath = writeStandingMemoryRuntimeConfig(t, "postgres", "", provider.URL)
		opts.StoreMode = "postgres"
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	singletonTarget := loadStandingMemoryTarget(t, backend, storeLocation, "memory-singleton")
	entity := sendStandingTelegramUpdate(t, firstURL, 101, 42)
	requireStandingTelegramCalls(t, telegramCalls, standingMemoryDiagnosticsLocation(backend, storeLocation), 42)
	if got := sendStandingTelegramUpdate(t, firstURL, 102, 42); got != entity {
		t.Fatalf("A2 entity = %q, want A1 entity %q", got, entity)
	}
	requireStandingTelegramCalls(t, telegramCalls, standingMemoryDiagnosticsLocation(backend, storeLocation), 42)
	if got := sendStandingTelegramUpdate(t, firstURL, 103, 84); got != entity {
		t.Fatalf("B1 entity = %q, want standing entity %q", got, entity)
	}
	requireStandingTelegramCalls(t, telegramCalls, standingMemoryDiagnosticsLocation(backend, storeLocation), 84)
	recorder.waitForCount(t, 3)
	publishStandingSingletonMemoryEvent(t, firstURL, bundleHash, singletonTarget, "singleton one", "first")
	recorder.waitForCount(t, 4)
	singletonSecond := publishStandingSingletonMemoryEvent(t, firstURL, bundleHash, singletonTarget, "singleton two", "second")
	if singletonSecond.RunID != singletonTarget.RunID || singletonSecond.NewRunCreated {
		t.Fatalf("singleton second publish = %#v, want existing run %s", singletonSecond, singletonTarget.RunID)
	}
	recorder.waitForCount(t, 5)
	waitForStandingMemoryCompletion(t, backend, storeLocation, 3)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}
	before := loadStandingMemorySessions(t, backend, storeLocation)
	requireStandingMemorySessionShape(t, before)

	if prepareRestart != nil {
		prepareRestart()
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	if got := sendStandingTelegramUpdate(t, secondURL, 104, 42); got != entity {
		t.Fatalf("A3 entity = %q, want standing entity %q", got, entity)
	}
	requireStandingMemoryTelegramCall(t, second, telegramCalls, standingMemoryDiagnosticsLocation(backend, storeLocation), 42)
	recorder.waitForCount(t, 6)
	singletonThird := publishStandingSingletonMemoryEvent(t, secondURL, bundleHash, singletonTarget, "singleton three", "third")
	if singletonThird.RunID != singletonTarget.RunID || singletonThird.NewRunCreated {
		t.Fatalf("singleton third publish = %#v, want recovered run %s", singletonThird, singletonTarget.RunID)
	}
	recorder.waitForCount(t, 7)
	waitForStandingMemoryCompletion(t, backend, storeLocation, 4)
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}
	after := loadStandingMemorySessions(t, backend, storeLocation)
	assertStandingMemorySessionContinuity(t, before, after)
	assertStandingMemoryProviderHistory(t, recorder.snapshot())
}

type standingMemorySession struct {
	SessionID    string
	AgentID      string
	FlowInstance string
	FlowTemplate string
	TurnCount    int
}

func loadStandingMemorySessions(t testing.TB, backend, location string) map[string]standingMemorySession {
	t.Helper()
	driver, dsn, query := "sqlite", location, `
		SELECT s.session_id, s.agent_id, s.flow_instance, COALESCE(fi.flow_template, ''), s.turn_count
		FROM agent_sessions s
		LEFT JOIN flow_instances fi ON fi.instance_id = s.flow_instance
		WHERE s.memory_enabled = 1
		ORDER BY s.flow_instance`
	if backend == "postgres" {
		driver, dsn, query = "postgres", location, `
			SELECT s.session_id::text, s.agent_id, s.flow_instance, COALESCE(fi.flow_template, ''), s.turn_count
			FROM agent_sessions s
			LEFT JOIN flow_instances fi ON fi.instance_id = s.flow_instance
			WHERE s.memory_enabled
			ORDER BY s.flow_instance`
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s memory store: %v", backend, err)
	}
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %s memory sessions: %v", backend, err)
	}
	defer rows.Close()
	out := map[string]standingMemorySession{}
	for rows.Next() {
		var row standingMemorySession
		if err := rows.Scan(&row.SessionID, &row.AgentID, &row.FlowInstance, &row.FlowTemplate, &row.TurnCount); err != nil {
			t.Fatalf("scan %s memory session: %v", backend, err)
		}
		out[row.FlowInstance] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s memory sessions: %v", backend, err)
	}
	return out
}

func waitForStandingMemoryCompletion(t testing.TB, backend, location string, wantAttempts int) {
	t.Helper()
	driver, dsn := "sqlite", location
	if backend == "postgres" {
		driver, dsn = "postgres", location
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s completion store: %v", backend, err)
	}
	defer db.Close()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		var succeeded, unfinishedDeliveries int
		if err := db.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND status = 'succeeded'`).Scan(&succeeded); err != nil {
			t.Fatalf("query %s completed Telegram attempts: %v", backend, err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE (subscriber_id LIKE 'phrase-bot%' OR subscriber_id = 'memory-bot') AND status <> 'delivered'`).Scan(&unfinishedDeliveries); err != nil {
			t.Fatalf("query %s unfinished agent deliveries: %v", backend, err)
		}
		if succeeded == wantAttempts && unfinishedDeliveries == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s supported path did not settle after %d Telegram attempts", backend, wantAttempts)
}

func requireStandingMemorySessionShape(t testing.TB, sessions map[string]standingMemorySession) {
	t.Helper()
	counts := map[string]int{}
	for _, row := range sessions {
		counts[row.FlowTemplate]++
	}
	if len(sessions) != 3 || counts["telegram-chat"] != 2 || counts["memory-singleton"] != 1 {
		t.Fatalf("memory sessions = %#v, want two isolated template owners and one singleton owner", sessions)
	}
}

func assertStandingMemorySessionContinuity(t testing.TB, before, after map[string]standingMemorySession) {
	t.Helper()
	requireStandingMemorySessionShape(t, after)
	advancedTemplates := 0
	advancedSingletons := 0
	for key, prior := range before {
		current, ok := after[key]
		if !ok || current.SessionID != prior.SessionID {
			t.Fatalf("memory owner %q after restart = %#v, want session %q", key, current, prior.SessionID)
		}
		delta := current.TurnCount - prior.TurnCount
		switch delta {
		case 0:
		case 1:
			switch current.FlowTemplate {
			case "telegram-chat":
				advancedTemplates++
			case "memory-singleton":
				advancedSingletons++
			default:
				t.Fatalf("memory owner %q has unexpected flow template %q", key, current.FlowTemplate)
			}
		default:
			t.Fatalf("memory owner %q turn delta = %d, want unchanged or one post-restart provider turn", key, delta)
		}
	}
	if advancedTemplates != 1 || advancedSingletons != 1 {
		t.Fatalf("advanced memory owners = template:%d singleton:%d, want A3 and singleton third only", advancedTemplates, advancedSingletons)
	}
}

func assertStandingMemoryProviderHistory(t testing.TB, requests []standingMemoryProviderRequest) {
	t.Helper()
	if len(requests) != 7 {
		t.Fatalf("OpenAI-compatible requests = %d, want 7 exact provider turns", len(requests))
	}
	a2 := requireStandingMemoryUserRequest(t, requests, "Reply to each Telegram message", "hello 102")
	assertStandingMemoryContains(t, a2, []string{"hello 101", "hello 102"}, nil)
	b1 := requireStandingMemoryUserRequest(t, requests, "Reply to each Telegram message", "hello 103")
	assertStandingMemoryContains(t, b1, []string{"hello 103"}, []string{"hello 101", "hello 102"})
	a3 := requireStandingMemoryUserRequest(t, requests, "Reply to each Telegram message", "hello 104")
	assertStandingMemoryContains(t, a3, []string{"hello 101", "hello 102", "hello 104"}, []string{"hello 103"})
	singletonSecond := requireStandingMemoryUserRequest(t, requests, "Remember each singleton ping", "singleton two")
	assertStandingMemoryContains(t, singletonSecond, []string{"singleton one", "singleton two"}, []string{"hello 101", "hello 103"})
	singletonThird := requireStandingMemoryUserRequest(t, requests, "Remember each singleton ping", "singleton three")
	assertStandingMemoryContains(t, singletonThird, []string{"singleton one", "singleton two", "singleton three"}, []string{"hello 101", "hello 103"})
}

type standingMemoryTarget struct {
	RunID        string
	FlowInstance string
	EntityID     string
}

func loadStandingMemoryTarget(t testing.TB, backend, location, flowTemplate string) standingMemoryTarget {
	t.Helper()
	driver, dsn, query := "sqlite", location, `
		SELECT es.run_id, es.flow_instance, es.entity_id
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE fi.flow_template = ? AND fi.status = 'active' AND fi.terminated_at IS NULL`
	if backend == "postgres" {
		driver, dsn, query = "postgres", location, `
			SELECT es.run_id::text, es.flow_instance, es.entity_id::text
			FROM entity_state es
			JOIN flow_instances fi ON fi.instance_id = es.flow_instance
			WHERE fi.flow_template = $1 AND fi.status = 'active' AND fi.terminated_at IS NULL`
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s standing target store: %v", backend, err)
	}
	defer db.Close()
	var target standingMemoryTarget
	if err := db.QueryRow(query, flowTemplate).Scan(&target.RunID, &target.FlowInstance, &target.EntityID); err != nil {
		t.Fatalf("load %s standing target %s: %v", backend, flowTemplate, err)
	}
	return target
}

func publishStandingSingletonMemoryEvent(t *testing.T, baseURL, bundleHash string, target standingMemoryTarget, text, suffix string) servedEventPublishRPCResult {
	t.Helper()
	params := map[string]any{
		"event_name":      "memory-singleton/memory.ping",
		"payload":         map[string]any{"text": text},
		"idempotency_key": "memory-singleton-" + suffix + "-" + uuid.NewString(),
		"bundle_hash":     bundleHash,
		"run_id":          target.RunID,
		"target": map[string]any{
			"flow_instance": target.FlowInstance,
			"entity_id":     target.EntityID,
		},
	}
	result := requireServedEventPublishRPCResult(t, strings.TrimRight(baseURL, "/")+"/v1/rpc", params)
	if result.EventID == "" || result.RunID != target.RunID || result.NewRunCreated {
		t.Fatalf("singleton event.publish result = %#v, want existing standing run %s", result, target.RunID)
	}
	assertServedEventPublishDeliveriesContainStatus(t, result.Deliveries, "agent", "memory-bot", "pending", "in_progress", "delivered")
	return result
}

func requireStandingMemoryUserRequest(t testing.TB, requests []standingMemoryProviderRequest, systemMarker, userMarker string) standingMemoryProviderRequest {
	t.Helper()
	for i := len(requests) - 1; i >= 0; i-- {
		request := requests[i]
		last := standingMemoryLastMessage(request.Messages)
		if last.Role == "user" && strings.Contains(last.Content, userMarker) && standingMemoryRequestContains(request, systemMarker) {
			return request
		}
	}
	t.Fatalf("provider request missing system=%q user=%q", systemMarker, userMarker)
	return standingMemoryProviderRequest{}
}

func assertStandingMemoryContains(t testing.TB, request standingMemoryProviderRequest, includes, excludes []string) {
	t.Helper()
	raw, _ := json.Marshal(request.Messages)
	text := string(raw)
	for _, want := range includes {
		if !strings.Contains(text, want) {
			t.Fatalf("provider history missing %q: %s", want, text)
		}
	}
	for _, forbidden := range excludes {
		if strings.Contains(text, forbidden) {
			t.Fatalf("provider history crossed memory owner with %q: %s", forbidden, text)
		}
	}
}

func standingMemoryLastMessage(messages []standingMemoryProviderMessage) standingMemoryProviderMessage {
	if len(messages) == 0 {
		return standingMemoryProviderMessage{}
	}
	return messages[len(messages)-1]
}

func standingMemoryRequestContains(request standingMemoryProviderRequest, marker string) bool {
	for _, message := range request.Messages {
		if strings.Contains(message.Content, marker) {
			return true
		}
	}
	return false
}

func standingMemoryLatestEventPayload(messages []standingMemoryProviderMessage) (map[string]any, error) {
	const prefix = "- payload: "
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		start := strings.Index(messages[i].Content, prefix)
		if start < 0 {
			continue
		}
		raw := messages[i].Content[start+len(prefix):]
		if end := strings.IndexByte(raw, '\n'); end >= 0 {
			raw = raw[:end]
		}
		var payload map[string]any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	return nil, fmt.Errorf("no event payload found")
}

func writeStandingMemoryRuntimeConfig(t *testing.T, backend, sqlitePath, providerURL string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: true",
		"workspace:",
		"  data_source: " + t.TempDir(),
		"store:",
		"  backend: " + backend,
	}
	if sqlitePath != "" {
		lines = append(lines, "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines,
		"llm:",
		"  backend: openai_compatible",
		"  openai_compatible:",
		"    base_url: "+providerURL,
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	text := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatalf("write standing memory runtime config: %v", err)
	}
	return path
}

func writeStandingMemoryServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	return canonicalrouting.CopyStandingTelegramMemoryServe(t, telegramBaseURL)
}

func standingMemoryDiagnosticsLocation(backend, location string) string {
	if backend == "postgres" {
		return "postgres:" + location
	}
	return location
}

func requireStandingMemoryTelegramCall(t testing.TB, process *serveRuntimeTestProcess, calls <-chan map[string]any, storeLocation string, chatID int) {
	t.Helper()
	select {
	case call := <-calls:
		if got := strings.TrimSpace(fmt.Sprint(call["chat_id"])); got != fmt.Sprint(chatID) {
			t.Fatalf("Telegram chat_id = %v, want %d", call["chat_id"], chatID)
		}
	case <-time.After(standingMemoryAsyncProofTimeout):
		diagnostics := standingSQLiteDiagnostics(storeLocation)
		if strings.HasPrefix(storeLocation, "postgres:") {
			diagnostics = standingPostgresDiagnostics(strings.TrimPrefix(storeLocation, "postgres:"))
		}
		t.Fatalf("timed out waiting for post-restart Telegram reply; recovery mailbox: %s; serve output:\n%s\ndiagnostics: %s", standingMemoryRecoveryMailbox(storeLocation), process.outputString(), diagnostics)
	}
}

func standingMemoryRecoveryMailbox(storeLocation string) string {
	driver, dsn, query := "sqlite", storeLocation, `SELECT COALESCE(summary, ''), COALESCE(payload, '') FROM mailbox ORDER BY created_at DESC LIMIT 1`
	if strings.HasPrefix(storeLocation, "postgres:") {
		driver, dsn = "postgres", strings.TrimPrefix(storeLocation, "postgres:")
		query = `SELECT COALESCE(summary, ''), COALESCE(payload::text, '') FROM mailbox ORDER BY created_at DESC LIMIT 1`
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err.Error()
	}
	defer db.Close()
	var summary, payload string
	if err := db.QueryRow(query).Scan(&summary, &payload); err != nil {
		return err.Error()
	}
	return summary + " " + payload
}
