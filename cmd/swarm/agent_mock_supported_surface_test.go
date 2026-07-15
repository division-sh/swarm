package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestMockAgentSupportedSurfaceSQLitePostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			elapsed := runMockAgentSupportedSurface(t, backend)
			t.Logf("mock served path timing: backend=%s elapsed=%s", backend, elapsed)
			if elapsed >= time.Minute {
				t.Fatalf("mock supported surface took %s, want less than 1m", elapsed)
			}
		})
	}
}

func TestForkChatSandboxBuildsCanonicalMockAdapter(t *testing.T) {
	harness := effecttest.New()
	runtime, err := buildForkChatSandboxLLMRuntime(
		&config.Config{LLM: config.LLMConfig{Backend: "mock"}},
		nil,
		toolgateway.Binding{},
		nil,
		harness,
		harness,
	)
	if err != nil {
		t.Fatalf("build fork-chat mock runtime: %v", err)
	}
	contract, err := runtimellm.RequireProviderContract(string(runtimeeffects.ExecutionModeMock), runtime)
	if err != nil {
		t.Fatalf("fork-chat mock provider contract: %v", err)
	}
	if contract.Provider != "mock" || contract.Transport != runtimellm.ProviderTransportInProcess {
		t.Fatalf("fork-chat mock provider contract = %#v", contract)
	}
}

func runMockAgentSupportedSurface(t *testing.T, backend string) time.Duration {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	var telegramCalls atomic.Int32
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		telegramCalls.Add(1)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer telegram.Close()

	contractsRoot := canonicalrouting.CopyStandingTelegramMockServe(t, telegram.URL)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsRoot)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{
		"webhook_signing.telegram": "telegram-secret",
		"telegram_bot_token":       "bound-but-fenced",
	} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	var location string
	var prepareRestart func()
	opts := serveOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case "sqlite":
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		location = filepath.Join(t.TempDir(), "mock.sqlite")
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, "sqlite", location)
		opts.StoreMode = "sqlite"
	case "postgres":
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		location = dsn
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
		oldWorkspace := configuredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			if _, bindErr := runtimePG.BindSchemaCapabilities(ctx); bindErr != nil {
				return storeBundle{}, bindErr
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
		prepareRestart = openStore
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, "postgres", "")
		opts.StoreMode = "postgres"
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}

	servedPathStarted := time.Now()
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	singleton := loadStandingMemoryTarget(t, backend, location, "memory-singleton")
	entityID := sendStandingTelegramUpdate(t, firstURL, 301, 42)
	publishStandingSingletonMemoryEvent(t, firstURL, bundleHash, singleton, "request review", "review")
	waitForMockAgentTurns(t, backend, location, 3)
	cardID := waitForMockDecisionCards(t, backend, location, 1)
	assertMockMailboxReadback(t, firstURL+"/v1/rpc", cardID)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d\n%s", code, first.outputString())
	}
	before := loadStandingMemorySessions(t, backend, location)
	requireMockSessionShape(t, before)

	if prepareRestart != nil {
		prepareRestart()
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	if got := sendStandingTelegramUpdate(t, secondURL, 302, 42); got != entityID {
		t.Fatalf("post-restart entity = %q, want %q", got, entityID)
	}
	publishStandingSingletonMemoryEvent(t, secondURL, bundleHash, singleton, "singleton three", "third")
	waitForMockAgentTurns(t, backend, location, 5)
	assertMockUsageReadback(t, secondURL+"/v1/rpc", "memory-bot")
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d\n%s", code, second.outputString())
	}
	after := loadStandingMemorySessions(t, backend, location)
	assertMockSessionContinuity(t, before, after)
	assertMockSupportedEvidence(t, backend, location, entityID)
	if got := telegramCalls.Load(); got != 0 {
		t.Fatalf("Telegram HTTP calls = %d, want zero before-launch mock fence", got)
	}
	return time.Since(servedPathStarted)
}

func writeMockAgentRuntimeConfig(t *testing.T, backend, sqlitePath string) string {
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
		"  backend: mock",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	source := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatalf("write mock runtime config: %v", err)
	}
	return path
}

func waitForMockAgentTurns(t testing.TB, backend, location string, want int) {
	t.Helper()
	db := openMockProofDB(t, backend, location)
	defer db.Close()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		var turns, unfinished int
		if err := db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE execution_mode = 'mock' AND (agent_id LIKE 'phrase-bot%' OR agent_id = 'memory-bot')`).Scan(&turns); err != nil {
			t.Fatalf("query %s mock turns: %v", backend, err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type = 'agent' AND (subscriber_id LIKE 'phrase-bot%' OR subscriber_id = 'memory-bot') AND status <> 'delivered'`).Scan(&unfinished); err != nil {
			t.Fatalf("query %s mock deliveries: %v", backend, err)
		}
		if turns >= want && unfinished == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	type turnFact struct {
		AgentID       string
		ExecutionMode string
		ParseOK       bool
		Request       string
		Response      string
		Failure       string
	}
	turnRows, err := db.Query(`SELECT agent_id, execution_mode, parse_ok, CAST(COALESCE(request_payload, '{}') AS TEXT), CAST(COALESCE(response_payload, '{}') AS TEXT), CAST(COALESCE(failure, '{}') AS TEXT) FROM agent_turns ORDER BY created_at`)
	if err != nil {
		t.Fatalf("%s mock path did not reach %d turns; inspect turns: %v", backend, want, err)
	}
	var turns []turnFact
	for turnRows.Next() {
		var fact turnFact
		if scanErr := turnRows.Scan(&fact.AgentID, &fact.ExecutionMode, &fact.ParseOK, &fact.Request, &fact.Response, &fact.Failure); scanErr != nil {
			_ = turnRows.Close()
			t.Fatalf("%s mock path did not reach %d turns; scan turns: %v", backend, want, scanErr)
		}
		turns = append(turns, fact)
	}
	_ = turnRows.Close()
	type deliveryFact struct {
		SubscriberID string
		Status       string
		ReasonCode   sql.NullString
	}
	deliveryRows, err := db.Query(`SELECT subscriber_id, status, reason_code FROM event_deliveries WHERE subscriber_type = 'agent' ORDER BY created_at`)
	if err != nil {
		t.Fatalf("%s mock path did not reach %d turns; turns=%#v inspect deliveries: %v", backend, want, turns, err)
	}
	var deliveries []deliveryFact
	for deliveryRows.Next() {
		var fact deliveryFact
		if scanErr := deliveryRows.Scan(&fact.SubscriberID, &fact.Status, &fact.ReasonCode); scanErr != nil {
			_ = deliveryRows.Close()
			t.Fatalf("%s mock path did not reach %d turns; turns=%#v scan deliveries: %v", backend, want, turns, scanErr)
		}
		deliveries = append(deliveries, fact)
	}
	_ = deliveryRows.Close()
	t.Fatalf("%s mock path did not reach %d turns; turns=%#v deliveries=%#v", backend, want, turns, deliveries)
}

func openMockProofDB(t testing.TB, backend, location string) *sql.DB {
	t.Helper()
	driver := "sqlite"
	if backend == "postgres" {
		driver = "postgres"
	}
	db, err := sql.Open(driver, location)
	if err != nil {
		t.Fatalf("open %s mock proof store: %v", backend, err)
	}
	return db
}

func waitForMockDecisionCards(t testing.TB, backend, location string, want int) string {
	t.Helper()
	db := openMockProofDB(t, backend, location)
	defer db.Close()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE execution_mode = 'mock'`).Scan(&count); err != nil {
			t.Fatalf("query %s mock decision cards: %v", backend, err)
		}
		if count >= want {
			var cardID string
			if err := db.QueryRow(`SELECT card_id FROM decision_cards WHERE execution_mode = 'mock' ORDER BY created_at LIMIT 1`).Scan(&cardID); err != nil {
				t.Fatalf("query %s mock decision-card identity: %v", backend, err)
			}
			return cardID
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s mock decision cards did not reach %d", backend, want)
	return ""
}

func requireMockSessionShape(t testing.TB, sessions map[string]standingMemorySession) {
	t.Helper()
	counts := map[string]int{}
	for _, session := range sessions {
		counts[session.FlowTemplate]++
	}
	if len(sessions) != 1 || counts["memory-singleton"] != 1 {
		t.Fatalf("mock sessions = %#v, want only the memory-enabled singleton owner", sessions)
	}
}

func assertMockSessionContinuity(t testing.TB, before, after map[string]standingMemorySession) {
	t.Helper()
	requireMockSessionShape(t, after)
	for flow, prior := range before {
		current, ok := after[flow]
		if !ok || current.SessionID != prior.SessionID || current.TurnCount <= prior.TurnCount {
			t.Fatalf("mock session %q after restart = %#v, want same session %q with advanced turns", flow, current, prior.SessionID)
		}
	}
}

func assertMockSupportedEvidence(t testing.TB, backend, location, entityID string) {
	t.Helper()
	db := openMockProofDB(t, backend, location)
	defer db.Close()
	assertMockCount(t, db, "turns", 5, `SELECT COUNT(*) FROM agent_turns WHERE execution_mode = 'mock' AND (agent_id LIKE 'phrase-bot%' OR agent_id = 'memory-bot')`)
	assertMockCount(t, db, "mock attempts", 5, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE adapter = 'mock_python' AND execution_mode = 'mock' AND state = 'settled'`)
	assertMockCount(t, db, "non-mock attempts", 0, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE execution_mode <> 'mock'`)
	assertMockCount(t, db, "Telegram activity attempts", 0, `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message'`)
	assertMockCount(t, db, "mock reply events", 2, `SELECT COUNT(*) FROM events WHERE event_name LIKE '%telegram.reply_requested' AND execution_mode = 'mock'`)
	assertMockCount(t, db, "mock decision cards", 2, `SELECT COUNT(*) FROM decision_cards WHERE execution_mode = 'mock'`)
	assertMockCount(t, db, "mock spend rows", 5, `SELECT COUNT(*) FROM spend_ledger WHERE execution_mode = 'mock'`)
	assertMockCount(t, db, "live spend rows", 0, `SELECT COUNT(*) FROM spend_ledger WHERE execution_mode = 'live'`)
	assertMockAtLeast(t, db, "mock author activity", 1, `SELECT COUNT(*) FROM author_activity_occurrences WHERE `+mockJSONTextExpression(backend, "projection", "execution_mode")+` = 'mock'`)
	assertMockAtLeast(t, db, "mock tool calls", 1, `SELECT COUNT(*) FROM agent_turns WHERE execution_mode = 'mock' AND CAST(tool_calls AS TEXT) LIKE '%read_memory_state%'`)
	var persistedEntityID string
	query := `SELECT entity_id FROM entity_state WHERE entity_id = ` + mockPlaceholder(backend, 1)
	if err := db.QueryRow(query, entityID).Scan(&persistedEntityID); err != nil || persistedEntityID != entityID {
		t.Fatalf("read %s mock entity identity = %q err=%v, want %q", backend, persistedEntityID, err, entityID)
	}
	var requestPayload string
	if err := db.QueryRow(`SELECT CAST(request_payload AS TEXT) FROM agent_turns WHERE agent_id = 'memory-bot' ORDER BY created_at DESC LIMIT 1`).Scan(&requestPayload); err != nil {
		t.Fatalf("read %s mock memory request: %v", backend, err)
	}
	for _, want := range []string{"request review", "singleton three"} {
		if !strings.Contains(requestPayload, want) {
			t.Fatalf("mock memory request missing %q: %s", want, requestPayload)
		}
	}
	var statelessRequest string
	if err := db.QueryRow(`SELECT CAST(request_payload AS TEXT) FROM agent_turns WHERE agent_id LIKE 'phrase-bot%' ORDER BY created_at DESC LIMIT 1`).Scan(&statelessRequest); err != nil {
		t.Fatalf("read %s stateless mock request: %v", backend, err)
	}
	if !strings.Contains(statelessRequest, "hello 302") || strings.Contains(statelessRequest, "hello 301") {
		t.Fatalf("memory:false mock request retained predecessor context: %s", statelessRequest)
	}
}

func assertMockMailboxReadback(t *testing.T, endpoint, cardID string) {
	t.Helper()
	var detail map[string]any
	requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
	card, ok := detail["decision_card"].(map[string]any)
	if detail["kind"] != "decision_card" || !ok || strings.TrimSpace(fmt.Sprint(card["execution_mode"])) != "mock" {
		t.Fatalf("mock mailbox.get = %#v, want decision_card with execution_mode mock", detail)
	}
}

func assertMockUsageReadback(t *testing.T, endpoint, agentID string) {
	t.Helper()
	var result map[string]any
	requireServedJSONRPCResult(t, endpoint, "agent.usage", map[string]any{"agent_id": agentID}, &result)
	breakdown, ok := result["breakdown"].([]any)
	if !ok || len(breakdown) == 0 {
		t.Fatalf("mock agent.usage = %#v, want non-empty breakdown", result)
	}
	for _, raw := range breakdown {
		row, ok := raw.(map[string]any)
		if !ok || strings.TrimSpace(fmt.Sprint(row["execution_mode"])) != "mock" {
			continue
		}
		cost := strings.TrimSpace(fmt.Sprint(row["cost_display"]))
		if strings.HasPrefix(cost, "~$") && strings.HasSuffix(cost, " (mock estimate)") {
			return
		}
	}
	t.Fatalf("mock agent.usage = %#v, want visibly labelled mock estimate", result)
}

func assertMockCount(t testing.TB, db *sql.DB, label string, want int, query string) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query %s: %v", label, err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", label, got, want)
	}
}

func assertMockAtLeast(t testing.TB, db *sql.DB, label string, want int, query string) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query %s: %v", label, err)
	}
	if got < want {
		t.Fatalf("%s = %d, want at least %d", label, got, want)
	}
}

func mockPlaceholder(backend string, ordinal int) string {
	if backend == "postgres" {
		return fmt.Sprintf("$%d::uuid", ordinal)
	}
	return "?"
}

func mockJSONTextExpression(backend, column, field string) string {
	if backend == "postgres" {
		return fmt.Sprintf("%s->>'%s'", column, field)
	}
	return fmt.Sprintf("json_extract(%s, '$.%s')", column, field)
}
