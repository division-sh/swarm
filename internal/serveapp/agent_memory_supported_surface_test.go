package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

const standingMemoryAsyncProofTimeout = 30 * time.Second

func TestCanonicalTelegramAgentSupportedSurfaceSQLitePostgres(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TelegramAgent)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runStandingTelegramMemorySupportedSurface(t, backend)
		})
	}
}

type standingLiveAnthropicRecorder struct {
	t testing.TB

	mu              sync.Mutex
	initialRequests [][]byte
}

func (r *standingLiveAnthropicRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/messages" {
		r.t.Errorf("Anthropic path = %q, want /v1/messages", req.URL.Path)
		http.Error(w, "unexpected path", http.StatusNotFound)
		return
	}
	if got := req.Header.Get("x-api-key"); got != "anthropic-key" {
		r.t.Errorf("Anthropic x-api-key = %q, want stored live credential", got)
		http.Error(w, "bad credential", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.t.Errorf("read Anthropic request: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if !bytes.Contains(body, []byte(`"emit_telegram_reply_requested"`)) {
		r.t.Errorf("Anthropic request omits exact reply tool: %s", body)
		http.Error(w, "missing reply tool", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	if bytes.Contains(body, []byte(`"kind":"tool_continuation"`)) {
		_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":8,"output_tokens":2},"content":[{"type":"text","text":"Telegram reply requested."}]}`))
		return
	}

	r.mu.Lock()
	ordinal := len(r.initialRequests) + 1
	r.initialRequests = append(r.initialRequests, append([]byte(nil), body...))
	r.mu.Unlock()

	wantCurrent := fmt.Sprintf("hello %d", 200+ordinal)
	if !bytes.Contains(body, []byte(wantCurrent)) {
		r.t.Errorf("Anthropic initial request %d omits current event %q: %s", ordinal, wantCurrent, body)
	}
	if ordinal == 2 && !bytes.Contains(body, []byte("hello 201")) {
		r.t.Errorf("Anthropic second request omits prior same-chat memory: %s", body)
	}
	reply := map[string]any{
		"model": "claude-test",
		"usage": map[string]any{"input_tokens": 12, "output_tokens": 4},
		"content": []any{map[string]any{
			"type": "tool_use",
			"id":   fmt.Sprintf("reply-%d", ordinal),
			"name": "emit_telegram_reply_requested",
			"input": map[string]any{
				"chat_id": "42",
				"text":    fmt.Sprintf("Live turn %d: %s", ordinal, wantCurrent),
			},
		}},
	}
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		r.t.Errorf("encode Anthropic response: %v", err)
	}
}

func (r *standingLiveAnthropicRecorder) waitForInitialCount(t testing.TB, want int) {
	t.Helper()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		got := len(r.initialRequests)
		r.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.mu.Lock()
	got := len(r.initialRequests)
	r.mu.Unlock()
	t.Fatalf("Anthropic initial requests = %d, want at least %d", got, want)
}

func TestCanonicalTelegramAgentExplicitLiveGraduation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)

	contractsRoot := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	removeExactCanonicalTelegramAgentMock(t, contractsRoot)
	configPath := filepath.Join(contractsRoot, "swarm.live.yaml")
	sqlitePath := filepath.Join(contractsRoot, ".swarm", "swarm.db")
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsRoot)

	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	t.Setenv("ANTHROPIC_API_KEY", "")
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{
		"webhook_signing.telegram": "telegram-secret",
		"telegram_bot_token":       "bot-token",
		"ANTHROPIC_API_KEY":        "anthropic-key",
	} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set live graduation credential %s: %v", key, err)
		}
	}

	var verifyOut, verifyErr bytes.Buffer
	if code := cliapp.Execute(context.Background(), contractsRoot, []string{
		"verify", "--config", configPath, "--contracts", contractsRoot,
	}, &verifyOut, &verifyErr, Run); code != 0 {
		t.Fatalf("explicit live graduation verify exit=%d\nstdout:\n%s\nstderr:\n%s", code, verifyOut.String(), verifyErr.String())
	}

	providerRecorder := &standingLiveAnthropicRecorder{t: t}
	provider := httptest.NewServer(providerRecorder)
	defer provider.Close()
	telegramCalls := make(chan map[string]any, 4)
	telegram := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/botbot-token/sendMessage" {
			t.Errorf("Telegram path = %q, want credential-bearing bot route", req.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode Telegram live request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		telegramCalls <- body
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer telegram.Close()
	redirectExternalHosts(t, map[string]string{
		"api.anthropic.com": provider.URL,
		"api.telegram.org":  telegram.URL,
	})

	opts := cliapp.ServeOptions{
		ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath),
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	process := startTelegramAgentServeRuntimeTestProcess(t, contractsRoot, opts)
	process.waitForReadyLine()
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	diagnostics := func() string {
		return process.outputString() + "\ndiagnostics: " + standingSQLiteDiagnostics(sqlitePath)
	}
	entityID := sendStandingTelegramUpdate(t, baseURL, 201, 42, diagnostics)
	if entityID == "" {
		t.Fatal("live graduation returned an empty standing entity")
	}
	if got := sendStandingTelegramUpdate(t, baseURL, 202, 42, diagnostics); got != entityID {
		t.Fatalf("live graduation second entity = %q, want same conversation owner %q", got, entityID)
	}
	providerRecorder.waitForInitialCount(t, 2)
	waitForStandingMemoryCompletion(t, "sqlite", sqlitePath, "live", 2)
	requireStandingLiveTelegramCalls(t, telegramCalls,
		"Live turn 1: hello 201",
		"Live turn 2: hello 202",
	)
	requireStandingPayloadOnlyTargetReadback(t, baseURL, bundleHash, "live", 1, []string{
		"Live turn 1: hello 201",
		"Live turn 2: hello 202",
	})
	if code := process.stop(); code != 0 {
		t.Fatalf("live graduation serve exit = %d\n%s", code, process.outputString())
	}
	sessions := loadStandingMemorySessions(t, "sqlite", sqlitePath)
	if len(sessions) != 1 {
		t.Fatalf("live graduation memory sessions = %#v, want one conversation owner", sessions)
	}
	for _, session := range sessions {
		if session.AgentID != "phrase-bot" || session.FlowTemplate != "telegram-chat" || session.TurnCount != 2 {
			t.Fatalf("live graduation memory session = %#v, want phrase-bot telegram-chat with two turns", session)
		}
	}
}

func startTelegramAgentServeRuntimeTestProcess(t *testing.T, repo string, opts cliapp.ServeOptions) *serveRuntimeTestProcess {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := &lockedBuffer{}
	opts.Output = out
	done := make(chan int, 1)
	process := &serveRuntimeTestProcess{
		t:      t,
		cancel: cancel,
		done:   done,
		out:    out,
	}
	priorRuntimeReadyHook := opts.TestRuntimeReadyHook
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
		process.mu.Lock()
		process.runtime = rt
		process.mu.Unlock()
		if priorRuntimeReadyHook != nil {
			priorRuntimeReadyHook(rt)
		}
	}
	t.Cleanup(process.cleanup)
	go func() {
		done <- Run(ctx, repo, opts)
	}()
	return process
}

func requireStandingLiveTelegramCalls(t testing.TB, calls <-chan map[string]any, wantTexts ...string) {
	t.Helper()
	for _, wantText := range wantTexts {
		select {
		case call := <-calls:
			if got := strings.TrimSpace(fmt.Sprint(call["chat_id"])); got != "42" {
				t.Fatalf("live Telegram chat_id = %q, want 42", got)
			}
			if got := strings.TrimSpace(fmt.Sprint(call["text"])); got != wantText {
				t.Fatalf("live Telegram text = %q, want %q", got, wantText)
			}
		case <-time.After(standingMemoryAsyncProofTimeout):
			t.Fatalf("timed out waiting for live Telegram call with text %q", wantText)
		}
	}
}

func removeExactCanonicalTelegramAgentMock(t testing.TB, contractsRoot string) {
	t.Helper()
	canonicalPath := filepath.Join(canonicalrouting.ExampleRoot(t, canonicalrouting.TelegramAgent), "bot", "flows", "telegram-chat", "agents.yaml")
	derivedPath := filepath.Join(contractsRoot, "bot", "flows", "telegram-chat", "agents.yaml")
	if got := countCanonicalTelegramAgentMocks(t, canonicalPath); got != 1 {
		t.Fatalf("checked canonical phrase-bot mock count = %d, want 1", got)
	}

	body, err := os.ReadFile(derivedPath)
	if err != nil {
		t.Fatalf("read copied Telegram agents: %v", err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatalf("parse copied Telegram agents: %v", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("copied Telegram agents root must be one mapping")
	}
	root := document.Content[0]
	var phraseBot *yaml.Node
	phraseBotCount := 0
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "phrase-bot" {
			phraseBotCount++
			phraseBot = root.Content[i+1]
		}
	}
	if phraseBotCount != 1 || phraseBot == nil || phraseBot.Kind != yaml.MappingNode {
		t.Fatalf("copied Telegram phrase-bot declarations = %d, want one mapping", phraseBotCount)
	}
	mockIndex := -1
	mockCount := 0
	var mock *yaml.Node
	for i := 0; i+1 < len(phraseBot.Content); i += 2 {
		if phraseBot.Content[i].Value == "mock" {
			mockCount++
			mockIndex = i
			mock = phraseBot.Content[i+1]
		}
	}
	if mockCount != 1 || mock == nil || mock.Kind != yaml.MappingNode || len(mock.Content) != 4 {
		t.Fatalf("copied Telegram phrase-bot mock shape = count:%d node:%#v, want one two-field mapping", mockCount, mock)
	}
	wantMock := map[string]string{"kind": "python", "module": "mocks/phrase-bot.py"}
	seenMock := map[string]string{}
	for i := 0; i+1 < len(mock.Content); i += 2 {
		if mock.Content[i].Kind != yaml.ScalarNode || mock.Content[i+1].Kind != yaml.ScalarNode {
			t.Fatalf("copied Telegram phrase-bot mock entry must be scalar: %#v", mock.Content[i:i+2])
		}
		seenMock[mock.Content[i].Value] = mock.Content[i+1].Value
	}
	if !equalStringValues(seenMock, wantMock) {
		t.Fatalf("copied Telegram phrase-bot mock = %#v, want exact %#v", seenMock, wantMock)
	}
	phraseBot.Content = append(append([]*yaml.Node(nil), phraseBot.Content[:mockIndex]...), phraseBot.Content[mockIndex+2:]...)
	updated, err := yaml.Marshal(&document)
	if err != nil {
		t.Fatalf("encode graduated Telegram agents: %v", err)
	}
	if err := os.WriteFile(derivedPath, updated, 0o644); err != nil {
		t.Fatalf("write graduated Telegram agents: %v", err)
	}
	if got := countCanonicalTelegramAgentMocks(t, canonicalPath); got != 1 {
		t.Fatalf("graduation mutated checked canonical mock count to %d, want 1", got)
	}
	if got := countCanonicalTelegramAgentMocks(t, derivedPath); got != 0 {
		t.Fatalf("graduated copied phrase-bot mock count = %d, want 0", got)
	}
}

func countCanonicalTelegramAgentMocks(t testing.TB, path string) int {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Telegram agents %s: %v", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatalf("parse Telegram agents %s: %v", path, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return 0
	}
	count := 0
	for i := 0; i+1 < len(document.Content[0].Content); i += 2 {
		if document.Content[0].Content[i].Value != "phrase-bot" || document.Content[0].Content[i+1].Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(document.Content[0].Content[i+1].Content); j += 2 {
			if document.Content[0].Content[i+1].Content[j].Value == "mock" {
				count++
			}
		}
	}
	return count
}

func equalStringValues(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func runStandingTelegramMemorySupportedSurface(t *testing.T, backend string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	contractsRoot := writeStandingMemoryServeFixture(t, "")
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsRoot)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := credentialStore.Set(context.Background(), "webhook_signing.telegram", "telegram-secret"); err != nil {
		t.Fatalf("set webhook signing credential: %v", err)
	}

	var storeLocation string
	var prepareRestart func()
	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	switch backend {
	case "sqlite":
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		storeLocation = filepath.Join(t.TempDir(), "memory.sqlite")
		opts.ConfigPath = writeStandingMockRuntimeConfig(t, "sqlite", storeLocation)
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
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			storetest.BootstrapPostgresRuntimeStore(t, runtimePG)
			return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(runtimePG), cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(workspace.Lookup, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		prepareRestart = openStore
		opts.ConfigPath = writeStandingMockRuntimeConfig(t, "postgres", "")
		opts.StoreMode = "postgres"
		opts.StoreModeSet = true
	default:
		t.Fatalf("unsupported backend %q", backend)
	}

	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	firstURL := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	firstDiagnostics := func() string {
		return first.outputString() + "\ndiagnostics: " + standingMemoryStoreDiagnostics(backend, storeLocation)
	}
	requireStandingTelegramSignatureRejection(t, firstURL)
	requireStandingTelegramEvidenceCounts(t, backend, storeLocation, standingTelegramEvidenceCounts{}, "rejected signatures")
	sendStandingTelegramUnmatchedUpdate(t, firstURL, 100)
	waitForStandingTelegramRawCount(t, backend, storeLocation, 1)
	requireStandingTelegramEvidenceCounts(t, backend, storeLocation, standingTelegramEvidenceCounts{Raw: 1}, "unmatched raw-only update")
	entity := sendStandingTelegramUpdate(t, firstURL, 101, 42, firstDiagnostics)
	waitForStandingMemoryCompletion(t, backend, storeLocation, "mock", 1)
	if got := sendStandingTelegramDuplicate(t, firstURL, 101, 42); got != entity {
		t.Fatalf("exact duplicate entity = %q, want %q", got, entity)
	}
	requireStandingMemoryAttemptCount(t, backend, storeLocation, "mock", 1, "same-process exact duplicate")
	if got := sendStandingTelegramUpdate(t, firstURL, 102, 42, firstDiagnostics); got != entity {
		t.Fatalf("A2 entity = %q, want A1 entity %q", got, entity)
	}
	if got := sendStandingTelegramUpdate(t, firstURL, 103, 84, firstDiagnostics); got != entity {
		t.Fatalf("B1 entity = %q, want standing entity %q", got, entity)
	}
	waitForStandingMemoryCompletion(t, backend, storeLocation, "mock", 3)
	requireStandingPayloadOnlyTargetReadback(t, firstURL, bundleHash, "mock", 2, []string{
		"Mock turn 1: hello 101",
		"Mock turn 2: hello 102",
		"Mock turn 1: hello 103",
	})
	before := loadStandingMemorySessions(t, backend, storeLocation)
	requireStandingMemorySessionShape(t, before)
	if code := first.stop(); code != 0 {
		t.Fatalf("first serve exit = %d", code)
	}

	if prepareRestart != nil {
		prepareRestart()
	}
	second := startServeRuntimeTestProcess(t, opts)
	second.waitForReadyLine()
	secondURL := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	if got := sendStandingTelegramDuplicate(t, secondURL, 101, 42); got != entity {
		t.Fatalf("post-restart exact duplicate entity = %q, want %q", got, entity)
	}
	requireStandingMemoryAttemptCount(t, backend, storeLocation, "mock", 3, "post-restart exact duplicate")
	if got := sendStandingTelegramUpdate(t, secondURL, 104, 42); got != entity {
		t.Fatalf("A3 entity = %q, want standing entity %q", got, entity)
	}
	waitForStandingMemoryCompletion(t, backend, storeLocation, "mock", 4)
	requireStandingPayloadOnlyTargetReadback(t, secondURL, bundleHash, "mock", 2, []string{
		"Mock turn 1: hello 101",
		"Mock turn 2: hello 102",
		"Mock turn 1: hello 103",
		"Mock turn 3: hello 104",
	})
	if code := second.stop(); code != 0 {
		t.Fatalf("second serve exit = %d", code)
	}
	after := loadStandingMemorySessions(t, backend, storeLocation)
	assertStandingMemorySessionContinuity(t, before, after)
}

type standingTelegramEvidenceCounts struct {
	Raw        int
	Normalized int
	Replies    int
	Activities int
}

func requireStandingTelegramSignatureRejection(t testing.TB, baseURL string) {
	t.Helper()
	body := []byte(`{"update_id":90,"message":{"message_id":90,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"must not publish"}}`)
	for _, tc := range []struct {
		name   string
		secret string
	}{
		{name: "missing"},
		{name: "invalid", secret: "wrong-secret"},
	} {
		req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new %s-signature Telegram request: %v", tc.name, err)
		}
		req.Header.Set("content-type", "application/json")
		if tc.secret != "" {
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", tc.secret)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("send %s-signature Telegram request: %v", tc.name, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s-signature Telegram status = %d, want %d", tc.name, resp.StatusCode, http.StatusUnauthorized)
		}
	}
}

func sendStandingTelegramUnmatchedUpdate(t testing.TB, baseURL string, updateID int) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"channel_post":{"message_id":%d,"chat":{"id":-100,"type":"channel"},"text":"raw only"}}`, updateID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new unmatched Telegram request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send unmatched Telegram request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read unmatched Telegram response: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unmatched Telegram status = %d body=%q, want %d", resp.StatusCode, strings.TrimSpace(string(responseBody)), http.StatusAccepted)
	}
}

func waitForStandingTelegramRawCount(t testing.TB, backend, location string, want int) {
	t.Helper()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		if got := loadStandingTelegramEvidenceCounts(t, backend, location).Raw; got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s raw Telegram events did not reach %d", backend, want)
}

func requireStandingTelegramEvidenceCounts(t testing.TB, backend, location string, want standingTelegramEvidenceCounts, label string) {
	t.Helper()
	time.Sleep(250 * time.Millisecond)
	if got := loadStandingTelegramEvidenceCounts(t, backend, location); got != want {
		t.Fatalf("%s %s evidence = %#v, want %#v", backend, label, got, want)
	}
}

func loadStandingTelegramEvidenceCounts(t testing.TB, backend, location string) standingTelegramEvidenceCounts {
	t.Helper()
	driver, dsn := "sqlite", location
	if backend == "postgres" {
		driver, dsn = "postgres", location
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s Telegram evidence store: %v", backend, err)
	}
	defer db.Close()
	var counts standingTelegramEvidenceCounts
	queries := []struct {
		label string
		query string
		out   *int
	}{
		{label: "raw events", query: `SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram'`, out: &counts.Raw},
		{label: "normalized events", query: `SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram.text_message'`, out: &counts.Normalized},
		{label: "reply events", query: `SELECT COUNT(*) FROM events WHERE event_name = 'telegram.reply_requested' OR event_name LIKE '%/telegram.reply_requested'`, out: &counts.Replies},
		{label: "Telegram activities", query: `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message'`, out: &counts.Activities},
	}
	for _, query := range queries {
		if err := db.QueryRow(query.query).Scan(query.out); err != nil {
			t.Fatalf("query %s %s: %v", backend, query.label, err)
		}
	}
	return counts
}

func sendStandingTelegramDuplicate(t testing.TB, baseURL string, updateID, chatID int) string {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":%d},"chat":{"id":%d,"type":"private"},"text":"hello %d"}}`, updateID, updateID, chatID, chatID, updateID))
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new duplicate webhook request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send duplicate webhook: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read duplicate webhook response: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode duplicate webhook response status=%d body=%q: %v", resp.StatusCode, strings.TrimSpace(string(raw)), err)
	}
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(fmt.Sprint(payload["status"])) != "duplicate" {
		t.Fatalf("duplicate webhook status=%d payload=%#v, want 200 duplicate", resp.StatusCode, payload)
	}
	return strings.TrimSpace(fmt.Sprint(payload["entity_id"]))
}

func requireStandingPayloadOnlyTargetReadback(t *testing.T, baseURL, bundleHash, executionMode string, wantOwners int, wantTexts []string) {
	t.Helper()
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/rpc"
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{
		"bundle_hash": bundleHash,
		"limit":       500,
	}, &runs)
	responder, err := runtimeidentity.AdmitExecutableNodeDeclaration("bot", "telegram-chat", "telegram-responder")
	if err != nil {
		t.Fatalf("admit package-backed responder identity: %v", err)
	}
	owners := map[string]string{}
	seen := 0
	texts := map[string]int{}
	for _, run := range runs.Runs {
		var result operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
			"filter": map[string]any{"run_id": run.RunID},
			"limit":  500,
		}, &result)
		for _, event := range result.Events {
			if !strings.HasSuffix(event.EventName, "/telegram.reply_requested") {
				continue
			}
			seen++
			if string(event.ExecutionMode) != executionMode {
				t.Fatalf("reply event %s execution mode = %q, want %q", event.EventID, event.ExecutionMode, executionMode)
			}
			texts[strings.TrimSpace(fmt.Sprint(event.Payload["text"]))]++
			if event.NoDelivery != nil || len(event.DeadLetters) != 0 {
				t.Fatalf("reply event %s settlement = no_delivery:%#v dead_letters:%#v", event.EventID, event.NoDelivery, event.DeadLetters)
			}
			matched := 0
			for _, delivery := range event.Deliveries {
				if delivery.SubscriberID != responder.Key() {
					continue
				}
				matched++
				target := delivery.Target
				if target.Kind != "existing_entity" || target.FlowID != "telegram-chat" || target.FlowInstance == "" || target.EntityID == "" ||
					target.EntityID != event.EntityID || delivery.Status != "delivered" || !delivery.Terminal {
					t.Fatalf("reply event %s responder delivery = %#v event_entity=%q, want terminal exact existing owner", event.EventID, delivery, event.EntityID)
				}
				if previous, ok := owners[target.FlowInstance]; ok && previous != target.EntityID {
					t.Fatalf("flow instance %q changed owner from %q to %q", target.FlowInstance, previous, target.EntityID)
				}
				owners[target.FlowInstance] = target.EntityID
			}
			if matched != 1 {
				t.Fatalf("reply event %s responder deliveries = %d, want exactly one %q: %#v", event.EventID, matched, responder.Key(), event.Deliveries)
			}
		}
	}
	wantTextCounts := map[string]int{}
	for _, value := range wantTexts {
		wantTextCounts[value]++
	}
	if seen != len(wantTexts) || len(owners) != wantOwners || !equalStringCounts(texts, wantTextCounts) {
		t.Fatalf("reply readback = events:%d owners:%#v texts:%#v, want %d events over %d exact chat owners with texts %#v", seen, owners, texts, len(wantTexts), wantOwners, wantTextCounts)
	}
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

func waitForStandingMemoryCompletion(t testing.TB, backend, location, executionMode string, wantAttempts int) {
	t.Helper()
	driver, dsn := "sqlite", location
	modeQuery := `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = ? AND status = 'succeeded'`
	if backend == "postgres" {
		driver, dsn = "postgres", location
		modeQuery = `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = $1 AND status = 'succeeded'`
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s completion store: %v", backend, err)
	}
	defer db.Close()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		var succeeded, unfinishedDeliveries int
		if err := db.QueryRow(modeQuery, executionMode).Scan(&succeeded); err != nil {
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

func requireStandingMemoryAttemptCount(t testing.TB, backend, location, executionMode string, want int, label string) {
	t.Helper()
	time.Sleep(250 * time.Millisecond)
	driver, dsn := "sqlite", location
	query := `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = ? AND status = 'succeeded'`
	if backend == "postgres" {
		driver, dsn = "postgres", location
		query = `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = $1 AND status = 'succeeded'`
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s activity store: %v", backend, err)
	}
	defer db.Close()
	var got int
	if err := db.QueryRow(query, executionMode).Scan(&got); err != nil {
		t.Fatalf("query %s %s activity attempts: %v", backend, label, err)
	}
	if got != want {
		t.Fatalf("%s %s Telegram attempts = %d, want %d", backend, label, got, want)
	}
}

func equalStringCounts(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func requireStandingMemorySessionShape(t testing.TB, sessions map[string]standingMemorySession) {
	t.Helper()
	counts := map[string]int{}
	for _, row := range sessions {
		counts[row.FlowTemplate]++
	}
	if len(sessions) != 2 || counts["telegram-chat"] != 2 {
		t.Fatalf("memory sessions = %#v, want two isolated Telegram chat owners", sessions)
	}
}

func assertStandingMemorySessionContinuity(t testing.TB, before, after map[string]standingMemorySession) {
	t.Helper()
	requireStandingMemorySessionShape(t, after)
	advancedTemplates := 0
	for key, prior := range before {
		current, ok := after[key]
		if !ok || current.SessionID != prior.SessionID {
			t.Fatalf("memory owner %q after restart = %#v, want session %q", key, current, prior.SessionID)
		}
		delta := current.TurnCount - prior.TurnCount
		switch delta {
		case 0:
		case 1:
			if current.FlowTemplate != "telegram-chat" {
				t.Fatalf("memory owner %q has unexpected flow template %q", key, current.FlowTemplate)
			}
			advancedTemplates++
		default:
			t.Fatalf("memory owner %q turn delta = %d, want unchanged or one post-restart provider turn", key, delta)
		}
	}
	if advancedTemplates != 1 {
		t.Fatalf("advanced Telegram memory owners = %d, want only chat A after restart", advancedTemplates)
	}
}

type telegramAgentRoundTripFunc func(*http.Request) (*http.Response, error)

func (f telegramAgentRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func redirectExternalHosts(t testing.TB, targets map[string]string) {
	t.Helper()
	parsed := make(map[string]*url.URL, len(targets))
	for host, target := range targets {
		targetURL, err := url.Parse(target)
		if err != nil {
			t.Fatalf("parse %s test endpoint: %v", host, err)
		}
		parsed[strings.ToLower(strings.TrimSpace(host))] = targetURL
	}
	base := http.DefaultTransport
	http.DefaultTransport = telegramAgentRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		targetURL := parsed[strings.ToLower(req.URL.Hostname())]
		if targetURL == nil {
			return base.RoundTrip(req)
		}
		clone := req.Clone(req.Context())
		redirected := *req.URL
		redirected.Scheme = targetURL.Scheme
		redirected.Host = targetURL.Host
		clone.URL = &redirected
		clone.Host = targetURL.Host
		return base.RoundTrip(clone)
	})
	t.Cleanup(func() { http.DefaultTransport = base })
}

func writeStandingMockRuntimeConfig(t *testing.T, backend, sqlitePath string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  execution_posture: mock_only",
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
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write standing mock runtime config: %v", err)
	}
	return path
}

func writeStandingMemoryServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	_ = telegramBaseURL
	return canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
}

func standingMemoryStoreDiagnostics(backend, location string) string {
	if backend == "postgres" {
		return standingPostgresDiagnostics(location)
	}
	return standingSQLiteDiagnostics(location)
}
