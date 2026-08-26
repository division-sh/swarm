package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

var channelOnboardingChallengePattern = regexp.MustCompile(`SWARM-[A-Z2-7]{16}`)

type channelOnboardingTelegramProvider struct {
	mu            sync.Mutex
	callbackURL   string
	signingSecret string
	registrations int
	deliveries    []map[string]any
}

func (p *channelOnboardingTelegramProvider) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		_, _ = w.Write([]byte(`{"ok":true,"result":{"id":420079}}`))
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.callbackURL = strings.TrimSpace(fmt.Sprint(payload["url"]))
		p.signingSecret = strings.TrimSpace(fmt.Sprint(payload["secret_token"]))
		p.registrations++
		p.mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
		p.mu.Lock()
		callbackURL := p.callbackURL
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"url": callbackURL}})
	case strings.HasSuffix(request.URL.Path, "/sendMessage"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.deliveries = append(p.deliveries, payload)
		messageID := len(p.deliveries)
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": messageID}})
	default:
		http.Error(w, `{"ok":false}`, http.StatusNotFound)
	}
}

func (p *channelOnboardingTelegramProvider) registration() (string, string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.callbackURL, p.signingSecret, p.registrations
}

func (p *channelOnboardingTelegramProvider) delivery(index int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.deliveries) {
		return nil
	}
	result := make(map[string]any, len(p.deliveries[index]))
	for key, value := range p.deliveries[index] {
		result[key] = value
	}
	return result
}

func TestChannelConnectTelegramFirstUserJourney(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingRetryLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runChannelConnectTelegramFirstUserJourney)
}

func runChannelConnectTelegramFirstUserJourney(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	started := time.Now()
	isolateCLIAPIConfigEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	provider := &channelOnboardingTelegramProvider{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	disableChannelOnboardingBusinessConsumers(t, contractsRoot)
	publicListen := reserveChannelOnboardingListenAddress(t)
	redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + publicListen})

	var db *sql.DB
	var resetSelectedStore func()
	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		PublicWebhookBaseURL: "https://hooks.channel-onboarding.test", PublicWebhookListen: publicListen,
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestLLMRuntime: telegramPhraseBotLLMRuntime{},
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "channel-onboarding.sqlite")
		oldBuildStores := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				db = selectedStoreDatabaseForTest(t, stores)
			}
			return stores, err
		}
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil)
		opts.StoreMode = "sqlite"
		resetSelectedStore = func() {
			if err := os.Remove(sqlitePath); err != nil && !os.IsNotExist(err) {
				t.Fatalf("reset SQLite channel selected store: %v", err)
			}
		}
	case servedparity.BackendExplicitPostgres:
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		oldBuildStores := buildStoresForServe
		oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		buildStoresForServe = func(_ context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return nil, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			db = storetest.DatabaseForTest(pg)
			return openSelectedPostgresOwner(t, dsn, db, cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(workspace.Lookup, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "postgres", "", nil)
		opts.StoreMode = "postgres"
		resetSelectedStore = func() {
			resetStore, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("open PostgreSQL channel selected store for reset: %v", err)
			}
			defer resetStore.Close()
			resetDB := storetest.DatabaseForTest(resetStore)
			if _, err := resetDB.ExecContext(context.Background(), `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
				t.Fatalf("reset PostgreSQL channel selected store: %v", err)
			}
		}
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)

	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())

	direct := runChannelOnboardingCLIJourney(t, opts.ConfigPath, endpoint, provider, "connect", "bot-token", 1001, "private", 0)
	if direct.Identity.ConversationScope != "direct" || direct.Readiness == nil || !direct.Readiness.Ready {
		t.Fatalf("%s direct channel readback = %#v", backend, direct)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" direct", direct)
	reconnected := runChannelOnboardingReconnectJourney(t, opts.ConfigPath, endpoint, provider, 1, "")
	assertChannelOnboardingIdentityPreserved(t, string(backend)+" direct reconnect", direct, reconnected)
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" direct reconnect", reconnected)
	if reconnected.Activation == nil || direct.Activation == nil || reconnected.Activation.Revision <= direct.Activation.Revision {
		t.Fatalf("%s reconnect activation revisions = %#v/%#v", backend, direct.Activation, reconnected.Activation)
	}
	if reconnected.Readiness.ActivationGeneration == direct.Readiness.ActivationGeneration {
		t.Fatalf("%s reconnect retained predecessor activation generation %q", backend, direct.Readiness.ActivationGeneration)
	}
	shared := runChannelOnboardingCLIJourney(t, opts.ConfigPath, endpoint, provider, "rebind", "", -2001, "group", 2)
	if shared.Identity.ConversationScope != "shared" || shared.Readiness == nil || !shared.Readiness.Ready {
		t.Fatalf("%s shared channel readback = %#v", backend, shared)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" shared", shared)
	if shared.Identity.BindingRevision <= direct.Identity.BindingRevision || shared.Activation == nil || shared.Activation.Revision <= reconnected.Activation.Revision {
		t.Fatalf("%s rebind revisions direct/reconnected/shared = %#v/%#v/%#v", backend, direct, reconnected, shared)
	}
	if shared.Readiness.ActivationGeneration == reconnected.Readiness.ActivationGeneration {
		t.Fatalf("%s rebind retained predecessor activation generation %q", backend, reconnected.Readiness.ActivationGeneration)
	}
	runChannelOnboardingUnbindJourney(t, opts.ConfigPath, endpoint, shared.Identity.Interface.Selector)
	unbound := readChannelOnboardingRow(t, opts.ConfigPath, endpoint, "unbound")
	if unbound.Identity.ProofID != shared.Identity.ProofID || unbound.Identity.ProofRevision != shared.Identity.ProofRevision || unbound.Identity.ProofStatus != "active" {
		t.Fatalf("%s unbound proof readback = %#v, want retained active proof %#v", backend, unbound.Identity, shared.Identity)
	}
	assertChannelOnboardingCredentialStoreEmpty(t, credentialPath, string(backend)+" unbind")
	if code := process.stop(); code != 0 {
		t.Fatalf("%s pre-reset serve exit = %d", backend, code)
	}
	resetSelectedStore()
	process = startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	restartedIdentity := readCurrentChannelOnboardingJourney(t, opts.ConfigPath, endpoint)
	if restartedIdentity.Identity.ConversationScope != shared.Identity.ConversationScope || restartedIdentity.Activation != nil || restartedIdentity.Readiness != nil ||
		restartedIdentity.Recovery == nil || restartedIdentity.Recovery.Reason != "activation_not_current" || restartedIdentity.Recovery.Provider != "telegram" ||
		len(restartedIdentity.Recovery.Commands) != 1 || restartedIdentity.Recovery.Commands[0] != "swarm channel reconnect telegram" {
		t.Fatalf("%s restarted reset readback = %#v, want proof-restored identity without activation/readiness", backend, restartedIdentity)
	}
	listOut, listErr := &lockedBuffer{}, &lockedBuffer{}
	if code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", endpoint}, listOut, listErr, nil); code != 0 {
		t.Fatalf("%s reset channel list exited %d: %s", backend, code, listErr.String())
	}
	if !strings.Contains(listOut.String(), "channel telegram: identity verified, activation lost with store - run swarm channel reconnect telegram") {
		t.Fatalf("%s reset channel list lacks reconnect teaching:\n%s", backend, listOut.String())
	}
	recovered := runChannelOnboardingReconnectJourney(t, opts.ConfigPath, endpoint, provider, 3, "replacement-bot-token")
	if recovered.Identity.AccountReference != shared.Identity.AccountReference || recovered.Identity.ConversationRef != shared.Identity.ConversationRef || recovered.Identity.ConversationScope != shared.Identity.ConversationScope {
		t.Fatalf("%s reset reconnect changed external identity: before=%#v after=%#v", backend, shared.Identity, recovered.Identity)
	}
	if recovered.Readiness == nil || !recovered.Readiness.Ready || recovered.Activation == nil {
		t.Fatalf("%s reset reconnect readback = %#v", backend, recovered)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" reset reconnect", recovered)
	if recovered.Readiness.ActivationGeneration == shared.Readiness.ActivationGeneration {
		t.Fatalf("%s reset reconnect retained unavailable predecessor activation generation %q", backend, shared.Readiness.ActivationGeneration)
	}
	runChannelOnboardingUnbindJourney(t, opts.ConfigPath, endpoint, recovered.Identity.Interface.Selector)
	assertChannelOnboardingCredentialStoreEmpty(t, credentialPath, string(backend)+" recovered unbind")
	runChannelOnboardingProofRevokeJourney(t, opts.ConfigPath, endpoint, recovered.Identity.Interface.Selector)
	revoked := readChannelOnboardingRow(t, opts.ConfigPath, endpoint, "unbound")
	if revoked.Identity.ProofStatus != "revoked" || revoked.Identity.ProofID != recovered.Identity.ProofID || revoked.Identity.ProofRevision <= recovered.Identity.ProofRevision {
		t.Fatalf("%s revoked proof readback = %#v, want retained revoked proof identity %#v", backend, revoked.Identity, recovered.Identity)
	}
	if db == nil {
		t.Fatalf("%s selected store database was not captured", backend)
	}
	for _, scenario := range scenariosForChannelOnboardingJourney() {
		servedparity.AssertSettlementPostconditions(t, scenario, servedparity.SettlementCounts{})
	}
	if elapsed := time.Since(started); elapsed >= time.Minute {
		t.Fatalf("%s first-user channel journey took %s, want under 60s", backend, elapsed)
	}
}

type channelOnboardingJourneyReadback struct {
	Identity struct {
		Interface struct {
			Selector string `json:"selector"`
		} `json:"interface"`
		Status            string `json:"status"`
		BindingRevision   int64  `json:"binding_revision"`
		AccountReference  string `json:"account_reference"`
		ConversationRef   string `json:"conversation_reference"`
		ConversationScope string `json:"conversation_scope"`
		ProofID           string `json:"proof_id"`
		ProofRevision     int64  `json:"proof_revision"`
		ProofStatus       string `json:"proof_status"`
	} `json:"identity"`
	Activation *struct {
		Revision int64 `json:"revision"`
	} `json:"activation"`
	Readiness *struct {
		Ready                bool   `json:"ready"`
		Reason               string `json:"reason"`
		ActivationGeneration string `json:"activation_generation"`
	} `json:"readiness"`
	Recovery *struct {
		Reason   string   `json:"reason"`
		Provider string   `json:"provider"`
		Commands []string `json:"commands"`
	} `json:"recovery"`
}

func assertChannelOnboardingReadinessGeneration(t *testing.T, label string, readback channelOnboardingJourneyReadback) {
	t.Helper()
	if readback.Readiness == nil || readback.Readiness.ActivationGeneration == "" {
		t.Fatalf("%s readiness lacks exact activation generation: %#v", label, readback.Readiness)
	}
}

func runChannelOnboardingReconnectJourney(t *testing.T, configPath, endpoint string, provider *channelOnboardingTelegramProvider, deliveryIndex int, credential string) channelOnboardingJourneyReadback {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	args := []string{"--config", configPath, "channel", "reconnect", "telegram", "--yes", "--api-server", endpoint}
	if credential != "" {
		args = append(args, "--credential-stdin")
		priorStdin := os.Stdin
		input, err := os.CreateTemp(t.TempDir(), "channel-reconnect-input-*")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			os.Stdin = priorStdin
			_ = input.Close()
		}()
		if _, err := input.WriteString(credential + "\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := input.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		os.Stdin = input
	}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), args, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel reconnect exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	surface := stdout.String() + "\n" + stderr.String()
	if !strings.Contains(surface, "READY") || channelOnboardingChallengePattern.MatchString(surface) {
		t.Fatalf("channel reconnect readiness/ceremony contract violated\n%s", surface)
	}
	delivery := waitChannelOnboardingDelivery(t, provider, deliveryIndex)
	if delivery["text"] != "Swarm channel connected." {
		t.Fatalf("channel reconnect confirmation = %#v", delivery)
	}
	return readCurrentChannelOnboardingJourney(t, configPath, endpoint)
}

func runChannelOnboardingUnbindJourney(t *testing.T, configPath, endpoint, selector string) {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "unbind", selector, "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel unbind exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func runChannelOnboardingProofRevokeJourney(t *testing.T, configPath, endpoint, selector string) {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "revoke-proof", selector, "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel proof revoke exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func assertChannelOnboardingCredentialStoreEmpty(t *testing.T, credentialPath, label string) {
	t.Helper()
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("%s open credential store: %v", label, err)
	}
	keys, err := credentialStore.List(context.Background())
	if err != nil {
		t.Fatalf("%s list credential store: %v", label, err)
	}
	if len(keys) != 0 {
		t.Fatalf("%s retained channel credentials: %v", label, keys)
	}
}

func assertChannelOnboardingIdentityPreserved(t *testing.T, label string, before, after channelOnboardingJourneyReadback) {
	t.Helper()
	if after.Identity.BindingRevision != before.Identity.BindingRevision ||
		after.Identity.AccountReference != before.Identity.AccountReference ||
		after.Identity.ConversationRef != before.Identity.ConversationRef ||
		after.Identity.ConversationScope != before.Identity.ConversationScope ||
		after.Identity.ProofID != before.Identity.ProofID ||
		after.Identity.ProofRevision != before.Identity.ProofRevision {
		t.Fatalf("%s changed identity: before=%#v after=%#v", label, before.Identity, after.Identity)
	}
}

func runChannelOnboardingCLIJourney(t *testing.T, configPath, endpoint string, provider *channelOnboardingTelegramProvider, verb, credential string, chatID int64, chatType string, deliveryIndex int) channelOnboardingJourneyReadback {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	args := []string{"--config", configPath, "channel", verb, "telegram", "--yes", "--api-server", endpoint}

	priorStdin := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-onboarding-input-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := input.WriteString(credential + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	t.Cleanup(func() {
		os.Stdin = priorStdin
		_ = input.Close()
	})
	go func() {
		done <- cliapp.Execute(context.Background(), cliapp.RepoRoot(), args, stdout, stderr, nil)
	}()

	challenge := waitChannelOnboardingChallenge(t, stdout, stderr, done)
	callbackURL, signingSecret := waitChannelOnboardingRegistration(t, provider, stdout, stderr, done)
	requestBody, err := json.Marshal(map[string]any{
		"update_id": time.Now().UnixNano(),
		"message": map[string]any{
			"message_id": deliveryIndex + 1,
			"from":       map[string]any{"id": 7000 + deliveryIndex, "username": fmt.Sprintf("operator_%d", deliveryIndex)},
			"chat":       map[string]any{"id": chatID, "type": chatType},
			"text":       challenge,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, callbackURL, strings.NewReader(string(requestBody)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signingSecret)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("publish %s Telegram claim: %v", chatType, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s claim admission: %v", chatType, err)
	}
	var admission map[string]any
	if err := json.Unmarshal(responseBody, &admission); err != nil {
		t.Fatalf("decode %s claim admission status/body %d/%q: %v", chatType, response.StatusCode, responseBody, err)
	}
	eventIDs, eventIDsOK := admission["event_ids"].([]any)
	eventNames, eventNamesOK := admission["event_names"].([]any)
	if response.StatusCode != http.StatusAccepted || admission["operator_channel_claim_disposition"] != "consumed_by_binding" || !eventIDsOK || len(eventIDs) != 0 || !eventNamesOK || len(eventNames) != 0 {
		t.Fatalf("%s claim admission status/body = %d/%#v", chatType, response.StatusCode, admission)
	}

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("channel %s exited %d\nstdout:\n%s\nstderr:\n%s", verb, code, stdout.String(), stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("channel %s did not complete\nstdout:\n%s\nstderr:\n%s", verb, stdout.String(), stderr.String())
	}
	secretSurface := stdout.String() + "\n" + stderr.String()
	if !strings.Contains(secretSurface, "READY") || strings.Contains(secretSurface, "bot-token") || strings.Contains(secretSurface, signingSecret) {
		t.Fatalf("channel %s output violated readiness/secret contract\n%s", verb, secretSurface)
	}
	delivery := waitChannelOnboardingDelivery(t, provider, deliveryIndex)
	if fmt.Sprint(delivery["chat_id"]) != fmt.Sprint(chatID) || delivery["text"] != "Swarm channel connected." {
		t.Fatalf("channel %s confirmation = %#v", verb, delivery)
	}

	return readCurrentChannelOnboardingJourney(t, configPath, endpoint)
}

func readCurrentChannelOnboardingJourney(t *testing.T, configPath, endpoint string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, configPath, endpoint) {
		if row.Identity.Status == "current" {
			return row
		}
	}
	t.Fatal("channel list has no current row")
	return channelOnboardingJourneyReadback{}
}

func readChannelOnboardingRow(t *testing.T, configPath, endpoint, status string) channelOnboardingJourneyReadback {
	t.Helper()
	rows := readChannelOnboardingRows(t, configPath, endpoint)
	for _, row := range rows {
		if row.Identity.Status == status {
			return row
		}
	}
	t.Fatalf("channel list has no %s row: %#v", status, rows)
	return channelOnboardingJourneyReadback{}
}

func readChannelOnboardingRows(t *testing.T, configPath, endpoint string) []channelOnboardingJourneyReadback {
	t.Helper()
	listOut, listErr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"--config", configPath, "channel", "list", "--json", "--api-server", endpoint}, listOut, listErr, nil)
	if code != 0 {
		t.Fatalf("channel list exited %d: %s", code, listErr.String())
	}
	var list struct {
		Channels []channelOnboardingJourneyReadback `json:"channels"`
	}
	if err := json.Unmarshal([]byte(listOut.String()), &list); err != nil {
		t.Fatalf("decode channel list: %v\n%s", err, listOut.String())
	}
	return list.Channels
}

func waitChannelOnboardingChallenge(t *testing.T, stdout, stderr *lockedBuffer, done <-chan int) string {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if challenge := channelOnboardingChallengePattern.FindString(stdout.String()); challenge != "" {
			return challenge
		}
		select {
		case code := <-done:
			t.Fatalf("channel command exited %d before challenge\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for channel challenge\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		case <-ticker.C:
		}
	}
}

func waitChannelOnboardingRegistration(t *testing.T, provider *channelOnboardingTelegramProvider, stdout, stderr *lockedBuffer, done <-chan int) (string, string) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		callbackURL, signingSecret, _ := provider.registration()
		if callbackURL != "" && signingSecret != "" {
			return callbackURL, signingSecret
		}
		select {
		case code := <-done:
			t.Fatalf("channel command exited %d before registration\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for provider registration\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		case <-ticker.C:
		}
	}
}

func waitChannelOnboardingDelivery(t *testing.T, provider *channelOnboardingTelegramProvider, index int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if delivery := provider.delivery(index); delivery != nil {
			return delivery
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Telegram confirmation %d", index)
	return nil
}

func reserveChannelOnboardingListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func enableChannelOnboardingRecoveryOnStartup(t *testing.T, configPath string) {
	t.Helper()
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read channel onboarding runtime config: %v", err)
	}
	updated := strings.Replace(string(contents), "  recovery_on_startup: false", "  recovery_on_startup: true", 1)
	if updated == string(contents) {
		t.Fatalf("channel onboarding runtime config does not declare recovery_on_startup: false")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("enable channel onboarding startup recovery: %v", err)
	}
}

func disableChannelOnboardingBusinessConsumers(t *testing.T, contractsRoot string) {
	t.Helper()
	files := map[string]string{
		"package.yaml": `name: telegram-agent
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - id: bot
    path: bot
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
`,
		filepath.Join("bot", "package.yaml"): `name: telegram-channel-onboarding
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
provider_trigger_events:
  imports:
    - provider: telegram
      event: inbound.telegram.text_message
flows:
  - id: telegram-chat
    flow: telegram-chat
    mode: template
`,
		filepath.Join("bot", "flows", "telegram-chat", "agents.yaml"): "{}\n",
		filepath.Join("bot", "flows", "telegram-chat", "nodes.yaml"):  "{}\n",
		filepath.Join("bot", "flows", "telegram-chat", "events.yaml"): "{}\n",
	}
	for relative, contents := range files {
		if err := os.WriteFile(filepath.Join(contractsRoot, relative), []byte(contents), 0o644); err != nil {
			t.Fatalf("disable onboarding fixture business consumer %s: %v", relative, err)
		}
	}
}

func scenariosForChannelOnboardingJourney() []servedparity.Scenario {
	return []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingRetryLifecycle),
	}
}

func TestWebhookOnboardingAdmissionIsRejectedAtActivationWithoutPublicIngress(t *testing.T) {
	if err := rejectWebhookPrebindingWithoutPublicIngress(serveChannelActivationSnapshot{}); err != nil {
		t.Fatalf("read-only catalog snapshot rejected without ingress: %v", err)
	}
	err := rejectWebhookPrebindingWithoutPublicIngress(serveChannelActivationSnapshot{Prebinding: []servePrebindingActivation{{
		Candidate: channelonboarding.Candidate{Posture: channelonboarding.ActivationWebhookRegistration},
	}}})
	terminal, ok := channelonboarding.AsTerminalActivationError(err)
	if !ok || terminal.Code != "public_ingress_unavailable" {
		t.Fatalf("webhook activation error = %#v, %v", terminal, err)
	}
}

func TestConnectedChannelRecoveryRunsWithoutPublicIngressOwner(t *testing.T) {
	order := []string{}
	teardown := &serveChannelRecoveryProbe{name: "teardown", order: &order}
	onboarding := &serveChannelRecoveryProbe{name: "onboarding", order: &order}
	if err := recoverServeConnectedChannelLifecycle(context.Background(), teardown, onboarding); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "teardown,onboarding" {
		t.Fatalf("recovery order = %s, want teardown,onboarding", got)
	}
}

type serveChannelRecoveryProbe struct {
	name  string
	order *[]string
}

func (p *serveChannelRecoveryProbe) Recover(context.Context) error {
	*p.order = append(*p.order, p.name)
	return nil
}
