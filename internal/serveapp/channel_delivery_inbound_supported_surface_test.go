package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/telegramapi"
)

func TestChannelDeliveryInboundDispositionE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, false)
		})
	}
}

func TestChannelDeliveryNoticeFirstE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, true)
		})
	}
}

type telegramNoticeLLMRuntime struct{ telegramPhraseBotLLMRuntime }

func (r telegramNoticeLLMRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ManagedCall) (*runtimellm.Response, error) {
	response, err := r.telegramPhraseBotLLMRuntime.ContinueManagedSession(ctx, session, call)
	if err != nil || len(response.ToolCalls) == 0 {
		return response, err
	}
	response.ToolCalls[0].Name = "notify_human"
	response.ToolCalls[0].Arguments = map[string]any{
		"summary": "Observed ordinary business text",
		"context": map[string]any{"source": "signed Telegram inbound"},
	}
	response.Message.ToolCalls = append([]runtimellm.ToolCall(nil), response.ToolCalls...)
	return response, nil
}

func runChannelDeliveryInboundDispositionE2E(t *testing.T, backend string, notice bool) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	configureStandingLifecycleCredentials(t)
	provider := &telegramapi.Double{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	publicListener := reserveChannelOnboardingListener(t)
	publicListen := publicListener.Addr().String()
	redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + publicListen})
	var configPath string
	if backend == "postgres" {
		dsn, _, _ := testutil.StartPostgres(t)
		configPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
	} else {
		sqlitePath := filepath.Join(t.TempDir(), "channel-inbound.sqlite")
		configPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, channelOnboardingHostWorkspaceFields())
	}
	enableChannelOnboardingRecoveryOnStartup(t, configPath)
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath, ConfigPath: configPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		PublicWebhookBaseURL: "https://hooks.channel-onboarding.test", PublicWebhookListen: publicListen,
		PublicWebhookListener: publicListener, SelfCheck: true, AbandonActiveRuns: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
		StoreMode: backend, StoreModeSet: true,
	}
	if notice {
		opts.TestLLMRuntime = telegramNoticeLLMRuntime{}
	}
	process := startServeRuntimeTestProcess(t, opts)
	t.Cleanup(func() { _ = process.stop() })
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	runChannelOnboardingCLIJourney(t, configPath, endpoint, provider, "connect", "bot-token", 1001, "private", 0)
	callbackURL, signing, _ := provider.Registration()
	requestBody, err := json.Marshal(map[string]any{
		"update_id": time.Now().UnixMilli(),
		"message": map[string]any{
			"message_id": 9101, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": "ordinary business text",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, callbackURL, strings.NewReader(string(requestBody)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signing)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("business text admission status=%d body=%s", response.StatusCode, responseBody)
	}
	var admitted struct {
		EventNames []string `json:"event_names"`
	}
	if err := json.Unmarshal(responseBody, &admitted); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted || len(admitted.EventNames) == 0 {
		t.Fatalf("ordinary text was consumed without a business event: status=%d events=%v", response.StatusCode, admitted.EventNames)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		for index := 1; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if fmt.Sprint(delivery["chat_id"]) == "1001" && strings.Contains(fmt.Sprint(delivery["text"]), "ordinary business text") {
				if notice && !strings.Contains(fmt.Sprint(delivery["text"]), "Observed ordinary business text") {
					continue
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("business flow did not answer ordinary text; deliveries=%v", provider.Delivery(1))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
