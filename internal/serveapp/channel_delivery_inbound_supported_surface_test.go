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
			runChannelDeliveryInboundDispositionE2E(t, backend, "business")
		})
	}
}

func TestChannelDeliveryNoticeFirstE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "notice")
		})
	}
}

func TestChannelDeliveryNativeInboxE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox")
		})
	}
}

func TestChannelDeliveryNativeInboxLostAcknowledgmentE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox_loss")
		})
	}
}

func TestChannelDeliveryVerdictAcknowledgmentE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "verdict")
		})
	}
}

func TestChannelDeliveryVerdictAcknowledgmentLossE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "verdict_ack_loss")
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

func runChannelDeliveryInboundDispositionE2E(t *testing.T, backend, scenario string) {
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
	if scenario == "notice" {
		opts.TestLLMRuntime = telegramNoticeLLMRuntime{}
	}
	process := startServeRuntimeTestProcess(t, opts)
	t.Cleanup(func() { _ = process.stop() })
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	runChannelOnboardingCLIJourney(t, configPath, endpoint, provider, "connect", "bot-token", 1001, "private", 0)
	inputText := "ordinary business text"
	if scenario == "inbox" || scenario == "inbox_loss" {
		deadline := time.Now().Add(15 * time.Second)
		for {
			for _, write := range provider.CommandWrites() {
				scope, _ := write["scope"].(map[string]any)
				if scope["type"] != "chat" || fmt.Sprint(scope["chat_id"]) != "1001" {
					continue
				}
				commands, _ := write["commands"].([]map[string]any)
				if len(commands) == 1 {
					inputText = "/" + fmt.Sprint(commands[0]["command"])
					break
				}
			}
			if inputText != "ordinary business text" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("native Open inbox command was not installed: %v", provider.CommandWrites())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if scenario == "inbox_loss" {
		deadline := time.Now().Add(15 * time.Second)
		for provider.Delivery(1) == nil {
			if time.Now().After(deadline) {
				t.Fatal("initial card delivery did not settle before loss probe")
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(250 * time.Millisecond)
		provider.LoseNextDeliveryAcknowledgment()
	}
	callbackURL, signing, _ := provider.Registration()
	update := map[string]any{
		"update_id": time.Now().UnixMilli(),
		"message": map[string]any{
			"message_id": 9101, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": inputText,
		},
	}
	if scenario == "verdict" || scenario == "verdict_ack_loss" {
		deadline := time.Now().Add(15 * time.Second)
		for provider.Delivery(1) == nil {
			if time.Now().After(deadline) {
				t.Fatal("card was not delivered before callback")
			}
			time.Sleep(20 * time.Millisecond)
		}
		cardMessage := provider.Delivery(1)
		markup, ok := cardMessage["reply_markup"].(map[string]any)
		if !ok {
			t.Fatalf("card has no callback controls: %v", cardMessage)
		}
		rows, ok := markup["inline_keyboard"].([]any)
		if !ok || len(rows) == 0 {
			t.Fatalf("card has no callback rows: %v", markup)
		}
		row, ok := rows[0].([]any)
		if !ok || len(row) == 0 {
			t.Fatalf("card has no first callback row: %v", rows)
		}
		control, ok := row[0].(map[string]any)
		if !ok || fmt.Sprint(control["callback_data"]) == "" {
			t.Fatalf("card has no callback token: %v", row)
		}
		update = map[string]any{
			"update_id": time.Now().UnixMilli(),
			"callback_query": map[string]any{
				"id": "callback-9102", "from": map[string]any{"id": 7000},
				"message": map[string]any{"message_id": 2, "chat": map[string]any{"id": 1001, "type": "private"}},
				"data":    control["callback_data"],
			},
		}
		if scenario == "verdict_ack_loss" {
			provider.LoseNextCallbackAcknowledgment()
		}
	}
	requestBody, err := json.Marshal(update)
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
	if (scenario == "inbox" || scenario == "inbox_loss" || scenario == "verdict" || scenario == "verdict_ack_loss") && len(admitted.EventNames) != 0 {
		t.Fatalf("native inbox entry leaked into business events: %v", admitted.EventNames)
	}
	if scenario != "inbox" && scenario != "inbox_loss" && scenario != "verdict" && scenario != "verdict_ack_loss" && len(admitted.EventNames) == 0 {
		t.Fatalf("ordinary text was consumed without a business event: status=%d events=%v", response.StatusCode, admitted.EventNames)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if (scenario == "verdict" || scenario == "verdict_ack_loss") && len(provider.Acknowledgments()) > 0 && len(provider.Edits()) > 0 {
			if scenario == "verdict_ack_loss" {
				time.Sleep(2300 * time.Millisecond)
				if got := len(provider.Acknowledgments()); got != 1 {
					t.Fatalf("lost callback acknowledgment was sent %d times", got)
				}
			}
			return
		}
		for index := 1; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			message := fmt.Sprint(delivery["text"])
			if (scenario == "inbox" || scenario == "inbox_loss") && fmt.Sprint(delivery["chat_id"]) == "1001" &&
				strings.HasPrefix(message, "Inbox\nUnread notices: ") && strings.Contains(message, "Retire service") {
				if scenario == "inbox_loss" {
					_, before := provider.Counts()
					time.Sleep(2500 * time.Millisecond)
					_, after := provider.Counts()
					if after != before {
						t.Fatalf("uncertain native inbox response was resent: before=%d after=%d", before, after)
					}
				}
				return
			}
			if fmt.Sprint(delivery["chat_id"]) == "1001" && strings.Contains(message, "ordinary business text") {
				if scenario == "notice" && !strings.Contains(message, "Observed ordinary business text") {
					continue
				}
				if scenario != "inbox" && scenario != "inbox_loss" {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s flow did not answer %q; deliveries=%v acknowledgments=%v edits=%v", scenario, inputText, provider.Delivery(1), provider.Acknowledgments(), provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
