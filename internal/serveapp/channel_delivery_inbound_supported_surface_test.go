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

	"github.com/division-sh/swarm/internal/apiv1"
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

func TestChannelNoticeAcknowledgmentE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "notice_ack")
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

func TestChannelNativeInboxUnknownWriteBlocksReadinessE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox_install_loss")
		})
	}
}

func TestChannelNativeInboxReadbackFailureBlocksReadinessE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox_readback_failure")
		})
	}
}

func TestChannelNativeInboxForeignCommandsBlockReadinessE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox_foreign_commands")
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

func TestChannelDeliveryViewFullE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "view_full")
		})
	}
}

func TestChannelDeliveryBacklogSummaryOpenInboxE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "summary_open")
		})
	}
}

func TestChannelDeliveryBacklogOpenInboxAcrossPublicPagesE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "summary_open_many")
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

type telegramLongNoticeLLMRuntime struct{ telegramPhraseBotLLMRuntime }

func (r telegramLongNoticeLLMRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ManagedCall) (*runtimellm.Response, error) {
	response, err := r.telegramPhraseBotLLMRuntime.ContinueManagedSession(ctx, session, call)
	if err != nil || len(response.ToolCalls) == 0 {
		return response, err
	}
	response.ToolCalls[0].Name = "notify_human"
	response.ToolCalls[0].Arguments = map[string]any{
		"summary": "Long detail", "context": map[string]any{"trace": strings.Repeat("A", 7500)},
	}
	response.Message.ToolCalls = append([]runtimellm.ToolCall(nil), response.ToolCalls...)
	return response, nil
}

func runChannelDeliveryInboundDispositionE2E(t *testing.T, backend, scenario string) {
	t.Helper()
	nativeInboxScenario := scenario == "inbox" || scenario == "inbox_loss"
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
	if scenario == "notice" || scenario == "notice_ack" || scenario == "summary_open" || scenario == "summary_open_many" {
		opts.TestLLMRuntime = telegramNoticeLLMRuntime{}
	} else if scenario == "view_full" {
		opts.TestLLMRuntime = telegramLongNoticeLLMRuntime{}
	}
	process := startServeRuntimeTestProcess(t, opts)
	t.Cleanup(func() { _ = process.stop() })
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	if scenario == "summary_open" || scenario == "summary_open_many" {
		rpcEndpoint := endpoint + "/v1/rpc"
		var identity apiv1.RuntimeIdentityResult
		requireServedJSONRPCResult(t, rpcEndpoint, "runtime.identity", map[string]any{}, &identity)
		if len(identity.SourceArtifacts) != 1 {
			t.Fatalf("preconnection runtime sources = %+v, want one", identity.SourceArtifacts)
		}
		count := 1
		if scenario == "summary_open_many" {
			count = 7
		}
		for index := 0; index < count; index++ {
			var published map[string]any
			requireServedJSONRPCResult(t, rpcEndpoint, "event.publish", map[string]any{
				"bundle_hash":     identity.SourceArtifacts[0].BundleHash,
				"event_name":      "telegram-chat/inbound.telegram.text_message",
				"idempotency_key": fmt.Sprintf("preconnection-notice-%d", index),
				"payload": map[string]any{
					"text": "ordinary business text", "external_account_reference": "7000",
					"conversation_reference": "1001", "conversation_scope": "direct",
					"provider_message_reference": 9100 + index,
				},
			}, &published)
			deadline := time.Now().Add(15 * time.Second)
			for {
				var listed map[string]any
				requireServedJSONRPCResult(t, rpcEndpoint, "mailbox.list", map[string]any{"status": "pending"}, &listed)
				items, _ := listed["items"].([]any)
				notices := 0
				for _, raw := range items {
					item, _ := raw.(map[string]any)
					if item["kind"] == "notice" {
						notices++
					}
				}
				if notices >= index+1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("preconnection notice %d did not commit: %v", index+1, listed)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	if scenario == "inbox_install_loss" {
		provider.LoseNextCommandWriteAcknowledgment()
	} else if scenario == "inbox_readback_failure" {
		provider.FailCommandReadbackAfterWrite()
	} else if scenario == "inbox_foreign_commands" {
		if err := provider.SeedCommands("bot-token", map[string]any{"type": "chat", "chat_id": "1001"}, "",
			[]map[string]any{{"command": "foreign", "description": "Foreign owner"}}); err != nil {
			t.Fatal(err)
		}
	}
	runChannelOnboardingCLIJourney(t, configPath, endpoint, provider, "connect", "bot-token", 1001, "private", 0)
	if scenario == "inbox_install_loss" || scenario == "inbox_readback_failure" || scenario == "inbox_foreign_commands" {
		time.Sleep(5500 * time.Millisecond)
		wantWrites := 1
		if scenario == "inbox_foreign_commands" {
			wantWrites = 0
		}
		if writes := provider.CommandWrites(); len(writes) != wantWrites {
			t.Fatalf("non-usable native setting caused %d writes, want %d: %v", len(writes), wantWrites, writes)
		}
		if delivery := provider.Delivery(1); delivery != nil {
			reads, failures := provider.CommandReadbackCounts()
			t.Fatalf("non-usable native setting admitted channel delivery: %v; readbacks=%d failures=%d writes=%v", delivery, reads, failures, provider.CommandWrites())
		}
		return
	}
	if scenario == "summary_open" || scenario == "summary_open_many" {
		callbackURL, signing, _ := provider.Registration()
		count := 1
		if scenario == "summary_open_many" {
			count = 7
		}
		proveChannelBacklogSummaryOpenInbox(t, provider, callbackURL, signing, count)
		return
	}
	inputText := "ordinary business text"
	if nativeInboxScenario {
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
	if scenario == "view_full" {
		if len(admitted.EventNames) == 0 {
			t.Fatal("view-full source did not enter the business flow")
		}
		proveChannelViewFullCallbackPages(t, provider, callbackURL, signing)
		return
	}
	if scenario == "notice_ack" {
		if len(admitted.EventNames) == 0 {
			t.Fatal("notice source did not enter the business flow")
		}
		proveChannelNoticeAcknowledgment(t, provider, callbackURL, signing, endpoint+"/v1/rpc")
		return
	}
	if (nativeInboxScenario || scenario == "verdict" || scenario == "verdict_ack_loss") && len(admitted.EventNames) != 0 {
		t.Fatalf("native inbox entry leaked into business events: %v", admitted.EventNames)
	}
	if !nativeInboxScenario && scenario != "verdict" && scenario != "verdict_ack_loss" && len(admitted.EventNames) == 0 {
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
			if nativeInboxScenario && fmt.Sprint(delivery["chat_id"]) == "1001" &&
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
				if !nativeInboxScenario {
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

func proveChannelViewFullCallbackPages(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	messageIndex := -1
	for messageIndex < 0 {
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), "Notice: Long detail") {
				messageIndex = index
				break
			}
		}
		if messageIndex < 0 {
			if time.Now().After(deadline) {
				t.Fatal("long notice was not delivered")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var reconstructed strings.Builder
	pageCount := 0
	for {
		delivery := provider.Delivery(messageIndex)
		label := "View full"
		if pageCount > 0 {
			label = "Next page"
		}
		token, found := telegramCallbackToken(delivery, label)
		if !found {
			t.Fatalf("page %d lacks %q control: %v", pageCount, label, delivery)
		}
		update := map[string]any{
			"update_id": time.Now().UnixMilli() + int64(pageCount)*1000,
			"callback_query": map[string]any{
				"id": fmt.Sprintf("view-full-%d", pageCount), "from": map[string]any{"id": 7000},
				"message": map[string]any{"message_id": messageIndex + 1, "chat": map[string]any{"id": 1001, "type": "private"}},
				"data":    token,
			},
		}
		body, err := json.Marshal(update)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, callbackURL, strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signing)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		responseBody, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusAccepted {
			t.Fatalf("view-full callback status=%d body=%s err=%v", response.StatusCode, responseBody, err)
		}
		nextIndex := messageIndex + 1
		for {
			candidate := provider.Delivery(nextIndex)
			if candidate != nil && strings.HasPrefix(fmt.Sprint(candidate["text"]), fmt.Sprintf("Page %d/", pageCount+1)) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("view-full page %d was not delivered; acknowledgments=%v", pageCount+1, provider.Acknowledgments())
			}
			if candidate == nil {
				time.Sleep(20 * time.Millisecond)
			} else {
				nextIndex++
			}
		}
		page := provider.Delivery(nextIndex)
		parts := strings.SplitN(fmt.Sprint(page["text"]), "\n", 2)
		var ordinal, advertised int
		if len(parts) != 2 {
			t.Fatalf("view-full page lacks content: %v", page)
		}
		if _, err := fmt.Sscanf(parts[0], "Page %d/%d", &ordinal, &advertised); err != nil || ordinal != pageCount+1 {
			t.Fatalf("view-full page order changed: %q, err=%v", parts[0], err)
		}
		reconstructed.WriteString(parts[1])
		pageCount++
		messageIndex = nextIndex
		if _, more := telegramCallbackToken(page, "Next page"); !more {
			if pageCount != advertised || pageCount < 2 || !strings.Contains(reconstructed.String(), strings.Repeat("A", 7500)) ||
				len(provider.Acknowledgments()) != pageCount {
				t.Fatalf("view-full pages incomplete: pages=%d advertised=%d acknowledgments=%d", pageCount, advertised, len(provider.Acknowledgments()))
			}
			return
		}
	}
}

func telegramCallbackToken(delivery map[string]any, label string) (string, bool) {
	markup, ok := delivery["reply_markup"].(map[string]any)
	if !ok {
		return "", false
	}
	rows, ok := markup["inline_keyboard"].([]any)
	if !ok {
		return "", false
	}
	for _, rawRow := range rows {
		row, ok := rawRow.([]any)
		if !ok {
			continue
		}
		for _, rawControl := range row {
			control, ok := rawControl.(map[string]any)
			if ok && control["text"] == label {
				token, ok := control["callback_data"].(string)
				return token, ok && token != ""
			}
		}
	}
	return "", false
}

func postChannelTelegramUpdate(t *testing.T, callbackURL, signing string, update map[string]any) []string {
	return postChannelTelegramUpdateStatus(t, callbackURL, signing, update, http.StatusAccepted)
}

func postChannelTelegramUpdateStatus(t *testing.T, callbackURL, signing string, update map[string]any, expectedStatus int) []string {
	t.Helper()
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, callbackURL, strings.NewReader(string(body)))
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
	if err != nil || response.StatusCode != expectedStatus {
		t.Fatalf("Telegram update status=%d body=%s err=%v", response.StatusCode, responseBody, err)
	}
	var admitted struct {
		EventNames []string `json:"event_names"`
	}
	if err := json.Unmarshal(responseBody, &admitted); err != nil {
		t.Fatal(err)
	}
	return admitted.EventNames
}

func proveChannelBacklogSummaryOpenInbox(t *testing.T, provider *telegramapi.Double, callbackURL, signing string, count int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	summaryIndex := -1
	for summaryIndex < 0 {
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), "Notice: Observed ordinary business text") {
				t.Fatalf("preconnection notice was sent individually: %v", delivery)
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), fmt.Sprintf("%d earlier", count)) &&
				strings.Contains(fmt.Sprint(delivery["text"]), "waiting in your inbox") {
				summaryIndex = index
				break
			}
		}
		if summaryIndex < 0 {
			if time.Now().After(deadline) {
				t.Fatal("first connection did not deliver its bounded backlog summary")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	token, found := telegramCallbackToken(provider.Delivery(summaryIndex), "Open inbox")
	if !found {
		t.Fatalf("backlog summary has no Open inbox action: %v", provider.Delivery(summaryIndex))
	}
	admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1000,
		"callback_query": map[string]any{
			"id": "backlog-summary-open", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": summaryIndex + 1, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    token,
		},
	})
	if len(admitted) != 0 {
		t.Fatalf("summary callback leaked into business events: %v", admitted)
	}
	for {
		for index := summaryIndex + 1; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			message := fmt.Sprint(delivery["text"])
			if strings.HasPrefix(message, fmt.Sprintf("Inbox\nUnread notices: %d", count)) &&
				strings.Count(message, "- operator_notice") == count {
				if len(provider.Acknowledgments()) != 1 {
					t.Fatalf("summary callback acknowledgment count = %d", len(provider.Acknowledgments()))
				}
				return
			}
		}
		if time.Now().After(deadline) {
			var later []map[string]any
			for index := summaryIndex + 1; provider.Delivery(index) != nil; index++ {
				later = append(later, provider.Delivery(index))
			}
			t.Fatalf("summary action did not open unread inbox; summary=%v later=%v acknowledgments=%v", provider.Delivery(summaryIndex), later, provider.Acknowledgments())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelNoticeAcknowledgment(t *testing.T, provider *telegramapi.Double, callbackURL, signing, rpcEndpoint string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	messageIndex := -1
	for messageIndex < 0 {
		for index := 0; provider.Delivery(index) != nil; index++ {
			if strings.Contains(fmt.Sprint(provider.Delivery(index)["text"]), "Notice: Observed ordinary business text") {
				messageIndex = index
				break
			}
		}
		if messageIndex < 0 {
			if time.Now().After(deadline) {
				t.Fatal("notice was not delivered before acknowledgment")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var listed map[string]any
	requireServedJSONRPCResult(t, rpcEndpoint, "mailbox.list", map[string]any{"status": "pending"}, &listed)
	if listed["unread_informational_notices"] != float64(1) {
		t.Fatalf("notice unread count before acknowledgment = %v", listed)
	}
	token, found := telegramCallbackToken(provider.Delivery(messageIndex), "Acknowledge")
	if !found {
		t.Fatalf("notice has no Acknowledge action: %v", provider.Delivery(messageIndex))
	}
	update := map[string]any{
		"update_id": time.Now().UnixMilli() + 2000,
		"callback_query": map[string]any{
			"id": "notice-ack-1", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": messageIndex + 1, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    token,
		},
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, update); len(admitted) != 0 {
		t.Fatalf("notice acknowledgment leaked into business events: %v", admitted)
	}
	for {
		requireServedJSONRPCResult(t, rpcEndpoint, "mailbox.list", map[string]any{"status": "pending"}, &listed)
		edited := false
		for _, edit := range provider.Edits() {
			if strings.Contains(fmt.Sprint(edit["text"]), "Acknowledged") {
				if _, stale := telegramCallbackToken(edit, "Acknowledge"); stale {
					t.Fatalf("acknowledged notice retained its mutation control: %v", edit)
				}
				edited = true
			}
		}
		if listed["unread_informational_notices"] == float64(0) && len(provider.Acknowledgments()) == 1 && edited {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notice did not acknowledge and edit through canonical state: list=%v callbacks=%v edits=%v", listed, provider.Acknowledgments(), provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if admitted := postChannelTelegramUpdateStatus(t, callbackURL, signing, update, http.StatusOK); len(admitted) != 0 {
		t.Fatalf("duplicate notice callback leaked into business events: %v", admitted)
	}
	requireServedJSONRPCResult(t, rpcEndpoint, "mailbox.list", map[string]any{"status": "pending"}, &listed)
	if listed["unread_informational_notices"] != float64(0) {
		t.Fatalf("duplicate acknowledgment changed unread state: %v", listed)
	}
}
