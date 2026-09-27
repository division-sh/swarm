package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestChannelNativeInboxDelayedApplyDuringReconnectStartE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "inbox_delayed_apply")
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

func TestChannelDeliveryRequiredInputE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "required_input")
		})
	}
}

func TestChannelDeliveryInvalidTypedInputE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "invalid_input")
		})
	}
}

func TestChannelDeliveryOrderedInputE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "ordered_input")
		})
	}
}

func TestChannelDeliveryCancelInputE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "cancel_input")
		})
	}
}

func TestChannelDeliverySkipFinalOptionalInputE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "skip_input")
		})
	}
}

func TestChannelDeliveryBareInputChooserE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "draft_chooser")
		})
	}
}

func TestChannelDeliveryQuotedTwoDraftsE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "quoted_two")
		})
	}
}

func TestChannelDeliveryManualResendAfterLostResponseE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "delivery_loss_resend")
		})
	}
}

func TestChannelDeliveryManualResendAfterLostPromptEditE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "edit_loss_resend")
		})
	}
}

func TestChannelDeliverySharedAudienceE2E(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runChannelDeliveryInboundDispositionE2E(t, backend, "shared_audience")
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
	var commandApplyArrived <-chan struct{}
	var releaseCommandApply func()
	if scenario == "inbox_delayed_apply" {
		commandApplyArrived, releaseCommandApply = provider.PauseNextCommandApply()
	}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	if releaseCommandApply != nil {
		t.Cleanup(releaseCommandApply)
	}
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	if scenario == "draft_chooser" || scenario == "quoted_two" {
		sourceRoot = writeMixedStandingTelegramServeFixture(t, telegram.URL)
	}
	if scenario == "required_input" || scenario == "invalid_input" || scenario == "ordered_input" || scenario == "edit_loss_resend" ||
		scenario == "cancel_input" || scenario == "skip_input" || scenario == "draft_chooser" || scenario == "quoted_two" {
		flowNames := []string{"telegram-ingress"}
		if scenario == "draft_chooser" || scenario == "quoted_two" {
			flowNames = append(flowNames, "telegram-stopped")
		}
		for _, flowName := range flowNames {
			schemaPath := filepath.Join(sourceRoot, flowName, "schema.yaml")
			raw, err := os.ReadFile(schemaPath)
			if err != nil {
				t.Fatal(err)
			}
			inputType := "text"
			if scenario == "invalid_input" {
				inputType = "integer"
			}
			inputBlock := "            reason: {type: " + inputType + ", required: true}\n"
			if scenario == "ordered_input" {
				inputBlock = "            zeta: {type: integer, required: true}\n            alpha: {type: boolean, required: true}\n"
			} else if scenario == "skip_input" {
				inputBlock = "            reason: {type: text, required: false}\n"
			}
			modified := strings.Replace(string(raw), "        retire:\n          advances_to: done",
				"        retire:\n          input:\n"+inputBlock+"          advances_to: done", 1)
			if modified == string(raw) {
				t.Fatal("standing Telegram fixture did not contain the expected gate outcome")
			}
			if err := os.WriteFile(schemaPath, []byte(modified), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
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
	if scenario == "notice" || scenario == "notice_ack" || scenario == "shared_audience" || scenario == "summary_open" ||
		scenario == "summary_open_many" || scenario == "delivery_loss_resend" {
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
	chatID, chatType := int64(1001), "private"
	if scenario == "shared_audience" {
		chatID, chatType = -1001, "group"
	}
	runChannelOnboardingCLIJourney(t, configPath, endpoint, provider, "connect", "bot-token", chatID, chatType, 0)
	if scenario == "shared_audience" {
		callbackURL, signing, _ := provider.Registration()
		proveChannelSharedAudience(t, provider, callbackURL, signing)
		return
	}
	if scenario == "inbox_delayed_apply" {
		select {
		case <-commandApplyArrived:
		case <-time.After(15 * time.Second):
			t.Fatal("native setting write did not launch")
		}
		var reconnect map[string]any
		requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "channel.onboarding_start", map[string]any{
			"provider": "telegram", "verb": "reconnect", "save_proof": false,
		}, &reconnect)
		operation, _ := reconnect["operation"].(map[string]any)
		if operation["phase"] != "awaiting_external_identity" {
			t.Fatalf("reconnect did not admit an independent ceremony: %v", reconnect)
		}
		if writes := provider.CommandWrites(); len(writes) != 0 {
			t.Fatalf("successor wrote over launched predecessor: %v", writes)
		}
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.HasPrefix(fmt.Sprint(delivery["text"]), "Retire service") {
				t.Fatalf("card delivered before native setting write settled: %v", delivery)
			}
		}
		releaseCommandApply()
		deadline := time.Now().Add(15 * time.Second)
		for {
			writes := provider.CommandWrites()
			cardDelivered := false
			for index := 0; ; index++ {
				delivery := provider.Delivery(index)
				if delivery == nil {
					break
				}
				cardDelivered = cardDelivered || strings.HasPrefix(fmt.Sprint(delivery["text"]), "Retire service")
			}
			if len(writes) == 1 && cardDelivered {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("delayed predecessor did not converge: writes=%v card=%v", writes, cardDelivered)
			}
			time.Sleep(20 * time.Millisecond)
		}
		return
	}
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
	if scenario == "delivery_loss_resend" {
		deadline := time.Now().Add(15 * time.Second)
		for provider.Delivery(1) == nil {
			if time.Now().After(deadline) {
				t.Fatal("initial card delivery did not settle before notice loss probe")
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
	if scenario == "verdict" || scenario == "verdict_ack_loss" || scenario == "required_input" || scenario == "invalid_input" || scenario == "edit_loss_resend" ||
		scenario == "ordered_input" || scenario == "cancel_input" || scenario == "skip_input" || scenario == "draft_chooser" || scenario == "quoted_two" {
		cardMessageID := 2
		if scenario == "quoted_two" {
			cardMessageID = waitChannelCardMessageID(t, provider, "telegram-ingress")
		}
		deadline := time.Now().Add(15 * time.Second)
		for provider.Delivery(cardMessageID-1) == nil {
			if time.Now().After(deadline) {
				t.Fatal("card was not delivered before callback")
			}
			time.Sleep(20 * time.Millisecond)
		}
		cardMessage := provider.Delivery(cardMessageID - 1)
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
				"message": map[string]any{"message_id": cardMessageID, "chat": map[string]any{"id": 1001, "type": "private"}},
				"data":    control["callback_data"],
			},
		}
		if scenario == "verdict_ack_loss" {
			provider.LoseNextCallbackAcknowledgment()
		}
		if scenario == "edit_loss_resend" {
			provider.LoseNextEditAcknowledgment()
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
	if scenario == "delivery_loss_resend" {
		if len(admitted.EventNames) == 0 {
			t.Fatal("lost-response notice source did not enter the business flow")
		}
		proveChannelManualResendAfterLostNotice(t, provider, callbackURL, signing)
		return
	}
	if scenario == "edit_loss_resend" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("lost prompt edit callback escaped into business events: %v", admitted.EventNames)
		}
		proveChannelEditLossResendQuotedInput(t, provider, callbackURL, signing)
		return
	}
	if scenario == "required_input" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelRequiredInputText(t, provider, callbackURL, signing)
		return
	}
	if scenario == "invalid_input" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("typed draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelInvalidInputText(t, provider, callbackURL, signing)
		return
	}
	if scenario == "ordered_input" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("ordered draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelOrderedInputText(t, provider, callbackURL, signing)
		return
	}
	if scenario == "cancel_input" || scenario == "skip_input" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelInputControl(t, provider, callbackURL, signing, scenario)
		return
	}
	if scenario == "draft_chooser" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelBareInputChooser(t, provider, callbackURL, signing)
		return
	}
	if scenario == "quoted_two" {
		if len(admitted.EventNames) != 0 {
			t.Fatalf("quoted-draft begin leaked business events: %v", admitted.EventNames)
		}
		proveChannelQuotedTwoDrafts(t, provider, callbackURL, signing)
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

func proveChannelRequiredInputText(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		prompted := false
		for _, edit := range provider.Edits() {
			prompted = prompted || strings.Contains(fmt.Sprint(edit["text"]), "Input: reason (text) required")
		}
		if prompted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("required-input verdict did not enter a visible draft: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	answer := "the operator's private reason"
	admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1000,
		"message": map[string]any{
			"message_id": 9103, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
		},
	})
	if len(admitted) != 0 {
		t.Fatalf("required-input answer escaped into business events: %v", admitted)
	}
	for {
		decided := false
		for _, edit := range provider.Edits() {
			visible := fmt.Sprint(edit["text"])
			if strings.Contains(visible, answer) {
				t.Fatalf("private answer echoed in card edit: %v", edit)
			}
			decided = decided || strings.Contains(visible, "Decision: retire")
		}
		if decided {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("required-input answer did not decide card: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelInvalidInputText(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		prompted := false
		for _, edit := range provider.Edits() {
			prompted = prompted || strings.Contains(fmt.Sprint(edit["text"]), "Input: reason (integer) required")
		}
		if prompted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("typed-input verdict did not enter a visible draft: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	answer := "not-a-number"
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1100,
		"message": map[string]any{
			"message_id": 9104, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
		},
	}); len(admitted) != 0 {
		t.Fatalf("invalid typed answer escaped into business events: %v", admitted)
	}
	for {
		for index := 2; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			visible := fmt.Sprint(delivery["text"])
			if strings.Contains(visible, answer) {
				t.Fatalf("rejected private answer echoed in response: %v", delivery)
			}
			if strings.Contains(visible, "That answer does not match the requested field") {
				for _, edit := range provider.Edits() {
					if strings.Contains(fmt.Sprint(edit["text"]), "Decision: retire") {
						t.Fatalf("invalid typed answer decided the card: %v", edit)
					}
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("invalid typed answer received no teaching response: deliveries=%v edits=%v", provider.Delivery(2), provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelOrderedInputText(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	waitForEdit := func(fragment string) {
		t.Helper()
		for {
			for _, edit := range provider.Edits() {
				visible := fmt.Sprint(edit["text"])
				if strings.Contains(visible, "private value") {
					t.Fatalf("ordered input echoed private answer: %v", edit)
				}
				if strings.Contains(visible, fragment) {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("ordered input did not reach %q: edits=%v", fragment, provider.Edits())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitForEdit("Input: zeta (integer) required")
	postAnswer := func(messageID int, answer string) {
		t.Helper()
		if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
			"update_id": time.Now().UnixMilli() + int64(messageID),
			"message": map[string]any{
				"message_id": messageID, "from": map[string]any{"id": 7000},
				"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
			},
		}); len(admitted) != 0 {
			t.Fatalf("ordered answer escaped into business events: %v", admitted)
		}
	}
	postAnswer(9105, "7")
	waitForEdit("Input: alpha (boolean) required")
	for _, edit := range provider.Edits() {
		if strings.Contains(fmt.Sprint(edit["text"]), "Decision: retire") {
			t.Fatalf("partial ordered answer decided card: %v", edit)
		}
	}
	postAnswer(9106, "true")
	waitForEdit("Decision: retire")
}

func proveChannelInputControl(t *testing.T, provider *telegramapi.Double, callbackURL, signing, scenario string) {
	t.Helper()
	label := "Cancel input"
	if scenario == "skip_input" {
		label = "Skip field"
	}
	deadline := time.Now().Add(20 * time.Second)
	var token string
	for token == "" {
		for _, edit := range provider.Edits() {
			if !strings.Contains(fmt.Sprint(edit["text"]), "Input: reason (text)") {
				continue
			}
			markup, _ := edit["reply_markup"].(map[string]any)
			rows, _ := markup["inline_keyboard"].([]any)
			for _, raw := range rows {
				row, _ := raw.([]any)
				for _, item := range row {
					button, _ := item.(map[string]any)
					if button["text"] == label {
						token, _ = button["callback_data"].(string)
					}
				}
			}
		}
		if token != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s control was not rendered on current prompt: edits=%v", label, provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1200,
		"callback_query": map[string]any{
			"id": "callback-input-control", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": 2, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    token,
		},
	}); len(admitted) != 0 {
		t.Fatalf("%s control escaped into business events: %v", label, admitted)
	}
	for {
		for _, edit := range provider.Edits() {
			visible := fmt.Sprint(edit["text"])
			if scenario == "skip_input" && strings.Contains(visible, "Decision: retire") {
				return
			}
			if scenario == "cancel_input" && strings.Contains(visible, "Decision: pending") &&
				!strings.Contains(visible, "Input: reason") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not converge through card mutation: edits=%v", label, provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelBareInputChooser(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	beginOtherChannelDraft(t, provider, callbackURL, signing, 3)
	deadline := time.Now().Add(25 * time.Second)
	answer := "private chooser answer"
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1400,
		"message": map[string]any{
			"message_id": 9107, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
		},
	}); len(admitted) != 0 {
		t.Fatalf("ambiguous private answer escaped into business events: %v", admitted)
	}
	var chooserToken string
	chooserMessageID := 0
	for chooserToken == "" {
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			visible := fmt.Sprint(delivery["text"])
			if strings.Contains(visible, answer) {
				t.Fatalf("chooser echoed private answer: %v", delivery)
			}
			if !strings.Contains(visible, "Choose the card for your reply") {
				continue
			}
			markup, _ := delivery["reply_markup"].(map[string]any)
			rows, _ := markup["inline_keyboard"].([]any)
			if len(rows) < 2 {
				t.Fatalf("chooser did not include both drafts: %v", delivery)
			}
			choice, _ := rows[1].([]any)
			selected, _ := choice[0].(map[string]any)
			chooserToken, _ = selected["callback_data"].(string)
			chooserMessageID = index + 1
		}
		if chooserToken != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ambiguous answer did not produce a chooser: deliveries=%v edits=%v", provider.Delivery(3), provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1500,
		"callback_query": map[string]any{
			"id": "callback-select-draft", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": chooserMessageID, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    chooserToken,
		},
	}); len(admitted) != 0 {
		t.Fatalf("draft selection escaped into business events: %v", admitted)
	}
	for {
		decided := 0
		for _, edit := range provider.Edits() {
			visible := fmt.Sprint(edit["text"])
			if strings.Contains(visible, answer) {
				t.Fatalf("chosen private answer echoed in card edit: %v", edit)
			}
			if strings.Contains(visible, "Decision: retire") {
				decided++
			}
		}
		if decided == 1 {
			return
		}
		if decided > 1 {
			t.Fatalf("chooser decided multiple cards: edits=%v", provider.Edits())
		}
		if time.Now().After(deadline) {
			t.Fatalf("chosen retained answer did not decide one card: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelCardMessageID(t *testing.T, provider *telegramapi.Double, flowName string) int {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for {
		for index := 1; index <= 2; index++ {
			card := provider.Delivery(index)
			if card != nil && strings.Contains(fmt.Sprint(card["text"]), "Gate: "+flowName+" / active") {
				return index + 1
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s card was not delivered: first=%v second=%v", flowName, provider.Delivery(1), provider.Delivery(2))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func beginOtherChannelDraft(t *testing.T, provider *telegramapi.Double, callbackURL, signing string, cardMessageID int) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	var second map[string]any
	for second == nil {
		second = provider.Delivery(cardMessageID - 1)
		if second != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second standing card was not delivered: deliveries=%v edits=%v", provider.Delivery(1), provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(fmt.Sprint(second["text"]), "Retire service") {
		t.Fatalf("second delivery is not a canonical card: %v", second)
	}
	markup, _ := second["reply_markup"].(map[string]any)
	rows, _ := markup["inline_keyboard"].([]any)
	if len(rows) == 0 {
		t.Fatalf("second card has no verdict action: %v", second)
	}
	row, _ := rows[0].([]any)
	if len(row) == 0 {
		t.Fatalf("second card first action is absent: %v", second)
	}
	button, _ := row[0].(map[string]any)
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1300,
		"callback_query": map[string]any{
			"id": "callback-second-draft", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": cardMessageID, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    button["callback_data"],
		},
	}); len(admitted) != 0 {
		t.Fatalf("second draft begin escaped into business events: %v", admitted)
	}
	for {
		prompts := map[string]bool{}
		for _, edit := range provider.Edits() {
			if strings.Contains(fmt.Sprint(edit["text"]), "Input: reason (text) required") {
				prompts[fmt.Sprint(edit["message_id"])] = true
			}
		}
		if prompts["2"] && prompts["3"] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("two standing drafts were not prompted: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelQuotedTwoDrafts(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	stoppedMessageID := waitChannelCardMessageID(t, provider, "telegram-stopped")
	ingressMessageID := waitChannelCardMessageID(t, provider, "telegram-ingress")
	beginOtherChannelDraft(t, provider, callbackURL, signing, stoppedMessageID)
	deadline := time.Now().Add(25 * time.Second)
	for index, cardMessageID := range []int{stoppedMessageID, ingressMessageID} {
		answer := fmt.Sprintf("private quoted answer %d", cardMessageID)
		if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
			"update_id": time.Now().UnixMilli() + int64(1400+index*100),
			"message": map[string]any{
				"message_id": 9107 + index, "from": map[string]any{"id": 7000},
				"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
				"reply_to_message": map[string]any{"message_id": cardMessageID},
			},
		}); len(admitted) != 0 {
			t.Fatalf("quoted answer to card %d escaped into business events: %v", cardMessageID, admitted)
		}
		for {
			decided := map[string]bool{}
			for _, edit := range provider.Edits() {
				visible := fmt.Sprint(edit["text"])
				if strings.Contains(visible, "private quoted answer") {
					t.Fatalf("quoted private answer echoed in card edit: %v", edit)
				}
				if strings.Contains(visible, "Decision: retire") {
					decided[fmt.Sprint(edit["message_id"])] = true
				}
			}
			if decided[fmt.Sprint(cardMessageID)] {
				if index == 0 && decided[fmt.Sprint(ingressMessageID)] {
					t.Fatalf("quoted second-card answer decided the first card: %v", provider.Edits())
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("quoted answer for card %d did not decide that card: %v", cardMessageID, provider.Edits())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func proveChannelManualResendAfterLostNotice(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	countNotice := func() int {
		count := 0
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), "Notice: Observed ordinary business text") {
				count++
			}
		}
		return count
	}
	for countNotice() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("lost-response notice never reached provider: deliveries=%v", provider.Delivery(2))
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(2200 * time.Millisecond)
	if got := countNotice(); got != 1 {
		t.Fatalf("uncertain notice was resent without consent %d times", got)
	}
	resendToken, resendMessageID := requestChannelRecoveryAction(t, provider, callbackURL, signing, "Resend notice")
	update := map[string]any{
		"update_id": time.Now().UnixMilli() + 1700,
		"callback_query": map[string]any{
			"id": "callback-manual-resend", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": resendMessageID, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    resendToken,
		},
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, update); len(admitted) != 0 {
		t.Fatalf("manual resend escaped into business events: %v", admitted)
	}
	for countNotice() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("manual resend did not create one new notice delivery: count=%d", countNotice())
		}
		time.Sleep(20 * time.Millisecond)
	}
	postChannelTelegramUpdateStatus(t, callbackURL, signing, update, http.StatusOK)
	time.Sleep(2200 * time.Millisecond)
	if got := countNotice(); got != 2 {
		t.Fatalf("duplicate manual resend created %d notice deliveries, want 2", got)
	}
}

func requestChannelRecoveryAction(t *testing.T, provider *telegramapi.Double, callbackURL, signing, label string) (string, int) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	command := ""
	for _, write := range provider.CommandWrites() {
		scope, _ := write["scope"].(map[string]any)
		if scope["type"] != "chat" || fmt.Sprint(scope["chat_id"]) != "1001" {
			continue
		}
		commands, _ := write["commands"].([]map[string]any)
		if len(commands) == 1 {
			command = fmt.Sprint(commands[0]["command"])
		}
	}
	if command == "" {
		t.Fatalf("native recovery entry was not installed: %v", provider.CommandWrites())
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1600,
		"message": map[string]any{
			"message_id": 9110, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command,
		},
	}); len(admitted) != 0 {
		t.Fatalf("native recovery entry escaped into business events: %v", admitted)
	}
	for {
		for index := 0; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if !strings.Contains(fmt.Sprint(delivery["text"]), "Uncertain deliveries - check the chat before resending") {
				continue
			}
			markup, _ := delivery["reply_markup"].(map[string]any)
			rows, _ := markup["inline_keyboard"].([]any)
			for _, raw := range rows {
				row, _ := raw.([]any)
				for _, item := range row {
					button, _ := item.(map[string]any)
					if strings.HasPrefix(fmt.Sprint(button["text"]), label) {
						token, _ := button["callback_data"].(string)
						if token != "" {
							return token, index + 1
						}
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("native inbox did not expose %s: deliveries=%v", label, provider.Delivery(3))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelEditLossResendQuotedInput(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	countPromptEdits := func() int {
		count := 0
		for _, edit := range provider.Edits() {
			if strings.Contains(fmt.Sprint(edit["text"]), "Input: reason (text) required") {
				count++
			}
		}
		return count
	}
	for countPromptEdits() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("lost prompt edit never reached provider: %v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(2200 * time.Millisecond)
	if got := countPromptEdits(); got != 1 {
		t.Fatalf("uncertain prompt edit retried without consent %d times", got)
	}
	token, responseMessageID := requestChannelRecoveryAction(t, provider, callbackURL, signing, "Resend card")
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1700,
		"callback_query": map[string]any{
			"id": "callback-resend-card", "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": responseMessageID, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    token,
		},
	}); len(admitted) != 0 {
		t.Fatalf("resend-card action escaped into business events: %v", admitted)
	}
	resendMessageID := 0
	for resendMessageID == 0 {
		for index := 2; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), "Input: reason (text) required") {
				resendMessageID = index + 1
				break
			}
		}
		if resendMessageID != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("manual resend did not deliver the active prompt: deliveries=%v", provider.Delivery(3))
		}
		time.Sleep(20 * time.Millisecond)
	}
	answer := "private answer after explicit resend"
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 1800,
		"message": map[string]any{
			"message_id": 9120, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": answer,
			"reply_to_message": map[string]any{"message_id": resendMessageID},
		},
	}); len(admitted) != 0 {
		t.Fatalf("quoted answer escaped into business events: %v", admitted)
	}
	for {
		for _, edit := range provider.Edits() {
			visible := fmt.Sprint(edit["text"])
			if strings.Contains(visible, answer) {
				t.Fatalf("private answer echoed after resend: %v", edit)
			}
			if strings.Contains(visible, "Decision: retire") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("quoted answer to resent card did not finish the draft: edits=%v", provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelSharedAudience(t *testing.T, provider *telegramapi.Double, callbackURL, signing string) {
	t.Helper()
	command := waitNativeInboxCommand(t, provider, "chat_member", "-1001", "7000")
	deadline := time.Now().Add(25 * time.Second)
	var card map[string]any
	for card == nil {
		card = provider.Delivery(1)
		if time.Now().After(deadline) {
			t.Fatal("shared destination received no initial card")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fmt.Sprint(card["chat_id"]) != "-1001" || !strings.HasPrefix(fmt.Sprint(card["text"]), "Retire service") {
		t.Fatalf("shared card was not disclosed to the confirmed group: %v", card)
	}
	markup, _ := card["reply_markup"].(map[string]any)
	rows, _ := markup["inline_keyboard"].([]any)
	if len(rows) == 0 {
		t.Fatalf("shared card has no verdict controls: %v", card)
	}
	first, _ := rows[0].([]any)
	if len(first) == 0 {
		t.Fatalf("shared card has an empty verdict row: %v", card)
	}
	button, _ := first[0].(map[string]any)
	verdictToken, _ := button["callback_data"].(string)
	if verdictToken == "" {
		t.Fatalf("shared card has no verdict token: %v", card)
	}
	postGroupCallback := func(updateID int64, callbackID string, member int64) {
		t.Helper()
		if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
			"update_id": updateID,
			"callback_query": map[string]any{
				"id": callbackID, "from": map[string]any{"id": member},
				"message": map[string]any{"message_id": 2, "chat": map[string]any{"id": -1001, "type": "group"}},
				"data":    verdictToken,
			},
		}); len(admitted) != 0 {
			t.Fatalf("shared card callback escaped into business events: %v", admitted)
		}
	}
	postGroupCallback(time.Now().UnixMilli()+2000, "shared-foreign-member", 7001)
	time.Sleep(250 * time.Millisecond)
	for _, edit := range provider.Edits() {
		if strings.Contains(fmt.Sprint(edit["text"]), "Decision: retire") {
			t.Fatalf("foreign group member decided the card: %v", edit)
		}
	}
	beforeForeignEntry := 0
	for provider.Delivery(beforeForeignEntry) != nil {
		beforeForeignEntry++
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 2050,
		"message": map[string]any{
			"message_id": 9129, "from": map[string]any{"id": 7001},
			"chat": map[string]any{"id": -1001, "type": "group"},
			"text": "/" + command + "@SwarmTestBot",
		},
	}); len(admitted) != 0 {
		t.Fatalf("foreign group native entry escaped into business events: %v", admitted)
	}
	time.Sleep(350 * time.Millisecond)
	for index := beforeForeignEntry; ; index++ {
		delivery := provider.Delivery(index)
		if delivery == nil {
			break
		}
		if strings.HasPrefix(fmt.Sprint(delivery["text"]), "Inbox\n") {
			t.Fatalf("foreign group member received operator inbox: %v", delivery)
		}
	}
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 2100,
		"message": map[string]any{
			"message_id": 9130, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": -1001, "type": "group"},
			"text": "/" + command + "@SwarmTestBot",
		},
	}); len(admitted) != 0 {
		t.Fatalf("shared native entry escaped into business events: %v", admitted)
	}
	for {
		for index := 2; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.HasPrefix(fmt.Sprint(delivery["text"]), "Inbox\n") {
				if fmt.Sprint(delivery["chat_id"]) != "-1001" {
					t.Fatalf("shared inbox response escaped to another conversation: %v", delivery)
				}
				goto inboxVisible
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared native entry produced no visible inbox: %v", provider.Delivery(2))
		}
		time.Sleep(20 * time.Millisecond)
	}
inboxVisible:
	if admitted := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
		"update_id": time.Now().UnixMilli() + 2200,
		"message": map[string]any{
			"message_id": 9131, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": -1001, "type": "group"},
			"text": "@SwarmTestBot ordinary business text",
		},
	}); len(admitted) == 0 {
		t.Fatal("addressed group business text did not enter the flow")
	}
	for {
		for index := 2; ; index++ {
			delivery := provider.Delivery(index)
			if delivery == nil {
				break
			}
			if strings.Contains(fmt.Sprint(delivery["text"]), "Notice: Observed ordinary business text") {
				if fmt.Sprint(delivery["chat_id"]) != "-1001" {
					t.Fatalf("shared notice changed audience: %v", delivery)
				}
				goto noticeVisible
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared destination received no future notice: %v", provider.Delivery(3))
		}
		time.Sleep(20 * time.Millisecond)
	}
noticeVisible:
	postGroupCallback(time.Now().UnixMilli()+2300, "shared-authorized-member", 7000)
	for {
		for _, edit := range provider.Edits() {
			if strings.Contains(fmt.Sprint(edit["text"]), "Decision: retire") {
				if fmt.Sprint(edit["chat_id"]) != "-1001" {
					t.Fatalf("shared card update changed audience: %v", edit)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("authorized group verdict did not update its card: %v", provider.Edits())
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
