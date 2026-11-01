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
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelOneBotManyConversationsPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend)+"/dynamic_holder", func(t *testing.T) {
			runChannelDynamicHolderLivePickup(t, backend)
		})
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			backendName := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				backendName = "postgres"
			}
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
			llm := &receptionistAnthropicDouble{t: t, history: map[string][]string{}}
			llmServer := httptest.NewServer(llm)
			t.Cleanup(llmServer.Close)
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": llmServer.URL})
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "telegram.telegram_bot_token", "bot-token"); err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "telegram.webhook_signing_secret", "boot-signing-secret"); err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "ANTHROPIC_API_KEY", "anthropic-key"); err != nil {
				t.Fatal(err)
			}
			bundleHash := servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot)
			h.start(t)
			defer h.stop(t)
			config, err := os.ReadFile(h.opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, h.opts.ConfigPath, string(config)+"\nconnection:\n  api_server: "+h.endpoint+"\n")
			registered := registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079", h.provider)
			var cardMessageID int
			var token string
			if !registered.DeploymentRegistration {
				connected := runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider,
					"connect", "bot-token", 1001, "private", 0)
				if connected.Readiness == nil || !connected.Readiness.Ready {
					t.Fatalf("public connection is not ready: %#v", connected)
				}
				db, err := sql.Open(backendName, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				cardMessageID = waitChannelCardMessageID(t, db, backendName, ".")
				card := h.provider.Delivery(cardMessageID - 1)
				if fmt.Sprint(card["chat_id"]) != "1001" {
					t.Fatalf("operator card escaped operator conversation: %v", card)
				}
				var found bool
				token, found = telegramCallbackToken(card, "retire")
				if !found {
					t.Fatalf("operator card has no retire action: %v", card)
				}
			} else {
				registeredChannelMailboxCLI(t, h, true, "list")
			}
			beforeMailbox := receptionistPendingMailbox(t, h)
			var replies []string
			expectedDestinations := map[string]string{}
			send := func(updateID, chatID int64, text string) {
				t.Helper()
				callbackURL, signing, _ := h.provider.Registration()
				names := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
					"update_id": updateID,
					"message": map[string]any{
						"message_id": updateID, "from": map[string]any{"id": chatID},
						"chat": map[string]any{"id": chatID, "type": "private"}, "text": text,
					},
				})
				if !slices.Contains(names, "inbound.telegram.text_message") {
					t.Fatalf("customer text was not publicly admitted: %v", names)
				}
				reply := "Swarm heard: " + text
				replies = append(replies, reply)
				expectedDestinations[reply] = fmt.Sprint(chatID)
				waitForStandingMemoryCompletion(t, backendName, h.storeDSN, "live", len(replies))
			}
			send(83101, 2001, "store A customer A first")
			send(83102, 2002, "store A customer B first")
			send(83103, 2001, "store A customer A second")
			send(83104, 2002, "store A customer B second")
			requireStandingPayloadOnlyTargetReadback(t, h.endpoint, bundleHash, "live", 2, replies)
			before := loadStandingMemorySessions(t, backendName, h.storeDSN)
			requireReceptionistMemory(t, before, 2)
			requireReceptionistReplies(t, h, expectedDestinations)
			// Channel-token rejection remains deferred; there is no registered
			// channel-card token in the CLI-only v0.7 journey (6526882297).
			if !registered.DeploymentRegistration {
				for index, foreign := range []struct{ user, chat int64 }{
					{2001, 1001}, {7000, 2001}, {2002, 2002},
				} {
					callbackURL, signing, _ := h.provider.Registration()
					providerID := fmt.Sprintf("receptionist-foreign-%d", index)
					if names := postChannelTelegramUpdate(t, callbackURL, signing, map[string]any{
						"update_id": 83200 + index,
						"callback_query": map[string]any{
							"id": providerID, "from": map[string]any{"id": foreign.user},
							"message": map[string]any{"message_id": cardMessageID,
								"chat": map[string]any{"id": foreign.chat, "type": "private"}},
							"data": token,
						},
					}); len(names) != 0 {
						t.Fatalf("customer callback became business authority: %v", names)
					}
					waitReceptionistActionRejection(t, h, fmt.Sprint(83200+index))
				}
			}
			if after := receptionistPendingMailbox(t, h); !reflect.DeepEqual(beforeMailbox, after) {
				t.Fatalf("customer callbacks mutated operator inbox: before=%v after=%v", beforeMailbox, after)
			}
			registrations, _ := h.provider.Counts()
			if registrations != 1 {
				t.Fatalf("conversation routing created %d registrations, want one physical connection", registrations)
			}
			h.stop(t)
			h.start(t)
			if after := receptionistPendingMailbox(t, h); !reflect.DeepEqual(beforeMailbox, after) {
				t.Fatalf("restart mutated operator inbox: before=%v after=%v", beforeMailbox, after)
			}
			send(83105, 2002, "store A customer B after restart")
			send(83106, 2001, "store A customer A after restart")
			requireStandingPayloadOnlyTargetReadback(t, h.endpoint, bundleHash, "live", 2, replies)
			after := loadStandingMemorySessions(t, backendName, h.storeDSN)
			requireReceptionistMemory(t, after, 3)
			for key, prior := range before {
				current := after[key]
				if current.SessionID != prior.SessionID || current.RunID != prior.RunID || current.AgentID != prior.AgentID {
					t.Fatalf("restart replaced conversation memory: before=%#v after=%#v", prior, current)
				}
			}
			requireReceptionistReplies(t, h, expectedDestinations)
		})
	}
}

func runChannelDynamicHolderLivePickup(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	root := h.opts.SourceRoot
	writeChannelOnboardingLiveToolCredentials(t, h.credentialPath)
	if err := os.Remove(filepath.Join(root, "policy.yaml")); err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, filepath.Join(root, "entities.yaml"), "telegram_service:\n  operator_account: text\n")
	writeStandingCandidateFile(t, filepath.Join(root, "events.yaml"), "account.assigned:\n  operator_account: text\n")
	writeStandingCandidateFile(t, filepath.Join(root, "schema.yaml"), "stages: {active: {}}\npins:\n  inputs: [inbound.telegram.text_message, account.assigned]\n")
	writeStandingCandidateFile(t, filepath.Join(root, "nodes.yaml"), `assign-account:
  execution_type: system_node
  subscribes_to: [account.assigned]
  event_handlers:
    account.assigned:
      data_accumulation:
        source_event: account.assigned
        writes: [{target_field: operator_account, value: payload.operator_account}]
`)
	registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079")
	hash := servedEventPublishFixtureBundleHash(t, root)
	h.start(t)
	defer h.stop(t)
	if registrations, _ := h.provider.Counts(); registrations != 0 {
		t.Fatal("an account without a holder acquired a webhook")
	}
	var listed struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.list", map[string]any{"bundle_hash": hash, "limit": 100}, &listed)
	runID := ""
	for _, run := range listed.Runs {
		if run.FlowPath == "." && run.Status == "live" && run.BundleHash == hash {
			if runID != "" {
				t.Fatal("more than one current account-holder run")
			}
			runID = run.RunID
		}
	}
	if runID == "" {
		t.Fatal("missing current account-holder run")
	}
	var published struct {
		RunID   string `json:"run_id"`
		EventID string `json:"event_id"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.publish", map[string]any{
		"run_id": runID, "event_name": "account.assigned", "idempotency_key": "assign-registered-account",
		"payload": map[string]any{"operator_account": "420079"},
	}, &published)
	if published.RunID != runID || published.EventID == "" {
		t.Fatal("field update did not target the existing holder run")
	}
	callback, secret := waitChannelOnboardingRegistration(t, h.provider, &lockedBuffer{}, &lockedBuffer{}, nil)
	deadline := time.Now().Add(15 * time.Second)
	for {
		response, err := http.Get(callback)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode == http.StatusMethodNotAllowed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registered callback did not become current: HTTP %d", response.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if names := postChannelTelegramUpdate(t, callback, secret, map[string]any{
		"update_id": 84101, "message": map[string]any{"message_id": 84101, "from": map[string]any{"id": 2001},
			"chat": map[string]any{"id": 2001, "type": "private"}, "text": "new holder customer"},
	}); !slices.Contains(names, "inbound.telegram.text_message") {
		t.Fatalf("field-only holder did not admit customer traffic: %v", names)
	}
}

func requireReceptionistMemory(t *testing.T, sessions map[string]standingMemorySession, wantTurns int) {
	t.Helper()
	if len(sessions) != 2 {
		t.Fatalf("conversation sessions=%#v, want two independent customers", sessions)
	}
	ids := map[string]bool{}
	for _, session := range sessions {
		if session.FlowTemplate != "telegram-chat" || session.TurnCount != wantTurns || session.SessionID == "" || ids[session.SessionID] {
			t.Fatalf("conversation memory is not independent: %#v", sessions)
		}
		ids[session.SessionID] = true
	}
}

func requireReceptionistReplies(t *testing.T, h *channelOnboardingE2EHarness, expected map[string]string) {
	t.Helper()
	seen := map[string]int{}
	for index := 0; ; index++ {
		delivery := h.provider.Delivery(index)
		if delivery == nil {
			break
		}
		text := fmt.Sprint(delivery["text"])
		chat := fmt.Sprint(delivery["chat_id"])
		if strings.HasPrefix(text, "Swarm heard: ") {
			if want, ok := expected[text]; !ok || chat != want {
				t.Fatalf("reply was misaddressed or unexpected: %v", delivery)
			}
			seen[text]++
		} else if chat != "1001" {
			t.Fatalf("operator content escaped to a customer: %v", delivery)
		}
	}
	for text := range expected {
		if seen[text] != 1 {
			t.Fatalf("reply %q delivery count=%d, want exactly one", text, seen[text])
		}
	}
}

func receptionistPendingMailbox(t *testing.T, h *channelOnboardingE2EHarness) []any {
	t.Helper()
	var result struct {
		Items []any `json:"items"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.list", map[string]any{"status": "pending", "limit": 200}, &result)
	if len(result.Items) == 0 {
		t.Fatal("operator inbox has no pending card")
	}
	return result.Items
}

func waitReceptionistActionRejection(t *testing.T, h *channelOnboardingE2EHarness, providerID string) {
	t.Helper()
	providerID = channelFixtureProviderEventID(t, providerID)
	driver, placeholder := "sqlite", "?"
	if h.backend == servedparity.BackendExplicitPostgres {
		driver, placeholder = "postgres", "$1"
	}
	db, err := sql.Open(driver, h.storeDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var state, disposition string
		err := db.QueryRow("SELECT state, COALESCE(disposition, '') FROM operator_channel_action_intents WHERE provider_event_id="+placeholder, providerID).Scan(&state, &disposition)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err == nil && state == "settled" {
			if disposition != "rejected" {
				t.Fatalf("foreign action disposition=%q", disposition)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("foreign customer action did not reach durable rejection")
}

type receptionistAnthropicDouble struct {
	t       *testing.T
	mu      sync.Mutex
	history map[string][]string
}

func (p *receptionistAnthropicDouble) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/v1/messages" || request.Header.Get("x-api-key") != "anthropic-key" {
		p.t.Errorf("unexpected managed LLM request: %s", request.URL.Path)
		http.Error(w, "unexpected managed LLM request", http.StatusUnauthorized)
		return
	}
	var body struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Messages) == 0 {
		p.t.Errorf("invalid managed LLM request: %v", err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	content := body.Messages[len(body.Messages)-1].Content
	w.Header().Set("Content-Type", "application/json")
	if content == "ok" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "claude-test", "usage": map[string]int{"input_tokens": 8, "output_tokens": 2},
			"content": []any{map[string]any{"type": "text", "text": "Telegram reply sent."}},
		})
		return
	}
	var turn struct {
		Conversation string `json:"conversation_reference"`
		Text         string `json:"text"`
	}
	if err := json.Unmarshal([]byte(content), &turn); err != nil || turn.Conversation == "" {
		p.t.Errorf("invalid managed provider frame: %v content=%s", err, content)
		http.Error(w, "invalid frame", http.StatusBadRequest)
		return
	}
	chatID, text := turn.Conversation, turn.Text
	joined := ""
	for _, message := range body.Messages {
		joined += message.Content + "\n"
	}
	p.mu.Lock()
	for chat, texts := range p.history {
		for _, prior := range texts {
			contains := strings.Contains(joined, prior)
			if contains != (chat == chatID) {
				p.t.Errorf("managed memory for chat %s contains prior %q=%t, prior chat=%s", chatID, prior, contains, chat)
			}
		}
	}
	p.history[chatID] = append(p.history[chatID], text)
	p.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"model": "claude-test", "usage": map[string]int{"input_tokens": 12, "output_tokens": 4},
		"content": []any{map[string]any{
			"type": "tool_use", "id": "reply-" + chatID, "name": "emit_telegram_reply_requested",
			"input": map[string]any{"chat_id": chatID, "text": "Swarm heard: " + text},
		}},
	})
}
