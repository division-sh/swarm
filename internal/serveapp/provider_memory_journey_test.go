package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

func TestChannelOneBotOpenAICompatibleEmptyMemoryJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, true)
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
			provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "memory-key", func(request llmapi.Request) (llmapi.Reply, error) {
				return emptyAssistantMemoryReply(t, request)
			})
			configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", provider.URL+"/v1")
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token": "bot-token", "telegram.webhook_signing_secret": "telegram-secret", "OPENAI_COMPATIBLE_API_KEY": "memory-key",
			} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079", h.provider)
			h.start(t)
			defer h.stop(t)
			callback, signing, _ := h.provider.Registration()
			replies := map[string]string{}
			var sessionID, runID string
			for turn := 1; turn <= 2; turn++ {
				update := map[string]any{"update_id": 835200 + turn, "message": map[string]any{
					"message_id": 835200 + turn, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": fmt.Sprintf("memory input %d", turn),
				}}
				accepted := postResponsesChannelUpdate(t, callback, signing, update, http.StatusAccepted)
				if accepted.RunID == "" || (runID != "" && accepted.RunID != runID) {
					t.Fatalf("memory conversation changed its run: before=%q after=%+v", runID, accepted)
				}
				runID = accepted.RunID
				current := requireProviderMemoryTurns(t, h.rpcEndpoint(), runID, turn)
				if sessionID != "" && current != sessionID {
					t.Fatalf("memory conversation rotated: before=%q after=%q", sessionID, current)
				}
				sessionID = current
				reply := fmt.Sprintf("Swarm heard: memory reply %d", turn)
				waitUncertainCopyDelivery(t, h, turn-1, reply)
				replies[reply] = "2001"
				requireReceptionistReplies(t, h, replies)
			}
			if len(provider.Requests()) != 2 {
				t.Fatalf("memory requests=%d, want exactly one per turn", len(provider.Requests()))
			}
		})
	}
}

func emptyAssistantMemoryReply(t *testing.T, request llmapi.Request) (llmapi.Reply, error) {
	t.Helper()
	var body struct {
		Messages []map[string]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	if request.Number < 1 || request.Number > 2 || !strings.Contains(string(request.Body), fmt.Sprintf("memory input %d", request.Number)) {
		return llmapi.Reply{}, fmt.Errorf("memory request lost its exact current turn")
	}
	assistants := 0
	for _, message := range body.Messages {
		if string(message["role"]) != `"assistant"` {
			continue
		}
		assistants++
		content, present := message["content"]
		if !present && len(message["tool_calls"]) == 0 {
			const refusal = `{"error":{"message":"Invalid assistant message: content or tool_calls must be set"}}`
			t.Logf("strict provider returned HTTP 400: %s", refusal)
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: refusal}, nil
		}
		if string(content) != `""` {
			return llmapi.Reply{}, fmt.Errorf("memory replay did not preserve the empty assistant content: %s", content)
		}
	}
	if assistants != request.Number-1 || (request.Number == 2 && !strings.Contains(string(request.Body), "memory input 1")) {
		return llmapi.Reply{}, fmt.Errorf("memory replay lost its first user/assistant pair")
	}
	if request.Number == 2 {
		if _, err := retainedProviderToolResult(request, "memory-reply-1", "emit_telegram_reply_requested"); err != nil {
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: err.Error()}, nil
		}
	}
	arguments, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": fmt.Sprintf("Swarm heard: memory reply %d", request.Number)})
	return llmapi.Reply{ID: fmt.Sprintf("memory-response-%d", request.Number), Model: "strict-compatible", InputTokens: 10, OutputTokens: 3, TotalTokens: 13,
		Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("memory-reply-%d", request.Number), Name: "emit_telegram_reply_requested", Arguments: arguments}}}, nil
}

func requireProviderMemoryTurns(t *testing.T, endpoint, runID string, want int) string {
	t.Helper()
	deadline := time.Now().Add(standingMemoryAsyncProofTimeout)
	for time.Now().Before(deadline) {
		var sessions operatorread.OperatorConversationListResult
		requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": runID}, &sessions)
		if len(sessions.Conversations) == 1 {
			session := sessions.Conversations[0]
			var turns operatorread.OperatorConversationTurnListResult
			requireServedJSONRPCResult(t, endpoint, "conversation.list_turns", map[string]any{"session_id": session.SessionID}, &turns)
			if len(turns.Turns) == want {
				for _, turn := range turns.Turns {
					if turn.Failure != nil || !turn.ParseOK {
						t.Fatalf("memory turn %d failed: %+v", turn.Ordinal, turn.Failure)
					}
				}
				return session.SessionID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("memory conversation did not retain %d provider turns", want)
	return ""
}
