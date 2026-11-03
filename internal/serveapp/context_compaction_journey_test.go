package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func TestContextCompactionPreservesMemoryAcrossRestartBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, true)
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
			const model = "claude-3-haiku-20240307"
			const summary = "Context checkpoint: the first fact is cedar-731. Continue the pending conversation."
			var summaries, turns atomic.Int32
			var compacted atomic.Bool
			provider := llmapi.New(t, llmapi.Anthropic, "/v1/messages", "compaction-key", func(request llmapi.Request) (llmapi.Reply, error) {
				var body struct {
					Messages []struct {
						Role    string          `json:"role"`
						Content json.RawMessage `json:"content"`
					} `json:"messages"`
				}
				if err := json.Unmarshal(request.Body, &body); err != nil || len(body.Messages) == 0 {
					return llmapi.Reply{}, fmt.Errorf("compaction journey has no provider messages: %v", err)
				}
				if bytes.Contains(body.Messages[len(body.Messages)-1].Content, []byte("Context compaction:")) {
					if !bytes.Contains(request.Body, []byte("cedar-731")) || !bytes.Contains(request.Body, []byte("emit_telegram_reply_requested")) {
						return llmapi.Reply{}, fmt.Errorf("summary lost its original facts or frozen tools")
					}
					summaries.Add(1)
					return llmapi.Reply{Model: model, Text: summary, InputTokens: 125000, OutputTokens: 20}, nil
				}
				turn := turns.Add(1)
				if turn > 1 && bytes.Contains(request.Body, []byte(summary)) && !bytes.Contains(request.Body, []byte(strings.Repeat("shadowed ", 100))) {
					compacted.Store(true)
				}
				arguments, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": fmt.Sprintf("compaction reply %d", turn)})
				return llmapi.Reply{ID: fmt.Sprintf("compaction-response-%d", turn), Model: model, InputTokens: 125000, OutputTokens: 8,
					Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("compaction-reply-%d", turn), Name: "emit_telegram_reply_requested", Arguments: arguments}}}, nil
			})
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
			configureProviderJourneyLLM(t, h.opts.ConfigPath, "anthropic", "", model)
			raw, err := os.ReadFile(h.opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			var configuration map[string]any
			if err := yaml.Unmarshal(raw, &configuration); err != nil {
				t.Fatal(err)
			}
			delete(configuration["llm"].(map[string]any), "anthropic")
			raw, err = yaml.Marshal(configuration)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.opts.ConfigPath, raw, 0o600); err != nil {
				t.Fatalf("write compaction provider selection: %v", err)
			}
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token": "bot-token", "telegram.webhook_signing_secret": "telegram-secret", "ANTHROPIC_API_KEY": "compaction-key",
			} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079", h.provider)
			h.start(t)
			defer h.stop(t)
			inputs := []string{"first fact cedar-731: " + strings.Repeat("shadowed ", 55555), "kept-tail: " + strings.Repeat("recent ", 8571), "What was the first fact?"}
			var sessionID, runID string
			for i, input := range inputs {
				if i == 2 {
					h.stop(t)
					h.start(t)
				}
				callback, signing, _ := h.provider.Registration()
				update := map[string]any{"update_id": 835900 + i, "message": map[string]any{
					"message_id": 835900 + i, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": input,
				}}
				accepted := postResponsesChannelUpdate(t, callback, signing, update, http.StatusAccepted)
				if runID != "" && accepted.RunID != runID {
					t.Fatalf("compaction replaced the live run: %s -> %s", runID, accepted.RunID)
				}
				runID = accepted.RunID
				current := requireProviderMemoryTurns(t, h.rpcEndpoint(), runID, i+1)
				if sessionID != "" && current != sessionID {
					t.Fatalf("compaction replaced the memory session: %s -> %s", sessionID, current)
				}
				sessionID = current
				waitUncertainCopyDelivery(t, h, i, fmt.Sprintf("compaction reply %d", i+1))
				if i > 0 && (summaries.Load() != 1 || !compacted.Load()) {
					t.Fatalf("context boundary was not summarized once and replayed: summaries=%d compacted=%t", summaries.Load(), compacted.Load())
				}
			}
			if turns.Load() != 3 || summaries.Load() != 1 {
				t.Fatalf("restart regenerated maintenance or business work: turns=%d summaries=%d", turns.Load(), summaries.Load())
			}
		})
	}
}
