package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func TestChannelOneBotOpenAIResponsesPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, true)
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
			agentPath := filepath.Join(h.opts.SourceRoot, "telegram-chat", "agents.yaml")
			agentBody, err := os.ReadFile(agentPath)
			if err != nil {
				t.Fatal(err)
			}
			var agents map[string]any
			if err := yaml.Unmarshal(agentBody, &agents); err != nil {
				t.Fatal(err)
			}
			agents["phrase-bot"].(map[string]any)["flow_data_access"] = []string{"customer.txt"}
			agentBody, err = yaml.Marshal(agents)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(agentPath, agentBody, 0o600); err != nil {
				t.Fatal(err)
			}
			dataDir := filepath.Join(h.opts.SourceRoot, "telegram-chat", "data")
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "customer.txt"), []byte("Responses customer\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			const reply = "Swarm heard: Responses customer"
			provider := llmapi.New(t, llmapi.OpenAIResponses, "/v1/responses", "responses-key", func(request llmapi.Request) (llmapi.Reply, error) {
				return channelResponsesReply(request, reply)
			})
			configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_responses", provider.URL+"/v1")
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token": "bot-token", "telegram.webhook_signing_secret": "telegram-secret", "OPENAI_API_KEY": "responses-key",
			} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079", h.provider)
			h.start(t)
			defer h.stop(t)
			callback, signing, _ := h.provider.Registration()
			update := map[string]any{"update_id": 835101, "message": map[string]any{
				"message_id": 835101, "from": map[string]any{"id": 2001},
				"chat": map[string]any{"id": 2001, "type": "private"}, "text": "Responses customer",
			}}
			postResponsesChannelUpdate(t, callback, "wrong-secret", update, http.StatusUnauthorized)
			if len(provider.Requests()) != 0 {
				t.Fatal("unauthenticated channel input reached the LLM")
			}
			accepted := postResponsesChannelUpdate(t, callback, signing, update, http.StatusAccepted)
			if accepted.Status != "accepted" || accepted.PublicationID == "" || len(accepted.EventIDs) != 2 || !slices.Contains(accepted.EventNames, "inbound.telegram.text_message") {
				t.Fatalf("Responses customer input was not admitted: %+v", accepted)
			}
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			waitForStandingMemoryCompletion(t, driver, h.storeDSN, "live", 1)
			waitUncertainCopyDelivery(t, h, 0, reply)
			requireReceptionistReplies(t, h, map[string]string{reply: "2001"})
			requireStandingPayloadOnlyTargetReadback(t, h.endpoint, servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot), "live", 1, []string{reply})
			replayed := postResponsesChannelUpdate(t, callback, signing, update, http.StatusOK)
			if replayed.Status != "duplicate" || replayed.PublicationID != accepted.PublicationID || replayed.RunID != accepted.RunID || !slices.Equal(replayed.EventIDs, accepted.EventIDs) || !slices.Equal(replayed.EventNames, accepted.EventNames) {
				t.Fatalf("replayed update did not retain the exact receipt: before=%+v after=%+v", accepted, replayed)
			}
			if len(provider.Requests()) != 2 {
				t.Fatalf("Responses requests=%d, want one tool roundtrip", len(provider.Requests()))
			}
			requireReceptionistReplies(t, h, map[string]string{reply: "2001"})
			db, err := sql.Open(driver, h.storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var completions, input, output int
			if err := db.QueryRow(`SELECT COUNT(*),SUM(input_tokens),SUM(output_tokens) FROM spend_ledger
				WHERE backend_profile='openai_responses' AND provider='openai' AND transport='api'
				AND resolved_model='gpt-5.4-nano' AND execution_mode='live'`).Scan(&completions, &input, &output); err != nil || completions != 2 || input != 25 || output != 9 {
				t.Fatalf("Responses committed evidence: completions=%d input=%d output=%d err=%v", completions, input, output, err)
			}
		})
	}
}

type responsesChannelReceipt struct {
	Status        string   `json:"status"`
	PublicationID string   `json:"publication_id"`
	RunID         string   `json:"run_id"`
	EventIDs      []string `json:"event_ids"`
	EventNames    []string `json:"event_names"`
}

func postResponsesChannelUpdate(t *testing.T, endpoint, signing string, update map[string]any, status int) responsesChannelReceipt {
	t.Helper()
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
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
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != status {
		t.Fatalf("channel update status=%d want=%d body=%s err=%v", response.StatusCode, status, raw, err)
	}
	var receipt responsesChannelReceipt
	if status != http.StatusUnauthorized {
		if err := json.Unmarshal(raw, &receipt); err != nil {
			t.Fatal(err)
		}
	}
	return receipt
}

func channelResponsesReply(request llmapi.Request, text string) (llmapi.Reply, error) {
	var body struct {
		Model string `json:"model"`
		Tools []struct {
			Name       string `json:"name"`
			Parameters struct {
				Properties map[string]json.RawMessage `json:"properties"`
			} `json:"parameters"`
		} `json:"tools"`
		Input []struct {
			Type    string `json:"type"`
			CallID  string `json:"call_id"`
			Content string `json:"content"`
			Output  string `json:"output"`
		} `json:"input"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	var staticID string
	var hasReply bool
	for _, tool := range body.Tools {
		hasReply = hasReply || tool.Name == "emit_telegram_reply_requested"
		if tool.Name == "read_flow_data" {
			var ids struct {
				Enum []string `json:"enum"`
			}
			if err := json.Unmarshal(tool.Parameters.Properties["static_id"], &ids); err == nil && len(ids.Enum) == 1 {
				staticID = ids.Enum[0]
			}
		}
	}
	if body.Model != "fixture-model" || !hasReply || staticID == "" {
		return llmapi.Reply{}, fmt.Errorf("Responses request lost its selected model or authored reply tool")
	}
	switch request.Number {
	case 1:
		if !strings.Contains(string(request.Body), "Responses customer") || !strings.Contains(string(request.Body), `\"kind\":\"initial\"`) {
			return llmapi.Reply{}, fmt.Errorf("Responses request lost the real channel execution frame")
		}
		args, _ := json.Marshal(map[string]string{"kind": "static_file", "static_id": staticID})
		return llmapi.Reply{ID: "response-1", Model: "gpt-5.4-nano", InputTokens: 12, OutputTokens: 4, TotalTokens: 16,
			Tools: []llmapi.ToolCall{{ID: "customer-read", OutputID: "read-call", Name: "read_flow_data", Arguments: args}}}, nil
	case 2:
		for _, input := range body.Input {
			if input.Type == "function_call_output" && input.CallID == "customer-read" {
				payload, err := providerJourneyToolResult(input.Output, input.CallID)
				if err != nil {
					return llmapi.Reply{}, err
				}
				var observed struct {
					OK     bool `json:"ok"`
					Result struct {
						StaticID string `json:"static_id"`
						Content  string `json:"content"`
					} `json:"result"`
				}
				if err := json.Unmarshal(payload, &observed); err != nil || !observed.OK || observed.Result.StaticID != staticID || observed.Result.Content != "Responses customer\n" {
					return llmapi.Reply{}, fmt.Errorf("Responses continuation lost its genuine read-only tool result")
				}
				args, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": text})
				return llmapi.Reply{ID: "response-2", Model: "gpt-5.4-nano", InputTokens: 13, OutputTokens: 5, TotalTokens: 18,
					Tools: []llmapi.ToolCall{{ID: "channel-reply", OutputID: "channel-call", Name: "emit_telegram_reply_requested", Arguments: args}}}, nil
			}
		}
		return llmapi.Reply{}, fmt.Errorf("Responses continuation lacks the real read-only tool result")
	default:
		return llmapi.Reply{}, fmt.Errorf("unexpected Responses request %d", request.Number)
	}
}

func providerJourneyToolResult(content, callID string) (json.RawMessage, error) {
	results, err := agentframe.DecodeProviderToolResults(content)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 || results[0].CallID != callID || !results[0].OK {
		return nil, fmt.Errorf("provider continuation lacks the exact successful tool result %q", callID)
	}
	return results[0].Payload, nil
}

func configureProviderJourneyLLM(t *testing.T, path, backend, baseURL string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	llm := document["llm"].(map[string]any)
	llm["backend"] = backend
	llm[backend] = map[string]any{"base_url": baseURL}
	llm["models"] = map[string]any{"regular": map[string]any{backend: "fixture-model"}}
	raw, err = yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func denyProviderJourneyExternalNetwork(t *testing.T) {
	t.Helper()
	base := http.DefaultTransport
	http.DefaultTransport = telegramAgentRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		host := request.URL.Hostname()
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("provider journey refused unexpected external host %s", host)
		}
		return base.RoundTrip(request)
	})
	t.Cleanup(func() { http.DefaultTransport = base })
}
