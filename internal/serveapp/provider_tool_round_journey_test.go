package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func TestChannelOneBotOpenAICompatibleReasoningToolRound(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, true)
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
			agentPath := filepath.Join(h.opts.SourceRoot, "telegram-chat", "agents.yaml")
			body, err := os.ReadFile(agentPath)
			if err != nil {
				t.Fatal(err)
			}
			var agents map[string]any
			if err := yaml.Unmarshal(body, &agents); err != nil {
				t.Fatal(err)
			}
			agents["phrase-bot"].(map[string]any)["flow_data_access"] = []string{"customer.txt"}
			body, err = yaml.Marshal(agents)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(agentPath, body, 0o600); err != nil {
				t.Fatal(err)
			}
			dataDir := filepath.Join(h.opts.SourceRoot, "telegram-chat", "data")
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "customer.txt"), []byte("round customer\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "round-key", func(request llmapi.Request) (llmapi.Reply, error) {
				return reasoningToolRoundReply(t, request)
			})
			configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", provider.URL+"/v1")
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token": "bot-token", "telegram.webhook_signing_secret": "telegram-secret", "OPENAI_COMPATIBLE_API_KEY": "round-key",
			} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			registerChannelFixtureAccount(t, h.opts.ConfigPath, "bot-token", "420079", h.provider)
			h.start(t)
			defer h.stop(t)
			callback, signing, _ := h.provider.Registration()
			accepted := postResponsesChannelUpdate(t, callback, signing, map[string]any{
				"update_id": 835901, "message": map[string]any{
					"message_id": 835901, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": "reasoning customer",
				},
			}, http.StatusAccepted)
			sessionID := requireProviderMemoryTurns(t, h.rpcEndpoint(), accepted.RunID, 2)
			const reply = "Swarm heard: round customer"
			waitUncertainCopyDelivery(t, h, 0, reply)
			requireReceptionistReplies(t, h, map[string]string{reply: "2001"})
			if len(provider.Requests()) != 2 {
				t.Fatalf("requests=%d, want one non-emit tool round", len(provider.Requests()))
			}
			next := postResponsesChannelUpdate(t, callback, signing, map[string]any{
				"update_id": 835902, "message": map[string]any{
					"message_id": 835902, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": "remember the completed round",
				},
			}, http.StatusAccepted)
			if next.RunID != accepted.RunID || requireProviderMemoryTurns(t, h.rpcEndpoint(), next.RunID, 3) != sessionID {
				t.Fatal("completed tool round did not retain its run/session")
			}
			const nextReply = "Swarm heard: remembered customer"
			waitUncertainCopyDelivery(t, h, 1, nextReply)
			requireReceptionistReplies(t, h, map[string]string{reply: "2001", nextReply: "2001"})
			if len(provider.Requests()) != 3 {
				t.Fatalf("requests=%d, want completed round plus one new user request", len(provider.Requests()))
			}
		})
	}
}

const toolRoundReasoning = "Read the admitted customer file before replying."

func reasoningToolRoundReply(t *testing.T, request llmapi.Request) (llmapi.Reply, error) {
	t.Helper()
	var body struct {
		Messages []struct {
			Role       string          `json:"role"`
			Content    string          `json:"content"`
			Reasoning  string          `json:"reasoning_content"`
			ToolCallID string          `json:"tool_call_id"`
			ToolCalls  json.RawMessage `json:"tool_calls"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name       string `json:"name"`
				Parameters struct {
					Properties map[string]json.RawMessage `json:"properties"`
				} `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	var staticID string
	for _, tool := range body.Tools {
		if tool.Function.Name == "read_flow_data" {
			var ids struct {
				Enum []string `json:"enum"`
			}
			if err := json.Unmarshal(tool.Function.Parameters.Properties["static_id"], &ids); err == nil && len(ids.Enum) == 1 {
				staticID = ids.Enum[0]
			}
		}
	}
	if staticID == "" {
		return llmapi.Reply{}, fmt.Errorf("request lost its exact authored data tool")
	}
	args, _ := json.Marshal(map[string]string{"kind": "static_file", "static_id": staticID})
	if request.Number == 1 {
		return llmapi.Reply{ID: "round-read", Model: "strict-thinking", Reasoning: toolRoundReasoning, InputTokens: 10, OutputTokens: 4, TotalTokens: 14,
			Tools: []llmapi.ToolCall{{ID: "round-call", Name: "read_flow_data", Arguments: args}}}, nil
	}
	if request.Number == 3 {
		read, err := retainedProviderToolResult(request, "round-call", "read_flow_data")
		if err != nil {
			return llmapi.Reply{}, err
		}
		emit, err := retainedProviderToolResult(request, "round-reply", "emit_telegram_reply_requested")
		if err != nil || read >= emit {
			return llmapi.Reply{}, fmt.Errorf("completed round lost ordered call/results: %v", err)
		}
		reply, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": "Swarm heard: remembered customer"})
		return llmapi.Reply{ID: "round-next", Model: "strict-thinking", InputTokens: 15, OutputTokens: 4, TotalTokens: 19,
			Tools: []llmapi.ToolCall{{ID: "round-next-reply", Name: "emit_telegram_reply_requested", Arguments: reply}}}, nil
	}
	if request.Number != 2 || len(body.Messages) < 2 {
		return llmapi.Reply{}, fmt.Errorf("unexpected tool-round request %d", request.Number)
	}
	assistant, tool := body.Messages[len(body.Messages)-2], body.Messages[len(body.Messages)-1]
	var calls []struct {
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	if err := json.Unmarshal(assistant.ToolCalls, &calls); err != nil || assistant.Role != "assistant" || assistant.Content != "" || len(calls) != 1 || calls[0].ID != "round-call" || calls[0].Function.Name != "read_flow_data" || calls[0].Function.Arguments != string(args) || tool.Role != "tool" || tool.ToolCallID != "round-call" {
		const refusal = `{"error":{"message":"Messages with role 'tool' must follow the assistant's exact tool_calls"}}`
		t.Logf("strict provider returned HTTP 400: %s", refusal)
		return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: refusal}, nil
	}
	if assistant.Reasoning != toolRoundReasoning {
		const refusal = `{"error":{"message":"The reasoning_content in the thinking mode must be passed back to the API"}}`
		t.Logf("strict provider returned HTTP 400: %s", refusal)
		return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: refusal}, nil
	}
	payload, err := providerJourneyToolResult(tool.Content, "round-call")
	if err != nil {
		return llmapi.Reply{}, err
	}
	var result struct {
		Result struct {
			StaticID string `json:"static_id"`
			Content  string `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(payload, &result); err != nil || result.Result.StaticID != staticID || result.Result.Content != "round customer\n" {
		return llmapi.Reply{}, fmt.Errorf("continuation lost its genuine read result")
	}
	reply, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": "Swarm heard: round customer"})
	return llmapi.Reply{ID: "round-emit", Model: "strict-thinking", InputTokens: 12, OutputTokens: 4, TotalTokens: 16,
		Tools: []llmapi.ToolCall{{ID: "round-reply", Name: "emit_telegram_reply_requested", Arguments: reply}}}, nil
}

func retainedProviderToolResult(request llmapi.Request, callID, name string) (int, error) {
	var body struct {
		Messages []struct {
			Role       string `json:"role"`
			Content    string `json:"content"`
			ToolCallID string `json:"tool_call_id"`
			ToolCalls  []struct {
				ID       string `json:"id"`
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return 0, err
	}
	for i, message := range body.Messages {
		for _, call := range message.ToolCalls {
			if message.Role != "assistant" || call.ID != callID || call.Function.Name != name {
				continue
			}
			if i+1 >= len(body.Messages) || body.Messages[i+1].Role != "tool" || body.Messages[i+1].ToolCallID != callID {
				return 0, fmt.Errorf("stored assistant call %s lost its immediately following result", callID)
			}
			payload, err := providerJourneyToolResult(body.Messages[i+1].Content, callID)
			if err != nil {
				return 0, err
			}
			var result struct {
				Name string `json:"name"`
				OK   bool   `json:"ok"`
			}
			if err := json.Unmarshal(payload, &result); err != nil || result.Name != name || !result.OK {
				return 0, fmt.Errorf("stored result %s lost its actual successful outcome", callID)
			}
			return i, nil
		}
	}
	return 0, fmt.Errorf("stored conversation lost prior assistant tool call %s", callID)
}
