package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
			prepareProviderRoundReadSource(t, h, "round customer\n")
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

func prepareProviderRoundReadSource(t *testing.T, h *channelOnboardingE2EHarness, content string) {
	t.Helper()
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
	if err := os.WriteFile(filepath.Join(dataDir, "customer.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestChannelOneBotAnthropicToolRound(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, true)
			prepareProviderRoundReadSource(t, h, "anthropic customer\n")
			provider := llmapi.New(t, llmapi.Anthropic, "/v1/messages", "anthropic-round-key", func(request llmapi.Request) (llmapi.Reply, error) {
				return anthropicToolRoundReply(t, request)
			})
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token": "bot-token", "telegram.webhook_signing_secret": "telegram-secret", "ANTHROPIC_API_KEY": "anthropic-round-key",
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
				"update_id": 835911, "message": map[string]any{
					"message_id": 835911, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": "read the Anthropic customer",
				},
			}, http.StatusAccepted)
			requireProviderMemoryTurns(t, h.rpcEndpoint(), accepted.RunID, 2)
			const reply = "Swarm heard: Anthropic customer"
			waitUncertainCopyDelivery(t, h, 0, reply)
			requireReceptionistReplies(t, h, map[string]string{reply: "2001"})
			if len(provider.Requests()) != 2 {
				t.Fatalf("Anthropic requests=%d, want one exact non-emit round", len(provider.Requests()))
			}
		})
	}
}

type anthropicRoundMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicRoundBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
}

func anthropicRoundBlocks(messages []anthropicRoundMessage) ([]anthropicRoundBlock, []anthropicRoundBlock, error) {
	var uses, results []anthropicRoundBlock
	seen, answered := map[string]bool{}, map[string]bool{}
	for _, message := range messages {
		if strings.HasPrefix(strings.TrimSpace(string(message.Content)), `"`) {
			continue
		}
		var blocks []anthropicRoundBlock
		if err := json.Unmarshal(message.Content, &blocks); err != nil {
			return nil, nil, err
		}
		for _, block := range blocks {
			switch block.Type {
			case "tool_use":
				if message.Role != "assistant" || block.ID == "" || seen[block.ID] {
					return nil, nil, fmt.Errorf("invalid or duplicate tool_use id %q", block.ID)
				}
				seen[block.ID] = true
				uses = append(uses, block)
			case "tool_result":
				if message.Role != "user" || block.ToolUseID == "" || !seen[block.ToolUseID] || answered[block.ToolUseID] {
					return nil, nil, fmt.Errorf("tool_result has no unanswered prior tool_use id %q", block.ToolUseID)
				}
				answered[block.ToolUseID] = true
				results = append(results, block)
			}
		}
	}
	return uses, results, nil
}

func anthropicToolRoundReply(t *testing.T, request llmapi.Request) (llmapi.Reply, error) {
	t.Helper()
	var body struct {
		Model    string                  `json:"model"`
		Messages []anthropicRoundMessage `json:"messages"`
		Tools    []struct {
			Name   string `json:"name"`
			Schema struct {
				Properties map[string]json.RawMessage `json:"properties"`
			} `json:"input_schema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	uses, results, err := anthropicRoundBlocks(body.Messages)
	if err != nil {
		refusal, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "invalid_request_error", "message": err.Error()}})
		return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: string(refusal)}, nil
	}
	var staticID string
	for _, tool := range body.Tools {
		if tool.Name == "read_flow_data" {
			var ids struct {
				Enum []string `json:"enum"`
			}
			if json.Unmarshal(tool.Schema.Properties["static_id"], &ids) == nil && len(ids.Enum) == 1 {
				staticID = ids.Enum[0]
			}
		}
	}
	if staticID == "" {
		return llmapi.Reply{}, fmt.Errorf("Anthropic request lost its admitted read tool")
	}
	args, _ := json.Marshal(map[string]string{"kind": "static_file", "static_id": staticID})
	if request.Number == 1 {
		return llmapi.Reply{ID: "anthropic-read-response", Model: body.Model, InputTokens: 10, OutputTokens: 4, TotalTokens: 14,
			Tools: []llmapi.ToolCall{{ID: "anthropic-round-read", Name: "read_flow_data", Arguments: args}}}, nil
	}
	if request.Number != 2 {
		return llmapi.Reply{}, fmt.Errorf("unexpected Anthropic tool-round request %d", request.Number)
	}
	// Losing both blocks is an assertion failure, not a made-up API refusal.
	if len(uses) != 1 || len(results) != 1 {
		t.Errorf("second Anthropic request lost stored round: tool_use=%d tool_result=%d, want 1/1 with id anthropic-round-read", len(uses), len(results))
	} else {
		if uses[0].ID != "anthropic-round-read" || uses[0].Name != "read_flow_data" || !bytes.Equal(uses[0].Input, args) || results[0].ToolUseID != uses[0].ID {
			t.Error("Anthropic replay changed the stored call id, name, arguments or result linkage")
		}
		var text string
		var value struct {
			StaticID string `json:"static_id"`
			Content  string `json:"content"`
		}
		if json.Unmarshal(results[0].Content, &text) != nil || json.Unmarshal([]byte(text), &value) != nil || value.StaticID != staticID || value.Content != "anthropic customer\n" {
			t.Error("Anthropic tool_result lost its actual admitted read output")
		}
	}
	reply, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": "Swarm heard: Anthropic customer"})
	return llmapi.Reply{ID: "anthropic-emit-response", Model: body.Model, InputTokens: 12, OutputTokens: 4, TotalTokens: 16,
		Tools: []llmapi.ToolCall{{ID: "anthropic-round-emit", Name: "emit_telegram_reply_requested", Arguments: reply}}}, nil
}

func TestAnthropicToolRoundDoubleRefusesOrphanResult(t *testing.T) {
	provider := llmapi.New(t, llmapi.Anthropic, "/v1/messages", "orphan-key", func(request llmapi.Request) (llmapi.Reply, error) {
		return anthropicToolRoundReply(t, request)
	})
	body := strings.NewReader(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"orphan-id","content":"value"}]}]}`)
	request, err := http.NewRequest(http.MethodPost, provider.URL+"/v1/messages", body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("x-api-key", "orphan-key")
	response, err := provider.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var refusal struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&refusal); err != nil || response.StatusCode != http.StatusBadRequest || !strings.Contains(refusal.Error.Message, `no unanswered prior tool_use id "orphan-id"`) {
		t.Fatalf("orphan tool_result HTTP=%d message=%q err=%v", response.StatusCode, refusal.Error.Message, err)
	}
}

const toolRoundReasoning = "Read the admitted customer file before replying."
const toolRoundFinalText = "The admitted customer file was read before replying."

func reasoningToolRoundReply(t *testing.T, request llmapi.Request) (llmapi.Reply, error) {
	t.Helper()
	var body struct {
		Messages []struct {
			Role       string          `json:"role"`
			Content    string          `json:"content"`
			Reasoning  json.RawMessage `json:"reasoning_content"`
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
		for _, message := range body.Messages {
			if len(message.Reasoning) != 0 {
				return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: "completed turn retained reasoning_content"}, nil
			}
		}
		read, err := retainedProviderToolResult(request, "round-call", "read_flow_data")
		if err != nil {
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: err.Error()}, nil
		}
		emit, err := retainedProviderToolResult(request, "round-reply", "emit_telegram_reply_requested")
		if err != nil || read >= emit || body.Messages[emit].Content != toolRoundFinalText {
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: fmt.Sprintf("completed round lost ordered call/results: %v", err)}, nil
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
	var reasoning string
	if err := json.Unmarshal(assistant.Reasoning, &reasoning); err != nil || reasoning != toolRoundReasoning {
		const refusal = `{"error":{"message":"The reasoning_content in the thinking mode must be passed back to the API"}}`
		t.Logf("strict provider returned HTTP 400: %s", refusal)
		return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: refusal}, nil
	}
	var result struct {
		StaticID string `json:"static_id"`
		Content  string `json:"content"`
	}
	if err := json.Unmarshal([]byte(tool.Content), &result); err != nil || result.StaticID != staticID || result.Content != "round customer\n" {
		return llmapi.Reply{}, fmt.Errorf("continuation lost its genuine read result")
	}
	reply, _ := json.Marshal(map[string]string{"chat_id": "2001", "text": "Swarm heard: round customer"})
	return llmapi.Reply{ID: "round-emit", Model: "strict-thinking", Text: toolRoundFinalText, InputTokens: 12, OutputTokens: 4, TotalTokens: 16,
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
			content := body.Messages[i+1].Content
			var read struct {
				Content string `json:"content"`
			}
			valid := content == "ok"
			if name == "read_flow_data" {
				valid = json.Unmarshal([]byte(content), &read) == nil && read.Content == "round customer\n"
			}
			if !valid {
				return 0, fmt.Errorf("stored result %s lost its actual successful outcome", callID)
			}
			return i, nil
		}
	}
	return 0, fmt.Errorf("stored conversation lost prior assistant tool call %s", callID)
}
