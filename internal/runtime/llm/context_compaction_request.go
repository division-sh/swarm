package llm

import (
	"encoding/json"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

const anthropicCompactionBeta = "compact-2026-09-04"

func (r *AnthropicAPIRuntime) buildContextCompactionRequest(prefix *Session, model, method string) ([]byte, error) {
	input := Message{Role: "user", Content: contextCompactionInstruction}
	if method == "anthropic_native" {
		if llmselection.NativeCompactionMethod("anthropic", r.apiURL, model) != method {
			return nil, contextCompactionLimitation("native_route_not_documented")
		}
		input = Message{}
	} else if method != "platform_summary" {
		return nil, contextCompactionLimitation("invalid_compaction_transport")
	}
	request, err := r.buildRequest(prefix, input, model)
	if err != nil {
		return nil, err
	}
	if method == "anthropic_native" {
		request.Compaction = &anthropicCompactionRequest{Type: "summarize"}
	}
	return json.Marshal(request)
}

func (r *OpenAICompatibleRuntime) buildContextCompactionRequest(prefix *Session, model string) ([]byte, error) {
	request, err := r.buildRequest(prefix, Message{Role: "user", Content: contextCompactionInstruction}, model)
	if err != nil {
		return nil, err
	}
	return json.Marshal(request)
}

func (r *OpenAIResponsesRuntime) buildContextCompactionRequest(prefix *Session, model, method string) ([]byte, error) {
	input := Message{Role: "user", Content: contextCompactionInstruction}
	if method == "openai_native" {
		if llmselection.NativeCompactionMethod("openai_responses", r.baseURL+"/responses", model) != method {
			return nil, contextCompactionLimitation("native_route_not_documented")
		}
		input = Message{}
	} else if method != "platform_summary" {
		return nil, contextCompactionLimitation("invalid_compaction_transport")
	}
	request, err := r.buildRequest(prefix, input, model)
	if err != nil {
		return nil, err
	}
	if method == "openai_native" {
		// /responses/compact accepts the window and instructions, not the
		// tools parameter used by /responses. The next ordinary call still
		// carries the unchanged frozen tools through the existing encoder.
		return json.Marshal(struct {
			Model        string `json:"model"`
			Instructions string `json:"instructions,omitempty"`
			Input        []any  `json:"input"`
		}{request.Model, request.Instructions, request.Input})
	}
	return json.Marshal(request)
}

func anthropicRequestNeedsCompactionBeta(payload []byte) bool {
	var request struct {
		Compaction *anthropicCompactionRequest `json:"compaction"`
		Messages   []struct {
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(payload, &request) != nil {
		return false
	}
	if request.Compaction != nil {
		return true
	}
	for _, message := range request.Messages {
		var blocks []struct{ Type string }
		if json.Unmarshal(message.Content, &blocks) == nil {
			for _, block := range blocks {
				if block.Type == "compaction" {
					return true
				}
			}
		}
	}
	return false
}

func contextCompactionNativeResult(method string, raw []byte) (json.RawMessage, error) {
	var response struct {
		StopReason string            `json:"stop_reason"`
		Content    []json.RawMessage `json:"content"`
		Output     []json.RawMessage `json:"output"`
		Error      json.RawMessage   `json:"error"`
	}
	if json.Unmarshal(raw, &response) != nil || len(response.Error) != 0 && string(response.Error) != "null" {
		return nil, contextCompactionLimitation("invalid_native_result")
	}
	if method == "anthropic_native" {
		if response.StopReason != "compaction" || len(response.Content) != 1 {
			return nil, contextCompactionLimitation("native_summary_not_returned")
		}
		var block struct{ Type, Content, Signature string }
		if json.Unmarshal(response.Content[0], &block) != nil || block.Type != "compaction" || strings.TrimSpace(block.Content) == "" || block.Signature == "" {
			return nil, contextCompactionLimitation("invalid_signed_summary")
		}
		return canonicalContextCompactionWindow(response.Content)
	}
	if method == "openai_native" && len(response.Output) != 0 {
		compacted := false
		for _, item := range response.Output {
			var header struct {
				Type             string `json:"type"`
				EncryptedContent string `json:"encrypted_content"`
			}
			// Inspect only the discriminator. Keep every item and unknown member.
			if json.Unmarshal(item, &header) != nil || header.Type == "" {
				return nil, contextCompactionLimitation("invalid_native_window")
			}
			if header.Type == "compaction" && header.EncryptedContent == "" {
				return nil, contextCompactionLimitation("invalid_native_compaction_item")
			}
			compacted = compacted || header.Type == "compaction"
		}
		if compacted {
			return canonicalContextCompactionWindow(response.Output)
		}
	}
	return nil, contextCompactionLimitation("native_summary_not_returned")
}

func canonicalContextCompactionWindow(items []json.RawMessage) (json.RawMessage, error) {
	raw, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}
	return canonicaljson.Canonicalize(raw)
}
