// Package llmapi supplies the HTTP provider boundary used by served LLM tests.
package llmapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type Protocol string

const (
	Anthropic        Protocol = "anthropic"
	OpenAICompatible Protocol = "openai_compatible"
	OpenAIResponses  Protocol = "openai_responses"
)

type Request struct {
	Number int
	Header http.Header
	Body   json.RawMessage
}

type ToolCall struct {
	ID        string
	OutputID  string
	Name      string
	Arguments json.RawMessage
}

type Reply struct {
	ID           string
	Model        string
	Text         string
	TextID       string
	Tools        []ToolCall
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}

type Server struct {
	*httptest.Server
	mu       sync.Mutex
	requests []Request
}

// New preserves the exact configured path; it does not alias /v1 endpoints.
func New(t testing.TB, protocol Protocol, path, key string, respond func(Request) (Reply, error)) *Server {
	t.Helper()
	if path == "" || key == "" || respond == nil {
		t.Fatal("LLM double requires an exact path, credential and responder")
	}
	if protocol != Anthropic && protocol != OpenAICompatible && protocol != OpenAIResponses {
		t.Fatalf("unsupported LLM double protocol %q", protocol)
	}
	s := &Server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != path {
			t.Errorf("unexpected LLM request: %s %s, want POST %s", r.Method, r.URL.Path, path)
			http.Error(w, "unexpected LLM endpoint", http.StatusNotFound)
			return
		}
		header, want := "Authorization", "Bearer "+key
		if protocol == Anthropic {
			header, want = "x-api-key", key
		}
		if r.Header.Get(header) != want {
			t.Errorf("LLM request has unexpected %s credential", header)
			http.Error(w, "fixture credential required", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !json.Valid(body) {
			t.Errorf("invalid LLM request JSON: %v", err)
			http.Error(w, "invalid LLM request", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		request := Request{Number: len(s.requests) + 1, Header: r.Header.Clone(), Body: append(json.RawMessage(nil), body...)}
		s.requests = append(s.requests, cloneRequest(request))
		s.mu.Unlock()
		reply, err := respond(request)
		if err == nil {
			err = WriteReply(w, protocol, reply)
		}
		if err != nil {
			t.Errorf("LLM responder request %d: %v", request.Number, err)
			http.Error(w, "invalid LLM fixture response", http.StatusInternalServerError)
			return
		}
	}))
	t.Cleanup(s.Close)
	return s
}

// WriteReply also serves callers whose request/history handler is test-owned.
func WriteReply(w http.ResponseWriter, protocol Protocol, reply Reply) error {
	if err := validateReply(protocol, reply); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(encodeReply(protocol, reply))
}

func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	requests := make([]Request, len(s.requests))
	for i, request := range s.requests {
		requests[i] = cloneRequest(request)
	}
	return requests
}

func cloneRequest(request Request) Request {
	return Request{Number: request.Number, Header: request.Header.Clone(), Body: append(json.RawMessage(nil), request.Body...)}
}

func validateReply(protocol Protocol, reply Reply) error {
	if reply.ID == "" || (reply.Text == "" && len(reply.Tools) == 0) {
		return fmt.Errorf("reply requires response ID and text or tool calls")
	}
	for _, tool := range reply.Tools {
		if tool.ID == "" || tool.Name == "" || !json.Valid(tool.Arguments) {
			return fmt.Errorf("reply requires exact tool identity and JSON arguments")
		}
		if protocol == OpenAIResponses && tool.OutputID == "" {
			return fmt.Errorf("Responses tool call requires its output item ID")
		}
	}
	if protocol == OpenAIResponses && reply.Text != "" && reply.TextID == "" {
		return fmt.Errorf("Responses text requires its output item ID")
	}
	return nil
}

func encodeReply(protocol Protocol, reply Reply) map[string]any {
	response := map[string]any{"id": reply.ID}
	if reply.Model != "" {
		response["model"] = reply.Model
	}
	switch protocol {
	case Anthropic:
		content := []any{}
		if reply.Text != "" {
			content = append(content, map[string]any{"type": "text", "text": reply.Text})
		}
		for _, tool := range reply.Tools {
			content = append(content, map[string]any{"type": "tool_use", "id": tool.ID, "name": tool.Name, "input": tool.Arguments})
		}
		response["content"] = content
		response["usage"] = map[string]int{"input_tokens": reply.InputTokens, "output_tokens": reply.OutputTokens}
	case OpenAICompatible:
		message := map[string]any{"role": "assistant", "content": reply.Text}
		reason := "stop"
		if len(reply.Tools) != 0 {
			calls := []any{}
			for _, tool := range reply.Tools {
				calls = append(calls, map[string]any{"id": tool.ID, "type": "function",
					"function": map[string]any{"name": tool.Name, "arguments": string(tool.Arguments)}})
			}
			message["tool_calls"] = calls
			reason = "tool_calls"
		}
		response["object"] = "chat.completion"
		response["choices"] = []any{map[string]any{"index": 0, "message": message, "finish_reason": reason}}
		response["usage"] = map[string]int{"prompt_tokens": reply.InputTokens, "completion_tokens": reply.OutputTokens, "total_tokens": reply.TotalTokens}
	case OpenAIResponses:
		output := []any{}
		for _, tool := range reply.Tools {
			output = append(output, map[string]any{"id": tool.OutputID, "type": "function_call", "call_id": tool.ID,
				"name": tool.Name, "arguments": string(tool.Arguments)})
		}
		if reply.Text != "" {
			output = append(output, map[string]any{"id": reply.TextID, "type": "message", "role": "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": reply.Text}}})
		}
		response["output"] = output
		response["usage"] = map[string]int{"input_tokens": reply.InputTokens, "output_tokens": reply.OutputTokens, "total_tokens": reply.TotalTokens}
	default:
		panic("unsupported LLM fixture protocol")
	}
	return response
}
