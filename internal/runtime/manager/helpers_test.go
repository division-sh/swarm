package manager

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWithSystemPrompt_RejectsMalformedJSON(t *testing.T) {
	_, err := WithSystemPrompt(json.RawMessage(`{"model":`), "prompt")
	if err == nil {
		t.Fatal("expected malformed config json to fail")
	}
	if !strings.Contains(err.Error(), "invalid agent config json") {
		t.Fatalf("error = %v", err)
	}
}

func TestWithSystemPrompt_PreservesExistingConfigOnSuccess(t *testing.T) {
	raw, err := WithSystemPrompt(json.RawMessage(`{"model":"cheap","tools":["a"]}`), "prompt")
	if err != nil {
		t.Fatalf("WithSystemPrompt error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal updated config: %v", err)
	}
	if got := strings.TrimSpace(decoded["system_prompt"].(string)); got != "prompt" {
		t.Fatalf("system_prompt = %q, want prompt", got)
	}
	if got := strings.TrimSpace(decoded["model"].(string)); got != "cheap" {
		t.Fatalf("model = %q, want cheap", got)
	}
}
