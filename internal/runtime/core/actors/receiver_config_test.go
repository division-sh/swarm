package actors

import (
	"encoding/json"
	"testing"
)

func TestReceiverConfigIsInertAndSeparateFromAgentAuthority(t *testing.T) {
	for _, raw := range []string{
		`{"flow_path":"business","model":"business","mode":"business","constraints":{"memory":"business"}}`,
		`{"nested":{"archived_record":{"system_prompt":"business"}},"list":[{"system_prompt":"business"}]}`,
		`{"config":{"tools":["business"],"permissions":["business"]},"integer":7,"double":7.0}`,
	} {
		cfg := AgentConfig{Model: "runtime-model", FlowPath: "runtime/path", Config: json.RawMessage(`{}`), ReceiverConfig: json.RawMessage(raw)}
		if err := cfg.ValidateReceiverConfig(); err != nil {
			t.Fatal(err)
		}
		if err := ValidateNoAuthoredSystemPrompt(cfg.Config); err != nil {
			t.Fatal(err)
		}
		cfg.NormalizeRuntimeDescriptor()
		if string(cfg.ReceiverConfig) != raw || cfg.Model != "runtime-model" || cfg.FlowPath != "runtime/path" || len(cfg.Tools) != 0 || len(cfg.Permissions) != 0 {
			t.Fatalf("business data changed or gained authority: %#v", cfg)
		}
	}
}

func TestReceiverConfigCarrierFailsClosed(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `7`, `"value"`, `{"a":1,"a":2}`, `{} {}`, string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'})} {
		if err := (AgentConfig{ReceiverConfig: json.RawMessage(raw)}).ValidateReceiverConfig(); err == nil {
			t.Fatalf("accepted malformed receiver carrier %q", raw)
		}
	}
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`{}`)} {
		if err := (AgentConfig{ReceiverConfig: raw}).ValidateReceiverConfig(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiverConfigMergeAndNormalizationDoNotAlias(t *testing.T) {
	base := AgentConfig{Config: json.RawMessage(`{"opaque":1}`), ReceiverConfig: json.RawMessage(`{"value":7.0}`)}
	copy := MergeAgentConfig(base, AgentConfig{})
	copy.Config[0], copy.ReceiverConfig[0] = '[', '['
	if string(base.Config) != `{"opaque":1}` || string(base.ReceiverConfig) != `{"value":7.0}` {
		t.Fatal("normalization aliases source bytes")
	}
	patch := AgentConfig{ReceiverConfig: json.RawMessage(`{"value":8.0}`)}
	copy = MergeAgentConfig(base, patch)
	patch.ReceiverConfig[0] = '['
	if string(copy.ReceiverConfig) != `{"value":8.0}` || string(copy.Config) != `{"opaque":1}` {
		t.Fatal("receiver replacement aliases patch or replaces agent namespace")
	}
}
