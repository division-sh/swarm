package agentpersistence

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPersistedAgentReceiverNamespaceIsInert(t *testing.T) {
	cases := map[string]string{
		"flow_path":       `{"flow_path":"business-path"}`,
		"model":           `{"model":"business-model"}`,
		"memory":          `{"constraints":{"memory":"business-memory"}}`,
		"mode":            `{"mode":"business-mode"}`,
		"nested_prompt":   `{"nested":{"archived_record":{"system_prompt":"ordinary archived business data"}}}`,
		"list_prompt":     `{"nested":[{"system_prompt":"business"},[{"system_prompt":null}],7,7.0,null]}`,
		"authority_names": `{"intent":"business","tools":["business"],"permissions":false,"memory":{"source":"business"},"config":{"model":7.0},"receiver_config":{"mode":null}}`,
	}
	for key := range runtimeConfigKeys {
		raw, err := json.Marshal(map[string]any{key: "business"})
		if err != nil {
			t.Fatal(err)
		}
		cases["runtime_name_"+key] = string(raw)
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := persistedIntentTestAgent(t)
			cfg.ReceiverConfig = json.RawMessage(raw)
			row, err := ProjectPersistedAgentConfig(cfg, "")
			if err != nil {
				t.Fatal(err)
			}
			withoutReceiver := cfg
			withoutReceiver.ReceiverConfig = nil
			baseline, err := ProjectPersistedAgentConfig(withoutReceiver, "")
			if err != nil {
				t.Fatal(err)
			}
			controlPlane := row
			controlPlane.ConfigJSON = baseline.ConfigJSON
			if !reflect.DeepEqual(controlPlane, baseline) {
				t.Fatal("receiver business data changed persisted control-plane fields")
			}
			got, err := HydratePersistedAgentConfig(row)
			if err != nil {
				t.Fatal(err)
			}
			if string(got.ReceiverConfig) != raw || string(got.Config) != `{}` {
				t.Fatalf("namespace or numeric drift: receiver=%s config=%s want=%s", got.ReceiverConfig, got.Config, raw)
			}
			if got.Model != cfg.Model || got.Memory != cfg.Memory || got.FlowPath != cfg.FlowPath ||
				!reflect.DeepEqual(got.Intent, cfg.Intent) || len(got.Tools) != 0 ||
				len(got.Permissions) != 0 || !got.Prompt.Empty() {
				t.Fatalf("receiver business data changed runtime authority: %+v", got)
			}
		})
	}
}

func TestPersistedAgentConfigEnvelopeRejectsHostileShapes(t *testing.T) {
	base, err := ProjectPersistedAgentConfig(persistedIntentTestAgent(t), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, raw, want string }{
		{"empty", ``, "invalid agent config envelope"},
		{"flattened", `{"business":1}`, "requires exactly config and receiver_config"},
		{"missing_config", `{"receiver_config":{}}`, "requires exactly config and receiver_config"},
		{"missing_receiver", `{"config":{}}`, "requires exactly config and receiver_config"},
		{"unknown", `{"config":{},"receiver_config":null,"extra":1}`, "requires exactly config and receiver_config"},
		{"case_alias", `{"Config":{},"receiver_config":null}`, "requires exactly config and receiver_config"},
		{"null", `null`, "requires exactly config and receiver_config"},
		{"array", `[]`, "must be a JSON object"},
		{"null_config", `{"config":null,"receiver_config":null}`, "config must be a json object"},
		{"array_config", `{"config":[],"receiver_config":null}`, "decode config"},
		{"array_receiver", `{"config":{},"receiver_config":[]}`, "receiver_config must be a JSON object or null"},
		{"string_receiver", `{"config":{},"receiver_config":"{}"}`, "receiver_config must be a JSON object or null"},
		{"bool_receiver", `{"config":{},"receiver_config":false}`, "receiver_config must be a JSON object or null"},
		{"number_receiver", `{"config":{},"receiver_config":7}`, "receiver_config must be a JSON object or null"},
		{"duplicate", `{"config":{},"config":{},"receiver_config":null}`, "duplicate JSON object key"},
		{"escaped_duplicate", `{"config":{},"receiver_config":{},"receiver_\u0063onfig":{}}`, "duplicate JSON object key"},
		{"nested_duplicate", `{"config":{},"receiver_config":{"nested":[{"a":1,"a":2}]}}`, "duplicate JSON object key"},
		{"opaque_duplicate", `{"config":{"a":1,"a":2},"receiver_config":null}`, "duplicate JSON object key"},
		{"trailing", `{"config":{},"receiver_config":null} {}`, "trailing JSON content"},
		{"invalid_utf8", "{\"config\":{},\"receiver_config\":{\"data\":\"" + string([]byte{0xff}) + "\"}}", "not valid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			row.ConfigJSON = []byte(tc.raw)
			if _, err := HydratePersistedAgentConfig(row); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("hydrate error=%v want %q", err, tc.want)
			}
		})
	}
}

func TestPersistedAgentOpaqueAuthorityRejectsInsteadOfStripping(t *testing.T) {
	cases := map[string]string{
		"system_prompt": `{"system_prompt":"override"}`,
		"nested_prompt": `{"nested":{"system_prompt":"override"}}`,
		"list_prompt":   `{"nested":[{"system_prompt":"override"}]}`,
	}
	for key := range runtimeConfigKeys {
		raw, err := json.Marshal(map[string]any{key: nil})
		if err != nil {
			t.Fatal(err)
		}
		cases[key] = string(raw)
	}
	for _, key := range []string{"mode", "conversation_mode", "session_scope", "session_scope_authority", "memory", "max_turns_per_task"} {
		raw, err := json.Marshal(map[string]any{"constraints": map[string]any{key: nil}})
		if err != nil {
			t.Fatal(err)
		}
		cases["constraints."+key] = string(raw)
	}
	base, err := ProjectPersistedAgentConfig(persistedIntentTestAgent(t), "")
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			want := "config contains runtime-owned keys: " + name
			if strings.Contains(name, "prompt") {
				want = "RETIRED: authored config"
			}
			cfg := persistedIntentTestAgent(t)
			cfg.Config = json.RawMessage(raw)
			if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("projection error=%v want %q", err, want)
			}
			row := base
			row.ConfigJSON = []byte(`{"config":` + raw + `,"receiver_config":{}}`)
			if _, err := HydratePersistedAgentConfig(row); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("hydration error=%v want %q", err, want)
			}
		})
	}
}

func TestPersistedAgentReceiverAdmissionAndOwnedBytes(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `false`, `7`, `"value"`, `{"a":1,"a":2}`} {
		cfg := persistedIntentTestAgent(t)
		cfg.ReceiverConfig = json.RawMessage(raw)
		if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil || !strings.Contains(err.Error(), "receiver configuration") {
			t.Fatalf("receiver %s projection error=%v", raw, err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"a":1,"a":2}`} {
		cfg := persistedIntentTestAgent(t)
		cfg.Config = json.RawMessage(raw)
		if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil {
			t.Fatalf("invalid opaque config %s accepted", raw)
		}
	}
	for _, receiver := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`{"nested":[7,7.0,7e0,null,{"system_prompt":"business"}]}`)} {
		cfg := persistedIntentTestAgent(t)
		cfg.Config = json.RawMessage(`{"opaque":[7,7.0,null]}`)
		cfg.ReceiverConfig = append(json.RawMessage(nil), receiver...)
		row, err := ProjectPersistedAgentConfig(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		saved := append([]byte(nil), row.ConfigJSON...)
		cfg.Config[0] = '['
		if len(cfg.ReceiverConfig) > 0 {
			cfg.ReceiverConfig[0] = '['
		}
		if !bytes.Equal(saved, row.ConfigJSON) {
			t.Fatal("projection aliases input")
		}
		first, err := HydratePersistedAgentConfig(row)
		if err != nil {
			t.Fatal(err)
		}
		second, err := HydratePersistedAgentConfig(row)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(second.ReceiverConfig, receiver) || string(second.Config) != `{"opaque":[7,7.0,null]}` {
			t.Fatalf("read changed namespaces: config=%s receiver=%s", second.Config, second.ReceiverConfig)
		}
		first.Config[0] = '['
		if len(first.ReceiverConfig) > 0 {
			first.ReceiverConfig[0] = '['
		}
		if !bytes.Equal(saved, row.ConfigJSON) || !bytes.Equal(second.ReceiverConfig, receiver) || second.Config[0] != '{' {
			t.Fatal("hydration aliases row or another read")
		}
		row.ConfigJSON[0] = '['
		if second.Config[0] != '{' || !bytes.Equal(second.ReceiverConfig, receiver) {
			t.Fatal("hydration aliases mutated row")
		}
		if _, err := HydratePersistedAgentConfig(row); err == nil {
			t.Fatal("second read rescued changed corrupt row")
		}
	}
}
