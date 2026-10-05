package agentpersistence

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestPersistedAgentConfigurationHasNoReceiverCopy(t *testing.T) {
	if _, found := reflect.TypeOf(actors.AgentConfig{}).FieldByName("ReceiverConfig"); found {
		t.Fatal("receiver configuration carrier restored")
	}
	cfg := persistedIntentTestAgent(t)
	cfg.Config = json.RawMessage(`{"opaque":[7,7.0,null]}`)
	row, err := ProjectPersistedAgentConfig(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), row.ConfigJSON...)
	if string(want) != `{"opaque":[7,7.0,null]}` {
		t.Fatalf("agent persistence added a second namespace: %s", want)
	}
	cfg.Config[0] = '['
	a, err := HydratePersistedAgentConfig(row)
	if err != nil {
		t.Fatal(err)
	}
	b, err := HydratePersistedAgentConfig(row)
	if err != nil {
		t.Fatal(err)
	}
	a.Config[0] = '['
	if !bytes.Equal(b.Config, want) || !bytes.Equal(row.ConfigJSON, want) {
		t.Fatal("agent projection/hydration aliases another value")
	}
	row.ConfigJSON[0] = '['
	if !bytes.Equal(b.Config, want) {
		t.Fatal("agent hydration aliases durable wire")
	}
	if _, err := HydratePersistedAgentConfig(row); err == nil {
		t.Fatal("corrupt durable evidence accepted")
	}
}

func TestPersistedAgentConfigRejectsMalformedValues(t *testing.T) {
	base, err := ProjectPersistedAgentConfig(persistedIntentTestAgent(t), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"", `null`, `[]`, `false`, `7`, `"value"`, `{"a":1,"a":2}`, `{"a":{ "x":1,"x":2}}`, `{} {}`, "{\"x\":\"" + string([]byte{0xff}) + "\"}"} {
		cfg := persistedIntentTestAgent(t)
		cfg.Config = json.RawMessage(raw)
		if raw != "" {
			if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil {
				t.Fatalf("malformed authored config accepted: %s", raw)
			}
		}
		row := base
		row.ConfigJSON = []byte(raw)
		if _, err := HydratePersistedAgentConfig(row); err == nil {
			t.Fatalf("malformed persisted config accepted: %s", raw)
		}
	}
}
