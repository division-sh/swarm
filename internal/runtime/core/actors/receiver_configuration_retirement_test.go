package actors

import (
	"encoding/json"
	"testing"
)

func TestOpaqueAgentConfigNormalizationIsIsolated(t *testing.T) {
	base := AgentConfig{Config: json.RawMessage(`{"opaque":[7,7.0,null]}`)}
	copy := base
	copy.NormalizeRuntimeDescriptor()
	copy.Config[0] = '['
	if string(base.Config) != `{"opaque":[7,7.0,null]}` {
		t.Fatal("normalization aliases source bytes")
	}
}
