package actors

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestAgentHasNoReceiverConfigurationCarrier(t *testing.T) {
	if _, found := reflect.TypeOf(AgentConfig{}).FieldByName("ReceiverConfig"); found {
		t.Fatal("per-instance business snapshot restored on agent configuration")
	}
	if _, found := reflect.TypeOf(AgentConfig{}).MethodByName("ValidateReceiverConfig"); found {
		t.Fatal("parallel receiver admission restored")
	}
}

func TestOpaqueAgentConfigNormalizationIsIsolated(t *testing.T) {
	base := AgentConfig{Config: json.RawMessage(`{"opaque":[7,7.0,null]}`)}
	copy := base
	copy.NormalizeRuntimeDescriptor()
	copy.Config[0] = '['
	if string(base.Config) != `{"opaque":[7,7.0,null]}` {
		t.Fatal("normalization aliases source bytes")
	}
}
