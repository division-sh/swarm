package failures

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestAgentToolFailurePresentationKeepsCanonicalEvidence(t *testing.T) {
	for _, class := range Classes() {
		t.Run(string(class), func(t *testing.T) {
			cause := errors.New("original caller cause")
			failure := Wrap(class, "presentation_control", "tool-executor", "execute", map[string]any{"attempt": "original"}, cause)
			envelope := Normalize(failure, "test", "control")
			original, err := MarshalEnvelope(envelope)
			if err != nil {
				t.Fatal(err)
			}
			value, err := AgentToolFailureValue(envelope)
			if err != nil {
				t.Fatal(err)
			}
			text, err := AgentToolFailureText(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if class == ClassOutcomeUncertain {
				wire, err := json.Marshal(value)
				if err != nil || text != agentToolOutcomeUnknown || value["message"] != text || strings.Contains(strings.ToLower(string(wire)), "reconcile") || value["presentation"] != "agent_tool_failure" {
					t.Fatalf("wrong agent-only projection: text=%q value=%s err=%v", text, wire, err)
				}
				if _, err := UnmarshalEnvelope(wire); err == nil {
					t.Fatal("agent presentation masquerades as a canonical machine envelope")
				}
				canonical, err := EnvelopeValue(envelope)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{"class", "detail", "retryable", "deterministic", "component", "operation"} {
					if !reflect.DeepEqual(value[key], canonical[key]) {
						t.Fatalf("projection changed %s: %+v vs %+v", key, value[key], canonical[key])
					}
				}
			} else {
				canonical, err := EnvelopeValue(envelope)
				if err != nil || !reflect.DeepEqual(value, canonical) || text != envelope.Message {
					t.Fatalf("non-uncertain control changed: %+v/%q: %v", value, text, err)
				}
			}
			value["detail"].(map[string]any)["attributes"].(map[string]any)["attempt"] = "changed view"
			after, err := MarshalEnvelope(envelope)
			if err != nil || string(original) != string(after) || !errors.Is(failure, cause) {
				t.Fatalf("presentation mutated canonical evidence or cause: %s: %v", after, err)
			}
		})
	}
}

func TestAgentToolFailurePresentationRefusesInvalidEnvelope(t *testing.T) {
	envelope := Normalize(New(ClassOutcomeUncertain, "presentation_control", "test", "control", nil), "test", "control")
	envelope.Remediation = "not canonical"
	if _, err := AgentToolFailureValue(envelope); err == nil {
		t.Fatal("invalid canonical envelope projected")
	}
	if _, err := AgentToolFailureText(envelope); err == nil {
		t.Fatal("invalid canonical envelope rendered")
	}
}
