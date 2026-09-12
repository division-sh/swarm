package forkrecipient

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSelectedEvidenceCodecPreservesCompleteRunlessEvidence(t *testing.T) {
	variants := evidenceVariants(t)
	variants["node_connect"] = connectEvidence(t, nodeInput(t), "plan", "pin")
	for name, original := range variants {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte("run_id")) || bytes.Contains(raw, []byte("agent_identity")) {
				t.Fatalf("runless codec contains live identity: %s", raw)
			}
			var decoded Evidence
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, decoded) {
				t.Fatalf("codec lost evidence:\n%#v\n%#v", original, decoded)
			}
			before, err := original.Key()
			if err != nil {
				t.Fatal(err)
			}
			after, err := decoded.Key()
			if err != nil || before != after {
				t.Fatalf("codec changed semantic key: %v", err)
			}
			firstHash, _ := original.Fingerprint()
			secondHash, _ := decoded.Fingerprint()
			if firstHash != secondHash {
				t.Fatal("codec changed semantic fingerprint")
			}
			if decoded.HandlerNode() != original.HandlerNode() || decoded.HandlerEvent() != original.HandlerEvent() {
				t.Fatal("handler accessors lost selected evidence")
			}
			plan, pin, connected := decoded.Connect()
			if connected != (decoded.authority == authorityConnect) || (connected && (plan.Empty() || pin.Empty())) {
				t.Fatal("connect association lost")
			}
		})
	}
}

func TestSelectedEvidenceCodecRejectsUnknownAndInvalidFieldsAtomically(t *testing.T) {
	agent := connectEvidence(t, agentInput(t), "plan-a", "pin-a")
	node := localEvidence(t, nodeInput(t))
	agentRaw, err := json.Marshal(agent)
	if err != nil {
		t.Fatal(err)
	}
	nodeRaw, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"null": []byte("null"), "empty": []byte("{}"), "array": []byte("[]"),
		"trailing_value": append(bytes.Clone(agentRaw), []byte(" {}")...),
		"trailing_junk":  append(bytes.Clone(agentRaw), []byte(" junk")...),
	}
	agentMutations := map[string]func(map[string]any){
		"unknown_top":       func(m map[string]any) { m["extra"] = true },
		"live_identity":     func(m map[string]any) { m["agent_identity"] = map[string]any{"run_id": "source"} },
		"unknown_authority": func(m map[string]any) { m["authority"].(map[string]any)["extra"] = true },
		"unknown_plan":      func(m map[string]any) { m["agent_plan"].(map[string]any)["run_id"] = "source" },
		"unknown_name": func(m map[string]any) {
			m["agent_plan"].(map[string]any)["name"].(map[string]any)["extra"] = true
		},
		"unknown_route": func(m map[string]any) {
			m["agent_plan"].(map[string]any)["route"].(map[string]any)["extra"] = true
		},
		"missing_plan":       func(m map[string]any) { delete(m, "agent_plan") },
		"null_plan":          func(m map[string]any) { m["agent_plan"] = nil },
		"incomplete_plan":    func(m map[string]any) { m["agent_plan"] = map[string]any{} },
		"unknown_kind":       func(m map[string]any) { m["subscriber_type"] = "observer" },
		"unknown_variant":    func(m map[string]any) { m["authority"].(map[string]any)["kind"] = "historical" },
		"missing_variant":    func(m map[string]any) { delete(m, "authority") },
		"null_variant":       func(m map[string]any) { m["authority"] = nil },
		"local_with_connect": func(m map[string]any) { m["authority"].(map[string]any)["kind"] = "local" },
		"missing_event":      func(m map[string]any) { delete(m, "handler_event") },
		"wildcard_event":     func(m map[string]any) { m["handler_event"] = "*" },
		"wrong_plan_agent":   func(m map[string]any) { m["subscriber_id"] = "other" },
	}
	for _, field := range []string{"plan_id", "receiver_pin"} {
		for _, value := range []string{"", "abc", strings.Repeat("0", 64), strings.Repeat("A", 64), strings.Repeat("z", 64)} {
			name := field + "_" + value
			agentMutations[name] = func(m map[string]any) { m["authority"].(map[string]any)[field] = value }
		}
	}
	for name, mutate := range agentMutations {
		var m map[string]any
		if err := json.Unmarshal(agentRaw, &m); err != nil {
			t.Fatal(err)
		}
		mutate(m)
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		tests[name] = raw
	}
	for name, mutate := range map[string]func(map[string]any){
		"node_missing_handler":       func(m map[string]any) { delete(m, "handler_node") },
		"node_null_handler":          func(m map[string]any) { m["handler_node"] = nil },
		"node_unknown_handler_field": func(m map[string]any) { m["handler_node"].(map[string]any)["extra"] = true },
		"node_with_agent_plan":       func(m map[string]any) { m["agent_plan"] = map[string]any{} },
	} {
		var m map[string]any
		if err := json.Unmarshal(nodeRaw, &m); err != nil {
			t.Fatal(err)
		}
		mutate(m)
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		tests[name] = raw
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			decoded := agent
			if err := decoded.UnmarshalJSON(raw); err == nil {
				t.Fatalf("accepted invalid wire: %s", raw)
			}
			if !reflect.DeepEqual(decoded, agent) {
				t.Fatal("failed decode changed its destination")
			}
		})
	}
	var nilDestination *Evidence
	if err := nilDestination.UnmarshalJSON(agentRaw); err == nil {
		t.Fatal("nil destination accepted")
	}
}
