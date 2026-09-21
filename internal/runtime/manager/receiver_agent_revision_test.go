package manager

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestReceiverAgentRevisionPreservesNamespaceAndNumberKinds(t *testing.T) {
	blueprints, err := TemplateFlowAgentMaterializationBlueprints(semanticview.Wrap(testFlowBundle(t, "")), "review", "review/inst-1", "ent-1", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	blueprint := blueprints[0]
	seen := map[string]string{}
	for name, raw := range map[string]string{
		"empty":              `{}`,
		"integer":            `{"number":7}`,
		"double":             `{"number":7.0}`,
		"nested_integer":     `{"items":[{"number":7}]}`,
		"nested_double":      `{"items":[{"number":7.0}]}`,
		"prompt_named_data":  `{"system_prompt":"inert"}`,
		"control_named_data": `{"model":"inert","flow_path":"inert","mode":"inert"}`,
	} {
		cfg := blueprint.Config
		cfg.ReceiverConfig = json.RawMessage(raw)
		revision, err := AgentConfigPlanRevision(cfg, blueprint.Identity)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if prior, ok := seen[revision]; ok {
			t.Fatalf("%s aliases %s", name, prior)
		}
		seen[revision] = name
		wire, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var recovered = cfg
		if err := json.Unmarshal(wire, &recovered); err != nil {
			t.Fatal(err)
		}
		recoveredRevision, err := AgentConfigPlanRevision(recovered, blueprint.Identity)
		if err != nil || recoveredRevision != revision {
			t.Fatalf("%s roundtrip changed revision: %s %s %v", name, revision, recoveredRevision, err)
		}
	}
	left, right := blueprint.Config, blueprint.Config
	left.Config, left.ReceiverConfig = json.RawMessage(`{"number":7}`), json.RawMessage(`{}`)
	right.Config, right.ReceiverConfig = json.RawMessage(`{}`), json.RawMessage(`{"number":7}`)
	leftRevision, err := AgentConfigPlanRevision(left, blueprint.Identity)
	if err != nil {
		t.Fatal(err)
	}
	rightRevision, err := AgentConfigPlanRevision(right, blueprint.Identity)
	if err != nil || leftRevision == rightRevision {
		t.Fatalf("agent and receiver namespaces alias: %s %s %v", leftRevision, rightRevision, err)
	}
}
