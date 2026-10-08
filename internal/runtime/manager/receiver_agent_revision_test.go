package manager

import (
	"encoding/json"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestAgentRevisionPreservesOpaqueNumberKinds(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	parent := runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), managerIdentityTestRunID, managerIdentityTestRunID, managerIdentityTestRunID, "")
	instance, err := runtimeflowidentity.KeyedChild(source, parent, "review", "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := ConstructedFlowMaterialization(source, managerIdentityTestRunID, instance)
	if err != nil {
		t.Fatal(err)
	}
	blueprint := materialization.Agents[0]
	seen := map[string]string{}
	for name, raw := range map[string]string{"empty": `{}`, "integer": `{"number":7}`, "double": `{"number":7.0}`, "nested_integer": `{"items":[{"number":7}]}`, "nested_double": `{"items":[{"number":7.0}]}`} {
		cfg := blueprint.Config
		cfg.Config = json.RawMessage(raw)
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
}
