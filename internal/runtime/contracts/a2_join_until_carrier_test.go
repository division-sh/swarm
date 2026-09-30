package contracts

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestA2JoinUntilCarrierCannotBeAuthoredOrSerialized(t *testing.T) {
	for _, name := range []string{"join_until_plans", "JoinUntilPlans"} {
		var handler SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte(name+": []\n"), &handler); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("compiler carrier authored via %s: %v", name, err)
		}
	}
	handler := SystemNodeEventHandler{JoinUntilPlans: []WorkflowJoinPlan{{UntilEvent: "compiler.secret"}}}
	raw, err := json.Marshal(handler)
	if err != nil || strings.Contains(string(raw), "compiler.secret") || strings.Contains(string(raw), "JoinUntilPlans") {
		t.Fatalf("compiler-only carrier serialized: %s %v", raw, err)
	}
}

func TestA2JoinMemberSourceRejectsRetiredEntityAlias(t *testing.T) {
	bundle, handler := a2JoinCompilerFixture(t, "[text]")
	handler.Join.Members.From = "entity.members"
	if _, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler); err == nil || !strings.Contains(err.Error(), "state.<field>") {
		t.Fatalf("legacy entity membership alias admitted: %v", err)
	}
}
