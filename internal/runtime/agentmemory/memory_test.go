package agentmemory

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestPlanContainsOnlyEnablement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		plan := Plan{Enabled: enabled}
		raw, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"enabled":false}`
		if enabled {
			want = `{"enabled":true}`
		}
		if string(raw) != want {
			t.Fatalf("plan = %s, want %s", raw, want)
		}
		var restored Plan
		if err := json.Unmarshal(raw, &restored); err != nil || restored != plan {
			t.Fatalf("restored = %#v, err = %v", restored, err)
		}
	}
}

func TestIdentityRequiresExactRunAgentAndFlowInstance(t *testing.T) {
	valid := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")
	if err := ValidateIdentity(valid, true); err != nil {
		t.Fatalf("valid identity: %v", err)
	}
	missingRun := valid
	missingRun.RunID = ""
	root := agentidentitytest.RootRuntimeForRun(t, "run-a", "agent-a", "test-fixture")

	for _, tc := range []struct {
		name     string
		identity Identity
		want     string
	}{
		{name: "run", identity: missingRun, want: "run_id"},
		{name: "agent", identity: Identity{RunID: "run-a"}, want: "agent_id"},
		{name: "flow instance", identity: root, want: "flow_instance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateIdentity(tc.identity, true); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestIdentityValueIsolatesRunsAndConcreteAgents(t *testing.T) {
	base := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")
	otherRun := base
	otherRun.RunID = "run-b"
	otherFlow := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-b", "support/chat-b")
	if base == otherRun {
		t.Fatal("different runs produced the same memory identity key")
	}
	if base == otherFlow {
		t.Fatal("different flow instances produced the same memory identity key")
	}
}
