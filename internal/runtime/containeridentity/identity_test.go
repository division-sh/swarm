package containeridentity

import (
	"testing"

	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestIdentityLabelsRoundTripResetEligibleAgent(t *testing.T) {
	identity := Identity{
		Owner:          OwnerRuntime,
		Kind:           KindAgent,
		ResetEligible:  true,
		CreationSource: "workspace.ResolveWorkspace",
		ContainerName:  "swarm-agent-agent-a",
		WorkspaceScope: "per-agent",
		RunID:          "11111111-1111-1111-1111-111111111111",
		AgentIdentity: runtimeagentidentitytest.Declared(
			t,
			"agent-a",
			"test/agents.yaml",
			"flow",
			"instance-a",
			"flow/instance-a",
		),
	}
	labels := identity.Labels()
	got, ok, err := FromLabels(labels)
	if err != nil {
		t.Fatalf("FromLabels: %v", err)
	}
	if !ok {
		t.Fatal("FromLabels ok = false, want true")
	}
	if !got.ResetEligibleManaged() || got.AgentIdentity.AgentID() != "agent-a" || got.RunID == "" {
		t.Fatalf("identity = %#v, want reset-eligible agent with run lineage", got)
	}
}

func TestIdentityRejectsPartialAgentLabels(t *testing.T) {
	_, _, err := FromLabels(map[string]string{
		LabelOwner:         OwnerRuntime,
		LabelKind:          KindAgent,
		LabelResetEligible: "true",
		LabelContainerName: "swarm-agent-agent-a",
		LabelAgentID:       "agent-a",
	})
	if err == nil {
		t.Fatal("FromLabels error = nil, want partial concrete identity rejection")
	}
}

func TestIdentityRejectsResetEligibleSystemContainer(t *testing.T) {
	_, _, err := FromLabels(map[string]string{
		LabelOwner:         OwnerRuntime,
		LabelKind:          KindSystem,
		LabelResetEligible: "true",
		LabelContainerName: "swarm-system",
	})
	if err == nil {
		t.Fatal("FromLabels error = nil, want reset-eligible system rejection")
	}
}

func TestIdentityReportsAbsentForUnlabeledContainer(t *testing.T) {
	if _, ok, err := FromLabels(nil); ok || err != nil {
		t.Fatalf("FromLabels(nil) = ok:%v err:%v, want absent nil", ok, err)
	}
}
