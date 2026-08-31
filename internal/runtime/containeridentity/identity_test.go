package containeridentity

import (
	"strings"
	"testing"

	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
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

func TestIdentityEqualityUsesNormalizedCompleteIdentity(t *testing.T) {
	agent := runtimeagentidentitytest.Declared(t, "agent-a", "test/agents.yaml", "child", "one", "child/one")
	left := Identity{Owner: OwnerRuntime, Kind: KindFlow, ResetEligible: true, ContainerName: " flow ", RunID: " run-a ", FlowInstance: "/child/one/", AgentIdentity: agent}
	right := Identity{Owner: OwnerRuntime, Kind: KindFlow, ResetEligible: true, ContainerName: "flow", RunID: "run-a", FlowInstance: "child/one", AgentIdentity: agent}
	if !left.Equal(right) {
		t.Fatal("normalized equal identities compare unequal")
	}
	right.RunID = "run-b"
	if left.Equal(right) {
		t.Fatal("different run identities compare equal")
	}
}

func TestIdentityLabelsRoundTripFlowActorAndDataProjection(t *testing.T) {
	identity := Identity{
		Owner:          OwnerRuntime,
		Kind:           KindFlow,
		ResetEligible:  true,
		CreationSource: "workspace.ResolveWorkspace",
		ContainerName:  "swarm-flow-child-one-agent-a",
		WorkspaceScope: "per-flow-instance",
		RunID:          "11111111-1111-1111-1111-111111111111",
		AgentIdentity:  runtimeagentidentitytest.Declared(t, "agent-a", "test/agents.yaml", "child", "one", "child/one"),
		FlowInstance:   "child/one",
		DataProjection: runtimedataaccess.ProjectionID("data-projection-v1:sha256:" + strings.Repeat("a", 64)),
	}
	got, ok, err := FromLabels(identity.Labels())
	if err != nil {
		t.Fatalf("FromLabels: %v", err)
	}
	if !ok || !got.Equal(identity) {
		t.Fatalf("flow identity round trip = ok:%v identity:%#v, want %#v", ok, got, identity)
	}
}

func TestIdentityRejectsFlowWithoutActorOrMalformedProjection(t *testing.T) {
	base := Identity{
		Owner: OwnerRuntime, Kind: KindFlow, ResetEligible: true, ContainerName: "swarm-flow-child-one",
		WorkspaceScope: "per-flow-instance", RunID: "run-a", FlowInstance: "child/one",
	}
	if err := base.Validate(); err == nil {
		t.Fatal("flow identity without actor validated")
	}
	base.AgentIdentity = runtimeagentidentitytest.Declared(t, "agent-a", "test/agents.yaml", "child", "one", "child/one")
	base.DataProjection = runtimedataaccess.ProjectionID("data-projection-v1:sha256:deadbeef")
	if err := base.Validate(); err == nil {
		t.Fatal("flow identity with malformed data projection validated")
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

func TestIdentityRejectsMalformedSourceProjectionIdentity(t *testing.T) {
	base := Identity{
		Owner:            OwnerRuntime,
		Kind:             KindSystem,
		ContainerName:    "swarm-system",
		BundleHash:       "bundle-v2:sha256:" + strings.Repeat("a", 64),
		SourceProjection: "runtime-projection-v1:" + strings.Repeat("b", 32),
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid projection-bearing identity rejected: %v", err)
	}
	for _, invalid := range []string{
		"runtime-projection-v1:deadbeef",
		"runtime-projection-v1:" + strings.Repeat("A", 32),
		"projection:" + strings.Repeat("b", 32),
	} {
		candidate := base
		candidate.SourceProjection = invalid
		if err := candidate.Validate(); err == nil {
			t.Fatalf("malformed source projection identity %q validated", invalid)
		}
	}
}

func TestIdentityReportsAbsentForUnlabeledContainer(t *testing.T) {
	if _, ok, err := FromLabels(nil); ok || err != nil {
		t.Fatalf("FromLabels(nil) = ok:%v err:%v, want absent nil", ok, err)
	}
}
