package authority

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestNewSourceProvider_UsesDeclaredAgentEmitEventsOnly(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:         "worker",
				Role:       "worker",
				EmitEvents: []string{"work.completed"},
			},
		},
	}

	provider := NewSourceProvider(authorityTestSource(bundle))
	got := provider.ProducerEventsForRole("worker")
	if len(got) != 1 || got[0] != "work.completed" {
		t.Fatalf("ProducerEventsForRole(worker) = %#v, want [work.completed]", got)
	}
	if got := provider.ProducerEventsForRole("dashboard"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(dashboard) = %#v, want nil/empty", got)
	}
	if got := provider.ProducerEventsForRole("actor-agent"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(actor-agent) = %#v, want nil/empty", got)
	}
}

func TestNewSourceProvider_UsesDeclaredRoleForProducerEvents(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-instance-1": {
				ID:         "agent-instance-1",
				Role:       "reviewer",
				EmitEvents: []string{"review.completed"},
			},
		},
	}

	provider := NewSourceProvider(authorityTestSource(bundle))
	got := provider.ProducerEventsForRole("reviewer")
	if len(got) != 1 || got[0] != "review.completed" {
		t.Fatalf("ProducerEventsForRole(reviewer) = %#v, want [review.completed]", got)
	}
	if got := provider.ProducerEventsForRole("agent-instance-1"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(agent-instance-1) = %#v, want nil/empty", got)
	}
}

func TestNewSourceProvider_UsesEffectiveDeclaredNameForMailboxRole(t *testing.T) {
	provider := NewSourceProvider(authorityTestSource(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"local-worker": {ID: "public-worker"},
		},
	}))

	if err := provider.AuthorizeMailboxSend(models.AgentConfig{Role: "public-worker"}); err != nil {
		t.Fatalf("effective declared name mailbox authority: %v", err)
	}
	if err := provider.AuthorizeMailboxSend(models.AgentConfig{Role: "local-worker"}); err == nil {
		t.Fatal("local declaration coordinate retained mailbox authority")
	}
}

func authorityTestSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	if bundle == nil {
		return nil
	}
	if bundle.URIRegistry.Agents == nil {
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.URIRegistry.ByURI == nil {
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	}
	for localID := range bundle.Agents {
		uri := "swarm-test://root/agents/" + localID
		ref := runtimecontracts.ContractURIRef{Kind: "agent", LocalID: localID, Full: uri}
		bundle.URIRegistry.Agents[localID] = ref
		bundle.URIRegistry.ByURI[uri] = ref
	}
	return semanticview.Wrap(bundle)
}

func TestNewSourceProvider_UsesEffectiveSystemNodeProduces(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"work.started": {Emit: runtimecontracts.EmitSpec{Event: "work.completed"}},
				},
			},
		},
	}

	provider := NewSourceProvider(semanticview.Wrap(bundle))
	got := provider.ProducerEventsForRole("worker")
	if len(got) != 1 || got[0] != "work.completed" {
		t.Fatalf("ProducerEventsForRole(worker) = %#v, want [work.completed]", got)
	}
}

func TestNewSourceProvider_AuthorityMatrix(t *testing.T) {
	provider := NewSourceProvider(authorityTestSource(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control-plane": {ID: "control-plane", Role: "control-plane"},
			"reviewer":      {ID: "reviewer", Role: "reviewer", ManagerFallback: "control-plane"},
			"worker":        {ID: "worker", Role: "worker", ManagerFallback: "reviewer"},
		},
	}))

	controlPlane := testAgentConfig(
		"control-plane",
		"control-plane",
		[]string{"message_flow", "mailbox_send"},
		"",
		"review/inst-1",
		"",
	)
	reviewer := testAgentConfig(
		"reviewer",
		"reviewer",
		[]string{"message_peers", "mailbox_send"},
		"",
		"review/inst-1",
		"control-plane",
	)
	worker := testAgentConfig(
		"worker-a",
		"worker",
		[]string{"message_peers"},
		"",
		"review/inst-1",
		"control-plane",
	)
	otherFlowWorker := testAgentConfig(
		"worker-b",
		"worker",
		[]string{"message_peers"},
		"",
		"review/inst-2",
		"control-plane",
	)

	if !provider.HasMessageAuthority(controlPlane, reviewer) {
		t.Fatal("expected control-plane to message reviewer in same flow instance")
	}
	if !provider.HasMessageAuthority(reviewer, worker) {
		t.Fatal("expected peers with same manager_fallback to message each other")
	}
	if provider.HasMessageAuthority(worker, otherFlowWorker) {
		t.Fatal("expected cross-flow peer messaging to be denied")
	}
	if err := provider.AuthorizeMailboxSend(reviewer); err != nil {
		t.Fatalf("expected reviewer mailbox permission: %v", err)
	}
}

func TestMessageSelfAuthorityRequiresExactConcreteIdentity(t *testing.T) {
	provider := NewSourceProvider(authorityTestSource(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {ID: "worker", Role: "worker"},
		},
	}))
	workerA := testAgentConfig("worker", "worker", nil, "", "review/inst-a", "")
	workerA.Identity = agentidentitytest.Runtime(t, "worker", "authority-test", "review", "inst-a", "review/inst-a")
	workerB := testAgentConfig("worker", "worker", nil, "", "review/inst-b", "")
	workerB.Identity = agentidentitytest.Runtime(t, "worker", "authority-test", "review", "inst-b", "review/inst-b")

	if !provider.HasMessageAuthority(workerA, workerA) {
		t.Fatal("exact concrete self message was denied")
	}
	if provider.HasMessageAuthority(workerA, workerB) {
		t.Fatal("same-slug sibling was authorized as self")
	}
	noop := NoopProvider()
	if !noop.HasMessageAuthority(workerA, workerA) {
		t.Fatal("noop provider denied exact concrete self")
	}
	if noop.HasMessageAuthority(workerA, workerB) {
		t.Fatal("noop provider authorized same-slug sibling as self")
	}
	malformed := workerA
	malformed.Identity = agentidentity.Identity{}
	if noop.HasMessageAuthority(malformed, malformed) {
		t.Fatal("noop provider authorized malformed identity")
	}
}

func testAgentConfig(id, role string, permissions []string, entityID, flowPath, managerFallback string) models.AgentConfig {
	return models.AgentConfig{
		ExecutionMode:   "live",
		ID:              id,
		Role:            role,
		Permissions:     permissions,
		Tools:           permissions,
		EntityID:        entityID,
		ParentAgent:     managerFallback,
		ManagerFallback: managerFallback,
		FlowPath:        flowPath,
	}
}
