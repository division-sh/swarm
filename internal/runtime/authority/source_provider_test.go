package authority

import (
	"strings"
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

	provider := NewSourceProvider(semanticview.Wrap(bundle))
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

	provider := NewSourceProvider(semanticview.Wrap(bundle))
	got := provider.ProducerEventsForRole("reviewer")
	if len(got) != 1 || got[0] != "review.completed" {
		t.Fatalf("ProducerEventsForRole(reviewer) = %#v, want [review.completed]", got)
	}
	if got := provider.ProducerEventsForRole("agent-instance-1"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(agent-instance-1) = %#v, want nil/empty", got)
	}
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
	provider := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control-plane": {ID: "control-plane", Role: "control-plane"},
			"reviewer":      {ID: "reviewer", Role: "reviewer", ManagerFallback: "control-plane"},
			"worker":        {ID: "worker", Role: "worker", ManagerFallback: "reviewer"},
		},
	}))

	controlPlane := testAgentConfig(
		"control-plane",
		"control-plane",
		[]string{"message_flow", "configure_routing", "agent_hire", "mailbox_send"},
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
	if err := provider.AuthorizeRouting(controlPlane, worker, "active"); err != nil {
		t.Fatalf("expected control-plane routing authority: %v", err)
	}
	if err := provider.AuthorizeManagement(controlPlane, reviewer); err != nil {
		t.Fatalf("expected control-plane to manage reviewer: %v", err)
	}
	if err := provider.AuthorizeManagement(controlPlane, worker); err != nil {
		t.Fatalf("expected control-plane to manage nested worker: %v", err)
	}
	if err := provider.AuthorizeManagement(reviewer, controlPlane); err == nil {
		t.Fatal("expected reviewer ancestor management to be denied")
	}
	if err := provider.AuthorizeMailboxSend(reviewer); err != nil {
		t.Fatalf("expected reviewer mailbox permission: %v", err)
	}
}

func TestMessageAndRoutingSelfAuthorityRequiresExactConcreteIdentity(t *testing.T) {
	provider := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
	workerA.Permissions = []string{"configure_routing"}
	if err := provider.AuthorizeRouting(workerA, workerB, "active"); err == nil {
		t.Fatal("same-slug sibling was authorized as routing self")
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

func TestSourceProvider_ManagedAgentGraphUpdates(t *testing.T) {
	provider, ok := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control-plane": {ID: "control-plane", Role: "control-plane"},
		},
	})).(*sourceProvider)
	if !ok {
		t.Fatal("expected sourceProvider")
	}

	controlPlane := testAgentConfig("control-plane", "control-plane", []string{"agent_hire"}, "", "review/inst-1", "")
	reviewer := testAgentConfig("reviewer", "reviewer", []string{}, "", "review/inst-1", "control-plane")
	worker := testAgentConfig("worker", "worker", []string{}, "", "review/inst-1", "reviewer")
	controlPlane.Identity = agentidentitytest.Runtime(t, "control-plane", "authority-test", "review", "inst-1", "review/inst-1")
	reviewer.Identity = agentidentitytest.Runtime(t, "reviewer", "authority-test", "review", "inst-1", "review/inst-1")
	worker.Identity = agentidentitytest.Runtime(t, "worker", "authority-test", "review", "inst-1", "review/inst-1")

	if err := provider.UpsertManagedAgent(reviewer.Identity, controlPlane.Identity); err != nil {
		t.Fatalf("upsert reviewer authority: %v", err)
	}
	if err := provider.UpsertManagedAgent(worker.Identity, reviewer.Identity); err != nil {
		t.Fatalf("upsert worker authority: %v", err)
	}
	if parent, found, err := ManagedAgentParent(provider, worker.Identity); err != nil || !found || parent != reviewer.Identity {
		t.Fatalf("managed worker parent = %#v found=%v err=%v, want exact reviewer identity", parent, found, err)
	}
	if err := provider.AuthorizeManagement(controlPlane, worker); err != nil {
		t.Fatalf("expected dynamic managed descendant authorization, got %v", err)
	}

	if err := provider.RemoveManagedAgent(reviewer.Identity); err != nil {
		t.Fatalf("remove reviewer authority: %v", err)
	}
	if _, found, err := ManagedAgentParent(provider, reviewer.Identity); err != nil || found {
		t.Fatalf("removed reviewer parent found=%v err=%v, want absent", found, err)
	}
	if err := provider.AuthorizeManagement(controlPlane, worker); err == nil {
		t.Fatal("expected descendant authorization to break after manager removal")
	}
}

func TestSourceProvider_ConcreteManagedParentOverridesDeclaredFallback(t *testing.T) {
	provider, ok := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"declared-manager": {ID: "declared-manager", Role: "declared-manager"},
			"concrete-manager": {ID: "concrete-manager", Role: "concrete-manager"},
			"worker":           {ID: "worker", Role: "worker", ManagerFallback: "declared-manager"},
		},
	})).(*sourceProvider)
	if !ok {
		t.Fatal("expected sourceProvider")
	}
	const flowPath = "review/inst-1"
	declaredManager := testAgentConfig("declared-manager", "declared-manager", []string{"agent_reconfigure"}, "", flowPath, "")
	concreteManager := testAgentConfig("concrete-manager", "concrete-manager", []string{"agent_reconfigure"}, "", flowPath, "")
	worker := testAgentConfig("worker", "worker", nil, "", flowPath, "declared-manager")
	declaredManager.Identity = agentidentitytest.Runtime(t, declaredManager.ID, "authority-test", "review", "inst-1", flowPath)
	concreteManager.Identity = agentidentitytest.Runtime(t, concreteManager.ID, "authority-test", "review", "inst-1", flowPath)
	worker.Identity = agentidentitytest.Runtime(t, worker.ID, "authority-test", "review", "inst-1", flowPath)

	if err := provider.UpsertManagedAgent(worker.Identity, concreteManager.Identity); err != nil {
		t.Fatalf("replace declared parent with concrete authority: %v", err)
	}
	if err := provider.AuthorizeManagement(concreteManager, worker); err != nil {
		t.Fatalf("concrete replacement parent lacks authority: %v", err)
	}
	if err := provider.AuthorizeManagement(declaredManager, worker); err == nil {
		t.Fatal("declared parent retained authority after concrete replacement")
	}
}

func TestSourceProvider_ManagedParentUpdateRejectsIndirectCycle(t *testing.T) {
	provider, ok := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})).(*sourceProvider)
	if !ok {
		t.Fatal("expected sourceProvider")
	}
	const flowPath = "review/inst-1"
	root := testAgentConfig("root", "root", []string{"agent_reconfigure"}, "", flowPath, "")
	first := testAgentConfig("first", "first", []string{"agent_reconfigure"}, "", flowPath, "root")
	second := testAgentConfig("second", "second", []string{"agent_reconfigure"}, "", flowPath, "first")
	root.Identity = agentidentitytest.Runtime(t, root.ID, "authority-test", "review", "inst-1", flowPath)
	first.Identity = agentidentitytest.Runtime(t, first.ID, "authority-test", "review", "inst-1", flowPath)
	second.Identity = agentidentitytest.Runtime(t, second.ID, "authority-test", "review", "inst-1", flowPath)

	if err := provider.UpsertManagedAgent(first.Identity, root.Identity); err != nil {
		t.Fatalf("seed first parent: %v", err)
	}
	if err := provider.UpsertManagedAgent(second.Identity, first.Identity); err != nil {
		t.Fatalf("seed second parent: %v", err)
	}
	if err := provider.UpsertManagedAgent(first.Identity, second.Identity); err == nil || !strings.Contains(err.Error(), "create a cycle") {
		t.Fatalf("cycle update error = %v, want cycle rejection", err)
	}
	if err := provider.AuthorizeManagement(root, first); err != nil {
		t.Fatalf("rejected cycle changed first parent: %v", err)
	}
	if err := provider.AuthorizeManagement(first, second); err != nil {
		t.Fatalf("rejected cycle changed second parent: %v", err)
	}
	if err := provider.AuthorizeManagement(second, first); err == nil {
		t.Fatal("rejected cycle granted reverse management")
	}
}

func TestSourceProvider_ManagedAgentRemovalKeepsSameSlugSiblingAuthority(t *testing.T) {
	provider, ok := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})).(*sourceProvider)
	if !ok {
		t.Fatal("expected sourceProvider")
	}

	managerA := testAgentConfig("manager", "manager", []string{"agent_fire"}, "", "review/inst-a", "")
	managerA.Identity = agentidentitytest.Runtime(t, "manager", "authority-test", "review", "inst-a", "review/inst-a")
	managerB := testAgentConfig("manager", "manager", []string{"agent_fire"}, "", "review/inst-b", "")
	managerB.Identity = agentidentitytest.Runtime(t, "manager", "authority-test", "review", "inst-b", "review/inst-b")
	workerA := testAgentConfig("worker", "worker", nil, "", "review/inst-a", "manager")
	workerA.Identity = agentidentitytest.Runtime(t, "worker", "authority-test", "review", "inst-a", "review/inst-a")
	workerB := testAgentConfig("worker", "worker", nil, "", "review/inst-b", "manager")
	workerB.Identity = agentidentitytest.Runtime(t, "worker", "authority-test", "review", "inst-b", "review/inst-b")

	if err := provider.UpsertManagedAgent(workerA.Identity, managerA.Identity); err != nil {
		t.Fatalf("upsert first worker authority: %v", err)
	}
	if err := provider.UpsertManagedAgent(workerB.Identity, managerB.Identity); err != nil {
		t.Fatalf("upsert sibling worker authority: %v", err)
	}
	if err := provider.RemoveManagedAgent(workerA.Identity); err != nil {
		t.Fatalf("remove first worker authority: %v", err)
	}
	if err := provider.AuthorizeManagement(managerA, workerA); err == nil {
		t.Fatal("expected removed concrete worker authority to be absent")
	}
	if err := provider.AuthorizeManagement(managerB, workerB); err != nil {
		t.Fatalf("same-slug sibling authority was removed: %v", err)
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
