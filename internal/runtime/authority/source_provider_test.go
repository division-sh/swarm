package authority

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
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

	if err := provider.AuthorizeNotifyHuman(models.AgentConfig{Role: "public-worker"}); err != nil {
		t.Fatalf("effective declared name mailbox authority: %v", err)
	}
	if err := provider.AuthorizeNotifyHuman(models.AgentConfig{Role: "local-worker"}); err == nil {
		t.Fatal("local declaration coordinate retained mailbox authority")
	}
}

func authorityTestSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return semanticviewtest.WrapRootAgents(bundle)
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

func TestNewSourceProvider_AuthorizeNotifyHuman(t *testing.T) {
	provider := NewSourceProvider(authorityTestSource(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {ID: "reviewer", Role: "reviewer", ManagerFallback: "control-plane"},
		},
	}))
	if err := provider.AuthorizeNotifyHuman(models.AgentConfig{ID: "reviewer", Role: "reviewer"}); err != nil {
		t.Fatalf("declared reviewer notification authority: %v", err)
	}
	if err := provider.AuthorizeNotifyHuman(models.AgentConfig{ID: "unknown", Role: "unknown"}); err == nil {
		t.Fatal("undeclared role gained notification authority")
	}
	if err := NoopProvider().AuthorizeNotifyHuman(models.AgentConfig{ID: "reviewer", Role: "reviewer"}); err == nil {
		t.Fatal("missing source gained notification authority")
	}
}
