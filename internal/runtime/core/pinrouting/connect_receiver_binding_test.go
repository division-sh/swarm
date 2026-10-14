package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestCompiledReceiverBindingPreservesDefinitionAndExactInstance(t *testing.T) {
	graph, plan, _ := scopedCandidateGraph(t)
	node, err := NewConnectNodeRecipient(identitytest.FlowNode(t, "consumer", "receiver"), "consumer")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("consumer", "declaration", "consumer")
	if err != nil {
		t.Fatal(err)
	}
	name := agentidentity.Name{AgentID: "reviewer", Owner: "test://agents/reviewer", Source: agentidentity.NameSourceDeclared}
	agentPlan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewConnectAgentRecipient("reviewer", "consumer", agentPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, recipient := range []ConnectRecipient{node, agent} {
		t.Run(recipient.ID(), func(t *testing.T) {
			definitions := graph.AdmitReceiverRecipient("consumer", "work.accepted", recipient)
			if len(definitions) != 1 {
				t.Fatalf("definitions=%v", definitions)
			}
			definition := definitions[0]
			for _, id := range []string{"one", "two"} {
				instance := flowidentity.Instance{TemplateID: "consumer", ScopeKey: "consumer", InstanceID: id, InstancePath: "consumer/" + id, EntityID: "entity-" + id}
				bound, err := graph.BindReceiverInstance(definition, instance)
				if err != nil {
					t.Fatal(err)
				}
				if definition != definitions[0] || definition.recipient != recipient || bound.receiverPin != definition.receiverPin || bound.recipient.handler != definition.recipient.handler {
					t.Fatal("instance binding mutated the compiled definition or handler")
				}
				if bound.recipient.Kind() == ConnectRecipientAgent {
					want, err := instance.Route().AgentIdentityRoute()
					if err != nil {
						t.Fatal(err)
					}
					if bound.recipient.agentPlan.Name != name || bound.recipient.agentPlan.Route != want {
						t.Fatalf("bound agent=%+v want name=%+v route=%+v", bound.recipient.agentPlan, name, want)
					}
				}
				target := events.RouteIdentity{FlowID: "consumer", FlowInstance: instance.InstancePath, EntityID: instance.EntityID}
				evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{target}, []ConnectRecipientRegistration{bound})
				if got := evaluation.Recipients(); len(got) != 1 || got[0].Path() != instance.InstancePath {
					t.Fatalf("exact bound recipients=%v", got)
				}
			}
			for _, invalid := range []flowidentity.Instance{
				{},
				{TemplateID: "consumer", InstancePath: "consumer/one"},
				{TemplateID: "unrelated", InstancePath: "unrelated/one", EntityID: "entity-one"},
			} {
				if _, err := graph.BindReceiverInstance(definition, invalid); err == nil {
					t.Fatalf("invalid binding accepted: %+v", invalid)
				}
			}
			instance := flowidentity.Instance{TemplateID: "consumer", InstancePath: "consumer/one", EntityID: "entity-one"}
			if _, err := (CompiledConnectGraph{}).BindReceiverInstance(definition, instance); err == nil {
				t.Fatal("foreign graph accepted a compiled definition")
			}
			if _, err := graph.BindReceiverInstance(ConnectRecipientRegistration{}, instance); err == nil {
				t.Fatal("empty registration manufactured receiver permission")
			}
		})
	}
}
