package runforkadmission

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestCompletedInputRecipientRequiresExactExecutionSlot(t *testing.T) {
	node := identitytest.FlowNode(t, "child", "worker")
	recipient, err := forkrecipient.NewLocal(forkrecipient.Input{
		Recipient: events.MustNodeDeliveryRecipient(node), Path: "child/one", HandlerNode: node, HandlerEvent: "work.first",
	})
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: recipient.Recipient, Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "child", FlowInstance: "child/one", EntityID: "source-entity",
	})}
	if !completedInputRecipient("source-run", route, recipient) {
		t.Fatal("exact completed recipient was not excluded")
	}
	for _, tc := range []struct {
		name   string
		target events.RouteIdentity
	}{
		{"other_instance", events.RouteIdentity{FlowID: "child", FlowInstance: "child/two", EntityID: "source-entity"}},
		{"other_flow", events.RouteIdentity{FlowID: "other", FlowInstance: "child/one", EntityID: "source-entity"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := route
			changed.Target = events.MustExistingEntityTarget(tc.target)
			if completedInputRecipient("source-run", changed, recipient) {
				t.Fatal("completion removed a distinct pending recipient")
			}
		})
	}
	other := route
	other.Recipient = events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "child", "other"))
	if completedInputRecipient("source-run", other, recipient) {
		t.Fatal("another handler removed pending work")
	}
	plan := recipientAuthorityAgentPlan(t, "child")
	agent, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient("worker"), Path: "child", AgentPlan: plan, HandlerEvent: "work.first"})
	if err != nil {
		t.Fatal(err)
	}
	live, err := plan.Live("source-run")
	if err != nil {
		t.Fatal(err)
	}
	agentRoute := events.DeliveryRoute{Recipient: agent.Recipient, AgentIdentity: live}
	if !completedInputRecipient("source-run", agentRoute, agent) {
		t.Fatal("exact completed agent was not excluded")
	}
	for _, different := range []agentidentity.Identity{
		mustInputAgent(t, plan, "other-run"), mustInputAgent(t, recipientAuthorityAgentPlan(t, "other"), "source-run"),
	} {
		agentRoute.AgentIdentity = different
		if completedInputRecipient("source-run", agentRoute, agent) {
			t.Fatal("foreign agent completion removed pending work")
		}
	}
}

func mustInputAgent(t *testing.T, plan agentidentity.Plan, run string) agentidentity.Identity {
	t.Helper()
	live, err := plan.Live(run)
	if err != nil {
		t.Fatal(err)
	}
	return live
}
