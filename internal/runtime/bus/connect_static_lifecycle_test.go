package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestConnectPendingAgentConsumesExactStaticDeclaration(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyReceiverMixedAgent(t))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	static := table.staticAgentDeclarationPlans()
	if len(static) != 1 {
		t.Fatalf("static declaration plans = %d, want 1", len(static))
	}
	var agent agentidentity.Plan
	for plan := range static {
		agent = plan
	}
	var connect runtimepinrouting.ConnectRoutePlan
	for _, plan := range runtimepinrouting.CompileConnectGraph(source).Plans() {
		if plan.ReceiverLocalEvent() == "work.completed" {
			connect = plan
		}
	}
	route := runtimepinrouting.ConnectDeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient(agent.Name.AgentID), AgentPlan: agent,
		Target: events.RouteIdentity{FlowID: "sink", FlowInstance: "sink", EntityID: eventtest.UUID("sink-owner")},
	}
	for _, test := range []struct {
		name                  string
		static, created, live bool
		want                  agentLifecycleAdmission
	}{
		{"static_first_delivery", true, false, false, agentLifecycleAdmissionStaticDeclaration},
		{"no_declaration_proof", false, false, false, agentLifecycleAdmissionMaterializingFlow},
		{"new_flow_requires_materialization", true, true, false, agentLifecycleAdmissionMaterializingFlow},
		{"already_live", true, false, true, agentLifecycleAdmissionNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			var declarations map[agentidentity.Plan]struct{}
			if test.static {
				declarations = static
			}
			var live []runtimepinrouting.ConnectDeliveryRoute
			if test.live {
				live = []runtimepinrouting.ConnectDeliveryRoute{route}
			}
			intents, err := connectRoutePlanDeliveryIntents(uuid.NewString(), connect, []runtimepinrouting.ConnectDeliveryRoute{route}, live, test.created, declarations)
			if err != nil || len(intents) != 1 || intents[0].AgentLifecycle != test.want {
				t.Fatalf("connect lifecycle = %+v, err=%v; want %v", intents, err, test.want)
			}
			intent := intents[0]
			intent.TargetOwnership = events.MustExistingEntityTarget(route.Target)
			err = validateAgentLifecycleTargetOwnership(intent)
			if (err != nil) != (test.want == agentLifecycleAdmissionMaterializingFlow) {
				t.Fatalf("existing target admission = %v, lifecycle=%v", err, test.want)
			}
		})
	}
}
