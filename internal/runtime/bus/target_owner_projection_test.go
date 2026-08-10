package bus

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestRouteTargetOwnerResolutionMatrix(t *testing.T) {
	rootRunID := eventtest.UUID("selected-root-run")
	rootOwner := eventtest.UUID("selected-root-owner")
	staticOwner := eventtest.UUID("selected-static-owner")
	singletonOwner := eventtest.UUID("selected-singleton-owner")
	templateOwner := eventtest.UUID("selected-template-owner")
	structuralOwner := eventtest.UUID("selected-structural-owner")

	projection := selectedRunTargetOwnerProjection{
		required: true,
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: rootRunID, EntityID: rootOwner},
			{ID: "static", FlowInstance: "review", EntityID: staticOwner},
			{ID: "singleton", FlowInstance: "portfolio", EntityID: singletonOwner},
			{ID: "template", FlowInstance: "operating/instance-a", EntityID: templateOwner},
		},
		structural: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: "operating", FlowInstance: "operating/instance-a", EntityID: structuralOwner,
		}),
	}

	tests := []struct {
		name            string
		blueprint       events.RouteIdentity
		allowStructural bool
		want            events.RouteIdentity
	}{
		{
			name: "root", blueprint: events.RouteIdentity{FlowID: "empire", FlowInstance: rootRunID},
			want: events.RouteIdentity{FlowID: "empire", FlowInstance: rootRunID, EntityID: rootOwner},
		},
		{
			name: "static", blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review"},
			want: events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: staticOwner},
		},
		{
			name: "nested static", blueprint: events.RouteIdentity{FlowID: "detail", FlowInstance: "operating/instance-a/detail"},
			allowStructural: true,
			want:            events.RouteIdentity{FlowID: "detail", FlowInstance: "operating/instance-a/detail", EntityID: structuralOwner},
		},
		{
			name: "singleton coordinator", blueprint: events.RouteIdentity{FlowID: "portfolio", FlowInstance: "portfolio"},
			want: events.RouteIdentity{FlowID: "portfolio", FlowInstance: "portfolio", EntityID: singletonOwner},
		},
		{
			name: "concrete template", blueprint: events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/instance-a"},
			want: events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/instance-a", EntityID: templateOwner},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := projection.resolveSelectedRoute(test.blueprint, test.allowStructural)
			if err != nil {
				t.Fatalf("resolve owner: %v", err)
			}
			if got.Route() != test.want.Normalized() || !got.ExistingEntity() {
				t.Fatalf("owner = %#v, want %#v", got, test.want.Normalized())
			}
		})
	}
}

func TestRouteTargetOwnerResolutionFailsClosedBeforeMutation(t *testing.T) {
	blueprint := events.RouteIdentity{FlowID: "portfolio", FlowInstance: "portfolio"}
	basePlan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient:       events.MustNodeDeliveryRecipient("portfolio-collector"),
		TargetBlueprint: blueprint,
		Producer:        routeIntentProducerConnectRoutePlan,
		Persist:         true,
	}}}.Normalized()

	tests := []struct {
		name       string
		projection selectedRunTargetOwnerProjection
		want       string
	}{
		{
			name: "missing", projection: selectedRunTargetOwnerProjection{required: true},
			want: "owner is missing",
		},
		{
			name: "ambiguous", projection: selectedRunTargetOwnerProjection{required: true, descriptors: []ActiveTargetDescriptor{
				{ID: "portfolio-a", FlowInstance: "portfolio", EntityID: eventtest.UUID("portfolio-owner-a")},
				{ID: "portfolio-b", FlowInstance: "portfolio", EntityID: eventtest.UUID("portfolio-owner-b")},
			}}, want: "owner is ambiguous",
		},
		{
			name: "foreign", projection: selectedRunTargetOwnerProjection{required: true, descriptors: []ActiveTargetDescriptor{
				{ID: "other", FlowInstance: "other", EntityID: eventtest.UUID("other-owner")},
			}}, want: "owner is missing",
		},
		{
			name: "disagreeing", projection: selectedRunTargetOwnerProjection{required: true, descriptors: []ActiveTargetDescriptor{
				{ID: "portfolio", FlowInstance: "portfolio", EntityID: eventtest.UUID("portfolio-owner")},
			}}, want: "owner is missing",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := basePlan
			plan.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), basePlan.DeliveryIntents...)
			if test.name == "disagreeing" {
				plan.DeliveryIntents[0].TargetBlueprint.EntityID = eventtest.UUID("foreign-authored-owner")
			}
			before := plan
			before.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), plan.DeliveryIntents...)
			if _, err := test.projection.resolveSelectedRoute(plan.DeliveryIntents[0].TargetBlueprint, false); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("resolve error = %v, want %q", err, test.want)
			}
			if !reflect.DeepEqual(plan, before) {
				t.Fatalf("failed resolution mutated plan: got %#v want %#v", plan, before)
			}
		})
	}
}

func TestActiveTargetDescriptorsRequireExactEntityOwner(t *testing.T) {
	tests := []struct {
		name       string
		descriptor ActiveTargetDescriptor
	}{
		{name: "selected target descriptor", descriptor: ActiveTargetDescriptor{ID: "portfolio", FlowInstance: "portfolio"}},
		{name: "active flow descriptor", descriptor: ActiveFlowInstanceDescriptor{
			InstanceID: "one", FlowInstance: "operating/one",
		}.TargetDescriptor()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := selectedRunTargetOwnerProjection{required: true, descriptors: []ActiveTargetDescriptor{test.descriptor}}
			if err := projection.validate(); err == nil || !strings.Contains(err.Error(), "missing exact entity identity") {
				t.Fatalf("validate error = %v, want missing exact entity identity", err)
			}
		})
	}
}

func TestExplicitAgentTargetConsumesExactTargetOwner(t *testing.T) {
	agentOwner := eventtest.UUID("explicit-agent-owner")
	agentIdentity := agentidentitytest.Runtime(t, "reviewer", "target-owner-proof", "review", "one", "review/one")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: agentOwner}
	projection := selectedRunTargetOwnerProjection{
		required:    true,
		descriptors: []ActiveTargetDescriptor{{ID: "reviewer", FlowInstance: target.FlowInstance, EntityID: agentOwner}},
	}
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: agentIdentity,
		TargetBlueprint: target, Producer: routeIntentProducerAgentPolicy, Persist: true,
	}}}.Normalized()

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve explicit agent target: %v", err)
	}
	routes := resolved.DeliveryRoutes()
	if len(routes) != 1 || routes[0].AgentIdentity != agentIdentity || routes[0].Target.Route() != target || !routes[0].Target.ExistingEntity() {
		t.Fatalf("resolved routes = %#v, want exact existing agent owner", routes)
	}

	untargeted := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: agentIdentity,
		Producer: routeIntentProducerAgentPolicy, Persist: true,
	}}}.Normalized()
	resolved, err = projection.resolveRoutePlan(untargeted)
	if err != nil {
		t.Fatalf("resolve identity-owned untargeted agent route: %v", err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].AgentIdentity != agentIdentity || !routes[0].Target.Empty() {
		t.Fatalf("untargeted agent routes = %#v, want exact AgentIdentity with explicit target absence", routes)
	}
}
