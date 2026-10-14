package bus

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRouteTargetOwnerResolutionMatrix(t *testing.T) {
	rootRunID := eventtest.UUID("selected-root-run")
	rootOwner := eventtest.UUID("selected-root-owner")
	staticOwner := eventtest.UUID("selected-static-owner")
	singletonOwner := eventtest.UUID("selected-singleton-owner")
	templateOwner := eventtest.UUID("selected-template-owner")

	projection := selectedRunTargetOwnerProjection{
		required: true,
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: rootRunID, EntityID: rootOwner},
			{ID: "static", FlowInstance: "review", EntityID: staticOwner},
			{ID: "singleton", FlowInstance: "portfolio", EntityID: singletonOwner},
			{ID: "template", FlowInstance: "operating/instance-a", EntityID: templateOwner},
		},
	}

	tests := []struct {
		name      string
		blueprint events.RouteIdentity
		want      events.RouteIdentity
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
			got, err := projection.resolveSelectedRoute(test.blueprint)
			if err != nil {
				t.Fatalf("resolve owner: %v", err)
			}
			if got.Route() != test.want.Normalized() || !got.ExistingEntity() {
				t.Fatalf("owner = %#v, want %#v", got, test.want.Normalized())
			}
		})
	}
}

func TestTargetOwnerDedupPreservesUnavailableEvidenceInBothOrders(t *testing.T) {
	blueprint := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{
			"review": runtimecontracts.BuildWorkflowStageTopology("review", "active", []string{"active", "done"}, []string{"done"}, nil, nil, nil),
		}},
	})
	active := ActiveTargetDescriptor{ID: "one", FlowInstance: blueprint.FlowInstance, EntityID: eventtest.UUID("exact-owner")}
	unavailable := active
	unavailable.Availability = runtimepipeline.NewDeliveryTargetAvailability("active", "draining", false)
	for _, descriptors := range [][]ActiveTargetDescriptor{{active, unavailable}, {unavailable, active}} {
		projection := selectedRunTargetOwnerProjection{required: true, source: source}
		ordered := newOrderedActiveTargetDescriptors(projection.descriptors)
		for _, descriptor := range descriptors {
			ordered.add(descriptor)
		}
		projection.descriptors = ordered.descriptors
		if len(projection.descriptors) != 2 {
			t.Fatalf("dedup erased contradictory availability: %#v", projection.descriptors)
		}
		if _, err := projection.resolveSelectedRoute(blueprint); err == nil || !strings.Contains(err.Error(), "owner is unavailable") {
			t.Fatalf("agent/exact-route projection admitted unavailable receiver: %v", err)
		}
	}
}

func TestSelectedTargetOwnerNeverUsesParentDescriptor(t *testing.T) {
	blueprint := events.RouteIdentity{FlowID: "parent/child", FlowInstance: "parent/child"}
	parent := ActiveTargetDescriptor{ID: "parent", FlowInstance: "parent", EntityID: eventtest.UUID("parent-owner")}
	projection := selectedRunTargetOwnerProjection{required: true, descriptors: []ActiveTargetDescriptor{parent}}
	if _, err := projection.resolveSelectedRoute(blueprint); err == nil {
		t.Fatal("parent descriptor became receiver-owned state")
	}
	child := ActiveTargetDescriptor{ID: "child", FlowInstance: blueprint.FlowInstance, EntityID: eventtest.UUID("child-owner")}
	projection.descriptors = append(projection.descriptors, child)
	got, err := projection.resolveSelectedRoute(blueprint)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ExistingEntity() || got.Route().EntityID != child.EntityID {
		t.Fatalf("wrong child owner: %#v", got)
	}
}

func TestRouteTargetOwnerResolutionFailsClosedBeforeMutation(t *testing.T) {
	blueprint := events.RouteIdentity{FlowID: "portfolio", FlowInstance: "portfolio"}
	basePlan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient:       events.MustNodeDeliveryRecipient(testRootNode(t, "portfolio-collector")),
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
			if _, err := test.projection.resolveSelectedRoute(plan.DeliveryIntents[0].TargetBlueprint); err == nil || !strings.Contains(err.Error(), test.want) {
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
		{name: "keyed target descriptor", descriptor: ActiveTargetDescriptor{
			FlowID: "operating", ID: "one", FlowInstance: "operating/one",
		}},
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
		required: true,
		agents:   map[agentidentity.Identity]ActiveAgentDescriptor{agentIdentity: {Identity: agentIdentity, EntityID: agentOwner}},
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("explicit-agent-publication"), "work.ready", "operator", "", nil, 0, agentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	plan := RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{{
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

	untargeted := RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: agentIdentity,
		Producer: routeIntentProducerDirectPolicy, Persist: true,
	}}}.Normalized()
	resolved, err = projection.resolveRoutePlan(untargeted)
	if err != nil {
		t.Fatalf("resolve direct untargeted agent route: %v", err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].AgentIdentity != agentIdentity || !routes[0].Target.Empty() {
		t.Fatalf("untargeted direct agent routes = %#v, want exact AgentIdentity with explicit target absence", routes)
	}
}

func TestRootAgentTargetAgreementRequiresExactRunFlowAndEntity(t *testing.T) {
	runID := eventtest.UUID("human-root-run")
	source := connectRoutePlanRootProducerSingletonSource(t)
	root, err := semanticview.AdmitRootExecutionCoordinate(source, runID)
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentitytest.RootDeclaredForRun(t, runID, "requester", "human-root-owner")
	entity := eventtest.UUID("human-root-entity")
	for _, tc := range []struct {
		name   string
		owner  string
		target events.RouteIdentity
		want   bool
	}{
		{"entityless", "", events.RouteIdentity{FlowID: ".", FlowInstance: runID}, true},
		{"entity_owned", entity, events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entity}, true},
		{"borrowed_entity", "", events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entity}, false},
		{"omitted_entity", entity, events.RouteIdentity{FlowID: ".", FlowInstance: runID}, false},
		{"foreign_entity", entity, events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: eventtest.UUID("foreign")}, false},
		{"foreign_run", "", events.RouteIdentity{FlowID: ".", FlowInstance: eventtest.UUID("foreign")}, false},
		{"foreign_flow", "", events.RouteIdentity{FlowID: "child", FlowInstance: runID}, false},
		{"missing_flow", "", events.RouteIdentity{FlowInstance: runID}, false},
		{"missing_instance", "", events.RouteIdentity{FlowID: "."}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := ActiveAgentDescriptor{Identity: identity, EntityID: tc.owner}
			if got := routeMatchesAgentDescriptor(tc.target, descriptor, root); got != tc.want {
				t.Fatalf("root target agreement=%v, want %v", got, tc.want)
			}
			_, singular := deliveryTargetForDescriptor(descriptor, tc.target, nil, root)
			_, multiple := deliveryTargetForDescriptor(descriptor, events.RouteIdentity{}, []events.RouteIdentity{tc.target}, root)
			if singular != tc.want || multiple != tc.want {
				t.Fatalf("singular/multiple target agreement=%v/%v, want %v", singular, multiple, tc.want)
			}
		})
	}
}

func TestExplicitAgentTargetPreservesEntitylessAndSelectedEntityOwners(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "entityless-target-proof", "review", "one", "review/one")
	plan := RoutePlan{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("entityless-agent-publication"), "work.ready", "operator", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC()), DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		TargetBlueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}, Persist: true,
	}}}.Normalized()
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			identity: {Identity: identity},
		},
		required: true,
	}

	assertEntityless := func() {
		t.Helper()
		resolved, err := projection.resolveRoutePlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		routes := resolved.DeliveryRoutes()
		if len(routes) != 1 || !routes[0].Target.EntitylessReceiver() || routes[0].Target.Route() != plan.DeliveryIntents[0].TargetBlueprint {
			t.Fatalf("resolved routes = %#v, want exact entityless agent target", routes)
		}
	}
	assertEntityless()

	selectedOwner := eventtest.UUID("review-one-selected-owner")
	projection.descriptors = []ActiveTargetDescriptor{{ID: "review-one", FlowInstance: "review/one", EntityID: selectedOwner}}
	projection.targetsAvailable = true
	assertEntityless()
	plan.DeliveryIntents[0].TargetBlueprint.EntityID = selectedOwner
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "disagrees with entityless active agent") {
		t.Fatalf("borrowed entity target error = %v, want rejection", err)
	}
	projection.agents[identity] = ActiveAgentDescriptor{Identity: identity, EntityID: selectedOwner}
	plan.DeliveryIntents[0].TargetBlueprint.EntityID = ""
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "disagrees with exact active agent entity") {
		t.Fatalf("empty actor entity became a wildcard: %v", err)
	}
	plan.DeliveryIntents[0].TargetBlueprint.EntityID = selectedOwner
	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve selected agent target: %v", err)
	}
	routes := resolved.DeliveryRoutes()
	if len(routes) != 1 || !routes[0].Target.ExistingEntity() || routes[0].Target.Route().EntityID != selectedOwner {
		t.Fatalf("resolved routes = %#v, want exact selected existing agent target", routes)
	}

	contradictory := plan
	contradictory.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), plan.DeliveryIntents...)
	contradictory.DeliveryIntents[0].TargetBlueprint.FlowInstance = "review/other"
	if _, err := projection.resolveRoutePlan(contradictory); err == nil || !strings.Contains(err.Error(), "disagrees with exact agent identity") {
		t.Fatalf("contradictory agent target error = %v, want exact identity disagreement", err)
	}
	contradictory.DeliveryIntents[0].TargetBlueprint = plan.DeliveryIntents[0].TargetBlueprint
	contradictory.DeliveryIntents[0].TargetBlueprint.FlowID = "foreign"
	if _, err := projection.resolveRoutePlan(contradictory); err == nil || !strings.Contains(err.Error(), "disagrees with exact agent identity scope") {
		t.Fatalf("foreign flow target error = %v, want exact scope disagreement", err)
	}
	contradictory.DeliveryIntents[0].TargetBlueprint = plan.DeliveryIntents[0].TargetBlueprint
	contradictory.DeliveryIntents[0].TargetBlueprint.EntityID = eventtest.UUID("foreign-agent-entity")
	if _, err := projection.resolveRoutePlan(contradictory); err == nil || !strings.Contains(err.Error(), "disagrees with exact active agent entity") {
		t.Fatalf("foreign entity target error = %v, want exact ownership disagreement", err)
	}
}

func TestSameFlowAgentPolicyDerivesExactSelectedRunOwners(t *testing.T) {
	runID := eventtest.UUID("same-flow-agent-run")
	rootIdentity := agentidentitytest.RootDeclaredForRun(t, runID, "root-reviewer", "same-flow-agent-root")
	nestedIdentity := agentidentitytest.DeclaredForRun(t, runID, "nested-reviewer", "same-flow-agent-nested", "consumer", "consumer", "consumer")
	rootOwner := eventtest.UUID("same-flow-agent-root-owner")
	nestedOwner := eventtest.UUID("same-flow-agent-nested-owner")
	source := connectRoutePlanRootProducerSingletonSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.FlowTree.Root == nil {
		t.Fatal("declared-agent source requires a root flow")
	}
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity, EntityID: rootOwner},
			nestedIdentity: {Identity: nestedIdentity, EntityID: nestedOwner},
		},
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: runID, EntityID: rootOwner},
			{ID: "consumer", FlowInstance: "consumer", EntityID: nestedOwner},
		},
		targetsAvailable: true,
		required:         true,
		source:           source,
	}
	projection.context = admittedAgentConstructionContext(t, source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-agent-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{
			{Recipient: events.MustAgentDeliveryRecipient("root-reviewer"), AgentIdentity: rootIdentity, Producer: routeIntentProducerAgentPolicy, Persist: true},
			{Recipient: events.MustAgentDeliveryRecipient("nested-reviewer"), AgentIdentity: nestedIdentity, Producer: routeIntentProducerAgentPolicy, Persist: true},
		},
	}

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve same-flow agent targets: %v", err)
	}
	routes := resolved.DeliveryRoutes()
	if len(routes) != 2 {
		t.Fatalf("resolved routes = %#v, want root and nested agents", routes)
	}
	want := map[agentidentity.Identity]events.RouteIdentity{
		rootIdentity:   {FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: rootOwner},
		nestedIdentity: {FlowID: "consumer", FlowInstance: "consumer", EntityID: nestedOwner},
	}
	for _, route := range routes {
		if !route.Target.ExistingEntity() || route.Target.Route() != want[route.AgentIdentity].Normalized() {
			t.Fatalf("route = %#v, want exact selected owner %#v", route, want[route.AgentIdentity].Normalized())
		}
	}
}

func TestSameFlowRuntimeCreatedAgentDoesNotConsumeSelectedOwner(t *testing.T) {
	runID := eventtest.UUID("same-flow-runtime-carrier-run")
	identity := agentidentitytest.RootRuntimeForRun(t, runID, "workflow-runtime", "same-flow-runtime-carrier")
	ownerID := eventtest.UUID("same-flow-runtime-carrier-owner")
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			identity: {Identity: identity},
		},
		descriptors:      []ActiveTargetDescriptor{{ID: "root", FlowInstance: runID, EntityID: ownerID, Materializing: true}},
		targetsAvailable: true,
		required:         true,
		source:           connectRoutePlanRootProducerSingletonSource(t),
	}
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-runtime-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Recipient: events.MustAgentDeliveryRecipient("workflow-runtime"), AgentIdentity: identity,
			Producer: routeIntentProducerAgentPolicy, Persist: true,
		}},
	}

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve runtime-created carrier: %v", err)
	}
	routes := resolved.DeliveryRoutes()
	if len(routes) != 1 || routes[0].AgentIdentity != identity || !routes[0].Target.Empty() {
		t.Fatalf("runtime-created carrier routes = %#v, want exact identity with explicit target absence", routes)
	}
}

func TestRuntimeCreatedTargetlessIgnoresNativeAndProspectiveConstruction(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	runID := eventtest.UUID("independent-runtime-native-observation")
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("independent-runtime-publication"), "work.ready", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	for _, root := range []bool{true, false} {
		for _, prospective := range []bool{false, true} {
			t.Run(fmt.Sprintf("root=%t/prospective=%t", root, prospective), func(t *testing.T) {
				identity := agentidentitytest.RootRuntimeForRun(t, runID, "workflow-runtime", "independent-runtime")
				flow := semanticview.RootExecutionFlowID(source)
				if !root {
					flow = "consumer"
					identity = agentidentitytest.RuntimeForRun(t, runID, "workflow-runtime", "independent-runtime", flow, flow, flow)
				}
				projection := selectedRunTargetOwnerProjection{required: true, source: source, agentsAvailable: true,
					agents: map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity}},
				}
				if prospective {
					instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
					owner := newTestFlowInstanceActivationOwner(nil)
					activation, err := owner.PrepareFlowInstanceActivation(constructionIndexContext(t, source), runtimepipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance, TriggerEvent: event, OccurredAt: event.CreatedAt()})
					if err != nil {
						t.Fatal(err)
					}
					projection, err = projection.withActivationPlans([]runtimepipeline.FlowInstanceActivationPlan{activation})
					if err != nil {
						t.Fatal(err)
					}
				} else {
					projection.context = admittedAgentConstructionContext(t, source, runID)
				}
				intent := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity, Producer: routeIntentProducerAgentPolicy, Persist: true}
				resolved, err := projection.resolveRoutePlan(RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}})
				if err != nil {
					t.Fatal(err)
				}
				if routes := resolved.DeliveryRoutes(); len(routes) != 1 || !routes[0].Target.Empty() || !routes[0].Initialization.Empty() {
					t.Fatalf("independent targetless actor acquired flow ownership: %+v", routes)
				}
				instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
				intent.TargetBlueprint = events.RouteIdentity{FlowID: flow, FlowInstance: instance.InstancePath}
				if _, err := projection.resolveRoutePlan(RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}}); err != nil {
					t.Fatalf("explicit matching runtime target refused: %v", err)
				}
				intent.TargetBlueprint.EntityID = instance.EntityID
				if _, err := projection.resolveRoutePlan(RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}}); err == nil {
					t.Fatal("explicit foreign actor entity was erased")
				}
			})
		}
	}
}

func TestSameFlowDeclaredEntitylessAgentsDoNotInheritSelectedNodeOwners(t *testing.T) {
	runID := eventtest.UUID("same-flow-entityless-run")
	rootIdentity := agentidentitytest.RootDeclaredForRun(t, runID, "root-reviewer", "same-flow-entityless-root")
	nestedIdentity := agentidentitytest.DeclaredForRun(t, runID, "nested-reviewer", "same-flow-entityless-nested", "consumer", "consumer", "consumer")
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity},
			nestedIdentity: {Identity: nestedIdentity},
		},
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: runID, EntityID: eventtest.UUID("same-flow-root-node-owner")},
			{ID: "consumer", FlowInstance: "consumer", EntityID: eventtest.UUID("same-flow-nested-node-owner")},
		},
		targetsAvailable: true,
		required:         true,
		source:           connectRoutePlanRootProducerSingletonSource(t),
	}
	projection.context = admittedAgentConstructionContext(t, projection.source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-entityless-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{
			{Recipient: events.MustAgentDeliveryRecipient("root-reviewer"), AgentIdentity: rootIdentity, Producer: routeIntentProducerAgentPolicy, Persist: true},
			{Recipient: events.MustAgentDeliveryRecipient("nested-reviewer"), AgentIdentity: nestedIdentity, Producer: routeIntentProducerAgentPolicy, Persist: true},
		},
	}

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve entityless declared agents: %v", err)
	}
	for _, route := range resolved.DeliveryRoutes() {
		if !route.Target.EntitylessReceiver() || route.Target.Route().EntityID != "" || route.Initialization.Empty() || route.Initialization.ConstructionTarget().Route().EntityID == "" {
			t.Fatalf("entityless declared agent route = %#v, want independent header dependency", route)
		}
	}
}

func admittedAgentConstructionContext(t testing.TB, source semanticview.Source, runID string) context.Context {
	t.Helper()
	var observations []runtimepipeline.FlowInstanceObservation
	for _, flow := range []string{semanticview.RootExecutionFlowID(source), "consumer"} {
		instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
		observations = append(observations, constructionIndexObservation(t, source, runID, instance, ""))
	}
	return context.WithValue(constructionIndexContext(t, source), selectedTargetOwnerLookupScopeKey{}, selectedTargetOwnerLookupScope{observations: observations})
}

func TestSameFlowRootAgentOwnerContradictionFailsClosed(t *testing.T) {
	runID := eventtest.UUID("same-flow-agent-contradiction-run")
	identity := agentidentitytest.RootDeclaredForRun(t, runID, "reviewer", "same-flow-agent-contradiction")
	source := connectRoutePlanRootProducerSingletonSource(t)
	activeOwner := eventtest.UUID("root-active-owner")
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents:          map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity, EntityID: activeOwner}},
		descriptors: []ActiveTargetDescriptor{
			{ID: "root-a", FlowInstance: runID, EntityID: eventtest.UUID("root-owner-a")},
			{ID: "root-b", FlowInstance: runID, EntityID: eventtest.UUID("root-owner-b")},
		},
		targetsAvailable: true,
		required:         true,
		source:           source,
	}
	projection.context = admittedAgentConstructionContext(t, source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-contradiction-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
			TargetBlueprint: events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: eventtest.UUID("foreign-agent-owner")},
			Producer:        routeIntentProducerAgentPolicy, Persist: true,
		}},
	}

	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "active agent entity") {
		t.Fatalf("contradictory same-flow agent owner error = %v, want fail-closed identity disagreement", err)
	}
}

func TestPendingAgentLifecycleRequiresInitializationBeyondExactMaterializingOwner(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "materializing-target-proof", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one-future-owner"),
	}
	plan := RoutePlan{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("pending-agent-publication"), "work.ready", "operator", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC()), DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		TargetBlueprint: target, AgentLifecycle: agentLifecycleAdmissionMaterializingFlow, Persist: true,
	}}}.Normalized()
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents:          map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity, EntityID: target.EntityID}},
		descriptors: []ActiveTargetDescriptor{{
			ID: "review/one", FlowInstance: target.FlowInstance, EntityID: target.EntityID, Materializing: true,
		}},
		targetsAvailable: true,
		required:         true,
	}

	owner, err := projection.resolveSelectedRoute(target)
	if err != nil || !owner.MaterializingEntity() || owner.Route() != target.Normalized() {
		t.Fatalf("resolved owner = %#v, err=%v, want exact materializing target %#v", owner, err, target.Normalized())
	}
	// Ownership alone cannot substitute for a compiled activation or node
	// initializer. The real activation's positive case is exercised through
	// EventBus, publication and both stores, not a fabricated descriptor here.
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "independent construction dependency") {
		t.Fatalf("future owner without initialization: %v", err)
	}

	projection.descriptors[0].Materializing = false
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "independent construction dependency") {
		t.Fatalf("existing owner for pending lifecycle error = %v, want construction dependency rejection", err)
	}
}

func TestPendingAgentLifecyclesConsumeIndependentStoredDependency(t *testing.T) {
	identity := agentidentitytest.Declared(t, "reviewer", "static-owner", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one-owner"),
	}
	dependency := events.MustExistingEntityTarget(target)
	target.EntityID = ""
	owner := events.MustEntitylessReceiverTarget(target)
	base := RoutePlanDeliveryIntent{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		TargetBlueprint: target, TargetOwnership: owner, Persist: true,
	}

	event := eventtest.ExistingRunRootIngress(eventtest.UUID("stored-pending-agent-publication"), "work.ready", "operator", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC())
	for _, lifecycle := range []agentLifecycleAdmission{agentLifecycleAdmissionStaticDeclaration, agentLifecycleAdmissionMaterializingFlow} {
		intent := base
		intent.AgentLifecycle = lifecycle
		projection := selectedRunTargetOwnerProjection{required: true, agents: map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity}}}
		if _, err := projection.resolveRoutePlan(RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}}); err == nil || !strings.Contains(err.Error(), "independent construction dependency") {
			t.Fatalf("pending lifecycle=%d escaped dependency admission: %v", lifecycle, err)
		}
		var err error
		intent.Initialization, err = events.AdmitStoredFlowReceiverInitialization(identity.RunID, eventtest.UUID("original-construction"), dependency, identity.Route)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := projection.resolveRoutePlan(RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}}); err != nil {
			t.Fatalf("pending lifecycle=%d refused original stored dependency: %v", lifecycle, err)
		}
	}
}

func TestSpeculativeNodeOwnerCannotAuthorizePendingAgent(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "speculative-owner", "review", "one", "review/one")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("future-node-owner")}
	node := testFlowNode(t, "review", "materializer")
	for _, agentFirst := range []bool{false, true} {
		intents := []RoutePlanDeliveryIntent{
			{Recipient: events.MustNodeDeliveryRecipient(node), TargetBlueprint: target, TargetOwnership: events.MustMaterializingEntityTarget(target), Persist: true},
			{Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity, TargetBlueprint: target, AgentLifecycle: agentLifecycleAdmissionMaterializingFlow, Persist: true},
		}
		if agentFirst {
			intents[0], intents[1] = intents[1], intents[0]
		}
		projection := selectedRunTargetOwnerProjection{agentsAvailable: true, targetsAvailable: true, required: true}
		if _, err := projection.resolveRoutePlan(RoutePlan{DeliveryIntents: intents}); err == nil {
			t.Fatalf("node's speculative future entity authorized agent lifecycle; agentFirst=%t", agentFirst)
		}
	}
}

func TestAdmittedTypedTargetOwnerRemainsAuthoritative(t *testing.T) {
	agentIdentity := agentidentitytest.Runtime(t, "reviewer", "admitted-target-proof", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("admitted-target-owner"),
	}
	owner := events.MustExistingEntityTarget(target)
	plan := RoutePlan{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("admitted-agent-publication"), "work.ready", "operator", "", nil, 0, agentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC()), DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: agentIdentity,
		TargetBlueprint: target, TargetOwnership: owner, Producer: routeIntentProducerDirectPolicy, Persist: true,
	}}}.Normalized()

	resolved, err := (selectedRunTargetOwnerProjection{required: true}).resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("preserve admitted target owner without current descriptor: %v", err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].Target != owner {
		t.Fatalf("resolved routes = %#v, want admitted owner %#v", routes, owner)
	}

	contradictory := plan
	contradictory.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), plan.DeliveryIntents...)
	contradictory.DeliveryIntents[0].TargetBlueprint.EntityID = eventtest.UUID("different-target-owner")
	if _, err := (selectedRunTargetOwnerProjection{required: true}).resolveRoutePlan(contradictory); err == nil || !strings.Contains(err.Error(), "blueprint and typed owner disagree") {
		t.Fatalf("contradictory admitted owner error = %v, want exact disagreement", err)
	}
}

func TestResolvedTargetOwnerCanonicalizesBlueprintForReplay(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "target-replay-proof", "review", "one", "review/one")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one")}
	plan := RoutePlan{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("replayed-agent-publication"), "work.ready", "operator", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC()), DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient:       events.MustAgentDeliveryRecipient("reviewer"),
		AgentIdentity:   identity,
		TargetBlueprint: target,
		Persist:         true,
	}}}.Normalized()
	projection := selectedRunTargetOwnerProjection{
		agents:   map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity, EntityID: target.EntityID}},
		required: true,
	}

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve route plan: %v", err)
	}
	if resolved.DeliveryIntents[0].TargetBlueprint != resolved.DeliveryIntents[0].TargetOwnership.Route() {
		t.Fatalf("resolved blueprint = %#v, owner = %#v", resolved.DeliveryIntents[0].TargetBlueprint, resolved.DeliveryIntents[0].TargetOwnership.Route())
	}
	if _, err := projection.resolveRoutePlan(resolved); err != nil {
		t.Fatalf("replay resolved route plan: %v", err)
	}
}
