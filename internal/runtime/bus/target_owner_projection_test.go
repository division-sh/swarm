package bus

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRouteTargetOwnerResolutionMatrix(t *testing.T) {
	rootRunID := eventtest.UUID("selected-root-run")
	rootOwner := eventtest.UUID("selected-root-owner")
	staticOwner := eventtest.UUID("selected-static-owner")
	singletonOwner := eventtest.UUID("selected-singleton-owner")
	templateOwner := eventtest.UUID("selected-template-owner")
	structuralOwner := eventtest.UUID("selected-structural-owner")
	structuralProof := testRootStaticStructuralOwnerProof(t, structuralOwner)
	structuralRoute := structuralProof.TargetOwner().Route()

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
		proof     runtimepinrouting.StructuralTargetOwnerProof
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
			name: "nested static", blueprint: structuralProof.TargetBlueprint(), proof: structuralProof,
			want: structuralRoute,
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
			got, err := projection.resolveSelectedRoute(test.blueprint, test.proof)
			if err != nil {
				t.Fatalf("resolve owner: %v", err)
			}
			if got.Route() != test.want.Normalized() || !got.ExistingEntity() {
				t.Fatalf("owner = %#v, want %#v", got, test.want.Normalized())
			}
		})
	}
}

func testRootStaticStructuralOwnerProof(t testing.TB, entityID string) runtimepinrouting.StructuralTargetOwnerProof {
	t.Helper()
	plans := runtimepinrouting.CompileConnectGraph(connectRoutePlanRootProducerStaticSource(t)).Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled root-to-static plans = %d, want 1", len(plans))
	}
	targets := plans[0].Readback().Targets
	if len(targets) != 1 {
		t.Fatalf("compiled root-to-static targets = %#v, want one", targets)
	}
	current := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "workflow", FlowInstance: eventtest.UUID("structural-proof-run"), EntityID: entityID,
	})
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatalf("root routing source: %v", err)
	}
	sourceEvent, err := runtimepinrouting.AdmitSourceEvent(events.EventType("root.ready"), routingSource)
	if err != nil {
		t.Fatalf("admit root source event: %v", err)
	}
	proof, ok, err := plans[0].ProveStructuralTargetOwner(targets[0], current, sourceEvent)
	if err != nil {
		t.Fatalf("prove root-to-static target owner: %v", err)
	}
	if !ok {
		t.Fatal("compiled root-to-static plan did not issue structural target-owner proof")
	}
	return proof
}

func TestStructuralTargetOwnerProofDuplicateAgreementFailsClosed(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource(t)
	consumerNode := testFlowNode(t, "consumer", "consumer-node")
	handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, consumerNode)
	if err != nil {
		t.Fatalf("admit consumer handler: %v", err)
	}
	proof := testRootStaticStructuralOwnerProof(t, eventtest.UUID("structural-proof-owner-a"))
	intent := RoutePlanDeliveryIntent{
		Recipient: events.MustNodeDeliveryRecipient(consumerNode), TargetBlueprint: proof.TargetBlueprint(),
		Handler: handler.ForEvent("root.ready"), Producer: routeIntentProducerConnectRoutePlan,
		StructuralOwnerProof: proof, Persist: true,
	}
	evt := connectRoutePlanRootProducerEvent(
		eventtest.UUID("structural-proof-event"), events.EventType("root.ready"), "", "", nil, 0,
		eventtest.UUID("structural-proof-run"), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	projection := selectedRunTargetOwnerProjection{source: source, required: true}

	identical := RoutePlan{Event: evt, DeliveryIntents: []RoutePlanDeliveryIntent{intent, intent}}
	resolved, err := projection.resolveRoutePlan(identical)
	if err != nil {
		t.Fatalf("resolve identical proofs: %v", err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].Target != proof.TargetOwner() {
		t.Fatalf("identical proof routes = %#v, want one exact structural owner %#v", routes, proof.TargetOwner())
	}

	absent := intent
	absent.StructuralOwnerProof = runtimepinrouting.StructuralTargetOwnerProof{}
	if _, err := projection.resolveRoutePlan(RoutePlan{Event: evt, DeliveryIntents: []RoutePlanDeliveryIntent{intent, absent}}); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("proof-plus-absent error = %v, want unproved duplicate rejection", err)
	}

	conflicting := intent
	conflicting.StructuralOwnerProof = testRootStaticStructuralOwnerProof(t, eventtest.UUID("structural-proof-owner-b"))
	if _, err := projection.resolveRoutePlan(RoutePlan{Event: evt, DeliveryIntents: []RoutePlanDeliveryIntent{intent, conflicting}}); err == nil || !strings.Contains(err.Error(), "conflicting compiled structural target-owner proofs") {
		t.Fatalf("conflicting proof error = %v, want exact proof disagreement", err)
	}
}

func TestSelectedTargetOwnerPrecedesStructuralFallback(t *testing.T) {
	proof := testRootStaticStructuralOwnerProof(t, eventtest.UUID("structural-proof-selected-owner"))
	selectedEntity := eventtest.UUID("distinct-selected-owner")
	projection := selectedRunTargetOwnerProjection{
		required: true,
		descriptors: []ActiveTargetDescriptor{{
			ID: "selected-owner", FlowInstance: proof.TargetBlueprint().FlowInstance, EntityID: selectedEntity,
		}},
	}
	owner, err := projection.resolveSelectedRoute(proof.TargetBlueprint(), proof)
	if err != nil {
		t.Fatalf("resolve selected owner with structural fallback present: %v", err)
	}
	want := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: proof.TargetBlueprint().FlowID, FlowInstance: proof.TargetBlueprint().FlowInstance, EntityID: selectedEntity,
	})
	if owner != want {
		t.Fatalf("selected owner = %#v, want exact selected evidence %#v", owner, want)
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
			if _, err := test.projection.resolveSelectedRoute(plan.DeliveryIntents[0].TargetBlueprint, runtimepinrouting.StructuralTargetOwnerProof{}); err == nil || !strings.Contains(err.Error(), test.want) {
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

func TestExplicitAgentTargetRequiresSelectedEntityOwner(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "entityless-target-proof", "review", "one", "review/one")
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
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

	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("missing selected agent target error = %v, want target owner rejection", err)
	}

	selectedOwner := eventtest.UUID("review-one-selected-owner")
	projection.descriptors = []ActiveTargetDescriptor{{ID: "review-one", FlowInstance: "review/one", EntityID: selectedOwner}}
	projection.targetsAvailable = true
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
}

func TestSameFlowAgentPolicyDerivesExactSelectedRunOwners(t *testing.T) {
	rootIdentity := agentidentitytest.RootDeclared(t, "root-reviewer", "same-flow-agent-root")
	nestedIdentity := agentidentitytest.Declared(t, "nested-reviewer", "same-flow-agent-nested", "review", "one", "review/one")
	runID := eventtest.UUID("same-flow-agent-run")
	rootOwner := eventtest.UUID("same-flow-agent-root-owner")
	nestedOwner := eventtest.UUID("same-flow-agent-nested-owner")
	source := connectRoutePlanRootProducerSingletonSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.FlowTree.Root == nil {
		t.Fatal("declared-agent source requires a root flow")
	}
	bundle.FlowTree.Root.Schema.Name = "authored-root"
	if source.WorkflowName() == semanticview.RootExecutionFlowID(source) {
		t.Fatalf("test requires authored root identity to differ from display name %q", source.WorkflowName())
	}
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity, EntityID: rootOwner},
			nestedIdentity: {Identity: nestedIdentity, EntityID: nestedOwner},
		},
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: runID, EntityID: rootOwner},
			{ID: "review-one", FlowInstance: "review/one", EntityID: nestedOwner},
		},
		targetsAvailable: true,
		required:         true,
		source:           source,
	}
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
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
		nestedIdentity: {FlowID: "review", FlowInstance: "review/one", EntityID: nestedOwner},
	}
	for _, route := range routes {
		if !route.Target.ExistingEntity() || route.Target.Route() != want[route.AgentIdentity].Normalized() {
			t.Fatalf("route = %#v, want exact selected owner %#v", route, want[route.AgentIdentity].Normalized())
		}
	}
}

func TestSameFlowRuntimeCreatedAgentDoesNotConsumeSelectedOwner(t *testing.T) {
	identity := agentidentitytest.RootRuntime(t, "workflow-runtime", "same-flow-runtime-carrier")
	runID := eventtest.UUID("same-flow-runtime-carrier-run")
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
		Event: eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
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

func TestSameFlowDeclaredEntitylessAgentsDoNotInheritSelectedNodeOwners(t *testing.T) {
	rootIdentity := agentidentitytest.RootDeclared(t, "root-reviewer", "same-flow-entityless-root")
	nestedIdentity := agentidentitytest.Declared(t, "nested-reviewer", "same-flow-entityless-nested", "review", "one", "review/one")
	runID := eventtest.UUID("same-flow-entityless-run")
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity},
			nestedIdentity: {Identity: nestedIdentity},
		},
		descriptors: []ActiveTargetDescriptor{
			{ID: "root", FlowInstance: runID, EntityID: eventtest.UUID("same-flow-root-node-owner")},
			{ID: "review-one", FlowInstance: "review/one", EntityID: eventtest.UUID("same-flow-nested-node-owner")},
		},
		targetsAvailable: true,
		required:         true,
		source:           connectRoutePlanRootProducerSingletonSource(t),
	}
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
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
		if !route.Target.Empty() {
			t.Fatalf("entityless declared agent route = %#v, want explicit target absence", route)
		}
	}
}

func TestSameFlowRootAgentOwnerContradictionFailsClosed(t *testing.T) {
	identity := agentidentitytest.RootDeclared(t, "reviewer", "same-flow-agent-contradiction")
	runID := eventtest.UUID("same-flow-agent-contradiction-run")
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
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
			Producer: routeIntentProducerAgentPolicy, Persist: true,
		}},
	}

	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "active agent entity") {
		t.Fatalf("contradictory same-flow agent owner error = %v, want fail-closed identity disagreement", err)
	}
}

func TestPendingAgentLifecycleConsumesExactMaterializingOwner(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "reviewer", "materializing-target-proof", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one-future-owner"),
	}
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		TargetBlueprint: target, AgentLifecycle: agentLifecycleAdmissionMaterializingFlow, Persist: true,
	}}}.Normalized()
	projection := selectedRunTargetOwnerProjection{
		agentsAvailable: true,
		descriptors: []ActiveTargetDescriptor{{
			ID: "review/one", FlowInstance: target.FlowInstance, EntityID: target.EntityID, Materializing: true,
		}},
		targetsAvailable: true,
		required:         true,
	}

	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("resolve pending materializing agent target: %v", err)
	}
	routes := resolved.DeliveryRoutes()
	if len(routes) != 1 || !routes[0].Target.MaterializingEntity() || routes[0].Target.Route() != target.Normalized() {
		t.Fatalf("resolved routes = %#v, want exact materializing agent target %#v", routes, target.Normalized())
	}

	projection.descriptors[0].Materializing = false
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "requires materializing_entity ownership") {
		t.Fatalf("existing owner for pending lifecycle error = %v, want materializing ownership rejection", err)
	}
}

func TestStaticAgentLifecycleConsumesExistingOwnerButConnectLifecycleDoesNot(t *testing.T) {
	identity := agentidentitytest.Declared(t, "reviewer", "static-owner", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one-owner"),
	}
	owner := events.MustExistingEntityTarget(target)
	base := RoutePlanDeliveryIntent{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		TargetBlueprint: target, TargetOwnership: owner, Persist: true,
	}

	static := base
	static.AgentLifecycle = agentLifecycleAdmissionStaticDeclaration
	if _, err := (selectedRunTargetOwnerProjection{required: true}).resolveRoutePlan(RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{static}}); err != nil {
		t.Fatalf("resolve static lifecycle against existing owner: %v", err)
	}

	connect := base
	connect.AgentLifecycle = agentLifecycleAdmissionMaterializingFlow
	if _, err := (selectedRunTargetOwnerProjection{required: true}).resolveRoutePlan(RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{connect}}); err == nil || !strings.Contains(err.Error(), "requires materializing_entity ownership") {
		t.Fatalf("connect lifecycle existing-owner error = %v, want materializing owner rejection", err)
	}
}

func TestAdmittedTypedTargetOwnerRemainsAuthoritative(t *testing.T) {
	agentIdentity := agentidentitytest.Runtime(t, "reviewer", "admitted-target-proof", "review", "one", "review/one")
	target := events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("admitted-target-owner"),
	}
	owner := events.MustExistingEntityTarget(target)
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
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
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient:       events.MustAgentDeliveryRecipient("reviewer"),
		AgentIdentity:   identity,
		TargetBlueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
		Persist:         true,
	}}}.Normalized()
	projection := selectedRunTargetOwnerProjection{
		descriptors: []ActiveTargetDescriptor{{
			ID: "reviewer", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one"),
		}},
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
