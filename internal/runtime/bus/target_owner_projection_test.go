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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRouteTargetOwnerResolutionMatrix(t *testing.T) {
	rootRunID := eventtest.UUID("selected-root-run")
	rootOwner := eventtest.UUID("selected-root-owner")
	staticOwner := eventtest.UUID("selected-static-owner")
	singletonOwner := eventtest.UUID("selected-singleton-owner")
	templateOwner := eventtest.UUID("selected-template-owner")

	projection := publicationTargetOwnerEvidence{
		required: true,
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
			{
				Route: events.RouteIdentity{FlowInstance: rootRunID,
					EntityID: rootOwner},
			},
			{
				Route: events.RouteIdentity{FlowInstance: "review",
					EntityID: staticOwner},
			},
			{
				Route: events.RouteIdentity{FlowInstance: "portfolio",
					EntityID: singletonOwner},
			},
			{
				Route: events.RouteIdentity{FlowInstance: "operating/instance-a",
					EntityID: templateOwner},
			},
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
			got, err := projection.replyOriginOwner(test.blueprint)
			if err != nil {
				t.Fatalf("resolve owner: %v", err)
			}
			if got.Route() != test.want.Normalized() || !got.ExistingEntity() {
				t.Fatalf("owner = %#v, want %#v", got, test.want.Normalized())
			}
		})
	}
	t.Run("explicit reply entity narrows same-instance candidates", func(t *testing.T) {
		candidate := projection
		candidate.candidates = append([]runtimepipeline.DeliveryTargetOwnerCandidate(nil), projection.candidates...)
		candidate.candidates = append(candidate.candidates, runtimepipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{FlowInstance: "review",
				EntityID: eventtest.UUID("foreign-reply-entity")},
		})
		origin := events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: staticOwner}
		owner, err := candidate.replyOriginOwner(origin)
		if err != nil || !owner.ExistingEntity() || owner.Route() != origin {
			t.Fatalf("explicit reply origin lost its entity narrowing: owner=%+v err=%v", owner, err)
		}
	})
	for _, required := range []bool{false, true} {
		t.Run(fmt.Sprintf("observed absence required=%t", required), func(t *testing.T) {
			origin := events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: staticOwner}
			owner, err := (publicationTargetOwnerEvidence{required: required}).replyOriginOwner(origin)
			if required {
				if err == nil || !owner.Empty() {
					t.Fatalf("required origin manufactured an absent owner: owner=%+v err=%v", owner, err)
				}
			} else if err != nil || !owner.ExistingEntity() || owner.Route() != origin {
				t.Fatalf("ephemeral observed-absence behavior changed: owner=%+v err=%v", owner, err)
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
	active := runtimepipeline.DeliveryTargetOwnerCandidate{
		Route: events.RouteIdentity{FlowInstance: blueprint.FlowInstance,
			EntityID: eventtest.UUID("exact-owner")},
	}
	unavailable := active
	unavailable.Availability = runtimepipeline.NewDeliveryTargetAvailability("active", "draining", false)
	for _, descriptors := range [][]runtimepipeline.DeliveryTargetOwnerCandidate{{active, unavailable}, {unavailable, active}} {
		projection := publicationTargetOwnerEvidence{required: true, source: source}
		ordered := newOrderedTargetOwnerCandidates(projection.candidates)
		for _, descriptor := range descriptors {
			ordered.add(descriptor)
		}
		projection.candidates = ordered.candidates
		if len(projection.candidates) != 2 {
			t.Fatalf("dedup erased contradictory availability: %#v", projection.candidates)
		}
		if _, err := projection.replyOriginOwner(blueprint); err == nil || !strings.Contains(err.Error(), "owner is unavailable") {
			t.Fatalf("agent/exact-route projection admitted unavailable receiver: %v", err)
		}
	}
}

func TestSelectedTargetOwnerNeverUsesParentDescriptor(t *testing.T) {
	blueprint := events.RouteIdentity{FlowID: "parent/child", FlowInstance: "parent/child"}
	parent := runtimepipeline.DeliveryTargetOwnerCandidate{
		Route: events.RouteIdentity{FlowInstance: "parent",
			EntityID: eventtest.UUID("parent-owner")},
	}
	projection := publicationTargetOwnerEvidence{required: true, candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{parent}}
	if _, err := projection.replyOriginOwner(blueprint); err == nil {
		t.Fatal("parent descriptor became receiver-owned state")
	}
	child := runtimepipeline.DeliveryTargetOwnerCandidate{
		Route: events.RouteIdentity{FlowInstance: blueprint.FlowInstance,
			EntityID: eventtest.UUID("child-owner")},
	}
	projection.candidates = append(projection.candidates, child)
	got, err := projection.replyOriginOwner(blueprint)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ExistingEntity() || got.Route().EntityID != child.Route.EntityID {
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
		projection publicationTargetOwnerEvidence
		want       string
	}{
		{
			name: "missing", projection: publicationTargetOwnerEvidence{required: true},
			want: "owner is missing",
		},
		{
			name: "ambiguous", projection: publicationTargetOwnerEvidence{required: true, candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner-a")},
				},
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner-b")},
				},
			}}, want: "owner is ambiguous",
		},
		{
			name: "foreign", projection: publicationTargetOwnerEvidence{required: true, candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "other",
						EntityID: eventtest.UUID("other-owner")},
				},
			}}, want: "owner is missing",
		},
		{
			name: "disagreeing", projection: publicationTargetOwnerEvidence{required: true, candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner")},
				},
			}}, want: "owner is missing",
		},
		{
			name: "ephemeral ambiguous", projection: publicationTargetOwnerEvidence{candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner-a")},
				},
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner-b")},
				},
			}}, want: "owner is ambiguous",
		},
		{
			name: "ephemeral unavailable", projection: publicationTargetOwnerEvidence{candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner")},
					Availability: runtimepipeline.NewDeliveryTargetAvailability("", "draining", false),
				},
			}}, want: "owner is unavailable",
		},
		{
			name: "ephemeral contradictory", projection: publicationTargetOwnerEvidence{candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner")},
				},
				{
					Route: events.RouteIdentity{FlowInstance: "portfolio",
						EntityID: eventtest.UUID("portfolio-owner")},
					Materializing: true,
				},
			}}, want: "contradictory existing and materializing",
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
			if _, err := test.projection.replyOriginOwner(plan.DeliveryIntents[0].TargetBlueprint); err == nil || !strings.Contains(err.Error(), test.want) {
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
		descriptor runtimepipeline.DeliveryTargetOwnerCandidate
	}{
		{name: "selected target descriptor", descriptor: runtimepipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{FlowInstance: "portfolio"},
		}},
		{name: "keyed target descriptor", descriptor: runtimepipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{FlowID: "operating",
				FlowInstance: "operating/one"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := publicationTargetOwnerEvidence{required: true, candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{test.descriptor}}
			if err := projection.validatePlanningShape(); err == nil || !strings.Contains(err.Error(), "missing exact entity identity") {
				t.Fatalf("validate error = %v, want missing exact entity identity", err)
			}
		})
	}
}

func TestExplicitAgentTargetConsumesExactTargetOwner(t *testing.T) {
	agentOwner := eventtest.UUID("explicit-agent-owner")
	agentIdentity := agentidentitytest.Runtime(t, "reviewer", "target-owner-proof", "review", "one", "review/one")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: agentOwner}
	projection := publicationTargetOwnerEvidence{
		required: true,
		agents:   map[agentidentity.Identity]ActiveAgentDescriptor{agentIdentity: {Identity: agentIdentity, EntityID: agentOwner}},
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("explicit-agent-publication"), "work.ready", "operator", "", nil, 0, agentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	plan := RoutePlan{Event: event, DeliveryIntents: []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: agentIdentity,
		TargetBlueprint: target, Producer: routeIntentProducerOrdinary, Persist: true,
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
		Producer: routeIntentProducerOrdinary, Persist: true,
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
	projection := publicationTargetOwnerEvidence{
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
	projection.candidates = []runtimepipeline.DeliveryTargetOwnerCandidate{{
		Route: events.RouteIdentity{FlowInstance: "review/one",
			EntityID: selectedOwner},
	}}
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
	projection := publicationTargetOwnerEvidence{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity, EntityID: rootOwner},
			nestedIdentity: {Identity: nestedIdentity, EntityID: nestedOwner},
		},
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
			{
				Route: events.RouteIdentity{FlowInstance: runID,
					EntityID: rootOwner},
			},
			{
				Route: events.RouteIdentity{FlowInstance: "consumer",
					EntityID: nestedOwner},
			},
		},
		targetsAvailable: true,
		required:         true,
		source:           source,
	}
	projection.context, projection.instances = admittedAgentConstructionFixture(t, source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-agent-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{
			{Recipient: events.MustAgentDeliveryRecipient("root-reviewer"), AgentIdentity: rootIdentity, Producer: routeIntentProducerOrdinary, Persist: true},
			{Recipient: events.MustAgentDeliveryRecipient("nested-reviewer"), AgentIdentity: nestedIdentity, Producer: routeIntentProducerOrdinary, Persist: true},
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
		if route.Initialization.Empty() || events.SameDeliveryTargetOwnership(route.Target, route.Initialization.ConstructionTarget()) {
			t.Fatalf("independent actor lost its distinct construction dependency: %#v", route)
		}
	}
	for _, prospective := range []bool{false, true} {
		for _, actor := range []agentidentity.Identity{rootIdentity, nestedIdentity} {
			t.Run(fmt.Sprintf("exact-construction/%s/prospective=%t", actor.AgentID(), prospective), func(t *testing.T) {
				flow := "consumer"
				if actor.Route.Presence == agentidentity.RouteRoot {
					flow = semanticview.RootExecutionFlowID(source)
				}
				instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
				candidate := projection
				candidate.agents = map[agentidentity.Identity]ActiveAgentDescriptor{actor: {Identity: actor, EntityID: instance.EntityID}}
				if prospective {
					owner := newTestFlowInstanceActivationOwner(nil)
					activation, err := owner.PrepareFlowInstanceActivation(constructionIndexContext(t, source), runtimepipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance, TriggerEvent: plan.Event, OccurredAt: plan.Event.CreatedAt()})
					if err != nil {
						t.Fatal(err)
					}
					candidate, err = candidate.withActivationPlans([]runtimepipeline.FlowInstanceActivationPlan{activation})
					if err != nil {
						t.Fatal(err)
					}
				}
				dependency, err := candidate.agentConstructionDependency(plan.Event, actor, events.RouteIdentity{})
				if err != nil {
					t.Fatal(err)
				}
				intent := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor, Producer: routeIntentProducerOrdinary, Persist: true}
				resolved, err := candidate.resolveRoutePlan(RoutePlan{Event: plan.Event, DeliveryIntents: []RoutePlanDeliveryIntent{intent}})
				if err != nil {
					t.Fatal(err)
				}
				if got := resolved.DeliveryRoutes(); len(got) != 1 || !events.SameDeliveryTargetOwnership(got[0].Target, dependency.ConstructionTarget()) || !got[0].Initialization.Equal(dependency) || got[0].Target.MaterializingEntity() != prospective {
					t.Fatalf("exact actor target lost construction receipt or required kind: %#v", got)
				}
			})
		}
	}
}

func TestSameFlowRuntimeCreatedAgentDoesNotConsumeSelectedOwner(t *testing.T) {
	runID := eventtest.UUID("same-flow-runtime-carrier-run")
	identity := agentidentitytest.RootRuntimeForRun(t, runID, "workflow-runtime", "same-flow-runtime-carrier")
	ownerID := eventtest.UUID("same-flow-runtime-carrier-owner")
	projection := publicationTargetOwnerEvidence{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			identity: {Identity: identity},
		},
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{{
			Route: events.RouteIdentity{FlowInstance: runID,
				EntityID: ownerID},
			Materializing: true,
		}},
		targetsAvailable: true,
		required:         true,
		source:           connectRoutePlanRootProducerSingletonSource(t),
	}
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-runtime-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Recipient: events.MustAgentDeliveryRecipient("workflow-runtime"), AgentIdentity: identity,
			Producer: routeIntentProducerOrdinary, Persist: true,
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
				projection := publicationTargetOwnerEvidence{required: true, source: source, agentsAvailable: true,
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
					projection.context, projection.instances = admittedAgentConstructionFixture(t, source, runID)
				}
				intent := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity, Producer: routeIntentProducerOrdinary, Persist: true}
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
	projection := publicationTargetOwnerEvidence{
		agentsAvailable: true,
		agents: map[agentidentity.Identity]ActiveAgentDescriptor{
			rootIdentity:   {Identity: rootIdentity},
			nestedIdentity: {Identity: nestedIdentity},
		},
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
			{
				Route: events.RouteIdentity{FlowInstance: runID,
					EntityID: eventtest.UUID("same-flow-root-node-owner")},
			},
			{
				Route: events.RouteIdentity{FlowInstance: "consumer",
					EntityID: eventtest.UUID("same-flow-nested-node-owner")},
			},
		},
		targetsAvailable: true,
		required:         true,
		source:           connectRoutePlanRootProducerSingletonSource(t),
	}
	projection.context, projection.instances = admittedAgentConstructionFixture(t, projection.source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-entityless-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{
			{Recipient: events.MustAgentDeliveryRecipient("root-reviewer"), AgentIdentity: rootIdentity, Producer: routeIntentProducerOrdinary, Persist: true},
			{Recipient: events.MustAgentDeliveryRecipient("nested-reviewer"), AgentIdentity: nestedIdentity, Producer: routeIntentProducerOrdinary, Persist: true},
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

func admittedAgentConstructionFixture(t testing.TB, source semanticview.Source, runID string) (context.Context, runtimepipeline.FlowInstanceIndexReader) {
	t.Helper()
	var observations []runtimepipeline.FlowInstanceObservation
	for _, flow := range []string{semanticview.RootExecutionFlowID(source), "consumer"} {
		instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
		observations = append(observations, constructionIndexObservation(t, source, runID, instance, ""))
	}
	ctx := constructionIndexContext(t, source)
	fact, _ := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	scope, err := runtimepipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{semanticview.RootExecutionFlowID(source), "consumer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return withSelectedTargetOwnerLookupScope(ctx, selectedTargetOwnerLookupScope{lookup: scope, observations: observations}), constructionIndexTestReader{observations: observations}
}

func TestSameFlowRootAgentOwnerContradictionFailsClosed(t *testing.T) {
	runID := eventtest.UUID("same-flow-agent-contradiction-run")
	identity := agentidentitytest.RootDeclaredForRun(t, runID, "reviewer", "same-flow-agent-contradiction")
	source := connectRoutePlanRootProducerSingletonSource(t)
	activeOwner := eventtest.UUID("root-active-owner")
	projection := publicationTargetOwnerEvidence{
		agentsAvailable: true,
		agents:          map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity, EntityID: activeOwner}},
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{
			{
				Route: events.RouteIdentity{FlowInstance: runID,
					EntityID: eventtest.UUID("root-owner-a")},
			},
			{
				Route: events.RouteIdentity{FlowInstance: runID,
					EntityID: eventtest.UUID("root-owner-b")},
			},
		},
		targetsAvailable: true,
		required:         true,
		source:           source,
	}
	projection.context, projection.instances = admittedAgentConstructionFixture(t, source, runID)
	plan := RoutePlan{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("same-flow-contradiction-publication"), "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}),
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Recipient: events.MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
			TargetBlueprint: events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: eventtest.UUID("foreign-agent-owner")},
			Producer:        routeIntentProducerOrdinary, Persist: true,
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
	projection := publicationTargetOwnerEvidence{
		agentsAvailable: true,
		agents:          map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity, EntityID: target.EntityID}},
		candidates: []runtimepipeline.DeliveryTargetOwnerCandidate{{
			Route: events.RouteIdentity{FlowInstance: target.FlowInstance,
				EntityID: target.EntityID},
			Materializing: true,
		}},
		targetsAvailable: true,
		required:         true,
	}

	owner, found, err := runtimepipeline.SelectDeliveryTargetOwner(projection.source, target, projection.candidates)
	if err != nil || !found || !owner.MaterializingEntity() || owner.Route() != target.Normalized() {
		t.Fatalf("resolved owner = %#v, err=%v, want exact materializing target %#v", owner, err, target.Normalized())
	}
	// Ownership alone cannot substitute for a compiled activation or node
	// initializer. The real activation's positive case is exercised through
	// EventBus, publication and both stores, not a fabricated descriptor here.
	if _, err := projection.resolveRoutePlan(plan); err == nil || !strings.Contains(err.Error(), "independent construction dependency") {
		t.Fatalf("future owner without initialization: %v", err)
	}

	projection.candidates[0].Materializing = false
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
		projection := publicationTargetOwnerEvidence{required: true, agents: map[agentidentity.Identity]ActiveAgentDescriptor{identity: {Identity: identity}}}
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
		projection := publicationTargetOwnerEvidence{agentsAvailable: true, targetsAvailable: true, required: true}
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
		TargetBlueprint: target, TargetOwnership: owner, Producer: routeIntentProducerOrdinary, Persist: true,
	}}}.Normalized()

	resolved, err := (publicationTargetOwnerEvidence{required: true}).resolveRoutePlan(plan)
	if err != nil {
		t.Fatalf("preserve admitted target owner without current descriptor: %v", err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].Target != owner {
		t.Fatalf("resolved routes = %#v, want admitted owner %#v", routes, owner)
	}

	contradictory := plan
	contradictory.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), plan.DeliveryIntents...)
	contradictory.DeliveryIntents[0].TargetBlueprint.EntityID = eventtest.UUID("different-target-owner")
	if _, err := (publicationTargetOwnerEvidence{required: true}).resolveRoutePlan(contradictory); err == nil || !strings.Contains(err.Error(), "blueprint and typed owner disagree") {
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
	projection := publicationTargetOwnerEvidence{
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
