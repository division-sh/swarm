package runforkexecution

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func testWorkflowRecipientProjection(t *testing.T, sourceEvents []runfork.RunForkSelectedContractSourceEvent) selectedContractWorkflowProjection {
	t.Helper()
	_, projection, err := projectSelectedContractSourceEvents("source-run", testWorkflowRecipientRoot(t), sourceEvents)
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func testWorkflowRecipientSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"test-node": {
			ExecutionType: "system_node", SubscribesTo: []string{"item.received"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"item.received": {}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"item.received": {}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:  runtimecontracts.WorkflowSemanticView{Name: "selected-workflow", Version: "v1"},
		RootSchema: &root.Schema,
		FlowTree: runtimecontracts.FlowTree{Root: &root,
			ByPath: map[string]*runtimecontracts.FlowContractView{".": &root},
			ByID:   map[string]*runtimecontracts.FlowContractView{".": &root}},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func testWorkflowRecipientRoot(t *testing.T) semanticview.RootExecutionCoordinate {
	t.Helper()
	root, err := semanticview.AdmitRootExecutionCoordinate(testWorkflowRecipientSource(t), "fork-run")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func testRecipientWorkflowSourceEvent() runfork.RunForkSelectedContractSourceEvent {
	return runfork.RunForkSelectedContractSourceEvent{
		SourceEventID: "source-event", EventName: "item.received",
		RoutingSource: eventtest.RootRoutingSource("source-run"),
	}
}

func testRecipientProjectionPlanning(recipients ...forkrecipient.Evidence) runfork.RunForkSelectedContractRecipientPlanning {
	return runfork.RunForkSelectedContractRecipientPlanning{
		Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
		NonMutating: true, RecipientPlanningSupported: true,
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event", EventName: "item.received", Recipients: recipients,
		}},
	}
}

func TestSelectedContractWorkflowRecipientProjectionBindsRootWithoutMutatingSelectedEvidence(t *testing.T) {
	sourceEvent := testRecipientWorkflowSourceEvent()
	projection := testWorkflowRecipientProjection(t, []runfork.RunForkSelectedContractSourceEvent{sourceEvent})
	node := mustRunForkRootNode("test-node")
	input := forkrecipient.Input{
		Recipient: events.MustNodeDeliveryRecipient(node), HandlerNode: node,
		HandlerEvent: "item.received", Path: node.FlowPath(), RouteSource: "subscription",
	}
	local, err := forkrecipient.NewLocal(input)
	if err != nil {
		t.Fatal(err)
	}
	connected, err := forkrecipient.NewConnect(input,
		events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan"))),
		events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte("pin"))))
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []forkrecipient.Evidence{local, connected} {
		before, err := json.Marshal(selected)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint, err := selected.Fingerprint()
		if err != nil {
			t.Fatal(err)
		}
		bound, err := projection.BindRecipient("source-event", selected)
		if err != nil {
			t.Fatal(err)
		}
		want := selected
		want.Path = "fork-run"
		if !reflect.DeepEqual(bound, want) {
			t.Fatalf("binding changed more than the admitted execution path: got %#v want %#v", bound, want)
		}
		if err := bound.Satisfies("fork-run", want, agentidentity.Identity{}); err != nil {
			t.Fatalf("bound exact actual refused: %v", err)
		}
		after, err := json.Marshal(selected)
		if err != nil {
			t.Fatal(err)
		}
		afterFingerprint, err := selected.Fingerprint()
		if err != nil || string(before) != string(after) || fingerprint != afterFingerprint {
			t.Fatalf("persisted selected evidence changed: %v", err)
		}
	}
}

func TestSelectedContractWorkflowRecipientProjectionRequiresExactCorrespondence(t *testing.T) {
	event := testRecipientWorkflowSourceEvent()
	selected := testNodeFrontierRecipient(mustRunForkRootNode("test-node"), "item.received", testWorkflowRecipientRoot(t).FlowID(), "subscription")
	for _, name := range []string{"absent source event", "different source event"} {
		t.Run(name, func(t *testing.T) {
			sourceEvents := []runfork.RunForkSelectedContractSourceEvent{event}
			lookup := "source-event"
			switch name {
			case "absent source event":
				sourceEvents = nil
			case "different source event":
				lookup = "unknown-event"
			}
			projection := testWorkflowRecipientProjection(t, sourceEvents)
			bound, err := projection.BindRecipient(lookup, selected)
			if err != nil || !reflect.DeepEqual(bound, selected) {
				t.Fatalf("absence must not manufacture correspondence: bound=%#v err=%v", bound, err)
			}
			actual := selected
			actual.Path = "fork-run"
			if err := bound.Satisfies("fork-run", actual, agentidentity.Identity{}); err == nil {
				t.Fatal("missing correspondence authorized child path")
			}
		})
	}
	for _, name := range []string{"foreign root entity", "missing event identity", "activity missing routing authority", "duplicate source event"} {
		t.Run(name, func(t *testing.T) {
			e := event
			switch name {
			case "foreign root entity":
				e.RoutingSource = eventtest.RootRoutingSource("other-entity")
			case "missing event identity":
				e.SourceEventID = ""
			case "activity missing routing authority":
				e.EventName = runfork.RunForkSelectedContractPlatformActivityEvent
				e.RoutingSource = events.NoRoutingSource()
			}
			inputs := []runfork.RunForkSelectedContractSourceEvent{e}
			if name == "duplicate source event" {
				inputs = append(inputs, e)
			}
			if _, _, err := projectSelectedContractSourceEvents("source-run", testWorkflowRecipientRoot(t), inputs); err == nil {
				t.Fatal("invalid source event acquired recipient correspondence")
			}
		})
	}
	projection := selectedContractWorkflowProjection{sourceEvents: map[string]struct{}{"source-event": {}}}
	if _, err := projection.BindRecipient("source-event", selected); err == nil {
		t.Fatal("missing selected root owner authorized root binding")
	}
	projection = testWorkflowRecipientProjection(t, []runfork.RunForkSelectedContractSourceEvent{event})
	padded := selected
	padded.Path = " " + selected.Path + " "
	if bound, err := projection.BindRecipient("source-event", padded); err != nil || bound.Path != "fork-run" || padded.Path != " "+selected.Path+" " {
		t.Fatalf("binding must consume canonical evidence normalization without mutating input: %#v, %v", bound, err)
	}
	for _, path := range []string{"", "arbitrary", "source-run", "sibling-run", "fork-run"} {
		other := selected
		other.Path = path
		if _, err := projection.BindRecipient("source-event", other); err == nil {
			t.Fatalf("root binding rewrote unselected path %q", path)
		}
	}
	// Every admitted event ID binds independently; no recipient state is needed.
	otherEvent := event
	otherEvent.SourceEventID = "same-entity-another-event"
	projection = testWorkflowRecipientProjection(t, []runfork.RunForkSelectedContractSourceEvent{event, otherEvent})
	if bound, err := projection.BindRecipient(otherEvent.SourceEventID, selected); err != nil || bound.Path != "fork-run" {
		t.Fatalf("lawful shared-entity correspondence rejected: %#v, %v", bound, err)
	}
}

func TestSelectedContractWorkflowRecipientProjectionKeepsNonRootAndAgentAxesStrict(t *testing.T) {
	projection := testWorkflowRecipientProjection(t, []runfork.RunForkSelectedContractSourceEvent{testRecipientWorkflowSourceEvent()})
	node := testNodeFrontierRecipient(mustRunForkNode("review", "receive"), "item.received", "review/case-one", "selected")
	bound, err := projection.BindRecipient("source-event", node)
	if err != nil || !reflect.DeepEqual(bound, node) {
		t.Fatalf("exact nonroot path changed: %#v, %v", bound, err)
	}
	for _, path := range []string{"review/other", "review", "fork-run"} {
		other := node
		other.Path = path
		unchanged, err := projection.BindRecipient("source-event", other)
		if err != nil || !reflect.DeepEqual(unchanged, other) {
			t.Fatalf("root-only binding rewrote nonroot path %q: %#v, %v", path, unchanged, err)
		}
		if err := bound.Satisfies("fork-run", unchanged, agentidentity.Identity{}); err == nil {
			t.Fatalf("exact recipient relation admitted wrong nonroot path %q", path)
		}
	}
	name, err := agentidentity.DeclaredName("reviewer", "selected/review")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("review", "case-one", "review/case-one")
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []agentidentity.Route{route, agentidentity.RootRoute()} {
		plan, err := agentidentity.NewPlan(name, route)
		if err != nil {
			t.Fatal(err)
		}
		agent := testAgentFrontierRecipient(plan, "item.received", plan.FlowInstance(), "selected")
		bound, err := projection.BindRecipient("source-event", agent)
		if err != nil || !reflect.DeepEqual(bound, agent) {
			t.Fatalf("workflow projection changed full agent plan: %#v, %v", bound, err)
		}
		live, err := plan.Live("fork-run")
		if err != nil {
			t.Fatal(err)
		}
		if err := bound.Satisfies("fork-run", agent, live); err != nil {
			t.Fatal(err)
		}
		for _, run := range []string{"source-run", "sibling-run"} {
			wrong, err := plan.Live(run)
			if err != nil {
				t.Fatal(err)
			}
			if err := bound.Satisfies("fork-run", agent, wrong); err == nil {
				t.Fatalf("agent live identity from %s admitted", run)
			}
		}
		other := agent
		other.AgentPlan.Name.Owner = "different-owner"
		if err := bound.Satisfies("fork-run", other, live); err == nil {
			t.Fatal("workflow projection erased agent owner difference")
		}
	}
}

func TestSelectedContractWorkflowRecipientProjectionGuardUsesBoundCopyAndExactChildRun(t *testing.T) {
	projection := testWorkflowRecipientProjection(t, []runfork.RunForkSelectedContractSourceEvent{testRecipientWorkflowSourceEvent()})
	planning := testRecipientProjectionPlanning(testNodeFrontierRecipient(mustRunForkRootNode("test-node"), "item.received", testWorkflowRecipientRoot(t).FlowID(), "subscription"))
	before, err := json.Marshal(planning)
	if err != nil {
		t.Fatal(err)
	}
	source := testWorkflowRecipientSource(t)
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, source, projection)
	if err != nil {
		t.Fatal(err)
	}
	guard.ExpectForkEvent("fork-event", "source-event")
	evt := selectedContractGuardEvent(t, "fork-event", "item.received", runfork.RunForkSelectedContractExecutionOwner, "source-event")
	_, expected, err := guard.expectedRecipientPlanEvent(evt)
	if err != nil || expected.Recipients[0].Path != "fork-run" {
		t.Fatalf("guard did not consume bound copy: %#v, %v", expected, err)
	}
	routes, err := guard.MaterializeNodeDeliveryRoutes(context.Background(), evt, runtimebus.PublishRecipientPlan{})
	if err != nil || len(routes) != 1 || routes[0].Target.FlowInstance != "fork-run" {
		t.Fatalf("materializer did not consume bound copy: %#v, %v", routes, err)
	}
	after, err := json.Marshal(planning)
	if err != nil || string(before) != string(after) {
		t.Fatalf("guard mutated persisted planning: %v", err)
	}
	for _, run := range []string{"source-run", "sibling-run"} {
		lineage, err := events.NewSelectedForkLineage(run, "historical-run", "source-event", "selected-contract-test", "", executionmode.Live)
		if err != nil {
			t.Fatal(err)
		}
		wrong := eventtest.SelectedForkReplay("fork-event", "item.received",
			eventtest.Producer(events.EventProducerPlatform, runfork.RunForkSelectedContractExecutionOwner),
			"", nil, 0, lineage, events.EventEnvelope{}, time.Time{})
		if err := guard.AuthorizeEvent(context.Background(), wrong); err == nil || !strings.Contains(err.Error(), "exact child run") {
			t.Fatalf("wrong-run event admitted: %v", err)
		}
		if _, err := guard.MaterializeNodeDeliveryRoutes(context.Background(), wrong, runtimebus.PublishRecipientPlan{}); err == nil {
			t.Fatal("wrong-run materialization admitted")
		}
		if err := guard.Authorize(context.Background(), wrong, runtimebus.PublishRecipientPlan{}); err == nil || !strings.Contains(err.Error(), "exact child run") {
			t.Fatalf("wrong-run actual authorization admitted: %v", err)
		}
	}
	if _, err := newSelectedContractRecipientPlanPublishGuard(planning, source, selectedContractWorkflowProjection{}); err == nil {
		t.Fatal("guard accepted unbound child run")
	}
}

func TestSelectedContractWorkflowRecipientProjectionAgentDeclarationPath(t *testing.T) {
	root := runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "not-a-routing-coordinate"},
		FlowTree:  runtimecontracts.FlowTree{Root: &root},
	})
	name, err := agentidentity.DeclaredName("worker", "selected-owner")
	if err != nil {
		t.Fatal(err)
	}
	rootPlan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	path, err := selectedContractAgentRecipientPath(source, rootPlan)
	if err != nil || path != semanticview.RootExecutionFlowID(source) || path == rootPlan.FlowInstance() {
		t.Fatalf("root plan lost its declaration coordinate: %q, %v", path, err)
	}
	padded := rootPlan
	padded.Route.Presence = agentidentity.RoutePresence(" root ")
	if got, err := selectedContractAgentRecipientPath(source, padded); err != nil || got != path {
		t.Fatalf("agent path projection bypassed canonical Plan normalization: %q, %v", got, err)
	}
	for _, absent := range []semanticview.Source{nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})} {
		if _, err := selectedContractAgentRecipientPath(absent, rootPlan); err == nil {
			t.Fatal("root plan gained a declaration path without a selected root owner")
		}
	}
	route, err := agentidentity.PresentRoute("review", "case-one", "review/case-one")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	path, err = selectedContractAgentRecipientPath(source, plan)
	if err != nil || path != route.InstancePath {
		t.Fatalf("selected root changed a present agent route: %q, %v", path, err)
	}
	invalid := rootPlan
	invalid.Route.InstancePath = route.InstancePath
	if _, err := selectedContractAgentRecipientPath(source, invalid); err == nil {
		t.Fatal("contradictory root/present plan admitted")
	}
	projection := testWorkflowRecipientProjection(t, nil)
	selected := testAgentFrontierRecipient(rootPlan, "item.received", semanticview.RootExecutionFlowID(source), "subscription")
	bound, err := projection.BindRecipient("source-event", selected)
	if err != nil || !reflect.DeepEqual(bound, selected) {
		t.Fatalf("root agent's selected declaration was rewritten: %#v, %v", bound, err)
	}
	actualPath, err := selectedContractAgentRecipientPath(source, rootPlan)
	if err != nil {
		t.Fatal(err)
	}
	actual := testAgentFrontierRecipient(rootPlan, "item.received", actualPath, "actual")
	live, err := rootPlan.Live("fork-run")
	if err != nil {
		t.Fatal(err)
	}
	if err := bound.Satisfies("fork-run", actual, live); err != nil {
		t.Fatalf("root declaration actual refused: %v", err)
	}
	for _, run := range []string{"source-run", "sibling-run"} {
		wrong, err := rootPlan.Live(run)
		if err != nil {
			t.Fatal(err)
		}
		if err := bound.Satisfies("fork-run", actual, wrong); err == nil {
			t.Fatalf("root declaration projection admitted %s agent", run)
		}
	}
}
