package runforkadmission

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRecipientAuthorityR06HistoricalClaimCannotOverrideChangedSelectedSource(t *testing.T) {
	oldSource := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: "consumer", node: "old-node", pins: []string{"old.ready"}}})
	selected := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: "consumer", node: "new-node", pins: []string{"new.ready"}}})
	oldPlan := recipientAuthorityConnectPlan(t, oldSource, "consumer", "old.ready")
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"historical_field": "historical_value"})
	if err != nil {
		t.Fatal(err)
	}
	oldNode := identitytest.FlowNode(t, "consumer", "old-node")
	blueprint := pinrouting.ConnectDeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(oldNode),
		Target:    oldPlan.ReceiverRoute("consumer", "historical-consumer-entity"), Handler: pinrouting.MustConnectReceiverHandler(oldNode),
		Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "historical-reply"}}, PayloadProjection: projection,
	}
	claim, err := pinrouting.ConnectExecutionClaim(oldPlan, blueprint)
	if err != nil {
		t.Fatal(err)
	}
	historical := events.DeliveryRoute{
		Recipient: blueprint.Recipient, Target: events.MustExistingEntityTarget(blueprint.Target),
		Context: blueprint.Context, PayloadProjection: projection, ConnectClaim: claim,
	}
	secondBlueprint := blueprint
	secondBlueprint.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "second-historical-reply"}}
	secondClaim, err := pinrouting.ConnectExecutionClaim(oldPlan, secondBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	secondHistorical := historical
	secondHistorical.Context, secondHistorical.ConnectClaim = secondBlueprint.Context, secondClaim
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationDeliveredCompleted} {
		t.Run(classification, func(t *testing.T) {
			plan := testRunForkPlan("producer/scan.requested", classification, "node", oldNode.Key())
			plan.PendingWork[0].DeliveryRoute = historical
			// Preserve both historical delivery facts, including their order and metadata.
			second := plan.PendingWork[0]
			second.DeliveryID = "second-historical-delivery"
			second.DeliveryRoute = secondHistorical
			plan.PendingWork = append(plan.PendingWork, second)
			plan.PendingWorkCount = len(plan.PendingWork)
			before := recipientAuthorityJSON(t, plan)
			oldRecipients, oldHistory := recipientAuthorityAdmit(t, plan, oldSource, classification)
			if len(oldRecipients) != 1 || oldRecipients[0].Recipient.ID() != oldNode.Key() || oldRecipients[0].HandlerEvent() != "old.ready" {
				t.Fatalf("unchanged selected source positive control: %#v", oldRecipients)
			}
			got, history := recipientAuthorityAdmit(t, plan, selected, classification)
			if len(got) != 1 || got[0].Recipient.ID() != identitytest.FlowNode(t, "consumer", "new-node").Key() || got[0].HandlerEvent() != "new.ready" {
				t.Fatalf("historical claim overrode selected declaration: %#v", got)
			}
			recipientAuthorityAssertConnect(t, got[0], recipientAuthorityConnectPlan(t, selected, "consumer", "new.ready"))
			wantHistory := []events.DeliveryRoute{historical, secondHistorical}
			if !reflect.DeepEqual(history, wantHistory) || !reflect.DeepEqual(oldHistory, wantHistory) {
				t.Fatalf("complete H changed: old=%#v selected=%#v want=%#v", oldHistory, history, wantHistory)
			}
			if after := recipientAuthorityJSON(t, plan); after != before {
				t.Fatalf("admission mutated source plan:\nbefore=%s\nafter=%s", before, after)
			}
		})
	}
}

func TestRecipientAuthorityR08SameAgentIDRetainsDistinctFullScopedPlans(t *testing.T) {
	source := recipientAuthoritySource(t, []recipientAuthorityReceiver{
		{path: "first", agent: true, pins: []string{"work.ready"}},
		{path: "second", agent: true, pins: []string{"work.ready"}},
	})
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationDeliveredCompleted} {
		t.Run(classification, func(t *testing.T) {
			plan := testRunForkPlan("producer/scan.requested", classification, "node", "source-node")
			got, _ := recipientAuthorityAdmit(t, plan, source, classification)
			if len(got) != 2 {
				t.Fatalf("same-ID scoped agents flattened: %#v", got)
			}
			seen := map[string]bool{}
			for _, recipient := range got {
				if recipient.Path != "first" && recipient.Path != "second" {
					t.Fatalf("unexpected agent path: %#v", recipient)
				}
				if seen[recipient.Path] {
					t.Fatalf("duplicate scoped recipient: %#v", got)
				}
				seen[recipient.Path] = true
				want := recipientAuthorityAgentPlan(t, recipient.Path)
				if !recipient.Recipient.IsAgent() || recipient.Recipient.ID() != "worker" || recipient.AgentPlan != want || !recipient.HandlerNode().Empty() || recipient.HandlerEvent() != "work.ready" {
					t.Fatalf("full declared agent identity lost: got=%#v want=%#v", recipient, want)
				}
				recipientAuthorityAssertConnect(t, recipient, recipientAuthorityConnectPlan(t, source, recipient.Path, "work.ready"))
			}
		})
	}
}

func TestRecipientAuthorityR09SameNodeRetainsTwoSelectedHandlerPlanAssociations(t *testing.T) {
	source := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: "consumer", node: "worker", pins: []string{"first.ready", "second.ready"}}})
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationDeliveredCompleted} {
		t.Run(classification, func(t *testing.T) {
			plan := testRunForkPlan("producer/scan.requested", classification, "node", "source-node")
			got, _ := recipientAuthorityAdmit(t, plan, source, classification)
			if len(got) != 2 {
				t.Fatalf("handler-local associations flattened by recipient: %#v", got)
			}
			seen := map[events.EventType]bool{}
			for _, recipient := range got {
				node := identitytest.FlowNode(t, "consumer", "worker")
				local := recipient.HandlerEvent()
				if (local != "first.ready" && local != "second.ready") || seen[local] || recipient.Recipient != events.MustNodeDeliveryRecipient(node) || recipient.HandlerNode() != node || recipient.Path != "consumer" || !recipient.AgentPlan.IsZero() {
					t.Fatalf("selected handler association changed: %#v", recipient)
				}
				seen[local] = true
				recipientAuthorityAssertConnect(t, recipient, recipientAuthorityConnectPlan(t, source, "consumer", string(local)))
			}
			firstPlan, firstPin, _ := got[0].Connect()
			secondPlan, secondPin, _ := got[1].Connect()
			if firstPlan == secondPlan || firstPin == secondPin {
				t.Fatalf("distinct declared plans/pins collapsed: %#v", got)
			}
		})
	}
}

func TestRecipientAuthorityR28SelectedAgentPlanIsRunlessHistoricalIdentityIsNot(t *testing.T) {
	source := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: "consumer", agent: true, pins: []string{"work.ready"}}})
	wantPlan := recipientAuthorityAgentPlan(t, "consumer")
	var first []forkrecipient.Evidence
	for _, runID := range []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"} {
		t.Run(runID, func(t *testing.T) {
			live, err := wantPlan.Live(runID)
			if err != nil {
				t.Fatal(err)
			}
			plan := testRunForkPlan("producer/scan.requested", runfork.RunForkPendingClassificationPending, "agent", "worker")
			plan.SourceRunID = runID
			plan.PendingWork[0].DeliveryRoute = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: live}
			before := recipientAuthorityJSON(t, plan)
			got, history := recipientAuthorityAdmit(t, plan, source, runfork.RunForkPendingClassificationPending)
			if len(got) != 1 || got[0].AgentPlan != wantPlan {
				t.Fatalf("selected plan lost runless declaration: %#v", got)
			}
			if len(history) != 1 || history[0].AgentIdentity != live || recipientAuthorityJSON(t, plan) != before {
				t.Fatalf("historical live identity changed: %#v", history)
			}
			if first == nil {
				first = got
			} else if !reflect.DeepEqual(first, got) {
				t.Fatalf("source run leaked into selected authority: first=%#v second=%#v", first, got)
			}
		})
	}
}

func TestRecipientAuthorityStaticRootOnlyPreservesConnectedRolesWithoutDynamicBlocker(t *testing.T) {
	source := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: ".", node: "root-node", agent: true, pins: []string{"root.ready"}}})
	selectedPlan := recipientAuthorityConnectPlan(t, source, ".", "root.ready")
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationDeliveredCompleted} {
		t.Run(classification, func(t *testing.T) {
			plan := testRunForkPlan("producer/scan.requested", classification, "node", "source-node")
			frontier, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source})
			if err != nil {
				t.Fatal(err)
			}
			if hasBlocker(frontier.UnsupportedBlockers, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
				t.Fatalf("static root incorrectly requires runtime resolution: %#v", frontier.UnsupportedBlockers)
			}
			got, _ := recipientAuthorityAdmit(t, plan, source, classification)
			if len(got) != 2 {
				t.Fatalf("root-only selected roles = %#v, want root node and root agent", got)
			}
			seen := map[string]bool{}
			for _, recipient := range got {
				if recipient.Path != "." || recipient.HandlerEvent() != "root.ready" {
					t.Fatalf("root selected coordinate/local event lost: %#v", recipient)
				}
				recipientAuthorityAssertConnect(t, recipient, selectedPlan)
				if recipient.Recipient.IsNode() {
					if recipient.HandlerNode() != identitytest.RootNode(t, "root-node") {
						t.Fatalf("wrong root handler: %#v", recipient)
					}
				} else if recipient.Recipient.ID() != "worker" || recipient.AgentPlan.Route != agentidentity.RootRoute() {
					t.Fatalf("wrong root agent: %#v", recipient)
				}
				seen[recipient.Recipient.Code()] = true
			}
			if len(seen) != 2 {
				t.Fatalf("root node/agent roles flattened: %#v", got)
			}
			if classification == runfork.RunForkPendingClassificationDeliveredCompleted {
				history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
				if err != nil {
					t.Fatal(err)
				}
				if hasBlocker(history.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) || history.SelectedRouteEvents[0].Disposition != runfork.RunForkSelectedContractDispositionEvidenceOnly {
					t.Fatalf("static root history incorrectly incomplete: %#v", history)
				}
			}
			plan.PendingWork[0].RoutingSource = events.NoRoutingSource()
			withoutSource, _ := recipientAuthorityAdmit(t, plan, source, classification)
			if len(withoutSource) != 0 {
				t.Fatalf("missing producer source minted root connect authority: %#v", withoutSource)
			}
		})
	}
}

func recipientAuthorityAdmit(t *testing.T, plan runfork.RunForkPlan, source semanticview.Source, classification string) ([]forkrecipient.Evidence, []events.DeliveryRoute) {
	t.Helper()
	frontier, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: SelectedContractSelection(source)})
	if err != nil {
		t.Fatal(err)
	}
	if classification == runfork.RunForkPendingClassificationPending {
		if len(frontier.FrontierEvents) != 1 {
			t.Fatalf("frontier events = %#v", frontier.FrontierEvents)
		}
		return frontier.FrontierEvents[0].DerivedRecipients, frontier.FrontierEvents[0].HistoricalDeliveryRoutes
	}
	history, err := AdmitSelectedContractRouteHistory(SelectedContractRouteHistoryRequest{Plan: plan, Source: source, ContractSelection: SelectedContractSelection(source), FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	if len(history.SelectedRouteEvents) != 1 {
		t.Fatalf("history events = %#v", history.SelectedRouteEvents)
	}
	return history.SelectedRouteEvents[0].DerivedRecipients, history.SelectedRouteEvents[0].HistoricalDeliveryRoutes
}

func recipientAuthorityAssertConnect(t *testing.T, recipient forkrecipient.Evidence, plan pinrouting.ConnectRoutePlan) {
	t.Helper()
	wantPlan, err := pinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	gotPlan, gotPin, ok := recipient.Connect()
	if !ok || gotPlan != wantPlan || gotPin != plan.ReceiverPinIdentity().EvidenceIdentity() {
		t.Fatalf("exact selected plan/receiver association lost: %#v", recipient)
	}
	if err := recipient.Validate(); err != nil {
		t.Fatal(err)
	}
}

func recipientAuthorityConnectPlan(t *testing.T, source semanticview.Source, path, pin string) pinrouting.ConnectRoutePlan {
	t.Helper()
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled fixture issues: %#v", issues)
	}
	for _, plan := range graph.Plans() {
		if receiver := plan.ReceiverEndpoint().Readback(); receiver.FlowPath == path && receiver.Pin == pin {
			return plan
		}
	}
	t.Fatalf("missing declared connect receiver %s/%s", path, pin)
	return pinrouting.ConnectRoutePlan{}
}

func recipientAuthorityAgentPlan(t *testing.T, path string) agentidentity.Plan {
	t.Helper()
	name, err := agentidentity.DeclaredName("worker", "test://recipient-authority/"+path+"/worker")
	if err != nil {
		t.Fatal(err)
	}
	route, err := flowidentity.StoredRoute("", "", path).AgentIdentityRoute()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func recipientAuthorityJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type recipientAuthorityReceiver struct {
	path, node string
	agent      bool
	pins       []string
}

// Like testContractFrontierSource, this fixture compiles declared source topology;
// no expected route is supplied to admission or synthesized from its output.
func recipientAuthoritySource(t *testing.T, receivers []recipientAuthorityReceiver) semanticview.Source {
	t.Helper()
	producer := runtimecontracts.FlowContractView{
		Path: "producer", Paths: runtimecontracts.FlowContractPaths{FlowPath: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "static", Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"scan.requested": {}},
	}
	root := runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{producer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:   runtimecontracts.WorkflowSemanticView{Name: "recipient-authority", Version: "v-test"},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{}, FlowSources: map[string]runtimecontracts.FlowSource{},
	}
	bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	for _, declaration := range receivers {
		flow := runtimecontracts.FlowContractView{
			Path: declaration.path, Paths: runtimecontracts.FlowContractPaths{FlowPath: declaration.path},
			Schema: runtimecontracts.FlowSchemaDocument{Mode: "static"}, Events: map[string]runtimecontracts.EventCatalogEntry{},
		}
		handlers := map[string]runtimecontracts.SystemNodeEventHandler{}
		for _, pin := range declaration.pins {
			flow.Schema.Pins.Inputs.EventPins = append(flow.Schema.Pins.Inputs.EventPins, runtimecontracts.FlowInputEventPin{Event: pin})
			flow.Events[pin] = runtimecontracts.EventCatalogEntry{}
			handlers[pin] = runtimecontracts.SystemNodeEventHandler{}
			root.Schema.Connect = append(root.Schema.Connect, runtimecontracts.FlowConnect{SourceLine: len(root.Schema.Connect) + 1, Event: "scan.requested", From: "producer", To: declaration.path, Rename: pin})
		}
		if declaration.node != "" {
			flow.Nodes = map[string]runtimecontracts.SystemNodeContract{declaration.node: {ID: declaration.node, SubscribesTo: declaration.pins, EventHandlers: handlers}}
		}
		if declaration.agent {
			owner := "test://recipient-authority/" + declaration.path + "/worker"
			flow.Agents = map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "worker", AuthoredFields: map[string]bool{"id": true}, Subscriptions: declaration.pins}}
			flow.AgentURIs = map[string]string{"worker": owner}
			ref := runtimecontracts.ContractURIRef{Kind: "agent", FlowID: declaration.path, LocalID: "worker", Full: owner}
			bundle.URIRegistry.ByURI[owner] = ref
			bundle.URIRegistry.Agents[declaration.path+"/worker"] = ref
		}
		if declaration.path == "." {
			root.Schema.Pins = flow.Schema.Pins
			root.Nodes, root.Events = flow.Nodes, flow.Events
			root.Agents, root.AgentURIs = flow.Agents, flow.AgentURIs
		} else {
			root.Children = append(root.Children, flow)
		}
	}
	bundle.RootSchema = &root.Schema
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root, ByPath: map[string]*runtimecontracts.FlowContractView{".": &root}, ByID: map[string]*runtimecontracts.FlowContractView{".": &root}}
	children := []string{}
	for i := range root.Children {
		flow := &root.Children[i]
		children = append(children, flow.Path)
		bundle.FlowTree.ByPath[flow.Path], bundle.FlowTree.ByID[flow.Path] = flow, flow
		bundle.FlowSchemas[flow.Path] = flow.Schema
		bundle.FlowSources[flow.Path] = runtimecontracts.FlowSource{FlowPath: flow.Path, Schema: flow.Path + "/schema.yaml"}
	}
	bundle.FlowSources["."] = runtimecontracts.FlowSource{FlowPath: ".", Schema: "schema.yaml", Children: children}
	return semanticview.Wrap(mustCompileContractFrontierBundle(bundle))
}
