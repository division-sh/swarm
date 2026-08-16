package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestOrdinaryFlowAPIEventPublicationAdmissionIsExactAndNonTransferable(t *testing.T) {
	source := staticAPIEventPublicationSource()
	endpoint, err := NewOrdinaryFlowAPIEventPublicationEndpoint(source, "child", "child/work.requested")
	if err != nil {
		t.Fatalf("admit exact static endpoint: %v", err)
	}
	if got := endpoint.Readback(); got.Kind != "ordinary_flow" || got.FlowID != "child" || got.EventType != "child/work.requested" {
		t.Fatalf("endpoint readback = %#v, want exact child/work.requested", got)
	}
	if _, err := NewOrdinaryFlowAPIEventPublicationEndpoint(source, "child", "child/work.missing"); err == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("unknown event error = %v, want exact ownership rejection", err)
	}

	runID := uuid.NewString()
	exact := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "child/work.requested", "operator", "", []byte(`{"work_id":"one"}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	admission, publicInput, err := endpoint.admit(source, exact)
	if err != nil || publicInput != nil {
		t.Fatalf("exact admission = %#v public=%#v err=%v", admission, publicInput, err)
	}
	if admission.flowID != "child" || admission.flowPath != "child" || admission.eventType != exact.Type() {
		t.Fatalf("exact admission = %#v, want child owner", admission)
	}

	mismatch := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "sibling/work.requested", "operator", "", []byte(`{}`),
		0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if _, _, err := endpoint.admit(source, mismatch); err == nil || !strings.Contains(err.Error(), "not sibling/work.requested") {
		t.Fatalf("cross-flow endpoint reuse error = %v, want exact event rejection", err)
	}
	existingRun := eventtest.OperatorInjected(
		uuid.NewString(), "child/work.requested", "operator", "", []byte(`{}`),
		0, uuid.NewString(), nil, events.EventEnvelope{}, time.Now().UTC(),
	)
	existingAdmission, publicInput, err := endpoint.admit(source, existingRun)
	if err != nil || publicInput != nil || existingAdmission.flowID != "child" || existingAdmission.eventType != existingRun.Type() {
		t.Fatalf("existing-run exact API admission = %#v public=%#v err=%v", existingAdmission, publicInput, err)
	}
}

func TestRootInputAPIEventPublicationAdmissionIsExactAndClosed(t *testing.T) {
	source := semanticview.Wrap(routedRootInputFlowNodeBundle())
	endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.created")
	if err != nil {
		t.Fatalf("admit root-input endpoint: %v", err)
	}
	if got := endpoint.Readback(); got.Kind != "root_input" || got.FlowID != "" || got.EventType != "thing.created" {
		t.Fatalf("root-input endpoint readback = %#v", got)
	}
	if _, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.missing"); err == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("unknown root-input error = %v, want exact ownership rejection", err)
	}
	existing := eventtest.OperatorInjected(
		uuid.NewString(), "thing.created", "operator", "", []byte(`{}`), 0, uuid.NewString(), nil, events.EventEnvelope{}, time.Now().UTC(),
	)
	admission, publicInput, err := endpoint.admit(source, existing)
	if err != nil || publicInput != nil || admission.kind != apiEventPublicationEndpointRootInput || admission.eventType != existing.Type() {
		t.Fatalf("root-input API admission = %#v public=%#v err=%v", admission, publicInput, err)
	}
	child := eventtest.ChildForProducerWithRoutingSource(
		uuid.NewString(), "thing.created", eventtest.Producer(events.EventProducerNode, "child"), "", []byte(`{}`), 0,
		events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
		events.EventEnvelope{}, eventtest.StaticFlowRoutingSource("child", "child", eventtest.UUID("root-input-child")), time.Now().UTC(),
	)
	if _, _, err := endpoint.admit(source, child); err == nil || !strings.Contains(err.Error(), "root-ingress or operator-injected") {
		t.Fatalf("child root-input endpoint error = %v, want admission-class rejection", err)
	}
	if got, want := int(apiEventPublicationEndpointKindCount-1), 3; got != want {
		t.Fatalf("API endpoint variants = %d, want %d closed variants", got, want)
	}
}

func TestTemplateAPIEventPublicationEndpointRejectsForgedCensusFacts(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
	association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
	endpoint, ok := association.Endpoint()
	if !ok {
		t.Fatalf("resolve template endpoint: %v", association.Err())
	}
	if _, err := NewTemplateAPIEventPublicationEndpoint(source, endpoint); err != nil {
		t.Fatalf("seal exact template endpoint: %v", err)
	}
	forged := endpoint
	forged.FlowID = "unrelated"
	if _, err := NewTemplateAPIEventPublicationEndpoint(source, forged); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("forged template endpoint error = %v, want census mismatch", err)
	}
}

func TestOrdinaryFlowAPIEventPublicationAdmissionOwnsOnlyItsExactNodeRoutes(t *testing.T) {
	source := staticAPIEventPublicationSource()
	endpoint, err := NewOrdinaryFlowAPIEventPublicationEndpoint(source, "child", "child/work.requested")
	if err != nil {
		t.Fatalf("admit exact static endpoint: %v", err)
	}
	store := newTargetRouteMemoryStore()
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "child/work.requested", "operator", "", []byte(`{"work_id":"one"}`),
		0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	admission, _, err := endpoint.admit(source, evt)
	if err != nil {
		t.Fatalf("admit publication event: %v", err)
	}
	plan, err := eventBus.CheckPublishRecipientPlan(withAPIEventPublicationAdmission(context.Background(), admission), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan with exact admission: %v", err)
	}
	routes := plan.DeliveryRoutes
	if len(routes) != 1 || routes[0].Recipient.LocalID() != "child-worker" || !routes[0].Target.EntitylessReceiver() {
		t.Fatalf("delivery routes = %#v, want one entityless child-worker", routes)
	}
	if target := routes[0].Target.Route(); target.FlowID != "child" || target.FlowInstance != "child" || target.EntityID != "" {
		t.Fatalf("delivery target = %#v, want exact child blueprint without borrowed entity", target)
	}

	if _, err := eventBus.CheckPublishRecipientPlan(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "without exact same-instance, explicit-target, or compiled-connect authority") {
		t.Fatalf("unadmitted plan error = %v, want generic subscription rejection", err)
	}
}

func staticAPIEventPublicationSource() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Path: "child", Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.requested": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-worker": {
				ID: "child-worker", SubscribesTo: []string{"work.requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {}},
			},
		},
	}
	sibling := runtimecontracts.FlowContractView{
		Path: "sibling", Paths: runtimecontracts.FlowContractPaths{ID: "sibling", Flow: "sibling"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.requested": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"sibling-worker": {
				ID: "sibling-worker", SubscribesTo: []string{"work.requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {}},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child, sibling}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0], "sibling": &root.Children[1],
			},
		},
	})
}
