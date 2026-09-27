package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestOrdinaryChildHasNoPublicAPIEventPublicationEndpoint(t *testing.T) {
	source := staticAPIEventPublicationSource()
	for _, event := range []string{"child/work.requested", "sibling/work.requested", "work.requested", "child/work.missing"} {
		if _, err := NewRootInputAPIEventPublicationEndpoint(source, event); err == nil {
			t.Fatalf("private child event %q acquired public authority", event)
		}
	}
}

func TestRootInputAPIEventPublicationAdmissionIsExactAndClosed(t *testing.T) {
	source := semanticview.Wrap(routedRootInputFlowNodeBundle())
	endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.created")
	if err != nil {
		t.Fatalf("admit root-input endpoint: %v", err)
	}
	if got := endpoint.Readback(); got.Kind != "root_input" || got.FlowID != "." || got.EventType != "thing.created" {
		t.Fatalf("root-input endpoint readback = %#v", got)
	}
	if _, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.missing"); err == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("unknown root-input error = %v, want exact ownership rejection", err)
	}
	runID := uuid.NewString()
	existing := eventtest.OperatorInjectedWithRoutingSource(
		uuid.NewString(), "thing.created", "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC(),
	)
	admission, _, err := endpoint.admit(source, existing)
	if err != nil || admission.eventType != existing.Type() {
		t.Fatalf("root-input API admission = %#v err=%v", admission, err)
	}
	for _, wrongSource := range []events.RoutingSource{
		events.NoRoutingSource(),
		eventtest.RootRoutingSource(uuid.NewString()),
		eventtest.StaticFlowRoutingSource("child", "child", uuid.NewString()),
	} {
		forged := eventtest.RunCreatingRootIngressWithRoutingSource(uuid.NewString(), "thing.created", "operator", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, wrongSource, time.Now().UTC())
		if _, _, err := endpoint.admit(source, forged); err == nil || !strings.Contains(err.Error(), "exact root routing source") {
			t.Fatalf("foreign or absent source accepted: %#v, %v", wrongSource, err)
		}
	}
	child := eventtest.ChildForProducerWithRoutingSource(
		uuid.NewString(), "thing.created", eventtest.Producer(events.EventProducerNode, "child"), "", []byte(`{}`), 0,
		events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
		events.EventEnvelope{}, eventtest.StaticFlowRoutingSource("child", "child", eventtest.UUID("root-input-child")), time.Now().UTC(),
	)
	if _, _, err := endpoint.admit(source, child); err == nil || !strings.Contains(err.Error(), "root-ingress or operator-injected") {
		t.Fatalf("child root-input endpoint error = %v, want admission-class rejection", err)
	}
}

func TestPublicInputAuthorityCannotTargetPrivateChild(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
	for _, event := range []string{"deploy.done", "consumer/deploy.done"} {
		if _, err := NewRootInputAPIEventPublicationEndpoint(source, event); err == nil {
			t.Fatalf("private template input %q acquired public authority", event)
		}
	}
}

func TestAPIEventPublicationRequiresEndpointBeforePlanningOrMutation(t *testing.T) {
	source := staticAPIEventPublicationSource()
	store := newTargetRouteMemoryStore()
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "child/work.requested", "operator", "", []byte(`{"work_id":"one"}`),
		0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	if _, err := eventBus.CheckAPIEventPublishRecipientPlan(context.Background(), evt, nil); err == nil {
		t.Fatal("missing endpoint acquired a public recipient plan")
	}
	if _, _, err := eventBus.PublishAPIEventAcknowledged(context.Background(), evt, nil, apiidempotency.Request{}, apiidempotency.Completion{}); err == nil || !strings.Contains(err.Error(), "requires an admitted endpoint") {
		t.Fatalf("missing endpoint must fail before store mutation: %v", err)
	}
}

func staticAPIEventPublicationSource() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Path: "child", Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.requested": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-worker": {
				SubscribesTo:  []string{"work.requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {}},
			},
		},
	}
	sibling := runtimecontracts.FlowContractView{
		Path: "sibling", Paths: runtimecontracts.FlowContractPaths{FlowPath: "sibling"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.requested": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"sibling-worker": {
				SubscribesTo:  []string{"work.requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {}},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Path: ".", Children: []runtimecontracts.FlowContractView{child, sibling}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0], "sibling": &root.Children[1],
			},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}

func acknowledgedRootInputEndpoint(t testing.TB) (semanticview.Source, APIEventPublicationEndpoint) {
	t.Helper()
	schema := runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			EventPins: []runtimecontracts.FlowInputEventPin{{Event: "task.requested"}},
		}},
	}
	root := runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Schema: schema}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &schema,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{".": schema},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "task.requested")
	if err != nil {
		t.Fatal(err)
	}
	return source, endpoint
}
