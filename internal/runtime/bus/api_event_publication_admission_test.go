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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRootInputAPIEventPublicationAdmissionIsExactAndClosed(t *testing.T) {
	source := semanticview.Wrap(routedRootInputFlowNodeBundle(t, true))
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
	source := staticAPIEventPublicationSource(t)
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

func staticAPIEventPublicationSource(t *testing.T) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadTargetRouteTempBundle(t, map[string]string{
		"manifest.yaml":         "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":           "stages: []\n",
		"child/manifest.yaml":   "version: 1.0.0\nplatform_version: '*'\n",
		"child/schema.yaml":     "stages: []\n",
		"child/events.yaml":     "work.requested:\n",
		"child/nodes.yaml":      "child-worker:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  event_handlers:\n    work.requested: {}\n",
		"sibling/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"sibling/schema.yaml":   "stages: []\n",
		"sibling/events.yaml":   "work.requested:\n",
		"sibling/nodes.yaml":    "sibling-worker:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  event_handlers:\n    work.requested: {}\n",
	}))
}

func acknowledgedRootInputEndpoint(t testing.TB) (semanticview.Source, APIEventPublicationEndpoint) {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\npins:\n  inputs: [task.requested]\n"),
		"events.yaml":   []byte("task.requested:\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "acknowledged-input", artifact)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "task.requested")
	if err != nil {
		t.Fatal(err)
	}
	return source, endpoint
}
