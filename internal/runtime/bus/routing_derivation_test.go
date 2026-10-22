package bus_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestKeylessConstructionIndexUsesExactRunOwners(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyParentConnectTimer(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	for _, flowID := range []string{".", "producer", "consumer"} {
		t.Run(flowID, func(t *testing.T) {
			var observations []runtimepipeline.FlowInstanceObservation
			for _, runID := range []string{eventBusTestRunID, eventtest.UUID("independent-keyless-run")} {
				instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, "", runID)
				owner := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: instance.Route()}
				request, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, owner)
				if err != nil {
					t.Fatal(err)
				}
				index := runtimebus.FlowInstanceIndexFixture(observations...)
				if observed, found, err := index.LookupFlowInstance(context.Background(), request); err != nil || found || observed.Valid() {
					t.Fatalf("declaration or sibling run fabricated native existence: found=%t err=%v", found, err)
				}
				observations = append(observations, runtimebus.AdmittedFlowInstanceObservationFixture(t, source, runID, instance, ""))
			}
			index := runtimebus.FlowInstanceIndexFixture(observations...)
			for _, observed := range observations {
				request, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, observed.Owner())
				if err != nil {
					t.Fatal(err)
				}
				if actual, found, err := index.LookupFlowInstance(context.Background(), request); err != nil || !found || actual.Identity() != observed.Identity() {
					t.Fatalf("exact keyless run owner changed: actual=%+v found=%t err=%v", actual.Identity(), found, err)
				}
				malformed := observed.Owner()
				malformed.Route.InstancePath = "foreign/" + malformed.Route.InstancePath
				if foreign, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, malformed); err == nil {
					if actual, found, err := index.LookupFlowInstance(context.Background(), foreign); actual.Valid() || found {
						t.Fatalf("keyless declaration elected a foreign concrete path: found=%t err=%v", found, err)
					}
				}
				if flowID == "." {
					foreignRun := observed.Owner()
					foreignRun.RunID = eventtest.UUID("third-keyless-run")
					if _, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, foreignRun); err == nil {
						t.Fatal("root route borrowed a different run's identity")
					}
				}
			}
		})
	}
}

func TestEventBusFlowInstanceTemplateDerivesSubscriptionsFromHandlerKeys(t *testing.T) {
	source := routeMaterializationNodeSource(t, "review", "  event_handlers:\n    task.started:\n      emit:\n        event: task.started\n")
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if got := eb.RouteTable().PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, runtimebus.ConstructedFlowInstanceIdentityFixture(source, "review", "inst-1", eventBusTestRunID), "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("resolved subscribers = %#v, want materialized-node declaration identity", got)
	}
}

func TestEventBusExactSubscriptionAdmissionPreservesLocalNodeAndSameScopeAgentRoutes(t *testing.T) {
	source := exactSubscriptionRouteSource(t, "task.done", []string{"child/task.done"})
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := routes.PubsubDeclarationDefinitionsFixture(t, "child", "child/task.done")
	if len(got) != 2 {
		t.Fatalf("Resolve(child/task.done) = %#v, want local node and same-scope agent", got)
	}
	seen := map[string]bool{}
	for _, subscriber := range got {
		seen[subscriber.Recipient.Code()+":"+subscriber.Recipient.LocalID()] = true
	}
	if !seen["node:listener"] || !seen["agent:observer"] {
		t.Fatalf("resolved subscribers = %#v, want listener and observer", got)
	}
}

func TestDeriveRouteTableRegistersNestedPhysicalAgentDeclarationExactlyOnce(t *testing.T) {
	source := loadNestedPhysicalAgentRouteSource(t)
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("canonical declarations = %#v, want one physical declaration", declarations)
	}
	flowPath := strings.Trim(strings.TrimSpace(source.FlowPath("parent/child/support")), "/")
	if flowPath == "" {
		t.Fatal("support flow path is empty")
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	resolved := routes.PubsubDeclarationDefinitionsFixture(t, "parent/child/support", flowPath+"/work.requested")
	if len(resolved) != 1 {
		t.Fatalf("nested agent subscribers = %#v, want one physical subscriber", resolved)
	}
	if resolved[0].Recipient.ID() != "public-worker" || resolved[0].AgentPlan.Name.Owner != declarations[0].OwnerURI {
		t.Fatalf("nested agent subscriber = %#v, want exact canonical declaration owner %#v", resolved[0], declarations[0])
	}
}

func loadNestedPhysicalAgentRouteSource(t *testing.T) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadEventBusTempBundle(t, map[string]string{
		"manifest.yaml":                      "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":                        "stages: []\n",
		"parent/child/support/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"parent/child/support/schema.yaml":   "stages:\n  active: {}\npins:\n  inputs: [work.requested]\n",
		"parent/child/support/events.yaml":   "work.requested:\n",
		"parent/child/support/agents.yaml": `
worker:
  id: public-worker
  role: worker
  intent: {inline: Register one nested physical subscriber.}
  model: regular
  subscriptions: [work.requested]
`,
	}))
}

func TestDeriveRouteTableRequiresExactPackageOwnerAcrossRouteSurfaces(t *testing.T) {
	for _, mode := range []string{"static", "template"} {
		for _, reverse := range []bool{false, true} {
			order := "a_then_b"
			if reverse {
				order = "b_then_a"
			}
			t.Run(mode+"/"+order, func(t *testing.T) {
				source := loadPackageCollisionRouteSource(t, mode, reverse)
				routes, err := runtimebus.DeriveRouteTable(source)
				if err != nil {
					t.Fatalf("DeriveRouteTable: %v", err)
				}
				if mode == "template" {
					instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "orders", "one", eventBusTestRunID)
					assertExactFlowRoute(t, routes.PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, instance, "orders/one/root.start"), "subscription", "orders")
					return
				}
				resolved := routes.PubsubDeclarationDefinitionsFixture(t, "orders", "orders/root.start")
				assertExactFlowRoute(t, resolved, "subscription", "orders")
				if got := routes.PubsubDeclarationDefinitionsFixture(t, "orders", "root.start"); len(got) != 0 {
					t.Fatalf("root input manufactured private subscription: %#v", got)
				}
			})
		}
	}
}

func assertExactFlowRoute(t *testing.T, subscribers []runtimebus.Subscriber, routeSource, flowPath string) {
	t.Helper()
	matches := 0
	for _, subscriber := range subscribers {
		if subscriber.RouteSourceCode() != routeSource {
			continue
		}
		node, ok := subscriber.Recipient.Node()
		if !ok {
			t.Fatalf("subscriber = %#v, want node recipient", subscriber)
		}
		if node.FlowPath() != flowPath || node.NodeID() != "shared" {
			t.Fatalf("subscriber owner = %q/%q, want %q/shared", node.FlowPath(), node.NodeID(), flowPath)
		}
		matches++
	}
	if matches != 1 {
		t.Fatalf("%s routes = %#v, want one exact filesystem flow owner", routeSource, subscribers)
	}
}

func loadPackageCollisionRouteSource(t *testing.T, mode string, reverse bool) semanticview.Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := t.TempDir()
	packages := []string{"orders/addon-a", "orders/addon-b"}
	if reverse {
		packages[0], packages[1] = packages[1], packages[0]
	}
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}

	write(filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	write(filepath.Join(root, "schema.yaml"), `stages: []
pins:
  inputs:
    - root.start
`)
	write(filepath.Join(root, "events.yaml"), "root.start:\n")

	instance := ""
	if mode == runtimecontracts.FlowModeTemplate {
		instance = "instance: order_id\n"
		write(filepath.Join(root, "orders", "entities.yaml"), "order:\n  order_id: text\n")
	}
	write(filepath.Join(root, "orders", "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	write(filepath.Join(root, "orders", "schema.yaml"), instance+`stages:
  active: {}
  done: {final: true}
pins:
  inputs: [root.start]
`)
	write(filepath.Join(root, "orders", "events.yaml"), "root.start:\naddon_a.start:\naddon_b.start:\n")
	write(filepath.Join(root, "orders", "nodes.yaml"), `
shared:
  execution_type: system_node
  event_handlers:
    root.start:
      advances_to: done
`)
	for _, name := range []string{"addon-a", "addon-b"} {
		dir := filepath.Join(root, "orders", name)

		eventName := strings.ReplaceAll(name, "-", "_")
		write(filepath.Join(dir, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
		write(filepath.Join(dir, "schema.yaml"), "# fixture: "+name+"\n")
		write(filepath.Join(dir, "events.yaml"), eventName+".start:\n")
		write(filepath.Join(dir, "nodes.yaml"), `
shared:
  execution_type: system_node
  event_handlers:
    `+eventName+`.start:
      advances_to: done
`)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func TestEventBusLocalNodeWildcardAdmissionPreservesScope(t *testing.T) {
	for _, authored := range []string{"task.*", "*.done", "*", "missing.*"} {
		t.Run(authored, func(t *testing.T) {
			routes, err := runtimebus.DeriveRouteTable(exactSubscriptionRouteSource(t, authored, nil))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			wantLocal := authored != "missing.*"
			if got := len(routes.PubsubDeclarationDefinitionsFixture(t, "child", "child/task.done")); (got == 1) != wantLocal {
				t.Fatalf("Resolve(child/task.done) count = %d, want local match %t", got, wantLocal)
			}
			if got := routes.PubsubDeclarationDefinitionsFixture(t, "child", "sibling/task.done"); len(got) != 0 {
				t.Fatalf("Resolve(sibling/task.done) = %#v, want no cross-scope subscriber", got)
			}
		})
	}
}

func TestEventBusTemplateAgentSameScopeExactAdmissionRendersConcreteInstanceRoute(t *testing.T) {
	bundle := routeMaterializationConfigVarBundle(t, "", "operating/opco.product_initialization_requested")
	source := semanticview.Wrap(bundle)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	route := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", route.InstanceID, eventBusTestRunID)
	got := rt.PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, instance, route.InstancePath+"/opco.product_initialization_requested")
	if len(got) != 1 || got[0].Recipient.LocalID() != "ceo" || got[0].AgentPlan.FlowInstance() != route.InstancePath {
		t.Fatalf("resolved subscribers = %#v, want concrete same-scope agent route", got)
	}
}

func exactSubscriptionRouteSource(t *testing.T, nodeSubscription string, agentSubscriptions []string) semanticview.Source {
	t.Helper()
	files := map[string]string{
		"manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":         "stages: []\n",
		"child/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"child/schema.yaml":   "stages: []\n",
		"child/events.yaml":   "task.done:\n",
		"child/nodes.yaml":    fmt.Sprintf("listener:\n  execution_type: system_node\n  subscribes_to: [%q]\n  event_handlers:\n    %q: {}\n", nodeSubscription, nodeSubscription),
	}
	if len(agentSubscriptions) > 0 {
		files["child/agents.yaml"] = "observer:\n  intent: {inline: 'Observe task completion.'}\n  subscriptions: [" + strings.Join(agentSubscriptions, ", ") + "]\n"
	}
	return semanticview.Wrap(loadEventBusTempBundle(t, files))
}

type routePersistenceTestStore struct {
	targetOwners []runtimebus.ActiveTargetDescriptor
	deliveries   map[string][]string
}

func (s *routePersistenceTestStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := s.InsertEventDeliveryRoutes(ctx, command.Commit.Event.ID(), command.Commit.DeliveryRoutes); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	return (runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}).WithCommitAcknowledgment(), nil
}

func (s *routePersistenceTestStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}
func (*routePersistenceTestStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *routePersistenceTestStore) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestEventBusCompiledFlowInstanceBindingDoesNotRewritePersistence(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource(t, "review", "  produces: [task.started]\n  subscribes_to: [task.started]\n")
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "review", "inst-1", eventBusTestRunID)
	observed := runtimebus.AdmittedFlowInstanceObservationFixture(t, source, eventBusTestRunID, instance, "acct-1")
	index := runtimebus.FlowInstanceIndexFixture(observed)
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source, Durable: runtimebus.DurableDependencies{Instances: index}})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if got := eb.RouteTable().PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, runtimebus.ConstructedFlowInstanceIdentityFixture(source, "review", "inst-1", eventBusTestRunID), "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("compiled subscribers = %#v, want materialized-node declaration identity", got)
	}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), observed.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if actual, found, err := index.LookupFlowInstance(context.Background(), lookup); err != nil || !found || !reflect.DeepEqual(actual, observed) {
		t.Fatalf("compiled binding changed native instance evidence: actual=%+v found=%t err=%v", actual, found, err)
	}
}

func TestCompiledFlowInstanceBindingRejectsUnknownCanonicalTemplateWithoutMutation(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource(t, "known", "  subscribes_to: [task.started]\n")
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "known", "inst-1", eventBusTestRunID)
	observed := runtimebus.AdmittedFlowInstanceObservationFixture(t, source, eventBusTestRunID, instance, "acct-1")
	index := runtimebus.FlowInstanceIndexFixture(observed)
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source, Durable: runtimebus.DurableDependencies{Instances: index}})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("unknown", "inst-1"))
	unknown := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "known", identity.Route.InstanceID, identity.RunID)
	unknown.TemplateID, unknown.ScopeKey, unknown.InstancePath, unknown.EntityID = "unknown", "unknown", identity.Route.InstancePath, runtimeflowidentity.EntityID(identity.Route.InstancePath)
	_, err = eb.RouteTable().PubsubReceiverDefinitions(identity.RunID, unknown, []string{identity.Route.InstancePath + "/task.started"})
	if err == nil || !strings.Contains(err.Error(), "construction identity has an unknown or inconsistent authored owner") {
		t.Fatalf("compiled binding error = %v, want unknown canonical template", err)
	}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), observed.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if actual, found, err := index.LookupFlowInstance(context.Background(), lookup); err != nil || !found || !reflect.DeepEqual(actual, observed) {
		t.Fatalf("unknown template changed native instance evidence: actual=%+v found=%t err=%v", actual, found, err)
	}
}

func TestIndexedFlowInstanceBindingRejectsCrossedExactOwners(t *testing.T) {
	source := routeMaterializationNodeSource(t, "review", "  subscribes_to: [task.started]\n")
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "review", "stored-one", eventBusTestRunID)
	instance.EntityID = eventtest.UUID("non-derived-stored-review")
	observed := runtimebus.AdmittedFlowInstanceObservationFixture(t, source, eventBusTestRunID, instance, "acct-1")
	index := runtimebus.FlowInstanceIndexFixture(observed)
	for _, test := range []struct {
		name   string
		mutate func(*runtimeflowidentity.RunScopedFlowInstance)
	}{
		{"foreign run", func(owner *runtimeflowidentity.RunScopedFlowInstance) { owner.RunID = eventtest.UUID("foreign-run") }},
		{"crossed flow", func(owner *runtimeflowidentity.RunScopedFlowInstance) { owner.Route.ScopeKey = "worker" }},
		{"crossed key", func(owner *runtimeflowidentity.RunScopedFlowInstance) { owner.Route.InstanceID = "other" }},
		{"crossed path", func(owner *runtimeflowidentity.RunScopedFlowInstance) { owner.Route.InstancePath = "review/other" }},
		{"uncanonical path", func(owner *runtimeflowidentity.RunScopedFlowInstance) {
			owner.Route.InstancePath = " /review/stored-one/ "
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := observed.Owner()
			test.mutate(&owner)
			request, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), owner)
			if err == nil {
				if actual, found, err := index.LookupFlowInstance(context.Background(), request); actual.Valid() || found {
					t.Fatalf("crossed owner selected an unrelated header: actual=%+v found=%t err=%v", actual.Identity(), found, err)
				}
			}
			request, err = runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), observed.Owner())
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				actual, found, err := index.LookupFlowInstance(context.Background(), request)
				if err != nil || !found || actual.Identity() != instance || actual.InstanceKey() != "acct-1" {
					t.Fatalf("refusal or repeated lookup changed stored construction: actual=%+v found=%t err=%v", actual.Identity(), found, err)
				}
			}
		})
	}
}

func TestEventBusIndexedFlowInstanceDeliversDeclaredAgentSubscriber(t *testing.T) {
	store := &routePersistenceTestStore{}
	bundle := routeMaterializationConfigVarBundle(t, "", "opco.product_initialization_requested")
	source := semanticview.Wrap(bundle)
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", "11111111-1111-4111-8111-111111111111", eventBusTestRunID)
	observed := runtimebus.AdmittedFlowInstanceObservationFixture(t, source, eventBusTestRunID, instance, instance.InstanceID)
	index := runtimebus.FlowInstanceIndexFixture(observed)
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		ContractBundle: source,
		Durable:        runtimebus.DurableDependencies{Instances: index},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
		return nil
	}))
	routeIdentity := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	identity := testRunScopedFlowRoute(routeIdentity)
	store.targetOwners = []runtimebus.ActiveTargetDescriptor{{
		ID: "operating-owner", FlowInstance: routeIdentity.InstancePath, EntityID: runtimeflowidentity.EntityID(routeIdentity.InstancePath),
	}}
	agentIdentity, admission := routeMaterializationAgentRoute(t, source, identity)
	agentPlan, err := agentIdentity.Plan()
	if err != nil {
		t.Fatalf("project active agent plan: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		Identity: agentIdentity, EntityID: runtimeflowidentity.EntityID(routeIdentity.InstancePath),
	})
	runtimebustest.SubscribeIdentity(t, eb, agentIdentity, admission)
	defer runtimebustest.UnsubscribeIdentity(eb, agentIdentity)
	resolved := eb.RouteTable().PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", identity.Route.InstanceID, eventBusTestRunID), "operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
	if len(resolved) != 1 || resolved[0].AgentPlan != agentPlan {
		t.Fatalf("active agent route = %#v, want exact identity %s", resolved, agentIdentity)
	}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), observed.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if actual, found, err := index.LookupFlowInstance(context.Background(), lookup); err != nil || !found || !reflect.DeepEqual(actual, observed) {
		t.Fatalf("compiled agent binding changed native instance evidence: actual=%+v found=%t err=%v", actual, found, err)
	}
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("event-rendered-route-delivery"),
		events.EventType("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested"), "", "", nil, 0, eventBusTestRunID,
		events.EventEnvelope{EntityID: instance.EntityID, FlowInstance: instance.InstancePath},
		eventtest.ConcreteTemplateRoutingSource(instance.TemplateID, instance.InstancePath, instance.EntityID), time.Time{})

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.deliveries[evt.ID()]
	if len(got) != 1 || got[0] != "ceo" {
		t.Fatalf("delivery recipients = %#v, want stable declared ceo name", got)
	}
}

func TestRouteTableFlowInstanceRouteKeepsAgentNameStableAcrossActivationConfig(t *testing.T) {
	source := semanticview.Wrap(routeMaterializationConfigVarBundle(t, "", "opco.product_initialization_requested"))
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	firstRoute := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	first := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", firstRoute.InstanceID, eventBusTestRunID)
	got, err := rt.PubsubReceiverDefinitions(eventBusTestRunID, first, []string{first.InstancePath + "/opco.product_initialization_requested"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one ceo route", got)
	}
	if got[0].Recipient.LocalID() != "ceo" {
		t.Fatalf("resolved subscriber id = %q, want stable declared ceo name", got[0].Recipient.LocalID())
	}
	if got[0].AgentPlan.AgentID() != got[0].Recipient.LocalID() || got[0].AgentPlan.FlowInstance() != firstRoute.InstancePath {
		t.Fatalf("resolved subscriber identity = %#v, want exact first instance", got[0].AgentPlan)
	}
	siblingRoute := runtimeflowidentity.DeriveRoute("operating", "22222222-2222-4222-8222-222222222222")
	second := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", siblingRoute.InstanceID, eventBusTestRunID)
	sibling, err := rt.PubsubReceiverDefinitions(eventBusTestRunID, second, []string{second.InstancePath + "/opco.product_initialization_requested"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sibling) != 1 || sibling[0].Recipient.LocalID() != got[0].Recipient.LocalID() ||
		sibling[0].AgentPlan.FlowInstance() != siblingRoute.InstancePath ||
		sibling[0].AgentPlan == got[0].AgentPlan {
		t.Fatalf("resolved sibling subscriber = %#v, want same slug with distinct concrete identity from %#v", sibling, got[0])
	}
	if len(rt.ConnectDeclarationDefinitions("operating")) != 0 {
		t.Fatal("local subscription acquired an undeclared connection")
	}
}

func TestRouteTableExplicitAgentNameChangesPublicNameNotScopedCoordinate(t *testing.T) {
	bundle := routeMaterializationConfigVarBundle(t, "executive", "opco.product_initialization_requested")
	source := semanticview.Wrap(bundle)

	scope, ok := source.FlowScopeByID("operating")
	if !ok {
		t.Fatal("operating flow scope not found")
	}
	plan, err := semanticview.FlowAgentNamePlan(source, scope, "ceo")
	if err != nil {
		t.Fatalf("AgentNamePlanFor: %v", err)
	}
	if plan.LocalID != "ceo" || plan.AgentID != "executive" {
		t.Fatalf("agent name plan = %#v, want local coordinate ceo and public name executive", plan)
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	identities := []runtimeflowidentity.Route{
		runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111"),
		runtimeflowidentity.DeriveRoute("operating", "22222222-2222-4222-8222-222222222222"),
	}
	var concrete []agentidentity.Plan
	for _, identity := range identities {
		instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "operating", identity.InstanceID, eventBusTestRunID)
		got := routes.PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, instance, identity.InstancePath+"/opco.product_initialization_requested")
		if len(got) != 1 || got[0].Recipient.LocalID() != "executive" || got[0].AgentPlan.FlowInstance() != identity.InstancePath {
			t.Fatalf("resolved %s = %#v, want executive on exact instance", identity.InstancePath, got)
		}
		concrete = append(concrete, got[0].AgentPlan)
	}
	if concrete[0] == concrete[1] {
		t.Fatalf("sibling concrete identities collapsed: %#v", concrete)
	}
}

func routeMaterializationNodeSource(t *testing.T, flowID, nodeBody string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadEventBusTempBundle(t, map[string]string{
		"manifest.yaml":           "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":             "stages: []\n",
		flowID + "/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		flowID + "/schema.yaml":   "stages: []\ninstance: instance_key\n",
		flowID + "/entities.yaml": "work:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n",
		flowID + "/events.yaml":   "task.started:\n",
		flowID + "/nodes.yaml":    "materialized-node:\n  execution_type: system_node\n" + nodeBody,
	}))
}

func routeMaterializationConfigVarBundle(t *testing.T, publicName, subscription string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	agent := "ceo:\n  role: ceo\n  intent: {inline: 'Observe product initialization.'}\n  subscriptions: [" + subscription + "]\n"
	if publicName != "" {
		agent += "  id: " + publicName + "\n"
	}
	return loadEventBusTempBundle(t, map[string]string{
		"manifest.yaml":           "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":             "stages: []\n",
		"operating/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"operating/schema.yaml":   "stages: []\ninstance: instance_key\npins:\n  inputs: [opco.product_initialization_requested]\n",
		"operating/entities.yaml": "work:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n",
		"operating/events.yaml":   "opco.product_initialization_requested:\n",
		"operating/agents.yaml":   agent,
	})
}

func routeMaterializationAgentRoute(
	t testing.TB,
	source semanticview.Source,
	owner runtimeflowidentity.RunScopedFlowInstance,
) (agentidentity.Identity, semanticview.FlowOwnedAgentSubscriptionAdmission) {
	t.Helper()
	route := owner.Route
	scope, ok := source.FlowScopeByID("operating")
	if !ok {
		t.Fatal("operating flow scope not found")
	}
	plan, err := semanticview.FlowAgentNamePlan(source, scope, "ceo")
	if err != nil {
		t.Fatalf("resolve operating/ceo declared name: %v", err)
	}
	name, err := plan.Materialize()
	if err != nil {
		t.Fatalf("materialize operating/ceo declared name: %v", err)
	}
	identityRoute, err := route.AgentIdentityRoute()
	if err != nil {
		t.Fatalf("build rendered agent route: %v", err)
	}
	identity, err := agentidentity.New(owner.RunID, name, identityRoute)
	if err != nil {
		t.Fatalf("build rendered agent identity: %v", err)
	}
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:  plan.AgentID,
		FlowID:   "operating",
		FlowPath: route.InstancePath,
	})
	if err != nil {
		t.Fatalf("admit rendered agent route: %v", err)
	}
	return identity, admission
}

func TestRouteTableTemplateOutputConnectDoesNotCreateCrossFlowPubSubSubscriber(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateOutputRootConnect(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load template-output observer fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 || len(plans) != 1 {
		t.Fatalf("template output connect plans = %#v issues = %#v, want one valid plan", plans, issues)
	}
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.PubsubDeclarationDefinitionsFixture(t, ".", "producer/component-a/deploy.done"); len(got) != 0 {
		t.Fatalf("direct template output subscribers = %#v, connect dispatch must own the boundary edge", got)
	}
	got := rt.PubsubDeclarationDefinitionsFixture(t, ".", "deploy.done")
	if len(got) != 1 {
		t.Fatalf("receiver-local subscribers = %#v, want one root-receiver route", got)
	}
	if got[0].Recipient.LocalID() != "root-receiver" || !got[0].Recipient.IsNode() {
		t.Fatalf("resolved subscriber = %#v, want root-receiver connect route", got[0])
	}

	if got := rt.PubsubDeclarationDefinitionsFixture(t, ".", "producer/component-b/deploy.done"); len(got) != 0 {
		t.Fatalf("resolved subscribers for never-added instance = %#v, want none", got)
	}
}

func TestDeriveRouteTable_HandlerOnlyInputPinsDoNotAutoWireFromProducerOutput(t *testing.T) {
	bundle := loadEventBusTempBundle(t, map[string]string{
		"manifest.yaml":          "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":            "stages: []\n",
		"producer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"producer/schema.yaml":   "stages: []\npins:\n  outputs: [scan.requested]\n",
		"producer/events.yaml":   "scan.requested:\n",
		"consumer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"consumer/schema.yaml":   "stages: []\npins:\n  inputs: [scan.requested]\n",
		"consumer/events.yaml":   "scan.requested:\n",
		"consumer/nodes.yaml":    "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    scan.requested: {}\n",
	})
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.PubsubDeclarationDefinitionsFixture(t, "consumer", "producer/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want none for retired sibling auto-wire", got)
	}
	got := rt.PubsubDeclarationDefinitionsFixture(t, "consumer", "consumer/scan.requested")
	if len(got) != 1 || got[0].Recipient.LocalID() != "consumer-node" {
		t.Fatalf("Resolve(consumer/scan.requested) = %#v, want consumer-node local input route", got)
	}
}

func TestDeriveRouteTable_StaticChildFlowInputSubscriptionsResolveCanonicalNodeOwners(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	for _, tc := range []struct {
		fixture     string
		eventType   string
		nodeID      string
		flowPath    string
		routeMatch  string
		routeSource string
	}{
		{
			fixture:     "test-child-flow-local-events",
			eventType:   "child/child.start",
			nodeID:      "child-intake",
			flowPath:    "child",
			routeMatch:  "child/child.start",
			routeSource: "subscription",
		},
		{
			fixture:     "test-nested-three-levels",
			eventType:   "child/step.begin",
			nodeID:      "child-relay",
			flowPath:    "child",
			routeMatch:  "child/step.begin",
			routeSource: "subscription",
		},
		{
			fixture:     "test-nested-three-levels",
			eventType:   "child/grandchild/micro.start",
			nodeID:      "grandchild-worker",
			flowPath:    "child/grandchild",
			routeMatch:  "child/grandchild/micro.start",
			routeSource: "subscription",
		},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", tc.fixture)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			got := rt.PubsubDeclarationDefinitionsFixture(t, tc.flowPath, tc.eventType)
			if len(got) != 1 ||
				got[0].Recipient.LocalID() != tc.nodeID ||
				!got[0].Recipient.IsNode() ||
				got[0].Path != tc.flowPath ||
				got[0].MatchPattern != tc.routeMatch ||
				got[0].RouteSourceCode() != tc.routeSource {
				t.Fatalf("Resolve(%s) = %#v, want %s %s %s from %s", tc.eventType, got, tc.nodeID, tc.flowPath, tc.routeMatch, tc.routeSource)
			}
		})
	}
}

func TestDeriveRouteTable_RuntimeProducedFollowUpSubscriptionsResolveCanonicalNodeOwners(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	for _, tc := range []struct {
		name       string
		fixture    string
		eventType  string
		nodeID     string
		flowPath   string
		routeMatch string
	}{
		{
			name:       "root-local timer follow-up",
			fixture:    filepath.Join("tests", "tier5-flow-lifecycle", "test-timer-fire"),
			eventType:  "timer.check",
			nodeID:     "test-node",
			flowPath:   ".",
			routeMatch: "timer.check",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, tc.fixture)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			got := rt.PubsubDeclarationDefinitionsFixture(t, tc.flowPath, tc.eventType)
			found := false
			for _, subscriber := range got {
				if subscriber.Recipient.LocalID() == tc.nodeID &&
					subscriber.Recipient.IsNode() &&
					subscriber.Path == tc.flowPath &&
					subscriber.MatchPattern == tc.routeMatch {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Resolve(%s) = %#v, want %s %s %s", tc.eventType, got, tc.nodeID, tc.flowPath, tc.routeMatch)
			}
		})
	}
}

func TestDeriveRouteTable_NestedPackageConnectLocalizesWithinParentFlow(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, canonicalrouting.CopyNestedFlowConnect(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load nested package connect fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 || len(plans) != 1 {
		t.Fatalf("nested connect plans = %#v issues = %#v, want one valid plan", plans, issues)
	}
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.PubsubDeclarationDefinitionsFixture(t, "child", "child/grandchild/micro.done"); len(got) != 0 {
		t.Fatalf("direct descendant route = %#v, connect dispatch must own the boundary edge", got)
	}
	got := rt.PubsubDeclarationDefinitionsFixture(t, "child", "child/micro.done")
	if len(got) != 1 || got[0].Recipient.LocalID() != "child-aggregator" {
		t.Fatalf("Resolve(child/micro.done) = %#v, want receiver-local child-aggregator carrier", got)
	}
	if got[0].Path != "child" {
		t.Fatalf("receiver carrier path = %q, want child", got[0].Path)
	}
}

func TestDeriveRouteTable_NestedTemplateInstancesBindSemanticScopeKey(t *testing.T) {
	bundle := loadEventBusTempBundle(t, map[string]string{
		"manifest.yaml":                  "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":                    "stages: []\n",
		"child/manifest.yaml":            "version: 1.0.0\nplatform_version: '*'\n",
		"child/schema.yaml":              "stages: []\n",
		"child/grandchild/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"child/grandchild/schema.yaml":   "stages: []\ninstance: instance_key\n",
		"child/grandchild/entities.yaml": "work:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n",
		"child/grandchild/events.yaml":   "micro.started:\n",
		"child/grandchild/nodes.yaml":    "worker:\n  execution_type: system_node\n  subscribes_to: [micro.started]\n  produces: [micro.started]\n",
	})
	source := semanticview.Wrap(bundle)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	instance := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "child/grandchild", "inst-1", eventBusTestRunID)
	routes, err := rt.PubsubReceiverDefinitions(eventBusTestRunID, instance, []string{instance.InstancePath + "/micro.started"})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("compiled receiver definitions = %#v, want 1 route", routes)
	}
	if instance.ScopeKey != "child/grandchild" || instance.InstanceID != "inst-1" {
		t.Fatalf("compiled receiver identity = %#v, want exact nested scope/key", instance)
	}
	node, isNode := routes[0].Recipient.Node()
	if !isNode || routes[0].Path != instance.InstancePath || node.FlowPath() != "child/grandchild" || routes[0].LocalizedEvent != "micro.started" {
		t.Fatalf("compiled subscriber = %#v, want exact nested handler/local event", routes[0])
	}

}
