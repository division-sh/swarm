package bus_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
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
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/flowownedprojectagent"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
)

func TestFlowOwnedProjectAgentTemplateRoutesFollowCanonicalOwnerLifecycle(t *testing.T) {
	source := flowownedprojectagent.LoadSource(t, runtimecontracts.FlowModeTemplate, false)
	declarations := semanticview.AgentDeclarationsForOwner(source, "support")
	if len(declarations) != 1 || declarations[0].Source.Layer != "project" {
		t.Fatalf("support declarations = %#v, want one project-layer declaration", declarations)
	}
	req := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("support", "one"))}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	assert := func(want int) {
		t.Helper()
		got := routes.ResolveForRun(eventBusTestRunID, "support/one/work.requested")
		if len(got) != want {
			t.Fatalf("resolved project-agent routes = %#v, want %d", got, want)
		}
		if want == 1 && (got[0].Recipient.ID() != "public-worker-left" || got[0].AgentPlan.Name.Owner != declarations[0].OwnerURI) {
			t.Fatalf("resolved project-agent route = %#v, want exact canonical declaration %#v", got[0], declarations[0])
		}
	}
	if err := routes.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	assert(1)
	if err := routes.RemoveFlowInstanceRoute(req.Identity); err != nil {
		t.Fatalf("RemoveFlowInstanceRoute: %v", err)
	}
	assert(0)
	if err := routes.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("re-add FlowInstanceRoute: %v", err)
	}
	assert(1)

	restarted, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("restart EventBus: %v", err)
	}
	if err := restarted.PublishPersistedFlowInstanceRoute(req); err != nil {
		t.Fatalf("restore persisted flow-instance route: %v", err)
	}
	got := restarted.RouteTable().ResolveForRun(eventBusTestRunID, "support/one/work.requested")
	if len(got) != 1 || got[0].AgentPlan.Name.Owner != declarations[0].OwnerURI {
		t.Fatalf("restored project-agent route = %#v, want exact canonical owner", got)
	}
}

func TestFlowOwnedProjectAgentTemplateRoutesPreserveDistinctPhysicalDeclarations(t *testing.T) {
	source := flowownedprojectagent.LoadSource(t, runtimecontracts.FlowModeTemplate, true)
	declarations := semanticview.AgentDeclarationsForOwner(source, "support")
	if len(declarations) != 2 || declarations[0].OwnerURI == declarations[1].OwnerURI {
		t.Fatalf("support declarations = %#v, want two distinct physical owners", declarations)
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("support", "one"))}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	got := routes.ResolveForRun(eventBusTestRunID, "support/one/work.requested")
	if len(got) != 2 {
		t.Fatalf("resolved project-agent routes = %#v, want both physical declarations", got)
	}
	owners := map[string]bool{}
	ids := map[string]bool{}
	for _, subscriber := range got {
		owners[subscriber.AgentPlan.Name.Owner] = true
		ids[subscriber.Recipient.ID()] = true
	}
	if len(owners) != 2 || !ids["public-worker-left"] || !ids["public-worker-right"] {
		t.Fatalf("resolved owners/ids = %#v/%#v, want both exact declarations", owners, ids)
	}
}

func TestEventBusRemoveFlowInstanceDropsDerivedRoutes(t *testing.T) {
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	wantNode := testFlowNode(t, "review", "materialized-node")
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != wantNode.Key() {
		t.Fatalf("resolved subscribers after add = %#v", got)
	}
	if err := eb.RemoveFlowInstanceRoute(testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1"))); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
}

func TestEventBusFlowInstanceTemplateDerivesSubscriptionsFromHandlerKeys(t *testing.T) {
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID: "reviewer-{instance_id}",
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"task.started": {Emit: runtimecontracts.EmitSpec{Event: "task.started"}},
		},
	})
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("resolved subscribers = %#v, want materialized-node declaration identity", got)
	}
}

func TestEventBusRejectsInvalidExactNodeSubscriptionsBeforeRouteInstallation(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
	}{
		{name: "same scope", key: "child/task.done"},
		{name: "unresolved", key: "missing/task.done"},
		{name: "descendant", key: "child/grandchild/task.done"},
		{name: "full uri", key: "swarm://child/task.done"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := exactSubscriptionRouteSource(tc.key, nil)
			if _, err := runtimebus.DeriveRouteTable(source); err == nil || !strings.Contains(err.Error(), "must use a local event name") {
				t.Fatalf("DeriveRouteTable error = %v, want typed exact-node rejection", err)
			}
			if _, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{ContractBundle: source}); err == nil || !strings.Contains(err.Error(), "must use a local event name") {
				t.Fatalf("NewEventBusWithOptions error = %v, want typed exact-node rejection", err)
			}
		})
	}
}

func TestEventBusExactSubscriptionAdmissionPreservesLocalNodeAndSameScopeAgentRoutes(t *testing.T) {
	source := exactSubscriptionRouteSource("task.done", []string{"child/task.done"})
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := routes.ResolveForRun(eventBusTestRunID, "child/task.done")
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
	flowPath := strings.Trim(strings.TrimSpace(source.FlowPath("support")), "/")
	if flowPath == "" {
		t.Fatal("support flow path is empty")
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	resolved := routes.ResolveForRun(eventBusTestRunID, flowPath+"/work.requested")
	if len(resolved) != 1 {
		t.Fatalf("nested agent subscribers = %#v, want one physical subscriber", resolved)
	}
	if resolved[0].Recipient.ID() != "public-worker" || resolved[0].AgentPlan.Name.Owner != declarations[0].OwnerURI {
		t.Fatalf("nested agent subscriber = %#v, want exact canonical declaration owner %#v", resolved[0], declarations[0])
	}
}

func loadNestedPhysicalAgentRouteSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
	write(filepath.Join(root, "package.yaml"), `
name: nested-agent-route
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - {path: parent}
`)
	write(filepath.Join(root, "schema.yaml"), "name: nested-agent-route\n")
	write(filepath.Join(root, "parent", "package.yaml"), `
name: parent
version: "1.0.0"
packages:
  - {path: child}
`)
	write(filepath.Join(root, "parent", "child", "package.yaml"), `
name: child
version: "1.0.0"
flows:
  - {id: support, flow: support, mode: static}
`)
	flowRoot := filepath.Join(root, "parent", "child", "flows", "support")
	write(filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	write(filepath.Join(flowRoot, "schema.yaml"), `
name: support
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [work.requested]
`)
	write(filepath.Join(flowRoot, "events.yaml"), "work.requested: {}\n")
	write(filepath.Join(flowRoot, "agents.yaml"), `
worker:
  id: public-worker
  role: worker
  intent: {inline: Register one nested physical subscriber.}
  model: regular
  memory: false
  subscriptions: [work.requested]
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
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
					if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("orders", "one"))}); err != nil {
						t.Fatalf("AddFlowInstanceRoute: %v", err)
					}
					assertExactPackageRoute(t, routes.ResolveForRun(eventBusTestRunID, "orders/one/root.start"), "subscription", "flows/orders")
					return
				}
				resolved := routes.ResolveForRun(eventBusTestRunID, "orders/root.start")
				assertExactPackageRoute(t, resolved, "subscription", "flows/orders")
				assertExactPackageRoute(t, routes.ResolveForRun(eventBusTestRunID, "root.start"), "root_input_flow", "flows/orders")
			})
		}
	}
}

func assertExactPackageRoute(t *testing.T, subscribers []runtimebus.Subscriber, routeSource, packageKey string) {
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
		if node.PackageKey() != packageKey || node.FlowID() != "orders" || node.NodeID() != "shared" {
			t.Fatalf("subscriber owner = %q/%q/%q, want %q/orders/shared", node.PackageKey(), node.FlowID(), node.NodeID(), packageKey)
		}
		matches++
	}
	if matches != 1 {
		t.Fatalf("%s routes = %#v, want one exact package owner", routeSource, subscribers)
	}
}

func loadPackageCollisionRouteSource(t *testing.T, mode string, reverse bool) semanticview.Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := t.TempDir()
	packages := []string{"flows/orders/addon-a", "flows/orders/addon-b"}
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
	write(filepath.Join(root, "package.yaml"), `
name: exact-package-routes
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: `+packages[0]+`
  - path: `+packages[1]+`
flows:
  - id: orders
    flow: orders
    mode: `+mode+`
`)
	write(filepath.Join(root, "schema.yaml"), `
name: exact-package-routes
pins:
  inputs:
    events: [root.start]
`)
	write(filepath.Join(root, "events.yaml"), "root.start: {}\n")
	write(filepath.Join(root, "flows", "orders", "package.yaml"), "name: orders\nversion: \"1.0.0\"\nflows: []\n")
	write(filepath.Join(root, "flows", "orders", "schema.yaml"), `
name: orders
mode: `+mode+`
initial_state: active
states: [active, done]
terminal_states: [done]
pins:
  inputs:
    events: [root.start]
`)
	write(filepath.Join(root, "flows", "orders", "events.yaml"), "root.start: {}\naddon_a.start: {}\naddon_b.start: {}\n")
	write(filepath.Join(root, "flows", "orders", "nodes.yaml"), `
shared:
  id: shared
  execution_type: system_node
  event_handlers:
    root.start:
      advances_to: done
`)
	for _, name := range []string{"addon-a", "addon-b"} {
		dir := filepath.Join(root, "flows", "orders", name)
		write(filepath.Join(dir, "package.yaml"), "name: "+name+"\nversion: \"1.0.0\"\nflows: []\n")
		eventName := strings.ReplaceAll(name, "-", "_")
		write(filepath.Join(dir, "nodes.yaml"), `
shared:
  id: shared
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
			routes, err := runtimebus.DeriveRouteTable(exactSubscriptionRouteSource(authored, nil))
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			wantLocal := authored != "missing.*"
			if got := len(routes.ResolveForRun(eventBusTestRunID, "child/task.done")); (got == 1) != wantLocal {
				t.Fatalf("Resolve(child/task.done) count = %d, want local match %t", got, wantLocal)
			}
			if got := routes.ResolveForRun(eventBusTestRunID, "sibling/task.done"); len(got) != 0 {
				t.Fatalf("Resolve(sibling/task.done) = %#v, want no cross-scope subscriber", got)
			}
		})
	}
}

func TestEventBusTemplateAgentSameScopeExactAdmissionRendersConcreteInstanceRoute(t *testing.T) {
	bundle := routeMaterializationConfigVarBundle()
	flow := bundle.FlowTree.ByID["operating"]
	agent := flow.Agents["ceo"]
	agent.Subscriptions = []string{"operating/opco.product_initialization_requested"}
	flow.Agents["ceo"] = agent

	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	route := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	identity := testRunScopedFlowRoute(route)
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity:            identity,
		ActivationVariables: map[string]string{"vertical_id": route.InstanceID},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	got := rt.ResolveForRun(eventBusTestRunID, route.InstancePath+"/opco.product_initialization_requested")
	if len(got) != 1 || got[0].Recipient.LocalID() != "ceo" || got[0].AgentPlan.FlowInstance() != route.InstancePath {
		t.Fatalf("resolved subscribers = %#v, want concrete same-scope agent route", got)
	}
}

func exactSubscriptionRouteSource(nodeSubscription string, agentSubscriptions []string) semanticview.Source {
	node := runtimecontracts.SystemNodeContract{
		ID:            "listener",
		SubscribesTo:  []string{nodeSubscription},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{nodeSubscription: {}},
	}
	flow := runtimecontracts.FlowContractView{
		Path:   "child",
		Paths:  runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"listener": node},
	}
	if len(agentSubscriptions) > 0 {
		flow.Agents = map[string]runtimecontracts.AgentRegistryEntry{
			"observer": {ID: "observer", Subscriptions: append([]string(nil), agentSubscriptions...)},
		}
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &root.Children[0]},
		},
	}
	if len(agentSubscriptions) > 0 {
		ref := runtimecontracts.ContractURIRef{Kind: "agent", FlowID: "child", LocalID: "observer", Full: "test://fixture/child/observer"}
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{"child/observer": ref}
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{ref.Full: ref}
	}
	return semanticview.Wrap(bundle)
}

type routePersistenceTestStore struct {
	routes           map[string]runtimebus.FlowInstanceRouteRecord
	flowInstances    []runtimebus.ActiveFlowInstanceDescriptor
	targetOwners     []runtimebus.ActiveTargetDescriptor
	stagedRoutes     []runtimebus.FlowInstanceRouteRecord
	deliveries       map[string][]string
	upsertErr        error
	deleteErr        error
	rollbackCalls    []string
	deleteCalls      []runtimeflowidentity.RunScopedFlowInstance
	replaceCalls     []runtimeflowidentity.RunScopedFlowInstance
	upsertCalls      int
	upsertAfterWrite bool
}

func (s *routePersistenceTestStore) ListSelectedRunTargetOwners(context.Context, string) ([]runtimebus.ActiveTargetDescriptor, error) {
	return append([]runtimebus.ActiveTargetDescriptor(nil), s.targetOwners...), nil
}

func (s *routePersistenceTestStore) ListActiveFlowInstanceDescriptors(_ context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	out := append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...)
	for idx := range out {
		if strings.TrimSpace(out[idx].RunID) == "" {
			out[idx].RunID = strings.TrimSpace(runID)
		}
		if out[idx].BundleHash == "" {
			out[idx].BundleHash = authorActivityTestBundleHash
			out[idx].BundleSource = authorActivityTestBundleSource
			out[idx].WorkflowVersion = "1.0.0"
		}
	}
	return out, nil
}

func (s *routePersistenceTestStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := s.InsertEventDeliveryRoutes(ctx, command.Commit.Event.ID(), command.Commit.DeliveryRoutes); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	return runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}, nil
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

func (s *routePersistenceTestStore) UpsertFlowInstanceRoute(_ context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	s.upsertCalls++
	s.stagedRoutes = append(s.stagedRoutes, route)
	if s.routes == nil {
		s.routes = map[string]runtimebus.FlowInstanceRouteRecord{}
	}
	s.routes[route.Identity.Key()] = route
	if s.upsertAfterWrite && s.upsertErr != nil {
		return s.upsertErr
	}
	if s.upsertErr != nil {
		delete(s.routes, route.Identity.Key())
		return s.upsertErr
	}
	return nil
}

func (s *routePersistenceTestStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.RunScopedFlowInstance,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	s.replaceCalls = append(s.replaceCalls, identity)
	for key, route := range s.routes {
		if route.Identity == identity {
			delete(s.routes, key)
		}
	}
	for _, route := range routes {
		if err := s.UpsertFlowInstanceRoute(ctx, route); err != nil {
			return err
		}
	}
	return nil
}

func (s *routePersistenceTestStore) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) error {
	before := make(map[string]runtimebus.FlowInstanceRouteRecord, len(s.routes))
	for key, route := range s.routes {
		before[key] = route
	}
	for _, set := range sets {
		if err := s.ReplaceFlowInstanceRouteRecords(ctx, set.Identity, set.Routes); err != nil {
			s.routes = before
			return err
		}
	}
	return nil
}

func (s *routePersistenceTestStore) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestEventBusPublishPersistedFlowInstanceRouteDoesNotRewritePersistence(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")),
	}
	if err := eb.PublishPersistedFlowInstanceRoute(req); err != nil {
		t.Fatalf("PublishPersistedFlowInstanceRoute: %v", err)
	}
	if store.upsertCalls != 0 || len(store.routes) != 0 {
		t.Fatalf("route recovery rewrote persistence: calls=%d routes=%#v", store.upsertCalls, store.routes)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("restored route subscribers = %#v, want materialized-node declaration identity", got)
	}
}

func TestEventBusStageFlowInstanceRouteKeepsPublicationManifestInvisibleUntilReadiness(t *testing.T) {
	store := &routePersistenceTestStore{}
	bundle := routeMaterializationConfigVarBundle()
	source := semanticview.Wrap(bundle)
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		ContractBundle: source,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
		return nil
	}))
	route := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	identity := testRunScopedFlowRoute(route)
	store.targetOwners = []runtimebus.ActiveTargetDescriptor{{
		ID: "operating-owner", FlowInstance: route.InstancePath, EntityID: runtimeflowidentity.EntityID(route.InstancePath),
	}}
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}
	stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
	if err := eb.StageFlowInstanceRouteContext(stageCtx, req); err != nil {
		t.Fatalf("StageFlowInstanceRouteContext: %v", err)
	}
	if len(store.routes) == 0 {
		t.Fatal("staged route was not persisted")
	}
	if eb.HasFlowInstanceRoute(identity) {
		t.Fatal("staged route became process-visible before readiness")
	}
	agentIdentity, admission := routeMaterializationAgentRoute(t, source, identity)
	agentPlan, err := agentIdentity.Plan()
	if err != nil {
		t.Fatalf("project materialized agent plan: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		Identity: agentIdentity, EntityID: runtimeflowidentity.EntityID(route.InstancePath),
	})
	runtimebustest.SubscribeIdentity(t, eb, agentIdentity, admission)
	defer runtimebustest.UnsubscribeIdentity(eb, agentIdentity)
	instanceEnvelope := events.EventEnvelope{
		EntityID: runtimeflowidentity.EntityID(route.InstancePath), FlowInstance: route.InstancePath,
	}
	before := eventtest.RunCreatingRootIngress(
		eventtest.UUID("event-before-runtime-readiness"),
		events.EventType("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested"),
		"", "", nil, 0, eventBusTestRunID, "", instanceEnvelope, time.Time{},
	)
	if err := eb.Publish(context.Background(), before); err != nil {
		t.Fatalf("Publish before readiness: %v", err)
	}
	if got := store.deliveries[before.ID()]; len(got) != 0 {
		t.Fatalf("pre-readiness delivery recipients = %#v, want none", got)
	}
	if err := eb.PublishPersistedFlowInstanceRoute(req); err != nil {
		t.Fatalf("PublishPersistedFlowInstanceRoute: %v", err)
	}
	resolved := eb.RouteTable().ResolveForRun(eventBusTestRunID, "operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
	if len(resolved) != 1 || resolved[0].AgentPlan != agentPlan {
		t.Fatalf("published agent route = %#v, want exact identity %s", resolved, agentIdentity)
	}
	after := eventtest.RunCreatingRootIngress(
		eventtest.UUID("event-after-runtime-readiness"),
		events.EventType("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested"),
		"", "", nil, 0, eventBusTestRunID, "", instanceEnvelope, time.Time{},
	)
	if err := eb.Publish(context.Background(), after); err != nil {
		t.Fatalf("Publish after readiness: %v", err)
	}
	got := store.deliveries[after.ID()]
	if len(got) != 1 || got[0] != "ceo" {
		t.Fatalf("post-readiness delivery recipients = %#v, want exact instantiated agent", got)
	}
}

func TestEventBusStageFlowInstanceRoutePersistsCompleteCrossInstanceObserverTopology(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		WorkerMode:               "template",
		ProducerMode:             "template",
		ProducerStaticDescendant: true,
		ObserveGrant:             "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	sourceRoute := runtimeflowidentity.DeriveRoute("producer", "source-1")
	consumerRoute := runtimeflowidentity.DeriveRoute("worker", "consumer-1")
	sourceIdentity := testRunScopedFlowRoute(sourceRoute)
	consumerIdentity := testRunScopedFlowRoute(consumerRoute)
	store := &routePersistenceTestStore{
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
			{InstanceID: sourceRoute.InstanceID, FlowInstance: sourceRoute.InstancePath, FlowTemplate: "producer"},
			{InstanceID: consumerRoute.InstanceID, FlowInstance: consumerRoute.InstancePath, FlowTemplate: "worker"},
			{InstanceID: "foreign-1", FlowInstance: "foreign/foreign-1", FlowTemplate: "foreign"},
		},
	}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
	if err := eb.StageFlowInstanceRouteContext(stageCtx, runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: sourceIdentity,
	}); err != nil {
		t.Fatalf("StageFlowInstanceRouteContext: %v", err)
	}
	var observerRouteFound bool
	workerNode := testPackageNode(t, "flows/worker", "worker", "worker-listener")
	for _, route := range store.stagedRoutes {
		if route.Identity == consumerIdentity &&
			route.EventPattern == "producer/source-1/task.done" &&
			route.SubscriberID == workerNode.Key() {
			observerRouteFound = true
			break
		}
	}
	if !observerRouteFound {
		t.Fatalf("staged routes = %#v, want consumer-owned cross-instance observer route", store.stagedRoutes)
	}
	if eb.HasFlowInstanceRoute(sourceIdentity) || eb.HasFlowInstanceRoute(consumerIdentity) {
		t.Fatal("complete staged topology became process-visible before readiness")
	}
}

func TestEventBusExactFlowInstanceRouteTopologyRemovesObsoleteObserverRows(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		WorkerMode:               "template",
		ProducerMode:             "template",
		ProducerStaticDescendant: true,
		ObserveGrant:             "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	sourceRoute := runtimeflowidentity.DeriveRoute("producer", "source-1")
	consumerRoute := runtimeflowidentity.DeriveRoute("worker", "consumer-1")
	sourceIdentity := testRunScopedFlowRoute(sourceRoute)
	consumerIdentity := testRunScopedFlowRoute(consumerRoute)
	for _, removed := range []runtimeflowidentity.RunScopedFlowInstance{sourceIdentity, consumerIdentity} {
		t.Run(removed.Route.ScopeKey, func(t *testing.T) {
			store := &routePersistenceTestStore{
				flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
					{InstanceID: sourceRoute.InstanceID, FlowInstance: sourceRoute.InstancePath, FlowTemplate: "producer"},
					{InstanceID: consumerRoute.InstanceID, FlowInstance: consumerRoute.InstancePath, FlowTemplate: "worker"},
				},
			}
			eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			if err := eb.RouteTable().AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: sourceIdentity}); err != nil {
				t.Fatalf("publish source route: %v", err)
			}
			if err := eb.RouteTable().AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: consumerIdentity}); err != nil {
				t.Fatalf("publish consumer route: %v", err)
			}
			stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
			if err := eb.StageFlowInstanceRouteContext(stageCtx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: sourceIdentity}); err != nil {
				t.Fatalf("stage complete topology: %v", err)
			}
			if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "producer/source-1/task.done"); len(got) != 1 {
				t.Fatalf("observer route before removal = %#v, want one", got)
			}

			remaining := store.flowInstances[:0]
			for _, descriptor := range store.flowInstances {
				if descriptor.FlowInstance != removed.Route.InstancePath {
					remaining = append(remaining, descriptor)
				}
			}
			store.flowInstances = remaining
			if err := eb.RemoveFlowInstanceRouteContext(context.Background(), removed); err != nil {
				t.Fatalf("RemoveFlowInstanceRouteContext: %v", err)
			}
			if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "producer/source-1/task.done"); len(got) != 0 {
				t.Fatalf("observer route after %s removal = %#v, want none", removed.Route.ScopeKey, got)
			}
			workerNode := testPackageNode(t, "flows/worker", "worker", "worker-listener")
			for _, route := range store.routes {
				if route.EventPattern == "producer/source-1/task.done" &&
					route.SubscriberID == workerNode.Key() {
					t.Fatalf("obsolete observer row survived %s removal: %#v", removed.Route.ScopeKey, route)
				}
			}
		})
	}
}

func TestEventBusStageFlowInstanceRouteRejectsForeignSemanticSourceDescriptorsBeforeReplacement(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		WorkerMode:   "template",
		ProducerMode: "template",
	})
	currentRoute := runtimeflowidentity.DeriveRoute("producer", "current")
	foreignRoute := runtimeflowidentity.DeriveRoute("producer", "foreign")
	current := testRunScopedFlowRoute(currentRoute)
	store := &routePersistenceTestStore{
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
			{InstanceID: currentRoute.InstanceID, FlowInstance: currentRoute.InstancePath, FlowTemplate: "producer"},
			{
				InstanceID: foreignRoute.InstanceID, FlowInstance: foreignRoute.InstancePath, FlowTemplate: "producer",
				BundleHash:   "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
				BundleSource: "ephemeral", WorkflowVersion: "1.0.0",
			},
		},
	}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	prior := runtimebus.FlowInstanceRouteRecord{
		Identity: current, EventPattern: "producer/current/prior.event",
		SubscriberType: "agent", SubscriberID: "prior-agent", SourceFlow: "producer",
	}
	store.routes = map[string]runtimebus.FlowInstanceRouteRecord{"prior": prior}
	stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
	err = eb.StageFlowInstanceRouteContext(stageCtx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: current})
	if err == nil || !strings.Contains(err.Error(), "semantic source does not match") {
		t.Fatalf("StageFlowInstanceRouteContext error = %v, want foreign semantic-source rejection", err)
	}
	if len(store.replaceCalls) != 0 {
		t.Fatalf("route owners replaced across foreign semantic-source rejection: %#v", store.replaceCalls)
	}
	if len(store.stagedRoutes) != 0 || len(store.routes) != 1 || store.routes["prior"] != prior {
		t.Fatalf("route truth changed across foreign semantic-source rejection: staged=%#v routes=%#v", store.stagedRoutes, store.routes)
	}
}

func TestEventBusStageFlowInstanceRouteAcceptsExactEmptyRouteSet(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource("observer", runtimecontracts.SystemNodeContract{ID: "observer-{instance_id}"})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("observer", "inst-1"))
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
	}
	stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
	if err := eb.StageFlowInstanceRouteContext(stageCtx, req); err != nil {
		t.Fatalf("StageFlowInstanceRouteContext: %v", err)
	}
	if store.upsertCalls != 0 {
		t.Fatalf("empty route set persistence calls = %d, want none", store.upsertCalls)
	}
	if eb.HasFlowInstanceRoute(identity) {
		t.Fatal("empty staged route became process-visible before readiness")
	}
	if err := eb.PublishPersistedFlowInstanceRoute(req); err != nil {
		t.Fatalf("PublishPersistedFlowInstanceRoute: %v", err)
	}
	if !eb.HasFlowInstanceRoute(identity) {
		t.Fatal("exact empty route topology was not published as process-ready")
	}
}

func TestEventBusFlowInstanceRouteRejectsUnknownCanonicalTemplateWithoutMutation(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource("known", runtimecontracts.SystemNodeContract{
		ID:           "known-{instance_id}",
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("unknown", "inst-1"))
	err = eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity})
	if err == nil || !strings.Contains(err.Error(), `route template "unknown" not found`) {
		t.Fatalf("AddFlowInstanceRoute error = %v, want unknown canonical template", err)
	}
	if eb.HasFlowInstanceRoute(identity) || len(store.routes) != 0 || store.upsertCalls != 0 {
		t.Fatalf("unknown template mutated route state: owner=%v routes=%#v upserts=%d", eb.HasFlowInstanceRoute(identity), store.routes, store.upsertCalls)
	}
}

func (s *routePersistenceTestStore) RollbackFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	s.rollbackCalls = append(s.rollbackCalls, identity.Key())
	delete(s.routes, identity.Key())
	return nil
}

func (s *routePersistenceTestStore) DeleteFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	s.deleteCalls = append(s.deleteCalls, identity)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.routes, identity.Key())
	return nil
}

func TestEventBusFlowInstanceRouteIdentityOwnerRejectsMismatchedExplicitPath(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	installedRoute := runtimeflowidentity.DeriveRoute("review", "inst-1")
	installed := testRunScopedFlowRoute(installedRoute)
	req := runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: installed,
	}
	if err := eb.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(req); err != nil {
		t.Fatalf("exact AddFlowInstanceRoute replay: %v", err)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("routes after exact replay = %#v, want one installed owner route", got)
	}
	replaceCalls := len(store.replaceCalls)
	mismatched := testUncheckedRunScopedFlowRoute(runtimeflowidentity.StoredRoute("worker", "other", installedRoute.InstancePath))
	if eb.RouteTable().HasFlowInstanceRoute(mismatched) {
		t.Fatal("HasFlowInstanceRoute accepted a different identity at the installed path")
	}
	mismatchedReq := req
	mismatchedReq.Identity = mismatched
	if err := eb.AddFlowInstanceRoute(mismatchedReq); err == nil || !strings.Contains(err.Error(), "identity is inconsistent") {
		t.Fatalf("mismatched AddFlowInstanceRoute error = %v, want complete-owner conflict", err)
	}
	if err := eb.RemoveFlowInstanceRoute(mismatched); err == nil || !strings.Contains(err.Error(), "identity is inconsistent") {
		t.Fatalf("mismatched RemoveFlowInstanceRoute error = %v, want complete-owner conflict", err)
	}
	if len(store.replaceCalls) != replaceCalls {
		t.Fatalf("persistence replacement calls = %#v, want unchanged after rejected removal", store.replaceCalls)
	}
	if !eb.RouteTable().HasFlowInstanceRoute(installed) {
		t.Fatal("installed identity disappeared after mismatched add/remove")
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "review", "materialized-node").Key() {
		t.Fatalf("routes after mismatched add/remove = %#v, want owner authority unchanged", got)
	}
	normalizedRemoval := runtimeflowidentity.RunScopedFlowInstance{
		RunID: " " + eventBusTestRunID + " ",
		Route: runtimeflowidentity.Route{
			ScopeKey:     " /review/ ",
			InstanceID:   " inst-1 ",
			InstancePath: " /review/inst-1/ ",
		},
	}
	if err := eb.RemoveFlowInstanceRoute(normalizedRemoval); err != nil {
		t.Fatalf("RemoveFlowInstanceRoute owner: %v", err)
	}
	if len(store.replaceCalls) != replaceCalls+1 || store.replaceCalls[len(store.replaceCalls)-1] != installed {
		t.Fatalf("persistence replacement calls = %#v, want one canonical owner removal after setup", store.replaceCalls)
	}
	if err := eb.RemoveFlowInstanceRoute(normalizedRemoval); err != nil {
		t.Fatalf("exact RemoveFlowInstanceRoute replay: %v", err)
	}
	if len(store.replaceCalls) != replaceCalls+2 {
		t.Fatalf("persistence replacement calls after absent replay = %#v, want exact replay reconciliation", store.replaceCalls)
	}
}

func (s *routePersistenceTestStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.RunScopedFlowInstance, error) {
	out := make([]runtimeflowidentity.RunScopedFlowInstance, 0, len(s.routes))
	for _, route := range s.routes {
		out = append(out, route.Identity)
	}
	return out, nil
}

func TestEventBusFlowInstanceRoutesPersistAcrossAddAndRemove(t *testing.T) {
	store := &routePersistenceTestStore{}
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if _, ok := store.routes[testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")).Key()]; !ok {
		t.Fatalf("persisted routes = %#v, want review/inst-1", store.routes)
	}
	if err := eb.RemoveFlowInstanceRoute(testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1"))); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes after remove = %#v, want none", store.routes)
	}
}

func TestEventBusAddFlowInstanceRouteDoesNotPublishWhenTopologyCommitFails(t *testing.T) {
	store := &routePersistenceTestStore{
		upsertErr:        context.DeadlineExceeded,
		upsertAfterWrite: true,
		deleteErr:        context.Canceled,
	}
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	err = eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1")),
	})
	if err == nil {
		t.Fatal("expected AddFlowInstanceRoute to fail")
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes after rollback = %#v, want none", store.routes)
	}
	if len(store.rollbackCalls) != 0 {
		t.Fatalf("external rollback calls = %#v, want none because the named operation owns rollback", store.rollbackCalls)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "review/inst-1/task.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after failed add = %#v, want none", got)
	}
}

func TestEventBusFlowInstanceRoutePersistsAndDeliversRenderedActivationConfigSubscriber(t *testing.T) {
	store := &routePersistenceTestStore{}
	bundle := routeMaterializationConfigVarBundle()
	source := semanticview.Wrap(bundle)
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		ContractBundle: source,
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
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	route, ok := store.routes[identity.Key()]
	if !ok {
		t.Fatalf("persisted routes = %#v, want operating instance route", store.routes)
	}
	if route.SubscriberID != "ceo" {
		t.Fatalf("persisted subscriber_id = %q, want stable declared ceo name", route.SubscriberID)
	}

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
	resolved := eb.RouteTable().ResolveForRun(eventBusTestRunID, "operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
	if len(resolved) != 1 || resolved[0].AgentPlan != agentPlan {
		t.Fatalf("active agent route = %#v, want exact identity %s", resolved, agentIdentity)
	}
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("event-rendered-route-delivery"),
		events.EventType("operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested"), "", "", nil, 0, eventBusTestRunID, "",
		events.EventEnvelope{EntityID: runtimeflowidentity.EntityID(routeIdentity.InstancePath), FlowInstance: routeIdentity.InstancePath}, time.Time{})

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.deliveries[evt.ID()]
	if len(got) != 1 || got[0] != "ceo" {
		t.Fatalf("delivery recipients = %#v, want stable declared ceo name", got)
	}
}

func TestEventBusRemoveNestedFlowInstanceDropsDerivedRoutes(t *testing.T) {
	source := routeMaterializationNodeSource("child/grandchild", runtimecontracts.SystemNodeContract{
		ID:           "worker-{instance_id}",
		Produces:     []string{"micro.started"},
		SubscribesTo: []string{"micro.started"},
	})
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")),
	}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "child/grandchild/inst-1/micro.started"); len(got) != 1 || got[0].Recipient.ID() != testFlowNode(t, "child/grandchild", "materialized-node").Key() {
		t.Fatalf("resolved subscribers after add = %#v", got)
	}
	if err := eb.RemoveFlowInstanceRoute(testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1"))); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if got := eb.RouteTable().ResolveForRun(eventBusTestRunID, "child/grandchild/inst-1/micro.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
}

func TestRouteTableConcreteTemplateInstanceNodeSubscriberResolvesBeforeDeliveryPlanning(t *testing.T) {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
				Event: "opco.product_initialization_requested",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"lifecycle-orchestrator": {
				ID:            "lifecycle-orchestrator",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"opco.product_initialization_requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"opco.product_initialization_requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{operating}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": {
				Mode: "template",
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
					Event: "opco.product_initialization_requested",
				},
			},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile route-table test semantics: %v", err)
	}
	source := semanticview.Wrap(bundle)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("operating", "inst-1"))}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	got := rt.ResolveForRun(eventBusTestRunID, "operating/inst-1/opco.product_initialization_requested")
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one lifecycle-orchestrator route", got)
	}
	if got[0].Recipient.LocalID() != "lifecycle-orchestrator" || !got[0].Recipient.IsNode() || got[0].Path != "operating/inst-1" {
		t.Fatalf("resolved subscriber = %#v, want node lifecycle-orchestrator at operating/inst-1", got[0])
	}
}

func TestRouteTableFlowInstanceRouteKeepsAgentNameStableAcrossActivationConfig(t *testing.T) {
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(routeMaterializationConfigVarBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	firstRoute := runtimeflowidentity.DeriveRoute("operating", "11111111-1111-4111-8111-111111111111")
	identity := testRunScopedFlowRoute(firstRoute)
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: identity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	got := rt.ResolveForRun(eventBusTestRunID, "operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
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
	siblingIdentity := testRunScopedFlowRoute(siblingRoute)
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: siblingIdentity,
		ActivationVariables: map[string]string{
			"vertical_id": "11111111-1111-4111-8111-111111111111",
		},
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute sibling: %v", err)
	}
	sibling := rt.ResolveForRun(eventBusTestRunID, "operating/22222222-2222-4222-8222-222222222222/opco.product_initialization_requested")
	if len(sibling) != 1 || sibling[0].Recipient.LocalID() != got[0].Recipient.LocalID() ||
		sibling[0].AgentPlan.FlowInstance() != siblingRoute.InstancePath ||
		sibling[0].AgentPlan == got[0].AgentPlan {
		t.Fatalf("resolved sibling subscriber = %#v, want same slug with distinct concrete identity from %#v", sibling, got[0])
	}
	routes := rt.MaterializedRoutes(identity)
	if len(routes) != 1 || routes[0].SubscriberID != "ceo" {
		t.Fatalf("materialized routes = %#v, want stable declared ceo subscriber", routes)
	}
}

func TestRouteTableExplicitAgentNameChangesPublicNameNotScopedCoordinate(t *testing.T) {
	bundle := routeMaterializationConfigVarBundle()
	flow := bundle.FlowTree.ByID["operating"]
	entry := flow.Agents["ceo"]
	entry.ID = "executive"
	flow.Agents["ceo"] = entry
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
		if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(identity)}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", identity.InstancePath, err)
		}
		got := routes.ResolveForRun(eventBusTestRunID, identity.InstancePath+"/opco.product_initialization_requested")
		if len(got) != 1 || got[0].Recipient.LocalID() != "executive" || got[0].AgentPlan.FlowInstance() != identity.InstancePath {
			t.Fatalf("resolved %s = %#v, want executive on exact instance", identity.InstancePath, got)
		}
		concrete = append(concrete, got[0].AgentPlan)
	}
	if concrete[0] == concrete[1] {
		t.Fatalf("sibling concrete identities collapsed: %#v", concrete)
	}
}

func routeMaterializationNodeSource(flowID string, node runtimecontracts.SystemNodeContract) semanticview.Source {
	eventsByName := make(map[string]runtimecontracts.EventCatalogEntry)
	for _, eventType := range runtimecontracts.EffectiveSystemNodeSubscriptions(node) {
		if eventType = strings.TrimSpace(eventType); eventType != "" {
			eventsByName[eventType] = runtimecontracts.EventCatalogEntry{}
		}
	}
	for _, eventType := range runtimecontracts.EffectiveSystemNodeProduces(node) {
		if eventType = strings.TrimSpace(eventType); eventType != "" {
			eventsByName[eventType] = runtimecontracts.EventCatalogEntry{}
		}
	}
	flow := runtimecontracts.FlowContractView{
		Path:   flowID,
		Paths:  runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "template"},
		Events: eventsByName,
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"materialized-node": node},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{flowID: {Mode: "template"}},
	})
}

func routeMaterializationConfigVarBundle() *runtimecontracts.WorkflowContractBundle {
	agentRef := runtimecontracts.ContractURIRef{
		Kind: "agent", FlowID: "operating", LocalID: "ceo",
		Full: "test://route-materialization/operating/ceo",
	}
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "opco.product_initialization_requested"}}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"ceo": {
				Type:          "generic",
				Role:          "ceo",
				Subscriptions: []string{"opco.product_initialization_requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{operating}}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "route-materialization",
			Version: "1.0.0",
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"operating/ceo": agentRef,
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{agentRef.Full: agentRef},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "opco.product_initialization_requested"}}},
				},
			},
		},
	}
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

func TestRouteTableTemplateOutputPinWildcardSubscriberResolvesThroughDerivedInstance(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := t.TempDir()
	writeRoutingFixtureFile(t, root, "package.yaml", `
name: template-output-observer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: component-scaffold
    flow: component-scaffold
    mode: template
`)
	writeRoutingFixtureFile(t, root, "schema.yaml", "name: template-output-observer\n")
	writeRoutingFixtureFile(t, root, "nodes.yaml", `
operating-accumulator:
  id: operating-accumulator
  execution_type: system_node
  subscribes_to: ["component-scaffold/*/component.scaffolded"]
  event_handlers:
    "component-scaffold/*/component.scaffolded": {}
`)
	writeRoutingFixtureFile(t, root, "flows/component-scaffold/schema.yaml", `
name: component-scaffold
mode: template
pins:
  outputs:
    events: [component.scaffolded]
`)
	writeRoutingFixtureFile(t, root, "flows/component-scaffold/events.yaml", "component.scaffolded: {}\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load template-output observer fixture: %v", err)
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("component-scaffold", "component-a"))
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	got := rt.ResolveForRun(eventBusTestRunID, "component-scaffold/component-a/component.scaffolded")
	if len(got) != 1 {
		t.Fatalf("resolved subscribers = %#v, want one operating-accumulator route", got)
	}
	if got[0].Recipient.LocalID() != "operating-accumulator" || !got[0].Recipient.IsNode() || got[0].MatchPattern != "component-scaffold/*/component.scaffolded" {
		t.Fatalf("resolved subscriber = %#v, want operating-accumulator wildcard route", got[0])
	}

	if err := rt.RemoveFlowInstanceRoute(identity); err != nil {
		t.Fatalf("RemoveFlowInstanceRoute: %v", err)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "component-scaffold/component-a/component.scaffolded"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "component-scaffold/component-b/component.scaffolded"); len(got) != 0 {
		t.Fatalf("resolved subscribers for never-added instance = %#v, want none", got)
	}
}

func TestDeriveRouteTable_InputPinsDoNotAutoWireFromProducerOutput(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "producer/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want none for retired sibling auto-wire", got)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(scan.requested) = %#v, want none", got)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "discovery/scan.requested"); len(got) != 1 || got[0].Recipient.LocalID() != "scan-orchestrator" {
		t.Fatalf("Resolve(discovery/scan.requested) = %#v, want scan-orchestrator local input route", got)
	}
}

func TestDeriveRouteTable_HandlerOnlyInputPinsDoNotAutoWireFromProducerOutput(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "producer",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "scan.requested"}}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID: "consumer-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"consumer-node": {
					"scan.requested": {},
				},
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "producer/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want none for retired sibling auto-wire", got)
	}
	got := rt.ResolveForRun(eventBusTestRunID, "consumer/scan.requested")
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
			got := rt.ResolveForRun(eventBusTestRunID, tc.eventType)
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
			flowPath:   "",
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
			got := rt.ResolveForRun(eventBusTestRunID, tc.eventType)
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

func TestDeriveRouteTable_AmbiguousInputPinsFailClosedWithoutEscapeHatch(t *testing.T) {
	producerA := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer_a", Flow: "producer_a"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "ticket.ready"}}},
			},
		},
		Path: "producer_a",
	}
	producerB := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer_b", Flow: "producer_b"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "ticket.ready"}}},
			},
		},
		Path: "producer_b",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "ticket.ready"}}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID: "consumer-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.ready": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producerA, producerB, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer_a": &root.Children[0],
				"producer_b": &root.Children[1],
				"consumer":   &root.Children[2],
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"consumer-node": {
					"ticket.ready": {},
				},
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "producer_a/ticket.ready"); len(got) != 0 {
		t.Fatalf("Resolve(producer_a/ticket.ready) = %#v, want none", got)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "producer_b/ticket.ready"); len(got) != 0 {
		t.Fatalf("Resolve(producer_b/ticket.ready) = %#v, want none", got)
	}
}

func TestDeriveRouteTable_InputPinsStayLocalWithoutExternalProducer(t *testing.T) {
	scoring := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "scoring", Flow: "scoring"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "score.dimension_complete"}}},
			},
		},
		Path: "scoring",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				ID:           "scoring-node",
				SubscribesTo: []string{"score.dimension_complete"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{scoring}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring": &root.Children[0],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.ResolveForRun(eventBusTestRunID, "scoring/score.dimension_complete")
	if len(got) != 1 || got[0].Recipient.LocalID() != "scoring-node" {
		t.Fatalf("Resolve(scoring/score.dimension_complete) = %#v, want scoring-node", got)
	}
	if got := rt.ResolveForRun(eventBusTestRunID, "score.dimension_complete"); len(got) != 0 {
		t.Fatalf("Resolve(score.dimension_complete) = %#v, want none", got)
	}
}

func TestDeriveRouteTable_NestedPackageConnectLocalizesWithinParentFlow(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, canonicalrouting.CopyNestedPackageConnect(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot),
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
	if got := rt.ResolveForRun(eventBusTestRunID, "child/grandchild/micro.done"); len(got) != 0 {
		t.Fatalf("direct descendant route = %#v, connect dispatch must own the boundary edge", got)
	}
	got := rt.ResolveForRun(eventBusTestRunID, "child/micro.done")
	if len(got) != 1 || got[0].Recipient.LocalID() != "child-aggregator" {
		t.Fatalf("Resolve(child/micro.done) = %#v, want receiver-local child-aggregator carrier", got)
	}
	if got[0].Path != "child" {
		t.Fatalf("receiver carrier path = %q, want child", got[0].Path)
	}
}

func writeRoutingFixtureFile(t testing.TB, root, relative, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create routing fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimPrefix(body, "\n")), 0o600); err != nil {
		t.Fatalf("write routing fixture %s: %v", relative, err)
	}
}

func TestDeriveRouteTable_NestedTemplateInstancesPersistSemanticScopeKey(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Path: "child/grandchild",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				SubscribesTo: []string{"micro.started"},
				Produces:     []string{"micro.started"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.started": {},
		},
	}
	child := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Path:     "child",
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "1.0.0"},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	firstRoute := runtimeflowidentity.DeriveRoute("child/grandchild", "inst-1")
	identity := testRunScopedFlowRoute(firstRoute)
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	routes := rt.MaterializedRoutes(identity)
	if len(routes) != 1 {
		t.Fatalf("MaterializedRoutes = %#v, want 1 route", routes)
	}
	if routes[0].Identity.Route.ScopeKey != "child/grandchild" {
		t.Fatalf("ScopeKey = %q, want child/grandchild", routes[0].Identity.Route.ScopeKey)
	}
	if routes[0].Identity.Route.InstanceID != "inst-1" {
		t.Fatalf("InstanceID = %q, want inst-1", routes[0].Identity.Route.InstanceID)
	}
	if routes[0].SourceFlow != "child/grandchild" {
		t.Fatalf("SourceFlow = %q, want child/grandchild", routes[0].SourceFlow)
	}

	store := &routePersistenceTestStore{flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{{
		InstanceID:    firstRoute.InstanceID,
		FlowInstance:  firstRoute.InstancePath,
		FlowTemplate:  "grandchild",
		AddressFields: map[string]string{"entity.account_id": "acct-1"},
	}}}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	secondRoute := runtimeflowidentity.DeriveRoute("child/grandchild", "inst-2")
	second := testRunScopedFlowRoute(secondRoute)
	stageCtx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})
	if err := eb.StageFlowInstanceRouteContext(stageCtx, runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: second,
	}); err != nil {
		t.Fatalf("stage second nested template instance: %v", err)
	}
	replaced := map[runtimeflowidentity.RunScopedFlowInstance]bool{}
	for _, route := range store.replaceCalls {
		replaced[route] = true
	}
	if !replaced[identity] || !replaced[second] {
		t.Fatalf("replaced nested route owners = %#v, want existing %s and included %s", store.replaceCalls, firstRoute.InstancePath, secondRoute.InstancePath)
	}

	store.flowInstances[0].FlowTemplate = "child"
	store.replaceCalls = nil
	store.stagedRoutes = nil
	err = eb.StageFlowInstanceRouteContext(stageCtx, runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: second,
	})
	if err == nil || !strings.Contains(err.Error(), "template child does not match route template grandchild") {
		t.Fatalf("mismatched nested descriptor error = %v, want exact template mapping rejection", err)
	}
	if len(store.replaceCalls) != 0 || len(store.stagedRoutes) != 0 {
		t.Fatalf("mismatched nested descriptor mutated routes: replacements=%#v staged=%#v", store.replaceCalls, store.stagedRoutes)
	}
}
