package bus_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestImportBoundaryWildcardScopesImportedPackageToOwnSubtreeByDefault(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("worker/task.done"); len(routes) != 1 || routes[0].ID != "worker-listener" {
		t.Fatalf("Resolve(worker/task.done) = %#v, want worker-listener", routes)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no sibling route without grant", routes)
	}
	if owners := source.RuntimeEventOwners("producer/task.done"); len(owners) != 0 {
		t.Fatalf("RuntimeEventOwners(producer/task.done) = %#v, want no sibling owner without grant", owners)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); ok {
		t.Fatal("NodeEventHandler(worker-listener, producer/task.done) matched ungranted sibling event")
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	local := eventtest.RootIngress("evt-worker-local", "worker/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), local); err != nil {
		t.Fatalf("Publish local: %v", err)
	}
	if got := store.deliveries["evt-worker-local"]; len(got) != 1 || got[0] != "worker-listener" {
		t.Fatalf("local persisted deliveries = %#v, want worker-listener", got)
	}
	sibling := eventtest.RootIngress("evt-producer-sibling", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), sibling); err != nil {
		t.Fatalf("Publish sibling: %v", err)
	}
	if got := store.deliveries["evt-producer-sibling"]; len(got) != 0 {
		t.Fatalf("sibling persisted deliveries = %#v, want none without grant", got)
	}
}

func TestImportBoundaryWildcardObserveGrantAddsNarrowSiblingCandidate(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 1 || routes[0].ID != "worker-listener" || routes[0].RouteSource != "import_boundary_wildcard_grant" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want worker-listener via grant", routes)
	}
	if owners := source.RuntimeEventOwners("producer/task.done"); len(owners) != 1 || owners[0] != "worker-listener" {
		t.Fatalf("RuntimeEventOwners(producer/task.done) = %#v, want worker-listener", owners)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); !ok {
		t.Fatal("NodeEventHandler(worker-listener, producer/task.done) did not resolve through grant")
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress("evt-granted-sibling", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "worker-listener" {
		t.Fatalf("routed recipients = %#v, want worker-listener", plan.RoutedRecipients)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish granted sibling: %v", err)
	}
	if got := store.deliveries["evt-granted-sibling"]; len(got) != 1 || got[0] != "worker-listener" {
		t.Fatalf("persisted deliveries = %#v, want worker-listener", got)
	}

}

func TestImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: flows/producer\n" +
			"          events: [task.done]\n",
	})

	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Matches) != 0 || len(relations.Issues) != 1 {
		t.Fatalf("typed pub/sub relations = %#v, want one ambiguity and no edge authority", relations)
	}
	if issue := relations.Issues[0]; issue.Failure != semanticview.TypedPubSubFailureAuthorizationAmbiguous || len(issue.Authorizations) != 2 {
		t.Fatalf("typed pub/sub issue = %#v, want two-proof authorization ambiguity", issue)
	} else if issue.Producer.ID != "" || issue.Event.EventKey() != "producer/task.done" {
		t.Fatalf("typed pub/sub issue = %#v, want producerless declared-event authority", issue)
	}

	topology := routingtopology.Build(source)
	if len(topology.Edges) != 0 || len(topology.Issues) != 1 || topology.Issues[0].Failure != semanticview.TypedPubSubFailureAuthorizationAmbiguous {
		t.Fatalf("routing topology = %#v, want ambiguity issue and no edge", topology)
	}

	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	if !importBoundaryWildcardReportContains(report.Errors(), semanticview.TypedPubSubFailureAuthorizationAmbiguous, "multiple distinct import authorization proofs") {
		t.Fatalf("verify errors = %#v, want typed pub/sub authorization hard invalidity", report.Errors())
	}
	if importBoundaryWildcardReportContains(report.Findings, "event_consumer_exists", "producer/task.done") {
		t.Fatalf("verify findings = %#v, ambiguity should replace the generic missing-consumer warning", report.Findings)
	}
	validationOpts := swruntime.DefaultWorkflowContractValidationOptions(nil)
	validationOpts.CheckMCPReachable = false
	validationOpts.FatalBootWarnings = false
	validationOpts.FatalToolImplementationWarning = false
	if _, err := swruntime.ValidateWorkflowContractSurface(context.Background(), source, validationOpts); err == nil || !strings.Contains(err.Error(), semanticview.TypedPubSubFailureAuthorizationAmbiguous) {
		t.Fatalf("runtime contract validation error = %v, want event.publish runtime-context admission failure", err)
	}

	routes, err := runtimebus.DeriveRouteTable(source)
	if routes != nil {
		t.Fatalf("route table = %#v, want no runtime authority for ambiguous relation", routes)
	}
	var authorizationErr *runtimebus.TypedPubSubAuthorizationError
	if !errors.As(err, &authorizationErr) || len(authorizationErr.Issues) != 1 {
		t.Fatalf("DeriveRouteTable error = %v, want typed pub/sub authorization error", err)
	}
	if eb, err := runtimebus.NewEventBusWithOptions(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source}); err == nil || eb != nil {
		t.Fatalf("NewEventBusWithOptions = (%#v, %v), want fail-closed startup", eb, err)
	}
	validSource := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	prebuiltRoutes, err := runtimebus.DeriveRouteTable(validSource)
	if err != nil {
		t.Fatalf("derive valid prebuilt route table: %v", err)
	}
	if eb, err := runtimebus.NewEventBusWithOptions(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source, RouteTable: prebuiltRoutes}); err == nil || eb != nil {
		t.Fatalf("NewEventBusWithOptions with prebuilt routes = (%#v, %v), want contract ambiguity to remain authoritative", eb, err)
	}
}

func TestImportBoundaryWildcardAuthorizationAmbiguityRetainsAuthoredProducerProof(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerAuthored: true,
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: flows/producer\n" +
			"          events: [task.done]\n",
	})

	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) != 1 || strings.TrimSpace(relations.Issues[0].Producer.ID) == "" {
		t.Fatalf("typed pub/sub relations = %#v, want one ambiguity with authored producer provenance", relations)
	}
	if routes, err := runtimebus.DeriveRouteTable(source); err == nil || routes != nil {
		t.Fatalf("DeriveRouteTable = (%#v, %v), want authored-producer ambiguity to remain fail closed", routes, err)
	}
}

func TestImportBoundaryWildcardExactDuplicateAuthorizationCollapsesAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n",
	})

	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) != 0 || len(relations.Matches) != 0 {
		t.Fatalf("typed pub/sub relations = %#v, want no conflict and no synthetic producer edge", relations)
	}
	topology := routingtopology.Build(source)
	if len(topology.Issues) != 0 || len(topology.Edges) != 0 {
		t.Fatalf("routing topology = %#v, want no conflict and no synthetic producer edge", topology)
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := routes.Resolve("producer/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want one deduplicated worker route", got)
	}
}

func TestImportBoundaryWildcardGrantFailsClosedWhenInvalid(t *testing.T) {
	cases := []struct {
		name        string
		grant       string
		wantMessage string
	}{
		{
			name:        "unknown source",
			grant:       "      observe:\n        - source: missing\n          events: [task.done]\n",
			wantMessage: "does not resolve",
		},
		{
			name:        "broad event",
			grant:       "      observe:\n        - source: producer\n          events: [\"**\"]\n",
			wantMessage: "unbounded wildcard",
		},
		{
			name:        "unknown event",
			grant:       "      observe:\n        - source: producer\n          events: [missing.done]\n",
			wantMessage: "does not match any event",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{ObserveGrant: tc.grant})
			report := bootverify.Run(context.Background(), source, bootverify.Options{})
			if !importBoundaryWildcardReportContains(report.Errors(), "flow_package_wildcard_observe_grant", tc.wantMessage) {
				t.Fatalf("expected flow_package_wildcard_observe_grant containing %q, got %#v", tc.wantMessage, report.Errors())
			}
		})
	}
}

func TestImportBoundaryWildcardExplicitCrossTreeSubscriptionWithoutGrantFailsClosed(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{WorkerSubscription: "producer/**/task.done"})
	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	if !importBoundaryWildcardReportContains(report.Errors(), "flow_package_wildcard_observe_grant", "no package-subtree candidate") {
		t.Fatalf("expected ungranted_or_unknown_subscription, got %#v", report.Errors())
	}
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no route for ungranted explicit cross-tree wildcard", routes)
	}
}

func TestImportBoundaryWildcardPreservesTemplateInstanceSubtree(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{WorkerMode: "template"})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("worker/inst-1/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve before materialization = %#v, want none", routes)
	}
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("worker", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	routes := rt.Resolve("worker/inst-1/task.done")
	if len(routes) != 1 || routes[0].ID != "worker-listener" || routes[0].Path != "worker/inst-1" {
		t.Fatalf("Resolve(worker/inst-1/task.done) = %#v, want materialized worker-listener", routes)
	}
	if sibling := rt.Resolve("producer/task.done"); len(sibling) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no sibling route for template wildcard without grant", sibling)
	}
}

func TestRootWildcardSubscriptionsRemainUnchanged(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{RootWildcard: true})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 1 || routes[0].ID != "root-listener" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want root-listener", routes)
	}
}

type importBoundaryWildcardFixtureOptions struct {
	ObserveGrant       string
	WorkerMode         string
	WorkerSubscription string
	RootWildcard       bool
	ProducerAuthored   bool
}

func loadBusImportBoundaryWildcardSource(t *testing.T, opts importBoundaryWildcardFixtureOptions) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryWildcardFixture(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeBusImportBoundaryWildcardFixture(t *testing.T, opts importBoundaryWildcardFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	mode := strings.TrimSpace(opts.WorkerMode)
	if mode == "" {
		mode = "static"
	}
	workerSubscription := strings.TrimSpace(opts.WorkerSubscription)
	if workerSubscription == "" {
		workerSubscription = "**/task.done"
	}
	rootNode := ""
	if opts.RootWildcard {
		rootNode = `
root-listener:
  id: root-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done": {}
`
	}
	workerBind := ""
	if strings.TrimSpace(opts.ObserveGrant) != "" {
		workerBind = "    bind:\n" + opts.ObserveGrant
	}
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: bus-import-boundary-wildcard
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: `+mode+`
`+workerBind+`  - id: producer
    flow: producer
    mode: static
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: bus-import-boundary-wildcard\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "nodes.yaml"), rootNode)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), "name: worker\nversion: \"1.0.0\"\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: `+mode+`
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "task.done: {}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["`+workerSubscription+`"]
  event_handlers:
    "`+workerSubscription+`": {}
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), "name: producer\nversion: \"1.0.0\"\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	producerEvents := "task.done: {}\n"
	producerNodes := "{}\n"
	if opts.ProducerAuthored {
		producerEvents += "task.start: {}\n"
		producerNodes = `
producer-source:
  id: producer-source
  execution_type: system_node
  subscribes_to: [task.start]
  produces: [task.done]
  event_handlers:
    task.start:
      emit:
        event: task.done
        broadcast: true
`
	}
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), producerEvents)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), producerNodes)
	return root
}

func writeBusImportBoundaryWildcardFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func importBoundaryWildcardReportContains(findings []bootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}
