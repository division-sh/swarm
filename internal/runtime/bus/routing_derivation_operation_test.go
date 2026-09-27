package bus

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type topologyOperationSource struct {
	semanticview.Source
	censuses atomic.Int64
}

func (s *topologyOperationSource) AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	s.censuses.Add(1)
	return s.Source.AuthoredEventEntries()
}

type topologyOperationDescriptors struct {
	rows          []ActiveFlowInstanceDescriptor
	calls         int
	scopedCalls   int
	templateScope [][]string
	instanceScope [][]string
}

func (s *topologyOperationDescriptors) ListActiveFlowInstanceDescriptors(context.Context, string) ([]ActiveFlowInstanceDescriptor, error) {
	s.calls++
	return append([]ActiveFlowInstanceDescriptor(nil), s.rows...), nil
}

func (s *topologyOperationDescriptors) ListActiveFlowInstanceDescriptorsForScope(_ context.Context, _ string, templateIDs, instancePaths []string) ([]ActiveFlowInstanceDescriptor, error) {
	s.scopedCalls++
	s.templateScope = append(s.templateScope, append([]string(nil), templateIDs...))
	s.instanceScope = append(s.instanceScope, append([]string(nil), instancePaths...))
	var out []ActiveFlowInstanceDescriptor
	for _, row := range s.rows {
		if slices.Contains(templateIDs, row.FlowTemplate) || slices.Contains(instancePaths, row.FlowInstance) {
			out = append(out, row)
		}
	}
	return out, nil
}

func topologyOperationFixture(t testing.TB) (*topologyOperationSource, *EventBus) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := &topologyOperationSource{Source: semanticview.Wrap(bundle)}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	return source, &EventBus{semanticSource: source, sourceArtifactFact: fact}
}

func completeObserverFixture(t testing.TB) (*topologyOperationSource, *EventBus) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyCompleteObserverDependencies(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := &topologyOperationSource{Source: semanticview.Wrap(bundle)}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	return source, &EventBus{semanticSource: source, sourceArtifactFact: fact}
}

func topologyOperationIdentity(t *testing.T, id string) runtimeflowidentity.RunScopedFlowInstance {
	t.Helper()
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, runtimeflowidentity.DeriveRoute("workers", id))
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestRouteTopologyPublicationSharesOneCensusAcrossNewActivations(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	lister := &topologyOperationDescriptors{}
	eb.durable.ActiveFlows = lister
	plans := make([]runtimepipeline.FlowInstanceActivationPlan, 0, 3)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		plans = append(plans, runtimepipeline.FlowInstanceActivationPlan{
			Identity:  runtimeflowidentity.Derive(source, "workers", id),
			Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
		})
	}
	source.censuses.Store(0)
	got, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), plans)
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || lister.calls != 0 || lister.scopedCalls != 1 || len(got) != len(plans) {
		t.Fatalf("independent publication work: censuses=%d full_reads=%d scoped_reads=%d routes=%d", source.censuses.Load(), lister.calls, lister.scopedCalls, len(got))
	}
	independent, err := DeriveRouteTable(source.Source)
	if err != nil {
		t.Fatal(err)
	}
	var identities []runtimeflowidentity.RunScopedFlowInstance
	for _, plan := range plans {
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.Readiness.RunID, plan.Identity.Route())
		if err != nil {
			t.Fatal(err)
		}
		if err := independent.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
		identities = append(identities, identity)
		if table.HasFlowInstanceRoute(identity) {
			t.Fatal("publication preparation mutated the live route table")
		}
	}
	if want := flowInstanceRouteTopologyRecordSets(independent, identities); !reflect.DeepEqual(got, want) {
		t.Fatalf("publication changed route evidence: got=%+v want=%+v", got, want)
	}
	// Unrelated descriptors, even hostile ones, are outside this activation's
	// compiled dependency scope and must not be loaded or affect its routes.
	lister.rows = []ActiveFlowInstanceDescriptor{{RunID: "foreign-run", FlowInstance: "collector/outsider", FlowTemplate: "collector"}}
	source.censuses.Store(0)
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), plans); err != nil {
		t.Fatalf("unrelated descriptor affected independent activation: %v", err)
	}
	if source.censuses.Load() != 1 || lister.calls != 0 || lister.scopedCalls != 2 {
		t.Fatalf("independent activation read unrelated descriptors: censuses=%d full_reads=%d scoped_reads=%d", source.censuses.Load(), lister.calls, lister.scopedCalls)
	}
}

func TestRouteTopologyPublicationGroupSharesCompilationButRereadsCurrentTopology(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	lister := &topologyOperationDescriptors{}
	eb.durable.ActiveFlows = lister
	scope := new(routeTopologyCompilationScope)
	source.censuses.Store(0)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		plan := runtimepipeline.FlowInstanceActivationPlan{
			Identity:  runtimeflowidentity.Derive(source, "workers", id),
			Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
		}
		sets, err := eb.prepareFlowInstanceActivationRouteTopologyWithSource(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan}, scope)
		if err != nil || len(sets) != 1 {
			t.Fatalf("prepare grouped route %s: sets=%d err=%v", id, len(sets), err)
		}
	}
	if source.censuses.Load() != 1 || lister.scopedCalls != 3 {
		t.Fatalf("grouped source and descriptor reads: censuses=%d scoped=%d", source.censuses.Load(), lister.scopedCalls)
	}
	first := scope.compiled
	table, err = DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	source.censuses.Store(0)
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Identity:  runtimeflowidentity.Derive(source, "workers", "delta"),
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
	}
	if _, err := eb.prepareFlowInstanceActivationRouteTopologyWithSource(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan}, scope); err != nil {
		t.Fatal(err)
	}
	if scope.compiled == first || source.censuses.Load() != 1 || lister.scopedCalls != 4 {
		t.Fatalf("route-table replacement reused source evidence or skipped descriptors: same=%t censuses=%d scoped=%d", scope.compiled == first, source.censuses.Load(), lister.scopedCalls)
	}
	source.censuses.Store(0)
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan}); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || lister.scopedCalls != 5 {
		t.Fatalf("ordinary publication retained grouped evidence or skipped descriptors: censuses=%d scoped=%d", source.censuses.Load(), lister.scopedCalls)
	}
}

func TestRouteTopologyPublicationReadsOnlyCompiledObserverDependency(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	observer := routeTemplateSourceObserver{
		RunID: busInternalTestRunID, SourceTemplatePath: "workers", SourceLocalEvent: "item.reported",
		Subscriber:             Subscriber{Recipient: events.MustAgentDeliveryRecipient("observer-agent")},
		SubscriberInstancePath: "workers/observer",
	}
	table.addTemplateSourceObserverLocked(observer)
	eb.routeTable = table
	old := topologyOperationIdentity(t, "old")
	newPlan := runtimepipeline.FlowInstanceActivationPlan{
		Identity:  runtimeflowidentity.Derive(source, "workers", "new"),
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
	}
	selected := ActiveFlowInstanceDescriptor{
		RunID: busInternalTestRunID, InstanceID: old.Route.InstanceID,
		FlowInstance: old.Route.InstancePath, FlowTemplate: "workers",
		BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
	}
	// This row would fail validation if a full-run read leaked into this operation.
	unrelated := ActiveFlowInstanceDescriptor{RunID: "foreign-run", FlowInstance: "collector", FlowTemplate: "collector"}
	lister := &topologyOperationDescriptors{rows: []ActiveFlowInstanceDescriptor{unrelated, selected}}
	eb.durable.ActiveFlows = lister
	source.censuses.Store(0)
	got, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{newPlan})
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || lister.calls != 0 || lister.scopedCalls != 1 || !reflect.DeepEqual(lister.templateScope[0], []string{"workers"}) || len(lister.instanceScope[0]) != 0 {
		t.Fatalf("dependency reads: censuses=%d full=%d scoped=%d templates=%#v paths=%#v", source.censuses.Load(), lister.calls, lister.scopedCalls, lister.templateScope, lister.instanceScope)
	}
	newOwner, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, newPlan.Identity.Route())
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := DeriveRouteTable(source.Source)
	if err != nil {
		t.Fatal(err)
	}
	oracle.addTemplateSourceObserverLocked(observer)
	for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{old, newOwner} {
		if err := oracle.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
	}
	want := flowInstanceRouteTopologyRecordSets(oracle, []runtimeflowidentity.RunScopedFlowInstance{old, newOwner})
	sort.Slice(got, func(i, j int) bool { return got[i].Identity.Route.InstancePath < got[j].Identity.Route.InstancePath })
	sort.Slice(want, func(i, j int) bool { return want[i].Identity.Route.InstancePath < want[j].Identity.Route.InstancePath })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scoped topology differs from full derivation: got=%#v want=%#v", got, want)
	}

	// Selected-scope corruption is still rejected, rather than masked by the
	// exclusion of unrelated run rows.
	lister.rows = []ActiveFlowInstanceDescriptor{{RunID: "foreign-run", FlowInstance: "workers/hostile", FlowTemplate: "workers"}}
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{newPlan}); err == nil || !strings.Contains(err.Error(), "escaped selected run") {
		t.Fatalf("selected foreign descriptor admission = %v, want refusal", err)
	}
	if lister.calls != 0 || lister.scopedCalls != 2 {
		t.Fatalf("selected foreign descriptor bypassed scoped read: full=%d scoped=%d", lister.calls, lister.scopedCalls)
	}
}

func TestRouteTopologyPublicationLoadsCompleteCompiledObserverContext(t *testing.T) {
	for _, observerFirst := range []bool{false, true} {
		name := "producers_first"
		if observerFirst {
			name = "observer_first"
		}
		t.Run(name, func(t *testing.T) {
			source, eb := completeObserverFixture(t)
			table, err := DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			eb.routeTable = table
			graph := runtimepinrouting.CompileConnectGraph(source)
			if len(graph.Issues()) != 0 || len(graph.Plans()) != 2 {
				t.Fatalf("compiled two-producer fixture: plans=%d issues=%#v", len(graph.Plans()), graph.Issues())
			}
			oldA, oldB := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "old-a")), testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "old-b"))
			otherOld := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("other", "old"))
			observer, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, runtimeflowidentity.Derive(source, "observer", "one").Route())
			if err != nil {
				t.Fatal(err)
			}
			newOwner := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "new"))
			descriptor := func(identity runtimeflowidentity.RunScopedFlowInstance, template string) ActiveFlowInstanceDescriptor {
				return ActiveFlowInstanceDescriptor{
					RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
					FlowInstance: identity.Route.InstancePath, FlowTemplate: template,
					BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
				}
			}
			rows := []ActiveFlowInstanceDescriptor{descriptor(oldA, "producer"), descriptor(oldB, "producer"), descriptor(otherOld, "other"), descriptor(observer, "observer")}
			if observerFirst {
				rows[0], rows[3] = rows[3], rows[0]
			}
			lister := &topologyOperationDescriptors{rows: rows}
			eb.durable.ActiveFlows = lister
			plan := runtimepipeline.FlowInstanceActivationPlan{
				Identity:  runtimeflowidentity.Derive(source, "producer", "new"),
				Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
			}
			got, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan})
			if err != nil {
				t.Fatal(err)
			}
			if lister.calls != 0 || lister.scopedCalls != 1 || !slices.Equal(lister.templateScope[0], []string{"observer", "other", "producer"}) {
				t.Fatalf("dependency scope full=%d scoped=%d templates=%#v", lister.calls, lister.scopedCalls, lister.templateScope)
			}
			oracle, err := DeriveRouteTable(source.Source)
			if err != nil {
				t.Fatal(err)
			}
			for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{oldA, oldB, otherOld, observer, newOwner} {
				if err := oracle.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
					t.Fatal(err)
				}
			}
			want := flowInstanceRouteTopologyRecordSets(oracle, []runtimeflowidentity.RunScopedFlowInstance{observer, newOwner})
			sort.Slice(got, func(i, j int) bool { return got[i].Identity.Key() < got[j].Identity.Key() })
			sort.Slice(want, func(i, j int) bool { return want[i].Identity.Key() < want[j].Identity.Key() })
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("observer replacement lost earlier producers: got=%#v want=%#v", got, want)
			}
		})
	}
}

func BenchmarkFlowInstanceActivationRouteTopology64(b *testing.B) {
	source, eb := topologyOperationFixture(b)
	table, err := DeriveRouteTable(source)
	if err != nil {
		b.Fatal(err)
	}
	eb.routeTable = table
	lister := &topologyOperationDescriptors{}
	for i := 0; i < 64; i++ {
		id := fmt.Sprintf("worker-%03d", i)
		route := runtimeflowidentity.Derive(source, "workers", id).Route()
		lister.rows = append(lister.rows, ActiveFlowInstanceDescriptor{
			RunID: busInternalTestRunID, InstanceID: route.InstanceID,
			FlowInstance: route.InstancePath, FlowTemplate: "workers",
			BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
		})
	}
	eb.durable.ActiveFlows = lister
	plans := []runtimepipeline.FlowInstanceActivationPlan{{
		Identity:  runtimeflowidentity.Derive(source, "workers", "new"),
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sets, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), plans)
		if err != nil || len(sets) != 1 {
			b.Fatalf("sets=%d err=%v", len(sets), err)
		}
	}
}

func TestRouteTopologyOperationSharesOneCensusAndRereadsDescriptors(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	if got := source.censuses.Load(); got != 1 {
		t.Fatalf("initial derivation built %d censuses, want 1", got)
	}
	alpha := topologyOperationIdentity(t, "alpha")
	beta := topologyOperationIdentity(t, "beta")
	gamma := topologyOperationIdentity(t, "gamma")
	descriptor := func(identity runtimeflowidentity.RunScopedFlowInstance) ActiveFlowInstanceDescriptor {
		return ActiveFlowInstanceDescriptor{
			RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
			FlowInstance: identity.Route.InstancePath, FlowTemplate: "workers",
			BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
		}
	}
	lister := &topologyOperationDescriptors{rows: []ActiveFlowInstanceDescriptor{descriptor(beta), descriptor(alpha)}}
	include := &FlowInstanceRouteMaterializationRequest{Identity: gamma}
	source.censuses.Store(0)
	staged, identities, err := eb.deriveFlowInstanceRouteTopology(context.Background(), table, lister, busInternalTestRunID, include, runtimeflowidentity.RunScopedFlowInstance{})
	if err != nil {
		t.Fatal(err)
	}
	if got := source.censuses.Load(); got != 1 || lister.calls != 1 || len(identities) != 3 {
		t.Fatalf("topology work: censuses=%d reads=%d identities=%v", got, lister.calls, identities)
	}
	independent, err := DeriveRouteTable(source.Source)
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		if err := independent.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
		if table.HasFlowInstanceRoute(identity) || !staged.HasFlowInstanceRoute(identity) {
			t.Fatal("staging mutated original table or dropped an admitted instance")
		}
		if len(staged.MaterializedRoutes(identity)) == 0 {
			t.Fatal("fixture must exercise concrete subscriber routes")
		}
	}
	if got, want := flowInstanceRouteTopologyRecordSets(staged, identities), flowInstanceRouteTopologyRecordSets(independent, identities); !reflect.DeepEqual(got, want) {
		t.Fatalf("shared resolver changed durable routes: got=%+v want=%+v", got, want)
	}
	for _, event := range []string{"item.registered", "item.finished", "workers/alpha/item.reported", "workers/beta/item.reported", "workers/gamma/item.reported"} {
		if got, want := subscriberSignature(staged.ResolveForRun(busInternalTestRunID, event)), subscriberSignature(independent.ResolveForRun(busInternalTestRunID, event)); got != want {
			t.Fatalf("recipient projection changed for %s: got=%s want=%s", event, got, want)
		}
	}
	// A later operation must read current descriptors, not retain the first
	// operation's membership or resolver on the route table.
	lister.rows = []ActiveFlowInstanceDescriptor{descriptor(gamma), descriptor(beta)}
	source.censuses.Store(0)
	next, nextIDs, err := eb.deriveFlowInstanceRouteTopology(context.Background(), staged, lister, busInternalTestRunID, nil, beta)
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || lister.calls != 2 || len(nextIDs) != 1 || nextIDs[0] != gamma || next.HasFlowInstanceRoute(alpha) || next.HasFlowInstanceRoute(beta) || !next.HasFlowInstanceRoute(gamma) {
		t.Fatalf("next operation retained stale membership: censuses=%d reads=%d identities=%v", source.censuses.Load(), lister.calls, nextIDs)
	}
}

func TestRouteTopologyRecordOnlyMatchesIndexedDerivation(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	alpha := topologyOperationIdentity(t, "alpha")
	beta := topologyOperationIdentity(t, "beta")
	gamma := topologyOperationIdentity(t, "gamma")
	descriptor := func(identity runtimeflowidentity.RunScopedFlowInstance) ActiveFlowInstanceDescriptor {
		return ActiveFlowInstanceDescriptor{
			RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
			FlowInstance: identity.Route.InstancePath, FlowTemplate: "workers",
			BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
		}
	}
	lister := &topologyOperationDescriptors{rows: []ActiveFlowInstanceDescriptor{descriptor(beta), descriptor(alpha)}}
	for _, tc := range []struct {
		name    string
		include *FlowInstanceRouteMaterializationRequest
		exclude runtimeflowidentity.RunScopedFlowInstance
	}{
		{name: "activation", include: &FlowInstanceRouteMaterializationRequest{Identity: gamma}},
		{name: "retirement", exclude: beta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexed, indexedIDs, err := eb.deriveFlowInstanceRouteTopology(context.Background(), table, lister, busInternalTestRunID, tc.include, tc.exclude)
			if err != nil {
				t.Fatal(err)
			}
			recordOnly, recordIDs, err := eb.deriveFlowInstanceRouteRecordTopology(context.Background(), table, lister, busInternalTestRunID, tc.include, tc.exclude)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(recordIDs, indexedIDs) || !reflect.DeepEqual(flowInstanceRouteTopologyRecordSets(recordOnly, recordIDs), flowInstanceRouteTopologyRecordSets(indexed, indexedIDs)) {
				t.Fatalf("record-only topology differs from indexed topology for %s", tc.name)
			}
		})
	}
}

func TestRouteTopologyOperationStandaloneReplayAndGenerationLease(t *testing.T) {
	source, _ := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	req := FlowInstanceRouteMaterializationRequest{Identity: topologyOperationIdentity(t, "alpha")}
	source.censuses.Store(0)
	before := table.snapshotGeneration()
	if err := table.AddFlowInstanceRoute(req); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || table.snapshotGenerationCurrent(before) {
		t.Fatal("standalone addition must prepare once and invalidate the generation")
	}
	before = table.snapshotGeneration()
	if err := table.AddFlowInstanceRoute(req); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 1 || !table.snapshotGenerationCurrent(before) {
		t.Fatal("replay must neither prepare nor change the generation")
	}
	_, resolver := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	source.censuses.Store(0)
	leasedRequest := FlowInstanceRouteMaterializationRequest{Identity: topologyOperationIdentity(t, "beta")}
	ctx := context.WithValue(context.Background(), routeTableGenerationLeaseKey{}, routeTableGenerationLease{table: table})
	table.generationMu.Lock()
	finished := make(chan error, 1)
	go func() {
		finished <- table.addFlowInstanceRouteForContextWithInputProducers(ctx, leasedRequest, &resolver)
	}()
	select {
	case err := <-finished:
		table.generationMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		table.generationMu.Unlock()
		<-finished
		t.Fatal("matching generation lease reacquired its own lock")
	}
	if source.censuses.Load() != 0 || table.snapshotGenerationCurrent(before) {
		t.Fatal("leased addition must reuse supplied preparation and invalidate generation")
	}
}

func TestRouteTopologyOperationPreservesIntrinsicIngressAdmission(t *testing.T) {
	scatter, _ := topologyOperationFixture(t)
	sources := map[string]semanticview.Source{
		"scatter":  scatter.Source,
		"external": loadHarnessRouteSource(t, canonicalrouting.CopyInputPinExternalScope(t)),
		"harness":  loadHarnessRouteSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)),
		"root":     loadHarnessRouteSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress)),
		"parent":   loadHarnessRouteSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.ParentConnect)),
	}
	intrinsicInputs := 0
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			_, resolver := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
			for _, scope := range source.FlowScopes() {
				rootInputs := routeRootInputEventSet(source)
				want := cloneStringSet(rootInputs)
				// Frozen pre-hoist admission: only intrinsic non-connect
				// evidence may add a flow input to the root-input set.
				for _, localEvent := range scope.InputEvents {
					flowID := strings.TrimSpace(scope.ID)
					resolution := semanticview.ResolveNonConnectFlowInputProducer(source, flowID, localEvent)
					if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) {
						continue
					}
					intrinsicInputs++
					if event := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, localEvent)); event != "" {
						want[event] = struct{}{}
					}
				}
				if got := routeAdmittedFlowIngressEventSet(source, scope, rootInputs, resolver); !reflect.DeepEqual(got, want) {
					t.Fatalf("scope %s changed ingress admission: got=%v want=%v", scope.ID, got, want)
				}
			}
		})
	}
	if intrinsicInputs == 0 {
		t.Fatal("fixtures must exercise positive intrinsic ingress admission")
	}
}
