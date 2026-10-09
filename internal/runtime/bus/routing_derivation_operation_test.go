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
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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
	source        semanticview.Source
	rows          []ActiveFlowInstanceDescriptor
	calls         int
	scopedCalls   int
	templateScope [][]string
	instanceScope [][]string
	replacements  [][]FlowInstanceRouteRecordSet
}

func (s *topologyOperationDescriptors) ReplaceFlowInstanceRouteTopology(_ context.Context, sets []FlowInstanceRouteRecordSet) (FlowInstanceRouteTopologyResult, error) {
	s.replacements = append(s.replacements, sets)
	return FlowInstanceRouteTopologyResult{Acknowledged: true}, nil
}

func (s *topologyOperationDescriptors) ListActiveFlowInstanceDescriptors(_ context.Context, runID string) ([]ActiveFlowInstanceDescriptor, error) {
	s.calls++
	return exactTestFlowInstanceDescriptors(s.rows, s.source.WorkflowVersion(), sourceartifactfixture.Fact(), runID, s.source), nil
}

func (s *topologyOperationDescriptors) ListActiveFlowInstanceDescriptorsForScope(_ context.Context, runID string, templateIDs, instancePaths []string) ([]ActiveFlowInstanceDescriptor, error) {
	s.scopedCalls++
	s.templateScope = append(s.templateScope, append([]string(nil), templateIDs...))
	s.instanceScope = append(s.instanceScope, append([]string(nil), instancePaths...))
	var out []ActiveFlowInstanceDescriptor
	for _, row := range s.rows {
		if slices.Contains(templateIDs, row.FlowTemplate) || slices.Contains(instancePaths, row.FlowInstance) {
			out = append(out, row)
		}
	}
	return exactTestFlowInstanceDescriptors(out, s.source.WorkflowVersion(), sourceartifactfixture.Fact(), runID, s.source), nil
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

func deriveFullRouteTopologyForTest(eb *EventBus, table *RouteTable, lister ActiveFlowInstanceDescriptorLister, runID string, include *FlowInstanceRouteMaterializationRequest, exclude runtimeflowidentity.RunScopedFlowInstance) (*RouteTable, []runtimeflowidentity.RunScopedFlowInstance, error) {
	graph, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(table.source)
	descriptors, err := eb.activeFlowInstanceDescriptorsForSemanticSource(context.Background(), lister, runID)
	if err != nil {
		return nil, nil, err
	}
	return eb.deriveFlowInstanceRouteTopologyFromDescriptors(context.Background(), table, runID, include, exclude, graph, inputProducers, false, descriptors)
}

func nestedTopologySink(t testing.TB, source semanticview.Source, side, revision string) runtimeflowidentity.Instance {
	t.Helper()
	outer := ConstructedFlowInstanceIdentityFixture(source, "outer", "", busInternalTestRunID)
	parent, err := runtimeflowidentity.KeyedChild(source, outer, "outer/"+side, "case-"+side)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := runtimeflowidentity.KeyedChild(source, parent, parent.TemplateID+"/sink", revision)
	if err != nil {
		t.Fatal(err)
	}
	return sink
}

func TestConstructedChildConnectTargetRequiresExactNativeParent(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	parent := nestedTopologySink(t, source, "left", "same-revision")
	child, err := runtimeflowidentity.KeylessChild(source, parent, parent.TemplateID+"/final")
	if err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"exact", "absent", "foreign_run", "crossed_parent", "missing_parent", "altered_parent_entity", "replacement"} {
		t.Run(variant, func(t *testing.T) {
			reader := constructionIndexTestReader{}
			if variant != "absent" {
				reader.observations = []runtimepipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, child, "")}
			}
			runID, selectedParent := busInternalTestRunID, parent
			switch variant {
			case "foreign_run":
				runID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
			case "crossed_parent":
				selectedParent = nestedTopologySink(t, source, "right", "same-revision")
			case "missing_parent":
				selectedParent = runtimeflowidentity.Instance{}
			case "altered_parent_entity":
				selectedParent.EntityID = "foreign-entity"
			case "replacement":
				other := nestedTopologySink(t, source, "left", "replacement-revision")
				replacement, err := runtimeflowidentity.KeylessChild(source, other, child.TemplateID)
				if err != nil {
					t.Fatal(err)
				}
				reader.observations = []runtimepipeline.FlowInstanceObservation{constructionIndexObservation(t, source, runID, replacement, "")}
			}
			request, err := runtimepipeline.NewDeclaredFlowInstanceLookup(source, fact, runID, child.TemplateID, selectedParent, nil)
			var observed runtimepipeline.FlowInstanceObservation
			var found bool
			if err == nil {
				observed, found, err = reader.LookupFlowInstance(context.Background(), request)
			}
			if variant == "exact" {
				if err != nil || !found || observed.Identity() != child {
					t.Fatalf("exact native child=%+v found=%v err=%v", observed.Identity(), found, err)
				}
			} else if found || observed.Valid() {
				t.Fatalf("parent context elected an unowned child: %+v found=%v err=%v", observed.Identity(), found, err)
			}
		})
	}
}

func TestRouteTopologyConstructionKeepsParentRelativeKeylessChild(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	for _, side := range []string{"left", "right"} {
		for _, path := range []string{"activation", "descriptor_preview"} {
			t.Run(side+"/"+path, func(t *testing.T) {
				parent := nestedTopologySink(t, source, side, "same-revision")
				child, err := runtimeflowidentity.KeylessChild(source, parent, parent.TemplateID+"/final")
				if err != nil {
					t.Fatal(err)
				}
				if child.InstancePath != parent.InstancePath+"/final" || child.ParentRoute.FlowInstance != parent.InstancePath || child.ParentEntityID != parent.EntityID {
					t.Fatalf("constructor lost exact parent: parent=%+v child=%+v", parent, child)
				}
				table, err := DeriveRouteTable(source)
				if err != nil {
					t.Fatal(err)
				}
				lister := &topologyOperationDescriptors{source: source}
				eb := &EventBus{semanticSource: source, sourceArtifactFact: fact, routeTable: table}
				eb.durable.ActiveFlows = lister
				var sets []FlowInstanceRouteRecordSet
				expectedInstances := []runtimeflowidentity.Instance{parent, child}
				switch path {
				case "activation":
					childPlan := runtimepipeline.FlowInstanceActivationPlan{
						Identity: child,
						Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
							RunID: busInternalTestRunID, Identity: child,
							BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
						},
					}
					parentPlan := runtimepipeline.FlowInstanceActivationPlan{
						Identity: parent,
						Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
							RunID: busInternalTestRunID, Identity: parent,
							BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
						},
						Children: []runtimepipeline.FlowInstanceActivationPlan{childPlan},
					}
					sets, err = eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{parentPlan})
				case "descriptor_preview":
					// The keyed-only census restores the existing parent;
					// the constructor supplies the new keyless child separately.
					lister.rows = []ActiveFlowInstanceDescriptor{{
						Identity: parent,
						RunID:    busInternalTestRunID, InstanceID: parent.InstanceID,
						EntityID: parent.EntityID, FlowInstance: parent.InstancePath,
						FlowTemplate: parent.TemplateID,
						BundleHash:   fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
					}}
					childOwner, ownerErr := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, child.Route())
					if ownerErr != nil {
						t.Fatal(ownerErr)
					}
					include := &FlowInstanceRouteMaterializationRequest{Identity: childOwner, Instance: child}
					var staged *RouteTable
					var owners []runtimeflowidentity.RunScopedFlowInstance
					staged, owners, err = deriveFullRouteTopologyForTest(eb, table, lister, busInternalTestRunID, include, runtimeflowidentity.RunScopedFlowInstance{})
					if err == nil {
						sets = flowInstanceRouteTopologyRecordSets(staged, owners)
					}
				}
				if err != nil {
					t.Fatalf("topology rejected canonical parent-relative construction: %v", err)
				}
				if len(sets) != 2 {
					t.Fatalf("exact parent/child route sets=%+v", sets)
				}
				for _, instance := range expectedInstances {
					owner, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, instance.Route())
					if err != nil {
						t.Fatal(err)
					}
					if table.HasFlowInstanceRoute(owner) {
						t.Fatal("non-executable topology preparation changed the live route table")
					}
					matched := false
					for _, set := range sets {
						if set.Identity == owner {
							matched = true
						}
					}
					if !matched {
						t.Fatalf("topology omitted exact constructed owner %+v", owner)
					}
				}
			})
		}
	}
}

func TestRouteTopologyPublicationSharesOneCensusAcrossNewActivations(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	lister := &topologyOperationDescriptors{source: source}
	eb.durable.ActiveFlows = lister
	plans := make([]runtimepipeline.FlowInstanceActivationPlan, 0, 3)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		plans = append(plans, runtimepipeline.FlowInstanceActivationPlan{
			Identity:  ConstructedFlowInstanceIdentityFixture(source, "workers", id, busInternalTestRunID),
			Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
		})
	}
	source.censuses.Store(0)
	got, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), plans)
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || lister.calls != 0 || lister.scopedCalls != 1 || len(got) != len(plans) {
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
		if err := independent.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
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
	// The ordinary workers-to-collector connection makes collector relevant.
	// A disconnected descriptor must still stay outside the compiled scope.
	lister.rows = []ActiveFlowInstanceDescriptor{{RunID: "foreign-run", FlowInstance: "unrelated/outsider", FlowTemplate: "unrelated"}}
	source.censuses.Store(0)
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), plans); err != nil {
		t.Fatalf("unrelated descriptor affected independent activation: %v", err)
	}
	if source.censuses.Load() != 0 || lister.calls != 0 || lister.scopedCalls != 2 {
		t.Fatalf("independent activation read unrelated descriptors: censuses=%d full_reads=%d scoped_reads=%d", source.censuses.Load(), lister.calls, lister.scopedCalls)
	}
	if !slices.Equal(lister.templateScope[1], []string{"collector", "workers"}) {
		t.Fatalf("ordinary collector dependency omitted: templates=%#v", lister.templateScope)
	}
}

func TestRouteTopologyPublicationUsesTableCompilationButRereadsCurrentTopology(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	lister := &topologyOperationDescriptors{source: source}
	eb.durable.ActiveFlows = lister
	source.censuses.Store(0)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		plan := runtimepipeline.FlowInstanceActivationPlan{
			Identity:  ConstructedFlowInstanceIdentityFixture(source, "workers", id, busInternalTestRunID),
			Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
		}
		sets, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan})
		if err != nil || len(sets) != 1 {
			t.Fatalf("prepare grouped route %s: sets=%d err=%v", id, len(sets), err)
		}
	}
	if source.censuses.Load() != 0 || lister.scopedCalls != 3 {
		t.Fatalf("table-owned source and fresh descriptor reads: censuses=%d scoped=%d", source.censuses.Load(), lister.scopedCalls)
	}
	first := table
	source.censuses.Store(0)
	table, err = DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || table == first {
		t.Fatalf("route-table replacement did not consume compiled source: raw_censuses=%d same=%t", source.censuses.Load(), table == first)
	}
	eb.routeTable = table
	source.censuses.Store(0)
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Identity:  ConstructedFlowInstanceIdentityFixture(source, "workers", "delta", busInternalTestRunID),
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
	}
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan}); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || lister.scopedCalls != 4 {
		t.Fatalf("route-table replacement recompiled during publication or skipped descriptors: censuses=%d scoped=%d", source.censuses.Load(), lister.scopedCalls)
	}
	source.censuses.Store(0)
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{plan}); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || lister.scopedCalls != 5 {
		t.Fatalf("ordinary publication recompiled table source or skipped descriptors: censuses=%d scoped=%d", source.censuses.Load(), lister.scopedCalls)
	}
}

func TestPreparedConstructionBindsCompiledPubsubWithoutRouteMembership(t *testing.T) {
	source, eb := topologyOperationFixture(t)
	live, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	resolver := newConnectRoutePlanResolver(source, live, nil, nil, nil)
	preview := func(id string) {
		t.Helper()
		current := &connectRoutePlanPreviewRoutes{}
		ctx := context.WithValue(context.Background(), connectRoutePlanPreviewRoutesKey{}, current)
		constructed := ConstructedFlowInstanceIdentityFixture(source, "workers", id, busInternalTestRunID)
		at := time.Unix(1700000000, 0).UTC()
		decision := connectInstanceSelection{
			identity: constructed,
			FlowInstanceSelection: runtimepipeline.FlowInstanceSelection{Activation: &runtimepipeline.FlowInstanceActivationPlan{
				Identity: constructed,
				Instance: runtimepipeline.WorkflowInstance{StorageRef: constructed.InstancePath, InstanceID: constructed.InstanceID},
				Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
					Identity: constructed, RunID: busInternalTestRunID, ExecutionMode: "live",
					BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
				},
				OccurredAt: at,
			}},
		}
		if err := resolver.installFlowConstructionPreview(ctx, busInternalTestRunID, *decision.Activation); err != nil {
			t.Fatal(err)
		}
		identity := topologyOperationIdentity(t, id)
		if len(current.plans) != 1 || !reflect.DeepEqual(current.selected["workers"], []runtimeflowidentity.Instance{constructed}) || live.HasFlowInstanceRoute(identity) {
			t.Fatalf("prepared construction %s must remain operation-local data", id)
		}
		oracle, err := DeriveRouteTable(source.Source)
		if err != nil {
			t.Fatal(err)
		}
		if err := oracle.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
		key := constructed.InstancePath + "/start"
		got, err := live.PubsubReceiverDefinitions(busInternalTestRunID, constructed, []string{key})
		if err != nil {
			t.Fatal(err)
		}
		if want := oracle.ResolveForRun(busInternalTestRunID, key); !reflect.DeepEqual(got, want) {
			t.Fatalf("pure binding %s differs from independent derivation: got=%#v want=%#v", id, got, want)
		}
	}
	source.censuses.Store(0)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		preview(id)
	}
	if got := source.censuses.Load(); got != 0 {
		t.Fatalf("three independent previews recompiled table source %d times", got)
	}
	source.censuses.Store(0)
	live, err = DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	if got := source.censuses.Load(); got != 0 {
		t.Fatalf("replacement route table bypassed compiled source: raw_censuses=%d", got)
	}
	resolver.routeTable = live
	source.censuses.Store(0)
	preview("delta")
	if got := source.censuses.Load(); got != 0 {
		t.Fatalf("new route-table preview recompiled source %d times", got)
	}
	source.censuses.Store(0)
	preview("epsilon")
	if got := source.censuses.Load(); got != 0 {
		t.Fatalf("later preview recompiled unchanged table source %d times", got)
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
		Identity:  ConstructedFlowInstanceIdentityFixture(source, "workers", "new", busInternalTestRunID),
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: busInternalTestRunID},
	}
	selected := ActiveFlowInstanceDescriptor{
		RunID: busInternalTestRunID, InstanceID: old.Route.InstanceID,
		FlowInstance: old.Route.InstancePath, FlowTemplate: "workers",
		BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
	}
	// This disconnected row would fail validation if a full-run read leaked into
	// this operation. Collector is a relevant ordinary receiver, not unrelated.
	unrelated := ActiveFlowInstanceDescriptor{RunID: "foreign-run", FlowInstance: "unrelated/hostile", FlowTemplate: "unrelated"}
	lister := &topologyOperationDescriptors{source: source, rows: []ActiveFlowInstanceDescriptor{unrelated, selected}}
	eb.durable.ActiveFlows = lister
	source.censuses.Store(0)
	got, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{newPlan})
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || lister.calls != 0 || lister.scopedCalls != 1 || !slices.Equal(lister.templateScope[0], []string{"collector", "workers"}) || len(lister.instanceScope[0]) != 0 {
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
		if err := oracle.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
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
	lister.rows = []ActiveFlowInstanceDescriptor{{RunID: "foreign-run", FlowInstance: "collector/hostile", FlowTemplate: "collector"}}
	if _, err := eb.prepareFlowInstanceActivationRouteTopology(context.Background(), []runtimepipeline.FlowInstanceActivationPlan{newPlan}); err == nil || !strings.Contains(err.Error(), "escaped selected run") {
		t.Fatalf("connected foreign collector admission = %v, want refusal", err)
	}
	if lister.calls != 0 || lister.scopedCalls != 3 {
		t.Fatalf("connected collector bypassed scoped read: full=%d scoped=%d", lister.calls, lister.scopedCalls)
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
			lister := &topologyOperationDescriptors{source: source, rows: rows}
			eb.durable.ActiveFlows = lister
			plan := runtimepipeline.FlowInstanceActivationPlan{
				Identity:  ConstructedFlowInstanceIdentityFixture(source, "producer", "new", busInternalTestRunID),
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
				if err := oracle.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
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

func TestRouteTopologyLifecycleStageReadsOnlyCompiledDependencies(t *testing.T) {
	source, eb := completeObserverFixture(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	eb.routeTable = table
	newOwner := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("observer", "new"))
	producer := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "source"))
	other := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("other", "source"))
	lister := &topologyOperationDescriptors{source: source, rows: []ActiveFlowInstanceDescriptor{{
		RunID: "foreign-run", FlowInstance: "unrelated/hostile", FlowTemplate: "unrelated",
	}}}
	for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{producer, other} {
		lister.rows = append(lister.rows, ActiveFlowInstanceDescriptor{
			RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
			FlowInstance: identity.Route.InstancePath, FlowTemplate: identity.Route.ScopeKey,
			BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
		})
	}
	for index := 0; index < 128; index++ {
		identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("observer", fmt.Sprintf("older-%03d", index)))
		lister.rows = append(lister.rows, ActiveFlowInstanceDescriptor{
			RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
			FlowInstance: identity.Route.InstancePath, FlowTemplate: "observer",
			BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
		})
	}
	eb.durable.ActiveFlows = lister
	eb.durable.FlowRouteTopology = lister
	source.censuses.Store(0)
	result, err := eb.StageFlowInstanceRouteContext(context.Background(), FlowInstanceRouteMaterializationRequest{Identity: newOwner, Instance: ConstructedFlowInstanceIdentityFixture(source, newOwner.Route.ScopeKey, newOwner.Route.InstanceID, newOwner.RunID)})
	if err != nil || !result.Acknowledged {
		t.Fatalf("stage independent child: acknowledged=%t err=%v", result.Acknowledged, err)
	}
	if source.censuses.Load() != 0 || lister.calls != 0 || lister.scopedCalls != 1 || !slices.Equal(lister.templateScope[0], []string{"other", "producer"}) {
		t.Fatalf("new observer stage selected wrong dependencies: census=%d full=%d scoped=%d templates=%#v", source.censuses.Load(), lister.calls, lister.scopedCalls, lister.templateScope)
	}
	oracle, err := DeriveRouteTable(source.Source)
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{producer, other, newOwner} {
		if err := oracle.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := lister.replacements[0], flowInstanceRouteTopologyRecordSets(oracle, []runtimeflowidentity.RunScopedFlowInstance{newOwner}); !reflect.DeepEqual(got, want) {
		t.Fatalf("independent child stage changed route evidence: got=%#v want=%#v", got, want)
	}
}

func TestRouteTopologyLifecycleStagePreservesCompleteObserverContext(t *testing.T) {
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
			oldA := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "old-a"))
			oldB := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "old-b"))
			otherOld := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("other", "old"))
			observer := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("observer", "one"))
			newOwner := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("producer", "new"))
			descriptor := func(identity runtimeflowidentity.RunScopedFlowInstance) ActiveFlowInstanceDescriptor {
				return ActiveFlowInstanceDescriptor{
					RunID: identity.RunID, InstanceID: identity.Route.InstanceID,
					FlowInstance: identity.Route.InstancePath, FlowTemplate: identity.Route.ScopeKey,
					BundleHash: eb.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(),
				}
			}
			rows := []ActiveFlowInstanceDescriptor{descriptor(oldA), descriptor(oldB), descriptor(otherOld), descriptor(observer)}
			if observerFirst {
				rows[0], rows[3] = rows[3], rows[0]
			}
			lister := &topologyOperationDescriptors{source: source, rows: append(rows, ActiveFlowInstanceDescriptor{
				RunID: "foreign-run", FlowInstance: "unrelated/hostile", FlowTemplate: "unrelated",
			})}
			eb.durable.ActiveFlows = lister
			eb.durable.FlowRouteTopology = lister
			result, err := eb.StageFlowInstanceRouteContext(context.Background(), FlowInstanceRouteMaterializationRequest{Identity: newOwner, Instance: ConstructedFlowInstanceIdentityFixture(source, newOwner.Route.ScopeKey, newOwner.Route.InstanceID, newOwner.RunID)})
			if err != nil || !result.Acknowledged {
				t.Fatalf("stage observer dependency: acknowledged=%t err=%v", result.Acknowledged, err)
			}
			if lister.calls != 0 || lister.scopedCalls != 1 || !slices.Equal(lister.templateScope[0], []string{"observer", "other", "producer"}) {
				t.Fatalf("observer stage scope full=%d scoped=%d templates=%#v", lister.calls, lister.scopedCalls, lister.templateScope)
			}
			oracle, err := DeriveRouteTable(source.Source)
			if err != nil {
				t.Fatal(err)
			}
			for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{oldA, oldB, otherOld, observer, newOwner} {
				if err := oracle.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
					t.Fatal(err)
				}
			}
			want := flowInstanceRouteTopologyRecordSets(oracle, []runtimeflowidentity.RunScopedFlowInstance{observer, newOwner})
			if got := lister.replacements[0]; !reflect.DeepEqual(got, want) {
				t.Fatalf("observer stage lost older producer context: got=%#v want=%#v", got, want)
			}
			lister.rows = append(lister.rows, descriptor(newOwner))
			if err := eb.RemoveFlowInstanceRouteContextFixture(context.Background(), oldA); err != nil {
				t.Fatalf("remove one producer: %v", err)
			}
			if lister.calls != 0 || lister.scopedCalls != 2 || !slices.Equal(lister.templateScope[1], []string{"observer", "other", "producer"}) {
				t.Fatalf("observer removal scope full=%d scoped=%d templates=%#v", lister.calls, lister.scopedCalls, lister.templateScope)
			}
			remaining, err := DeriveRouteTable(source.Source)
			if err != nil {
				t.Fatal(err)
			}
			for _, identity := range []runtimeflowidentity.RunScopedFlowInstance{oldB, otherOld, observer, newOwner} {
				if err := remaining.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
					t.Fatal(err)
				}
			}
			removedOwners := []runtimeflowidentity.RunScopedFlowInstance{oldA, observer}
			sort.Slice(removedOwners, func(i, j int) bool { return removedOwners[i].Key() < removedOwners[j].Key() })
			if got, want := lister.replacements[1], flowInstanceRouteTopologyRecordSets(remaining, removedOwners); !reflect.DeepEqual(got, want) {
				t.Fatalf("observer removal lost surviving producers: got=%#v want=%#v", got, want)
			}
			for index := range lister.rows {
				if lister.rows[index].FlowInstance == observer.Route.InstancePath {
					lister.rows[index].FlowTemplate = "producer"
				}
			}
			_, err = eb.StageFlowInstanceRouteContext(context.Background(), FlowInstanceRouteMaterializationRequest{Identity: newOwner, Instance: ConstructedFlowInstanceIdentityFixture(source, newOwner.Route.ScopeKey, newOwner.Route.InstanceID, newOwner.RunID)})
			if err == nil || !strings.Contains(err.Error(), "active flow-instance descriptor lost its exact construction identity") {
				t.Fatalf("selected observer template mismatch = %v, want refusal", err)
			}
			if len(lister.replacements) != 2 {
				t.Fatalf("selected observer corruption replaced routes: calls=%d", len(lister.replacements))
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
	lister := &topologyOperationDescriptors{source: source}
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
		Identity:  ConstructedFlowInstanceIdentityFixture(source, "workers", "new", busInternalTestRunID),
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
	if got := source.censuses.Load(); got != 0 {
		t.Fatalf("initial derivation rebuilt %d authored-event catalogs, want none", got)
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
	lister := &topologyOperationDescriptors{source: source, rows: []ActiveFlowInstanceDescriptor{descriptor(beta), descriptor(alpha)}}
	include := &FlowInstanceRouteMaterializationRequest{Identity: gamma, Instance: ConstructedFlowInstanceIdentityFixture(source, "workers", gamma.Route.InstanceID, gamma.RunID)}
	source.censuses.Store(0)
	staged, identities, err := deriveFullRouteTopologyForTest(eb, table, lister, busInternalTestRunID, include, runtimeflowidentity.RunScopedFlowInstance{})
	if err != nil {
		t.Fatal(err)
	}
	if got := source.censuses.Load(); got != 0 || lister.calls != 1 || len(identities) != 3 {
		t.Fatalf("topology work: censuses=%d reads=%d identities=%v", got, lister.calls, identities)
	}
	independent, err := DeriveRouteTable(source.Source)
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		if err := independent.AddConstructedFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
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
	next, nextIDs, err := deriveFullRouteTopologyForTest(eb, staged, lister, busInternalTestRunID, nil, beta)
	if err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || lister.calls != 2 || len(nextIDs) != 1 || nextIDs[0] != gamma || next.HasFlowInstanceRoute(alpha) || next.HasFlowInstanceRoute(beta) || !next.HasFlowInstanceRoute(gamma) {
		t.Fatalf("next operation retained stale membership: censuses=%d reads=%d identities=%v", source.censuses.Load(), lister.calls, nextIDs)
	}
}

func TestRouteTopologySelectedRecordMatchesIndependentFullDerivation(t *testing.T) {
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
	lister := &topologyOperationDescriptors{source: source, rows: []ActiveFlowInstanceDescriptor{descriptor(beta), descriptor(alpha)}}
	for _, tc := range []struct {
		name    string
		include *FlowInstanceRouteMaterializationRequest
		exclude runtimeflowidentity.RunScopedFlowInstance
	}{
		{name: "activation", include: &FlowInstanceRouteMaterializationRequest{Identity: gamma, Instance: ConstructedFlowInstanceIdentityFixture(source, "workers", gamma.Route.InstanceID, gamma.RunID)}},
		{name: "retirement", exclude: beta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexed, indexedIDs, err := deriveFullRouteTopologyForTest(eb, table, lister, busInternalTestRunID, tc.include, tc.exclude)
			if err != nil {
				t.Fatal(err)
			}
			fullReads := lister.calls
			recordOnly, recordIDs, err := eb.deriveFlowInstanceRouteRecordTopology(context.Background(), table, lister, busInternalTestRunID, tc.include, tc.exclude)
			if err != nil {
				t.Fatal(err)
			}
			if lister.calls != fullReads {
				t.Fatalf("selected record derivation performed a full-run descriptor read: before=%d after=%d", fullReads, lister.calls)
			}
			for _, identity := range recordIDs {
				if !slices.Contains(indexedIDs, identity) && identity != tc.exclude {
					t.Fatalf("selected record owner %s is absent from full derivation", identity.Key())
				}
			}
			if !reflect.DeepEqual(flowInstanceRouteTopologyRecordSets(recordOnly, recordIDs), flowInstanceRouteTopologyRecordSets(indexed, recordIDs)) {
				t.Fatalf("selected record topology differs from full derivation for %s: owners=%v", tc.name, recordIDs)
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
	if err := table.AddConstructedFlowInstanceRouteFixture(req); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || table.snapshotGenerationCurrent(before) {
		t.Fatal("standalone addition must use table preparation and invalidate the generation")
	}
	before = table.snapshotGeneration()
	if err := table.AddConstructedFlowInstanceRouteFixture(req); err != nil {
		t.Fatal(err)
	}
	if source.censuses.Load() != 0 || !table.snapshotGenerationCurrent(before) {
		t.Fatal("replay must neither recompile nor change the generation")
	}
	_, resolver := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	source.censuses.Store(0)
	leasedRequest := table.ConstructedRouteRequestFixture(FlowInstanceRouteMaterializationRequest{Identity: topologyOperationIdentity(t, "beta")})
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
