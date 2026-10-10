package bus

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
