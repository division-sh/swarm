package bus

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	rows  []ActiveFlowInstanceDescriptor
	calls int
}

func (s *topologyOperationDescriptors) ListActiveFlowInstanceDescriptors(context.Context, string) ([]ActiveFlowInstanceDescriptor, error) {
	s.calls++
	return append([]ActiveFlowInstanceDescriptor(nil), s.rows...), nil
}

func topologyOperationFixture(t *testing.T) (*topologyOperationSource, *EventBus) {
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

func topologyOperationIdentity(t *testing.T, id string) runtimeflowidentity.RunScopedFlowInstance {
	t.Helper()
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, runtimeflowidentity.DeriveRoute("workers", id))
	if err != nil {
		t.Fatal(err)
	}
	return identity
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
