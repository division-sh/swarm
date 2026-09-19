package pinrouting

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestOperationInputProducerResolverMatchesFreshResolution(t *testing.T) {
	sources := map[string]semanticview.Source{
		"scatter": operationScatterSource(t),
		"nil":     nil,
	}
	for _, kind := range []runtimecontracts.FlowInputPinSource{runtimecontracts.FlowInputPinSourceExternal, runtimecontracts.FlowInputPinSourceHarness} {
		sources[string(kind)] = testConnectRoutePlanSource([]connectRoutePlanFlow{{id: "worker", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.requested", Source: kind}}}}, nil)
	}
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			graph, resolver := CompileConnectGraphWithInputProducerResolver(source)
			fresh := CompileConnectGraph(source)
			if !reflect.DeepEqual(graph, fresh) {
				t.Fatal("operation resolver changed compiled plans, issues or collisions")
			}
			for _, flow := range []string{".", "workers", "collector", "worker", "sibling", " workers ", ""} {
				for _, event := range []string{"batch.submitted", "item.registered", "workers/item.reported", "item.reported", "work.requested", "platform.runtime_log", "item.*", " missing ", ""} {
					for _, allow := range []bool{false, true} {
						opts := runtimecontracts.FlowInputProducerResolutionOptions{AllowNonInputEvent: allow}
						want := ResolveFlowInputProducerWithOptions(source, flow, event, opts)
						got := resolver.ResolveWithOptions(flow, event, opts)
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("flow=%q event=%q allow=%t: got=%+v want=%+v", flow, event, allow, got, want)
						}
						if len(got.Evidence) > 0 {
							got.Evidence[0].Kind = "hostile caller mutation"
							if again := resolver.ResolveWithOptions(flow, event, opts); !reflect.DeepEqual(again, want) {
								t.Fatal("resolver retained caller-mutated evidence")
							}
						}
					}
				}
			}
		})
	}
}

func TestOperationInputProducerResolverBoundsCensusAndSeparatesOperations(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			base, endpoint := targetFreeSyntheticProjectionFixture(t, tc.mint, false)
			source := &censusCountingSource{Source: base}
			_, resolver := CompileConnectGraphWithInputProducerResolver(source)
			want := ResolveFlowInputProducer(base, endpoint.FlowID, endpoint.Event.Local)
			for i := 0; i < 100; i++ {
				if got := resolver.Resolve(endpoint.FlowID, endpoint.Event.Local); !reflect.DeepEqual(got, want) {
					t.Fatalf("query %d changed evidence: %+v", i, got)
				}
			}
			if source.builds != 1 {
				t.Fatalf("100 queries built %d censuses, want one", source.builds)
			}
			// End the first operation before changing its source. Neither the
			// new operation nor the one-shot API may inherit old evidence.
			source.withdraw = true
			graph, next := CompileConnectGraphWithInputProducerResolver(source)
			got := next.Resolve(endpoint.FlowID, endpoint.Event.Local)
			if source.builds != 2 || len(graph.receiverPlans) != 0 || !got.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
				t.Fatalf("new operation reused withdrawn source: builds=%d result=%+v", source.builds, got)
			}
			if fresh := ResolveFlowInputProducer(source, endpoint.FlowID, endpoint.Event.Local); source.builds != 3 || !reflect.DeepEqual(fresh, got) {
				t.Fatalf("one-shot API cached operation: builds=%d fresh=%+v want=%+v", source.builds, fresh, got)
			}
		})
	}
}

func TestOperationInputProducerResolverConcurrentResultIsolation(t *testing.T) {
	source := operationScatterSource(t)
	_, resolver := CompileConnectGraphWithInputProducerResolver(source)
	want := resolver.Resolve("workers", "item.registered")
	if !want.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("fixture lost exact parent connect: %+v", want)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				got := resolver.Resolve("workers", "item.registered")
				if !reflect.DeepEqual(got, want) {
					t.Error("concurrent query changed exact evidence")
					return
				}
				got.Evidence[0].Kind = "caller-private change"
			}
		}()
	}
	wg.Wait()
}

func operationScatterSource(t testing.TB) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func BenchmarkScatterInputProducerOperation(b *testing.B) {
	source := operationScatterSource(b)
	queries := [][2]string{{".", "batch.submitted"}, {".", "batch.finished"}, {"workers", "item.registered"}, {"workers", "item.finished"}, {"collector", "item.reported"}}
	for _, shared := range []bool{false, true} {
		name := "one_shot"
		if shared {
			name = "operation_local"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var resolver FlowInputProducerResolver
				if shared {
					_, resolver = CompileConnectGraphWithInputProducerResolver(source)
				}
				for _, query := range queries {
					var result runtimecontracts.FlowInputProducerResolution
					if shared {
						result = resolver.Resolve(query[0], query[1])
					} else {
						result = ResolveFlowInputProducer(source, query[0], query[1])
					}
					if !result.HasEvidence() {
						b.Fatalf("scatter input lost producer evidence: %+v", result)
					}
				}
			}
		})
	}
}
