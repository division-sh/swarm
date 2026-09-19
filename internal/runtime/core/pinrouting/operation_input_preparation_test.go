package pinrouting

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type operationInputReadSource struct {
	semanticview.Source
	reads atomic.Int64
}

func (s *operationInputReadSource) FlowHasInputEvent(flowID, eventType string) bool {
	s.reads.Add(1)
	return s.Source.FlowHasInputEvent(flowID, eventType)
}

func TestOperationInputProducerPreparationBoundsRepeatedQueries(t *testing.T) {
	base := operationScatterSource(t)
	source := &operationInputReadSource{Source: base}
	_, resolver := CompileConnectGraphWithInputProducerResolver(source)
	source.reads.Store(0)
	want := ResolveFlowInputProducer(base, "workers", "item.registered")
	first := resolver.Resolve("workers", "item.registered")
	firstReads := source.reads.Load()
	if firstReads == 0 || !reflect.DeepEqual(first, want) {
		t.Fatalf("first query skipped canonical admission: reads=%d result=%+v", firstReads, first)
	}
	first.Evidence[0].Kind = "caller mutation on miss"
	copyOfResolver := resolver
	for i := 0; i < 100; i++ {
		got := copyOfResolver.Resolve(" workers ", " item.registered ")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("query %d changed caller-private evidence: %+v", i, got)
		}
		got.Evidence[0].Detail = "caller mutation on hit"
	}
	if source.reads.Load() != firstReads {
		t.Fatalf("repeated normalized query reread source: first=%d total=%d", firstReads, source.reads.Load())
	}
	options := runtimecontracts.FlowInputProducerResolutionOptions{AllowNonInputEvent: true}
	wantOptions := ResolveFlowInputProducerWithOptions(base, "workers", "item.registered", options)
	if got := resolver.ResolveWithOptions("workers", "item.registered", options); !reflect.DeepEqual(got, wantOptions) || source.reads.Load() <= firstReads {
		t.Fatalf("query options reused different admission: %+v", got)
	}
	optionsReads := source.reads.Load()
	if got := resolver.ResolveWithOptions("workers", "item.registered", options); !reflect.DeepEqual(got, wantOptions) || source.reads.Load() != optionsReads {
		t.Fatal("identical options did not reuse exact query preparation")
	}
	_, next := CompileConnectGraphWithInputProducerResolver(source)
	source.reads.Store(0)
	if got := next.Resolve("workers", "item.registered"); !reflect.DeepEqual(got, want) || source.reads.Load() == 0 {
		t.Fatal("new operation inherited old prepared query")
	}
}

func TestOperationInputProducerPreparationConcurrentMissOnce(t *testing.T) {
	base := operationScatterSource(t)
	want := ResolveFlowInputProducer(base, "workers", "item.registered")
	source := &operationInputReadSource{Source: base}
	_, control := CompileConnectGraphWithInputProducerResolver(source)
	source.reads.Store(0)
	control.Resolve("workers", "item.registered")
	wantReads := source.reads.Load()
	if wantReads == 0 {
		t.Fatal("control did not exercise input admission")
	}
	_, resolver := CompileConnectGraphWithInputProducerResolver(source)
	source.reads.Store(0)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got := resolver.Resolve("workers", "item.registered")
			if !reflect.DeepEqual(got, want) {
				t.Error("concurrent miss changed evidence")
				return
			}
			got.Evidence[0].Kind = "caller-private mutation"
		}()
	}
	close(start)
	wg.Wait()
	if got := source.reads.Load(); got != wantReads {
		t.Fatalf("concurrent identical misses prepared more than once: got=%d want=%d", got, wantReads)
	}
}

func BenchmarkScatterRepeatedInputProducerPreparation(b *testing.B) {
	source := operationScatterSource(b)
	graph, census := compileConnectGraphWithCensus(source)
	queries := [][2]string{{"workers", "item.registered"}, {"workers", "item.finished"}}
	for _, memoized := range []bool{false, true} {
		name := "shared_census_only"
		if memoized {
			name = "operation_prepared_queries"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				resolver := FlowInputProducerResolver{graph: graph, census: census}
				if memoized {
					resolver.preparation = &flowInputProducerPreparation{}
				}
				for instance := 0; instance < 32; instance++ {
					for _, query := range queries {
						if result := resolver.Resolve(query[0], query[1]); !result.HasEvidence() {
							b.Fatalf("lost exact producer evidence: %+v", result)
						}
					}
				}
			}
		})
	}
}
