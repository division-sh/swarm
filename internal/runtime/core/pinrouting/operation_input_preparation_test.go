package pinrouting

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

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
