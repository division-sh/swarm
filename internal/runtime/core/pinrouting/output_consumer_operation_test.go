package pinrouting

import (
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func outputOperationSources(t testing.TB) []events.RoutingSource {
	t.Helper()
	root, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	child, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "workers", FlowInstance: "workers/one", EntityID: "child-entity"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "collector", FlowInstance: "collector/one", EntityID: "other-entity"})
	if err != nil {
		t.Fatal(err)
	}
	return []events.RoutingSource{events.NoRoutingSource(), root, child, other, events.NewPlatformControlRoutingSource()}
}

func TestOutputConsumerOperationPreservesEveryClassification(t *testing.T) {
	for _, source := range []semanticview.Source{nil, operationScatterSource(t)} {
		resolver := NewOutputConsumerResolver(source)
		for _, routing := range outputOperationSources(t) {
			for _, event := range []string{"batch.submitted", "batch.finished", "item.registered", "item.finished", "item.reported", "workers/item.finished", "absent", ""} {
				want := ClassifyRoutingSourceOutputConsumer(source, event, routing)
				for attempt := 0; attempt < 3; attempt++ {
					got := resolver.Classify(event, routing)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("routing=%+v event=%q attempt=%d: got=%+v want=%+v", routing, event, attempt, got, want)
					}
					got.classes[OutputConsumerHarness] = struct{}{}
					if len(got.connects) > 0 {
						got.connects[0] = ConnectRoutePlan{}
					}
				}
			}
		}
	}
}

func TestOutputConsumerOperationCompilesOnceAndNeverCrossesOperations(t *testing.T) {
	source := &censusCountingSource{Source: operationScatterSource(t)}
	resolver := NewOutputConsumerResolver(source)
	if source.builds != 0 {
		t.Fatal("unused resolver read source evidence")
	}
	for _, routing := range outputOperationSources(t) {
		for _, event := range []string{"batch.submitted", "item.finished", "absent"} {
			resolver.Classify(event, routing)
		}
	}
	if source.builds != 1 {
		t.Fatalf("one preparation built %d censuses", source.builds)
	}
	// The next operation must compile its own source, including a changed
	// declaration view; no resolver is retained by a bus or durable plan.
	source.withdraw = true
	next := NewOutputConsumerResolver(source)
	routing := outputOperationSources(t)[2]
	got := next.Classify("item.finished", routing)
	if source.builds != 2 {
		t.Fatalf("next operation retained old compilation: builds=%d", source.builds)
	}
	want := ClassifyRoutingSourceOutputConsumer(source, "item.finished", routing)
	if source.builds != 3 || !reflect.DeepEqual(got, want) {
		t.Fatalf("new source classification differs: builds=%d got=%+v want=%+v", source.builds, got, want)
	}
}

func TestOutputConsumerOperationConcurrentResultsAreIsolated(t *testing.T) {
	base := operationScatterSource(t)
	source := &censusCountingSource{Source: base}
	resolver := NewOutputConsumerResolver(source)
	routing := outputOperationSources(t)[2]
	want := ClassifyRoutingSourceOutputConsumer(base, "item.finished", routing)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				got := resolver.Classify("item.finished", routing)
				if !reflect.DeepEqual(got, want) {
					t.Error("concurrent classification substituted evidence")
					return
				}
				got.classes[OutputConsumerHarness] = struct{}{}
				if len(got.connects) > 0 {
					got.connects[0] = ConnectRoutePlan{}
				}
			}
		}()
	}
	wg.Wait()
	if source.builds != 1 {
		t.Fatalf("concurrent first use compiled %d times", source.builds)
	}
}

func BenchmarkOutputConsumerPublicationChunk(b *testing.B) {
	source := operationScatterSource(b)
	routing := outputOperationSources(b)[2]
	for _, shared := range []bool{false, true} {
		name := "per_item"
		if shared {
			name = "per_preparation"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				resolver := NewOutputConsumerResolver(source)
				for ordinal := 0; ordinal < 25; ordinal++ {
					if shared {
						resolver.Classify("item.finished", routing)
					} else {
						ClassifyRoutingSourceOutputConsumer(source, "item.finished", routing)
					}
				}
			}
		})
	}
}
