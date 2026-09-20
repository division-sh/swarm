package pinrouting

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type outputCensusReadSource struct {
	semanticview.Source
	reads int
}

func (s *outputCensusReadSource) AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	s.reads++
	return s.Source.AuthoredEventEntries()
}

func TestOutputConsumerSharesOnlyOperationLocalCensus(t *testing.T) {
	base := operationScatterSource(t)
	control := &outputCensusReadSource{Source: base}
	semanticview.BuildAuthoredEventEndpointCensus(control)
	censusReads := control.reads
	if censusReads == 0 {
		t.Fatal("census probe observed no source reads")
	}
	for _, query := range [][2]string{{".", "batch.submitted"}, {".", "batch.finished"}, {"workers", "item.registered"}, {"workers", "item.finished"}, {"collector", "item.reported"}, {"workers", "absent"}} {
		t.Run(query[0]+"/"+query[1], func(t *testing.T) {
			before := &outputCensusReadSource{Source: base}
			after := &outputCensusReadSource{Source: base}
			for call := 1; call <= 2; call++ {
				want := classifyOutputConsumerBeforeCensusReuse(before, query[0], query[1], events.NoRoutingSource())
				got := ClassifyOutputConsumer(after, query[0], query[1])
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("classification changed: got=%+v want=%+v", got, want)
				}
				if before.reads-after.reads != call*censusReads {
					t.Fatalf("call %d: source reads before=%d after=%d; want exactly one census (%d reads) removed per call", call, before.reads, after.reads, censusReads)
				}
				// Neither the census nor caller-owned classification is retained.
				got.classes[OutputConsumerHarness] = struct{}{}
			}
		})
	}
}

func TestOutputConsumerCensusReusePreservesRoutingSources(t *testing.T) {
	base := operationScatterSource(t)
	root, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	child, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "workers", FlowInstance: "workers/one", EntityID: "child-entity"})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []semanticview.Source{nil, base} {
		for _, routing := range []events.RoutingSource{events.NoRoutingSource(), root, child, events.NewPlatformControlRoutingSource()} {
			for _, event := range []string{"batch.submitted", "batch.finished", "item.finished", "absent"} {
				want := classifyOutputConsumerBeforeCensusReuse(source, routing.Route().FlowID, event, routing)
				got := ClassifyRoutingSourceOutputConsumer(source, event, routing)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("routing=%+v event=%s: got=%+v want=%+v", routing, event, got, want)
				}
			}
		}
	}
}

// Frozen pre-change classification, including its second independent census.
func classifyOutputConsumerBeforeCensusReuse(source semanticview.Source, flowID, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	classification := OutputConsumerClassification{classes: map[OutputConsumerClass]struct{}{}}
	if source == nil {
		return classification
	}
	outputPins := outputPinsForEvent(source, flowID, eventType)
	graph := CompileConnectGraph(source)
	for _, pin := range outputPins {
		if !pin.Sink().Valid() {
			classification.invalidSink = true
			continue
		}
		if pin.Sink() == runtimecontracts.FlowOutputSinkHarness {
			classification.classes[OutputConsumerHarness] = struct{}{}
		}
		if routingSource.Empty() {
			classification.connects = append(classification.connects, graph.PlansFromOutputPin(flowID, pin)...)
		}
	}
	if !routingSource.Empty() {
		if sourceEvent, err := AdmitSourceEvent(events.EventType(eventType), routingSource); err == nil {
			classification.connects = append(classification.connects, graph.MatchingSourceEvent(sourceEvent)...)
		}
	}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).MatchingConsumers(flowID, eventType) {
		if endpoint.Kind != semanticview.EventEndpointExternal {
			classification.classes[OutputConsumerSameFlow] = struct{}{}
			break
		}
	}
	if len(classification.connects) > 0 {
		classification.classes[OutputConsumerConnect] = struct{}{}
	}
	if structuralParentRouteEligible(source, flowID) {
		classification.classes[OutputConsumerStructuralParent] = struct{}{}
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	if ok && entry.AcceptedConsumerBoundary() == runtimecontracts.EventConsumerBoundaryExternal {
		classification.classes[OutputConsumerExternal] = struct{}{}
	}
	return classification
}

func BenchmarkOutputConsumerCensusReuse(b *testing.B) {
	source := operationScatterSource(b)
	for _, tc := range []struct {
		name     string
		classify func(semanticview.Source, string, string, events.RoutingSource) OutputConsumerClassification
	}{{"before", classifyOutputConsumerBeforeCensusReuse}, {"after", classifyOutputConsumer}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tc.classify(source, "workers", "item.finished", events.NoRoutingSource())
			}
		})
	}
}
