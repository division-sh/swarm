package pinrouting

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type outputCensusReadSource struct {
	semanticview.Source
	reads int
}

func (s *outputCensusReadSource) FlowScopes() []semanticview.FlowScope {
	s.reads++
	return s.Source.FlowScopes()
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
				got.classes[OutputConsumerClass(255)] = struct{}{}
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

// Reference classification with an independent census for each query.
func classifyOutputConsumerBeforeCensusReuse(source semanticview.Source, flowID, eventType string, routingSource events.RoutingSource) OutputConsumerClassification {
	classification := OutputConsumerClassification{classes: map[OutputConsumerClass]struct{}{}}
	if source == nil {
		return classification
	}
	if routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		if _, err := PublicationDeclarationForSourceEvent(events.EventType(eventType), routingSource); err != nil {
			return classification
		}
	}
	outputPins := outputPinsForEvent(source, flowID, eventType)
	graph := CompileConnectGraph(source)
	for _, pin := range outputPins {
		if routingSource.Empty() {
			classification.connects = append(classification.connects, graph.PlansFromOutputPin(flowID, pin)...)
		}
	}
	if !routingSource.Empty() {
		if sourceEvent, err := AdmitSourceEvent(events.EventType(eventType), routingSource); err == nil {
			classification.connects = append(classification.connects, graph.MatchingSourceEvent(sourceEvent)...)
		}
	}
	consumerEvent := eventType
	if routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		consumerEvent = semanticview.ResolveFlowEventProof(source, flowID, eventType).Local
	}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).MatchingConsumers(flowID, consumerEvent) {
		switch endpoint.Kind {
		case semanticview.EventEndpointNodeHandler, semanticview.EventEndpointAgent, semanticview.EventEndpointTimer:
			classification.classes[OutputConsumerSameFlow] = struct{}{}
		}
	}
	if len(classification.connects) > 0 {
		classification.classes[OutputConsumerConnect] = struct{}{}
	}
	if flowID == "" || flowID == "." {
		if _, exported := semanticview.SelectedRootOutputPin(source, eventType); exported {
			classification.classes[OutputConsumerRootExport] = struct{}{}
		}
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
