package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// This diagnostic re-admits the exact source facts recorded by the served v7
// failure. It does not rewrite or republish an event in the running system.
func TestLifecycleTemplateCompletionConnectAdmissionDiagnostic(t *testing.T) {
	root := canonicalrouting.CopyLifecycleNestedTemplates(t)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Issues()) != 0 || len(graph.ReceiverPinCollisions()) != 0 {
		t.Fatalf("source connect admission failed: issues=%#v collisions=%#v", graph.Issues(), graph.ReceiverPinCollisions())
	}
	for _, sample := range []struct{ side, instance, entity string }{
		{"left", "outer/left/sink/ti-cd58230627f3cf454bfb310a", "3fd606ac-4fb9-56d0-a64b-e4c5317534d9"},
		{"right", "outer/right/sink/ti-96a53d6d1dbbd380dc98ccee", "cc4b78ef-cff8-5c75-9828-588d5bbac18c"},
	} {
		t.Run(sample.side, func(t *testing.T) {
			flow := "outer/" + sample.side + "/sink"
			eventType := flow + "/work.completed"
			producers := census.MatchingProducers(flow, eventType)
			var consumers []semanticview.AuthoredEventEndpoint
			for _, endpoint := range census.Consumers() {
				if endpoint.FlowID == flow+"/final" && endpoint.Event.Local == "work.completed" {
					consumers = append(consumers, endpoint)
				}
			}
			if len(producers) != 1 || producers[0].Kind != semanticview.EventEndpointGateOutcome || len(consumers) != 1 || consumers[0].Kind != semanticview.EventEndpointNodeHandler {
				t.Fatalf("gate/final census not admitted: producers=%#v consumers=%#v", producers, consumers)
			}
			var selected []pinrouting.ConnectRoutePlan
			for _, plan := range graph.Plans() {
				readback := plan.Readback()
				if readback.Source.FlowID == flow && readback.Receiver.FlowID == flow+"/final" && readback.Source.ResolvedEvent == eventType {
					selected = append(selected, plan)
					t.Logf("TEMPLATE_CONNECT_ADMITTED side=%s producer=%#v consumer=%#v plan=%#v", sample.side, producers[0], consumers[0], readback)
				}
			}
			if len(selected) != 1 {
				t.Fatalf("exact sink/final compiled plans=%d", len(selected))
			}
			routingSource, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: sample.instance, EntityID: sample.entity})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := pinrouting.AdmitSourceEvent(events.EventType(eventType), routingSource)
			if err != nil {
				t.Fatal(err)
			}
			actualPlans := graph.MatchingSourceEvent(actual)
			// Matcher-only control, not a proposed event rewrite or runtime proof.
			instanceQualified, err := pinrouting.AdmitSourceEvent(events.EventType(sample.instance+"/work.completed"), routingSource)
			if err != nil {
				t.Fatal(err)
			}
			controlPlans := graph.MatchingSourceEvent(instanceQualified)
			t.Logf("TEMPLATE_CONNECT_APPLICABILITY side=%s recorded_event=%s recorded_route=%#v recorded_matches=%d instance_qualified_control=%s control_matches=%d", sample.side, eventType, routingSource.Route(), len(actualPlans), sample.instance+"/work.completed", len(controlPlans))
			if len(actualPlans) != 1 {
				t.Errorf("admitted gate outcome cannot select its compiled sink/final route: matches=%d", len(actualPlans))
			}
		})
	}
}
