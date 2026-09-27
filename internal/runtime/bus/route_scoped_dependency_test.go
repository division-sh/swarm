package bus

import (
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func TestCompiledRouteDependenciesPreserveCrossInstanceObserverInBothCreationOrders(t *testing.T) {
	graph := runtimepinrouting.CompiledConnectGraph{}
	runID := busInternalTestRunID
	observer := routeTemplateSourceObserver{
		RunID: runID, SourceTemplatePath: "producer", SourceLocalEvent: "work.ready",
		Subscriber:             Subscriber{Recipient: events.MustAgentDeliveryRecipient("observer-agent")},
		SubscriberInstancePath: "observer/one",
	}
	for _, observerFirst := range []bool{false, true} {
		name := "source_first"
		if observerFirst {
			name = "observer_first"
		}
		t.Run(name, func(t *testing.T) {
			rt := newRouteTableWithGraph(nil, graph)
			owners := make([]runtimeflowidentity.RunScopedFlowInstance, 0, 2)
			for _, instance := range []string{"one", "two"} {
				owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.DeriveRoute("producer", instance))
				if err != nil {
					t.Fatal(err)
				}
				owners = append(owners, owner)
				rt.eventPath[owner.Route.InstancePath+"/work.ready"] = struct{}{}
			}
			if observerFirst {
				rt.addTemplateSourceObserverLocked(observer)
			}
			for _, owner := range owners {
				rt.instanceOwners[owner] = owner
				rt.materializeTemplateSourceObserversLocked(owner)
			}
			if !observerFirst {
				rt.addTemplateSourceObserverLocked(observer)
			}
			gotPatterns := make([]string, 0, len(rt.patterns))
			for _, pattern := range rt.patterns {
				if pattern.RunID != runID || pattern.InstancePath != "observer/one" || pattern.SourceInstancePath == "" || pattern.Subscriber.Recipient.ID() != "observer-agent" {
					t.Fatalf("cross-instance observer pattern = %#v", pattern)
				}
				gotPatterns = append(gotPatterns, pattern.EventPattern)
			}
			sort.Strings(gotPatterns)
			if want := []string{"producer/one/work.ready", "producer/two/work.ready"}; !reflect.DeepEqual(gotPatterns, want) {
				t.Fatalf("observer patterns = %#v, want %#v", gotPatterns, want)
			}

			dependencies := rt.compiledRouteOwnerDependencies(runtimepinrouting.FlowInputProducerResolver{})
			selection := graph.SelectRouteDependencies([]string{"producer"}, dependencies)
			if !reflect.DeepEqual(selection.ContextFlowPaths, []string{"observer"}) || !reflect.DeepEqual(selection.AffectedFlowPaths, []string{"observer"}) {
				t.Fatalf("source activation dependencies = %#v, want observer context and affected owner", selection)
			}
			reverse := graph.SelectRouteDependencies([]string{"observer"}, dependencies)
			if !reflect.DeepEqual(reverse.ContextFlowPaths, []string{"producer"}) || len(reverse.AffectedFlowPaths) != 0 {
				t.Fatalf("observer activation dependencies = %#v, want producer context without unrelated owner replacement", reverse)
			}
		})
	}
}

func TestObserverRouteReplacementRetainsOlderProducerFamilies(t *testing.T) {
	for _, observerFirst := range []bool{false, true} {
		name := "producers_first"
		if observerFirst {
			name = "observer_first"
		}
		t.Run(name, func(t *testing.T) {
			rt := newRouteTableWithGraph(nil, runtimepinrouting.CompiledConnectGraph{})
			observerOwner := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("observer", "one"))
			rt.instanceOwners[observerOwner] = observerOwner
			for _, source := range []struct{ flow, event string }{{"producer", "work.ready"}, {"other", "other.ready"}} {
				observe := routeTemplateSourceObserver{
					RunID: busInternalTestRunID, SourceTemplatePath: source.flow, SourceLocalEvent: source.event,
					Subscriber:             Subscriber{Recipient: events.MustAgentDeliveryRecipient("observer-agent")},
					SubscriberInstancePath: observerOwner.Route.InstancePath,
				}
				if observerFirst {
					rt.addTemplateSourceObserverLocked(observe)
				}
			}
			addProducer := func(flow, id, event string) {
				owner := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute(flow, id))
				rt.instanceOwners[owner] = owner
				rt.eventPath[owner.Route.InstancePath+"/"+event] = struct{}{}
				rt.materializeTemplateSourceObserversLocked(owner)
			}
			addProducer("producer", "old-a", "work.ready")
			addProducer("producer", "old-b", "work.ready")
			addProducer("other", "old", "other.ready")
			if !observerFirst {
				for _, source := range []struct{ flow, event string }{{"producer", "work.ready"}, {"other", "other.ready"}} {
					rt.addTemplateSourceObserverLocked(routeTemplateSourceObserver{
						RunID: busInternalTestRunID, SourceTemplatePath: source.flow, SourceLocalEvent: source.event,
						Subscriber:             Subscriber{Recipient: events.MustAgentDeliveryRecipient("observer-agent")},
						SubscriberInstancePath: observerOwner.Route.InstancePath,
					})
				}
			}
			before := rt.MaterializedRoutes(observerOwner)
			if len(before) != 3 {
				t.Fatalf("initial observer routes = %#v, want three older producers", before)
			}
			addProducer("producer", "new", "work.ready")
			got := rt.MaterializedRoutes(observerOwner)
			patterns := make([]string, 0, len(got))
			for _, route := range got {
				patterns = append(patterns, route.EventPattern)
			}
			sort.Strings(patterns)
			want := []string{"other/old/other.ready", "producer/new/work.ready", "producer/old-a/work.ready", "producer/old-b/work.ready"}
			if !reflect.DeepEqual(patterns, want) {
				t.Fatalf("complete replacement patterns = %#v, want %#v", patterns, want)
			}
		})
	}
}
