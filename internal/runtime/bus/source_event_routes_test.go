package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestSourceEventRecipientsRespectRunReplacementAndRemoval(t *testing.T) {
	const runA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const runB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const concrete = "worker/one/work.ready"
	source := eventtest.ConcreteTemplateRoutingSource("worker", "worker/one", eventtest.UUID("one"))
	sibling := eventtest.ConcreteTemplateRoutingSource("worker", "worker/two", eventtest.UUID("two"))
	local := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "listener")), Path: "worker/one", routeSource: subscriberRouteSourceSubscription}
	connected := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "connected")), routeSource: subscriberRouteSourceConnectRoutePlan}
	rt := newRouteTable(nil)
	rt.eventPath[concrete] = struct{}{}
	for _, activeRun := range []string{runA, runB, ""} {
		rt.patterns = nil
		if activeRun != "" {
			rt.patterns = []routePattern{
				{RunID: activeRun, EventPattern: concrete, Subscriber: local},
				{RunID: activeRun, EventPattern: concrete, Subscriber: connected},
			}
		}
		rt.rebuildLocked()
		for _, run := range []string{runA, runB} {
			for _, name := range []events.EventType{"work.ready", "worker/work.ready", concrete} {
				got := rt.ResolveIndependentPubsubFromSource(run, name, source)
				if run == activeRun {
					if len(got) != 1 || got[0].Recipient != local.Recipient || got[0].Path != local.Path {
						t.Fatalf("run=%s event=%s active=%s: local consumer or authority changed: %+v", run, name, activeRun, got)
					}
				} else if len(got) != 0 {
					t.Fatalf("run=%s inherited active=%s routes: %+v", run, activeRun, got)
				}
			}
			if got := rt.ResolveIndependentPubsubFromSource(run, "worker/work.ready", sibling); len(got) != 0 {
				t.Fatalf("unregistered sibling inherited occurrence routes: %+v", got)
			}
			if got := rt.ResolveIndependentPubsubFromSource(run, "worker/work.ready", events.NoRoutingSource()); len(got) != 0 {
				t.Fatalf("absent source inherited occurrence routes: %+v", got)
			}
		}
	}
}
