package bus

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestSourceEventRecipientsRespectRunReplacementAndRemoval(t *testing.T) {
	const runA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const runB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const concrete = "account/one/account.ready"
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	reader := &constructionIndexTestReader{}
	resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
	ctx := constructionIndexContext(t, source)
	routing := eventtest.ConcreteTemplateRoutingSource("account", "account/one", eventtest.UUID("one"))
	sibling := eventtest.ConcreteTemplateRoutingSource("account", "account/two", eventtest.UUID("two"))
	resolve := func(run string, name events.EventType, routing events.RoutingSource) []Subscriber {
		t.Helper()
		event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("source-event"), name, "", "", nil, 0, run, events.EventEnvelope{}, routing, time.Now().UTC())
		got, err := resolver.resolvePubsubSubscribers(ctx, event, SourceEventRouteKeys(name, routing), ordinaryPublicationSource{})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, activeRun := range []string{runA, runB, ""} {
		reader.observations = nil
		if activeRun != "" {
			instance := ConstructedFlowInstanceIdentityFixture(source, "account", "one", activeRun)
			reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, activeRun, instance, "42")}
		}
		for _, run := range []string{runA, runB} {
			for _, name := range []events.EventType{"account.ready", "account/account.ready", concrete} {
				got := resolve(run, name, routing)
				if run == activeRun {
					if len(got) != 1 || got[0].Recipient.LocalID() != "account-node" || got[0].Path != "account/one" || got[0].routeSource == subscriberRouteSourceConnectRoutePlan {
						t.Fatalf("run=%s event=%s active=%s: local consumer or authority changed: %+v", run, name, activeRun, got)
					}
				} else if len(got) != 0 {
					t.Fatalf("run=%s inherited active=%s recipients: %+v", run, activeRun, got)
				}
			}
			if got := resolve(run, "account/account.ready", sibling); len(got) != 0 {
				t.Fatalf("absent sibling inherited occurrence recipients: %+v", got)
			}
			if got := resolve(run, "account/account.ready", events.NoRoutingSource()); len(got) != 0 {
				t.Fatalf("absent source inherited occurrence recipients: %+v", got)
			}
		}
	}
}
