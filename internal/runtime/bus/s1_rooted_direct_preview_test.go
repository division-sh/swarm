package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1DirectRecipientPreviewAdmitsFrozenRootBeforeSourceConsumers(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("direct preview control lacks its compiled bundle")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := sourceartifactfixture.Rooted(t, "other-root", rooted.Artifact())
	if rooted.Admission().Equal(foreign.Admission()) || rooted.Artifact().BundleHash() != foreign.Artifact().BundleHash() {
		t.Fatal("root conflict control does not preserve equal content")
	}
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	base := correlation.WithSourceArtifactFact(context.Background(), fact)
	canceled, cancel := context.WithCancel(base)
	cancel()
	for _, test := range []struct {
		name  string
		ctx   context.Context
		valid bool
	}{
		{name: "plain", ctx: context.Background(), valid: true},
		{name: "content-only-context", ctx: base, valid: true},
		{name: "matching", ctx: correlation.WithRootAdmission(base, rooted.Admission()), valid: true},
		{name: "same-content-other-root", ctx: correlation.WithRootAdmission(base, foreign.Admission())},
		{name: "explicit-clear", ctx: correlation.WithRootAdmission(base, sourceartifact.RootAdmission{})},
		{name: "root-without-content", ctx: correlation.WithRootAdmission(context.Background(), rooted.Admission())},
		{name: "canceled", ctx: canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected := newConnectRoutePlanStaticStore()
			payloadCalls := 0
			bus, err := newScopedTestEventBus(selected, EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact,
				PayloadAdmitter: func(ctx context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
					payloadCalls++
					root, present, err := correlation.RootAdmissionFromContext(ctx)
					if err != nil || !present || !root.Equal(rooted.Admission()) {
						return events.PayloadAdmission{}, fmt.Errorf("direct preview consumer lacks exact publisher root: present=%t err=%v", present, err)
					}
					return eventtest.PayloadAdmission(event, flow, "bootstrap.requested")
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			const agentID = "preview-agent"
			received := subscribeTestAgent(t, bus, agentID, events.EventType("bootstrap.requested"))
			identity := testAgentRouteIdentity(t, agentID, "")
			requested := []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}}
			event := eventtest.RunCreatingRootIngress(eventtest.UUID("rooted-direct-preview"), "bootstrap.requested", "operator", "", []byte(`{}`), 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
			status, err := bus.CheckDirectRoutes(test.ctx, event, requested)
			if test.valid {
				if err != nil || payloadCalls != 1 || len(status.Deliverable) != 1 || status.Deliverable[0].AgentIdentity != identity {
					t.Fatalf("valid direct preview lost its frozen source or exact recipient: payload_calls=%d status=%+v err=%v", payloadCalls, status, err)
				}
			} else if err == nil || payloadCalls != 0 || len(status.Deliverable) != 0 {
				t.Fatalf("invalid direct preview reached source consumers: payload_calls=%d err=%v", payloadCalls, err)
			}
			if test.name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("direct preview concealed cancellation: %v", err)
			}
			if len(selected.events) != 0 || len(selected.routes) != 0 || len(status.Missing) != 0 || len(status.Requested) != 1 || status.Requested[0].AgentIdentity != identity {
				t.Fatal("direct inspection persisted work or changed requested delivery identity")
			}
			select {
			case got := <-received:
				t.Fatalf("preview dispatched a local delivery: %v", got)
			default:
			}
		})
	}
}
