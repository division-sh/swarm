package bus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// This uses the production metadata resolver, not a native construction or
// publication journey. It must not be credited as the missing durable join.
func TestS6CreationReferenceOrderMetadataDiagnostic(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopySelectedForkLocalReadiness(t, 1, "node"))
	schema, found := source.FlowSchemaByID("worker-flow")
	if !found || schema.AutoEmitOnCreate.Event != "worker.inspect.requested" {
		t.Fatalf("compiled auto-emit reference: schema=%+v found=%t", schema, found)
	}
	eb := &EventBus{semanticSource: source}
	scope := authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleHash)
	ctx := authoractivity.WithScope(context.Background(), scope)
	routingSource := eventtest.ConcreteTemplateRoutingSource("worker-flow", "worker-flow/worker-001", eventtest.UUID("s6-order-owner"))
	for _, row := range []struct {
		local string
		story authoractivity.StoryDisposition
	}{
		{local: "worker.inspect", story: authoractivity.StoryAuthored},
		{local: schema.AutoEmitOnCreate.Event, story: authoractivity.StoryAuthored},
	} {
		t.Run(row.local, func(t *testing.T) {
			name := "worker-flow/worker-001/" + row.local
			event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(name), events.EventType(name), "flow-instance-activator", "", nil, 0,
				eventtest.UUID("s6-order-run"), events.EnvelopeForSourceRoute(events.EventEnvelope{}, routingSource.Route()), routingSource, time.Now().UTC())
			before := event.Clone()
			resolved, err := eb.withAuthorActivityEventDescriptor(ctx, event)
			if err != nil {
				t.Fatal(err)
			}
			descriptor, found, err := authoractivity.ResolvedEventDescriptorFromContext(resolved, scope, name)
			if err != nil || !found || descriptor.EventType != name || descriptor.Disposition != row.story {
				t.Fatalf("actual publication metadata: descriptor=%+v found=%t err=%v", descriptor, found, err)
			}
			admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
				AgentID: "worker-agent", FlowID: "worker-flow", FlowPath: "worker-flow/worker-001", Subscriptions: []string{row.local},
			})
			if err != nil || !admission.ValidForAgent("worker-agent") || !reflect.DeepEqual(admission.RoutePatterns(), []string{name}) {
				t.Fatalf("compiled local agent admission: patterns=%v err=%v", admission.RoutePatterns(), err)
			}
			if !reflect.DeepEqual(before, event) {
				t.Fatal("metadata resolution rewrote the publication")
			}
			t.Logf("exact source publication %s -> %s; agent eligibility retained; compiled/metadata characterization only", name, descriptor.Disposition)
		})
	}
}
