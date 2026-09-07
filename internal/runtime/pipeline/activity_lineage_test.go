package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestFreshActivityLineageRejectsCanonicallyRemintedForeignFacts(t *testing.T) {
	source := semanticview.Wrap(activityBoringFullFlowBundle(t, "http://proof.invalid"))
	parent := newActivityBoringSourceEvent(uuid.NewString(), uuid.NewString(), "https://input.invalid")
	intent := activityBoringExpectedIntentForSourceEvent(parent, "https://input.invalid")
	site := runtimecontracts.ActivitySitesForNode(mustActivityBoringNode("scanner"), source.ExecutableNodeEventHandlers(mustActivityBoringNode("scanner")))[0]
	result := runtimecontracts.ActivityResultEventsForSite(site)
	intent.RevisionEvent, intent.RejectedEvent = result.RevisionRequested, result.Rejected
	delivery := runtimedelivery.Snapshot{
		EventID: parent.ID(), RunID: parent.RunID(), Status: runtimedelivery.StatusDelivered,
		Route: events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustActivityBoringNode("scanner")), Target: events.MustExistingEntityTarget(intent.RoutingSource.Route())},
	}
	request, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FreshActivityRequestLineage(request.Event, parent, source, []runtimedelivery.Snapshot{delivery}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*runtimeengine.ActivityIntent)
	}{
		{"source_run", func(i *runtimeengine.ActivityIntent) { i.SourceRunID = uuid.NewString() }},
		{"causal_parent", func(i *runtimeengine.ActivityIntent) { i.SourceEventID = uuid.NewString() }},
		{"grandparent", func(i *runtimeengine.ActivityIntent) { i.ParentEventID = uuid.NewString() }},
		{"entity", func(i *runtimeengine.ActivityIntent) { i.EntityID = identity.NormalizeEntityID(uuid.NewString()) }},
		{"flow", func(i *runtimeengine.ActivityIntent) { i.ExecutionFlowID = identity.NormalizeFlowID("foreign") }},
		{"instance", func(i *runtimeengine.ActivityIntent) { i.FlowInstance = "research/foreign" }},
		{"handler", func(i *runtimeengine.ActivityIntent) { i.HandlerEventKey = "unrelated.event" }},
		{"activity", func(i *runtimeengine.ActivityIntent) { i.ActivityID = "foreign" }},
		{"tool", func(i *runtimeengine.ActivityIntent) { i.Tool = "foreign" }},
		{"result", func(i *runtimeengine.ActivityIntent) { i.SuccessEvent = "foreign.succeeded" }},
		{"effect", func(i *runtimeengine.ActivityIntent) {
			i.EffectClass = runtimecontracts.ActivityEffectClassNonIdempotentWrite
		}},
		{"retry", func(i *runtimeengine.ActivityIntent) { i.RetryMaxAttempts++ }},
		{"fork_policy", func(i *runtimeengine.ActivityIntent) { i.ForkPolicy = runtimecontracts.ActivityForkReuseRecordedResult }},
	} {
		t.Run(test.name, func(t *testing.T) {
			copy := intent
			test.mutate(&copy)
			forged, err := activityRequestEmitIntentFromAdmittedSource(copy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := FreshActivityRequestLineage(forged.Event, parent, source, []runtimedelivery.Snapshot{delivery}); err == nil {
				t.Fatal("canonical construction alone admitted foreign activity")
			}
		})
	}
	for _, status := range []runtimedelivery.Status{runtimedelivery.StatusPending, runtimedelivery.StatusDeadLetter} {
		t.Run(string(status), func(t *testing.T) {
			copy := delivery
			copy.Status = status
			if _, err := FreshActivityRequestLineage(request.Event, parent, source, []runtimedelivery.Snapshot{copy}); err == nil {
				t.Fatal("noncompleted parent admitted fresh request")
			}
		})
	}
}
