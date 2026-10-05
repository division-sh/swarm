package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
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
	lineage, err := FreshActivityRequestLineage(request.Event, parent, source, []ActivityParentExecution{{Delivery: delivery}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= intent.RetryMaxAttempts; attempt++ {
		copy := intent
		copy.Attempt = attempt
		emissions := &pipelineEmissionPlan{}
		dispatcher := pipelineActivityDispatcher{emissions: emissions}
		if err := dispatcher.publishActivitySuccess(context.Background(), copy, map[string]any{}); err != nil {
			t.Fatal(err)
		}
		if err := lineage.ValidateResult(emissions.immutableEvents()[0]); err != nil {
			t.Fatalf("request phase restriction rejected result attempt %d: %v", attempt, err)
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*runtimeengine.ActivityIntent)
	}{
		{"request_attempt_2", func(i *runtimeengine.ActivityIntent) { i.Attempt = 2 }},
		{"request_attempt_99", func(i *runtimeengine.ActivityIntent) { i.Attempt = 99 }},
		{"non_loop_stage", func(i *runtimeengine.ActivityIntent) { i.LoopStage = "foreign" }},
		{"non_loop_generation", func(i *runtimeengine.ActivityIntent) {
			i.Generation = attemptgeneration.Generation{FlowID: "foreign", LoopID: "foreign", ActivationID: "foreign", RevisionField: "revision", RevisionID: "foreign", Attempt: 1}
			i.LoopStage = "foreign"
		}},
		{"malformed_generation", func(i *runtimeengine.ActivityIntent) { i.Generation.LoopID = "partial" }},
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
			if _, err := FreshActivityRequestLineage(forged.Event, parent, source, []ActivityParentExecution{{Delivery: delivery}}, nil); err == nil {
				t.Fatal("canonical construction alone admitted foreign activity")
			}
		})
	}
	for _, status := range []runtimedelivery.Status{runtimedelivery.StatusPending, runtimedelivery.StatusDeadLetter} {
		t.Run(string(status), func(t *testing.T) {
			copy := delivery
			copy.Status = status
			if _, err := FreshActivityRequestLineage(request.Event, parent, source, []ActivityParentExecution{{Delivery: copy}}, nil); err == nil {
				t.Fatal("noncompleted parent admitted fresh request")
			}
		})
	}
}

func TestFreshRootActivityLineageSeparatesProducerAndReceiverCoordinates(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml": "name: root-activity-lineage\nstages: []\n",
		"events.yaml": "request:\n",
		"nodes.yaml":  "scanner:\n  execution_type: system_node\n  subscribes_to: [request]\n  event_handlers:\n    request:\n      activity: {id: fetch, tool: source_scrape, input: {}}\n",
		"tools.yaml":  "source_scrape:\n  description: Read-only lineage control.\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: 'https://proof.invalid'}\n",
	}))
	runID := uuid.NewString()
	parent := eventtest.ExistingRunRootIngress(uuid.NewString(), "request", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	node := identitytest.RootNode(t, "scanner")
	site := runtimecontracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node))[0]
	result := runtimecontracts.ActivityResultEventsForSite(site)
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(runtimecontracts.ActivityEffectClassReadOnly)
	intent := runtimeengine.ActivityIntent{
		RoutingSource: eventtest.StaticFlowRoutingSource(".", runID, runID), EntityID: identity.NormalizeEntityID(runID), FlowInstance: runID,
		Owner: activityidentity.MustNodeOwner(node), ExecutionFlowID: identity.NormalizeFlowID("."),
		ActivityID: result.ActivityID, Tool: "source_scrape", Input: mustActivityInput(map[string]any{}),
		EffectClass: runtimecontracts.ActivityEffectClassReadOnly, ForkPolicy: runtimecontracts.ActivityForkReexecuteRead,
		SuccessEvent: result.SuccessEvent, FailureEvent: result.FailureEvent, RevisionEvent: result.RevisionRequested, RejectedEvent: result.Rejected,
		RetryMaxAttempts: defaults.MaxAttempts, RetryBackoff: defaults.Backoff, HandlerEventKey: "request",
		SourceRunID: runID, SourceEventID: parent.ID(), Attempt: 1, ExecutionMode: parent.ExecutionMode(),
	}
	delivery := runtimedelivery.Snapshot{EventID: parent.ID(), RunID: runID, Status: runtimedelivery.StatusDelivered,
		Route: events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})},
	}
	for _, cell := range []string{"exact", "foreign_flow", "foreign_instance", "foreign_entity", "static_producer", "nonterminal_parent"} {
		t.Run(cell, func(t *testing.T) {
			copy, execution := intent, delivery
			switch cell {
			case "foreign_flow":
				copy.ExecutionFlowID = identity.NormalizeFlowID("foreign")
			case "foreign_instance":
				copy.FlowInstance = uuid.NewString()
			case "foreign_entity":
				copy.EntityID = identity.NormalizeEntityID(uuid.NewString())
			case "static_producer":
				copy.RoutingSource = eventtest.StaticFlowRoutingSource("foreign", "foreign", runID)
			case "nonterminal_parent":
				execution.Status = runtimedelivery.StatusPending
			}
			request, err := activityRequestEmitIntentFromAdmittedSource(copy)
			if err != nil {
				t.Fatal(err)
			}
			_, err = FreshActivityRequestLineage(request.Event, parent, source, []ActivityParentExecution{{Delivery: execution}}, nil)
			if cell == "exact" && err != nil {
				t.Fatalf("valid root producer/receiver refused: %v", err)
			}
			if cell != "exact" && err == nil {
				t.Fatal("foreign root execution or producer was admitted")
			}
		})
	}
}
