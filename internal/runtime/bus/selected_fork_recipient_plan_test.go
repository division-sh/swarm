package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestSelectedForkRecipientPlanScopesOnePublicationBeforePreparation(t *testing.T) {
	child, source, original := uuid.NewString(), uuid.NewString(), uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(child, source, original, "selected-owner", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.SelectedForkReplay(uuid.NewString(), "work.ready", eventtest.Producer(events.EventProducerPlatform, "selected-owner"),
		"", []byte(`{}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC())
	node := identitytest.FlowNode(t, ".", "unfinished")
	input := forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node), HandlerNode: node,
		HandlerEvent: "work.ready", Path: "."}
	selected, err := forkrecipient.NewLocal(input)
	if err != nil {
		t.Fatal(err)
	}
	sub := Subscriber{Recipient: selected.Recipient, Path: child, handlerNode: node, LocalizedEvent: "work.ready",
		targetHandler: pipeline.MustDeliveryTargetHandler(node).ForEvent("work.ready")}
	recipients := []forkrecipient.Evidence{selected}
	ctx, err := WithSelectedForkRecipientPlan(context.Background(), event, recipients)
	if err != nil {
		t.Fatal(err)
	}
	recipients[0] = forkrecipient.Evidence{}
	scope, found := selectedForkRecipientPlanFromContext(ctx, event)
	if !found || !scope.allowsSubscriber(sub, event) {
		t.Fatal("exact child publication lost its immutable unfinished root slot")
	}
	other := identitytest.FlowNode(t, ".", "settled")
	sub.Recipient, sub.handlerNode = events.MustNodeDeliveryRecipient(other), other
	sub.targetHandler = pipeline.MustDeliveryTargetHandler(other).ForEvent("work.ready")
	if scope.allowsSubscriber(sub, event) {
		t.Fatal("settled sibling was restored before receiver preparation")
	}
	otherEvent := eventtest.SelectedForkReplay(uuid.NewString(), event.Type(), eventtest.Producer(events.EventProducerPlatform, "selected-owner"),
		"", event.Payload(), 0, lineage, events.EventEnvelope{}, time.Now().UTC())
	if _, found := selectedForkRecipientPlanFromContext(ctx, otherEvent); found {
		t.Fatal("another selected publication borrowed the recipient scope")
	}
	ordinary := eventtest.ExistingRunRootIngress(uuid.NewString(), event.Type(), "operator", "", event.Payload(), 0, child, events.EventEnvelope{}, time.Now().UTC())
	if _, found := selectedForkRecipientPlanFromContext(ctx, ordinary); found {
		t.Fatal("normal follow-up inherited the initial-publication filter")
	}
	if _, err := WithSelectedForkRecipientPlan(ctx, ordinary, []forkrecipient.Evidence{selected}); err == nil {
		t.Fatal("ordinary publication acquired selected recipient scope")
	}
	if _, err := WithSelectedForkRecipientPlan(ctx, event, nil); err == nil {
		t.Fatal("unfinished ordinal admitted an empty selected partition")
	}
	ctx = correlation.WithInboundEvent(ctx, event)
	if _, found := selectedForkRecipientPlanForInbound(ctx); !found {
		t.Fatal("connect planning lost the exact inbound scope")
	}
	ctx = correlation.WithInboundEvent(ctx, ordinary)
	if _, found := selectedForkRecipientPlanForInbound(ctx); found {
		t.Fatal("ordinary follow-up borrowed connect-plan narrowing")
	}
}

func TestSelectedForkRecipientPlanConsumesCompiledHandlerEventProjection(t *testing.T) {
	source := mixedPubsubConnectStaticSource(t)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Plans()) == 0 {
		t.Fatal("fixture lacks its actual compiled connection")
	}
	plan := graph.Plans()[0]
	registrations := table.connectRecipientAdmissionsForRun(busInternalTestRunID)
	materialized := pinrouting.MaterializeConnectRoutePlan(plan, pinrouting.ConnectRoutePlanMaterializationInput{})
	evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{materialized.Target}, registrations)
	if _, err := evaluation.Ledger(); err != nil || len(evaluation.Recipients()) == 0 {
		t.Fatalf("canonical graph control has no receiver: %+v err=%v", evaluation.Recipients(), err)
	}
	id, err := pinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	var selected []forkrecipient.Evidence
	for _, recipient := range evaluation.Recipients() {
		evidence, err := forkrecipient.NewConnect(forkrecipient.Input{
			Recipient: events.MustNodeDeliveryRecipient(recipient.Handler().Node()), Path: recipient.Path(),
			HandlerNode: recipient.Handler().Node(), HandlerEvent: recipient.HandlerEvent(),
		}, id, plan.ReceiverPinIdentity().EvidenceIdentity())
		if err != nil {
			t.Fatal(err)
		}
		selected = append(selected, evidence)
	}
	scope := selectedForkRecipientPlan{recipients: selected}
	matched := 0
	for _, registration := range graph.ScopeRecipientRegistrations(plan, []events.RouteIdentity{materialized.Target}, registrations) {
		if registration.Recipient().HandlerEvent() != "" {
			t.Fatal("fixture no longer exercises an unlocalized registration")
		}
		if scope.allowsRegistration(plan, registration) {
			matched++
		}
		if scope.allowsRegistration(pinrouting.ConnectRoutePlan{}, registration) {
			t.Fatal("registration borrowed another plan's handler event")
		}
		settled := selectedForkRecipientPlan{}
		if settled.allowsRegistration(plan, registration) {
			t.Fatal("compiled handler projection restored a settled recipient")
		}
		if registration.Recipient().HandlerEvent() != "" {
			t.Fatal("compiled handler projection mutated the registration")
		}
	}
	if matched != len(selected) {
		t.Fatalf("filter dropped compiled handlers before admission: matched=%d expected=%d", matched, len(selected))
	}
}
