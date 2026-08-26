package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type inboundBatchAuthorizationVerifier struct {
	expected runtimeprovideroutput.Authorization
}

func (v inboundBatchAuthorizationVerifier) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	if !v.expected.Matches(actual) {
		return errors.New("authorization does not match current compiled owner")
	}
	return nil
}

func TestPrepareInboundDeliveryBatchRejectsInvalidProviderOutputAuthorizationBeforeMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	if _, err := runtimeprovideroutput.NewAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0", "", expected.Generation(),
	); err == nil {
		t.Fatal("incomplete provider-output authorization acquired authority")
	}
	testCases := []struct {
		name   string
		mutate func(*InboundDeliveryBatch)
	}{
		{name: "missing authorization", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[1].Authorization = runtimeprovideroutput.Authorization{}
		}},
		{name: "provider mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Provider = "telegram-stale"
		}},
		{name: "event mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[1].Event = inboundBatchPreflightEvent("inbound.telegram.edited_message")
		}},
		{name: "pack id mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), "provider.telegram.stale", a.PackVersion(), a.ManifestHash(), a.Generation(),
			)
		}},
		{name: "pack version mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), "0.9.0", a.ManifestHash(), a.Generation(),
			)
		}},
		{name: "manifest hash mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), a.PackVersion(), "sha256:"+strings.Repeat("b", 64), a.Generation(),
			)
		}},
		{name: "stale generation", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), a.PackVersion(), a.ManifestHash(),
				triggergeneration.FromCanonicalBytes([]byte("generation-stale")),
			)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &InMemoryEventStore{}
			bus, err := newScopedTestEventBus(store, EventBusOptions{
				ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := inboundBatchPreflightBatch(expected)
			tc.mutate(&batch)
			if _, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatch error = nil, want fail-closed authorization rejection")
			}
		})
	}
}

func TestPrepareInboundDeliveryBatchAcceptsOnlyExactCurrentProviderOutputAuthorizationIntoMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	store := &InMemoryEventStore{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{
		ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	batch := inboundBatchPreflightBatch(expected)
	plan, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("PrepareInboundDeliveryBatch: %v", err)
	}
	if got := len(plan.CommitCommands()); got != 2 {
		t.Fatalf("CommitCommands = %d, want 2", got)
	}
}

func TestPrepareInboundDeliveryBatchRejectsNonExclusiveOrMisorderedOutputsBeforeMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	testCases := []struct {
		name   string
		mutate func(*InboundDeliveryBatch)
	}{
		{name: "normalized only", mutate: func(batch *InboundDeliveryBatch) { batch.Events = batch.Events[1:] }},
		{name: "raw at ordinal one", mutate: func(batch *InboundDeliveryBatch) { batch.Events[0], batch.Events[1] = batch.Events[1], batch.Events[0] }},
		{name: "two normalized branches", mutate: func(batch *InboundDeliveryBatch) {
			second := batch.Events[1]
			second.Event = inboundBatchPreflightEvent("inbound.telegram.edited_message")
			a := second.Authorization
			second.Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), "inbound.telegram.edited_message", a.PackID(), a.PackVersion(), a.ManifestHash(), a.Generation(),
			)
			batch.Events = append(batch.Events, second)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &InMemoryEventStore{}
			bus, err := newScopedTestEventBus(store, EventBusOptions{
				ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := inboundBatchPreflightBatch(expected)
			tc.mutate(&batch)
			if _, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatch error = nil, want cardinality/order rejection")
			}
		})
	}
}

func TestProviderRawSettlementAdmissionRequiresCompleteInboundAuthority(t *testing.T) {
	entityID := eventtest.UUID("provider-raw-settlement-entity")
	exactTarget := events.RouteIdentity{FlowInstance: "telegram-ingress/standing", EntityID: entityID}
	externalSource := inboundRawSettlementRoutingSource(t, entityID)
	exactEvent := inboundRawSettlementEvent(externalSource, exactTarget)
	exactBus := &EventBus{semanticSource: inboundRawSettlementSource("external")}

	admission := exactBus.admitProviderRawSettlement(runtimeprovideroutput.KindRaw, exactEvent)
	liveNoSubscriber := RoutePlan{TargetFailure: runtimepinrouting.FailureTargetNotSubscribed}
	if !admission.authorizes(exactEvent, exactEvent, liveNoSubscriber) {
		t.Fatal("complete provider raw ingress authority did not admit deliberate empty settlement")
	}

	testCases := []struct {
		name  string
		bus   *EventBus
		kind  runtimeprovideroutput.Kind
		event events.Event
		plan  RoutePlan
	}{
		{name: "raw kind alone", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(events.NoRoutingSource(), exactTarget), plan: liveNoSubscriber},
		{name: "provider source alone", bus: &EventBus{semanticSource: inboundRawSettlementSource("harness")}, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: liveNoSubscriber},
		{name: "external input alone", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(events.NoRoutingSource(), exactTarget), plan: liveNoSubscriber},
		{name: "normalized kind", bus: exactBus, kind: runtimeprovideroutput.KindNormalized, event: exactEvent, plan: liveNoSubscriber},
		{name: "foreign entity target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowInstance: exactTarget.FlowInstance, EntityID: eventtest.UUID("foreign-provider-raw-target")}), plan: liveNoSubscriber},
		{name: "foreign flow scope", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowInstance: "other-flow/standing", EntityID: entityID}), plan: liveNoSubscriber},
		{name: "contradictory target flow id", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowID: "other-flow", FlowInstance: exactTarget.FlowInstance, EntityID: entityID}), plan: liveNoSubscriber},
		{name: "missing target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{}), plan: liveNoSubscriber},
		{name: "terminated target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: RoutePlan{TargetFailure: runtimepinrouting.FailureTargetUnreachableTerminated}},
		{name: "unproved live target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: RoutePlan{}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.bus.admitProviderRawSettlement(tc.kind, tc.event)
			if got.authorizes(tc.event, tc.event, tc.plan) {
				t.Fatal("partial or hostile facts minted deliberate provider raw settlement")
			}
		})
	}
}

func TestPrepareInboundDeliveryBatchPreservesLiveTargetPlanAndSettlesConsumerlessRawByDesign(t *testing.T) {
	entityID := eventtest.UUID("provider-raw-settlement-integrated-entity")
	target := events.RouteIdentity{FlowInstance: "telegram-ingress/standing", EntityID: entityID}
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(ActiveTargetDescriptor{ID: "standing", FlowInstance: target.FlowInstance, EntityID: target.EntityID})
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: inboundRawSettlementSource("external")})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	batch := InboundDeliveryBatch{Provider: "telegram", Events: []InboundDeliveryEvent{{
		Event: inboundRawSettlementEvent(inboundRawSettlementRoutingSource(t, entityID), target),
		Kind:  runtimeprovideroutput.KindRaw,
	}}}
	plan, err := bus.PrepareInboundDeliveryBatch(testAuthorActivityContext(context.Background()), batch)
	if err != nil {
		t.Fatalf("PrepareInboundDeliveryBatch: %v", err)
	}
	prepared := plan.PreparedPublications()
	commands := plan.CommitCommands()
	if len(prepared) != 1 || len(commands) != 1 {
		t.Fatalf("prepared/commands = %d/%d, want 1/1", len(prepared), len(commands))
	}
	if prepared[0].plan.TargetFailure != runtimepinrouting.FailureTargetNotSubscribed {
		t.Fatalf("generic target failure = %q, want preserved target_not_subscribed", prepared[0].plan.TargetFailure)
	}
	if prepared[0].targetFailure {
		t.Fatalf("approved deliberate raw empty remained an executable target failure: admission=%#v source=%#v target=%#v",
			prepared[0].providerRawSettlement, prepared[0].Event.RoutingSource().Route(), prepared[0].Event.TargetRoute())
	}
	commit := commands[0].Commit
	if !commit.RouteSettlement.NoDelivery() || commit.RouteSettlement.Reason() != events.NoDeliveryNoSubscriberByDesign {
		t.Fatalf("settlement = delivered:%t reason:%q, want no_subscriber_by_design", commit.RouteSettlement.Delivered(), commit.RouteSettlement.Reason().Code())
	}
	if len(commit.DeliveryRoutes) != 0 || commit.Disposition != nil || commit.DeadLetter != nil {
		t.Fatalf("deliberate raw empty materialized work/failure: routes=%#v disposition=%#v dead_letter=%#v", commit.DeliveryRoutes, commit.Disposition, commit.DeadLetter)
	}
}

func TestGenericPublicationCannotMintProviderRawSettlementAdmission(t *testing.T) {
	entityID := eventtest.UUID("generic-provider-raw-settlement-entity")
	target := events.RouteIdentity{FlowInstance: "telegram-ingress/standing", EntityID: entityID}
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(ActiveTargetDescriptor{ID: "standing", FlowInstance: target.FlowInstance, EntityID: target.EntityID})
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: inboundRawSettlementSource("external")})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	event := inboundRawSettlementEvent(inboundRawSettlementRoutingSource(t, entityID), target)
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	prepared, command, err := bus.prepareClosedPublication(testAuthorActivityContext(context.Background()), eventBusCommitPublishPlan{
		bus: bus, event: admitted.Event(), admitted: admitted,
	})
	if err != nil {
		t.Fatalf("prepareClosedPublication: %v", err)
	}
	if !prepared.providerRawSettlement.authorizes(prepared.Event, prepared.targetFailureInput, prepared.plan) &&
		prepared.targetFailure && command.Commit.RouteSettlement.Reason() == events.NoDeliveryResolutionBlocked &&
		command.Commit.Disposition != nil && command.Commit.DeadLetter != nil {
		return
	}
	t.Fatalf("generic provider-looking publication escaped fail-closed settlement: admission=%#v target_failure=%t reason=%q disposition=%#v dead_letter=%#v",
		prepared.providerRawSettlement, prepared.targetFailure, command.Commit.RouteSettlement.Reason().Code(), command.Commit.Disposition, command.Commit.DeadLetter)
}

func inboundRawSettlementSource(pinSource string) semanticview.Source {
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
		id: "telegram-ingress", mode: "singleton",
		inputs: []runtimecontracts.FlowInputEventPin{{Name: "telegram_raw", Event: "inbound.telegram", Source: pinSource}},
	}}, nil))
}

func inboundRawSettlementRoutingSource(t testing.TB, entityID string) events.RoutingSource {
	t.Helper()
	source, err := events.NewExternalIngressRoutingSource("telegram-ingress", entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatalf("NewExternalIngressRoutingSource: %v", err)
	}
	return source
}

func inboundRawSettlementEvent(source events.RoutingSource, target events.RouteIdentity) events.Event {
	envelope := events.EventEnvelope{}
	if !target.Empty() {
		envelope = events.EnvelopeForTargetRoute(envelope, target)
	}
	return eventtest.ExistingRunRootIngressWithRoutingSource(
		eventtest.UUID("provider-raw-settlement:"+target.FlowInstance+":"+target.EntityID+":"+source.Kind().StorageCode()),
		events.EventType("inbound.telegram"), "inbound-gateway", "", []byte(`{"update_id":1}`), 0,
		eventtest.UUID("provider-raw-settlement-run"), envelope, source, time.Unix(1, 0).UTC(),
	)
}

func inboundBatchPreflightBatch(authorization runtimeprovideroutput.Authorization) InboundDeliveryBatch {
	return InboundDeliveryBatch{
		Provider: "telegram",
		Events: []InboundDeliveryEvent{
			{Event: inboundBatchPreflightEvent("inbound.telegram"), Kind: runtimeprovideroutput.KindRaw},
			{
				Event: inboundBatchPreflightEvent("inbound.telegram.text_message"), Kind: runtimeprovideroutput.KindNormalized,
				Authorization: authorization,
			},
		},
	}
}

func inboundBatchCurrentAuthorization() runtimeprovideroutput.Authorization {
	return runtimeprovideroutput.MustAuthorization(
		"telegram",
		"inbound.telegram.text_message",
		"provider.telegram",
		"1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("generation-current")),
	)
}

func inboundBatchPreflightEvent(eventName string) events.Event {
	return eventtest.ExistingRunRootIngress(
		eventtest.UUID("inbound-batch:"+eventName), events.EventType(eventName), "inbound-gateway", "", []byte(`{"chat_id":"42"}`), 0,
		eventtest.UUID("inbound-batch-run"), events.EventEnvelope{}, time.Unix(1, 0).UTC(),
	)
}
