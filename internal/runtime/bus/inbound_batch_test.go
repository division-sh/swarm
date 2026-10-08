package bus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	source, catalog, batch := authenticatedTelegramBatchFixture(t, "telegram-ingress", true)
	store := &InMemoryEventStore{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source, ProviderOutputVerifier: catalog,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
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
	exactEvent := inboundRawSettlementEvent(externalSource, events.RouteIdentity{})
	exactBus := &EventBus{semanticSource: inboundRawSettlementSource(t, true)}

	admission := exactBus.admitProviderRawSettlement(runtimeprovideroutput.KindRaw, exactEvent)
	liveNoSubscriber := RoutePlan{ordinarySource: ordinaryPublicationSource{route: events.RouteIdentity{
		FlowID: "telegram-ingress", FlowInstance: exactTarget.FlowInstance, EntityID: entityID,
	}}}
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
		{name: "provider source alone", bus: &EventBus{semanticSource: inboundRawSettlementSource(t, false)}, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: liveNoSubscriber},
		{name: "declared ingress alone", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(events.NoRoutingSource(), exactTarget), plan: liveNoSubscriber},
		{name: "normalized kind", bus: exactBus, kind: runtimeprovideroutput.KindNormalized, event: exactEvent, plan: liveNoSubscriber},
		{name: "foreign entity target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowInstance: exactTarget.FlowInstance, EntityID: eventtest.UUID("foreign-provider-raw-target")}), plan: liveNoSubscriber},
		{name: "foreign flow scope", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowInstance: "other-flow/standing", EntityID: entityID}), plan: liveNoSubscriber},
		{name: "contradictory target flow id", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, events.RouteIdentity{FlowID: "other-flow", FlowInstance: exactTarget.FlowInstance, EntityID: entityID}), plan: liveNoSubscriber},
		{name: "preassigned local target", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: inboundRawSettlementEvent(externalSource, exactTarget), plan: liveNoSubscriber},
		{name: "foreign source owner", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: RoutePlan{ordinarySource: ordinaryPublicationSource{route: events.RouteIdentity{FlowID: "other-flow", FlowInstance: exactTarget.FlowInstance, EntityID: entityID}}}},
		{name: "foreign owner entity", bus: exactBus, kind: runtimeprovideroutput.KindRaw, event: exactEvent, plan: RoutePlan{ordinarySource: ordinaryPublicationSource{route: events.RouteIdentity{FlowID: "telegram-ingress", FlowInstance: exactTarget.FlowInstance, EntityID: "other-entity"}}}},
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

func TestPrepareInboundDeliveryBatchUsesLiveSourceOwnerAndSettlesConsumerlessRawByDesign(t *testing.T) {
	entityID := eventtest.UUID("provider-raw-settlement-integrated-entity")
	target := events.RouteIdentity{FlowInstance: "telegram-ingress/standing", EntityID: entityID}
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(ActiveTargetDescriptor{ID: "standing", FlowInstance: target.FlowInstance, EntityID: target.EntityID})
	source, catalog, batch := authenticatedTelegramBatchFixture(t, "telegram-ingress", false)
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, ProviderOutputVerifier: catalog})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	plan, err := bus.PrepareInboundDeliveryBatch(testAuthorActivityContext(context.Background()), batch)
	if err != nil {
		t.Fatalf("PrepareInboundDeliveryBatch: %v", err)
	}
	prepared := plan.PreparedPublications()
	commands := plan.CommitCommands()
	if len(prepared) != 1 || len(commands) != 1 {
		t.Fatalf("prepared/commands = %d/%d, want 1/1", len(prepared), len(commands))
	}
	if !prepared[0].plan.TargetFailure.Empty() {
		t.Fatalf("target-free source acquired target failure = %q", prepared[0].plan.TargetFailure)
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
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: inboundRawSettlementSource(t, true)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	event := inboundRawSettlementEvent(inboundRawSettlementRoutingSource(t, entityID), events.RouteIdentity{})
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	_, _, err = bus.prepareClosedPublication(testAuthorActivityContext(context.Background()), eventBusCommitPublishPlan{
		bus: bus, event: admitted.Event(), admitted: admitted,
	})
	if err == nil || !strings.Contains(err.Error(), "provider declaration publication requires its authenticated admission owner") {
		t.Fatalf("generic provider-looking publication refusal = %v", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 {
		t.Fatal("refused generic publication mutated persistence")
	}
}

func authenticatedTelegramBatchFixture(t testing.TB, flowID string, withText bool) (semanticview.Source, *providertriggers.CatalogSnapshot, InboundDeliveryBatch) {
	t.Helper()
	base := inboundRawSettlementSource(t, true)
	catalog, batch := authenticatedTelegramBatchForSource(t, base, flowID, withText)
	var authorizations []runtimeprovideroutput.Authorization
	for _, output := range batch.Events {
		if !output.Authorization.Empty() {
			authorizations = append(authorizations, output.Authorization)
		}
	}
	return providerOutputAuthorizedTestSource{Source: base, declaringFlow: flowID, generation: catalog.Generation(), authorizations: authorizations}, catalog, batch
}

func authenticatedTelegramBatchForSource(t testing.TB, source semanticview.Source, flowID string, withText bool) (*providertriggers.CatalogSnapshot, InboundDeliveryBatch) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(canonicalrouting.RepoRoot(t), "packs/provider-triggers/telegram/trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := providertriggers.ParseManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Manifest: manifest, Identity: providertriggers.PackIdentity{ID: "provider.telegram", Version: "1.0.0", ManifestHash: "sha256:" + hex.EncodeToString(hash[:]), Provenance: "platform"}, Source: "committed Telegram pack fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "fixture", Provider: "telegram", SigningSecret: "fixture-secret"})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"update_id":1}`)
	if withText {
		body = []byte(`{"update_id":1,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`)
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(body, &payload); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	admitted, err := plan.AdmitRequest(providertriggers.Request{Provider: "telegram", Target: providertriggers.Target{WebhookSecret: "fixture-secret"}, Body: body,
		Payload: payload, Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"fixture-secret"}}, Received: at})
	if err != nil {
		t.Fatal(err)
	}
	fact := authorActivityTestSourceArtifactFact
	if bundle, found := semanticview.Bundle(source); found && bundle.SourceArtifact != nil {
		fact, err = runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
		if err != nil {
			t.Fatal(err)
		}
	}
	delivery, admission, err := plan.ProjectPublication(admitted, fact.BundleHash(), flowID)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := events.NewExternalIngressRoutingSource(flowID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	batch := InboundDeliveryBatch{Provider: "telegram", Admission: admission}
	for _, output := range delivery.Events {
		payload, err := canonicaljson.Bytes(output.Payload)
		if err != nil {
			t.Fatal(err)
		}
		event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(t.Name()+":"+string(output.Name)), output.Name, "inbound-gateway", "", payload, 0, busInternalTestRunID, events.EventEnvelope{}, routing, at)
		batch.Events = append(batch.Events, InboundDeliveryEvent{Event: event, Kind: runtimeprovideroutput.Kind(output.Kind), Authorization: output.Authorization})
	}
	return catalog, batch
}

func inboundRawSettlementSource(t testing.TB, admitted bool) semanticview.Source {
	base := semanticview.Wrap(connectRoutePlanTestBundle(t, []connectRoutePlanTestFlow{{
		id: "telegram-ingress", mode: runtimecontracts.FlowModeStatic,
		inputs: []runtimecontracts.FlowInputEventPin{{Event: "inbound.telegram"}},
	}}, nil))
	if !admitted {
		return base
	}
	return semanticviewtest.WithProviderIngress(base, map[string][]string{"telegram-ingress": {"inbound.telegram"}})
}

func inboundRawSettlementRoutingSource(t testing.TB, entityID string) events.RoutingSource {
	t.Helper()
	source, err := events.NewExternalIngressRoutingSource("telegram-ingress", events.RoutingSourceAuthorityProviderAdmissionPlan)
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
