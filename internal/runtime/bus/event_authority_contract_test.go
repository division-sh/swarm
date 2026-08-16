package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestRuntimeIngressDispatchBypassRequiresTypedPlatformRuntimeAuthority(t *testing.T) {
	hostile := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "platform.paused", "runtime", "", nil, 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if runtimeIngressDispatchBypass(hostile) {
		t.Fatal("external root event bypassed runtime ingress by producer ID and label shape")
	}
	owned := eventtest.RuntimeControl(
		uuid.NewString(), "platform.paused", "runtime", "", nil, 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if !runtimeIngressDispatchBypass(owned) {
		t.Fatal("typed platform runtime event did not bypass its own ingress gate")
	}
}

func TestInMemoryCommitterRejectsClosedGenericClasses(t *testing.T) {
	for _, event := range []events.Event{
		mustBusContractEvent(t, events.EventTypePlatformRuntimeLog, events.EventAdmissionDiagnosticDirect),
		mustBusContractEvent(t, "work.replayed", events.EventAdmissionSelectedForkReplay),
	} {
		admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := (InMemoryEventStore{}).CommitPublication(context.Background(), PublicationCommand{Commit: CommitPublishRequest{Event: admitted}}); err == nil {
			t.Fatalf("in-memory generic committer accepted %s", event.AdmissionClass())
		}
	}
}

func TestCommitPublishRejectsMissingOrDualRouteSettlement(t *testing.T) {
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "work.started", "gateway", "", []byte(`{}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	normalEmpty, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	selectedEmpty, err := events.NewNoDeliverySettlement(events.EventWriteSelectedForkPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		commit CommitPublishRequest
	}{
		{name: "missing", commit: CommitPublishRequest{Event: admitted}},
		{name: "selected class", commit: CommitPublishRequest{Event: admitted, RouteSettlement: selectedEmpty}},
		{name: "delivery arm without route", commit: CommitPublishRequest{Event: admitted, RouteSettlement: delivery}},
		{name: "no delivery arm with route", commit: CommitPublishRequest{Event: admitted, RouteSettlement: normalEmpty, DeliveryRoutes: []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "consumer"))}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (InMemoryEventStore{}).CommitPublication(context.Background(), PublicationCommand{Commit: test.commit}); err == nil {
				t.Fatal("hostile publication command was accepted")
			}
		})
	}
}

func TestEveryGenericPublicationSurfaceRejectsClosedEventClasses(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eventsUnderTest := []events.Event{
		mustBusContractEvent(t, events.EventTypePlatformRuntimeLog, events.EventAdmissionDiagnosticDirect),
		mustBusContractEvent(t, events.EventTypePlatformInboundRecord, events.EventAdmissionDiagnosticDirect),
		mustBusContractEvent(t, events.EventTypePlatformAgentDirective, events.EventAdmissionDiagnosticDirect),
		mustBusContractEvent(t, "work.replayed", events.EventAdmissionSelectedForkReplay),
	}
	surfaces := []struct {
		name string
		run  func(context.Context, events.Event) error
	}{
		{name: "publish", run: eb.Publish},
		{name: "acknowledged", run: eb.PublishAcknowledged},
		{name: "direct", run: func(ctx context.Context, event events.Event) error {
			return eb.PublishDirect(ctx, event, []string{"agent-a"})
		}},
		{name: "deferred", run: eb.publishDeferred},
		{name: "engine_publication", run: func(ctx context.Context, event events.Event) error {
			_, err := eb.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{{Event: event}})
			return err
		}},
	}
	for _, event := range eventsUnderTest {
		for _, surface := range surfaces {
			event, surface := event, surface
			t.Run(string(event.Type())+"/"+surface.name, func(t *testing.T) {
				err := surface.run(context.Background(), event)
				if err == nil || !strings.Contains(err.Error(), "named persistence operation") {
					t.Fatalf("generic surface error = %v, want named-operation refusal", err)
				}
			})
		}
	}
}

func mustBusContractEvent(t *testing.T, eventType events.EventType, class events.EventAdmissionClass) events.Event {
	t.Helper()
	runID := uuid.NewString()
	if class == events.EventAdmissionDiagnosticDirect {
		return eventtest.DiagnosticDirect(
			uuid.NewString(), eventType, "runtime", "", nil, 0,
			runID, "", events.EventEnvelope{}, time.Now().UTC(),
		)
	}
	lineage, err := events.NewSelectedForkLineage(runID, uuid.NewString(), uuid.NewString(), "selection:test", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.SelectedForkReplay(
		uuid.NewString(), eventType, eventtest.Producer(events.EventProducerPlatform, "runtime"), "", nil, 0,
		lineage, events.EventEnvelope{}, time.Now().UTC(),
	)
}
