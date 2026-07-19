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
	hostile := eventtest.RootIngress(
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
		transaction := &inMemoryCommitPublishTransaction{}
		if _, err := transaction.BeginPreparedPublish(nil, PreparedPublishEvent{event: admitted}); err == nil {
			t.Fatalf("in-memory generic committer accepted %s", event.AdmissionClass())
		}
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
		{name: "transactional", run: func(ctx context.Context, event events.Event) error {
			return eb.PublishInMutation(WithCommitPublishTransaction(ctx, &inMemoryCommitPublishTransaction{}), event)
		}},
		{name: "transactional_direct", run: func(ctx context.Context, event events.Event) error {
			return eb.PublishDirectInMutation(WithCommitPublishTransaction(ctx, &inMemoryCommitPublishTransaction{}), event, []string{"agent-a"})
		}},
		{name: "engine_outbox", run: func(ctx context.Context, event events.Event) error {
			ctx = WithCommitPublishTransaction(ctx, &inMemoryCommitPublishTransaction{})
			return eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{{Event: event}})
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
