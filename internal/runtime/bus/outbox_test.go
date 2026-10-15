package bus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recordingEventStore struct {
	mu       sync.Mutex
	events   []events.Event
	prepared map[string]runtimebus.PreparedPublishEvent
}

// Malformed read projection only: it cannot commit durable work or issue a
// claim. Native store proofs above own persistence and transaction evidence.
type missingExactAgentRouteProjection struct {
	recordingEventStore
}

func (s *missingExactAgentRouteProjection) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	if _, found := s.prepared[eventID]; found {
		return []string{"agent-a"}, nil
	}
	return nil, nil
}

func (s *recordingEventStore) CommitPublication(_ context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	s.mu.Lock()
	s.events = append(s.events, command.Commit.Event.Event())
	s.mu.Unlock()
	return (runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}).WithCommitAcknowledgment(), nil
}

func (*recordingEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *recordingEventStore) LoadPreparedPublishEvent(_ context.Context, eventID string) (runtimebus.PreparedPublishEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared, ok := s.prepared[strings.TrimSpace(eventID)]
	return prepared, ok, nil
}

func (*recordingEventStore) SupportsPersistedReplay() bool { return false }

func seedRecordingNoDelivery(t testing.TB, store *recordingEventStore, event events.Event) {
	t.Helper()
	admitted, err := events.RevalidatePersistedEvent(event)
	if err != nil {
		t.Fatalf("admit recording event: %v", err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit recording evaluation ledger: %v", err)
	}
	settlement, err := events.NewNoDeliverySettlement(
		events.EventWriteNormalPublication,
		events.NoDeliveryDeclaredConsumerNoPlan,
		ledger,
	)
	if err != nil {
		t.Fatalf("admit recording no-delivery settlement: %v", err)
	}
	store.prepared = map[string]runtimebus.PreparedPublishEvent{
		event.ID(): {Event: admitted, Settlement: settlement},
	}
}

func (s *recordingEventStore) eventTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.events))
	for _, evt := range s.events {
		out = append(out, string(evt.Type()))
	}
	return out
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient == want.Recipient &&
			route.Target == want.Target {
			return true
		}
	}
	return false
}

type interceptingTestHandler struct{}

func (interceptingTestHandler) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() != events.EventType("custom.emitted") {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, []events.Event{eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.followup"),
		"runtime",
		"",
		nil,
		0,
		evt.RunID(),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)}, runtimepipelineobligation.Continue(), nil
}

type recordingDeliveryRouteInterceptor struct {
	mu     sync.Mutex
	routes []events.DeliveryRoute
}

func (*recordingDeliveryRouteInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func (r *recordingDeliveryRouteInterceptor) InterceptDeliveryRoute(_ context.Context, _ events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route.Normalized())
	return false, nil, runtimepipelineobligation.Continue(), nil
}

func TestEngineDispatcherDispatchesCommittedIntentWithoutHiddenCollector(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SetInterceptors(interceptingTestHandler{})
	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("custom.emitted"), "", "", nil, 0, eventtest.UUID("committed-intent-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()),
		ChainDepth: 3,
	}
	seedRecordingNoDelivery(t, store, intent.Event)
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if got := store.eventTypes(); len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcherDoesNotConsumeAmbientSQLTransactionAuthority(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "agent-a", eventtest.UUID("ent-1"), ""))
			eb := fixture.bus
			ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
			defer runtimebustest.Unsubscribe(eb, "agent-a")

			intent := runtimeengine.EmitIntent{
				Event: eventtest.ExistingRunRootIngress(
					eventtest.UUID("evt-post-commit-dispatch"),
					events.EventType("custom.emitted"),
					"",
					"",
					[]byte(`{"entity_id":"ent-1"}`),
					0,
					runtimebustest.DefaultRunID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
					time.Now().UTC(),
				),
			}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			observation, err := storetest.ObserveOutboxDispatchWithForeignTransaction(fixture.ctx, fixture.store, eb.EngineDispatcher(), []runtimeengine.EmitIntent{intent})
			if err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			if observation.PostCommitActions != 0 || observation.RollbackActions != 0 {
				t.Fatalf("ambient transaction actions = commit:%d rollback:%d, want none", observation.PostCommitActions, observation.RollbackActions)
			}
			if !observation.ForeignTransactionOpen {
				t.Fatal("dispatcher consumed the foreign transaction")
			}
			got := requireBusEvent(t, ch, "closed post-commit dispatch")
			if got.ID() != intent.Event.ID() {
				t.Fatalf("delivered event id = %s, want %s", got.ID(), intent.Event.ID())
			}

		})
	}
}

func TestCommittedReadinessFailureBlocksOnlyItsExactOutput(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "agent-a", eventtest.UUID("readiness-ent"), "")}...)
			eb := fixture.bus
			ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
			defer runtimebustest.Unsubscribe(eb, "agent-a")
			intents := make([]runtimeengine.EmitIntent, 2)
			for i := range intents {
				intents[i].Event = eventtest.ExistingRunRootIngress(eventtest.UUID(fmt.Sprintf("readiness-event-%d", i)), "custom.emitted", "", "", []byte(`{"entity_id":"readiness-ent"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("readiness-ent")), time.Now().UTC())
			}
			fault := errors.New("first committed agent readiness failed")
			seen := make([]string, 0, 2)
			eb.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(_ context.Context, event events.Event, _ []events.DeliveryRoute) error {
				seen = append(seen, event.ID())
				if event.ID() == intents[0].Event.ID() {
					return fault
				}
				return nil
			}))
			if err := fixture.commitError(intents); !errors.Is(err, fault) {
				t.Fatalf("committed finalization = %v, want first readiness fault", err)
			}
			if len(seen) != 2 {
				t.Fatalf("readiness attempted for %d outputs, want 2", len(seen))
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); !errors.Is(err, fault) {
				t.Fatalf("dispatch = %v, want exact blocked readiness error", err)
			}
			if got := requireBusEvent(t, ch, "independent ready output"); got.ID() != intents[1].Event.ID() {
				t.Fatalf("delivered %s, want second output %s", got.ID(), intents[1].Event.ID())
			}
			select {
			case delivery := <-ch:
				if delivery != nil {
					t.Fatalf("failed-readiness output dispatched: %s", delivery.Event().ID())
				}
			default:
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents[:1]); !errors.Is(err, fault) {
				t.Fatalf("repeat dispatch bypassed failed readiness: %v", err)
			}
		})
	}
}

func TestEngineDispatcherConsumesImmutableCommittedIntentWithoutAmbientQueue(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			originalIdentity := runtimebustest.IdentityForRun(t, runtimebustest.DefaultRunID, "agent-original", "")
			fixture := newNativeOutboxFixture(t, backend, runtimebus.ActiveAgentDescriptor{Identity: originalIdentity, EntityID: runtimebustest.DefaultRunID})
			eb := fixture.bus
			originalTarget := events.RouteIdentity{FlowID: ".", FlowInstance: fixture.identity.Route.InstancePath, EntityID: runtimebustest.DefaultRunID}
			originalSubscriptions := []string{"custom.snapshot"}
			originalAdmission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
				AgentID: "agent-original", LocalEvents: testLocalSubscriptionEvents(originalSubscriptions), Subscriptions: originalSubscriptions,
			})
			if err != nil {
				t.Fatalf("admit original snapshot recipient: %v", err)
			}
			originalCh := runtimebustest.SubscribeIdentity(t, eb, originalIdentity, originalAdmission)
			defer runtimebustest.UnsubscribeIdentity(eb, originalIdentity)
			mutatedCh := runtimebustest.Subscribe(t, eb, "agent-mutated", events.EventType("custom.snapshot"))
			defer runtimebustest.Unsubscribe(eb, "agent-mutated")

			payload := []byte(`{"value":"original"}`)
			targetSet := []events.RouteIdentity{originalTarget}
			recipients := []string{"agent-original"}
			intents := []runtimeengine.EmitIntent{{
				Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-queued-snapshot"),
					events.EventType("custom.snapshot"), "", "", payload, 0, runtimebustest.DefaultRunID, events.EventEnvelope{TargetSet: targetSet},
					time.Now().UTC()),

				Recipients: recipients,
			}}
			if err := fixture.commitError(intents); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			observation, err := storetest.ObserveOutboxDispatchWithForeignTransaction(fixture.ctx, fixture.store, eb.EngineDispatcher(), intents)
			if err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			if observation.PostCommitActions != 0 || observation.RollbackActions != 0 {
				t.Fatalf("ambient transaction actions = commit:%d rollback:%d, want none", observation.PostCommitActions, observation.RollbackActions)
			}
			if !observation.ForeignTransactionOpen {
				t.Fatal("dispatcher consumed the foreign transaction")
			}
			got := requireBusEvent(t, originalCh, "immutable committed intent delivery")
			copy(payload, []byte(`{"value":"mutated!"}`))
			targetSet[0] = events.RouteIdentity{FlowInstance: "flow-mutated", EntityID: "entity-mutated"}
			recipients[0] = "agent-mutated"
			intents[0].Event = eventtest.ExistingRunRootIngress(
				intents[0].Event.ID(),
				intents[0].Event.Type(),
				intents[0].Event.SourceAgent(),
				intents[0].Event.TaskID(),
				[]byte(`{"value":"reassigned"}`),
				intents[0].Event.ChainDepth(),
				intents[0].Event.RunID(),
				events.EventEnvelope{TargetSet: []events.RouteIdentity{{FlowInstance: "flow-reassigned", EntityID: "entity-reassigned"}}},
				intents[0].Event.CreatedAt())

			intents[0].Recipients = []string{"agent-reassigned"}

			if string(got.Payload()) != `{"value":"original"}` {
				t.Fatalf("delivered payload = %s, want original snapshot", string(got.Payload()))
			}
			target := got.TargetRoute()
			if target.FlowInstance != originalTarget.FlowInstance || target.EntityID != originalTarget.EntityID {
				t.Fatalf("delivered target route = %#v, want original snapshot", target)
			}
			requireNoBusEvent(t, mutatedCh, "mutated recipient delivery")
		})
	}
}

func TestEngineDispatcherAmbientSQLContextDoesNotBypassDurableManifest(t *testing.T) {
	fixture := newNativeOutboxFixture(t, "sqlite")
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	observation, err := storetest.ObserveOutboxDispatchWithForeignTransaction(context.Background(), fixture.store, eb.EngineDispatcher(), []runtimeengine.EmitIntent{{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-no-post-commit-queue"),
			events.EventType("custom.emitted"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}})
	if err == nil {
		t.Fatal("expected DispatchPostCommit to fail closed without post-commit queue")
	}
	if !strings.Contains(err.Error(), "prepared event settlement reader is required") {
		t.Fatalf("DispatchPostCommit error = %q, want prepared event authority failure", err)
	}
	if observation.PostCommitActions != 0 || observation.RollbackActions != 0 {
		t.Fatalf("missing manifest consumed foreign authority: %+v", observation)
	}
	if !observation.ForeignTransactionOpen {
		t.Fatal("missing manifest consumed the foreign transaction")
	}
}

func TestEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			entityID := uuid.NewString()
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", entityID, "")}...)
			recordingStore := fixture.store
			receiver := runtimebustest.Subscribe(t, fixture.bus, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-1"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"`+entityID+`"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()), Recipients: []string{"reviewer"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if err := fixture.bus.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatal(err)
			}
			requireBusEvent(t, receiver, "original publication before independent replay")
			intent.Recipients = []string{"late-reviewer"}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox exact duplicate: %v", err)
			}
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
			gotPersisted, err := recordingStore.ListEventDeliveryRecipients(fixture.ctx, eventtest.UUID("evt-1"))
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(gotPersisted, ",") != "reviewer" {
				t.Fatalf("persisted recipients after exact duplicate = %v, want original [reviewer]", gotPersisted)
			}
		})
	}
}

func TestEngineOutboxExactDuplicateDispatchIsOperationNoOp(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), ""), testActiveAgentDescriptor(t, "late-reviewer", eventtest.UUID("ent-1"), "")}...)
			store := fixture.store
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			lateReviewer := runtimebustest.Subscribe(t, eb, "late-reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-exact-duplicate-operation"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			persistAndDispatch := func(intent runtimeengine.EmitIntent) {
				t.Helper()
				if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatalf("WriteOutbox: %v", err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatalf("DispatchPostCommit: %v", err)
				}
			}
			persistAndDispatch(intent)
			_ = requireBusEvent(t, reviewer, "initial inserted outbox operation")
			requireNoBusEvent(t, lateReviewer, "initial recipient manifest")
			duplicate := intent
			duplicate.Recipients = []string{"late-reviewer"}
			persistAndDispatch(duplicate)
			requireNoBusEvent(t, reviewer, "exact duplicate redelivery")
			requireNoBusEvent(t, lateReviewer, "exact duplicate recipient expansion")
			got, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(got, ",") != "reviewer" {
				t.Fatalf("persisted recipients after exact duplicate = %v, want [reviewer]", got)
			}
		})
	}
}

func TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/commit_then_dispatch", func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-claim"), ""))
			eb, store := fixture.bus, fixture.store
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{
				Event: eventtest.ExistingRunRootIngress(
					eventtest.UUID("evt-outbox-claim-commit_then_dispatch"),
					events.EventType("custom.emitted"),
					"",
					"",
					[]byte(`{"entity_id":"ent-claim"}`),
					0,
					runtimebustest.DefaultRunID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-claim")),
					time.Now().UTC(),
				),
				Recipients: []string{"reviewer"},
			}

			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if _, err := store.PipelineObligations().ClaimPublication(fixture.ctx, intent.Event.ID()); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
				t.Fatalf("publication claim before designated dispatch = %v, want busy", err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			requireBusEvent(t, reviewer, "claimed outbox event")

			claim, err := store.PipelineObligations().ClaimPublication(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("publication claim after lifecycle completion: %v", err)
			}
			if err := store.PipelineObligations().Release(fixture.ctx, claim); err != nil {
				t.Fatalf("release publication claim: %v", err)
			}
		})
		t.Run(backend+"/rollback", func(t *testing.T) {
			proveNativeOutboxPostWriteRollback(t, backend)
		})
	}
}

func TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-same-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent, intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent, intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			if evt := requireBusEvent(t, reviewer, "inserted same-batch intent"); evt.ID() != intent.Event.ID() {
				t.Fatalf("dispatched event = %q, want %q", evt.ID(), intent.Event.ID())
			}
			requireNoBusEvent(t, reviewer, "same-batch exact duplicate")
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
		})
	}
}

func TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-preexisting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			writeAndDispatch := func(intents []runtimeengine.EmitIntent) {
				t.Helper()
				if err := fixture.commitError(intents); err != nil {
					t.Fatalf("WriteOutbox: %v", err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); err != nil {
					t.Fatalf("DispatchPostCommit: %v", err)
				}
			}
			writeAndDispatch([]runtimeengine.EmitIntent{intent})
			_ = requireBusEvent(t, reviewer, "initial inserted intent")
			writeAndDispatch([]runtimeengine.EmitIntent{intent, intent, intent})
			requireNoBusEvent(t, reviewer, "preexisting exact duplicate batch")
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
		})
	}
}

func TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			store := fixture.store
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-conflicting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","value":"first"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			conflict := intent
			conflict.Event = eventtest.ExistingRunRootIngress(intent.Event.ID(), events.EventType("custom.conflicting"), "", "", []byte(`{"entity_id":"ent-1","value":"conflict"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), intent.Event.CreatedAt())
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent, conflict}); err == nil || !strings.Contains(err.Error(), "conflicting event identity") {
				t.Fatalf("WriteOutbox conflict error = %v", err)
			}
			if got := fixture.eventCount(t); got != 0 {
				t.Fatalf("events after rollback = %d, want 0", got)
			}
			if got, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID()); err != nil || len(got) != 0 {
				t.Fatalf("deliveries after rollback = %v, %v", got, err)
			}
			writeAndDispatch := func() {
				t.Helper()
				if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatal(err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatal(err)
				}
			}
			writeAndDispatch()
			_ = requireBusEvent(t, reviewer, "insert after rolled-back conflict")
			writeAndDispatch()
			requireNoBusEvent(t, reviewer, "exact duplicate after rolled-back conflict")
		})
	}
}

func TestEngineOutboxDistinctIntentBatchDispatchesEachOnce(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intents := []runtimeengine.EmitIntent{{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-distinct-1"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":1}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}, {Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-distinct-2"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":2}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}}
			if err := fixture.commitError(intents); err != nil {
				t.Fatal(err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); err != nil {
				t.Fatal(err)
			}
			if evt := requireBusEvent(t, reviewer, "first distinct intent"); evt.ID() != intents[0].Event.ID() {
				t.Fatalf("first event = %q, want %q", evt.ID(), intents[0].Event.ID())
			}
			if evt := requireBusEvent(t, reviewer, "second distinct intent"); evt.ID() != intents[1].Event.ID() {
				t.Fatalf("second event = %q, want %q", evt.ID(), intents[1].Event.ID())
			}
			requireNoBusEvent(t, reviewer, "distinct intent over-dispatch")
		})
	}
}

func TestEngineOutboxSubscribedIntentConsumesCanonicalMaterializedRoutePlan(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadNativeOutboxSource(t, canonicalrouting.CopyOutboxMaterializedManifest(t))
			wantBlueprint := runtimebus.DeliveryRouteBlueprint{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "review", "target-node")), Target: events.RouteIdentity{
				FlowID:       "review",
				FlowInstance: "review/inst-1",
				EntityID:     runtimeflowidentity.EntityID("review/inst-1"),
			}, Handler: runtimepipeline.MustDeliveryTargetHandler(testFlowNode(t, "review", "target-node")).ForEvent("task.started")}
			want := events.DeliveryRoute{Recipient: wantBlueprint.Recipient, Target: events.MustExistingEntityTarget(wantBlueprint.Target)}
			guardSawMaterializedRoute := false
			fixture := newNativeOutboxFixtureWithOptions(t, backend, runtimebus.EventBusOptions{
				ContractBundle: source,
				RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if len(plan.DeliveryRoutes) != 0 {
						t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
					}
					return []runtimebus.DeliveryRouteBlueprint{wantBlueprint}, nil
				},
				RecipientPlanGuard: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
						t.Fatalf("guard delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
					}
					guardSawMaterializedRoute = true
					return nil
				},
			})
			constructed := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "review", "inst-1", fixture.identity.RunID)
			at := time.Now().UTC()
			seedComponentFlowConstruction(t, fixture.ctx, fixture.store, source, runtimepipeline.WorkflowInstance{
				WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(),
				InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
				ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
				EntityType: "review_item", Fields: map[string]any{"instance_key": "inst-1"}, CreatedAt: at, EnteredStageAt: at,
			})
			intent := runtimeengine.EmitIntent{
				Event: eventtest.RuntimeControl(eventtest.UUID("evt-outbox-materialized-route"),
					events.EventType("review/inst-1/task.started"), "", "", []byte(`{}`), 0, runtimebustest.DefaultRunID, "", events.EventEnvelope{}, time.Now().UTC()),
			}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if !guardSawMaterializedRoute {
				t.Fatal("recipient plan guard did not see materialized route")
			}
			prepared, found, err := fixture.store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(fixture.ctx, intent.Event.ID())
			if err != nil || !found {
				t.Fatalf("load materialized publication: found=%v err=%v", found, err)
			}
			if got := prepared.DeliveryRoutes; !deliveryRoutesContain(got, want) {
				t.Fatalf("persisted delivery routes = %#v, want %#v", got, want)
			}
		})
	}
}

func TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "control-plane", "", ""), testActiveAgentDescriptor(t, "reviewer-ent-1", eventtest.UUID("ent-1"), ""), testActiveAgentDescriptor(t, "reviewer-ent-2", eventtest.UUID("ent-2"), "")}...)
			store := fixture.store
			eb := fixture.bus
			controlCh := runtimebustest.Subscribe(t, eb, "control-plane")
			matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1")
			otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-direct-intent"), events.EventType("custom.direct"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"control-plane", "reviewer-ent-1"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			wantPersisted := []string{"control-plane", "reviewer-ent-1"}
			if strings.Join(gotPersisted, ",") != strings.Join(wantPersisted, ",") {
				t.Fatalf("persisted recipients = %v, want %v", gotPersisted, wantPersisted)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			_ = requireBusEvent(t, controlCh, "direct intent delivery to control-plane")
			evt := requireBusEvent(t, matchCh, "direct intent delivery to matching entity-scoped agent")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless matched event entity_id = %q, want empty route-local target", got)
			}
			requireNoBusEvent(t, otherCh, "direct intent delivery to filtered recipient")
		})
	}
}

func TestEngineOutboxCommittedTargetRefusalPreservesDisposition(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend)
			intent := fixture.refusedChildIntent(t, uuid.NewString())
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatal(err)
			}
			before, err := storetest.ReadExactPipelineReceiptOutcomeReason(fixture.ctx, fixture.store, intent.Event.ID())
			if err != nil || before.Outcome != string(runtimepipelineobligation.DispositionDeadLetter) {
				t.Fatalf("committed disposition = %+v err=%v, want dead letter", before, err)
			}
			for i := 0; i < 2; i++ {
				if err := fixture.bus.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatal(err)
				}
				got, err := storetest.ReadExactPipelineReceiptOutcomeReason(fixture.ctx, fixture.store, intent.Event.ID())
				if err != nil || got != before {
					t.Fatalf("dispatch %d replaced committed refusal with %+v err=%v", i, got, err)
				}
				assertNativeOutboxClaimReleased(t, fixture, intent.Event.ID())
			}
			prepared, found, err := fixture.store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(fixture.ctx, intent.Event.ID())
			if err != nil || !found {
				t.Fatalf("load refused event: found=%v err=%v", found, err)
			}
			if routes := prepared.DeliveryRoutes; len(routes) != 0 {
				t.Fatalf("refused publication acquired deliveries: %+v", routes)
			}
		})
	}
}

func TestEngineOutbox_TargetFailureDeadLetterErrorFailsClosed(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend)
			intent := fixture.refusedChildIntent(t, eventtest.UUID("evt-outbox-target-failure"))
			before := fixture.eventCount(t)
			t.Cleanup(func() {
				if err := storetest.SetOutboxDeadLetterInsertFault(context.Background(), fixture.store, intent.Event.ID(), false); err != nil {
					t.Error(err)
				}
			})
			if err := storetest.SetOutboxDeadLetterInsertFault(fixture.ctx, fixture.store, intent.Event.ID(), true); err != nil {
				t.Fatal(err)
			}
			result, err := fixture.commit([]runtimeengine.EmitIntent{intent})
			if result.Committed || err == nil || !strings.Contains(err.Error(), "outbox_dead_letter_insert_fault") {
				t.Fatalf("WriteOutbox error=%v result=%+v, want actual diagnostic insert failure", err, result)
			}
			if fixture.eventCount(t) != before {
				t.Fatal("diagnostic failure committed partial publication")
			}
			assertNativeOutboxClaimReleased(t, fixture, intent.Event.ID())
		})
	}
}

func TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "agent-a", "", "")}...)
			store := fixture.store
			eb := fixture.bus
			internalCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.emitted"))
			agentCh := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-internal-live"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC())}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(gotPersisted, ",") != "agent-a" {
				t.Fatalf("persisted recipients = %v, want [agent-a]", gotPersisted)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			evt := requireBusEvent(t, internalCh, "outbox event delivery to internal subscriber")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless internal event entity_id = %q, want empty route-local target", got)
			}
			evt = requireBusEvent(t, agentCh, "outbox event delivery to agent subscriber")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless agent event entity_id = %q, want empty route-local target", got)
			}
		})
	}
}

func TestEngineOutboxAndDispatcher_DoesNotFabricateRoutesForPendingInternalDeliveries(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend)
			eb := fixture.bus
			interceptor := &recordingDeliveryRouteInterceptor{}
			eb.SetInterceptors(interceptor)
			internalCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.emitted"))
			defer runtimebustest.Unsubscribe(eb, "workflow-runtime")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-pending-internal-route"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC())}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			if len(interceptor.routes) != 0 {
				t.Fatalf("delivery route interceptor received fabricated pending internal routes %#v", interceptor.routes)
			}
			requireBusEvent(t, internalCh, "pending internal event without a semantic delivery route")
		})
	}
}

func TestEngineDispatcherRunsInterceptorsForPersistedEmitIntents(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SetInterceptors(interceptingTestHandler{})

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-1"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			eventtest.UUID("persisted-interceptor-run"),
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	seedRecordingNoDelivery(t, store, intent.Event)
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	got := store.eventTypes()
	if len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcherFailsClosedWithoutPreparedEventAuthority(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-missing-manifest"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			runtimebustest.DefaultRunID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil {
		t.Fatal("expected DispatchPostCommit to fail without prepared event authority")
	}
	if got := err.Error(); !strings.Contains(got, "prepared event settlement reader is required") {
		t.Fatalf("DispatchPostCommit error = %q, want missing prepared event authority", got)
	}
}

func TestEngineDispatcher_DirectIntentWithoutPersistedExactRoutesFailsClosed(t *testing.T) {
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-direct-no-tx"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			eventtest.UUID("direct-intent-missing-route-run"),
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: "root"}),
			time.Now().UTC(),
		),

		Recipients: []string{"agent-a"},
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit direct intent evaluation ledger: %v", err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatalf("admit direct intent settlement: %v", err)
	}
	admitted, err := events.RevalidatePersistedEvent(intent.Event)
	if err != nil {
		t.Fatal(err)
	}
	store := &missingExactAgentRouteProjection{recordingEventStore: recordingEventStore{
		prepared: map[string]runtimebus.PreparedPublishEvent{intent.Event.ID(): {
			Event: admitted, Settlement: settlement, DeliveryRoutes: []events.DeliveryRoute{{
				Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "node-only")),
				Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: "root",
				}),
			}},
		}},
	}}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{Durable: runtimebus.ExactDurableTestDependencies(store)})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil || !strings.Contains(err.Error(), "persisted agent recipients without exact identity-bearing delivery routes") {
		t.Fatalf("DispatchPostCommit error = %v, want missing exact route failure", err)
	}
}

func TestEnginePublicationRejectsFilteredDirectIntentBeforePersistence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer-ent-2", eventtest.UUID("ent-2"), "")}...)
			store := fixture.store
			eb := fixture.bus
			filteredCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-empty-direct-manifest"), events.EventType("custom.direct"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer-ent-2"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err == nil || !strings.Contains(err.Error(), "direct delivery rejected recipients: reviewer-ent-2") {
				t.Fatalf("commit engine publications error = %v, want rejected recipient", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if len(gotPersisted) != 0 {
				t.Fatalf("persisted recipients = %v, want empty authoritative manifest", gotPersisted)
			}
			requireNoBusEvent(t, filteredCh, "rejected direct intent delivery")
		})
	}
}

func TestPublishDirectRejectsFilteredExplicitRecipient(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "requester-agent", "requester-entity", "provider/instance-a")}...)
			eb := fixture.bus
			evt := eventtest.ExistingRunRootIngress(eventtest.UUID("evt-filtered-direct-mutation"), events.EventType("human_task.approved"), "runtime", "", []byte(`{}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("wrong-entity"), FlowInstance: "wrong-flow"}), time.Now().UTC())
			err := eb.PublishDirect(fixture.ctx, evt, []string{"requester-agent"})
			if err == nil || !strings.Contains(err.Error(), "direct delivery rejected recipients: requester-agent") {
				t.Fatalf("PublishDirect error = %v", err)
			}
		})
	}
}
