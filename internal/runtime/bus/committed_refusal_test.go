package bus_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCommittedTargetRefusalDirectAndEngineBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"direct", "engine"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				proveCommittedTargetRefusalSurface(t, backend, surface)
			})
		}
	}
}

func proveCommittedTargetRefusalSurface(t *testing.T, backend, surface string) {
	t.Helper()
	fixture := newNativeRefusedOutboxFixture(t, backend)
	source, runID, ctx, bus := fixture.source, fixture.identity.RunID, fixture.ctx, fixture.bus
	selected := fixture.store.(targetFailurePublicationStore)
	at := time.Now().UTC()
	publication := func(target events.RouteIdentity) events.Event {
		return eventtest.ExistingRunRootIngress(uuid.NewString(), "root.ready", "api.v1", "", []byte(`{"entity_id":"consumer-one"}`), 0, runID,
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), at)
	}
	publish := func(event events.Event) error {
		if surface == "direct" {
			return bus.Publish(ctx, event)
		}
		return fixture.commitError([]engine.EmitIntent{{Event: event}})
	}
	absent := publication(events.RouteIdentity{FlowID: "consumer", FlowInstance: "missing-flow", EntityID: uuid.NewString()})
	absentBefore := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, absent.ID())
	if err := publish(absent); err == nil {
		t.Fatal("absent target was accepted as a committed diagnostic")
	}
	if after := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, absent.ID()); !reflect.DeepEqual(absentBefore, after) {
		t.Fatalf("absent target mutated durable evidence: before=%+v after=%+v", absentBefore, after)
	}
	absentView, err := selected.LoadOperatorEvent(ctx, absent.ID())
	if !errors.Is(err, operatorread.ErrEventNotFound) || len(absentView.Deliveries) != 0 || len(absentView.DeadLetters) != 0 {
		t.Fatalf("absent target gained a delivery or diagnostic: view=%+v err=%v", absentView, err)
	}
	owner := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "consumer", "", runID)
	coordinator := fixture.coordinator
	if err := coordinator.MarkTerminated(ctx, testRunScopedFlowRouteForRun(runID, owner.Route()), runtimeidentity.NormalizeEntityID(owner.EntityID), at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	event := publication(events.RouteIdentity{FlowID: owner.TemplateID, FlowInstance: owner.InstancePath, EntityID: owner.EntityID})
	intent := engine.EmitIntent{Event: event}
	if err := publish(event); err != nil {
		t.Fatal(err)
	}
	reader := selected.(operatorread.ObservabilityReader)
	before, err := reader.LoadOperatorEvent(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	if before.NoDelivery == nil || len(before.Deliveries) != 0 || len(before.DeadLetters) != 1 {
		t.Fatalf("refusal lacks exact terminal evidence: %+v", before)
	}
	if before.DeadLetters[0].Failure.Detail.Code != "target_unreachable_terminated" {
		t.Fatalf("authorized terminal receiver used another refusal: %+v", before.DeadLetters[0])
	}
	baseline, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{intent}); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("exact authorized-terminal replay: %v", err)
	}
	hostile, err := events.ResolveEnvelope(event, events.EnvelopeForTargetRoute(event.Envelope(), events.RouteIdentity{
		FlowID: owner.TemplateID, FlowInstance: owner.InstancePath, EntityID: uuid.NewString(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, hostile); err == nil {
		t.Fatal("changed explicit-target replay was accepted")
	}
	after, err := reader.LoadOperatorEvent(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(after)
	if err != nil || string(actual) != string(baseline) {
		t.Fatalf("refusal evidence changed: err=%v\nbefore=%s\nafter=%s", err, baseline, actual)
	}
	obligations := selected.(interface {
		PipelineObligations() pipelineobligation.Store
	}).PipelineObligations()
	if _, err := obligations.ClaimEvent(ctx, event.ID(), pipelineobligation.PurposeRecovery); !errors.Is(err, pipelineobligation.ErrIneligible) {
		t.Fatalf("closed refusal became recoverable: %v", err)
	}
	claim, err := obligations.ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatalf("original publication claim was not released: %v", err)
	}
	if err := obligations.Release(ctx, claim); err != nil {
		t.Fatal(err)
	}
	evidence := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, event.ID())
	if evidence.SettledDeliveryAttemptCount != 0 || evidence.NonPlatformReceiptCount != 0 || evidence.PipelineReceiptCount != 1 {
		t.Fatalf("terminal diagnostic acquired execution or duplicate settlement: %+v", evidence)
	}
}
