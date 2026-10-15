package bus_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
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
	var f completeEventDispatchFixture
	var native *nativeOutboxFixture
	if surface == "engine" {
		native = newNativeOutboxFixture(t, backend)
		parent := native.intent("compound-refusal-parent").Event
		agentID := "complete-event-agent"
		identity := runtimebustest.IdentityForRun(t, parent.RunID(), agentID, "")
		storetest.CommitSemanticEventWithRoutes(t, native.ctx, native.store, parent, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}}, pipelineobligation.ScopeSubscribed)
		f = completeEventDispatchFixture{store: native.store, ctx: native.ctx, bus: native.bus, event: parent, agentID: agentID, identity: identity, source: native.source}
	} else {
		f = newCompleteEventDispatchFixture(t, backend, false)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(
		uuid.NewString(), f.event.Type(), "api.v1", "", []byte(`{}`), 0, f.event.RunID(),
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
			EntityID: uuid.NewString(), FlowInstance: "missing-flow",
		}), eventtest.RootRoutingSource(f.event.RunID()), time.Now().UTC(), f.event.ExecutionMode(),
	)
	intent := engine.EmitIntent{Event: event}
	if native != nil {
		intent.Event = eventtest.ChildWithLineageAndRoutingSource(event.ID(), "output.done", "runtime", "", []byte(`{}`), 0, events.LineageFromEvent(f.event), event.Envelope(), eventtest.RootRoutingSource(f.event.RunID()), event.CreatedAt())
		event = intent.Event
	}
	if surface == "direct" {
		if err := f.bus.Publish(f.ctx, event); err != nil {
			t.Fatal(err)
		}
	} else if err := native.commitError([]engine.EmitIntent{intent}); err != nil {
		t.Fatal(err)
	}
	reader := f.store.(operatorread.ObservabilityReader)
	before, err := reader.LoadOperatorEvent(f.ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	if before.NoDelivery == nil || len(before.Deliveries) != 0 || len(before.DeadLetters) != 1 {
		t.Fatalf("refusal lacks exact terminal evidence: %+v", before)
	}
	baseline, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, []engine.EmitIntent{intent}); err != nil {
			t.Fatal(err)
		}
	}
	// An unresolved explicit target still refuses generic republish.
	// Exact committed callbacks above consume only closed durable work.
	if err := f.bus.Publish(f.ctx, event); err == nil || err.Error() != "durable event route facts conflict with the admitted event target" {
		t.Fatalf("explicit-target replay refusal changed: %v", err)
	}
	after, err := reader.LoadOperatorEvent(f.ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(after)
	if err != nil || string(actual) != string(baseline) {
		t.Fatalf("refusal evidence changed: err=%v\nbefore=%s\nafter=%s", err, baseline, actual)
	}
	if _, err := f.store.PipelineObligations().ClaimEvent(f.ctx, event.ID(), pipelineobligation.PurposeRecovery); !errors.Is(err, pipelineobligation.ErrIneligible) {
		t.Fatalf("closed refusal became recoverable: %v", err)
	}
	claim, err := f.store.PipelineObligations().ClaimPublication(f.ctx, event.ID())
	if err != nil {
		t.Fatalf("original publication claim was not released: %v", err)
	}
	if err := f.store.PipelineObligations().Release(f.ctx, claim); err != nil {
		t.Fatal(err)
	}
	f.assertNoAgentDispatchMutation(t)
}
