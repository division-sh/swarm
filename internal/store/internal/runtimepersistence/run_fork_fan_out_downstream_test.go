package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func consumeForkFanOutEmissions(t *testing.T, fixture authorActivityReceiptFixture, backend string, source semanticview.Source, ctx context.Context, intent fanoutobligation.Intent, claim fanoutobligation.Claim, emissions []engine.EmitIntent) {
	t.Helper()
	selected := fixture.store.(storeTestDurableEventBusStore)
	workflow := fixture.store.(workflowTestSelectedStore)
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("source artifact fact missing")
	}
	work := storeTestWorkOwner(t)
	eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work})
	if err != nil {
		t.Fatal(err)
	}
	childCtx := correlation.WithRunID(ctx, intent.Request.Key.RunID)
	prepared, err := eventBus.PrepareEnginePublications(childCtx, emissions)
	if err != nil || len(prepared) != len(emissions) {
		t.Fatalf("admit actual ordinal emissions: count=%d err=%v", len(prepared), err)
	}
	outcomes := make([]pipeline.FanOutChunkOutcome, 0, len(prepared))
	for ordinal, plan := range prepared {
		publication, ok := plan.(bus.EnginePublicationPlan)
		if !ok {
			t.Fatalf("unexpected publication %T", plan)
		}
		routes := publication.PublicationCommand().Commit.DeliveryRoutes
		if len(routes) != 1 || routes[0].Recipient != events.MustNodeDeliveryRecipient(mustPersistenceRootNode("item-consumer")) || routes[0].Target.Route().EntityID != intent.Request.Key.RunID {
			t.Fatalf("ordinal did not admit exact child consumer: %+v", routes)
		}
		outcomes = append(outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
	}
	beforeCommit := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	committed, err := fixture.store.(selectedFanOutOwner).CommitFanOutChunk(childCtx, pipeline.FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: time.Now().UTC()})
	if err != nil || committed.Intent.Cursor != len(emissions) {
		if !reflect.DeepEqual(beforeCommit, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("failed ordinal commit mutated persisted state")
		}
		t.Fatalf("commit ordinal plans: cursor=%d err=%v", committed.Intent.Cursor, err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: runForkGateWorkflowModule{source: source}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
	})
	if coordinator == nil {
		t.Fatal("real pipeline dependencies incomplete")
	}
	for _, emit := range emissions {
		persisted, found, err := selected.LoadPreparedPublishEvent(childCtx, emit.Event.ID())
		if err != nil || !found || len(persisted.DeliveryRoutes) != 1 {
			t.Fatalf("ordinal durable readback: found=%v err=%v", found, err)
		}
		delivery, err := events.NewDeliveryEvent(persisted.Event.Event(), persisted.DeliveryRoutes[0])
		if err != nil {
			t.Fatal(err)
		}
		forward, _, result, err := coordinator.InterceptDeliveryRoute(childCtx, delivery, persisted.DeliveryRoutes[0])
		if _, disposed := result.Disposition(); err != nil || forward || disposed {
			t.Fatalf("actual downstream execution: forward=%v outcome=%+v err=%v", forward, result, err)
		}
		var fields string
		if err := fixture.db.QueryRowContext(childCtx, `SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$1`, intent.Request.Key.RunID).Scan(&fields); err != nil {
			t.Fatal(err)
		}
		var value, payload map[string]any
		if err := json.Unmarshal([]byte(fields), &value); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		if value["processed_value"] != payload["value"] {
			t.Fatalf("downstream business write missing: fields=%s payload=%v", fields, payload)
		}
		var status string
		if err := fixture.db.QueryRowContext(childCtx, `SELECT status FROM event_deliveries WHERE event_id=$1`, emit.Event.ID()).Scan(&status); err != nil || status != "delivered" {
			t.Fatalf("downstream settlement=%s err=%v", status, err)
		}
		before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
		if _, _, _, err := coordinator.InterceptDeliveryRoute(childCtx, delivery, persisted.DeliveryRoutes[0]); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("duplicate downstream consumption mutated history")
		}
	}
}
