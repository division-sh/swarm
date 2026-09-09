package runtimepersistence

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type forkFanOutConsumerModule struct {
	runForkGateWorkflowModule
	definition *pipeline.WorkflowDefinition
	nodes      []pipeline.WorkflowNode
}

func (m forkFanOutConsumerModule) WorkflowDefinition() *pipeline.WorkflowDefinition {
	return m.definition
}
func (m forkFanOutConsumerModule) WorkflowNodes() []pipeline.WorkflowNode {
	return append([]pipeline.WorkflowNode(nil), m.nodes...)
}
func (m forkFanOutConsumerModule) GuardRegistry() pipeline.GuardRegistry {
	return pipeline.NewContractGuardRegistry(m.source)
}
func (m forkFanOutConsumerModule) ActionRegistry() pipeline.ActionRegistry {
	return pipeline.NewContractActionRegistry(m.source)
}

func consumeForkFanOutEmissions(t *testing.T, fixture authorActivityReceiptFixture, backend string, source semanticview.Source, ctx context.Context, intent fanoutobligation.Intent, claim fanoutobligation.Claim, emissions []engine.EmitIntent, expectedFailure string) {
	t.Helper()
	selected := fixture.store.(storeTestDurableEventBusStore)
	workflow := fixture.store.(workflowTestSelectedStore)
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("source artifact fact missing")
	}
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, ok := authoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("author scope missing")
	}
	lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
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
		admitted := publication.PublicationCommand().Commit.Event.Event()
		originalOrigin, originalInherited := emissions[ordinal].Event.InheritedFanOutOrigin()
		admittedOrigin, admittedInherited := admitted.InheritedFanOutOrigin()
		if admitted.ID() == "" || !bytes.Equal(admitted.Payload(), emissions[ordinal].Event.Payload()) || originalOrigin != admittedOrigin || originalInherited != admittedInherited {
			t.Fatal("publication admission changed ordinal payload/origin or omitted durable identity")
		}
		emissions[ordinal].Event = admitted
		outcomes = append(outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
	}
	beforeCommit := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	if _, inherited := emissions[0].Event.InheritedFanOutOrigin(); inherited {
		requireFanOutOriginNamedAdmission(t, fixture, backend, eventBus, childCtx, claim, emissions[0].Event, prepared[0])
	}
	committed, err := fixture.store.(selectedFanOutOwner).CommitFanOutChunk(childCtx, pipeline.FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: time.Now().UTC()})
	if err != nil || committed.Intent.Cursor != len(emissions) {
		if !reflect.DeepEqual(beforeCommit, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("failed ordinal commit mutated persisted state")
		}
		t.Fatalf("commit ordinal plans: cursor=%d err=%v", committed.Intent.Cursor, err)
	}
	definition, err := pipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, definition, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
	})
	if coordinator == nil {
		t.Fatal("real pipeline dependencies incomplete")
	}
	eventBus.SetInterceptors(coordinator)
	if err := eventBus.FinalizeEnginePublications(childCtx, committed.Publications); err != nil {
		t.Fatal(err)
	}
	for _, emit := range emissions {
		beforeExecution := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
		if err := eventBus.EngineDispatcher().DispatchPostCommit(childCtx, []engine.EmitIntent{emit}); err != nil {
			t.Fatalf("dispatch actual committed ordinal: %v", err)
		}
		persisted, found, err := selected.LoadPreparedPublishEvent(childCtx, emit.Event.ID())
		if err != nil || !found || len(persisted.DeliveryRoutes) != 1 {
			t.Fatalf("ordinal durable readback: found=%v err=%v", found, err)
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
		var status, reason, failure string
		if err := fixture.db.QueryRowContext(childCtx, `SELECT status,COALESCE(reason_code,''),COALESCE(CAST(failure AS TEXT),'') FROM event_deliveries WHERE event_id=$1`, emit.Event.ID()).Scan(&status, &reason, &failure); err != nil {
			t.Fatal(err)
		}
		if expectedFailure == "" {
			if value["processed_value"] != payload["value"] {
				t.Fatalf("downstream business write missing: fields=%s payload=%v status=%s reason=%s failure=%s", fields, payload, status, reason, failure)
			}
			if status != "delivered" {
				t.Fatalf("downstream settlement=%s", status)
			}
		} else {
			var envelope failures.Envelope
			if err := json.Unmarshal([]byte(failure), &envelope); err != nil || status != "dead_letter" || reason != "handler_terminal_failure" || envelope.Class != failures.ClassStaleArrival || envelope.Detail.Code != expectedFailure {
				t.Fatalf("exact stale-revision refusal missing: status=%s reason=%s failure=%s err=%v", status, reason, failure, err)
			}
			afterExecution := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
			for _, table := range []string{"entity_state", "entity_mutations"} {
				if _, present := beforeExecution[table]; !present || !reflect.DeepEqual(beforeExecution[table], afterExecution[table]) {
					t.Fatalf("stale revision changed business history or lacked %s coverage", table)
				}
			}
		}
		before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
		if err := eventBus.EngineDispatcher().DispatchPostCommit(childCtx, []engine.EmitIntent{emit}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("duplicate downstream consumption mutated history")
		}
		requireFanOutOriginReadback(t, fixture, backend, childCtx, emit.Event)
	}
	// Read the committed origin through the fixed-revision owner, not live event
	// SQL, before using this run as a further fork source.
	checkpoint := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, intent.Request.Key.RunID, events.EventEnvelope{}, eventtest.RootRoutingSource(intent.Request.Key.RunID), time.Now().UTC())
	if err := insertCanonicalEventRecordFixture(childCtx, fixture.store, checkpoint); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, childCtx, fixture.db, intent.Request.Key.RunID, backend == "postgres")
	plan, err := fixture.store.(selectedActivityProjectionStore).PlanRunFork(childCtx, runfork.RunForkPlanRequest{SourceRunID: intent.Request.Key.RunID, At: checkpoint.ID()})
	if err != nil || len(plan.FanOutObligations) != 1 || len(plan.FanOutObligations[0].Outcomes) != len(emissions) {
		t.Fatalf("fixed-revision origin/outcome readback: %+v %v", plan.FanOutObligations, err)
	}
}
