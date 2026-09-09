package runtimepersistence

import (
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
	"github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestForkHistoricalAgentGenerationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, cell := range []string{"current", "historical", "missing_original", "foreign_original", "unknown_revision"} {
				t.Run(cell, func(t *testing.T) {
					source := selectedActivityProducerSourceWithLoops(t, true, false)
					runID := uuid.NewString()
					ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
					descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
					if err != nil {
						t.Fatal(err)
					}
					scope, ok := authoractivity.ScopeFromContext(ctx)
					if !ok {
						t.Fatal("source scope missing")
					}
					lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
					if err != nil {
						t.Fatal(err)
					}
					defer lease.Release()
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					activation, err := loopruntime.New(runID, runID, ".", "revision", "opaque_revision", uuid.NewString(), "pending", 3, at)
					if err != nil {
						t.Fatal(err)
					}
					generation := activation.Generation()
					if cell == "historical" {
						if _, err := activation.Repeat("pending", uuid.NewString(), at.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, activation); err != nil {
						t.Fatal(err)
					}
					seedWorkflowTargetStateForTransition(t, backend.name, fixture.db, runID, runID, runID, "initial", 1, at)
					if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET entity_type='root' WHERE run_id=$1`, runID); err != nil {
						t.Fatal(err)
					}
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "initial", 1, at)
					record.CurrentState, record.EntityType, record.Mode = "pending", "root", "static"
					record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
					if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
						t.Fatal(err)
					}
					revision := generation.RevisionID
					if cell == "unknown_revision" {
						revision = "not-owned-by-source"
					}
					payload := json.RawMessage(forkTestJSON(t, map[string]any{"opaque_revision": revision, "business_revision": generation.RevisionID, "large": json.Number("9007199254740993")}))
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "ordinary.ready", "operator", "", payload, 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(2*time.Minute))
					agent := bustest.IdentityForRun(t, runID, "historical-agent", "")
					route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent}
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store.(storeTestDurableEventBusStore), event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, backend.name == "postgres")
					owner := fixture.store.(interface {
						PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
						MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
						ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
						LoadRunForkSourceRunID(context.Context, string) (string, error)
						LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
					})
					plan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: event.ID()})
					if err != nil || !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
						t.Fatalf("historical agent policy not admitted: %+v %v", plan.UnsupportedBlockers, err)
					}
					child, err := owner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: event.ID()})
					if err != nil {
						t.Fatal(err)
					}
					originalRun, err := owner.LoadRunForkSourceRunID(ctx, child.ForkRunID)
					if err != nil || originalRun != runID {
						t.Fatalf("original run lookup: %s %v", originalRun, err)
					}
					original := originalCarriageForRun(t, fixture.store, originalRun)
					switch cell {
					case "missing_original":
						original = semanticview.OriginalLoopCarriage{}
					case "foreign_original":
						original, err = semanticview.CompileOriginalLoopCarriage(selectedActivityProducerSource(t))
						if err != nil {
							t.Fatal(err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					request := runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID, AllowSourceFreeze: true, OriginalLoopCarriage: original, HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{}}
					activated, err := owner.ActivateRunFork(ctx, request)
					if cell == "missing_original" || cell == "foreign_original" || cell == "unknown_revision" {
						if err == nil {
							t.Fatalf("historical %s was admitted", cell)
						}
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("historical rejection mutated stores")
						}
						return
					}
					if err != nil || !activated.Activated || activated.DeliveryEventReplay == nil || activated.DeliveryEventReplay.ReplayedDeliveryCount != 1 {
						t.Fatalf("historical replay: %+v %v", activated, err)
					}
					want, err := loopruntime.ForkGeneration(generation, child.ForkRunID, child.ForkRunID)
					if err != nil {
						t.Fatal(err)
					}
					id := deterministicRunForkReplayEventID(child.ForkRunID, event.ID())
					prepared, found, err := owner.LoadPreparedPublishEvent(ctx, id)
					if err != nil || !found {
						t.Fatalf("historical aggregate readback: found=%v %v", found, err)
					}
					if err := prepared.Validate(); err != nil {
						t.Fatal(err)
					}
					readback := prepared.Event.Event()
					if readback.RunID() != child.ForkRunID || readback.RoutingSource().Route().EntityID != child.ForkRunID || readback.Envelope().Source != readback.RoutingSource().Route() || len(prepared.DeliveryRoutes) != 1 || prepared.DeliveryRoutes[0].AgentIdentity.RunID != child.ForkRunID {
						t.Fatalf("historical aggregate retained source ownership: %+v", prepared)
					}
					var raw []byte
					if err := fixture.db.QueryRowContext(ctx, `SELECT payload_bytes FROM events WHERE event_id=$1`, id).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					if string(raw) != string(readback.Payload()) {
						t.Fatal("historical aggregate payload differs from durable bytes")
					}
					originalEvent, found, err := owner.LoadPreparedPublishEvent(ctx, event.ID())
					if err != nil || !found || string(originalEvent.Event.Event().Payload()) != string(payload) || originalEvent.Event.Event().RoutingSource() != event.RoutingSource() {
						t.Fatalf("historical replay changed source event: found=%v %v", found, err)
					}
					var got map[string]json.RawMessage
					if err := json.Unmarshal(raw, &got); err != nil {
						t.Fatal(err)
					}
					if string(got["opaque_revision"]) != forkTestJSON(t, want.RevisionID) || string(got["business_revision"]) != forkTestJSON(t, generation.RevisionID) || string(got["large"]) != "9007199254740993" {
						t.Fatalf("historical payload changed incorrectly: %s", raw)
					}
					before = snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if _, err := owner.ActivateRunFork(ctx, request); err == nil {
						t.Fatal("existing repeat-activation policy was bypassed")
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("repeat refusal mutated historical evidence")
					}
				})
			}
		})
	}
}
