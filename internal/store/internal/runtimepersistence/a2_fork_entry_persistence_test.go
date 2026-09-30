package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// The actual mutation owner and fork ledger admit the source snapshot. This is
// history/materialization coverage, not child lifecycle execution permission.
func TestA2ForkStageEntryPersistenceOnBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, root := range []bool{true, false} {
				name := "nonroot"
				if root {
					name = "root"
				}
				t.Run(name, func(t *testing.T) {
					opened := backend.open(t)
					f := snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, runID: uuid.NewString(), entityID: uuid.NewString()}
					flowID, path := "owner", "owner/one"
					if root {
						f.entityID, flowID, path = f.runID, ".", f.runID
					}
					f.ctx = correlation.WithRunID(testAuthorActivityContext(), f.runID)
					requireDefaultSourceArtifactForTest(t, f.ctx, opened.store)
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					if _, err := f.store.SetupScenarioEntities(f.ctx, pipeline.ScenarioSetupRequest{RunID: f.runID, CreatedAt: at,
						Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "subject", EntityID: f.entityID, FlowInstance: path, EntityType: "review_item", CurrentState: "active"}},
					}); err != nil {
						t.Fatal(err)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(f.runID, flowidentity.RouteForInstancePath(path))
					if err != nil {
						t.Fatal(err)
					}
					effect, err := workflowlifecycle.NewInitialEntry(owner.Route, identity.NormalizeEntityID(f.entityID), "active", executionmode.Live, at)
					if err != nil {
						t.Fatal(err)
					}
					entry, found, err := effect.StageEntry(owner)
					if err != nil || !found || entry.OriginRunID != "" {
						t.Fatalf("fresh construction = %#v %v", entry, err)
					}
					bookkeeping := map[string]any{}
					if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
						t.Fatal(err)
					}
					loop, err := loopruntime.New(f.runID, f.entityID, flowID, "review", "revision", "construction", "active", 4, at)
					if err != nil {
						t.Fatal(err)
					}
					captured := loop.Generation()
					node, err := identity.AdmitExecutableNodeDeclaration(flowID, "collector")
					if err != nil {
						t.Fatal(err)
					}
					ref, err := timeridentity.NewJoinRef(node, "result", "active", "join")
					if err != nil {
						t.Fatal(err)
					}
					ref, err = ref.BindStageEntry(entry, captured)
					if err != nil {
						t.Fatal(err)
					}
					arm, err := joinruntime.NewActivation(ref, []string{"a", "b"}, nil, at, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := arm.Add("a", map[string]any{"event_id": "authored-business"}); err != nil {
						t.Fatal(err)
					}
					arm.Status, arm.CloseReason, arm.TimerCancelled, arm.OutcomePending = joinruntime.StatusClosed, joinruntime.CloseReasonUntil, true, true
					if _, err := loop.Repeat("active", "repeat", at.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if err := loop.Close("done", "close", at.Add(2*time.Second)); err != nil {
						t.Fatal(err)
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, loop); err != nil {
						t.Fatal(err)
					}
					if err := joinruntime.Store(buckets, arm); err != nil {
						t.Fatal(err)
					}
					bucket := timeridentity.NewAccumulatorBucketRefForGeneration(node, "result", captured)
					acc := accumulator.Load(nil)
					if duplicate, err := acc.Admit(&contracts.AccumulateSpec{Into: "items", From: "payload", Key: "payload.id"}, map[string]any{"id": " exact ", "event_id": "authored-business", "payload": map[string]any{"id": "nested-business"}}, "source-delivery"); err != nil || duplicate {
						t.Fatalf("source business state = duplicate %v %v", duplicate, err)
					}
					accRaw, err := json.Marshal(acc)
					if err != nil {
						t.Fatal(err)
					}
					var accValue map[string]any
					if err := json.Unmarshal(accRaw, &accValue); err != nil {
						t.Fatal(err)
					}
					buckets[node.Key()] = map[string]any{"handler_accumulators": map[string]any{bucket.Key(): accValue}}
					f.state = stateOnlyWorkflowEngineMutationRecord(t, f.runID, flowID, path, f.entityID, "active", 1, at)
					f.state.Identity, f.state.CurrentState = owner, "active"
					f.state.Slug, f.state.Name = "entry-subject", "Entry Subject"
					if root {
						f.state.Mode = "static"
					}
					config, err := pipeline.WorkflowInstanceConfigPayloadForRoute(owner.Route, "1", nil)
					if err != nil {
						t.Fatal(err)
					}
					f.state.Config, err = json.Marshal(config)
					if err != nil {
						t.Fatal(err)
					}
					f.state.Bookkeeping, err = json.Marshal(bookkeeping)
					if err != nil {
						t.Fatal(err)
					}
					f.state.Accumulator, err = json.Marshal(runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
						t.Fatal(err)
					}
					f.eventID = uuid.NewString()
					event := eventtest.ExistingRunRootIngress(f.eventID, "fork.entry_cut", "entry-proof", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
					if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
						t.Fatal(err)
					}
					plan := f.plan(t)
					if len(plan.Entities) != 1 {
						t.Fatalf("fixed entities = %#v", plan.Entities)
					}
					source := readSnapshotOwnershipEntity(t, f, f.runID)
					request := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}
					result, err := f.store.MaterializeRunFork(f.ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					childFixture := f
					if root {
						childFixture.entityID = result.ForkRunID
					}
					child := readSnapshotOwnershipEntity(t, childFixture, result.ForkRunID)
					inherited, found, err := workflowlifecycle.LoadStageEntry(child.Buckets[2])
					want := entry
					want.RunID, want.OriginRunID = result.ForkRunID, f.runID
					if root {
						want.EntityID, want.FlowScope, want.InstanceID, want.InstancePath = result.ForkRunID, result.ForkRunID, result.ForkRunID, result.ForkRunID
					}
					if err != nil || !found || inherited != want {
						t.Fatalf("persisted inherited owner = %#v want %#v: %v", inherited, want, err)
					}
					carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, child.Buckets[3])
					if err != nil {
						t.Fatal(err)
					}
					arms, err := joinruntime.List(carrier.StateBuckets)
					if err != nil || len(arms) != 1 {
						t.Fatalf("persisted retained arm = %#v %v", arms, err)
					}
					wantGeneration, err := loopruntime.ForkGeneration(captured, result.ForkRunID, childFixture.entityID)
					if err != nil {
						t.Fatal(err)
					}
					if arms[0].Generation() != wantGeneration || arms[0].Generation().Attempt != 1 || arms[0].JoinRef().StageEntry() != want || arms[0].Status != arm.Status || arms[0].CloseReason != arm.CloseReason || !reflect.DeepEqual(arms[0].Outputs, arm.Outputs) || !arms[0].OutcomePending {
						t.Fatal("materialized arm changed its exact history/business payload")
					}
					bucket.Generation = wantGeneration
					stored := carrier.StateBuckets[node.Key()]["handler_accumulators"].(map[string]any)
					gotAccumulator := accumulator.Load(stored[bucket.Key()].(map[string]any))
					if gotAccumulator.Err() != nil || !reflect.DeepEqual(gotAccumulator.Items, acc.Items) || !reflect.DeepEqual(gotAccumulator.Received, acc.Received) {
						t.Fatal("materialized accumulator lost exact business admission/history")
					}
					before := snapshotOwnershipCounts(t, f, result.ForkRunID)
					again, err := f.store.MaterializeRunFork(f.ctx, request)
					if err != nil || again.ForkRunID != result.ForkRunID || snapshotOwnershipCounts(t, f, result.ForkRunID) != before {
						t.Fatalf("fork exact replay changed history: %#v %v", again, err)
					}
					if !reflect.DeepEqual(source, readSnapshotOwnershipEntity(t, f, f.runID)) || !reflect.DeepEqual(plan.Entities, f.plan(t).Entities) {
						t.Fatal("fork projection changed source/fixed history")
					}
					var childEvents int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, result.ForkRunID).Scan(&childEvents); err != nil || childEvents != 0 {
						t.Fatalf("history projection invented child events: %d %v", childEvents, err)
					}
				})
			}
		})
	}
}
