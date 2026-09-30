package cataloge2e

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestGuardTerminationHistoricalForkPreservesCauseBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyGuardForkContinuation(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			h.seedInitialState(pipeline.FlowInstanceEntityID(catalogRuntimeRunID))
			step := catalogTriggerStep{Event: "check.requested", Payload: map[string]any{"score": 50}}
			if err := h.publishRuntimeEventResultForStep(step, 10*time.Second, true); err != nil {
				t.Fatal(err)
			}
			h.waitForCatalogStoreQuiescence(10 * time.Second)
			ctx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
			if _, err := runScopedCatalogStore(t, h).PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "guard child settled before agent frontier", ControlledBy: "cataloge2e"}); err != nil {
				t.Fatal(err)
			}
			before, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil || len(before) != 2 {
				t.Fatalf("guard child and unfinished parent missing: err=%v instances=%+v", err, before)
			}
			var guarded pipeline.WorkflowInstance
			for _, instance := range before {
				if instance.CurrentState == "killed" {
					guarded = instance
				}
			}
			if guarded.CurrentState != "killed" || len(guarded.TransitionHistory) != 1 {
				t.Fatalf("missing real guard termination: %+v", before)
			}
			if !reflect.DeepEqual(guarded.TransitionHistory[0].GuardsEvaluated, []string{"score_check"}) {
				t.Fatal("source did not record the exact evaluated guard")
			}
			// The supported selected-contract operation admits the exact root
			// pending input. Original-contract unversioned route history remains
			// fenced; there is no migration or historical node redelivery here.
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "task.ready", Payload: map[string]any{}}, 10*time.Second, true); err != nil {
				t.Fatal(err)
			}
			var point string
			if err := h.db.QueryRowContext(h.ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='task.ready'`, catalogRuntimeRunID).Scan(&point); err != nil {
				t.Fatal(err)
			}
			var artifacts interface {
				storetest.DurableDataCatalogStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			if h.sqlite != nil {
				artifacts = h.sqlite
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, artifacts)
			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			fork, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: point, AllowSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !fork.Activation.Activated || fork.ExecutedEventCount != 1 || len(fork.ForkEvents) != 1 || fork.Materialization.SourceRunID != catalogRuntimeRunID || fork.Materialization.ForkPoint.EventID != point {
				t.Fatalf("selected historical fork did not resume its exact agent input: %+v", fork)
			}
			// The fork reconstructs entity state; completed execution evidence
			// remains source-owned through the exact historical fork lineage.
			plan, err := runScopedCatalogStore(t, h).PlanRunFork(h.ctx, runfork.RunForkPlanRequest{
				SourceRunID: fork.Materialization.ForkRunID, At: fork.ForkEvents[0].ForkEventID,
			})
			if err != nil || len(plan.Entities) != 2 {
				t.Fatalf("historical fork snapshot missing: err=%v plan=%+v", err, plan)
			}
			preserved := false
			for _, entity := range plan.Entities {
				if entity.EntityID != guarded.EntityID {
					continue
				}
				metadata := entity.MaterializationMetadata
				if metadata == nil || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner || metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState || metadata.FlowInstance != guarded.StorageRef {
					t.Fatalf("guard history lacks exact snapshot ownership: %+v", entity)
				}
				preserved = entity.CurrentState == "killed"
			}
			if !preserved {
				t.Fatalf("historical fork lost guard-terminated state: %+v", plan.Entities)
			}
			lineage, err := runScopedCatalogStore(t, h).PlanRunFork(h.ctx, runfork.RunForkPlanRequest{
				SourceRunID: fork.Materialization.SourceRunID, At: fork.Materialization.ForkPoint.EventID,
			})
			if err != nil {
				t.Fatalf("read exact historical cause lineage: %v", err)
			}
			preserved = false
			for _, entity := range lineage.Entities {
				if entity.EntityID != guarded.EntityID {
					continue
				}
				metadata := entity.MaterializationMetadata
				if metadata == nil || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner || metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState || metadata.FlowInstance != guarded.StorageRef {
					t.Fatalf("historical guard cause lacks exact source ownership: %+v", entity)
				}
				instance, err := pipeline.DecodeWorkflowInstancePersistenceRecord(pipeline.WorkflowInstancePersistenceRecord{
					EntityID: entity.EntityID, FlowInstance: metadata.FlowInstance, EntityType: metadata.EntityType,
					Slug: metadata.Slug, Name: metadata.Name, WorkflowName: guarded.WorkflowName,
					CurrentState: entity.CurrentState, Config: metadata.FlowConfig,
				})
				if err != nil {
					t.Fatalf("decode source-at-revision guard evidence: %v", err)
				}
				preserved = instance.CurrentState == "killed" && reflect.DeepEqual(instance.TransitionHistory, guarded.TransitionHistory)
			}
			if !preserved {
				t.Fatalf("historical fork lost exact cause lineage: %+v", lineage.Entities)
			}
			original, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(original, before) {
				t.Fatal("historical fork changed source guard state")
			}
		})
	}
}
