package cataloge2e

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
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
			// Pause through the supported lifecycle owner after the real handler
			// settles its cause, before the run's terminal barrier closes it.
			probe := &pauseReadinessEntry{h: h, node: "test-node"}
			h.rt.Pipeline.SetTestLifecycleProbe(probe)
			defer h.rt.Pipeline.SetTestLifecycleProbe(nil)
			step := catalogTriggerStep{Event: "check.requested", Payload: map[string]any{"score": 50}}
			if err := h.publishRuntimeEventResultForStep(step, 10*time.Second, true); err != nil {
				t.Fatal(err)
			}
			if probe.err != nil {
				t.Fatal(probe.err)
			}
			h.waitForCatalogStoreQuiescence(10 * time.Second)
			before, found, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute())
			if err != nil || !found || before.CurrentState != "killed" || len(before.TransitionHistory) != 1 {
				t.Fatalf("missing real guard termination: found=%v err=%v state=%+v", found, err, before)
			}
			if !reflect.DeepEqual(before.TransitionHistory[0].GuardsEvaluated, []string{"score_check"}) {
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
			ctx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
			fork, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: point, AllowSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !fork.Activation.Activated || fork.ExecutedEventCount != 1 || len(fork.ForkEvents) != 1 {
				t.Fatalf("selected historical fork did not resume its exact agent input: %+v", fork)
			}
			instances, err := h.workflow.ListWorkflowInstances(h.ctx, fork.Materialization.ForkRunID)
			if err != nil || len(instances) != 1 || instances[0].CurrentState != before.CurrentState || !reflect.DeepEqual(instances[0].TransitionHistory, before.TransitionHistory) {
				t.Fatalf("historical fork changed exact guard cause: err=%v instances=%+v fork=%+v", err, instances, fork)
			}
			original, found, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute())
			if err != nil || !found || !reflect.DeepEqual(original, before) {
				t.Fatal("historical fork changed source guard state")
			}
		})
	}
}
