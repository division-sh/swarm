package bus_test

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestEventBusPublish_ConnectRoutePlanPersistsCreatedAgentBeforeLiveCarrier(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifyConnectRoutePlanPersistsCreatedAgentBeforeLiveCarrierForTest(t, func(t *testing.T, source semanticview.Source) pipeline.FlowInstanceActivationPlanner {
				bundle, found := semanticview.Bundle(source)
				if !found {
					t.Fatal("prospective agent preparation requires its admitted bundle")
				}
				var selected completeEventDispatchStore
				if backend == "sqlite" {
					selected = storetest.StartSQLiteRuntimeStore(t)
				} else {
					selected = storetest.StartPostgresRuntimeStore(t)
				}
				ctx := testAuthorActivityContextForSource(context.Background(), source)
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: "99999999-9999-9999-9999-999999999999",
					Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC(),
				})
				eventBus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				workflow := newEventBusWorkflowCoordinator(eventBus, selected, newFixtureWorkflowModule(t, bundle))
				process := worklifetime.NewProcess()
				owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: testSourceArtifactFact(source).BundleHash(),
				})
				if err != nil {
					t.Fatal(err)
				}
				planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
					ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: testSourceArtifactFact(source),
					SemanticSource: source, WorkflowInstances: workflow, WorkOwner: owner, ReceiverExecution: eventreceiver.NormalExecution(),
				})
				t.Cleanup(func() {
					if err := planner.Shutdown(); err != nil {
						t.Error(err)
					}
					if _, err := owner.RetireAndWait(context.Background()); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(context.Background()); err != nil {
						t.Error(err)
					}
				})
				return planner
			})
		})
	}
}
