package pipeline_test

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type timerReplaySelectedStore interface {
	scopedTestDurableStore
	gateRecoveryDecisionStore
	pipeline.WorkflowPersistenceOwner
	pipeline.WorkflowTimerActivationPersistence
	runlifecycle.CandidateStore
	Close() error
}

func openTimerReplayNativeStore(t *testing.T, backend string) (timerReplaySelectedStore, func() error, func() timerReplaySelectedStore) {
	t.Helper()
	switch backend {
	case "sqlite":
		selected, reopen := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
		return selected, selected.Close, func() timerReplaySelectedStore { return reopen() }
	case "postgres":
		selected, reopen := storetest.StartPostgresRuntimeStoreWithReopen(t)
		return selected, selected.Close, func() timerReplaySelectedStore { return reopen() }
	default:
		t.Fatalf("unknown native timer backend %q", backend)
		return nil, nil, nil
	}
}

func newTimerReplayCoordinator(t *testing.T, bus *runtimebus.EventBus, selected timerReplaySelectedStore, options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
	t.Helper()
	options.ExecutionPosture = executionposture.Live
	options.ReceiverExecution = eventreceiver.NormalExecution()
	options.SourceArtifactFact = authorActivityTestSourceArtifactFact
	options.Persistence = pipeline.NewWorkflowPersistence(selected)
	options.DeliveryStore = selected
	options.DeadLetters = selected
	options.DecisionCards = selected
	options.ProposedEffects = selected
	options.HumanTasks = selected
	options.DecisionCardDraftExpiry = selected
	options.HumanTaskExpiry = selected
	options.DeliveryRuntime = bus
	options.RunLifecycle = selected
	options.PipelineObligations = selected.PipelineObligations()
	pc := pipeline.NewPipelineCoordinatorWithOptions(bus, options)
	if pc == nil {
		t.Fatal("native timer coordinator rejected complete semantic ports")
	}
	return pc
}

func newTimerCauseReplayNativeFixture(t *testing.T, backend string, bundle *contracts.WorkflowContractBundle) pipeline.WorkflowTimerCauseReplayFixtureForTest {
	t.Helper()
	selected, _, _ := openTimerReplayNativeStore(t, backend)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
	ctx = withLiveGateExecution(correlation.WithRunID(ctx, runID))
	admitAttachment, _ := newTimerReplayAttachmentOwner(t, ctx, selected)
	source := semanticview.Wrap(bundle)
	bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source}, "platform.stage_timer")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := pipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
	if err := scheduler.PrepareStartup(); err != nil {
		t.Fatal(err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	pc := newTimerReplayCoordinator(t, bus, selected, pipeline.PipelineCoordinatorOptions{
		Module: proposedEffectProofModule{source: source, nodes: nodes}, TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
	})
	bus.SetInterceptors(pc)
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
			t.Error(err)
		}
		scheduler.Stop()
		if err := scheduler.Wait(join); err != nil {
			t.Error(err)
		}
		if err := bus.WaitForQuiescence(join); err != nil {
			t.Error(err)
		}
	})
	return pipeline.WorkflowTimerCauseReplayFixtureForTest{Context: ctx, Coordinator: pc, Publish: bus.PublishAndWait,
		CommitConstruction: func(ctx context.Context, owner flowidentity.RunScopedFlowInstance, instance pipeline.WorkflowInstance, at time.Time) (pipeline.DynamicFlowRuntimeActivationAttempt, pipeline.DynamicFlowRuntimeReadinessPlan) {
			plan := commitA2FixtureConstruction(t, pc, selected, ctx, owner, instance, at)
			return admitAttachment(plan.Readiness), plan.Readiness
		}, Observe: func() pipeline.WorkflowTimerCauseReplayStorageForTest {
			observed := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, selected, runID, runID)
			return pipeline.WorkflowTimerCauseReplayStorageForTest(observed)
		}}
}

func TestWorkflowTimerCauseReplayEngineConsumersOnBothStores(t *testing.T) {
	pipeline.VerifyWorkflowTimerCauseReplayEngineConsumersOnBothStoresForTest(t, newTimerCauseReplayNativeFixture)
}

func TestMutationFreeAcceptedEventTimersBothStores(t *testing.T) {
	pipeline.VerifyMutationFreeAcceptedEventTimersBothStoresForTest(t, newTimerCauseReplayNativeFixture)
}
