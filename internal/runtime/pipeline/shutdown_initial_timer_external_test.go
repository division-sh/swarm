package pipeline_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type shutdownInitialTimerStore struct {
	pipeline.WorkflowPersistenceOwner
	fail atomic.Bool
	err  error
}

func (s *shutdownInitialTimerStore) ListWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]pipeline.WorkflowTimerActivation, error) {
	if s.fail.Load() {
		return nil, s.err
	}
	return s.WorkflowPersistenceOwner.ListWorkflowTimerActivations(ctx, runID, entityID, activeOnly)
}

func TestAcceptedInitialTimerReconciliationDuringShutdownBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, readFailure := range []bool{false, true} {
			name := backend + "/accepted"
			if readFailure {
				name = backend + "/independent_read_failure"
			}
			t.Run(name, func(t *testing.T) {
				selected := openSQLiteGateRecoveryStore
				if backend == "postgres" {
					selected = openPostgresGateRecoveryStore
				}
				store := selected(t)
				failure := errors.New("initial timer read failed independently")
				timers := &shutdownInitialTimerStore{WorkflowPersistenceOwner: store.events.(pipeline.WorkflowPersistenceOwner), err: failure}
				store.persistence = pipeline.NewWorkflowPersistence(timers)
				runID := uuid.NewString()
				ctx, cancel := context.WithTimeout(withLiveGateExecution(correlation.WithRunID(context.Background(), runID)), 10*time.Second)
				defer cancel()
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, map[string]string{
					"manifest.yaml":        "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
					"schema.yaml":          "stages:\n  active: {}\n",
					"events.yaml":          "work.requested:\n  order_id: text\n",
					"orders/manifest.yaml": "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
					"orders/schema.yaml":   "instance: order_id\npins:\n  inputs: [order.created, work.requested]\nstages:\n  active:\n    timers:\n      - {after: 1h, emit: timer.elapsed}\n",
					"orders/events.yaml":   "order.created:\n  order_id: text\nwork.requested:\n  order_id: text\ntimer.elapsed:\n",
					"orders/state.yaml":    "order_id: {type: text, indexed: true}\n",
					"orders/agents.yaml":   "worker:\n  model: regular\n  intent: {inline: Observe work.}\n  subscriptions: [work.requested]\n",
				}))
				bundle, _ := semanticview.Bundle(source)
				fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
				ctx = testAuthorActivityContextForSource(t, ctx, fact)
				ctx, original := newPartialAttachmentOriginal(t, ctx, store)
				if err := storetest.MaterializeRun(ctx, store.events.(storetest.RunFixtureStore), storetest.RunFixture{
					Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash(),
				}); err != nil {
					t.Fatal(err)
				}
				bus, err := newScopedTestEventBus(t, store.events, partialAttachmentBusOptions(ctx, source, fact))
				if err != nil {
					t.Fatal(err)
				}
				scheduler := pipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwnerForSource(t, fact))
				if err := scheduler.PrepareStartup(); err != nil {
					t.Fatal(err)
				}
				pc := newGateRecoveryCoordinator(bus, store, pipeline.PipelineCoordinatorOptions{
					Module: proposedEffectProofModule{source: source}, TimerScheduler: scheduler,
					WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact),
				})
				bus.SetInterceptors(pc)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unhold := func() { once.Do(func() { close(release) }) }
				defer unhold()
				am, grant, releaseProcess := newPartialAttachmentManager(t, ctx, store, original, source, pc, bus, func(cfg actors.AgentConfig) (manager.Agent, error) {
					close(entered)
					select {
					case <-release:
						return &partialAttachmentAgent{config: cfg}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				})
				rt := &runtimepkg.Runtime{Manager: am, Pipeline: pc, Scheduler: scheduler, Bus: bus}
				t.Cleanup(func() {
					unhold()
					_ = rt.Shutdown()
					if err := grant.Retire(context.WithoutCancel(ctx)); err != nil {
						t.Error(err)
					}
					releaseProcess()
				})
				parent, err := flowidentity.StandingForGeneration(source, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				instance, err := flowidentity.KeyedChild(source, parent, "orders", "one")
				if err != nil {
					t.Fatal(err)
				}
				// The causal clock is fixed and already due; PrepareStartup prevents
				// wall-clock firing until the test explicitly releases that boundary.
				at := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
				constructA2StructuralRoot(t, ctx, am, bus, source, parent, at)
				requireIndexedConstructionFixture(t, ctx, store.persistence, source, runID, parent)
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "order.created", "operator", "", []byte(`{"order_id":"one"}`), 0, runID, events.EventEnvelope{}, at)
				plan, err := am.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance,
					ConstructorInput: "order.created", ResolvedKey: "one", TriggerEvent: event, OccurredAt: at})
				if err != nil {
					t.Fatal(err)
				}
				committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged {
					t.Fatalf("native construction: acknowledged=%t err=%v", committed.Acknowledged, err)
				}
				requireIndexedConstructionFixture(t, ctx, store.persistence, source, runID, instance)
				attached := make(chan error, 1)
				go func() { attached <- am.FinalizeCommittedFlowInstanceActivation(ctx, committed) }()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				before, err := timers.ListWorkflowTimerActivations(ctx, runID, instance.EntityID, false)
				if err != nil || len(before) != 1 {
					t.Fatalf("native initial timer: %+v, %v", before, err)
				}
				if err := pc.RestoreWorkflowTimers(ctx); err != nil {
					t.Fatal(err)
				}
				probe, cancelProbe := context.WithCancel(ctx)
				cancelProbe()
				if err := scheduler.Wait(probe); !errors.Is(err, context.Canceled) {
					t.Fatalf("startup-held idle projection did not retain its task: %v", err)
				}
				stopped := make(chan error, 1)
				go func() { stopped <- rt.ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}) }()
				if err := scheduler.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				if err := scheduler.ReleaseStartup(ctx); err != nil {
					t.Logf("release scheduler startup after idle withdrawal: %v", err)
				}
				timers.fail.Store(readFailure)
				unhold()
				for label, result := range map[string]<-chan error{"attachment": attached, "shutdown": stopped} {
					select {
					case err := <-result:
						if label == "attachment" {
							t.Logf("retained registration waiter: %v", err)
						} else if readFailure && !errors.Is(err, failure) || !readFailure && err != nil {
							t.Errorf("%s: %v", label, err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				timers.fail.Store(false)
				after, err := timers.ListWorkflowTimerActivations(ctx, runID, instance.EntityID, false)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("shutdown changed accepted timer or fired a fresh occurrence: before=%+v after=%+v err=%v", before, after, err)
				}
				if !readFailure {
					row, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
					if err != nil || !found || row.AttemptState != "retired" || row.Phase != pipeline.FlowAttachmentReady {
						t.Fatalf("accepted readiness did not finish before terminal retirement: %+v, %t, %v", row, found, err)
					}
				}
			})
		}
	}
}
