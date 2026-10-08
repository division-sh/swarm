package pipeline_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestWorkflowFinalInitialTimersRemainUnarmedAcrossRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, final := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/final_%t", backend, final), func(t *testing.T) {
				selected, closeStore, reopen := openTimerReplayNativeStore(t, backend)
				runID := uuid.NewString()
				entityID := flowidentity.EntityID(runID)
				ctx := testAuthorActivityContext(t, context.Background())
				storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
				ctx = withLiveGateExecution(correlation.WithRunID(ctx, runID))
				var finals []string
				if final {
					finals = []string{"done"}
				}
				node, err := identity.AdmitExecutableNodeDeclaration(".", "timer-owner")
				if err != nil {
					t.Fatal(err)
				}
				// Deliberately supply a previously accepted timer at a final stage:
				// runtime no-new-arm is independently proven, not waived by admission.
				root := &contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "final-timer"}}
				source := semanticview.Wrap(&contracts.WorkflowContractBundle{
					RootSchema: &root.Schema,
					FlowTree:   contracts.FlowTree{Root: root, ByID: map[string]*contracts.FlowContractView{".": root}, ByPath: map[string]*contracts.FlowContractView{".": root}},
					Events:     map[string]contracts.EventCatalogEntry{"timer.tick": {}},
					Semantics: contracts.WorkflowSemanticView{
						Name: "final-timer", Version: "1.0.0",
						StageTopologies: map[string]contracts.WorkflowStageTopology{".": contracts.BuildWorkflowStageTopology(".", "done", []string{"done"}, finals, nil, nil, nil)},
						Timers:          []contracts.WorkflowTimerContract{{ID: "new-arm", Node: node, Owner: "runtime", Event: "timer.tick", Delay: "1h", StartOn: "state:done"}},
					},
				})
				identity := testRunScopedWorkflowInstanceForRun(runID, runID)
				createdAt := time.Now().UTC().Truncate(time.Microsecond)
				want := 1
				if final {
					want = 0
				}
				var readiness pipeline.DynamicFlowRuntimeReadinessPlan
				for generation := 0; generation < 2; generation++ {
					admitAttachment, closeAttachment := newTimerReplayAttachmentOwner(t, ctx, selected)
					bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source}, "platform.stage_timer")
					if err != nil {
						t.Fatal(err)
					}
					scheduler := pipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
					if err := scheduler.PrepareStartup(); err != nil {
						t.Fatal(err)
					}
					pc := newTimerReplayCoordinator(t, bus, selected, pipeline.PipelineCoordinatorOptions{
						Module: gateRecoveryModule{source: source}, TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
					})
					bus.SetInterceptors(pc)
					stop := func() {
						t.Helper()
						join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
							t.Fatal(err)
						}
						scheduler.Stop()
						if err := scheduler.Wait(join); err != nil {
							t.Fatal(err)
						}
						if err := bus.WaitForQuiescence(join); err != nil {
							t.Fatal(err)
						}
						closeAttachment()
					}
					t.Cleanup(stop)
					if generation == 0 {
						plan := commitA2FixtureConstruction(t, pc, selected, ctx, identity, pipeline.WorkflowInstance{
							InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "1.0.0",
							CurrentState: "done", CreatedAt: createdAt, EnteredStageAt: createdAt, EntityType: "test_entity",
						}, createdAt)
						readiness = plan.Readiness
					}
					attempt := admitAttachment(readiness)
					if generation != 0 {
						if err := pc.RestoreWorkflowTimers(ctx); err != nil {
							t.Fatal(err)
						}
						if err := pc.ReconcileInitialEntryTimersForAttempt(ctx, identity, attempt, readiness); err != nil {
							t.Fatal(err)
						}
					}
					if err := pc.ArmInitialEntryTimers(ctx, identity); err != nil {
						t.Fatal(err)
					}
					rows, err := selected.ListWorkflowTimerActivations(ctx, runID, entityID, false)
					if err != nil || len(rows) != want {
						t.Fatalf("generation=%d final=%t: timer activations=%+v want=%d: %v", generation, final, rows, want, err)
					}
					runRows, err := selected.ListWorkflowTimerActivations(ctx, runID, "", false)
					if err != nil || len(runRows) != want {
						t.Fatalf("generation=%d final=%t: run-wide schedules=%+v want=%d: %v", generation, final, runRows, want, err)
					}
					if generation != 0 && !final {
						// Replacement joins prior wakeups asynchronously; observe their
						// settlement before asserting that none remain draining.
						for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
							_, draining := pipeline.WorkflowTimerScheduledCountsForTest(scheduler)
							if draining == 0 {
								break
							}
							runtime.Gosched()
						}
					}
					if active, draining := pipeline.WorkflowTimerScheduledCountsForTest(scheduler); active != want || draining != 0 {
						t.Fatalf("generation=%d final=%t: wakeups active=%d draining=%d want=%d", generation, final, active, draining, want)
					}
					observed := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, selected, runID, entityID)
					if observed.Timers != want || observed.ActiveTimers != want {
						t.Fatalf("generation=%d final=%t: persisted schedules=%+v want=%d", generation, final, observed, want)
					}
					stop()
					if generation == 0 {
						if err := closeStore(); err != nil {
							t.Fatal(err)
						}
						selected = reopen()
					}
				}
			})
		}
	}
}
