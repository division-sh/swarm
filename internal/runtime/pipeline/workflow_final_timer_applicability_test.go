package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestWorkflowFinalTimerApplicabilityUsesExactCatalog(t *testing.T) {
	root := contracts.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "Done"}, []string{"Done"}, nil, nil, nil)
	child := contracts.BuildWorkflowStageTopology("child", "waiting", []string{"waiting", "Done"}, nil, nil, nil, nil)
	stateless := contracts.BuildWorkflowStageTopology("stateless", "", nil, nil, nil, nil, nil)
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		StageTopologies: map[string]contracts.WorkflowStageTopology{".": root, "child": child, "stateless": stateless},
	}})
	for _, owned := range []bool{false, true} {
		for _, tc := range []struct {
			flow, stage string
			mayArm      bool
			valid       bool
		}{
			{".", "waiting", true, true},
			{".", "Done", false, true},
			{".", "done", false, false},
			{".", "pending", false, false},
			{"child", "Done", true, true},
			{"stateless", "pending", true, true},
			{"stateless", "Pending", false, false},
			{"unknown", "Done", false, false},
		} {
			timer := contracts.WorkflowTimerContract{ID: "check", FlowID: tc.flow, Stage: tc.stage, StageOwned: owned}
			mayArm, err := workflowTimerMayArmAtStage(source, timer, tc.stage)
			if (err == nil) != tc.valid || mayArm != tc.mayArm {
				t.Fatalf("flow=%q stage=%q stageOwned=%t: mayArm=%t err=%v", tc.flow, tc.stage, owned, mayArm, err)
			}
		}
	}
	if _, err := workflowTimerMayArmAtStage(nil, contracts.WorkflowTimerContract{ID: "check", FlowID: "."}, "waiting"); err == nil {
		t.Fatal("missing source catalog treated as eligibility")
	}
}

func TestWorkflowFinalTimerNoArmDoesNotChangeAcceptedTopologyValidation(t *testing.T) {
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		StageTopologies: map[string]contracts.WorkflowStageTopology{".": contracts.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "done"}, []string{"done"}, nil, nil, nil)},
	}})
	timer := contracts.WorkflowTimerContract{ID: "already-accepted", FlowID: ".", Stage: "waiting", StageOwned: true}
	if mayArm, err := workflowTimerMayArmAtStage(source, timer, "done"); err != nil || mayArm {
		t.Fatalf("final stage can newly arm: %t %v", mayArm, err)
	}
	if err := validateWorkflowTimerTopology(source, timer); err != nil {
		t.Fatalf("new-arm applicability leaked into accepted topology validation: %v", err)
	}
}

func TestWorkflowFinalInitialTimersRemainUnarmedAcrossRestartBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			for _, final := range []bool{false, true} {
				t.Run(fmt.Sprintf("final_%t", final), func(t *testing.T) {
					store, ctx := backend.open(t)
					ctx = effects.WithExecutionMode(ctx, executionmode.Live)
					var finals []string
					if final {
						finals = []string{"done"}
					}
					node, err := identity.AdmitExecutableNodeDeclaration(".", "timer-owner")
					if err != nil {
						t.Fatal(err)
					}
					source := semanticview.Wrap(&contracts.WorkflowContractBundle{
						Events: map[string]contracts.EventCatalogEntry{"timer.tick": {}},
						Semantics: contracts.WorkflowSemanticView{
							Name: "final-timer", Version: "1.0.0",
							StageTopologies: map[string]contracts.WorkflowStageTopology{".": contracts.BuildWorkflowStageTopology(".", "done", []string{"done"}, finals, nil, nil, nil)},
							Timers:          []contracts.WorkflowTimerContract{{ID: "new-arm", Node: node, Owner: "runtime", Event: "timer.tick", Delay: "1h", StartOn: "state:done"}},
						},
					})
					route, entityID := workflowTimerRootRoute(ctx), uuid.NewString()
					createdAt := canonicalWorkflowTimerTime(time.Now().UTC())
					want := 1
					if final {
						want = 0
					}
					for generation := 0; generation < 2; generation++ {
						owner := pipelineTestWorkOwner(t)
						scheduler := newWorkflowTimerTestScheduler(t, owner)
						pc := newWorkflowTimerOwnerPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
							Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), WorkOwner: owner, TimerScheduler: scheduler,
						})
						if generation == 0 {
							prepared, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowRoute(ctx, route), workflowTimerMaterializedInstance(ctx, entityID, route.InstancePath, WorkflowInstance{
								WorkflowName: ".", WorkflowVersion: "1.0.0", CurrentState: "done", CreatedAt: createdAt, EntityType: "test_entity",
							}), createdAt)
							if err != nil {
								t.Fatal(err)
							}
							if err := store.upsert(ctx, prepared); err != nil {
								t.Fatal(err)
							}
							var committed CommittedWorkflowLifecycleMutation
							if err := store.runPipelineMutation(ctx, func(txctx context.Context) error {
								var err error
								committed, err = commitPipelineTestWorkflowLifecycle(txctx, store, lifecycle)
								return err
							}); err != nil {
								t.Fatal(err)
							}
							if err := pc.FinalizeInitialEntryLifecycle(ctx, committed); err != nil {
								t.Fatal(err)
							}
						} else {
							if err := pc.workflowTimers.Restore(ctx); err != nil {
								t.Fatal(err)
							}
							if err := pc.ReconcileInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, route)); err != nil {
								t.Fatal(err)
							}
						}
						if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowRoute(ctx, route)); err != nil {
							t.Fatal(err)
						}
						rows := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, false)
						if len(rows) != want {
							t.Fatalf("generation=%d final=%t: persisted %d timer activations, want %d", generation, final, len(rows), want)
						}
						if active, draining := workflowTimerScheduledCounts(scheduler); active != want || draining != 0 {
							t.Fatalf("generation=%d final=%t: wakeups active=%d draining=%d want=%d", generation, final, active, draining, want)
						}
						query := "SELECT COUNT(*) FROM timers WHERE run_id = ? AND task_type = 'workflow_timer'"
						if !store.isSQLite() {
							query = "SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND task_type = 'workflow_timer'"
						}
						var schedules int
						if err := store.testDB().QueryRowContext(ctx, query, correlation.RunIDFromContext(ctx)).Scan(&schedules); err != nil || schedules != want {
							t.Fatalf("generation=%d final=%t: persisted schedules=%d want=%d: %v", generation, final, schedules, want, err)
						}
						stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						err := pc.StopWorkflowTimerLifecycle(stopCtx)
						cancel()
						if err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
