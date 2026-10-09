package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

func TestWorkflowTimerCauseReplayReopenAndIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"active", "fired", "cancelled", "advanced", "advanced_cancelled"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				selected, closeStore, reopen := openTimerReplayNativeStore(t, backend)
				runID, entityID := uuid.NewString(), uuid.NewString()
				ctx := testAuthorActivityContext(t, context.Background())
				storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
				ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
				bundle := workflowTimerServedLifecycleBundle(t, state == "advanced" || state == "advanced_cancelled")
				source := semanticview.Wrap(bundle)
				identity := testRunScopedWorkflowInstanceForRun(runID, runID)
				build := func() (*runtimepipeline.PipelineCoordinator, func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) runtimepipeline.DynamicFlowRuntimeActivationAttempt, func()) {
					t.Helper()
					admitAttachment, closeAttachment := newTimerReplayAttachmentOwner(t, ctx, selected)
					bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source}, "platform.stage_timer")
					if err != nil {
						t.Fatal(err)
					}
					// Keep actual admitted wakeups dormant while making each temporal
					// cut explicitly through the real fire/cancel owners.
					scheduler := runtimepipeline.NewSchedulerWithWorkOwner(pipelineExternalTestWorkOwner(t))
					if err := scheduler.PrepareStartup(); err != nil {
						t.Fatal(err)
					}
					pc := newTimerReplayCoordinator(t, bus, selected, runtimepipeline.PipelineCoordinatorOptions{
						Module: gateRecoveryModule{source: source}, TimerScheduler: scheduler, WorkOwner: pipelineExternalTestWorkOwner(t),
					})
					var once sync.Once
					stop := func() {
						t.Helper()
						once.Do(func() {
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
							closeAttachment()
						})
					}
					t.Cleanup(stop)
					return pc, admitAttachment, stop
				}
				pc, admitAttachment, stop := build()
				at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
				instance := runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "waiting", EntityType: "test_entity", CreatedAt: at, EnteredStageAt: at,
				}
				initialized, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, identity, instance, at)
				if err != nil {
					t.Fatal(err)
				}
				construction, err := flowactivationfixture.Command(ctx, initialized, lifecycle, at)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, construction)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("commit real initial timer: %+v, %v", committed, err)
				}
				if err := pc.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
					t.Fatal(err)
				}
				attempt := admitAttachment(construction.Plan.Readiness)
				reader := runtimepipeline.WorkflowTimerActivationPersistence(selected)
				rows, err := reader.ListWorkflowTimerActivations(ctx, runID, entityID, false)
				if err != nil || len(rows) != 1 {
					t.Fatalf("initial selected rows: %+v, %v", rows, err)
				}
				initial := rows[0]
				cancelTimer := func() {
					t.Helper()
					if result, err := reader.CommitWorkflowTimerReconciliation(ctx, runtimepipeline.WorkflowTimerReconciliationCommand{
						RunID: runID, Route: initial.Route, EntityID: entityID, ActivationAttempt: &attempt,
						Plan: runtimepipeline.WorkflowLifecycleMutationPlan{Timers: []runtimepipeline.WorkflowTimerMutation{{Kind: runtimepipeline.WorkflowTimerMutationCancel, Activation: initial}}},
					}); err != nil || !result.Committed {
						t.Fatalf("real monotonic cancellation: %+v, %v", result, err)
					}
				}
				if state == "fired" || state == "advanced" || state == "advanced_cancelled" {
					if outcome, err := runtimepipeline.FireWorkflowTimerOccurrenceForTest(ctx, pc, initial); err != nil || outcome != runtimepipeline.WorkflowTimerFireCommitted {
						t.Fatalf("real occurrence commit: %s, %v", outcome, err)
					}
				}
				if state == "cancelled" || state == "advanced_cancelled" {
					cancelTimer()
					cancelTimer()
				}
				before, found, err := reader.LoadWorkflowTimerActivation(ctx, initial.Ref.ActivationID)
				if err != nil || !found {
					t.Fatalf("load current occurrence: %v, %v", found, err)
				}
				if state == "advanced" || state == "advanced_cancelled" {
					if !before.FireAt.Equal(initial.FireAt.Add(initial.RecurrenceInterval)) || before.FiredAt.IsZero() {
						t.Fatal("real fire did not establish the advanced coordinate")
					}
				}
				baseline := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, selected, runID, entityID)
				stop()
				if err := closeStore(); err != nil {
					t.Fatal(err)
				}
				selected = reopen()
				pc, admitAttachment, _ = build()
				attempt = admitAttachment(construction.Plan.Readiness)
				reader = selected
				assertUnchanged := func() {
					t.Helper()
					got, found, err := reader.LoadWorkflowTimerActivation(ctx, initial.Ref.ActivationID)
					if err != nil || !found || !reflect.DeepEqual(got, before) {
						t.Fatalf("replay altered exact persisted activation: %+v, %v, %v", got, found, err)
					}
					if got := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, selected, runID, entityID); got != baseline {
						t.Fatalf("replay altered timer/publication/revision effects: before=%+v after=%+v", baseline, got)
					}
				}
				assertUnchanged()
				if result, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, construction); err != nil || !result.Acknowledged || result.Created || len(result.Lifecycle.Wakeups) != 0 || len(result.Lifecycle.Cancellations) != 0 {
					t.Fatalf("construction's earlier exact-replay gate: %+v, %v", result, err)
				}
				assertUnchanged()
				for i := 0; i < 2; i++ {
					if err := pc.ReconcileInitialEntryTimersForAttempt(ctx, identity, attempt, construction.Plan.Readiness); err != nil {
						t.Fatalf("production declaration reconciliation: %v", err)
					}
					assertUnchanged()
				}
				command := runtimepipeline.WorkflowTimerReconciliationCommand{
					RunID: runID, Route: initial.Route, EntityID: entityID, ActivationAttempt: &attempt,
					Plan: runtimepipeline.WorkflowLifecycleMutationPlan{Timers: []runtimepipeline.WorkflowTimerMutation{{Kind: runtimepipeline.WorkflowTimerMutationInsert, Activation: initial}}},
				}
				start, results := make(chan struct{}), make(chan error, 4)
				for i := 0; i < 4; i++ {
					go func() {
						<-start
						result, err := reader.CommitWorkflowTimerReconciliation(ctx, command)
						if err == nil && (!result.Committed || len(result.Wakeups) != 1 || result.Wakeups[0] != initial.Ref) {
							err = fmt.Errorf("exact replay returned invalid committed wakeup evidence: %+v", result)
						}
						results <- err
					}()
				}
				close(start)
				var replayErrors []error
				for i := 0; i < 4; i++ {
					replayErrors = append(replayErrors, <-results)
				}
				if err := errors.Join(replayErrors...); err != nil {
					t.Fatalf("concurrent exact selected replay: %v", err)
				}
				assertUnchanged()
				t.Run("wrong_attachment_instance", func(t *testing.T) {
					changed := initial
					changed.Route = flowidentity.StoredRoute(".", "other", "other")
					negative := runtimepipeline.WorkflowTimerReconciliationCommand{
						RunID: runID, Route: changed.Route, EntityID: entityID, ActivationAttempt: &attempt,
						Plan: runtimepipeline.WorkflowLifecycleMutationPlan{Timers: []runtimepipeline.WorkflowTimerMutation{{Kind: runtimepipeline.WorkflowTimerMutationInsert, Activation: changed}}},
					}
					if err := negative.Validate(); err == nil {
						t.Fatal("foreign instance accepted the exact attachment receipt")
					}
					if result, err := reader.CommitWorkflowTimerReconciliation(ctx, negative); err == nil || result.Committed {
						t.Fatalf("foreign attachment instance committed: %+v, %v", result, err)
					}
					assertUnchanged()
				})
				for _, test := range []struct {
					name   string
					change func(*runtimepipeline.WorkflowTimerActivation)
				}{
					{"owner", func(a *runtimepipeline.WorkflowTimerActivation) { a.OwnerAgent += ".other" }},
					{"mode", func(a *runtimepipeline.WorkflowTimerActivation) { a.ExecutionMode = executionmode.Mock }},
					{"payload", func(a *runtimepipeline.WorkflowTimerActivation) { a.Payload = []byte(`{"different":true}`) }},
					{"declaration", func(a *runtimepipeline.WorkflowTimerActivation) { a.Ref.DeclarationKey += ".other" }},
					{"revision", func(a *runtimepipeline.WorkflowTimerActivation) { a.Ref.DeclarationRevision += ".other" }},
					{"cause", func(a *runtimepipeline.WorkflowTimerActivation) {
						a.Ref.Cause = timeridentity.WorkflowTimerActivationCauseEvent
					}},
					{"route", func(a *runtimepipeline.WorkflowTimerActivation) {
						a.Route = flowidentity.StoredRoute("other", a.Route.InstanceID, a.Route.InstancePath)
					}},
					{"source", func(a *runtimepipeline.WorkflowTimerActivation) {
						var err error
						a.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "timer-proof", FlowInstance: a.Route.InstancePath, EntityID: a.EntityID})
						if err != nil {
							t.Fatal(err)
						}
					}},
					{"lineage", func(a *runtimepipeline.WorkflowTimerActivation) {
						a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromEventID, a.ReconstructionOwner = uuid.NewString(), uuid.NewString(), uuid.NewString(), "fork-owner"
						a.ForkedFromPointKind, a.ForkedFromPointRevision, a.SourceArmedAt = "event", 1, a.CreatedAt
					}},
					{"requested_ahead", func(a *runtimepipeline.WorkflowTimerActivation) {
						a.FireAt = before.FireAt.Add(time.Hour)
					}},
				} {
					t.Run("rollback_"+test.name, func(t *testing.T) {
						changed := initial
						test.change(&changed)
						candidate := changed
						candidate.Ref.ActivationID = uuid.NewString()
						candidate.Ref.DeclarationKey += ".rollback-probe"
						negative := runtimepipeline.WorkflowTimerReconciliationCommand{
							RunID: runID, Route: changed.Route, EntityID: entityID, ActivationAttempt: &attempt,
							Plan: runtimepipeline.WorkflowLifecycleMutationPlan{Timers: []runtimepipeline.WorkflowTimerMutation{
								{Kind: runtimepipeline.WorkflowTimerMutationInsert, Activation: candidate},
								{Kind: runtimepipeline.WorkflowTimerMutationInsert, Activation: changed},
							}},
						}
						if err := negative.Validate(); err != nil {
							t.Fatalf("counterexample must reach selected mutation admission: %v", err)
						}
						if result, err := reader.CommitWorkflowTimerReconciliation(ctx, negative); err == nil || result.Committed {
							t.Fatalf("changed cause facts committed: %+v, %v", result, err)
						}
						if _, found, err := reader.LoadWorkflowTimerActivation(ctx, candidate.Ref.ActivationID); err != nil || found {
							t.Fatalf("preceding insert survived rejected replay: found=%v, %v", found, err)
						}
						assertUnchanged()
					})
				}
				if state != "active" {
					if outcome, err := runtimepipeline.FireWorkflowTimerOccurrenceForTest(ctx, pc, initial); err != nil || outcome != runtimepipeline.WorkflowTimerFireTerminal {
						t.Fatalf("stale/terminal occurrence gained execution authority: %s, %v", outcome, err)
					}
					assertUnchanged()
				}
			})
		}
	}
}
