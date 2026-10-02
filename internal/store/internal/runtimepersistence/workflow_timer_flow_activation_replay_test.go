package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Flow activation converges at its immutable materialization gate, before
// timer insertion. This is caller coverage, not insertion replay coverage.
func TestWorkflowTimerCauseReplayFlowActivationGateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"active", "fired", "cancelled", "advanced", "advanced_cancelled"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				recurring := state == "advanced" || state == "advanced_cancelled"
				f := newReceiverConfigActivationFixtureWithTimer(t, backend, false, false, true, recurring)
				req := f.request("timer-business-key", "ti-timer-replay", "committed")
				req.OccurredAt = time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
				if err != nil || len(plan.Lifecycle.Timers) != 1 {
					t.Fatalf("real flow planner omitted timer lifecycle: %+v, %v", plan.Lifecycle, err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if result, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged || !result.Created || len(result.Lifecycle.Wakeups) != 1 {
					t.Fatalf("real flow activation creation: %+v, %v", result, err)
				}
				reader := f.store.(pipeline.WorkflowTimerActivationPersistence)
				initial := plan.Lifecycle.Timers[0].Activation
				if state == "fired" || recurring {
					source := semanticview.Wrap(f.bundle)
					descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
					if err != nil {
						t.Fatal(err)
					}
					descriptors = append(descriptors, authoractivity.EventDescriptor{EventType: "platform.stage_timer", Disposition: authoractivity.StoryDifferent})
					scope, ok := authoractivity.ScopeFromContext(f.ctx)
					if !ok {
						t.Fatal("missing admitted author scope")
					}
					lease, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(lease.Release)
					fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
					publisher, err := newStoreTestEventBus(t, f.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact})
					if err != nil {
						t.Fatal(err)
					}
					occurrence := initial.Occurrence()
					firedAt := time.Now().UTC().Truncate(time.Microsecond)
					event := eventtest.InExecutionMode(eventtest.RuntimeControlWithRoutingSource(
						timeridentity.WorkflowTimerOccurrenceEventID(occurrence), events.EventType(initial.EventType),
						"runtime.workflow_timer", occurrence.TaskID(), initial.Payload, 0, initial.RunID, "",
						events.EventEnvelope{EntityID: initial.EntityID, FlowInstance: initial.Route.InstancePath}, initial.RoutingSource, firedAt), initial.ExecutionMode)
					publications, err := publisher.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: event}})
					if err != nil || len(publications) != 1 {
						t.Fatalf("real occurrence publication preparation: %d, %v", len(publications), err)
					}
					t.Cleanup(func() {
						if err := publisher.ReleaseEnginePublications(context.WithoutCancel(f.ctx), publications); err != nil {
							t.Error(err)
						}
					})
					if result, err := f.store.(pipeline.WorkflowTimerOccurrenceOwner).CommitWorkflowTimerOccurrence(f.ctx, pipeline.WorkflowTimerOccurrenceCommand{
						Activation: initial, Occurrence: occurrence, FiredAt: firedAt, Publication: publications[0],
					}); err != nil || result.Outcome != pipeline.WorkflowTimerOccurrenceCommitted {
						t.Fatalf("real flow timer occurrence: %+v, %v", result, err)
					}
				}
				if state == "cancelled" || state == "advanced_cancelled" {
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					request := pipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding)
					admitted, err := f.workflows.BeginDynamicFlowRuntimeActivation(f.ctx, request)
					if err != nil || !admitted.Acknowledged {
						t.Fatalf("real flow activation attempt: %+v, %v", admitted, err)
					}
					t.Cleanup(func() {
						if err := f.workflows.RetireDynamicFlowRuntimeActivationAttempt(context.WithoutCancel(f.ctx), admitted.Attempt); err != nil {
							t.Error(err)
						}
					})
					command := pipeline.WorkflowTimerReconciliationCommand{
						RunID: initial.RunID, Route: initial.Route, EntityID: initial.EntityID, ActivationAttempt: &admitted.Attempt,
						Plan: pipeline.WorkflowLifecycleMutationPlan{Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationCancel, Activation: initial}}},
					}
					for i := 0; i < 2; i++ {
						if result, err := reader.CommitWorkflowTimerReconciliation(f.ctx, command); err != nil || !result.Committed {
							t.Fatalf("real monotonic flow timer cancellation: %+v, %v", result, err)
						}
					}
				}
				before, found, err := reader.LoadWorkflowTimerActivation(f.ctx, initial.Ref.ActivationID)
				if err != nil || !found {
					t.Fatalf("load current flow timer: %v, %v", found, err)
				}
				storage, err := ObserveWorkflowTimerReplayStorageForTest(f.ctx, f.store, initial.RunID, initial.EntityID)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					result, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !result.Acknowledged || result.Created || len(result.Lifecycle.Wakeups) != 0 || len(result.Lifecycle.Cancellations) != 0 {
						t.Fatalf("earlier immutable flow replay gate: %+v, %v", result, err)
					}
					got, found, err := reader.LoadWorkflowTimerActivation(f.ctx, initial.Ref.ActivationID)
					if err != nil || !found || !reflect.DeepEqual(got, before) {
						t.Fatalf("flow replay changed current timer: %+v, %v, %v", got, found, err)
					}
					if got, err := ObserveWorkflowTimerReplayStorageForTest(f.ctx, f.store, initial.RunID, initial.EntityID); err != nil || got != storage {
						t.Fatalf("flow replay changed timer/publication/revision effects: %+v, %v", got, err)
					}
				}
			})
		}
	}
}
