package runtimepersistence

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestBundleUpgradeAdmissionPreservesLiveWorkBothStores(t *testing.T) {
	files := map[string]string{
		"schema.yaml":        "name: bundle-upgrade\nstages:\n  pending: {}\nschedules:\n  removed: {every: 1h, emit: clock.removed}\n  retained: {every: 1h, emit: clock.retained}\n",
		"events.yaml":        "clock.removed:\nclock.retained:\n",
		"review/schema.yaml": "name: review\ninstance: request_id\nstages:\n  pending:\n    timers:\n      - {id: pending.timeout, after: 1h, emit: timer.elapsed}\npins:\n  inputs: [task.create]\n",
		"review/state.yaml":  "request_id: text\nlabel: text\nnested: json\nenabled: true\n",
		"review/events.yaml": "task.create:\n  request_id: text\n  label: text\n  nested: json\ntask.started:\ntimer.elapsed:\n",
	}
	candidateFiles := maps.Clone(files)
	candidateFiles["schema.yaml"] = strings.Replace(files["schema.yaml"], "  removed: {every: 1h, emit: clock.removed}\n", "", 1)
	candidateFiles["review/schema.yaml"] = strings.Replace(files["review/schema.yaml"], "  pending:\n    timers:\n      - {id: pending.timeout, after: 1h, emit: timer.elapsed}", "  pending: {}", 1)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []struct{ paused, retainedTimer bool }{{}, {paused: true}, {retainedTimer: true}, {paused: true, retainedTimer: true}} {
			paused, retainedTimer := scenario.paused, scenario.retainedTimer
			selectedFiles := maps.Clone(candidateFiles)
			if retainedTimer {
				selectedFiles["review/schema.yaml"] = strings.ReplaceAll(strings.Replace(files["review/schema.yaml"], "after: 1h", "after: 2h", 1), "timer.elapsed", "timer.changed")
				selectedFiles["review/events.yaml"] = files["review/events.yaml"] + "timer.changed:\n"
			}
			name := backend + "/live"
			if paused {
				name = backend + "/parked"
			}
			if retainedTimer {
				name += "/retained_successor"
			}
			t.Run(name, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
				f.constructKeylessRoot(t)
				reader := f.store.(runbundle.UpgradeReader)
				load := func(context.Context, string) (semanticview.Source, error) { return semanticview.Wrap(f.bundle), nil }
				removed := loadLifecyclePersistenceFixtureForTest(t, map[string]string{"schema.yaml": files["schema.yaml"], "events.yaml": files["events.yaml"]})
				removedPlan, err := runbundle.AdmitPinnedSources(f.ctx, reader, semanticview.Wrap(removed), load)
				if err != nil {
					t.Fatal(err)
				}
				activation, err := f.manager.PrepareFlowInstanceActivation(f.ctx, f.request("upgrade-item", "upgrade-instance", "committed"))
				if err != nil || len(activation.Lifecycle.Timers) != 1 {
					t.Fatalf("prepare genuine timed instance: timers=%d err=%v", len(activation.Lifecycle.Timers), err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if result, err := committer.CommitFlowInstanceActivation(f.ctx, activation); err != nil || !result.Acknowledged {
					t.Fatalf("commit genuine timed instance: %+v, %v", result, err)
				}
				runID := correlation.RunIDFromContext(f.ctx)
				clockStore := f.store.(genericschedule.Store)
				clockSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
				if err != nil {
					t.Fatal(err)
				}
				clockIDs := map[string]string{}
				for _, name := range []string{"removed", "retained"} {
					commit, err := clockStore.AdmitGenericScheduleOutcome(f.ctx, genericschedule.AdmissionCommand{
						ScheduleKey: name, RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance,
						OwnerID: ".", EventType: "clock." + name, Payload: semanticvalue.EmptyObject(),
						RoutingSource: clockSource, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Hour),
					})
					if err != nil || !commit.Acknowledged {
						t.Fatalf("admit genuine %s generic schedule: %+v, %v", name, commit, err)
					}
					clockIDs[name] = commit.Result.Activation.ID
				}
				readClocks := func() map[string]genericschedule.Activation {
					clocks := map[string]genericschedule.Activation{}
					for name, id := range clockIDs {
						clock, found, err := clockStore.LoadGenericScheduleActivation(f.ctx, id)
						if err != nil || !found {
							t.Fatalf("read %s generic schedule: found=%t err=%v", name, found, err)
						}
						clocks[name] = clock
					}
					return clocks
				}
				if paused {
					controller := runcontrol.NewController(f.store.(runcontrol.Store), nil, runcontrol.Options{})
					if _, err := controller.Pause(f.ctx, runcontrol.TransitionRequest{RunID: correlation.RunIDFromContext(f.ctx), Reason: "upgrade hold"}); err != nil {
						t.Fatal(err)
					}
				}
				candidate := loadLifecyclePersistenceFixtureForTest(t, selectedFiles)
				view := semanticview.Wrap(candidate)
				prepared, err := runbundle.AdmitPinnedSources(f.ctx, reader, view, load)
				if err != nil {
					t.Fatal(err)
				}
				capability, err := agentfixture.ProcessCapability(t, context.Background(), f.store)
				if err != nil {
					t.Fatal(err)
				}
				scope, ok := authoractivity.ScopeFromContext(f.ctx)
				if !ok {
					t.Fatal("fixture lacks its admitted author scope")
				}
				original, err := startupownership.WithProcessMutationAuthority(authoractivity.WithScope(context.Background(), scope), capability)
				if err != nil {
					t.Fatal(err)
				}
				next, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: prepared.BundleHash}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				head, exists, err := capability.CurrentSourceSet(original)
				if err != nil || !exists {
					t.Fatalf("admitted predecessor: exists=%t err=%v", exists, err)
				}
				timerReader := f.store.(pipeline.WorkflowTimerActivationPersistence)
				readTimer := func() pipeline.WorkflowTimerActivation {
					timer, found, err := timerReader.LoadWorkflowTimerActivation(original, activation.Lifecycle.Timers[0].Activation.Ref.ActivationID)
					if err != nil || !found {
						t.Fatalf("read exact timer: found=%t err=%v", found, err)
					}
					return timer
				}
				beforeTimer := readTimer()
				beforeClocks := readClocks()
				before := captureSelectedReadSideEffects(t, f.db, backend == "postgres", runID)
				restore := func(plan *runbundle.UpgradePlan) error {
					_, err := capability.RestoreSourceSet(original, agenttopology.SourceSetCommitRequest{
						OperationID: uuid.NewString(), ExpectedRevision: head.Revision, Plan: next, BundleUpgrade: plan,
					})
					return err
				}
				if !paused {
					// The child was created after the first read. The native policy must
					// now report the same stranded instance as a fresh verify read.
					removedNext, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: removedPlan.BundleHash}}, nil)
					if err != nil {
						t.Fatal(err)
					}
					_, nativeErr := capability.RestoreSourceSet(original, agenttopology.SourceSetCommitRequest{
						OperationID: uuid.NewString(), ExpectedRevision: head.Revision, Plan: removedNext, BundleUpgrade: removedPlan,
					})
					_, verifyErr := runbundle.AdmitPinnedSources(original, reader, semanticview.Wrap(removed), load)
					var blocked, verifyBlocked *runbundle.UpgradeConflictError
					if !errors.As(nativeErr, &blocked) || !errors.As(verifyErr, &verifyBlocked) || blocked.Error() != verifyBlocked.Error() {
						t.Fatalf("native/verify refusal differed: native=%v verify=%v", nativeErr, verifyErr)
					}
					found := false
					for _, blocker := range blocked.Blockers {
						found = found || (blocker.RunID == runID && blocker.FlowInstance == activation.Instance.StorageRef && blocker.Kind == "stage_removed" && strings.Contains(blocker.Location, ":"))
					}
					if !found || before != captureSelectedReadSideEffects(t, f.db, backend == "postgres", runID) || !reflect.DeepEqual(beforeTimer, readTimer()) || !reflect.DeepEqual(beforeClocks, readClocks()) {
						t.Fatal("late instance was omitted or native refusal changed retained work")
					}
					// The canonical run owner refuses the absent candidate artifact after
					// timer cancellation was staged; the native boundary must undo both.
					var absent *runlifecycle.SourceArtifactUnavailableError
					if err := restore(prepared); !errors.As(err, &absent) || absent.BundleHash != prepared.BundleHash {
						t.Fatalf("upgrade did not reach canonical candidate-artifact refusal: %v", err)
					}
					currentHead, _, err := capability.CurrentSourceSet(original)
					if err != nil || currentHead.Revision != head.Revision || !reflect.DeepEqual(beforeTimer, readTimer()) || !reflect.DeepEqual(beforeClocks, readClocks()) || before != captureSelectedReadSideEffects(t, f.db, backend == "postgres", runID) {
						t.Fatalf("refused native upgrade left partial timer/source/facts: head=%s err=%v", currentHead.Revision, err)
					}
					availability, err := f.store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(original, runID)
					if err != nil || availability.BundleHash != f.bundle.SourceArtifact.BundleHash() {
						t.Fatalf("refused native upgrade changed run source: %+v, %v", availability, err)
					}
				}
				liveRunID := uuid.NewString()
				if _, err := f.store.(runlifecycle.OperationOwner).CreateRun(original, runlifecycle.CreateRequest{
					RunID: liveRunID, LiveScope: runlifecycle.LiveRunScope{FlowPath: "."}, Origin: runlifecycle.DeploymentRunOrigin(),
					Source: sourceartifactfixture.FactFor(f.bundle.SourceArtifact), StartedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatalf("create live deployment run through its lifecycle owner: %v", err)
				}
				sourceartifactfixture.RequireArtifact(t, original, f.store.(sourceartifactfixture.Writer), candidate.SourceArtifact)
				if paused {
					if err := restore(prepared); err == nil {
						t.Fatal("native transition loaded declarations absent from its request")
					} else if failure, ok := failures.As(err); !ok || failure.Failure.Class != failures.ClassDependencyUnavailable {
						t.Fatalf("unknown retained source lost its typed refusal: %v", err)
					}
					prepared, err = runbundle.AdmitPinnedSources(original, reader, view, load)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := restore(prepared); err != nil {
					t.Fatalf("compatible native upgrade: %v", err)
				}
				currentHead, _, err := capability.CurrentSourceSet(original)
				if err != nil || currentHead.Revision != next.Revision {
					t.Fatalf("source head did not switch: %+v, %v", currentHead, err)
				}
				timer := readTimer()
				clocks := readClocks()
				if !reflect.DeepEqual(clocks["retained"], beforeClocks["retained"]) {
					t.Fatal("upgrade changed a retained generic schedule")
				}
				if paused {
					if !reflect.DeepEqual(clocks, beforeClocks) {
						t.Fatal("upgrade changed a parked run's generic schedules")
					}
				} else if clock := clocks["removed"]; clock.Status != genericschedule.StatusCancelled || clock.CancelCause != "rule_removed" || clock.CancelledAt.IsZero() || !reflect.DeepEqual(clock.Command, beforeClocks["removed"].Command) || !clock.CurrentDueAt.Equal(beforeClocks["removed"].CurrentDueAt) {
					t.Fatalf("removed generic schedule did not cancel with its original arm: %+v", clock)
				}
				availability, err := f.store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(original, runID)
				if err != nil {
					t.Fatal(err)
				}
				live, err := f.store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(original, liveRunID)
				if err != nil || live.BundleHash != candidate.SourceArtifact.BundleHash() {
					t.Fatalf("live run did not follow the bundle: %+v, %v", live, err)
				}
				if paused {
					if availability.BundleHash != f.bundle.SourceArtifact.BundleHash() || !reflect.DeepEqual(timer, beforeTimer) {
						t.Fatal("upgrade changed a parked run or its timer hold")
					}
				} else if retainedTimer {
					selected, err := pipeline.SelectWorkflowTimerSuccessor(semanticview.Wrap(f.bundle), view, beforeTimer, activation.Instance)
					if err != nil || selected == nil || selected.Ref.ActivationID == beforeTimer.Ref.ActivationID {
						t.Fatalf("changed retained timer lacks a distinct successor: %+v, %v", selected, err)
					}
					successor, found, err := timerReader.LoadWorkflowTimerActivation(original, selected.Ref.ActivationID)
					if err != nil || !found || !reflect.DeepEqual(successor, *selected) || !successor.FireAt.Equal(beforeTimer.CreatedAt.Add(2*time.Hour)) {
						t.Fatalf("successor did not preserve its original cause: %+v, found=%t err=%v", successor, found, err)
					}
					expected := beforeTimer
					expected.Status = "cancelled"
					if availability.BundleHash != candidate.SourceArtifact.BundleHash() || !reflect.DeepEqual(timer, expected) {
						t.Fatalf("successor rewrote its predecessor or lost uncaused cancellation: %+v", timer)
					}
					backSource := semanticview.Wrap(f.bundle)
					back, err := runbundle.AdmitPinnedSources(original, reader, backSource, func(_ context.Context, hash string) (semanticview.Source, error) {
						if hash == candidate.SourceArtifact.BundleHash() {
							return view, nil
						}
						return backSource, nil
					})
					if err != nil {
						t.Fatal(err)
					}
					backSet, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: back.BundleHash}}, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := capability.RestoreSourceSet(original, agenttopology.SourceSetCommitRequest{
						OperationID: uuid.NewString(), ExpectedRevision: currentHead.Revision, Plan: backSet, BundleUpgrade: back,
					}); err != nil {
						t.Fatalf("return compatible source B to A: %v", err)
					}
					active, err := timerReader.ListWorkflowTimerActivations(original, runID, activation.Instance.EntityID, true)
					if err != nil || len(active) != 1 || active[0].Ref.ActivationID == beforeTimer.Ref.ActivationID || active[0].Ref.ActivationID == successor.Ref.ActivationID {
						t.Fatalf("A -> B -> A lost its genuinely new active successor: %+v, %v", active, err)
					}
					backArm, err := pipeline.SelectWorkflowTimerSuccessor(view, backSource, successor, activation.Instance)
					if err != nil || backArm == nil || !reflect.DeepEqual(active[0], *backArm) || !active[0].FireAt.Equal(beforeTimer.FireAt) {
						t.Fatalf("returning A arm lost original cause or selected facts: %+v, %v", active, err)
					}
					cancelledB, found, err := timerReader.LoadWorkflowTimerActivation(original, successor.Ref.ActivationID)
					successor.Status = "cancelled"
					if err != nil || !found || !reflect.DeepEqual(cancelledB, successor) || !reflect.DeepEqual(readTimer(), expected) {
						t.Fatalf("returning A revived history or rewrote B: %+v, %v", cancelledB, err)
					}
				} else if availability.BundleHash != candidate.SourceArtifact.BundleHash() || timer.Status != "cancelled" || timer.CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || timer.CancelledAt.IsZero() || timer.SourceTimerID != "" || !timer.FireAt.Equal(beforeTimer.FireAt) {
					t.Fatalf("timer and run did not switch atomically: run=%+v timer=%+v", availability, timer)
				}
			})
		}
	}
}
