package manager_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestAgentRowNativeLostAckCacheAdoptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"replacement", "lost_ack", "cancel_after_commit", "read_after_commit", "historical", "retired_process", "rollback", "shutdown_during_factory", "reintroduction_lost_ack", "reintroduction_cancel_after_commit", "reintroduction_read_after_commit"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				var selected storetest.AgentFixtureStore
				var reopen func() manager.AgentLifecycleStateReader
				if backend == "postgres" {
					location := testutil.StartPostgresDSN(t)
					selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
					reopen = func() manager.AgentLifecycleStateReader {
						owner, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, location)
						return owner
					}
				} else {
					location := filepath.Join(t.TempDir(), "agent-row.db")
					selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
					reopen = func() manager.AgentLifecycleStateReader {
						owner, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
						return owner
					}
				}
				ctx := manager.TerminalPanicFixtureContext()
				record := manager.ReconfigureMemoryNativeFixture(t, false)
				if cut != "replacement" {
					storetest.RequireRunningRun(t, ctx, selected.(storetest.RunFixtureStore), record.Config.Identity.RunID, time.Now().UTC())
					identity, err := record.Config.Identity.Plan()
					if err != nil {
						t.Fatal(err)
					}
					revision, err := manager.AgentConfigPlanRevision(record.Config, identity)
					if err != nil {
						t.Fatal(err)
					}
					coordinate := agenttopology.SourceCoordinate{BundleHash: sourceartifactfixture.BundleHash}
					plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, []agenttopology.DesiredAgent{{Identity: identity, ConfigRevision: revision, Source: coordinate}})
					if err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(cut, "reintroduction_") {
						spawned, err := agentfixture.CommitStatic(t, ctx, selected, manager.AgentLifecycleTransition{
							OperationID: uuid.NewString(), RequestHash: uuid.NewString(), OperationKind: "spawn", Trigger: "spawn",
							Identity: record.Config.Identity, AgentID: record.Config.ID, Agent: &record,
							TargetEpoch: 1, TargetGeneration: 1, TargetPhase: manager.AgentLifecycleRegistered,
							ConfigRevision: revision, RunMode: manager.AgentRunModeStopped, Now: time.Now().UTC(),
						})
						if err != nil {
							t.Fatal(err)
						}
						if _, err := agentfixture.CommitStatic(t, ctx, selected, manager.AgentLifecycleTransition{
							OperationID: uuid.NewString(), RequestHash: uuid.NewString(), OperationKind: "teardown", Trigger: "teardown",
							Identity: record.Config.Identity, AgentID: record.Config.ID,
							ExpectedEpoch: spawned.RuntimeEpoch, ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
							TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1, TargetPhase: manager.AgentLifecycleTerminated,
							ConfigRevision: revision, RunMode: manager.AgentRunModeStopped, Now: time.Now().UTC(),
						}); err != nil {
							t.Fatal(err)
						}
						process, err := agentfixture.ProcessCapability(t, ctx, selected)
						if err != nil {
							t.Fatal(err)
						}
						current, _, err := process.CurrentSourceSet(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan}); err != nil {
							t.Fatal(err)
						}
					}
					grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, coordinate)
					if err != nil {
						t.Fatal(err)
					}
					record.Topology, err = agenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, agenttopology.LifetimeDurableManaged)
					if err != nil {
						t.Fatal(err)
					}
					manager.ProveAgentRowNativeSpawnAdoption(t, grant, selected, selected.(manager.AgentLifecycleOutcomeReader), record, func() any {
						value, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
						if err != nil {
							t.Fatal(err)
						}
						return value
					}, func() {
						process, err := agentfixture.ProcessCapability(t, ctx, selected)
						if err != nil {
							t.Fatal(err)
						}
						if err := process.Release(ctx); err != nil {
							t.Fatal(err)
						}
					}, cut)
					return
				}
				storetest.RequireStaticAgentFixture(t, ctx, selected, record)
				view, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
				if err != nil || !found {
					t.Fatalf("native row: found=%v err=%v", found, err)
				}
				record.Topology = view.Topology
				proveNativeAgentRowCAS(t, selected, reopen(), record)
				outcomes, ok := selected.(manager.AgentLifecycleOutcomeReader)
				if !ok {
					t.Fatal("selected store lacks the native immutable-outcome owner")
				}
				manager.ProveAgentRowNativeLostAckCacheAdoption(t, storetest.AgentLifecycleFixture(t, selected), selected, outcomes, record, func() any {
					snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					return snapshot
				}, func(candidate actors.AgentConfig) (manager.AgentLifecyclePersistence, agenttopology.Admission) {
					return admitNativeReconfigureCandidate(t, selected, candidate)
				}, func(controller agentcontrol.Controller) {
					proveNativeAgentRestartAPICompletionLoss(t, selected, record.Config, controller)
				})
			})
		}
	}
}

func proveNativeAgentRowCAS(t *testing.T, selected storetest.AgentFixtureStore, peer manager.AgentLifecycleStateReader, record manager.PersistedAgent) {
	t.Helper()
	ctx := manager.TerminalPanicFixtureContext()
	before, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || before.Phase != manager.AgentLifecycleRunning {
		t.Fatalf("native CAS predecessor: found=%v row=%+v err=%v", found, before, err)
	}
	other, found, err := peer.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || !reflect.DeepEqual(before, other) {
		t.Fatalf("second native handle disagrees: found=%v row=%+v err=%v", found, other, err)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := process.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("native CAS source set: found=%v err=%v", found, err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, agenttopology.SourceCoordinate{BundleHash: before.ProcessBinding.BundleHash})
	if err != nil {
		t.Fatal(err)
	}
	initialRelease := manager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), RequestHash: uuid.NewString(), OperationKind: "self_release", Trigger: "self_release",
		Identity: before.Identity, AgentID: before.AgentID,
		ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
		TargetEpoch: before.RuntimeEpoch, TargetGeneration: before.Generation, TargetPhase: manager.AgentLifecycleRegistered,
		ConfigRevision: before.ConfigRevision, RunMode: manager.AgentRunModeStopped, Topology: before.Topology,
		DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation}, Now: time.Now().UTC(),
	}
	if _, err := grant.CommitAgentLifecycleTransition(ctx, initialRelease); err != nil {
		t.Fatal(err)
	}
	before, found, err = selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || before.Phase != manager.AgentLifecycleRegistered {
		t.Fatalf("native CAS released predecessor: found=%v row=%+v err=%v", found, before, err)
	}
	other, found, err = peer.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || !reflect.DeepEqual(before, other) {
		t.Fatalf("second native handle lost the released predecessor: found=%v row=%+v err=%v", found, other, err)
	}
	request := manager.AgentLifecycleTransition{
		Identity: before.Identity, AgentID: before.AgentID, OperationKind: "start", Trigger: "start",
		ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
		TargetEpoch: before.RuntimeEpoch, TargetGeneration: before.Generation + 1, TargetPhase: manager.AgentLifecycleRunning,
		ConfigRevision: before.ConfigRevision, RunMode: manager.AgentRunModeStandard, Topology: before.Topology,
		DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation}, Now: time.Now().UTC(),
	}
	type outcome struct {
		request manager.AgentLifecycleTransition
		result  manager.AgentLifecycleTransitionResult
		err     error
	}
	start, results := make(chan struct{}), make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		candidate := request
		candidate.OperationID, candidate.RequestHash = uuid.NewString(), uuid.NewString()
		go func() {
			<-start
			result, err := grant.CommitAgentLifecycleTransition(ctx, candidate)
			results <- outcome{request: candidate, result: result, err: err}
		}()
	}
	close(start)
	var winner outcome
	accepted := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			accepted++
			winner = result
			continue
		}
		var conflict *failures.Error
		if !errors.As(result.err, &conflict) || conflict.Failure.Class != failures.ClassLifecycleConflict {
			t.Fatalf("native contested CAS returned a non-conflict: %v", result.err)
		}
	}
	if accepted != 1 || winner.result.Generation != before.Generation+1 {
		t.Fatalf("native CAS accepted %d writers: %+v", accepted, winner.result)
	}
	snapshot := func() any {
		t.Helper()
		value, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	after := snapshot()
	replay, err := grant.CommitAgentLifecycleTransition(ctx, winner.request)
	expectedBytes, expectedErr := canonicaljson.Bytes(winner.result)
	replayBytes, replayErr := canonicaljson.Bytes(replay)
	if err != nil || expectedErr != nil || replayErr != nil || !replay.Replayed || winner.result.Replayed || string(expectedBytes) != string(replayBytes) || !reflect.DeepEqual(after, snapshot()) {
		t.Fatalf("native CAS replay changed its exact result or application evidence: %v", err)
	}
	for _, name := range []string{"epoch", "generation", "phase", "configuration", "topology", "duplicate_spawn"} {
		stale := request
		stale.OperationID, stale.RequestHash = uuid.NewString(), uuid.NewString()
		stale.ExpectedGeneration, stale.ExpectedPhase = winner.result.Generation, winner.result.Phase
		stale.TargetGeneration, stale.OperationKind, stale.Trigger = winner.result.Generation+1, "restart", "restart"
		switch name {
		case "epoch":
			stale.ExpectedEpoch++
		case "generation":
			stale.ExpectedGeneration--
		case "phase":
			stale.ExpectedPhase = manager.AgentLifecycleRegistered
		case "configuration":
			stale.ConfigRevision = "foreign-config"
		case "topology":
			stale.Topology = agenttopology.Admission{}
		case "duplicate_spawn":
			stale.OperationKind, stale.Trigger = "spawn", "spawn"
			stale.ExpectedEpoch, stale.ExpectedGeneration, stale.ExpectedPhase = 0, 0, ""
			stale.TargetGeneration, stale.TargetPhase, stale.RunMode = 1, manager.AgentLifecycleRegistered, manager.AgentRunModeStopped
			stale.Agent = &record
		}
		if _, err := grant.CommitAgentLifecycleTransition(ctx, stale); err == nil || !reflect.DeepEqual(after, snapshot()) {
			t.Fatalf("native %s refusal changed application evidence: %v", name, err)
		}
	}
	release := request
	release.OperationID, release.RequestHash = uuid.NewString(), uuid.NewString()
	release.OperationKind, release.Trigger = "self_release", "self_release"
	release.ExpectedGeneration, release.ExpectedPhase = winner.result.Generation, winner.result.Phase
	release.TargetGeneration, release.TargetPhase, release.RunMode = winner.result.Generation, manager.AgentLifecycleRegistered, manager.AgentRunModeStopped
	if _, err := grant.CommitAgentLifecycleTransition(ctx, release); err != nil {
		t.Fatal(err)
	}
	after = snapshot()
	request.OperationID, request.RequestHash = uuid.NewString(), uuid.NewString()
	if _, err := grant.CommitAgentLifecycleTransition(ctx, request); err == nil || !reflect.DeepEqual(after, snapshot()) {
		t.Fatalf("same phase after ABA admitted the old generation: %v", err)
	}
	current, found, err := peer.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || current.Phase != before.Phase || current.Generation != winner.result.Generation {
		t.Fatalf("second-handle ABA readback: found=%v row=%+v err=%v", found, current, err)
	}
}

func TestAgentRowIndividualSourceSetReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected storetest.AgentFixtureStore
			if backend == "postgres" {
				selected = storetest.StartPostgresRuntimeStore(t)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			ctx := manager.TerminalPanicFixtureContext()
			record := manager.TerminalPanicFixture(t)
			record.Config.LLMBackend, record.Config.ResolvedLLMBackend = "anthropic", "anthropic"
			record.Topology = agenttopology.Admission{}
			storetest.RequireStaticAgentFixture(t, ctx, selected, record)
			view, found, err := selected.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found {
				t.Fatalf("native fixture row: found=%v err=%v", found, err)
			}
			record.Topology = view.Topology
			manager.ProveAgentRowIndividualSourceSetReplacement(t, storetest.AgentLifecycleFixture(t, selected), selected, record, func() (manager.AgentLifecyclePersistence, agenttopology.Admission) {
				process, err := agentfixture.ProcessCapability(t, ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				current, found, err := process.CurrentSourceSet(ctx)
				if err != nil || !found {
					t.Fatalf("native source set: found=%v err=%v", found, err)
				}
				sources := append([]agenttopology.SourceCoordinate(nil), current.Sources...)
				sources = append(sources, agenttopology.SourceCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("c", 64)})
				plan, err := agenttopology.NewSourceSetPlan(sources, current.Agents)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan}); err != nil {
					t.Fatal(err)
				}
				stamp, found, err := selected.LoadAgentLifecycleState(context.Background(), record.Config.Identity)
				if err != nil || !found {
					t.Fatalf("native predecessor stamp: found=%v err=%v", found, err)
				}
				grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: stamp.ProcessBinding.BundleHash, RuntimeInstanceID: stamp.ProcessBinding.RuntimeInstanceID, RuntimeGeneration: stamp.ProcessBinding.RuntimeGeneration + 1, SourceSetRevision: plan.Revision})
				if err != nil {
					t.Fatal(err)
				}
				topology, err := agenttopology.StaticAdmission(plan.Revision, stamp.ProcessBinding.BundleHash, stamp.Topology.Lifetime)
				if err != nil {
					t.Fatal(err)
				}
				return grant, topology
			})
		})
	}
}
