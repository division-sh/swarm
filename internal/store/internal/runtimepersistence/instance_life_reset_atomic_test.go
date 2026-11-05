package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestInstanceLifeResetCommitBeforeFinalizeReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, reset, _ := nativeInstanceResetFixture(t, backend)
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			committed, err := committer.CommitFlowInstanceActivation(f.ctx, reset)
			if err != nil {
				t.Fatal(err)
			}
			if !committed.StartedOver || len(committed.RestartedAgents) != 1 || len(committed.Lifecycle.Wakeups) != 1 {
				t.Fatalf("reset lost committed finalization evidence: %+v", committed)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			// Simulate an acknowledged commit whose caller never ran its finalizer.
			replayed, err := committer.CommitFlowInstanceActivation(f.ctx, reset)
			if err != nil || !replayed.StartedOver || len(replayed.RestartedAgents) != 1 || len(replayed.Lifecycle.Wakeups) != 1 {
				t.Fatalf("committed reset replay lost finalization evidence: %+v err=%v", replayed, err)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("exact replay changed durable reset/session/history evidence")
			}
			if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, replayed); err != nil {
				t.Fatal(err)
			}
			assertNativeResetSessions(t, f, reset)
			finishNativeResetLife(t, f, reset)
			next := prepareNativeResetPlan(t, f, reset)
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, next); err != nil {
				t.Fatal(err)
			}
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, reset); err == nil {
				t.Fatal("superseded reset replay revived its preceding life")
			} else if failure, ok := failures.EnvelopeFromError(err); !ok || failure.Detail.Code != "instance_restarted" {
				t.Fatalf("superseded replay error=%v", err)
			}
			if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, replayed); err == nil {
				t.Fatal("superseded committed finalizer revived its preceding life")
			}
		})
	}
}

func TestPostgresInstanceResetAndLifecycleAcquireActorsBeforeSourceHead(t *testing.T) {
	f, reset, actor := nativeInstanceResetFixture(t, "postgres")
	pg := f.store.(*PostgresStore)
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	state, found, err := pg.agentPostgresOwner.LoadAgentLifecycleState(ctx, actor)
	if err != nil || !found {
		t.Fatalf("load exact actor: found=%t err=%v", found, err)
	}
	locked, proceed := make(chan int, 1), make(chan struct{})
	done := make(chan error, 1)
	finished, resetFinished := make(chan struct{}), make(chan struct{})
	var release sync.Once
	resetStarted := false
	t.Cleanup(func() {
		cancel()
		release.Do(func() { close(proceed) })
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("lifecycle interleave did not join")
		}
		if resetStarted {
			select {
			case <-resetFinished:
			case <-time.After(5 * time.Second):
				t.Error("reset interleave did not join")
			}
		}
	})
	go func() {
		defer close(finished)
		outcome := mutationprotocol.RunPostgres(ctx, pg.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bool, error) {
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				fingerprint, err := actor.Fingerprint()
				if err != nil {
					return err
				}
				// Pause the real lifecycle caller at its existing actor-lock seat.
				if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "swarm:agent-lifecycle:"+fingerprint); err != nil {
					return err
				}
				var pid int
				if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
					return err
				}
				locked <- pid
				select {
				case <-proceed:
				case <-ctx.Done():
					return ctx.Err()
				}
				_, err = pg.agentPostgresOwner.CommitAgentLifecycleTransitionTx(ctx, attempt, manager.AgentLifecycleTransition{
					DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
					OperationID:      uuid.NewString(), OperationKind: "start", RequestHash: "native-reset-interleave", Identity: actor, AgentID: actor.AgentID(), Trigger: "start",
					ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
					TargetEpoch: state.RuntimeEpoch + 1, TargetGeneration: state.Generation + 1, TargetPhase: state.Phase, ConfigRevision: state.ConfigRevision,
					RunMode: state.RunMode, Topology: state.Topology, ProcessBinding: state.ProcessBinding, Now: time.Now().UTC(),
				})
				return err
			})
			return err == nil, err
		})
		done <- outcome.Err()
	}()
	var pid int
	select {
	case pid = <-locked:
	case err := <-done:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	resetDone := make(chan error, 1)
	resetStarted = true
	go func() {
		defer close(resetFinished)
		_, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, reset)
		resetDone <- err
	}()
	blocked := false
	for !blocked {
		if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if !blocked {
			select {
			case <-time.After(time.Millisecond):
			case err := <-resetDone:
				t.Fatalf("reset did not wait at the held actor: %v", err)
			}
		}
	}
	release.Do(func() { close(proceed) })
	if err := <-done; err != nil {
		t.Errorf("lifecycle interleave: %v", err)
	}
	if err := <-resetDone; err != nil {
		t.Errorf("reset interleave: %v", err)
	}
	assertNativeResetSessions(t, f, reset)
}

func nativeInstanceResetFixture(t *testing.T, backend string) (receiverConfigActivationFixture, pipeline.FlowInstanceActivationPlan, agentidentity.Identity) {
	t.Helper()
	f, first := newWorkflowTargetConstructionFixture(t, backend, true)
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
		t.Fatal(err)
	}
	agents, err := f.store.LoadAgents(f.ctx)
	if err != nil || len(agents) != 1 {
		t.Fatalf("expected one declared actor: %v %v", agents, err)
	}
	actor := agents[0].Config.Identity
	registry := f.store.(interface {
		sessions.Registry
		llm.ConversationPersistence
	})
	ctx := effects.WithDifferentOwner(f.ctx, effects.OwnerBuildTestInfrastructure)
	life := events.InstanceLife{RunID: first.Readiness.RunID, FlowInstance: first.Identity.InstancePath, Life: 1}
	ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Life: &life})
	lease, err := registry.Acquire(ctx, actor, "native-reset-memory")
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.UpsertConversation(ctx, lease, llm.ConversationRecord{SessionID: lease.SessionID, AgentID: actor.AgentID(), Identity: actor, Memory: agentmemory.Plan{Enabled: true}, Messages: []llm.Message{{Role: "assistant", Content: "retained preceding life"}}, TurnCount: 1, Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if result, err := registry.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
		t.Fatalf("release fixture session: %+v %v", result, err)
	}
	finishNativeResetLife(t, f, first)
	return f, prepareNativeResetPlan(t, f, first), actor
}

func finishNativeResetLife(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	current, found, err := f.workflows.LoadConstructedFlowInstance(f.ctx, planOwner(plan), identity.NormalizeEntityID(plan.Identity.EntityID))
	if err != nil || !found {
		t.Fatalf("load current life: %t %v", found, err)
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	state.ExpectedRevision, state.ExpectedState = current.Revision, current.CurrentState
	state.CurrentState = "done"
	state.EnteredStageAt, state.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	var claim deliverylifecycle.Claim
	state = workflowTargetMutationEntry(t, f, state, "finish.requested", &claim)
	_, err = f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: state, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}})
	if err != nil {
		t.Fatal(err)
	}
}

func planOwner(plan pipeline.FlowInstanceActivationPlan) flowidentity.RunScopedFlowInstance {
	return flowidentity.RunScopedFlowInstance{RunID: plan.Readiness.RunID, Route: plan.Identity.Route()}
}

func prepareNativeResetPlan(t *testing.T, f receiverConfigActivationFixture, previous pipeline.FlowInstanceActivationPlan) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	current, found, err := f.workflows.LoadConstructedFlowInstance(f.ctx, planOwner(previous), identity.NormalizeEntityID(previous.Identity.EntityID))
	if err != nil || !found {
		t.Fatalf("load finished life: %t %v", found, err)
	}
	graph, _ := f.bundle.WorkflowStageTopology("review")
	stage, err := graph.ResolveStoredStage(current.CurrentState)
	if err != nil {
		t.Fatal(err)
	}
	req := sqliteFlowActivationRequest(f.bundle, "review", previous.Identity.InstanceID, "", previous.Identity.InstancePath)
	req.Instance, req.ConstructorInput, req.ResolvedKey = previous.Identity, "review.created", "preserved"
	req.OccurredAt = time.Now().UTC()
	req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "review.created", "constructor-fixture", "", []byte(`{"account_id":"preserved"}`), 0, previous.Readiness.RunID, events.EventEnvelope{}, req.OccurredAt)
	req.Restart = &pipeline.FlowInstanceRestart{Life: current.Life, Revision: current.Revision, FinalStage: stage}
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	plan.Instance.CreatedAt, plan.Instance.TemplateVersion, plan.Instance.Status = current.CreatedAt, current.TemplateVersion, current.Status
	plan.Instance.TransitionHistory = current.TransitionHistory
	plan.Readiness, plan.Children = previous.Readiness, nil
	return plan
}

func assertNativeResetSessions(t *testing.T, f receiverConfigActivationFixture, reset pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	var count, active, predecessorTurns, successorTurns int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),COUNT(CASE WHEN status='active' THEN 1 END),COALESCE(SUM(CASE WHEN status='terminated' THEN turn_count END),0),COALESCE(SUM(CASE WHEN status='active' THEN turn_count END),0) FROM agent_sessions WHERE run_id=$1 AND flow_instance=$2`, reset.Readiness.RunID, reset.Identity.InstancePath).Scan(&count, &active, &predecessorTurns, &successorTurns); err != nil {
		t.Fatal(err)
	}
	if count != 2 || active != 1 || predecessorTurns != 1 || successorTurns != 0 {
		t.Fatalf("reset session evidence: count=%d active=%d predecessor_turns=%d successor_turns=%d", count, active, predecessorTurns, successorTurns)
	}
	var predecessor, successor string
	var previousRaw, currentRaw []byte
	if err := f.db.QueryRowContext(f.ctx, `SELECT old.session_id, current.session_id, old.conversation, current.conversation
		FROM agent_sessions old JOIN agent_sessions current ON current.session_id=old.successor_session_id
		WHERE old.run_id=$1 AND old.flow_instance=$2 AND old.status='terminated' AND current.status='active'
		AND current.run_id=old.run_id AND current.flow_instance=old.flow_instance AND current.agent_id=old.agent_id`, reset.Readiness.RunID, reset.Identity.InstancePath).Scan(&predecessor, &successor, &previousRaw, &currentRaw); err != nil {
		t.Fatal(err)
	}
	var previous, current []llm.Message
	if err := json.Unmarshal(previousRaw, &previous); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(currentRaw, &current); err != nil {
		t.Fatal(err)
	}
	if predecessor == successor || len(previous) != 1 || previous[0].Content != "retained preceding life" || len(current) != 0 {
		t.Fatal("reset changed predecessor history, lost its exact successor link, or retained old memory")
	}
}
