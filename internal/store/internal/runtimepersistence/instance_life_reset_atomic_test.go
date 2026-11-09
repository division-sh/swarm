package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestInstanceLifeResetCommitBeforeFinalizeReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/fieldless_history", func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithOwnership(t, backend, false, map[string]string{
				"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "schema.yaml": "stages: []\n",
				"review/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
				"review/schema.yaml":   "stages:\n  active: {}\n  done: {final: true}\n",
				"review/events.yaml":   "finish.requested:\n",
				"review/nodes.yaml":    "progress:\n  finish.requested:\n    advances_to: done\n",
			}, nil, ownStoreTestAgentManager, nil)
			fact, err := correlation.NewSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			f.ctx = correlation.WithSourceArtifactFact(f.ctx, fact)
			f.constructKeylessRoot(t)
			source := semanticview.Wrap(f.bundle)
			owner, err := flowidentity.StandingForGeneration(source, "review", correlation.RunIDFromContext(f.ctx))
			if err != nil {
				t.Fatal(err)
			}
			first, err := f.manager.PrepareFlowInstanceActivation(f.ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: owner, OccurredAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			finishNativeResetLife(t, f, first)
			reset := prepareNativeResetPlan(t, f, first)
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, reset); err != nil {
				t.Fatal(err)
			}
			reader := f.store.(interface {
				LoadOperatorEntityAtLife(context.Context, string, string, int64) (operatorread.OperatorEntityFull, error)
			})
			for _, life := range []int64{1, 2} {
				got, err := reader.LoadOperatorEntityAtLife(f.ctx, owner.EntityID, reset.Readiness.RunID, life)
				if err != nil || got.Entity.Life == nil || *got.Entity.Life != life || got.Entity.EntityID != owner.EntityID ||
					got.Entity.CreatedAt != nil || got.Entity.UpdatedAt != nil || got.Entity.LifecycleCreatedAt == nil || len(got.Fields) != 0 {
					t.Fatalf("fieldless life %d lost exact header history or fabricated data clocks: %+v err=%v", life, got, err)
				}
			}
		})
		t.Run(backend+"/historical_life1", func(t *testing.T) {
			f, first := newWorkflowTargetConstructionFixture(t, backend, true)
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			committed, err := committer.CommitFlowInstanceActivation(f.ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
				t.Fatal(err)
			}
			agents, err := f.store.LoadAgents(f.ctx)
			if err != nil || len(agents) != 1 {
				t.Fatalf("historical fixture actors: %v %v", agents, err)
			}
			attempt := proveNativeTurnAdmissionLife(t, f, first, agents[0].Config.Identity, true)
			finishNativeResetLife(t, f, first)
			reset := prepareNativeResetPlan(t, f, first)
			committed, err = committer.CommitFlowInstanceActivation(f.ctx, reset)
			if err != nil {
				t.Fatal(err)
			}
			// This is native post-commit currentness, not an in-memory manager
			// replay after the fixture's direct lifecycle admission above.
			_, err = f.store.(effects.TurnLifetimeStore).RequestTurnTimeout(f.ctx, attempt, time.Now().UTC())
			if failure, found := failures.EnvelopeFromError(err); !found || failure.Detail.Code != "instance_restarted" {
				t.Fatalf("historical life1 accepted after reset: %v", err)
			}
		})
		for _, explicitStatus := range []bool{true, false} {
			name := "omitted_status"
			if explicitStatus {
				name = "explicit_status"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f, reset, actor := nativeInstanceResetFixture(t, backend, explicitStatus)
				firstEvent := recordContextOnlyLifeTrace(t, f, reset, actor, 1)
				assertContextOnlyLifeTrace(t, f, reset, nil, firstEvent)
				firstLife := int64(1)
				assertContextOnlyLifeTrace(t, f, reset, &firstLife, firstEvent)
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				committed, err := committer.CommitFlowInstanceActivation(f.ctx, reset)
				if err != nil {
					t.Fatal(err)
				}
				if !committed.StartedOver || len(committed.RestartedAgents) != 1 || len(committed.Lifecycle.Wakeups) != 1 {
					t.Fatalf("reset lost committed finalization evidence: %+v", committed)
				}
				assertContextOnlyLifeTrace(t, f, reset, nil, "")
				assertContextOnlyLifeTrace(t, f, reset, &firstLife, firstEvent)
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
				var preceding effects.Attempt
				t.Run("turn_admission", func(t *testing.T) { preceding = proveNativeTurnAdmissionLife(t, f, reset, actor) })
				currentEvent := recordContextOnlyLifeTrace(t, f, reset, actor, reset.Instance.Life)
				assertContextOnlyLifeTrace(t, f, reset, nil, currentEvent)
				assertContextOnlyLifeTrace(t, f, reset, &reset.Instance.Life, currentEvent)
				finishNativeResetLife(t, f, reset)
				next := prepareNativeResetPlan(t, f, reset)
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, next); err != nil {
					t.Fatal(err)
				}
				_, err = f.store.(effects.TurnLifetimeStore).RequestTurnTimeout(f.ctx, preceding, time.Now().UTC())
				if failure, found := failures.EnvelopeFromError(err); !found || failure.Detail.Code != "instance_restarted" {
					t.Fatalf("captured preceding life accepted after reset: %v", err)
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
}

func proveNativeTurnAdmissionLife(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan, actor agentidentity.Identity, historical ...bool) effects.Attempt {
	t.Helper()
	store := f.store.(providerDirectiveTestStore)
	state, found, err := store.LoadAgentLifecycleState(f.ctx, actor)
	if err != nil || !found {
		t.Fatalf("read admitted actor: found=%t err=%v", found, err)
	}
	started, err := f.grant.CommitAgentLifecycleTransition(f.ctx, manager.AgentLifecycleTransition{
		DiagnosticOrigin: runfork.LifecycleDiagnosticOrigin{Owner: runfork.LifecycleDiagnosticNormal, Causality: runfork.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "start", RequestHash: "turn-admission-start", Identity: actor, AgentID: actor.AgentID(), Trigger: "start",
		ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
		TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation + 1, TargetPhase: manager.AgentLifecycleRunning,
		ConfigRevision: state.ConfigRevision, RunMode: manager.AgentRunModeStandard, Topology: state.Topology, Now: time.Now().UTC(),
	})
	if err != nil || started.Identity != actor || started.Phase != manager.AgentLifecycleRunning {
		t.Fatalf("start exact declared actor: %+v %v", started, err)
	}
	state, found, err = store.LoadAgentLifecycleState(f.ctx, actor)
	if err != nil || !found || state.Phase != manager.AgentLifecycleRunning {
		t.Fatalf("read running actor: found=%t err=%v", found, err)
	}
	token := effects.LifecycleToken{Identity: actor, AgentID: actor.AgentID(), RuntimeEpoch: state.RuntimeEpoch, Generation: state.Generation}
	ctx := effects.WithLifecycleToken(f.ctx, token)
	registry := f.store.(sessions.Registry)
	lease, err := registry.Acquire(ctx, actor, "turn-admission-proof")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if released, err := registry.ReleaseOutcome(ctx, lease); err != nil || !released.Acknowledged {
			t.Errorf("release exact admission session: %+v %v", released, err)
		}
	}()
	authority := effects.NormalAgentAuthority(token, lease.LockOwner, lease.ExpiresAt)
	agents, err := f.store.LoadAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, admitted := range agents {
		if admitted.Config.Identity == actor {
			authority.ExecutionMode = admitted.Config.ExecutionMode
		}
	}
	authority.Target = effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(),
		RunID: actor.RunID, AgentID: actor.AgentID(), AgentIdentity: actor, FlowInstance: actor.FlowInstance(),
		EntityID: plan.Identity.EntityID, SessionID: lease.SessionID, Memory: agentmemory.Plan{Enabled: true}}
	_, sqlite := f.store.(*SQLiteRuntimeStore)
	fixture := completionSettlementFixture{store: store, authority: authority, original: ctx, db: f.db, sqlite: sqlite}
	op, event := reserveProviderDirectiveOperation(t, fixture, store, "instance-life-admission")
	foreignSource, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("f", 64))
	if err != nil {
		t.Fatal(err)
	}
	req := agentcontrol.DirectiveExecutionAdmissionRequest{OperationID: op.OperationID, OwnerID: uuid.NewString(),
		Now: time.Now().UTC(), Lease: time.Minute, ExecutionPosture: executionposture.Live, Source: semanticview.Wrap(f.bundle)}
	for _, invalid := range []struct {
		name string
		ctx  context.Context
		req  agentcontrol.DirectiveExecutionAdmissionRequest
	}{
		{name: "missing_source", ctx: ctx, req: func() agentcontrol.DirectiveExecutionAdmissionRequest { value := req; value.Source = nil; return value }()},
		{name: "foreign_source", ctx: correlation.WithSourceArtifactFact(ctx, foreignSource), req: req},
		{name: "stale_actor", ctx: effects.WithLifecycleToken(ctx, effects.LifecycleToken{Identity: actor, AgentID: actor.AgentID(), RuntimeEpoch: token.RuntimeEpoch, Generation: token.Generation + 1}), req: req},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			admitted, err := store.AdmitDirectiveExecution(invalid.ctx, invalid.req)
			if err == nil || admitted.Acknowledged {
				t.Fatal("invalid admission acquired ownership")
			}
			current, found, err := store.LoadDirectiveOperation(ctx, op.OperationID)
			if err != nil || !found || current.State != agentcontrol.DirectiveOperationPrepared {
				t.Fatalf("refused admission changed operation: %+v %v", current, err)
			}
		})
	}
	admitted, err := store.AdmitDirectiveExecution(ctx, req)
	want := events.InstanceOwnership{Life: &events.InstanceLife{RunID: actor.RunID, FlowInstance: plan.Identity.InstancePath, Life: plan.Instance.Life}}
	if err != nil || !admitted.Acknowledged || !admitted.Ownership.Equal(want) {
		t.Fatalf("life-2 directive admission: %+v err=%v", admitted, err)
	}
	origin, err := agentcontrol.NewDirectiveExecutionOrigin(admitted.Operation)
	if err != nil {
		t.Fatal(err)
	}
	providerCtx := effects.WithController(effects.WithAuthority(ctx, authority), newCompletionControllerForTest(store))
	providerCtx = effects.WithSessionGrant(providerCtx, effects.SessionGrant{SessionID: lease.SessionID, GrantID: lease.GrantID, LockOwner: lease.LockOwner})
	providerCtx = effects.WithTurnInstanceSource(providerCtx, semanticview.Wrap(f.bundle))
	providerCtx = withManagedCompletionTestSurface(t, providerCtx, authority, "anthropic_api")
	deliveryCtx := providerCtx
	providerCtx = effects.WithDirectiveCompletionOrigin(providerCtx, origin)
	providerCtx = correlation.WithInboundEvent(providerCtx, event)
	for _, invalid := range []events.InstanceOwnership{{}, {RunOwned: true}, {Life: &events.InstanceLife{RunID: actor.RunID, FlowInstance: plan.Identity.InstancePath, Life: 3 - plan.Instance.Life}}} {
		_, err := beginManagedCompletionForTest(t, effects.WithTurnInstanceOwnership(providerCtx, invalid), "anthropic_api", []byte("invalid-life"))
		if err == nil {
			t.Fatal("provider admitted missing, run-owned or stale instance evidence")
		}
		if invalid.Life != nil {
			if failure, found := failures.EnvelopeFromError(err); !found || failure.Detail.Code != "instance_restarted" {
				t.Fatalf("stale admitted life lost typed refusal: %v", err)
			}
		}
	}
	var count int
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_directive_operation_id=$1`, op.OperationID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refused provider admission wrote attempts: count=%d err=%v", count, err)
	}
	providerCtx = effects.WithTurnInstanceOwnership(providerCtx, admitted.Ownership)
	providerCtx = effects.WithTurnTimeout(providerCtx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "review/timer.elapsed"})
	handle, err := beginManagedCompletionForTest(t, providerCtx, "anthropic_api", []byte("life-2-admission"))
	if err != nil || handle == nil || !handle.Attempt().InstanceOwnership.Equal(want) {
		t.Fatalf("life-2 provider admission: %v", err)
	}
	observed := handle.Attempt()
	observed.InstanceOwnership.Life.Life++
	if !handle.Attempt().InstanceOwnership.Equal(want) {
		t.Fatal("attempt inspection changed the admitted ownership statement")
	}
	var raw []byte
	if err := f.db.QueryRowContext(ctx, `SELECT authority_evidence FROM runtime_external_effect_attempts WHERE attempt_id=$1`, handle.Attempt().AttemptID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Ownership events.InstanceOwnership `json:"instance_ownership"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil || !evidence.Ownership.Equal(want) {
		t.Fatalf("attempt lost exact immutable life: %+v %v", evidence, err)
	}
	failure := failures.FromError(context.Canceled, "turn-admission-proof", "unlaunched_cleanup").Failure
	for _, changed := range []events.InstanceOwnership{{}, {RunOwned: true}, {Life: &events.InstanceLife{RunID: actor.RunID, FlowInstance: plan.Identity.InstancePath, Life: 1}}} {
		err := store.SettleExternalAttempt(providerCtx, effects.Settlement{
			OperationID: handle.Attempt().OperationID, AttemptID: handle.Attempt().AttemptID, Authority: authority,
			InstanceOwnership: changed, State: effects.StateTerminalFailure, Failure: &failure,
			Evidence: map[string]any{"launch_rejected": true}, Now: time.Now().UTC(),
		})
		if err == nil || !strings.Contains(err.Error(), "external effect result differs from its original attempt authority") {
			t.Fatalf("changed result ownership did not reach exact original-attempt refusal: %v", err)
		}
		var state effects.State
		if err := f.db.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=$1`, handle.Attempt().AttemptID).Scan(&state); err != nil || state != effects.StateAuthorized {
			t.Fatalf("refused result changed attempt: state=%s err=%v", state, err)
		}
	}
	if err := handle.MarkLaunched(providerCtx); err != nil {
		t.Fatal(err)
	}
	clock, found := handle.LogicalTurnClock()
	if !found {
		t.Fatal("life-2 directive omitted its logical clock")
	}
	intent, err := store.(effects.TurnLifetimeStore).RequestTurnTimeout(providerCtx, handle.Attempt(), clock.DeadlineAt)
	if err != nil || intent.ValidateIntent() != nil {
		t.Fatalf("life-2 timeout admission: %+v %v", intent, err)
	}
	if err := handle.Settle(providerCtx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
		t.Fatal(err)
	}
	if len(historical) != 0 && historical[0] {
		if plan.Instance.Life != 1 {
			t.Fatal("historical fixture must model the no-counter life1 population")
		}
		var old map[string]json.RawMessage
		if err := json.Unmarshal(raw, &old); err != nil {
			t.Fatal(err)
		}
		delete(old, "instance_ownership")
		raw, err = json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.ExecContext(providerCtx, `UPDATE runtime_external_effect_attempts SET authority_evidence=$1 WHERE attempt_id=$2`, string(raw), handle.Attempt().AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	beforeRecovery := snapshotForkHistoricalExecutionTables(t, f.db, !sqlite)
	turns, err := store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(providerCtx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
	if err != nil || len(turns) != 1 || !turns[0].Attempt.InstanceOwnership.Equal(want) {
		t.Fatalf("life-2 recovery lost captured statement: %+v %v", turns, err)
	}
	if !reflect.DeepEqual(beforeRecovery, snapshotForkHistoricalExecutionTables(t, f.db, !sqlite)) {
		t.Fatal("recovery inventory rewrote evidence or emitted a revision")
	}
	if !sqlite {
		t.Run("recovery_interleave", func(t *testing.T) {
			proveRecoveryAcquiresActorBeforeSourceHead(t, providerCtx, fixture, want)
		})
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	scope, runtimeFound := authoractivity.ScopeFromContext(f.ctx)
	if !present || !runtimeFound {
		t.Fatal("native timeout fixture lost its source/runtime owners")
	}
	catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{{EventType: "review/timer.elapsed", Disposition: authoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Release()
	publication, err := newStoreTestEventBus(t, f.store.(storeTestDurableEventBusStore), bus.EventBusOptions{
		ContractBundle: semanticview.Wrap(f.bundle), SourceArtifactFact: fact, RuntimeInstanceID: scope.RuntimeInstanceID,
		Interceptors: []bus.EventInterceptor{f.workflows},
		MutationContext: func(ctx context.Context) (context.Context, error) {
			return startupownership.CarryMutationAuthority(ctx, f.ctx), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reaction, err := publication.PrepareTurnTimeoutReaction(providerCtx, turns[0])
	if err != nil {
		t.Fatal(err)
	}
	projected := reaction.(bus.EnginePublicationPlan).PublicationCommand().Commit.Event.Event().RoutingSource()
	if projected.Life() != want.Life.Life {
		t.Fatalf("timeout reaction restamped captured life: got=%d want=%d", projected.Life(), want.Life.Life)
	}
	committed, err := store.(effects.CanceledTurnStore).CommitCanceledTurn(providerCtx, effects.CanceledTurnCommandForAttempt(turns[0].Attempt, reaction))
	if err != nil || committed.Validate() != nil {
		t.Fatalf("commit life-2 recovered timeout: %+v %v", committed, err)
	}
	if err := publication.DispatchTurnTimeoutReaction(providerCtx, committed.Publication); err != nil {
		t.Fatal(err)
	}
	t.Run("delivery_snapshot", func(t *testing.T) {
		input := eventtest.RuntimeControlWithRoutingSource(uuid.NewString(), "review/admission.requested", "fixture", "", []byte(`{}`), 0,
			actor.RunID, "", events.EventEnvelope{EntityID: plan.Identity.EntityID}, events.NewPlatformControlRoutingSource(), time.Now().UTC())
		route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor,
			Context: events.DeliveryContext{Life: want.Life}}
		if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, input, []events.DeliveryRoute{route}); err != nil {
			t.Fatal(err)
		}
		claimed, err := claimDeliveryFixture(ctx, f.store.(deliveryFixtureStore), input, route)
		if err != nil {
			t.Fatal(err)
		}
		// A caller-supplied absence must not replace the acquired snapshot's life.
		ctx := effects.WithTurnInstanceOwnership(deliverylifecycle.WithClaim(deliveryCtx, claimed.Claim), events.InstanceOwnership{RunOwned: true})
		ctx = correlation.WithInboundEvent(ctx, input)
		delivered, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("life-2-delivery-admission"))
		if err != nil || delivered == nil || !delivered.Attempt().InstanceOwnership.Equal(want) {
			t.Fatalf("delivery did not capture exact current snapshot life: %v", err)
		}
		if err := delivered.Settle(ctx, effects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.(deliveryFixtureStore).SettleSuccess(ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
			t.Fatal(err)
		}
	})
	return handle.Attempt()
}

func proveRecoveryAcquiresActorBeforeSourceHead(t *testing.T, parent context.Context, f completionSettlementFixture, want events.InstanceOwnership) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	blocker, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done, joined := make(chan error, 1), make(chan struct{})
	started := false
	defer func() {
		cancel()
		_ = blocker.Rollback()
		if started {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("recovery interleave did not join")
			}
		}
	}()
	fingerprint, err := f.authority.Target.AgentIdentity.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "swarm:agent-lifecycle:"+fingerprint); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	started = true
	go func() {
		defer close(joined)
		turns, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
		if err == nil && (len(turns) != 1 || !turns[0].Attempt.InstanceOwnership.Equal(want)) {
			err = fmt.Errorf("interleaved recovery lost exact originating life")
		}
		done <- err
	}()
	for {
		var blocked bool
		if err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("recovery bypassed actor lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// A recovery waiting for the actor must not already hold the source head.
	var revision string
	if err := blocker.QueryRowContext(ctx, `SELECT revision FROM agent_topology_source_set_head WHERE singleton_id=1 FOR UPDATE NOWAIT`).Scan(&revision); err != nil {
		t.Fatalf("recovery inverted actor/source lock order: %v", err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func recordContextOnlyLifeTrace(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan, actor agentidentity.Identity, life int64) string {
	t.Helper()
	event := eventtest.RuntimeControlWithRoutingSource(uuid.NewString(), "review/task.started", "fixture", "", []byte(`{}`), 0,
		plan.Readiness.RunID, "", events.EventEnvelope{EntityID: plan.Identity.EntityID}, events.NewPlatformControlRoutingSource(), time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor,
		Context: events.DeliveryContext{Life: &events.InstanceLife{RunID: event.RunID(), FlowInstance: plan.Identity.InstancePath, Life: life}}}
	if !route.Target.Empty() || event.RoutingSource().Life() != 0 {
		t.Fatal("trace control must carry receiver life only in context")
	}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, f.store.(deliveryFixtureStore), event, route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.(deliveryFixtureStore).SettleSuccess(f.ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
		t.Fatal(err)
	}
	return event.ID()
}

func assertContextOnlyLifeTrace(t *testing.T, f receiverConfigActivationFixture, plan pipeline.FlowInstanceActivationPlan, life *int64, want string) {
	t.Helper()
	reader := f.store.(interface {
		LoadRunDebugTracePage(context.Context, string, operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error)
	})
	rows, next, err := reader.LoadRunDebugTracePage(f.ctx, plan.Readiness.RunID, operatorread.RunDebugTraceQueryOptions{Limit: 10,
		Filter: operatorread.RunDebugTraceFilter{Iter: life, EntityIDs: []string{plan.Identity.EntityID}, EventNames: []string{"review/task.started"}}})
	if err != nil || next != "" || want == "" && len(rows) != 0 || want != "" && (len(rows) != 1 || rows[0].EventID != want) {
		t.Fatalf("context-only receiver trace selected life=%v rows=%+v next=%q want=%q err=%v", life, rows, next, want, err)
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
	fixture := completionSettlementFixture{store: pg, authority: effects.Authority{Normal: effects.LifecycleToken{Identity: actor}, Target: effects.UsageTarget{RunID: actor.RunID, AgentIdentity: actor}}, original: ctx}
	op, _ := reserveProviderDirectiveOperation(t, fixture, pg, "reset-admission-interleave")
	locked, proceed := make(chan int, 1), make(chan struct{})
	done := make(chan error, 1)
	finished, resetFinished := make(chan struct{}), make(chan struct{})
	admissionDone, admissionFinished := make(chan error, 1), make(chan struct{})
	var release sync.Once
	resetStarted, admissionStarted := false, false
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
		if admissionStarted {
			select {
			case <-admissionFinished:
			case <-time.After(5 * time.Second):
				t.Error("admission interleave did not join")
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
					DiagnosticOrigin: runfork.LifecycleDiagnosticOrigin{Owner: runfork.LifecycleDiagnosticNormal, Causality: runfork.LifecycleDiagnosticObservation},
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
	admissionStarted = true
	go func() {
		defer close(admissionFinished)
		token := effects.LifecycleToken{Identity: actor, AgentID: actor.AgentID(), RuntimeEpoch: state.RuntimeEpoch, Generation: state.Generation}
		admitted, err := pg.AdmitDirectiveExecution(effects.WithLifecycleToken(ctx, token), agentcontrol.DirectiveExecutionAdmissionRequest{
			OperationID: op.OperationID, OwnerID: uuid.NewString(), Now: time.Now().UTC(), Lease: time.Minute,
			ExecutionPosture: executionposture.Live, Source: semanticview.Wrap(f.bundle),
		})
		if admitted.Acknowledged || err == nil || err.Error() != "turn admission actor is no longer current" {
			admissionDone <- fmt.Errorf("superseded admission after lock prefix: acknowledged=%t err=%v", admitted.Acknowledged, err)
			return
		}
		admissionDone <- nil
	}()
	blocked := false
	for !blocked {
		if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) >= 2 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, pid).Scan(&blocked); err != nil {
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
	if err := <-admissionDone; err != nil {
		t.Error(err)
	}
	assertNativeResetSessions(t, f, reset)
}

func nativeInstanceResetFixture(t *testing.T, backend string, explicitStatus ...bool) (receiverConfigActivationFixture, pipeline.FlowInstanceActivationPlan, agentidentity.Identity) {
	t.Helper()
	f, first := newWorkflowTargetConstructionFixture(t, backend, true)
	if len(explicitStatus) == 0 || explicitStatus[0] {
		first.Instance.Status = "active"
	}
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
	if schema, found := semanticview.Wrap(f.bundle).FlowSchemaByID(previous.Identity.TemplateID); found && schema.Instance.Empty() {
		req.ConstructorInput, req.ResolvedKey, req.TriggerEvent = "", "", events.Event{}
	}
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
