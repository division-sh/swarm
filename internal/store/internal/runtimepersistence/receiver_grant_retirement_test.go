package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/tools"
	deliveryowner "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Only scheduling is replaced: the real owner must authorize and lock the
// receiver before this barrier leaves the outer claim transaction open.
type receiverGrantClaimBarrier struct {
	owner   deliveryowner.ReceiverExecutionAdmission
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *receiverGrantClaimBarrier) ReceiverExecutionReadyTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, claim bool) (bool, error) {
	ready, err := b.owner.ReceiverExecutionReadyTx(ctx, tx, route, authority, claim)
	if err != nil || !ready || !claim {
		return ready, err
	}
	b.once.Do(func() { close(b.ready) })
	select {
	case <-b.release:
		return ready, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestReceiverOrdinaryGrantRetirementFencesClaimBothStores(t *testing.T) {
	proveReceiverGrantRetirementFencesClaimBothStores(t, false)
}

func TestReceiverSelectedGrantRetirementFencesClaimBothStores(t *testing.T) {
	proveReceiverGrantRetirementFencesClaimBothStores(t, true)
}

func proveReceiverGrantRetirementFencesClaimBothStores(t *testing.T, selectedFork bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, ordering := range []string{"claim_wins", "retirement_wins", "claim_rollback"} {
			t.Run(backend+"/"+ordering, func(t *testing.T) {
				base, selected := newAgentFixtureAuthorityStore(t, backend)
				ctx, cancel := context.WithTimeout(base, 20*time.Second)
				defer cancel()
				runID, entityID := uuid.NewString(), uuid.NewString()
				ctx = correlation.WithRunID(ctx, runID)
				identity := mustTestAgentIdentityForRun(runID, "grant-receiver", "global")
				var grant startupownership.GenerationGrant
				if selectedFork {
					identity, grant = selectedReceiverClaimGrant(t, ctx, selected)
					runID = identity.RunID
					ctx = correlation.WithRunID(ctx, runID)
				} else {
					if err := agentfixture.UpsertStatic(t, ctx, selected, agentFixtureStaticRecord(t, identity)); err != nil {
						t.Fatal(err)
					}
					plan := currentAgentFixtureSourceSet(t, ctx, selected)
					if len(plan.Sources) != 1 {
						t.Fatalf("fixture source census: %+v", plan.Sources)
					}
					var err error
					grant, err = agentfixture.AdmitGeneration(t, ctx, selected, plan, plan.Sources[0])
					if err != nil {
						t.Fatal(err)
					}
					if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
						t.Fatal(err)
					}
					if _, err := grant.AdmitExecution(ctx); err != nil {
						t.Fatal(err)
					}
				}
				evidence, err := grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
				if err != nil || !found || state.ProcessBinding.GenerationGrantID != evidence.GrantID || (evidence.SelectedFork != nil) != selectedFork {
					t.Fatalf("exact lifecycle/grant: state=%+v grant=%+v found=%v err=%v", state, evidence, found, err)
				}
				writer := selected.(interface {
					CreateEntity(context.Context, tools.EntityCreateRecord) error
				})
				if err := writer.CreateEntity(ctx, tools.EntityCreateRecord{
					RunID: runID, EntityID: entityID, FlowInstance: identity.FlowInstance(),
					EntityType: "receiver", CurrentState: "active", FieldsJSON: []byte(`{}`), CreatedAt: time.Now().UTC(),
					Writer: tools.EntityMutationWriter{Type: "agent", ID: identity.AgentID(), HandlerStep: "grant_probe_setup"},
				}); err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{
					Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
					Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "global", FlowInstance: identity.FlowInstance(), EntityID: entityID}),
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if selectedFork {
					event = eventtest.TargetRouted(event, route.Target.Route())
				}
				// This grant-boundary fixture starts after initialization. Preserve
				// the future-target/readiness path under test with explicit supplier
				// evidence; real activation admission has its own both-store journey.
				route.Initialization, err = events.AdmitFlowReceiverInitialization(event, route.Target)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := deliverylifecycle.NewNormalExecutionAuthority(mustStoreTestSourceArtifactFact(evidence.BundleHash), evidence.GrantID, evidence.RuntimeGeneration)
				if selectedFork {
					authority, err = deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(evidence.BundleHash), evidence.SelectedFork.ExecutionID, runID, evidence.SelectedFork.ExecutionGeneration)
				}
				if err != nil {
					t.Fatal(err)
				}
				if selectedFork {
					commitSelectedReceiverClaimEvent(t, ctx, selected, event, route, authority)
				} else if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				if !selectedFork {
					if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
						t.Fatal(err)
					}
				}
				before := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), route)
				if before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 || !before.Authority.Equal(authority) {
					t.Fatalf("initial exact pending obligation: %+v", before)
				}
				if ordering == "retirement_wins" {
					if err := grant.Retire(ctx); err != nil {
						t.Fatal(err)
					}
					result, err := selected.ClaimDelivery(ctx, authority, event, route)
					if err != nil {
						t.Fatal(err)
					}
					if _, acquired := result.Acquired(); acquired {
						t.Fatal("retired grant accepted delivery")
					}
					after := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), route)
					if after.ClaimVersion != before.ClaimVersion || after.Status != before.Status {
						t.Fatalf("retirement-wins changed pending delivery: %+v", after)
					}
					var attempts int
					if err := receiverClaimTestDB(t, selected).QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1`, before.DeliveryID).Scan(&attempts); err != nil || attempts != 0 {
						t.Fatalf("retirement-wins created attempts: %d err=%v", attempts, err)
					}
					return
				}
				barrier := &receiverGrantClaimBarrier{ready: make(chan struct{}), release: make(chan struct{})}
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
				defer release()
				var claimStore deliverylifecycle.Store
				var db *sql.DB
				switch store := selected.(type) {
				case *PostgresStore:
					barrier.owner = store.agentPostgresOwner
					owner, err := deliveryowner.NewDeliveryPostgresOwner(store.deliveryPostgresOwner.DeadLetterPostgresOwner, store.runLifecyclePostgresOwner, barrier)
					if err != nil {
						t.Fatal(err)
					}
					if err := owner.BindReceiverTargetPersistence(store.pipelinePostgresOwner); err != nil {
						t.Fatal(err)
					}
					claimStore, db = owner, store.backend.ConstructionHandle()
				case *SQLiteRuntimeStore:
					barrier.owner = store.agentSQLiteOwner
					owner, err := deliveryowner.NewDeliverySQLiteOwner(store.deliverySQLiteOwner.DeadLetterSQLiteOwner, store.runLifecycleSQLiteOwner, barrier, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := owner.BindReceiverTargetPersistence(store.pipelineSQLiteOwner); err != nil {
						t.Fatal(err)
					}
					claimStore, db = owner, store.backend.ConstructionHandle()
				default:
					t.Fatalf("unsupported fixture %T", selected)
				}
				type claimOutcome struct {
					result deliverylifecycle.ClaimResult
					err    error
				}
				claimed := make(chan claimOutcome, 1)
				claimCtx, cancelClaim := context.WithCancel(ctx)
				defer cancelClaim()
				go func() {
					result, err := claimStore.ClaimDelivery(claimCtx, authority, event, route)
					claimed <- claimOutcome{result, err}
				}()
				select {
				case <-barrier.ready:
				case result := <-claimed:
					t.Fatalf("claim did not reach real authorized receiver barrier: %+v err=%v", result.result, result.err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// A bounded retirement call is deterministic with respect to the barrier:
				// no claim write or commit can occur until this call has returned.
				retireCtx, retireCancel := context.WithTimeout(ctx, time.Second)
				retireErr := grant.Retire(retireCtx)
				retireCancel()
				var durableGrantState string
				if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&durableGrantState); err != nil {
					t.Fatal(err)
				}
				if ordering == "claim_rollback" {
					cancelClaim()
				} else {
					release()
				}
				var outcome claimOutcome
				select {
				case outcome = <-claimed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if ordering == "claim_rollback" {
					if !errors.Is(outcome.err, context.Canceled) {
						t.Fatalf("cancelled held claim: %v", outcome.err)
					}
				} else if outcome.err != nil {
					t.Fatalf("claim: %v", outcome.err)
				}
				after := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), route)
				var attempts int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1`, before.DeliveryID).Scan(&attempts); err != nil {
					t.Fatal(err)
				}
				t.Logf("backend=%s grant=%s run=%s delivery=%s retireWhileClaimPaused=%v durableGrantBeforeRelease=%s claim=%s status=%s claimVersion=%d attempts=%d", backend, evidence.GrantID, runID, before.DeliveryID, retireErr, durableGrantState, outcome.result.Disposition, after.Status, after.ClaimVersion, attempts)
				if ordering == "claim_rollback" {
					if !errors.Is(retireErr, context.DeadlineExceeded) || durableGrantState != string(startupownership.GrantAdmitted) || after.ClaimVersion != 0 || attempts != 0 || after.Status != deliverylifecycle.StatusPending {
						t.Fatalf("cancelled claim or blocked retirement leaked mutation: retire=%v grant=%s delivery=%+v attempts=%d", retireErr, durableGrantState, after, attempts)
					}
					if err := grant.Retire(ctx); err != nil {
						t.Fatalf("retire after claim rollback: %v", err)
					}
					return
				}
				if !errors.Is(retireErr, context.DeadlineExceeded) || durableGrantState != string(startupownership.GrantAdmitted) {
					t.Fatalf("claim-wins did not fence retirement through commit: retire=%v grant=%s claim=%s claimVersion=%d attempts=%d", retireErr, durableGrantState, outcome.result.Disposition, after.ClaimVersion, attempts)
				}
				if _, acquired := outcome.result.Acquired(); !acquired || after.ClaimVersion != 1 || attempts != 1 {
					t.Fatalf("live fenced claim failed: %+v attempts=%d", outcome.result, attempts)
				}
				if err := grant.Retire(ctx); err != nil {
					t.Fatalf("retire after claim commit: %v", err)
				}
			})
		}
	}
}

func receiverClaimTestDB(t *testing.T, selected agentFixtureFlowStore) *sql.DB {
	t.Helper()
	switch store := selected.(type) {
	case *PostgresStore:
		return store.backend.ConstructionHandle()
	case *SQLiteRuntimeStore:
		return store.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported fixture %T", selected)
		return nil
	}
}

func commitSelectedReceiverClaimEvent(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, release, err := semanticEventFixtureContext(ctx, selected, event)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	owner := pipelineObligationOwnerForFixture(selected)
	claim, err := owner.ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Release(context.WithoutCancel(ctx), claim); err != nil {
			t.Error(err)
		}
	}()
	scope, hasScope := authoractivity.ScopeFromContext(ctx)
	descriptor, hasDescriptor, err := authoractivity.ResolvedEventDescriptorFromContext(ctx, scope, string(event.Type()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = selected.(interface {
		CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
	}).CommitPublication(ctx, bus.PublicationCommand{
		Commit:      bus.CommitPublishRequest{Event: admitted, RouteSettlement: testRouteSettlement(event, []events.DeliveryRoute{route}), DeliveryRoutes: []events.DeliveryRoute{route}, DeliveryAuthority: authority, ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: claim},
		AuthorScope: scope, HasAuthorScope: hasScope, AuthorDescriptor: descriptor, HasAuthorDescriptor: hasDescriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func selectedReceiverClaimGrant(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) (agentidentity.Identity, startupownership.GenerationGrant) {
	t.Helper()
	_, sqlite := selected.(*SQLiteRuntimeStore)
	fixture := newSelectedCompletionFixture(t, selected.(selectedCompletionAuthorityStore), receiverClaimTestDB(t, selected), sqlite)
	ctx = correlation.WithRunID(ctx, fixture.forkRun)
	identity := mustTestAgentIdentityForRun(fixture.forkRun, "grant-receiver", "global")
	record := agentFixtureStaticRecord(t, identity)
	plan, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := manager.AgentConfigPlanRevision(record.Config, plan)
	if err != nil {
		t.Fatal(err)
	}
	hash := fixture.request.DeclarationPlan.BundleHash
	fixture.request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(hash, []agenttopology.DesiredAgent{{
		Identity: plan, ConfigRevision: revision, Source: agenttopology.SourceCoordinate{BundleHash: hash},
	}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Preparation.DeclarationPlanFingerprint = fixture.request.DeclarationPlan.Revision
	fixture.request.Preparation.Actors = []runfork.SelectedForkPreparedActor{{
		Plan: plan, ConfigurationRevision: revision, Backend: selection.BackendAnthropic, Mode: executionmode.Live,
	}}
	grant := selectedMutationFenceGrant(t, ctx, fixture)
	topology, err := agenttopology.SelectedDeclarationAdmission(fixture.forkRun, fixture.request.DeclarationPlan)
	if err != nil {
		t.Fatal(err)
	}
	record.Topology = topology
	now := time.Now().UTC()
	registered, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(),
		Identity: identity, AgentID: identity.AgentID(), Trigger: "selected-claim-fence-test",
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: manager.AgentLifecycleRegistered,
		ConfigRevision: revision, RunMode: manager.AgentRunModeStopped, Agent: &record, Topology: topology, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: uuid.NewString(),
		Identity: identity, AgentID: identity.AgentID(), Trigger: "selected-claim-fence-test",
		ExpectedEpoch: registered.RuntimeEpoch, ExpectedGeneration: registered.Generation, ExpectedPhase: registered.Phase,
		TargetEpoch: registered.RuntimeEpoch, TargetGeneration: registered.Generation + 1, TargetPhase: manager.AgentLifecycleRunning,
		ConfigRevision: revision, RunMode: manager.AgentRunModeAuthoritativeDeliveryOnly, Topology: topology, Now: now.Add(time.Nanosecond),
	}); err != nil {
		t.Fatal(err)
	}
	return identity, grant
}
