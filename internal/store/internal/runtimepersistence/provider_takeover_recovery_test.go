package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestProviderTakeoverRecoveryNativeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"commit_refused", "commit_ack_lost", "healthy"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				fixture, control := openCompletionOutcomeFixture(t, backend)
				attempts := makeProviderTakeoverAttempts(t, fixture)
				grant, request, successor := prepareProviderTakeover(t, fixture, nil)
				var submitted atomic.Int32
				registration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(
					testAuthorActivityContext(), runlifecycle.CandidateScope{BundleHash: request.Topology.Authority.Static.BundleHash},
					&completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
						submitted.Add(1)
						return nil
					}},
				)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				control.phase, control.failure = phase, errors.New("restart recovery native commit diagnostic")
				control.statement = func(query string) bool {
					if !completionOutcomeAgentTurnInsert(query) {
						return false
					}
					control.enabled.Store(true)
					return true
				}
				result, commitErr := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request)
				control.enabled.Store(false)
				if phase != "healthy" && !errors.Is(commitErr, control.failure) {
					t.Fatalf("native boundary error was not preserved: %v", commitErr)
				}
				if phase == "healthy" && (commitErr != nil || result.Generation != request.TargetGeneration) {
					t.Fatalf("takeover lost acknowledged result: result=%+v error=%v", result, commitErr)
				}
				if control.writes.Load() != 2 {
					t.Fatalf("native recovery did not reach both turn writes: %d", control.writes.Load())
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "commit_refused" {
					if !reflect.DeepEqual(before, after) || submitted.Load() != 0 {
						t.Fatalf("refused native commit retained recovery rows or completion: submitted=%d", submitted.Load())
					}
					t.Log("before-commit cut: all native rows conserved; no completion submitted")
					return
				}
				wantSubmitted := int32(0)
				if phase == "healthy" {
					wantSubmitted = 1
				}
				if submitted.Load() != wantSubmitted {
					t.Fatalf("completion publication crossed uncertain acknowledgement: got=%d want=%d", submitted.Load(), wantSubmitted)
				}
				for i, attempt := range attempts {
					wantTurns, wantState := 1, "outcome_uncertain"
					if i == 0 {
						wantTurns, wantState = 0, "terminal_failure"
					}
					requireCompletionRecoveryRows(t, fixture, attempt, wantTurns, wantTurns, 0)
					rows, err := DecodeSelectedForkSnapshotRowsForTest(after["runtime_external_effect_attempts"])
					if err != nil {
						t.Fatal(err)
					}
					found := false
					for _, row := range rows {
						id, _ := row["attempt_id"].Text()
						state, _ := row["state"].Text()
						if id == attempt {
							found = true
							if state != wantState {
								t.Fatalf("recovered attempt state=%q want=%q", state, wantState)
							}
						}
					}
					if !found {
						t.Fatal("recovered attempt disappeared")
					}
				}
				if phase == "commit_ack_lost" {
					// An uncertain retained-session exit requires a new process grant.
					grant, request, _ = prepareProviderTakeover(t, fixture, successor)
					if _, err := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request); err != nil {
						t.Fatalf("restart after committed acknowledgement loss: %v", err)
					}
					reopened, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
					if err != nil {
						t.Fatal(err)
					}
					for _, table := range []string{"runtime_external_effect_attempts", "runtime_external_effect_operations", "runtime_provider_attempt_drains", "agent_turns", "spend_ledger", "runtime_effect_budget_reservations", "event_deliveries", "event_delivery_attempts"} {
						if !reflect.DeepEqual(after[table], reopened[table]) {
							t.Fatalf("restart after acknowledgement loss changed settled %s rows", table)
						}
					}
					after = reopened
				}
				replay, err := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request)
				if err != nil || !replay.Replayed || replay.Generation != request.TargetGeneration {
					t.Fatalf("exact takeover replay lost its committed outcome: result=%+v error=%v", replay, err)
				}
				repeated, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
				if err != nil || !reflect.DeepEqual(after, repeated) || submitted.Load() != wantSubmitted {
					t.Fatalf("exact replay duplicated recovery or completion: submitted=%d error=%v", submitted.Load(), err)
				}
				t.Log("native commit cut: all predecessor dispositions committed together; exact replay conserved every native row")
			})
		}
	}
}

func TestProviderTakeoverRecoveryBundleSiblingIsolationBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		fact := sourceartifactfixture.RequireArtifact(t, testAuthorActivityContext(), fixture.store.(sourceartifactfixture.Writer), sourceartifactfixture.New("sibling/schema.yaml", []byte("name: sibling\n")))
		bundle := fact.BundleHash()
		ctx := testAuthorActivityContextForBundle(bundle)
		registerTestAuthorActivityCatalogForContext(t, fixture.store.(testAuthorActivityCatalogRegistrar), ctx)
		identity := mustTestAgentIdentityForRun(uuid.NewString(), fixture.agentID, "sibling")
		actor := withRuntimePersistenceTestIntent(t, actors.AgentConfig{
			Identity: identity, ID: fixture.agentID, Role: "worker", Type: "managed", ExecutionMode: "live",
			Model: "regular", LLMBackend: "claude_cli", ResolvedLLMBackend: "claude_cli", FlowPath: identity.FlowInstance(),
		})
		if err := agentfixture.UpsertStaticForSource(t, ctx, fixture.store, manager.PersistedAgent{
			Config: actor, Status: "active", StartedAt: time.Now().UTC(),
		}, fact); err != nil {
			t.Fatal(err)
		}
		processOwner, err := agentfixture.ProcessCapability(t, testAuthorActivityContext(), fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		plan, exists, err := processOwner.CurrentSourceSet(testAuthorActivityContext())
		if err != nil || !exists {
			t.Fatalf("read combined source plan: found=%t error=%v", exists, err)
		}
		originalGrant, err := agentfixture.AdmitGeneration(t, testAuthorActivityContext(), fixture.store, plan, agenttopology.SourceCoordinate{BundleHash: authorActivityTestBundleHash})
		if err != nil {
			t.Fatal(err)
		}
		original := providerDrainFixtureState(t, fixture)
		topology, err := agenttopology.StaticAdmission(plan.Revision, authorActivityTestBundleHash, original.Topology.Lifetime)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := originalGrant.CommitAgentLifecycleTransition(testAuthorActivityContext(), manager.AgentLifecycleTransition{
			DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
			OperationID:      uuid.NewString(), OperationKind: "source_set_rebind", RequestHash: uuid.NewString(),
			Identity: original.Identity, AgentID: original.AgentID, Trigger: "source_set_rebind",
			ExpectedEpoch: original.RuntimeEpoch, ExpectedGeneration: original.Generation, ExpectedPhase: original.Phase,
			TargetEpoch: original.RuntimeEpoch, TargetGeneration: original.Generation, TargetPhase: original.Phase,
			ConfigRevision: original.ConfigRevision, RunMode: original.RunMode, Topology: topology, Now: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		state, found, err := fixture.store.LoadAgentLifecycleState(ctx, identity)
		if err != nil || !found {
			t.Fatalf("read sibling lifecycle: found=%t error=%v", found, err)
		}
		sibling := fixture
		sibling.authority = effects.NormalAgentAuthority(effects.LifecycleToken{
			Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: state.RuntimeEpoch, Generation: state.Generation,
		}, fixture.leaseHolder, time.Now().Add(time.Minute))
		sibling.authority.ExecutionMode = effects.ExecutionModeLive
		sibling.authority.Target = fixture.authority.Target
		sibling.authority.Target.AgentIdentity, sibling.authority.Target.FlowInstance = identity, identity.FlowInstance()
		sibling.authority.Target.RunID = identity.RunID
		sibling.authority.Target.ID, sibling.authority.Target.SessionID = uuid.NewString(), uuid.NewString()
		sibling.authority.Target.Memory = agentmemory.Plan{}
		sibling.grantID = ""
		sibling.origin = claimCompletionOriginForTest(t, ctx, fixture.store, sibling.authority, time.Now())
		siblingCtx := providerDrainContext(t, sibling, "other-bundle-sibling")
		siblingCtx = correlation.WithSourceArtifactFact(siblingCtx, mustStoreTestSourceArtifactFact(bundle))
		siblingCtx = authoractivity.WithScope(siblingCtx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, bundle))
		siblingCtx = withManagedCompletionTestSurface(t, siblingCtx, sibling.authority, "anthropic_api")
		siblingAttempt := beginObservedCompletionForSettlementTest(t, siblingCtx, "anthropic_api", "other-bundle-sibling").Attempt()
		attempts := makeProviderTakeoverAttempts(t, fixture)
		grant, request, process := prepareProviderTakeover(t, fixture, nil)
		before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request); err != nil {
			t.Fatalf("recover exact original cell: %v", err)
		}
		after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range []struct{ table, column, id string }{
			{"runtime_external_effect_attempts", "attempt_id", siblingAttempt.AttemptID},
			{"runtime_external_effect_operations", "operation_id", siblingAttempt.OperationID},
			{"event_deliveries", "delivery_id", sibling.origin.DeliveryID()},
		} {
			selectRows := func(snapshot map[string]SelectedForkStorageTableSnapshot) []map[string]SelectedForkSnapshotCell {
				rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot[target.table])
				if err != nil {
					t.Fatal(err)
				}
				var exact []map[string]SelectedForkSnapshotCell
				for _, row := range rows {
					if value, _ := row[target.column].Text(); value == target.id {
						exact = append(exact, row)
					}
				}
				return exact
			}
			left, right := selectRows(before), selectRows(after)
			if len(left) != 1 || !reflect.DeepEqual(left, right) {
				t.Fatalf("original cell recovery changed foreign-bundle sibling %s", target.table)
			}
		}
		for _, attempt := range attempts[1:] {
			requireCompletionRecoveryRows(t, fixture, attempt, 1, 1, 0)
		}
		grant, request, _ = prepareProviderTakeover(t, sibling, process)
		if _, err := grant.CommitAgentLifecycleTransition(ctx, request); err != nil {
			t.Fatalf("recover sibling under its own bundle grant: %v", err)
		}
		final, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		stories, err := DecodeSelectedForkSnapshotRowsForTest(final["author_activity_occurrences"])
		if err != nil {
			t.Fatal(err)
		}
		foundTurn := false
		for _, story := range stories {
			if id, _ := story["source_identity"].Text(); id == siblingAttempt.Authority.Target.ID+":failed" {
				gotBundle, _ := story["bundle_hash"].Text()
				if gotBundle != bundle {
					t.Fatal("sibling recovery borrowed another bundle's story scope")
				}
				foundTurn = true
			}
		}
		if !foundTurn {
			t.Fatal("sibling recovery omitted its own scoped turn story")
		}
	})
}

func makeProviderTakeoverAttempts(t *testing.T, fixture completionSettlementFixture) []string {
	t.Helper()
	var attempts []string
	for i := 0; i < 3; i++ {
		authority := fixture.authority
		authority.Target.ID = uuid.NewString()
		authority.Target.EntityID = uuid.NewString()
		authority.BudgetScopes = []effects.BudgetAdmissionScope{{Kind: "entity", Key: authority.Target.EntityID, CapUSD: 1}}
		origin := fixture.origin
		if i != 0 {
			origin = newProviderDrainOriginClaim(t, fixture, i)
		}
		ctx := providerDrainAttemptContext(t, fixture, authority, origin, "anthropic_api", "restart-native-"+authority.Target.ID)
		handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("restart-native"))
		if err != nil {
			t.Fatal(err)
		}
		if i != 0 {
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
		}
		attempts = append(attempts, handle.Attempt().AttemptID)
	}
	return attempts
}

func prepareProviderTakeover(t *testing.T, fixture completionSettlementFixture, predecessor startupownership.ProcessCapability) (startupownership.LiveGenerationGrant, manager.AgentLifecycleTransition, startupownership.ProcessCapability) {
	t.Helper()
	ctx := testAuthorActivityContext()
	state := providerDrainFixtureState(t, fixture)
	var err error
	if predecessor == nil {
		predecessor, err = agentfixture.ProcessCapability(fixture.agentOwner, ctx, fixture.store)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := predecessor.Release(ctx); err != nil {
		t.Fatal(err)
	}
	acquire := startupownership.AcquireRequest{OwnerID: "provider-restart-recovery", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
	successor, err := fixture.store.AcquireProcessCapability(ctx, acquire)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = successor.Release(context.Background()) })
	plan, found, err := successor.CurrentSourceSet(ctx)
	if err != nil || !found || state.Topology.Authority.Static == nil || plan.Revision != state.Topology.Authority.Static.SourceSetRevision {
		t.Fatalf("load original source plan on reopen: found=%t error=%v", found, err)
	}
	grant, err := successor.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: state.ProcessBinding.BundleHash, RuntimeInstanceID: acquire.RuntimeInstanceID,
		RuntimeGeneration: state.ProcessBinding.RuntimeGeneration + 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	return grant, manager.AgentLifecycleTransition{
		DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "process_takeover", RequestHash: uuid.NewString(),
		Identity: state.Identity, AgentID: state.AgentID, Trigger: "process_takeover",
		ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
		TargetEpoch: state.RuntimeEpoch + 1, TargetGeneration: state.Generation + 1, TargetPhase: state.Phase,
		ConfigRevision: state.ConfigRevision, RunMode: state.RunMode, Topology: state.Topology, Now: time.Now().UTC(),
	}, successor
}
