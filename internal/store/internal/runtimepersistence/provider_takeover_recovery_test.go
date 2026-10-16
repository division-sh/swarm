package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestProviderTakeoverRecoveryNativeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"commit_refused", "commit_ack_lost", "healthy"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				fixture, control := openCompletionOutcomeFixture(t, backend)
				attempts := makeProviderTakeoverAttempts(t, fixture)
				grant, request := prepareProviderTakeover(t, fixture)
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

func prepareProviderTakeover(t *testing.T, fixture completionSettlementFixture) (startupownership.LiveGenerationGrant, manager.AgentLifecycleTransition) {
	t.Helper()
	ctx := testAuthorActivityContext()
	state := providerDrainFixtureState(t, fixture)
	predecessor, err := agentfixture.ProcessCapability(fixture.agentOwner, ctx, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := predecessor.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("load original source plan: found=%t error=%v", found, err)
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
	}
}
