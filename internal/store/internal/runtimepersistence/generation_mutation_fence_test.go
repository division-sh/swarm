package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// The caller supplies the real takeover/repair operation. No grant transition or
// authorization result is replaced; only the outer mutation lifetime is held.
func proveBulkRetirementWaitsForMutation(t *testing.T, selected any, db *sql.DB, backend string, evidence startupownership.GrantEvidence, retire func(context.Context) error) {
	t.Helper()
	ctx := testAuthorActivityContext()
	if backend == "sqlite" {
		// An independent native owner must contend on the database fence, not
		// the retiring owner's in-process writer permit.
		selected = NewSQLiteRuntimeStoreForTest(db)
	}
	rolledBack := errors.New("release bulk retirement fence by rollback")
	retired := make(chan error, 1)
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(txctx context.Context, tx *sql.Tx) error {
		if err := generationauthority.FenceMutation(txctx, tx, backend == "sqlite"); err != nil {
			return err
		}
		waiting, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		go func() { retired <- retire(waiting) }()
		<-waiting.Done()
		return rolledBack
	})
	if !errors.Is(err, rolledBack) {
		t.Fatalf("bulk retirement fence rollback: %v", err)
	}
	if err := <-retired; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bulk retirement must wait for held mutation fence until cancelled: %v", err)
	}
	var state string
	var version uint64
	if err := db.QueryRowContext(ctx, `SELECT state,state_version FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != string(evidence.State) || version != evidence.StateVersion {
		t.Fatalf("cancelled bulk retirement changed head: %s/%d", state, version)
	}
}

func TestGenerationMutationFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"normal", "selected"} {
			for _, finish := range []string{"commit", "rollback"} {
				t.Run(backend+"/"+kind+"/"+finish, func(t *testing.T) {
					store, db, sqlite := selectedForkDiscardTestStore(t, backend)
					mutationStore := store
					if sqlite {
						mutationStore = NewSQLiteRuntimeStoreForTest(db)
					}
					ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
					defer cancel()
					var grant startupownership.GenerationGrant
					var runID string
					if kind == "normal" {
						runID = uuid.NewString()
						selected := store.(agentFixtureFlowStore)
						identity := mustTestAgentIdentityForRun(runID, "fence-owner", "global")
						if err := agentfixture.UpsertStatic(t, ctx, selected, agentFixtureStaticRecord(t, identity)); err != nil {
							t.Fatal(err)
						}
						plan := currentAgentFixtureSourceSet(t, ctx, selected)
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
					} else {
						fixture := newSelectedCompletionFixture(t, store, db, sqlite)
						grant = selectedMutationFenceGrant(t, ctx, fixture)
						runID = fixture.forkRun
					}
					evidence, err := grant.Evidence()
					if err != nil {
						t.Fatal(err)
					}
					binding, err := grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					req := manager.AgentLifecycleTransition{Identity: mustTestAgentIdentityForRun(runID, "fence-owner", "global"), ProcessBinding: binding}
					rolledBack := errors.New("release generation mutation fence by rollback")
					retired := make(chan error, 1)
					err = runUnrevisionedEventFixtureTransactionForTest(ctx, mutationStore, func(txctx context.Context, tx *sql.Tx) error {
						if err := agentpersistence.AuthorizeGenerationMutationTx(txctx, tx, req, sqlite); err != nil {
							return err
						}
						// Only the native transaction's completion is held. Selected
						// execution/run/FK locks and grant authorization are real.
						waiting, stop := context.WithTimeout(ctx, time.Second)
						defer stop()
						go func() { retired <- grant.Retire(waiting) }()
						<-waiting.Done()
						if finish == "rollback" {
							return rolledBack
						}
						return nil
					})
					if (finish == "commit" && err != nil) || (finish == "rollback" && !errors.Is(err, rolledBack)) {
						t.Fatalf("exact pooled mutation authorization and %s: %v", finish, err)
					}
					if err := <-retired; !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("retirement crossed accepted mutation: %v", err)
					}
					var state string
					var version uint64
					if err := db.QueryRowContext(ctx, `SELECT state,state_version FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&state, &version); err != nil {
						t.Fatal(err)
					}
					if state != string(evidence.State) || version != evidence.StateVersion {
						t.Fatalf("cancelled retirement changed head: %s/%d", state, version)
					}
					// A transaction that observed the old head must reread after taking
					// the fence, not authorize its stale append-only predecessor.
					retiredGrant := false
					err = runUnrevisionedEventFixtureTransactionForTest(ctx, mutationStore, func(txctx context.Context, late *sql.Tx) error {
						if err := late.QueryRowContext(txctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&state); err != nil {
							return err
						}
						if !retiredGrant {
							if state != string(evidence.State) {
								t.Fatalf("stale-head setup did not observe the admitted grant: %s", state)
							}
							if err := grant.Retire(ctx); err != nil {
								t.Fatalf("retire after mutation releases dependency: %v", err)
							}
							retiredGrant = true
						}
						if err := agentpersistence.AuthorizeGenerationMutationTx(txctx, late, req, sqlite); err != nil {
							return err
						}
						t.Fatal("stale-head transaction authorized retired grant")
						return nil
					})
					if err == nil || !strings.Contains(err.Error(), "lifecycle generation grant is retired") {
						t.Fatalf("stale-head refusal must use current grant, not a missing ordering scope: %v", err)
					}
				})
			}
		}
	}
}

func selectedMutationFenceGrant(t *testing.T, ctx context.Context, fixture selectedCompletionFixture) startupownership.GenerationGrant {
	t.Helper()
	fixture.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
	fixture.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
	fixture.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
	issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "mutation-fence-selected", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := fixture.store.(interface {
		RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	}).RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
	if err != nil {
		t.Fatal(err)
	}
	process, err := fixture.process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	req := startupownership.SelectedForkGrantRequest{RuntimeInstanceID: process.RuntimeInstanceID, Binding: startupownership.SelectedForkGrantBinding{
		BindingID: bound.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
		ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration, ExecutionOwner: authority.ExecutionOwner,
		AdmissionFingerprint: issued.AdmissionFingerprint, ContainerPlanFingerprint: issued.ContainerPlanFingerprint,
		ActorCensusFingerprint: issued.ActorCensusFingerprint, EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
		DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint, PreparationFingerprint: issued.PreparationFingerprint,
	}}
	if err := fixture.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&req.BundleHash); err != nil {
		t.Fatal(err)
	}
	grant, err := fixture.process.IssueSelectedForkGenerationGrant(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	return grant
}
