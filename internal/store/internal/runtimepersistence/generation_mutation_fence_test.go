package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestGenerationMutationFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"normal", "selected"} {
			for _, finish := range []string{"commit", "rollback"} {
				t.Run(backend+"/"+kind+"/"+finish, func(t *testing.T) {
					selected, mutationStore, db, postgres := newFanOutOwnerPairForTest(t, backend)
					store := selected.(selectedForkDiscardStore)
					sqlite := !postgres
					ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
					ctx = originalStoreFixtureContext(t, ctx, selected)
					defer cancel()
					ctx = originalStoreFixtureContext(t, ctx, store)
					var grant startupownership.GenerationGrant
					var runID string
					if kind == "normal" {
						runID = uuid.NewString()
						selected := store.(agentFixtureFlowStore)
						identity := mustTestAgentIdentityForRun(runID, "fence-owner", "global")
						ctx = requireRunningRunForTest(t, ctx, store, runID, time.Now().UTC())
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
						rooted, err := selected.(interface {
							LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
						}).LoadRunRootedSource(ctx, fixture.forkRun)
						if err != nil {
							t.Fatal(err)
						}
						ctx = correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx,
							mustStoreTestSourceArtifactFact(rooted.Admission().BundleHash())), rooted.Admission())
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
					if err := grant.Retire(ctx); err != nil {
						t.Fatalf("retire after mutation releases dependency: %v", err)
					}
					// Native admission acquires the mutation fence before this
					// callback; the retained binding must still reject the new head.
					err = runUnrevisionedEventFixtureTransactionForTest(ctx, mutationStore, func(txctx context.Context, late *sql.Tx) error {
						if err := agentpersistence.AuthorizeGenerationMutationTx(txctx, late, req, sqlite); err != nil {
							return err
						}
						t.Fatal("retained binding authorized retired grant")
						return nil
					})
					if err == nil || !strings.Contains(err.Error(), "lifecycle generation grant is retired") {
						t.Fatalf("retained-binding refusal must use current grant, not a missing ordering scope: %v", err)
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
	return selectedFixtureGrantForAuthority(t, ctx, fixture, issued, authority)
}

func selectedFixtureGrantForAuthority(t *testing.T, ctx context.Context, fixture selectedCompletionFixture, issued runfork.SelectedContractRuntimeExecution, authority runtimeeffects.Authority) startupownership.GenerationGrant {
	t.Helper()
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
	req.BundleHash = fixture.request.DeclarationPlan.BundleHash
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
