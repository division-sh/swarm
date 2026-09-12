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
func proveBulkRetirementWaitsForMutation(t *testing.T, db *sql.DB, backend string, evidence startupownership.GrantEvidence, retire func(context.Context) error) {
	t.Helper()
	ctx := testAuthorActivityContext()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := generationauthority.FenceMutation(ctx, tx, backend == "sqlite"); err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	retired := make(chan error, 1)
	go func() { retired <- retire(waiting) }()
	<-waiting.Done()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
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
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if err := agentpersistence.AuthorizeGenerationMutationTx(ctx, tx, req, sqlite); err != nil {
						t.Fatalf("exact pooled mutation authorization: %v", err)
					}
					// The authorizer is the production one; only the outer transaction's
					// completion is held. Selected execution/run/FK locks are real.
					waiting, stop := context.WithTimeout(ctx, time.Second)
					retired := make(chan error, 1)
					go func() { retired <- grant.Retire(waiting) }()
					<-waiting.Done()
					if finish == "commit" {
						err = tx.Commit()
					} else {
						err = tx.Rollback()
					}
					if err != nil {
						t.Fatal(err)
					}
					if err := <-retired; !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("retirement crossed accepted mutation: %v", err)
					}
					stop()
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
					late, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer late.Rollback()
					if err := late.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&state); err != nil {
						t.Fatal(err)
					}
					if err := grant.Retire(ctx); err != nil {
						t.Fatalf("retire after mutation releases dependency: %v", err)
					}
					if err := agentpersistence.AuthorizeGenerationMutationTx(ctx, late, req, sqlite); err == nil {
						t.Fatal("stale-head transaction authorized retired grant")
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
