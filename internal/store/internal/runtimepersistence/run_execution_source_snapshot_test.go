package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRunExecutionSourceSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContext()
			process := fixture.process
			authority, err := process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			var hash string
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.sourceRun).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: hash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			operationID := uuid.NewString()
			if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: operationID, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
				BundleHash: hash, RuntimeInstanceID: authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatal(err)
			}
			collector, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			inspect := func(t *testing.T, callCtx context.Context, runID string, want manager.RunExecutionOwnership, wantError string, transactions uint64) {
				t.Helper()
				before := collector.Snapshot()
				got, err := grant.InspectRunExecutionOwnership(callCtx, runID)
				if got != want || (wantError == "" && err != nil) || (wantError != "" && (err == nil || !strings.Contains(err.Error(), wantError))) {
					t.Fatalf("ownership=%v err=%v want=%v error containing %q", got, err, want, wantError)
				}
				after := collector.Snapshot()
				inspection := after.ByOperation[transactiontest.RunExecutionInspection]
				previous := before.ByOperation[transactiontest.RunExecutionInspection]
				if after.Active != 0 || after.Total.BeginAttempts-before.Total.BeginAttempts != transactions ||
					inspection.BeginAttempts-previous.BeginAttempts != transactions ||
					after.ByOperation[transactiontest.SourceSetLoad] != before.ByOperation[transactiontest.SourceSetLoad] ||
					after.Total.Revision != before.Total.Revision {
					t.Fatalf("inspection must use one owner transaction with no separate source load or revision write: before=%+v after=%+v", before, after)
				}
				commits, failures := uint64(0), transactions
				if wantError == "" {
					commits, failures = transactions, 0
				}
				if inspection.WriteCommits-previous.WriteCommits != commits || inspection.Failed-previous.Failed != failures {
					t.Fatalf("inspection acknowledgement mismatch: before=%+v after=%+v", previous, inspection)
				}
				t.Logf("inspection transactions=%d commits=%d failed=%d separate_source_loads=0 revision_writes=0", transactions, commits, failures)
			}
			setPlan := func(t *testing.T, value agenttopology.SourceSetPlan) {
				t.Helper()
				raw, err := canonicaljson.Bytes(value)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO agent_topology_source_set_head (singleton_id,revision,plan,operation_id,updated_at)
					VALUES (1,$1,$2,$3,$4) ON CONFLICT(singleton_id) DO UPDATE SET revision=excluded.revision,plan=excluded.plan`, value.Revision, string(raw), operationID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			t.Run("owned_one_transaction", func(t *testing.T) {
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1)
			})
			t.Run("selected_child_is_foreign", func(t *testing.T) {
				inspect(t, ctx, fixture.forkRun, manager.RunExecutionForeign, "", 1)
			})
			t.Run("missing_run", func(t *testing.T) {
				inspect(t, ctx, uuid.NewString(), 0, "load run execution binding", 1)
			})
			otherArtifact := sourceartifactfixture.New("agents.yaml", []byte("# other execution source\nagents: {}\n"))
			otherHash := otherArtifact.BundleHash()
			otherPlan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: otherHash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Run("other_normal_source", func(t *testing.T) {
				otherRun := uuid.NewString()
				requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
					RunID: otherRun, BundleHash: otherHash, Artifact: otherArtifact,
					Origin: semanticScenarioSetupRunOriginForTest(), StartedAt: time.Now().UTC(),
				})
				inspect(t, ctx, otherRun, manager.RunExecutionOtherNormalSource, "", 1)
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1)
			})
			t.Run("stale_source_revision", func(t *testing.T) {
				defer setPlan(t, plan)
				setPlan(t, otherPlan)
				inspect(t, ctx, fixture.sourceRun, 0, "source-set revision is not current", 1)
			})
			t.Run("matching_revision_corrupt_full_plan", func(t *testing.T) {
				defer setPlan(t, plan)
				corrupt := otherPlan
				corrupt.Revision = plan.Revision
				setPlan(t, corrupt)
				inspect(t, ctx, fixture.sourceRun, 0, "revision does not match canonical plan", 1)
			})
			t.Run("missing_source_head", func(t *testing.T) {
				defer setPlan(t, plan)
				if _, err := db.ExecContext(ctx, `DELETE FROM agent_topology_source_set_head`); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "source-set revision is not current", 1)
			})
			t.Run("durable_grant_retirement", func(t *testing.T) {
				evidence, err := grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				persist := func(value startupownership.GrantEvidence) {
					raw, err := canonicaljson.Bytes(value)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := db.ExecContext(ctx, `UPDATE runtime_generation_grants SET state=$1,state_version=$2,snapshot=$3 WHERE grant_id=$4`, string(value.State), value.StateVersion, string(raw), value.GrantID); err != nil {
						t.Fatal(err)
					}
				}
				defer persist(evidence)
				retired := evidence
				retired.State, retired.StateVersion = startupownership.GrantRetired, evidence.StateVersion+1
				persist(retired)
				inspect(t, ctx, fixture.sourceRun, 0, "generation grant is no longer current", 1)
			})
			t.Run("caller_cancelled", func(t *testing.T) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				inspect(t, cancelled, fixture.sourceRun, 0, "context canceled", 0)
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1)
			})
			t.Run("locally_retired_grant", func(t *testing.T) {
				if err := grant.Retire(ctx); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "generation is retired", 0)
			})
			t.Run("released_process", func(t *testing.T) {
				if err := process.Release(ctx); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "capability is terminal", 0)
			})
		})
	}
}
