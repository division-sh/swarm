package runtimepersistence

import (
	"context"
	"maps"
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
			validAccounting := func(before, after transactiontest.Snapshot, transactions, proofs, commits, failures uint64) bool {
				inspection, previous := after.ByOperation[transactiontest.RunExecutionInspection], before.ByOperation[transactiontest.RunExecutionInspection]
				proof, previousProof := after.ByOperation[transactiontest.Other], before.ByOperation[transactiontest.Other]
				total := transactions + proofs
				retained := sqlite || (after.Retained.BeginAttempts-before.Retained.BeginAttempts == total &&
					after.Retained.Begun-before.Retained.Begun == total &&
					after.Retained.ReadCommits-before.Retained.ReadCommits == proofs &&
					after.Retained.WriteCommits-before.Retained.WriteCommits == commits &&
					after.Retained.Failed-before.Retained.Failed == failures)
				return retained && after.Active == 0 &&
					after.Total.BeginAttempts-before.Total.BeginAttempts == total && after.Total.Begun-before.Total.Begun == total &&
					after.Total.ReadCommits-before.Total.ReadCommits == proofs && after.Total.WriteCommits-before.Total.WriteCommits == commits &&
					after.Total.Failed-before.Total.Failed == failures && after.Total.CommitAttempts-before.Total.CommitAttempts == commits+proofs &&
					after.Total.RollbackAttempts-before.Total.RollbackAttempts == failures && after.Total.CommitFailures == before.Total.CommitFailures &&
					after.Total.CleanupFailures == before.Total.CleanupFailures && after.Total.Revision == before.Total.Revision &&
					after.ByOperation[transactiontest.SourceSetLoad] == before.ByOperation[transactiontest.SourceSetLoad] &&
					inspection.BeginAttempts-previous.BeginAttempts == transactions && inspection.Begun-previous.Begun == transactions &&
					inspection.ReadCommits == previous.ReadCommits && inspection.WriteCommits-previous.WriteCommits == commits &&
					inspection.Failed-previous.Failed == failures && inspection.CommitAttempts-previous.CommitAttempts == commits &&
					inspection.RollbackAttempts-previous.RollbackAttempts == failures &&
					proof.BeginAttempts-previousProof.BeginAttempts == proofs && proof.Begun-previousProof.Begun == proofs &&
					proof.ReadCommits-previousProof.ReadCommits == proofs && proof.CommitAttempts-previousProof.CommitAttempts == proofs &&
					proof.WriteCommits == previousProof.WriteCommits && proof.Failed == previousProof.Failed &&
					proof.RollbackAttempts == previousProof.RollbackAttempts
			}
			inspect := func(t *testing.T, callCtx context.Context, runID string, want manager.RunExecutionOwnership, wantError string, transactions, postgresProofs uint64) {
				t.Helper()
				before := collector.Snapshot()
				got, err := grant.InspectRunExecutionOwnership(callCtx, runID)
				if got != want || (wantError == "" && err != nil) || (wantError != "" && (err == nil || !strings.Contains(err.Error(), wantError))) {
					t.Fatalf("ownership=%v err=%v want=%v error containing %q", got, err, want, wantError)
				}
				after := collector.Snapshot()
				proofs := postgresProofs
				if sqlite {
					proofs = 0
				}
				commits, failures := uint64(0), transactions
				if wantError == "" {
					commits, failures = transactions, 0
				}
				if !validAccounting(before, after, transactions, proofs, commits, failures) {
					t.Fatalf("inspection must use its exact owner transaction plus %d read-only possession proofs, with no separate source load or revision write: before=%+v after=%+v", proofs, before, after)
				}
				t.Logf("inspection transactions=%d commits=%d failed=%d possession_proofs=%d proof_read_commits=%d proof_writes=0 separate_source_loads=0 revision_writes=0", transactions, commits, failures, proofs, proofs)
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
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1, 1)
			})
			t.Run("selected_child_is_foreign", func(t *testing.T) {
				inspect(t, ctx, fixture.forkRun, manager.RunExecutionForeign, "", 1, 1)
			})
			t.Run("missing_run", func(t *testing.T) {
				inspect(t, ctx, uuid.NewString(), 0, "load run execution binding", 1, 2)
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
				inspect(t, ctx, otherRun, manager.RunExecutionOtherNormalSource, "", 1, 1)
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1, 1)
			})
			t.Run("stale_source_revision", func(t *testing.T) {
				defer setPlan(t, plan)
				setPlan(t, otherPlan)
				inspect(t, ctx, fixture.sourceRun, 0, "source-set revision is not current", 1, 2)
			})
			t.Run("matching_revision_corrupt_full_plan", func(t *testing.T) {
				defer setPlan(t, plan)
				corrupt := otherPlan
				corrupt.Revision = plan.Revision
				setPlan(t, corrupt)
				inspect(t, ctx, fixture.sourceRun, 0, "revision does not match canonical plan", 1, 2)
			})
			t.Run("missing_source_head", func(t *testing.T) {
				defer setPlan(t, plan)
				if _, err := db.ExecContext(ctx, `DELETE FROM agent_topology_source_set_head`); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "source-set revision is not current", 1, 2)
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
				inspect(t, ctx, fixture.sourceRun, 0, "generation grant is no longer current", 1, 2)
			})
			t.Run("caller_cancelled", func(t *testing.T) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				inspect(t, cancelled, fixture.sourceRun, 0, "context canceled", 0, 0)
				inspect(t, ctx, fixture.sourceRun, manager.RunExecutionOwned, "", 1, 1)
			})
			t.Run("receipt_hostility", func(t *testing.T) {
				before := collector.Snapshot()
				if got, err := grant.InspectRunExecutionOwnership(ctx, fixture.sourceRun); err != nil || got != manager.RunExecutionOwned {
					t.Fatalf("load positive inspection receipt: ownership=%v err=%v", got, err)
				}
				after := collector.Snapshot()
				proofs := uint64(0)
				if !sqlite {
					proofs = 1
				}
				if !validAccounting(before, after, 1, proofs, 1, 0) {
					t.Fatal("positive inspection receipt rejected")
				}
				for _, violation := range []string{"extra_read", "duplicated_inspection", "separate_source_load", "revision_write", "proof_write", "unfinished_work"} {
					t.Run(violation, func(t *testing.T) {
						corrupt := after
						corrupt.ByOperation = maps.Clone(after.ByOperation)
						switch violation {
						case "extra_read":
							corrupt.Total.BeginAttempts++
							corrupt.Total.Begun++
							corrupt.Total.ReadCommits++
							corrupt.Total.CommitAttempts++
						case "duplicated_inspection":
							count := corrupt.ByOperation[transactiontest.RunExecutionInspection]
							count.BeginAttempts++
							corrupt.ByOperation[transactiontest.RunExecutionInspection] = count
						case "separate_source_load":
							count := corrupt.ByOperation[transactiontest.SourceSetLoad]
							count.BeginAttempts++
							corrupt.ByOperation[transactiontest.SourceSetLoad] = count
						case "revision_write":
							corrupt.Total.Revision.Finalizations++
						case "proof_write":
							count := corrupt.ByOperation[transactiontest.Other]
							count.WriteCommits++
							corrupt.ByOperation[transactiontest.Other] = count
						case "unfinished_work":
							corrupt.Active++
						}
						if validAccounting(before, corrupt, 1, proofs, 1, 0) {
							t.Fatalf("accepted %s beyond the exact inspection/proof receipt", violation)
						}
					})
				}
			})
			t.Run("locally_retired_grant", func(t *testing.T) {
				if err := grant.Retire(ctx); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "generation is retired", 0, 1)
			})
			t.Run("released_process", func(t *testing.T) {
				if err := process.Release(ctx); err != nil {
					t.Fatal(err)
				}
				inspect(t, ctx, fixture.sourceRun, 0, "capability is terminal", 0, 0)
			})
		})
	}
}
