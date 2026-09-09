package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestResetCleanupSourceTopologyAndReceiptAtomicityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, include := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/include_source_artifacts=%t", backend, include), func(t *testing.T) {
				var selected interface {
					startupownership.Store
					destructivereset.QuiescenceStore
					sourceartifactfixture.Writer
				}
				var db *sql.DB
				if backend == "sqlite" {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected, db = s, s.backend.ConstructionHandle()
				} else {
					_, database, _ := testutil.StartPostgres(t)
					selected, db = newTestPostgresStore(t, database), database
				}
				ctx := testAuthorActivityContext()
				artifact := sourceartifactfixture.Artifact()
				sourceartifactfixture.RequireArtifact(t, ctx, selected, artifact)
				cap, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-source-atomicity"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cap.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				initial, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: artifact.BundleHash()}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cap.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: initial}); err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				fixture := runlifecyclefixture.Fixture{RunID: runID, BundleHash: artifact.BundleHash(), Origin: runlifecyclefixture.ScenarioSetupOrigin()}
				if backend == "sqlite" {
					runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
				} else {
					runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
				}
				op, err := cap.AdmitResetOperation(ctx, destructivereset.Request{
					OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "source-atomicity",
					IncludeSourceArtifacts: include, IncludeSourceArtifactsSet: true, RequestedAt: time.Now().UTC(),
				})
				if err != nil {
					t.Fatal(err)
				}
				planned := op
				if !reflect.DeepEqual(op.SourceSet, &initial) {
					t.Fatalf("admission did not capture exact canonical source topology: %+v", op.SourceSet)
				}
				rewritten := op
				rewritten.Revision++
				rewritten.SourceSet = nil
				if err := cap.AdvanceResetOperation(ctx, op, rewritten); err == nil {
					t.Fatal("durable source snapshot could be rewritten")
				}
				planned.Phase, planned.Revision = destructivereset.PhasePlanned, op.Revision+1
				planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: op.Request.RequestedAt, IncludeSourceArtifacts: include,
					Plan: destructivereset.Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: include,
						ActiveRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}, CleanupRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}}}
				if err := cap.AdvanceResetOperation(ctx, op, planned); err != nil {
					t.Fatal(err)
				}
				quiescence, err := selected.ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
					OperationID: op.Request.OperationID, ActorTokenID: op.Request.ActorTokenID, RequestedAt: op.Request.RequestedAt, Result: *planned.Plan,
				})
				if err != nil {
					t.Fatal(err)
				}
				cleanup := destructivereset.CleanupRequest{OperationID: op.Request.OperationID, ActorTokenID: op.Request.ActorTokenID, RequestedAt: op.Request.RequestedAt, Result: *planned.Plan, Quiescence: quiescence}
				var topology *agenttopology.SourceSetCommitRequest
				final := initial
				if include {
					final, err = agenttopology.NewSourceSetPlan(nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					topology = &agenttopology.SourceSetCommitRequest{OperationID: op.Request.OperationID, ExpectedRevision: initial.Revision, Plan: final}
				}
				assertState := func(wantPlan agenttopology.SourceSetPlan, wantRuns, wantArtifacts int) {
					t.Helper()
					plan, exists, err := cap.CurrentSourceSet(ctx)
					if err != nil || !exists || !reflect.DeepEqual(plan, wantPlan) {
						t.Fatalf("source topology = %+v, %t, %v; want %+v", plan, exists, err, wantPlan)
					}
					for table, want := range map[string]int{"runs": wantRuns, "source_artifacts": wantArtifacts} {
						var count int
						if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != want {
							t.Fatalf("%s count=%d, %v; want %d", table, count, err, want)
						}
					}
				}
				for _, invalid := range []string{"dry run", "retain sources", "different operation", "nonempty replacement"} {
					t.Run("reject topology/"+invalid, func(t *testing.T) {
						request := cleanup
						request.Result.IncludeSourceArtifacts = true
						empty, err := agenttopology.EmptySourceSetPlan()
						if err != nil {
							t.Fatal(err)
						}
						mutation := &agenttopology.SourceSetCommitRequest{OperationID: request.OperationID, ExpectedRevision: initial.Revision, Plan: empty}
						switch invalid {
						case "dry run":
							request.Result.DryRun = true
						case "retain sources":
							request.Result.IncludeSourceArtifacts = false
						case "different operation":
							mutation.OperationID = uuid.NewString()
						case "nonempty replacement":
							mutation.Plan = initial
						}
						if _, err := cap.ApplyDestructiveResetCleanup(ctx, request, mutation); err == nil || !strings.Contains(err.Error(), "reset topology must clear sources for the exact apply operation") {
							t.Fatalf("invalid reset topology was not rejected at its semantic boundary: %v", err)
						}
						assertState(initial, 1, 1)
					})
				}
				if include {
					if _, err := cap.ApplyDestructiveResetCleanup(ctx, cleanup, nil); err == nil || !strings.Contains(err.Error(), "requires its atomic topology mutation") {
						t.Fatalf("source deletion without topology mutation was not rejected: %v", err)
					}
					assertState(initial, 1, 1)
				}
				empty, err := agenttopology.EmptySourceSetPlan()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cap.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: initial.Revision, Plan: empty}); err != nil {
					t.Fatal(err)
				}
				if _, err := cap.ApplyDestructiveResetCleanup(ctx, cleanup, topology); err == nil || !strings.Contains(err.Error(), "differs from its admitted snapshot") {
					t.Fatalf("reset consumed a later source topology: %v", err)
				}
				assertState(empty, 1, 1)
				if _, err := cap.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: empty.Revision, Plan: initial}); err != nil {
					t.Fatal(err)
				}
				drop := installResetReceiptFault(t, db, backend, "cleanup_committed")
				if _, err := cap.ApplyDestructiveResetCleanup(ctx, cleanup, topology); err == nil {
					t.Fatal("receipt fault did not reject cleanup")
				}
				assertState(initial, 1, 1)
				pending, err := cap.ReadResetOperation(ctx, op.Request.OperationID)
				if err != nil || pending.Phase != destructivereset.PhaseQuiesced || pending.Cleanup != nil {
					t.Fatalf("rolled back receipt = %+v, %v", pending, err)
				}
				drop()
				result, err := cap.ApplyDestructiveResetCleanup(ctx, cleanup, topology)
				if err != nil {
					t.Fatal(err)
				}
				wantArtifacts := 1
				if include {
					wantArtifacts = 0
				}
				assertState(final, 0, wantArtifacts)
				committed, err := cap.ReadResetOperation(ctx, op.Request.OperationID)
				if err != nil || !reflect.DeepEqual(committed.Cleanup, &result) {
					t.Fatalf("committed receipt = %+v, %v", committed, err)
				}
			})
		}
	}
}
