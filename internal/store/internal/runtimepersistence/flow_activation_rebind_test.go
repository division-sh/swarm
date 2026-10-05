package runtimepersistence_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestFlowActivationSourceSetRebindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected flowActivationAttemptTestStore
			var db *sql.DB
			sqlite := backend == "sqlite"
			if sqlite {
				store := storetest.StartSQLiteRuntimeStore(t)
				selected, db = store, storetest.Database(store)
			} else {
				_, database, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, db = storetest.AdmitPostgresRuntimeStore(t, database), database
			}
			ctx := testAuthorActivityContext()
			runID, path := uuid.NewString(), "account/restamp"
			hash := mustExternalStoreTestSourceArtifactFact().BundleHash()
			requireReadinessRun(t, ctx, db, sqlite, runID, hash)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runID, uuid.NewString(), path, hash)
			runtimeID := uuid.NewString()
			process, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "readiness-restamp", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := process.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			sourceSet, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: hash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: sourceSet}); err != nil {
				t.Fatal(err)
			}
			var generation uint64
			issue := func() startupownership.LiveGenerationGrant {
				generation++
				grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: hash, RuntimeInstanceID: runtimeID, RuntimeGeneration: generation, SourceSetRevision: sourceSet.Revision})
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
			grant := issue()
			for _, phase := range []pipeline.FlowAttachmentPhase{pipeline.FlowAttachmentPlanned, pipeline.FlowAttachmentAgentsRegistered, pipeline.FlowAttachmentRouteInstalled, pipeline.FlowAttachmentTimersArmed, pipeline.FlowAttachmentReady} {
				t.Run(string(phase), func(t *testing.T) {
					row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, flowidentity.RouteForInstancePath(path))
					if err != nil || !found {
						t.Fatalf("readiness: found=%v err=%v", found, err)
					}
					binding, err := grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					request := pipeline.NewDynamicFlowRuntimeActivationRequest(row.Plan, row.AttemptOrdinal, row.AttemptState, binding)
					admitted, err := selected.BeginDynamicFlowRuntimeActivation(ctx, request)
					if err != nil || !admitted.Acknowledged {
						t.Fatalf("admission: %+v %v", admitted, err)
					}
					for prev := pipeline.FlowAttachmentPlanned; prev != phase; {
						result, err := selected.AdvanceFlowAttachment(ctx, admitted.Attempt, prev, time.Now().UTC())
						if err != nil || !result.Admitted() {
							t.Fatalf("advance %s: %+v %v", prev, result, err)
						}
						prev = result.Phase
					}
					before, _, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, row.Plan.Identity.Route())
					if err != nil {
						t.Fatal(err)
					}
					successor := issue()
					owner := successor.(manager.FlowReadinessSourceSetRebindPersistence)
					rebind := manager.FlowReadinessSourceSetRebindRequest{Attempt: admitted.Attempt, PlanHash: row.PlanHash}
					wrong := rebind
					wrong.PlanHash = "changed-plan"
					if _, err := owner.RebindFlowReadinessSourceSet(ctx, wrong); err == nil {
						t.Fatal("changed plan admitted re-stamp")
					}
					wrong = rebind
					wrong.Attempt, _ = pipeline.NewDynamicFlowRuntimeActivationAttempt("999", runID, path, binding)
					if _, err := owner.RebindFlowReadinessSourceSet(ctx, wrong); err == nil {
						t.Fatal("foreign attempt admitted re-stamp")
					}
					result, err := owner.RebindFlowReadinessSourceSet(ctx, rebind)
					if err != nil || result.Attempt.ID() != admitted.Attempt.ID() || len(result.Transitions) != 0 {
						t.Fatalf("agentless re-stamp: %+v %v", result, err)
					}
					after, _, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, row.Plan.Identity.Route())
					if err != nil || after.Phase != phase || after.AttemptOrdinal != before.AttemptOrdinal || after.PlanHash != before.PlanHash || !after.CreationEventEmittedAt.Equal(before.CreationEventEmittedAt) {
						t.Fatalf("re-stamp changed attachment progress: before=%+v after=%+v err=%v", before, after, err)
					}
					rawAfter := flowReadinessStampSnapshot(t, ctx, db, sqlite, runID, path)
					duplicate, err := owner.RebindFlowReadinessSourceSet(ctx, rebind)
					if err != nil || !reflect.DeepEqual(result, duplicate) {
						t.Fatalf("duplicate re-stamp: %+v %v", duplicate, err)
					}
					again, _, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, row.Plan.Identity.Route())
					if err != nil || !reflect.DeepEqual(after, again) || !reflect.DeepEqual(rawAfter, flowReadinessStampSnapshot(t, ctx, db, sqlite, runID, path)) {
						t.Fatalf("duplicate changed readiness bytes: %v", err)
					}
					if err := grant.Retire(ctx); err != nil {
						t.Fatal(err)
					}
					resolver := selected.(interface {
						ResolveDynamicFlowRuntimeActivation(context.Context, pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationResolution, error)
					})
					resolved, err := resolver.ResolveDynamicFlowRuntimeActivation(ctx, request)
					if err != nil || resolved.Disposition != pipeline.FlowActivationAdmitted || request.ValidateResolution(resolved) != nil || resolved.Attempt.ID() != admitted.Attempt.ID() {
						t.Fatalf("original lost-ack request after re-stamp: %+v %v", resolved, err)
					}
					unrelated := pipeline.NewDynamicFlowRuntimeActivationRequest(row.Plan, request.Predecessor(), request.PredecessorDisposition(), binding)
					foreign, err := resolver.ResolveDynamicFlowRuntimeActivation(ctx, unrelated)
					if err != nil || foreign.Disposition != pipeline.FlowActivationForeign || unrelated.ValidateResolution(foreign) != nil {
						t.Fatalf("unrelated request acquired lost-ack authority: %+v %v", foreign, err)
					}
					if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err == nil {
						t.Fatal("retired grant still authorized forward progress")
					}
					if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, result.Attempt); err != nil {
						t.Fatalf("current grant cannot verify same attachment: %v", err)
					}
					if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
						t.Fatalf("original owner lost exact-attempt cleanup rights: %v", err)
					}
					if _, err := owner.RebindFlowReadinessSourceSet(ctx, rebind); err == nil {
						t.Fatal("retired attempt revived by re-stamp")
					}
					grant = successor
				})
			}
		})
	}
}

func flowReadinessStampSnapshot(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, runID, path string) []any {
	t.Helper()
	query := `SELECT activation_attempt_grant_id::text, activation_request_id::text, updated_at, phase, plan::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
	if sqlite {
		query = `SELECT activation_attempt_grant_id, activation_request_id, updated_at, phase, plan FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	}
	values := make([]any, 5)
	if err := db.QueryRowContext(ctx, query, runID, path).Scan(&values[0], &values[1], &values[2], &values[3], &values[4]); err != nil {
		t.Fatal(err)
	}
	return values
}
