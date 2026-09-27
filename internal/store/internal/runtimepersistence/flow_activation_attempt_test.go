package runtimepersistence_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type flowActivationAttemptTestStore interface {
	runtimestartupownership.Store
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	BeginDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeReadinessPlan, uint64, runtimeprocessbinding.Binding) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error)
	VerifyDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
	MarkDynamicFlowRuntimeTopologyReadyForAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt, runtimepipeline.DynamicFlowRuntimeReadinessPlan, time.Time) (runtimepipeline.DynamicFlowRuntimeTopologyReadyResult, error)
	RetireDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
	AbandonDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
	ReconcileDynamicFlowRuntimeReadinessPlans(context.Context, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error)
}

func TestFlowActivationAttemptAdmissionBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (flowActivationAttemptTestStore, *sql.DB, bool)
	}{
		{"postgres", func(t *testing.T) (flowActivationAttemptTestStore, *sql.DB, bool) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db, false
		}},
		{"sqlite", func(t *testing.T) (flowActivationAttemptTestStore, *sql.DB, bool) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.Database(selected), true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, sqlite := tc.open(t)
			ctx := testAuthorActivityContext()
			runID, path := uuid.NewString(), "account/attempt"
			hash := mustExternalStoreTestSourceArtifactFact().BundleHash()
			requireReadinessRun(t, ctx, db, sqlite, runID, hash)
			seedExactFlowInstanceDescriptorOwner(t, db, sqlite, runID, uuid.NewString(), path, hash)
			readiness, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found {
				t.Fatalf("load readiness: found=%v err=%v", found, err)
			}

			acquire := runtimestartupownership.AcquireRequest{OwnerID: "flow-attempt-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			process, err := selected.AcquireProcessCapability(ctx, acquire)
			if err != nil {
				t.Fatalf("acquire process: %v", err)
			}
			t.Cleanup(func() { _ = process.Release(context.Background()) })
			sourceSet, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{BundleHash: hash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := process.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: sourceSet}); err != nil {
				t.Fatalf("install source set: %v", err)
			}
			grant, err := process.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{BundleHash: hash, RuntimeInstanceID: acquire.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: sourceSet.Revision})
			if err != nil {
				t.Fatalf("issue grant: %v", err)
			}
			binding, err := grant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			if result, err := selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding); err == nil || result.Acknowledged {
				t.Fatalf("prepared grant admitted activation: result=%+v err=%v", result, err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			admitted, err := selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding)
			if err != nil || !admitted.Acknowledged || admitted.Reused || admitted.Attempt.Validate() != nil {
				t.Fatalf("begin exact attempt: result=%+v err=%v", admitted, err)
			}
			forgedCurrentBinding := binding
			forgedCurrentBinding.ProcessBootID = uuid.NewString()
			forgedCurrent, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(admitted.Attempt.ID(), admitted.Attempt.RunID(), admitted.Attempt.InstancePath(), admitted.Attempt.PlanRevision(), forgedCurrentBinding)
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, forgedCurrent); err == nil {
				t.Fatal("invented current binding settled the exact attempt")
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("invented retirement affected current attempt: %v", err)
			}
			if again, err := selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding); err == nil || again.Acknowledged || !strings.Contains(err.Error(), "unsettled") {
				t.Fatalf("unsettled same-process retry: result=%+v err=%v", again, err)
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("verify admitted attempt: %v", err)
			}
			ready, err := selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, admitted.Attempt, readiness.Plan, time.Now().UTC())
			if err != nil || !ready.Acknowledged {
				t.Fatalf("complete exact attempt: ready=%+v err=%v", ready, err)
			}
			replayedReady, err := selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, admitted.Attempt, readiness.Plan, time.Now().UTC())
			if err != nil || !replayedReady.Acknowledged {
				t.Fatalf("replay acknowledged topology completion: ready=%+v err=%v", replayedReady, err)
			}
			reused, err := selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding)
			if err != nil || !reused.Acknowledged || !reused.Reused || reused.Attempt.ID() != admitted.Attempt.ID() {
				t.Fatalf("reuse committed attempt: result=%+v err=%v", reused, err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("retire committed attempt: %v", err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("repeat orderly retirement: %v", err)
			}
			if err := selected.AbandonDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err == nil {
				t.Fatal("failure disposition replaced settled orderly retirement")
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err == nil {
				t.Fatal("retired attempt retained activation authority")
			}
			readiness, found, err = selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() {
				t.Fatalf("process retirement erased completed durable topology: found=%v readiness=%+v err=%v", found, readiness, err)
			}
			admitted, err = selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding)
			if err != nil || !admitted.Acknowledged || admitted.Reused || admitted.Attempt.ID() == reused.Attempt.ID() {
				t.Fatalf("fresh attempt after retirement: result=%+v err=%v", admitted, err)
			}
			readiness, found, err = selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() {
				t.Fatalf("fresh process admission erased completed durable topology: found=%v readiness=%+v err=%v", found, readiness, err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("retire interrupted pre-run reconstruction: %v", err)
			}
			readiness, found, err = selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() {
				t.Fatalf("interrupted pre-run reconstruction erased completed durable topology: found=%v readiness=%+v err=%v", found, readiness, err)
			}
			admitted, err = selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding)
			if err != nil || !admitted.Acknowledged || admitted.Reused {
				t.Fatalf("readmit completed reconstruction after interruption: result=%+v err=%v", admitted, err)
			}
			ready, err = selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, admitted.Attempt, readiness.Plan, time.Now().UTC())
			if err != nil || !ready.Acknowledged {
				t.Fatalf("complete fresh attempt: ready=%+v err=%v", ready, err)
			}
			readiness, found, err = selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() {
				t.Fatalf("load completed fresh readiness: found=%v readiness=%+v err=%v", found, readiness, err)
			}
			revised := readiness.Plan
			revised.WorkflowVersion = "2.0.0"
			results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: readiness, Expected: revised}}, time.Now().UTC())
			if err != nil || len(results) != 1 || results[0].PlanRevision != readiness.PlanRevision+1 {
				t.Fatalf("replace plan: results=%+v err=%v", results, err)
			}
			if stale, err := selected.BeginDynamicFlowRuntimeActivation(ctx, readiness.Plan, readiness.PlanRevision, binding); err == nil || stale.Acknowledged {
				t.Fatalf("stale revision admitted activation: result=%+v err=%v", stale, err)
			}
			if stale, err := selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, admitted.Attempt, readiness.Plan, time.Now().UTC()); err == nil || stale.Acknowledged {
				t.Fatalf("superseded attempt completed topology: result=%+v err=%v", stale, err)
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err == nil {
				t.Fatal("superseded attempt retained activation authority")
			}
			if next, err := selected.BeginDynamicFlowRuntimeActivation(ctx, revised, results[0].PlanRevision, binding); err == nil || next.Acknowledged || !strings.Contains(err.Error(), "unsettled") {
				t.Fatalf("successor bypassed predecessor settlement: result=%+v err=%v", next, err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
				t.Fatalf("settle predecessor: %v", err)
			}
			next, err := selected.BeginDynamicFlowRuntimeActivation(ctx, revised, results[0].PlanRevision, binding)
			if err != nil || !next.Acknowledged || next.Reused || next.Attempt.ID() == admitted.Attempt.ID() {
				t.Fatalf("begin successor after settlement: result=%+v err=%v", next, err)
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatalf("retire grant: %v", err)
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, next.Attempt); err == nil {
				t.Fatal("retired grant retained activation authority")
			}
			if committed, err := selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, next.Attempt, revised, time.Now().UTC()); err == nil || committed.Acknowledged {
				t.Fatalf("retired grant completed topology: result=%+v err=%v", committed, err)
			}
			if err := process.Release(ctx); err != nil {
				t.Fatalf("release predecessor process: %v", err)
			}
			successorRequest := runtimestartupownership.AcquireRequest{OwnerID: "flow-attempt-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			successorProcess, err := selected.AcquireProcessCapability(ctx, successorRequest)
			if err != nil {
				t.Fatalf("acquire successor process: %v", err)
			}
			t.Cleanup(func() { _ = successorProcess.Release(context.Background()) })
			successorGrant, err := successorProcess.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{BundleHash: hash, RuntimeInstanceID: successorRequest.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: sourceSet.Revision})
			if err != nil {
				t.Fatalf("issue successor grant: %v", err)
			}
			if _, err := successorGrant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := successorGrant.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			successorBinding, err := successorGrant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			successor, err := selected.BeginDynamicFlowRuntimeActivation(ctx, revised, results[0].PlanRevision, successorBinding)
			if err != nil || !successor.Acknowledged || successor.Reused {
				t.Fatalf("begin exact successor after process takeover: result=%+v err=%v", successor, err)
			}
			forgedBinding := binding
			forgedBinding.ProcessBootID = uuid.NewString()
			forged, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(next.Attempt.ID(), next.Attempt.RunID(), next.Attempt.InstancePath(), next.Attempt.PlanRevision(), forgedBinding)
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, forged); err == nil {
				t.Fatal("invented predecessor binding settled after foreign takeover")
			}
			if err := selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, next.Attempt); err != nil {
				t.Fatalf("settle predecessor after foreign takeover: %v", err)
			}
			if err := selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, successor.Attempt); err != nil {
				t.Fatalf("predecessor retirement affected successor: %v", err)
			}
			if ready, err := selected.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, successor.Attempt, revised, time.Now().UTC()); err != nil || !ready.Acknowledged {
				t.Fatalf("complete successor after predecessor retirement: result=%+v err=%v", ready, err)
			}
		})
	}
}
