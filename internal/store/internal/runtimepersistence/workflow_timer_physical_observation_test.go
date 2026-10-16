package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowTimerPhysicalReadersKeepTaskTypeScopeAndDriverTimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithTimer(t, backend, false, false, true, false)
			f.constructKeylessRoot(t)
			req := f.request("physical-timer-key", "physical-timer-instance", "native")
			req.OccurredAt = time.Now().UTC().Truncate(time.Microsecond)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil || len(plan.Lifecycle.Timers) != 1 {
				t.Fatalf("genuine compiled flow planning omitted timer: %+v %v", plan, err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged || !result.Created {
				t.Fatalf("genuine atomic construction omitted timer: %+v %v", result, err)
			}
			initial := plan.Lifecycle.Timers[0].Activation
			var want WorkflowTimerOccurrenceStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT timer_name, fire_at, status FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer'`, initial.RunID, initial.EntityID).
				Scan(&want.TaskID, &want.FireAt, &want.Status); err != nil || want.TaskID == "" || want.Status != "active" || want.FireAt == nil {
				t.Fatalf("real timer lacks positive physical identity/time/status evidence: %+v %v", want, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadWorkflowTimerActivationStorageForTest(f.ctx, f.store, initial.RunID, initial.EntityID); err != nil || got != (WorkflowTimerActivationStorage{TaskID: want.TaskID, Status: want.Status}) {
				t.Fatalf("activation observation changed original task/status: %+v %v", got, err)
			}
			if got, err := ReadWorkflowTimerOccurrenceStorageForTest(f.ctx, f.store, initial.RunID, initial.EntityID); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("occurrence changed raw driver time/bytes/type/clock: %+v want=%+v %v", got, want, err)
			}
			if got, err := CountWorkflowTimerStatusRowsForTest(f.ctx, f.store, initial.RunID, initial.EntityID, "active"); err != nil || got != 1 {
				t.Fatalf("active timer count lost actual admission: %d %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 3 || counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 {
				t.Fatalf("timer readers merged cuts or bypassed original selected coordinator: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), initial.EntityID}, {initial.RunID, uuid.NewString()}} {
				if got, err := ReadWorkflowTimerActivationStorageForTest(f.ctx, f.store, scope[0], scope[1]); !errors.Is(err, sql.ErrNoRows) || got != (WorkflowTimerActivationStorage{}) {
					t.Fatalf("foreign run/entity supplied activation evidence: %+v %v", got, err)
				}
				if got, err := ReadWorkflowTimerOccurrenceStorageForTest(f.ctx, f.store, scope[0], scope[1]); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, WorkflowTimerOccurrenceStorage{}) {
					t.Fatalf("foreign run/entity supplied occurrence evidence: %+v %v", got, err)
				}
				if got, err := CountWorkflowTimerStatusRowsForTest(f.ctx, f.store, scope[0], scope[1], "active"); err != nil || got != 0 {
					t.Fatalf("foreign run/entity supplied status count: %d %v", got, err)
				}
			}
			if got, err := CountWorkflowTimerStatusRowsForTest(f.ctx, f.store, initial.RunID, initial.EntityID, "fired"); err != nil || got != 0 {
				t.Fatalf("timer count ignored the original status predicate: %d %v", got, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			assertWorkflowTimerPhysicalReadFailure(t, cancelled, f.store, initial.RunID, initial.EntityID, context.Canceled)
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("timer readback changed complete application/history/lease/clock state")
			}
			restore()
			binding, err := f.grant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := f.workflows.BeginDynamicFlowRuntimeActivation(f.ctx, pipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding))
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("real cancellation activation admission: %+v %v", admitted, err)
			}
			defer func() {
				if err := f.workflows.RetireDynamicFlowRuntimeActivationAttempt(context.WithoutCancel(f.ctx), admitted.Attempt); err != nil {
					t.Error(err)
				}
			}()
			reader := f.store.(pipeline.WorkflowTimerActivationPersistence)
			command := pipeline.WorkflowTimerReconciliationCommand{
				RunID: initial.RunID, Route: initial.Route, EntityID: initial.EntityID, ActivationAttempt: &admitted.Attempt,
				Plan: pipeline.WorkflowLifecycleMutationPlan{Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationCancel, Activation: initial}}},
			}
			if result, err := reader.CommitWorkflowTimerReconciliation(f.ctx, command); err != nil || !result.Committed {
				t.Fatalf("real timer cancellation: %+v %v", result, err)
			}
			if got, err := CountWorkflowTimerStatusRowsForTest(f.ctx, f.store, initial.RunID, initial.EntityID, "cancelled"); err != nil || got != 1 {
				t.Fatalf("physical timer history lost actual cancellation: %d %v", got, err)
			}
			want.Status = "cancelled"
			if got, err := ReadWorkflowTimerOccurrenceStorageForTest(f.ctx, f.store, initial.RunID, initial.EntityID); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("cancelled occurrence lost historical task/time or added active-only filter: %+v %v", got, err)
			}
		})
	}
}

func assertWorkflowTimerPhysicalReadFailure(t *testing.T, ctx context.Context, owner any, runID, entityID string, cause error) {
	t.Helper()
	check := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if got, err := ReadWorkflowTimerActivationStorageForTest(ctx, owner, runID, entityID); !check(err) || got != (WorkflowTimerActivationStorage{}) {
		t.Fatalf("failed timer owner supplied activation: %+v %v", got, err)
	}
	if got, err := ReadWorkflowTimerOccurrenceStorageForTest(ctx, owner, runID, entityID); !check(err) || !reflect.DeepEqual(got, WorkflowTimerOccurrenceStorage{}) {
		t.Fatalf("failed timer owner supplied occurrence: %+v %v", got, err)
	}
	if got, err := CountWorkflowTimerStatusRowsForTest(ctx, owner, runID, entityID, "active"); !check(err) || got != 0 {
		t.Fatalf("failed timer owner supplied status count: %d %v", got, err)
	}
}

func TestWorkflowTimerPhysicalReadersRefuseRawAndClosedOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertWorkflowTimerPhysicalReadFailure(t, context.Background(), selected, uuid.NewString(), uuid.NewString(), nil)
	}
	f, _ := newCompetingRevisionPairFixture(t, "sqlite")
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	assertWorkflowTimerPhysicalReadFailure(t, f.ctx, f.store, uuid.NewString(), uuid.NewString(), nil)
}
