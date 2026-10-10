package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowSchedulerObservationKeepsPhysicalHistoryAndExactScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			run, otherRun, entity, path := uuid.NewString(), uuid.NewString(), uuid.NewString(), "mutation-flow"
			ctx := selectedScheduleTestContext(t, run)
			for _, id := range []string{run, otherRun} {
				requireRunningRunForTest(t, ctx, f.store, id, time.Now().UTC())
			}
			selected := f.store.(runtimegenericschedule.Store)
			for i, scope := range []struct {
				run, entity, path, owner string
				terminal                 bool
			}{
				{run, entity, path, "workflow-runtime", false},
				{run, entity, path, "workflow-runtime", true},
				{otherRun, entity, path, "workflow-runtime", false},
				{run, uuid.NewString(), path, "workflow-runtime", false},
				{run, entity, "other-flow", "workflow-runtime", false},
				{run, entity, path, "another-owner", false},
			} {
				source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: scope.path, FlowInstance: scope.path, EntityID: scope.entity})
				if err != nil {
					t.Fatal(err)
				}
				committed, err := selected.AdmitGenericScheduleOutcome(ctx, runtimegenericschedule.AdmissionCommand{
					ScheduleKey: uuid.NewString(), RunID: scope.run, EntityID: scope.entity, FlowInstance: scope.path,
					OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: scope.owner,
					EventType: "timer.task_timeout", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
					ExecutionMode: executionmode.Live, Due: runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(2 * time.Hour)),
				})
				if err != nil || !committed.Acknowledged || committed.Result.Outcome != runtimegenericschedule.AdmissionCreated {
					t.Fatalf("admit physical timer %d: %+v %v", i, committed, err)
				}
				if scope.terminal {
					cancelled, err := selected.CancelGenericScheduleOutcome(ctx, runtimegenericschedule.CancelCommand{
						ActivationID: committed.Result.Activation.ID, Cause: "observation-proof", CancelledAt: time.Now().UTC(),
					})
					if err != nil || !cancelled.Acknowledged || cancelled.Result.Outcome != runtimegenericschedule.CancelChanged {
						t.Fatalf("cancel actual timer: %+v %v", cancelled, err)
					}
				}
			}
			var physical int64
			if err := f.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timers WHERE entity_id=$1 AND flow_instance=$2 AND owner_agent=$3", entity, path, "workflow-runtime").Scan(&physical); err != nil || physical != 3 {
				t.Fatalf("independent physical timer count: %d %v", physical, err)
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := CountWorkflowSchedulerTimersForTest(ctx, f.store, entity, path); err != nil || got != physical {
				t.Fatalf("timer history/scope changed: got=%d want=%d err=%v", got, physical, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("observation escaped original read coordinator: %+v", counts)
			}
			if got, err := CountWorkflowSchedulerTimersForTest(ctx, f.store, uuid.NewString(), path); err != nil || got != 0 {
				t.Fatalf("absent scope count: %d %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := CountWorkflowSchedulerTimersForTest(cancelled, f.store, entity, path); !errors.Is(err, context.Canceled) || got != 0 {
				t.Fatalf("cancelled read supplied evidence: %d %v", got, err)
			}
			for _, key := range [][2]string{{"", path}, {"invalid", path}, {uuid.Nil.String(), path}, {entity, ""}, {entity, " /mutation-flow/ "}} {
				if got, err := CountWorkflowSchedulerTimersForTest(ctx, f.store, key[0], key[1]); err == nil || got != 0 {
					t.Fatalf("invalid coordinate supplied evidence: %d %v", got, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := CountWorkflowSchedulerTimersForTest(ctx, f.store, entity, path); err == nil || got != 0 {
				t.Fatalf("closed owner supplied evidence: %d %v", got, err)
			}
		})
	}
}

func TestWorkflowSchedulerObservationRefusesNonNativeOwners(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := CountWorkflowSchedulerTimersForTest(context.Background(), selected, uuid.NewString(), "mutation-flow"); err == nil || got != 0 {
			t.Fatalf("invalid owner supplied evidence: %d %v", got, err)
		}
	}
}
