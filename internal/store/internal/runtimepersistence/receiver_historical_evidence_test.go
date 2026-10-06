package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverHistoricalEvidenceUsesExactCanonicalCutBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			plan, err := fixture.store.(forkWorkflowOwnershipStore).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID, At: request.At})
			if err != nil || len(plan.Entities) != 1 || plan.ForkPoint.Revision <= 0 {
				t.Fatalf("canonical creation cut: %+v %v", plan, err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: request.SourceRunID, Route: flowidentity.StoredRoute(".", request.SourceRunID, request.SourceRunID)}
			entity, revision := plan.Entities[0].EntityID, plan.ForkPoint.Revision
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadReceiverHistoricalEntityStateForTest(ctx, fixture.store, owner, entity, revision)
			if err != nil || !reflect.DeepEqual(got, plan.Entities[0]) {
				t.Fatalf("historical observation bypassed canonical reconstruction: got=%+v want=%+v err=%v", got, plan.Entities[0], err)
			}
			got.Fields["marker"] = "not persisted"
			again, err := ReadReceiverHistoricalEntityStateForTest(ctx, fixture.store, owner, entity, revision)
			if err != nil || !reflect.DeepEqual(again, plan.Entities[0]) {
				t.Fatalf("detached historical state changed its fixed cut: %+v %v", again, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("historical evidence escaped original read owner: %+v", counts)
			}
			for _, which := range []string{"run", "route", "entity", "future", "zero"} {
				wrong, id, cut := owner, entity, revision
				switch which {
				case "run":
					wrong.RunID = uuid.NewString()
				case "route":
					wrong.Route = flowidentity.StoredRoute("other", "ti-foreign", "other/ti-foreign")
				case "entity":
					id = uuid.NewString()
				case "future":
					cut += 1000000
				case "zero":
					cut = 0
				}
				if got, err := ReadReceiverHistoricalEntityStateForTest(ctx, fixture.store, wrong, id, cut); err == nil || !reflect.DeepEqual(got, runfork.RunForkEntityState{}) {
					t.Fatalf("foreign/missing %s returned partial history: %+v %v", which, got, err)
				}
			}
		})
	}
}

func TestReceiverHistoricalEvidenceRefusesInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	owner := flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.StoredRoute("review", "ti-history", "review/ti-history")}
	entity := uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadReceiverHistoricalEntityStateForTest(context.Background(), selected, owner, entity, 1); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, runfork.RunForkEntityState{}) {
			t.Fatalf("invalid owner returned partial or absent history: %+v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if got, err := ReadReceiverHistoricalEntityStateForTest(ctx, fixture.store, owner, entity, 1); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, runfork.RunForkEntityState{}) {
				t.Fatalf("cancelled history lost its error: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReceiverHistoricalEntityStateForTest(context.Background(), fixture.store, owner, entity, 1); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, runfork.RunForkEntityState{}) {
				t.Fatalf("closed owner returned partial or absent history: %+v %v", got, err)
			}
		})
	}
}
