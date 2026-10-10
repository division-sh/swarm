package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunStopStoragePreservesExactRunCountsAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "stop.storage", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			sibling := eventtest.RunCreatingRootIngress(uuid.NewString(), "stop.storage.sibling", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, event.CreatedAt())
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{"worker", "other-worker"}); err != nil {
				t.Fatal(err)
			}
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, sibling, []string{"foreign-worker", "foreign-other", "foreign-third"}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			before, err := ReadRunStopStorageForTest(ctx, fixture.store, event.RunID())
			if err != nil || before.Status != "running" || before.Control != "" || before.Pending != 2 || before.Revisions <= 0 {
				t.Fatalf("initial exact stop storage=%+v,err=%v", before, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("stop storage escaped original read snapshot: %+v", counts)
			}
			transition, err := fixture.store.(runtimeruncontrol.Store).PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: event.RunID(), Reason: "fixture", ControlledBy: "test", Now: time.Now().UTC()})
			if err != nil || !transition.Acknowledged {
				t.Fatalf("pause=%+v,err=%v", transition, err)
			}
			after, err := ReadRunStopStorageForTest(ctx, fixture.store, event.RunID())
			if err != nil || after.Status != "paused" || after.Control != "paused" || after.Pending != before.Pending || after.Revisions != before.Revisions {
				t.Fatalf("paused exact stop storage=%+v,err=%v;before=%+v", after, err, before)
			}
			foreign, err := ReadRunStopStorageForTest(ctx, fixture.store, sibling.RunID())
			if err != nil || foreign.Status != "running" || foreign.Control != "" || foreign.Pending != 3 {
				t.Fatalf("sibling storage=%+v,err=%v", foreign, err)
			}
			if out, err := ReadRunStopStorageForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || out != (RunStopStorage{}) {
				t.Fatalf("missing run returned evidence: %+v,%v", out, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if out, err := ReadRunStopStorageForTest(cancelled, fixture.store, event.RunID()); !errors.Is(err, context.Canceled) || out != (RunStopStorage{}) {
				t.Fatalf("cancelled read returned evidence: %+v,%v", out, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if out, err := ReadRunStopStorageForTest(ctx, fixture.store, event.RunID()); err == nil || out != (RunStopStorage{}) {
				t.Fatalf("closed read returned evidence: %+v,%v", out, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if out, err := ReadRunStopStorageForTest(context.Background(), invalid, uuid.NewString()); err == nil || out != (RunStopStorage{}) {
			t.Fatalf("missing read returned evidence: %+v,%v", out, err)
		}
	}
}
