package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPausedRunControlFaultPreservesRunDeliveryRevisionsAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, missing := range []bool{false, true} {
			name := "contradictory"
			if missing {
				name = "missing"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				run, sibling := uuid.NewString(), uuid.NewString()
				for _, id := range []string{run, sibling} {
					event := eventtest.RunCreatingRootIngress(uuid.NewString(), "control.fault", "runtime", "", []byte(`{}`), 0, id, "", events.EventEnvelope{}, time.Now().UTC())
					if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{"worker"}); err != nil {
						t.Fatal(err)
					}
					out, err := fixture.store.(runtimeruncontrol.Store).PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: id, Reason: "fixture", Now: event.CreatedAt()})
					if err != nil || !out.Acknowledged {
						t.Fatalf("pause=%+v,%v", out, err)
					}
				}
				before, err := ReadRunStopStorageForTest(ctx, fixture.store, run)
				if err != nil {
					t.Fatal(err)
				}
				foreign, err := ReadRunStopStorageForTest(ctx, fixture.store, sibling)
				if err != nil {
					t.Fatal(err)
				}
				if before.Status != "paused" || before.Control != "paused" || before.Pending != 1 {
					t.Fatalf("fault prerequisite=%+v", before)
				}
				probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				apply := ContradictPausedRunControlForTest
				if missing {
					apply = RemovePausedRunControlForTest
				}
				if err := apply(ctx, fixture.store, run); err != nil {
					t.Fatal(err)
				}
				if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
					t.Fatalf("control fault escaped original writer: %+v", counts)
				}
				after, err := ReadRunStopStorageForTest(ctx, fixture.store, run)
				if err != nil {
					t.Fatal(err)
				}
				want := before
				want.Control = "running"
				if missing {
					want.Control = ""
				}
				if after != want {
					t.Fatalf("control fault changed run/delivery/revision fields: before=%+v,after=%+v,want=%+v", before, after, want)
				}
				if err := apply(ctx, fixture.store, uuid.NewString()); err == nil {
					t.Fatal("wrong run accepted a control fault")
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if err := apply(cancelled, fixture.store, sibling); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled control fault=%v", err)
				}
				if got, err := ReadRunStopStorageForTest(ctx, fixture.store, sibling); err != nil || got != foreign {
					t.Fatalf("control fault changed sibling: %+v,%v;want%+v", got, err, foreign)
				}
				if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Active != 0 {
					t.Fatalf("rejected control faults committed: %+v", counts)
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
				if err := apply(ctx, fixture.store, sibling); err == nil {
					t.Fatal("closed owner accepted a control fault")
				}
			})
		}
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if err := RemovePausedRunControlForTest(context.Background(), invalid, uuid.NewString()); err == nil {
			t.Fatal("missing owner accepted missing-control fault")
		}
		if err := ContradictPausedRunControlForTest(context.Background(), invalid, uuid.NewString()); err == nil {
			t.Fatal("missing owner accepted contradictory-control fault")
		}
	}
}
