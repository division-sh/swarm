package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestEventChainDepthFixtureReachesNativeConstraintAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "chain.depth.fixture", "runtime", "", []byte(`{}`), 3, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			err = SetEventChainDepthForTest(ctx, fixture.store, event.ID(), -1)
			if backend.name == "postgres" {
				var native *pq.Error
				if !errors.As(err, &native) || native.Code != "23514" {
					t.Fatalf("negative depth did not reach native CHECK: %T %v", err, err)
				}
			} else {
				var native interface{ Code() int }
				if !errors.As(err, &native) || native.Code() != 275 {
					t.Fatalf("negative depth did not reach SQLite CHECK: %T %v", err, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.RollbackAttempts != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("constraint rejection escaped original writer: %+v", counts)
			}
			assertDepth := func(want int) {
				t.Helper()
				out, exists, err := fixture.store.(preparedPublishEventReadbackStore).LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !exists || out.Event.Event().ChainDepth() != want {
					t.Fatalf("physical depth=%+v,err=%v,want%d", out, err, want)
				}
			}
			assertDepth(3)
			if err := SetEventChainDepthForTest(ctx, fixture.store, event.ID(), 4); err != nil {
				t.Fatal(err)
			}
			assertDepth(4)
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Active != 0 {
				t.Fatalf("exact update escaped original writer: %+v", counts)
			}
			if err := SetEventChainDepthForTest(ctx, fixture.store, uuid.NewString(), 1); err == nil {
				t.Fatal("missing event accepted mutation")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := SetEventChainDepthForTest(cancelled, fixture.store, event.ID(), 2); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fault=%v", err)
			}
			if err := SetEventChainDepthForTest(ctx, fixture.store, "invalid", 2); err == nil {
				t.Fatal("invalid event accepted mutation")
			}
			assertDepth(4)
			switch selected := fixture.store.(type) {
			case *PostgresStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := SetEventChainDepthForTest(ctx, fixture.store, event.ID(), 2); err == nil {
				t.Fatal("closed owner accepted mutation")
			}
		})
	}
	for _, missing := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if err := SetEventChainDepthForTest(context.Background(), missing, uuid.NewString(), -1); err == nil {
			t.Fatal("missing owner accepted mutation")
		}
	}
}
