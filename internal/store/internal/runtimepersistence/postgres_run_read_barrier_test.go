package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestPostgresRunReadBarrierOwnsJoinedRollbackAndRefusesForeignAuthority(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := context.Background()
			if backend.name != "postgres" {
				if barrier, err := HoldPostgresRunTableReadBarrierForTest(ctx, fixture.store); err == nil || barrier != nil {
					t.Fatalf("foreign backend received a PostgreSQL barrier: %+v,%v", barrier, err)
				}
				if n, err := ReadPostgresRunOriginLockCountForTest(ctx, fixture.store); err == nil || n != 0 {
					t.Fatalf("foreign backend received origin evidence: %d,%v", n, err)
				}
				if n, err := ReadPostgresDatabaseLockCountForTest(ctx, fixture.store); err == nil || n != 0 {
					t.Fatalf("foreign backend received lock evidence: %d,%v", n, err)
				}
				return
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			barrier, err := HoldPostgresRunTableReadBarrierForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := barrier.Close(); err != nil {
					t.Error(err)
				}
			}()
			if counts := probe.Snapshot(); counts.Active != 1 || counts.ActiveByClass[transactiontest.ActiveClass{Operation: transactiontest.Other, Phase: transactiontest.PhaseOperation}] != 1 || counts.Total.WriteCommits != 0 {
				t.Fatalf("run barrier bypassed original active writer: %+v", counts)
			}
			if n, err := ReadPostgresRunOriginLockCountForTest(ctx, fixture.store); err != nil || n != 0 {
				t.Fatalf("idle origin count=%d,%v", n, err)
			}
			if n, err := ReadPostgresDatabaseLockCountForTest(ctx, fixture.store); err != nil || n != 0 {
				t.Fatalf("idle lock count=%d,%v", n, err)
			}
			for range 2 {
				if err := barrier.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.WriteCommits != 0 || counts.Total.ReadCommits != 2 || counts.Total.RollbackAttempts != 1 || counts.Total.Failed != 1 || counts.Total.CleanupFailures != 0 {
				t.Fatalf("run barrier did not join one original rollback: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if barrier, err := HoldPostgresRunTableReadBarrierForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || barrier != nil {
				t.Fatalf("cancelled barrier escaped: %+v,%v", barrier, err)
			}
			if n, err := ReadPostgresRunOriginLockCountForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || n != 0 {
				t.Fatalf("cancelled origin evidence escaped: %d,%v", n, err)
			}
			if n, err := ReadPostgresDatabaseLockCountForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || n != 0 {
				t.Fatalf("cancelled lock evidence escaped: %d,%v", n, err)
			}
			if err := fixture.store.(*PostgresStore).Close(); err != nil {
				t.Fatal(err)
			}
			if barrier, err := HoldPostgresRunTableReadBarrierForTest(ctx, fixture.store); err == nil || barrier != nil {
				t.Fatalf("closed barrier escaped: %+v,%v", barrier, err)
			}
			if n, err := ReadPostgresRunOriginLockCountForTest(ctx, fixture.store); err == nil || n != 0 {
				t.Fatalf("closed origin evidence escaped: %d,%v", n, err)
			}
			if n, err := ReadPostgresDatabaseLockCountForTest(ctx, fixture.store); err == nil || n != 0 {
				t.Fatalf("closed lock evidence escaped: %d,%v", n, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if barrier, err := HoldPostgresRunTableReadBarrierForTest(context.Background(), invalid); err == nil || barrier != nil {
			t.Fatalf("missing barrier escaped: %+v,%v", barrier, err)
		}
		if n, err := ReadPostgresRunOriginLockCountForTest(context.Background(), invalid); err == nil || n != 0 {
			t.Fatalf("missing origin evidence escaped: %d,%v", n, err)
		}
		if n, err := ReadPostgresDatabaseLockCountForTest(context.Background(), invalid); err == nil || n != 0 {
			t.Fatalf("missing lock evidence escaped: %d,%v", n, err)
		}
	}
}
