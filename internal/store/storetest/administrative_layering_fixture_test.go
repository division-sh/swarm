package storetest

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/store"
)

func TestAdministrativeLayeringPortsRefuseForeignCancelledAndClosedOwners(t *testing.T) {
	ctx := context.Background()
	actions := administrativeLayeringOwnershipActions()
	var nilPG *store.PostgresStore
	for _, foreign := range []any{nil, nilPG, &sql.DB{}, &sql.Tx{}, struct{}{}, StartSQLiteRuntimeStore(t)} {
		for _, action := range actions {
			if zero, err := action(ctx, foreign); !zero || err == nil {
				t.Fatalf("foreign %T supplied administrative ownership/evidence: %t/%v", foreign, zero, err)
			}
		}
	}
	selected := StartPostgresRuntimeStore(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, action := range actions {
		if zero, err := action(canceled, selected); !zero || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled owner supplied administrative ownership/evidence: %t/%v", zero, err)
		}
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, action := range actions {
		if zero, err := action(ctx, selected); !zero || err == nil {
			t.Fatalf("closed owner supplied administrative ownership/evidence: %t/%v", zero, err)
		}
	}
	var empty PostgresAdministrativeSurroundingRead
	if value, err := empty.ReadAfterLeaseRelease(ctx); value != 0 || err == nil {
		t.Fatalf("uninitialized read supplied evidence: %d/%v", value, err)
	}
	if err := empty.Close(); err == nil {
		t.Fatal("uninitialized read reported joined ownership")
	}
}

func administrativeLayeringOwnershipActions() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			return true, LimitPostgresAdministrativeFixturePool(ctx, selected)
		},
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresAdministrativeUnrelatedProgress(ctx, selected)
			return value == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			read, err := HoldPostgresAdministrativeSurroundingRead(ctx, selected)
			if read != nil {
				err = errors.Join(err, read.Close())
			}
			return read == nil, err
		},
	}
}

func TestAdministrativeSurroundingReadPreservesExactNativePoolAndJoinedRollback(t *testing.T) {
	ctx := context.Background()
	selected := StartPostgresRuntimeStore(t)
	if err := LimitPostgresAdministrativeFixturePool(ctx, selected); err != nil {
		t.Fatal(err)
	}
	collector := CollectTransactions(t, selected, TransactionProbeOptions{})
	read, err := HoldPostgresAdministrativeSurroundingRead(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	pool := ObserveSelectedPool(t, selected)
	if pool.MaxOpenConnections != 1 || pool.InUse != 1 || collector.Snapshot().Active != 1 {
		t.Fatalf("surrounding read did not hold original one-slot pool: %+v/%+v", pool, collector.Snapshot())
	}
	lease, acquired, err := selected.AcquireDestructiveReset(ctx)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("separate operation lease: %t/%v", acquired, err)
	}
	defer func() {
		if err := lease.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	if pool := ObserveSelectedPool(t, selected); pool.MaxOpenConnections != 2 || pool.InUse != 2 {
		t.Fatalf("native operation lease did not retain independent capacity: %+v", pool)
	}
	if err := LimitPostgresAdministrativeFixturePool(ctx, selected); err == nil {
		t.Fatal("fixture resized retained native operation capacity")
	}
	if pool := ObserveSelectedPool(t, selected); pool.MaxOpenConnections != 2 {
		t.Fatalf("refused fixture resize changed retained capacity: %+v", pool)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if value, err := read.ReadAfterLeaseRelease(ctx); err != nil || value != 1 {
		t.Fatalf("native lease release settled surrounding transaction: %d/%v", value, err)
	}
	if pool := ObserveSelectedPool(t, selected); pool.MaxOpenConnections != 1 || pool.InUse != 1 {
		t.Fatalf("lease release did not preserve original surrounding ownership: %+v", pool)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := read.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if counts := collector.Snapshot(); counts.Active != 0 || counts.Total.Begun != 1 || counts.Total.WriteCommits != 0 {
		t.Fatalf("surrounding rollback did not join its exact transaction: %+v", counts)
	}
	if pool := ObserveSelectedPool(t, selected); pool.MaxOpenConnections != 1 || pool.InUse != 0 {
		t.Fatalf("joined surrounding read retained pool work: %+v", pool)
	}
	if value, err := read.ReadAfterLeaseRelease(ctx); value != 0 || err == nil {
		t.Fatalf("closed surrounding read replayed evidence: %d/%v", value, err)
	}
	JoinReporterTransactions(t, collector)
}

func TestAdministrativeSurroundingReadCancellationAndEarlyCloseJoinNativeOwner(t *testing.T) {
	for _, cut := range []string{"cancel-before-verify", "close-before-verify"} {
		t.Run(cut, func(t *testing.T) {
			selected := StartPostgresRuntimeStore(t)
			if err := LimitPostgresAdministrativeFixturePool(context.Background(), selected); err != nil {
				t.Fatal(err)
			}
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			read, err := HoldPostgresAdministrativeSurroundingRead(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := read.Close(); err != nil && err != context.Canceled {
					t.Errorf("joined early-failure cleanup: %v", err)
				}
			}()
			if cut == "cancel-before-verify" {
				cancel()
				if value, err := read.ReadAfterLeaseRelease(context.Background()); value != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled surrounding read supplied evidence: %d/%v", value, err)
				}
				if err := read.Close(); err != nil && err != context.Canceled {
					t.Fatalf("joined cancellation: %v", err)
				}
			} else if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			if value, err := read.ReadAfterLeaseRelease(context.Background()); value != 0 || err == nil {
				t.Fatalf("closed surrounding read supplied evidence: %d/%v", value, err)
			}
			if counts := collector.Snapshot(); counts.Active != 0 || counts.Total.Begun != 1 || counts.Total.WriteCommits != 0 {
				t.Fatalf("aborted witness escaped its joined native transaction: %+v", counts)
			}
			if pool := ObserveSelectedPool(t, selected); pool.InUse != 0 || pool.MaxOpenConnections != 1 {
				t.Fatalf("aborted witness retained original capacity: %+v", pool)
			}
			JoinReporterTransactions(t, collector)
		})
	}
}
