package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRootAccumulatorWireFaultRespectsOriginalSQLiteWriterAdmission(t *testing.T) {
	f, _ := newCompetingRevisionPairFixture(t, "sqlite")
	owner := f.store.(*SQLiteRuntimeStore)
	run := correlation.RunIDFromContext(f.ctx)
	before := competingRevisionPairSnapshot(t, f)
	probe, restore, err := InstallTransactionProbeForTest(owner, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- owner.backend.RunTransaction(ctx, "root accumulator admission holder", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	holderJoined := false
	defer func() {
		unblock()
		if !holderJoined {
			if err := <-holderDone; err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	faultCtx, cancelFault := context.WithCancel(ctx)
	faultDone := make(chan error, 1)
	go func() {
		changed, err := SetRootFlowAccumulatorWireForTest(faultCtx, owner, run, []byte("[]"))
		if changed != 0 {
			err = errors.Join(err, errors.New("queued root fault supplied write evidence"))
		}
		faultDone <- err
	}()
	joined := false
	defer func() {
		cancelFault()
		unblock()
		if !joined {
			<-faultDone
		}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for owner.backend.QueuedWritersForTest() != 1 {
		select {
		case err := <-faultDone:
			joined = true
			t.Fatalf("root fault escaped the original writer admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Total.WriteCommits != 0 || counts.Active != 1 {
		t.Fatalf("queued root fault began independent storage work: %+v", counts)
	}
	cancelFault()
	select {
	case err := <-faultDone:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued root cancellation lost its cause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if owner.backend.QueuedWritersForTest() != 0 {
		t.Fatal("cancelled root fault retained writer admission")
	}
	unblock()
	if err := <-holderDone; err != nil {
		holderJoined = true
		t.Fatal(err)
	}
	holderJoined = true
	// This fixture now lawfully constructs both the root and its child. A foreign
	// run has neither; the separate exact-root control below preserves the child.
	if changed, err := SetRootFlowAccumulatorWireForTest(ctx, owner, uuid.NewString(), []byte("[]")); err == nil || changed != 0 {
		t.Fatalf("root-only fault borrowed another run's construction: %d %v", changed, err)
	}
	if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 2 || counts.Total.WriteCommits != 1 || counts.Total.Failed != 1 || counts.Total.RollbackAttempts != 1 {
		t.Fatalf("absent root did not roll back behind the exact original writer: %+v", counts)
	}
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("queued or absent-root fault changed child, history, data or clocks")
	}
	restore()
	original, err := ReadRootFlowAccumulatorStorageForTest(f.ctx, owner, run)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := SetRootFlowAccumulatorWireForTest(f.ctx, owner, run, []byte("[]")); err != nil || changed != 1 {
		t.Fatalf("exact constructed root fault: changed=%d error=%v", changed, err)
	}
	if changed, err := SetRootFlowAccumulatorWireForTest(f.ctx, owner, run, original); err != nil || changed != 1 {
		t.Fatalf("exact constructed root restoration: changed=%d error=%v", changed, err)
	}
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("exact root fault or restoration changed the child, another field, history or clocks")
	}
}

func TestRootAccumulatorWirePortsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertRootAccumulatorWireUnavailable(t, selected)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, run := range []string{"", "invalid", uuid.Nil.String()} {
				if changed, err := SetRootFlowAccumulatorWireForTest(context.Background(), f.store, run, []byte("[]")); err == nil || changed != 0 {
					t.Fatalf("invalid run supplied a fault receipt: %d %v", changed, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertRootAccumulatorWireUnavailable(t, f.store)
		})
	}
}

func assertRootAccumulatorWireUnavailable(t *testing.T, selected any) {
	t.Helper()
	run := uuid.NewString()
	if changed, err := SetRootFlowAccumulatorWireForTest(context.Background(), selected, run, []byte("[]")); err == nil || changed != 0 {
		t.Fatalf("unavailable owner manufactured root fault: %d %v", changed, err)
	}
	if got, err := ReadRootFlowAccumulatorStorageForTest(context.Background(), selected, run); err == nil || got != nil {
		t.Fatalf("unavailable owner manufactured retained accumulator: %s %v", got, err)
	}
}
