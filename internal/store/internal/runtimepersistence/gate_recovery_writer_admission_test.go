package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestGateRecoveryFaultsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	for _, fault := range []struct {
		name  string
		apply func(context.Context, any, string) error
	}{
		{"creation_trigger", func(ctx context.Context, selected any, _ string) error {
			return InstallGateProposedEffectCreationFailureForTest(ctx, selected)
		}},
		{"attempt", func(ctx context.Context, selected any, event string) error {
			return SetGateDecisionRouteAttemptFaultForTest(ctx, selected, event, 2, at)
		}},
		{"due", func(ctx context.Context, selected any, event string) error {
			return SetGateDecisionRouteDueFaultForTest(ctx, selected, event, at)
		}},
	} {
		t.Run(fault.name, func(t *testing.T) { assertGateFaultWriterAdmission(t, fault.apply) })
	}
}

func assertGateFaultWriterAdmission(t *testing.T, apply func(context.Context, any, string) error) {
	t.Helper()
	f, _ := newWorkflowTargetConstructionFixture(t, "sqlite")
	owner := f.store.(*SQLiteRuntimeStore)
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	probe, restore, err := InstallTransactionProbeForTest(owner, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	entered, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- owner.backend.RunTransaction(ctx, "gate fault exact writer-holder proof", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	holderJoined := false
	defer func() {
		unblock()
		if !holderJoined {
			if err := <-holderDone; err != nil {
				t.Errorf("joined holder: %v", err)
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
	go func() { faultDone <- apply(faultCtx, owner, uuid.NewString()) }()
	faultJoined := false
	defer func() {
		cancelFault()
		unblock()
		if !faultJoined {
			<-faultDone
		}
	}()
	awaitGateFaultWriterQueue(t, ctx, owner, faultDone, &faultJoined)
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Total.WriteCommits != 0 || counts.Active != 1 {
		t.Fatalf("queued gate fault began another transaction: %+v", counts)
	}
	cancelFault()
	select {
	case err := <-faultDone:
		faultJoined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued gate fault cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if owner.backend.QueuedWritersForTest() != 0 {
		t.Fatal("cancelled gate fault retained writer admission")
	}
	unblock()
	err = <-holderDone
	holderJoined = true
	if err != nil {
		t.Fatal(err)
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Active != 0 {
		t.Fatalf("joined gate fault/holder escaped original coordinator: %+v", counts)
	}
}

func awaitGateFaultWriterQueue(t *testing.T, ctx context.Context, owner *SQLiteRuntimeStore, done <-chan error, joined *bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for owner.backend.QueuedWritersForTest() != 1 {
		select {
		case err := <-done:
			*joined = true
			t.Fatalf("gate fault bypassed exact writer admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
