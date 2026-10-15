package runtimepersistence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestRecoveryRunFaultCancellationWaitsBehindExactSQLiteWriter(t *testing.T) {
	selected := storetest.StartSQLiteRuntimeStore(t)
	run, target := uuid.NewString(), uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), run)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: run})
	storetest.SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, run, target, []byte(`{"message":"recovery fault control","log_level":"error"}`), time.Now().UTC())
	release, err := storetest.HoldUnstampedPipelineAdmissionTransaction(ctx, selected, run)
	if err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	faultCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	joined := false
	defer func() {
		cancel()
		if err := release(); err != nil {
			t.Errorf("join held native SQLite writer: %v", err)
		}
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("join cancelled recovery fault timed out")
			}
		}
	}()
	go func() {
		defer close(done)
		done <- storetest.ClearEventRunForRecovery(faultCtx, selected, target)
	}()
	waitRecoveryRunFaultQueued(t, selected, done)
	cancel()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued fault cancellation=%v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fault did not settle cancellation while original writer remained held")
	}
	if waiting := private.RecoveryRunFaultQueuedWritersForTest(selected); waiting != 0 {
		t.Fatalf("cancelled fault retained original writer admission: %d", waiting)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if row, err := storetest.ReadPublicationEventIdentityStorage(ctx, selected, target); row.RunID != run || err != nil {
		t.Fatalf("cancelled admission performed the corruption: %+v err=%v", row, err)
	}
	if receipt := probe.Snapshot(); receipt.Active != 0 || receipt.Total.WriteCommits != 0 {
		t.Fatalf("cancelled fault committed or leaked work: %+v", receipt)
	}
}

func waitRecoveryRunFaultQueued(t *testing.T, selected *private.SQLiteRuntimeStore, done <-chan error) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for private.RecoveryRunFaultQueuedWritersForTest(selected) != 1 {
		select {
		case err := <-done:
			t.Fatalf("fault escaped exact writer admission before cancellation: %v", err)
		case <-deadline.C:
			t.Fatal("fault never entered the original writer queue")
		case <-tick.C:
		}
	}
}
