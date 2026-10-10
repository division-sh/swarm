package storetest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresObservationTimeConsumesExactReadOwner(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	at, err := ReadPostgresObservationTime(context.Background(), selected)
	if err != nil || at.IsZero() || at.Location() != time.UTC {
		t.Fatalf("native PostgreSQL observation time=%v err=%v", at, err)
	}
	if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
		t.Fatalf("clock observation escaped its exact read owner: %+v", proof)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if at, err := ReadPostgresObservationTime(ctx, selected); !errors.Is(err, context.Canceled) || !at.IsZero() {
		t.Fatalf("canceled observation yielded a clock fact: %v %v", at, err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	if at, err := ReadPostgresObservationTime(context.Background(), selected); err == nil || !at.IsZero() {
		t.Fatalf("closed owner yielded a clock fact: %v %v", at, err)
	}
}

func TestPostgresObservationTimeRejectsForeignClockOwners(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}, StartSQLiteRuntimeStore(t)} {
		if at, err := ReadPostgresObservationTime(context.Background(), selected); err == nil || !at.IsZero() {
			t.Fatalf("foreign clock owner %T yielded a PostgreSQL clock: %v %v", selected, at, err)
		}
	}
}
