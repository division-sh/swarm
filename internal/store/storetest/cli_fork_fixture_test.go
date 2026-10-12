package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestCLIForkObservationPortsUseOriginalOwnerAndRefuseProvisionalEvidence(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	ctx := context.Background()
	run, source, event := uuid.NewString(), uuid.NewString(), uuid.NewString()
	readers := []func(context.Context, any) (int, error){
		func(ctx context.Context, owner any) (int, error) {
			return CountSelectedForkExactExecutionStorage(ctx, owner, run, source, event)
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountSelectedForkDeliveryReplayStorage(ctx, owner, run)
		},
		func(ctx context.Context, owner any) (int, error) { return CountRunDeliveryStorage(ctx, owner, run) },
		func(ctx context.Context, owner any) (int, error) {
			return CountPostgresForkTypedRuntimeDiagnostic(ctx, owner, run, event, "exact-owner")
		},
	}
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	for _, read := range readers {
		if count, err := read(ctx, selected); err != nil || count != 0 {
			t.Fatalf("absent CLI fact manufactured evidence: %d/%v", count, err)
		}
	}
	if rows, err := ReadPostgresGlobalRuntimeDiagnosticStorage(ctx, selected); err != nil || rows != "[]" {
		t.Fatalf("empty global diagnostics changed: %q/%v", rows, err)
	}
	if counts := probe.Snapshot(); counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
		t.Fatalf("CLI observations escaped original read owner: %+v", counts)
	}
	if mode, err := ReadSelectedForkBindingModeStorage(ctx, selected, run, source, event); !errors.Is(err, sql.ErrNoRows) || mode != "" {
		t.Fatalf("absent binding supplied a mode: %q/%v", mode, err)
	}
	if state, err := ReadCLIMaterializedEntityState(ctx, selected, run, event); !errors.Is(err, sql.ErrNoRows) || state != "" {
		t.Fatalf("absent entity supplied state: %q/%v", state, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	var nilPostgres *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	for _, read := range readers {
		if count, err := read(cancelled, selected); !errors.Is(err, context.Canceled) || count != 0 {
			t.Fatalf("cancelled observation supplied evidence: %d/%v", count, err)
		}
		for _, owner := range []any{nil, nilPostgres, nilSQLite, &sql.DB{}, &sql.Tx{}} {
			if count, err := read(ctx, owner); err == nil || count != 0 {
				t.Fatalf("foreign %T supplied CLI facts: %d/%v", owner, count, err)
			}
		}
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range readers {
		if count, err := read(ctx, selected); err == nil || count != 0 {
			t.Fatalf("closed owner supplied CLI facts: %d/%v", count, err)
		}
	}
}
