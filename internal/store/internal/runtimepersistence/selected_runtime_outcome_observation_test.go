package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedDiagnosticLineageCountsUseOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := CountRunEventOrSourceEventStorageForTest(ctx, selected, event.RunID(), event.ID()); err != nil || got != 1 {
				t.Fatalf("native exact event lost physical identity: %d/%v", got, err)
			}
			readers := []func(context.Context, any, string, string) (int, error){
				CountRunEventOrSourceEventStorageForTest,
				CountSelectedForkSourceEventLineageStorageForTest,
			}
			absent := uuid.NewString()
			for _, read := range readers {
				if got, err := read(ctx, selected, event.RunID(), absent); err != nil || got != 0 {
					t.Fatalf("absent diagnostic fact supplied evidence: %d/%v", got, err)
				}
			}
			if got, err := CountSelectedForkSourceEventLineageStorageForTest(ctx, selected, event.RunID(), event.ID()); err != nil || got != 0 {
				t.Fatalf("source event fabricated fork lineage: %d/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostic observations escaped original coordinator or collapsed cuts: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, read := range readers {
				if got, err := read(cancelled, selected, event.RunID(), absent); !errors.Is(err, context.Canceled) || got != 0 {
					t.Fatalf("cancelled diagnostic read supplied evidence: %d/%v", got, err)
				}
				for _, owner := range []any{nil, struct{}{}, &sql.DB{}} {
					if got, err := read(ctx, owner, event.RunID(), absent); err == nil || got != 0 {
						t.Fatalf("foreign/raw diagnostic owner supplied evidence: %T/%d/%v", owner, got, err)
					}
				}
				for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
					if got, err := read(ctx, selected, invalid, absent); err == nil || got != 0 {
						t.Fatalf("invalid diagnostic run supplied evidence: %q/%d/%v", invalid, got, err)
					}
					if got, err := read(ctx, selected, event.RunID(), invalid); err == nil || got != 0 {
						t.Fatalf("invalid diagnostic event supplied evidence: %q/%d/%v", invalid, got, err)
					}
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range readers {
				if got, err := read(ctx, selected, event.RunID(), absent); err == nil || got != 0 {
					t.Fatalf("closed diagnostic owner supplied evidence: %d/%v", got, err)
				}
			}
		})
	}
}

func TestSelectedCompetitionStorageCountsUseOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, context.Background()
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			readers := []func(context.Context, any, string) (int, error){
				CountClosedFirstSelectedRuntimeExecutionStorageForTest,
				CountSelectedExecutionSettledAttemptsStorageForTest,
				CountCompletionAttemptBudgetReservationsStorageForTest,
			}
			id := uuid.NewString()
			for _, read := range readers {
				if got, err := read(ctx, selected, id); err != nil || got != 0 {
					t.Fatalf("absent competition fact supplied evidence: %d/%v", got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("competition observations escaped original read owner or collapsed cuts: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, read := range readers {
				if got, err := read(cancelled, selected, id); !errors.Is(err, context.Canceled) || got != 0 {
					t.Fatalf("cancelled competition read supplied evidence: %d/%v", got, err)
				}
				for _, owner := range []any{nil, struct{}{}, &sql.DB{}} {
					if got, err := read(ctx, owner, id); err == nil || got != 0 {
						t.Fatalf("foreign or raw competition owner supplied evidence: %T/%d/%v", owner, got, err)
					}
				}
				for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
					if got, err := read(ctx, selected, invalid); err == nil || got != 0 {
						t.Fatalf("invalid competition coordinate supplied evidence: %q/%d/%v", invalid, got, err)
					}
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range readers {
				if got, err := read(ctx, selected, id); err == nil || got != 0 {
					t.Fatalf("closed competition owner supplied evidence: %d/%v", got, err)
				}
			}
		})
	}
}

func TestSelectedAuthoredHTTPCountsUseOriginalOwnerAndRefuseInvalidSurfaceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, context.Background()
			execution, surface := uuid.NewString(), uuid.NewString()
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			readers := []func(context.Context, any, string) (int, error){
				CountSelectedExecutionAttemptsStorageForTest,
				CountSelectedAuthoredHTTPAttemptsStorageForTest,
				func(ctx context.Context, owner any, id string) (int, error) {
					return CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageForTest(ctx, owner, id, surface)
				},
			}
			for _, read := range readers {
				if got, err := read(ctx, selected, execution); err != nil || got != 0 {
					t.Fatalf("absent authored HTTP fact supplied evidence: %d/%v", got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("authored HTTP reads escaped original coordinator or collapsed temporal cuts: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, read := range readers {
				if got, err := read(cancelled, selected, execution); !errors.Is(err, context.Canceled) || got != 0 {
					t.Fatalf("cancelled authored HTTP read supplied evidence: %d/%v", got, err)
				}
				for _, owner := range []any{nil, struct{}{}, &sql.DB{}} {
					if got, err := read(ctx, owner, execution); err == nil || got != 0 {
						t.Fatalf("foreign/raw authored HTTP owner supplied evidence: %T/%d/%v", owner, got, err)
					}
				}
				for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
					if got, err := read(ctx, selected, invalid); err == nil || got != 0 {
						t.Fatalf("invalid authored HTTP execution supplied evidence: %q/%d/%v", invalid, got, err)
					}
				}
			}
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				if got, err := CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageForTest(ctx, selected, execution, invalid); err == nil || got != 0 {
					t.Fatalf("invalid authored HTTP surface supplied evidence: %q/%d/%v", invalid, got, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range readers {
				if got, err := read(ctx, selected, execution); err == nil || got != 0 {
					t.Fatalf("closed authored HTTP owner supplied evidence: %d/%v", got, err)
				}
			}
		})
	}
}

func TestSelectedRuntimeOutcomeScalarsPreservePhysicalScopeAndOriginalCoordinatorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			foreign := runForkSnapshotFixtureEvent(t, ctx, selected)
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, coordinate := range []struct {
				run, event string
				want       int
			}{
				{event.RunID(), event.ID(), 1},
				{foreign.RunID(), event.ID(), 0},
				{event.RunID(), foreign.ID(), 0},
				{event.RunID(), uuid.NewString(), 0},
			} {
				if count, err := CountRunExactEventStorageForTest(ctx, selected, coordinate.run, coordinate.event); err != nil || count != coordinate.want {
					t.Fatalf("physical event count changed scope: %d/%v want %d", count, err, coordinate.want)
				}
			}
			if count, err := CountSelectedContractBindingStorageForTest(ctx, selected); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated a selected binding: %d/%v", count, err)
			}
			if count, err := CountSelectedRuntimeExecutionStorageForTest(ctx, selected, event.RunID()); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated runtime execution: %d/%v", count, err)
			}
			if count, err := CountRunFlowInstanceStorageForTest(ctx, selected, event.RunID()); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated construction: %d/%v", count, err)
			}
			if state, err := ReadSelectedRuntimeExecutionStateStorageForTest(ctx, selected, event.RunID()); !errors.Is(err, sql.ErrNoRows) || state != "" {
				t.Fatalf("source run status acquired execution-state authority: %q/%v", state, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical outcome reads escaped original coordinator: %+v", counts)
			}
		})
	}
}
