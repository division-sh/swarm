package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestServedEventRunIdentityObservationRemainsPhysicalAfterEnvelopeCorruptionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claim := seedDeliveryRecoveryClaim(t, fixture, ctx)
			id, run := claim.Snapshot.EventID, claim.Claim.RunID()
			original, err := LoadCanonicalEventRecordForTest(ctx, fixture.store, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := CorruptReplayEventTargetRouteForTest(ctx, fixture.store, id); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := RestoreReplayEventTargetRouteForTest(ctx, fixture.store, original); err != nil {
					t.Error(err)
				}
			}()
			if _, _, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, id); !errors.Is(err, eventrecord.ErrCorrupt) {
				t.Fatalf("fault did not make full event decoding refuse: %v", err)
			}
			requireServedIdentityReadScopeAndImmutability(t, ctx, fixture.store, id, run)
			if got, err := ReadServedEventRunIDStorageForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || got != "" {
				t.Fatalf("absent event fabricated run identity: %s/%v", got, err)
			}
			full, err := ReadServedRunDebugSummaryForTest(ctx, fixture.store, run)
			if err != nil {
				t.Fatal(err)
			}
			one, err := ReadServedRuntimeLogDebugSummaryForTest(ctx, fixture.store, run)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(one, "runtime_logs: ") || !strings.HasSuffix(full, one) {
				t.Fatalf("fixed log section changed contents/order/error rendering: %q/%q", one, full)
			}
		})
	}
}

func requireServedIdentityReadScopeAndImmutability(t *testing.T, ctx context.Context, selected any, id, run string) {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadServedEventRunIDStorageForTest(ctx, selected, id)
	counts := probe.Snapshot()
	restore()
	if err != nil || got != run || counts.Total.BeginAttempts != 1 || counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Total.Failed != 0 || counts.Active != 0 {
		t.Fatalf("physical identity changed scope/role: got=%s want=%s counters=%+v err=%v", got, run, counts, err)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("diagnostic identity read changed storage: %v", err)
	}
}

func TestServedEventRunIdentityAndLogDiagnosticRefuseRawInvalidCancelledAndClosedBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadServedEventRunIDStorageForTest(context.Background(), selected, uuid.NewString()); err == nil || got != "" {
			t.Fatalf("unowned identity became evidence: %s/%v", got, err)
		}
		if got, err := ReadServedRuntimeLogDebugSummaryForTest(context.Background(), selected, uuid.NewString()); err == nil || got != "" {
			t.Fatalf("unowned log section became evidence: %s/%v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"invalid", "cancelled", "closed"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture, ctx := backend.open(t), context.Background()
				id := uuid.NewString()
				switch cut {
				case "invalid":
					id = "not-an-identity"
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "closed":
					if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				}
				if got, err := ReadServedEventRunIDStorageForTest(ctx, fixture.store, id); err == nil || got != "" {
					t.Fatalf("failed %s lookup produced partial identity: %s/%v", cut, got, err)
				}
				if got, err := ReadServedRuntimeLogDebugSummaryForTest(ctx, fixture.store, id); (cut != "closed" && err == nil) || got == "runtime_logs: []" {
					t.Fatalf("failed %s log lookup fabricated empty success: %q/%v", cut, got, err)
				}
			})
		}
	}
}
