package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func closedReceiverObservationCases(t *testing.T, runID string) []fanOutSemanticReadCase {
	t.Helper()
	node := identitytest.RootNode(t, "bridge-node").Key()
	return []fanOutSemanticReadCase{
		{"ready", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverReadinessStorageForTest(ctx, selected, runID, node)
		}, ClosedReceiverReadinessStorage{}, false},
		{"authority", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverAuthorityStorageForTest(ctx, selected)
		}, []ClosedReceiverAuthorityStorage(nil), false},
		{"completed", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverEmissionStorageForTest(ctx, selected, runID, "task.completed")
		}, ClosedReceiverEmissionStorage{}, true},
		{"finalized", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverEmissionStorageForTest(ctx, selected, runID, "task.finalized")
		}, ClosedReceiverEmissionStorage{}, true},
		{"pipeline", func(ctx context.Context, selected any) (any, error) {
			return CountClosedReceiverPipelineSuccessStorageForTest(ctx, selected, runID)
		}, 0, false},
		{"deliveries", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverDeliveryStorageForTest(ctx, selected, runID)
		}, []ClosedReceiverDeliveryStorage(nil), false},
		{"receipts", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverReceiptStorageForTest(ctx, selected, runID)
		}, []ClosedReceiverReceiptStorage(nil), false},
		{"agents", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverAgentStorageForTest(ctx, selected)
		}, []ClosedReceiverAgentStorage(nil), false},
		{"routes", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverAgentRouteStorageForTest(ctx, selected, runID)
		}, []ClosedReceiverAgentRouteStorage(nil), false},
		{"logs", func(ctx context.Context, selected any) (any, error) {
			return ReadClosedReceiverRuntimeLogStorageForTest(ctx, selected, runID)
		}, []string(nil), false},
	}
}

func TestClosedReceiverObservationsKeepAtomicReadinessAndTenIndependentNativeCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range closedReceiverObservationCases(t, uuid.NewString()) {
				got, err := tc.read(f.ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("absent %s borrowed storage or changed absence: %+v %v", tc.name, got, err)
				}
				if tc.absentError && !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("absent %s changed missing-row refusal: %v", tc.name, err)
				}
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 10 || counts.Total.ReadCommits != 8 || counts.Total.WriteCommits != 0 {
				t.Fatalf("observation split atomic readiness, collapsed independent cuts or wrote: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("observation changed complete storage or lifecycle clocks")
			}
		})
	}
}

func TestClosedReceiverObservationsRefuseRawCancelledInvalidAndClosedOwnershipBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, cause error) {
		for _, tc := range closedReceiverObservationCases(t, uuid.NewString()) {
			got, err := tc.read(ctx, owner)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			for _, tc := range closedReceiverObservationCases(t, "invalid") {
				if tc.name == "authority" || tc.name == "agents" {
					continue // The two original witnesses are deliberately global.
				}
				if got, err := tc.read(f.ctx, f.store); err == nil || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("invalid %s returned evidence: %+v %v", tc.name, got, err)
				}
			}
			if got, err := ReadClosedReceiverEmissionStorageForTest(f.ctx, f.store, state.Identity.RunID, "foreign.event"); err == nil || got != (ClosedReceiverEmissionStorage{}) {
				t.Fatalf("foreign event selector returned evidence: %+v %v", got, err)
			}
			if got, err := ReadClosedReceiverReadinessStorageForTest(f.ctx, f.store, state.Identity.RunID, ""); err == nil || got != (ClosedReceiverReadinessStorage{}) {
				t.Fatalf("missing bridge node returned evidence: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			refuse(cancelled, f.store, context.Canceled)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, nil)
		})
	}
}
