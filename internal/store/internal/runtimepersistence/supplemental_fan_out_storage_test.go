package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func supplementalFanOutStorageCases(run, event string) []fanOutSemanticReadCase {
	key := fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: event,
		ElementRef: contracts.FanOutElementRef{FlowPath: "physical", Family: "fan_out", SemanticPath: "physical.source"}}
	return []fanOutSemanticReadCase{
		{"committed_counts", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalCommittedOutcomeCardinalityForTest(ctx, selected, run)
		}, SupplementalOutcomeCardinalityStorage{}, false},
		{"global_outcomes", func(ctx context.Context, selected any) (any, error) {
			return CountSupplementalGlobalOutcomesForTest(ctx, selected)
		}, 0, false},
		{"global_intents", func(ctx context.Context, selected any) (any, error) {
			return CountServingGlobalIntentsForTest(ctx, selected)
		}, 0, false},
		{"global_capacity", func(ctx context.Context, selected any) (any, error) {
			return ReadServingGlobalCapacityForTest(ctx, selected)
		}, ServingCapacityStorage{}, true},
		{"strict_global_cardinality", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalGlobalIntentCardinalityForTest(ctx, selected)
		}, SupplementalIntentCardinalityStorage{}, true},
		{"strict_global_claim_cursor", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalGlobalClaimCursorForTest(ctx, selected)
		}, SupplementalClaimCursorStorage{}, true},
		{"global_progress", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalGlobalProgressForTest(ctx, selected)
		}, SupplementalProgressStorage{}, false},
		{"global_pressure", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalGlobalPressureForTest(ctx, selected)
		}, SupplementalPressureStorage{}, false},
		{"expired_claim", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalExpiredClaimForTest(ctx, selected, key)
		}, SupplementalExpiredClaimStorage{}, true},
		{"history", func(ctx context.Context, selected any) (any, error) {
			return ReadSupplementalFanOutObligationHistoryForTest(ctx, selected, run)
		}, []FanOutObligationHistoryStorage(nil), false},
	}
}

func TestSupplementalFanOutStorageRetainsStrictNullAbsenceAndSeparateReadCutsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range supplementalFanOutStorageCases(uuid.NewString(), uuid.NewString()) {
				out, err := tc.read(ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(out, tc.empty) {
					t.Fatalf("%s changed original NULL/absence or returned provisional data: %+v %v", tc.name, out, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 10 || counts.Total.ReadCommits != 6 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical observations did not retain ten independent original read cuts: %+v", counts)
			}
		})
	}
}

func TestSupplementalFanOutStorageRefusesRawCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, selected any) {
		t.Helper()
		for _, tc := range supplementalFanOutStorageCases(uuid.NewString(), uuid.NewString()) {
			out, err := tc.read(ctx, selected)
			if err == nil || !reflect.DeepEqual(out, tc.empty) {
				t.Fatalf("%s supplied evidence without the original read owner: %+v %v", tc.name, out, err)
			}
		}
	}
	for _, selected := range []any{nil, struct{}{}, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		refuse(t, context.Background(), selected)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, f.store)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), f.store)
		})
	}
}
