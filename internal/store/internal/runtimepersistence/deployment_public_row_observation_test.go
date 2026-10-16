package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func deploymentPublicRowObservationCases(run string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"pin", func(ctx context.Context, selected any) (any, error) {
			return ReadPortfolioAccountResourcePinStorageForTest(ctx, selected, run)
		}, "", true},
		{"receivers", func(ctx context.Context, selected any) (any, error) {
			return CountAccountDeploymentReceiversStorageForTest(ctx, selected, run)
		}, 0, false},
		{"routes", func(ctx context.Context, selected any) (any, error) {
			return CountDeploymentRouteObligationsStorageForTest(ctx, selected, run)
		}, 0, false},
		{"delivered", func(ctx context.Context, selected any) (any, error) {
			return CountPortfolioDeliveredDeploymentRowsStorageForTest(ctx, selected, run)
		}, 0, false},
		{"all", func(ctx context.Context, selected any) (any, error) {
			return CountPortfolioAllDeploymentRowsStorageForTest(ctx, selected, run)
		}, 0, false},
	}
}

func TestDeploymentPublicRowWitnessesKeepFiveIndependentNativeCutsAndHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range deploymentPublicRowObservationCases(uuid.NewString()) {
				got, err := tc.read(f.ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) || tc.absentError && !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("absent %s borrowed storage or lost absence: %+v %v", tc.name, got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 5 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 {
				t.Fatalf("deployment witness collapsed cuts or wrote: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("deployment witness changed complete storage or lifecycle clocks")
			}
		})
	}
}

func TestDeploymentPublicRowWitnessesRefuseRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, run string, cause error) {
		for _, tc := range deploymentPublicRowObservationCases(run) {
			got, err := tc.read(ctx, owner)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			refuse(f.ctx, f.store, "invalid", nil)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			refuse(cancelled, f.store, state.Identity.RunID, context.Canceled)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, state.Identity.RunID, nil)
		})
	}
}
