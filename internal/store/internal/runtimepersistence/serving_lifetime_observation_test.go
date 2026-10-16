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

func servingLifetimeReadCases(run string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"metadata", func(ctx context.Context, owner any) (any, error) {
			return ReadServingLifetimeMetadataStorageForTest(ctx, owner, run)
		}, ServingLifetimeMetadataStorage{}, true},
		{"claim_owner", func(ctx context.Context, owner any) (any, error) {
			return ReadServingLifetimeClaimOwnerStorageForTest(ctx, owner, run)
		}, "", true},
		{"history", func(ctx context.Context, owner any) (any, error) {
			return CountServingLifetimeHistoryStorageForTest(ctx, owner, run)
		}, 0, false},
		{"receipt", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedPipelineReceiptOutcomeForTest(ctx, owner, run)
		}, "", true},
	}
}

func TestServingLifetimeReadPortsPreserveForeignAbsenceAndOtherFamiliesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			var total, history int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`, state.Identity.RunID).Scan(&total); err != nil || total == 0 {
				t.Fatalf("actual construction has no positive other-family control: %d %v", total, err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'`, state.Identity.RunID).Scan(&history); err != nil || history != 0 {
				t.Fatalf("constructor unexpectedly owns fan-out obligations: %d %v", history, err)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, run := range []string{state.Identity.RunID, uuid.NewString()} {
				for _, tc := range servingLifetimeReadCases(run) {
					got, err := tc.read(f.ctx, f.store)
					if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) {
						t.Fatalf("missing %s borrowed header/claim/receipt/history or changed strict absence: %+v %v", tc.name, got, err)
					}
				}
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 8 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 {
				t.Fatalf("independent observers changed original native cuts or committed absent metadata: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("serving observations changed complete storage/history/clocks")
			}
		})
	}
}

func TestServingLifetimeReadPortsRefuseRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, run string, cause error) {
		for _, tc := range servingLifetimeReadCases(run) {
			got, err := tc.read(ctx, owner)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s exposed serving evidence: %+v %v", tc.name, got, err)
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
