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

type selectedCapabilityReadCase struct {
	name                 string
	read                 func(context.Context, any, string) (any, error)
	empty                any
	absent, postgresOnly bool
}

func selectedCapabilityReadCases() []selectedCapabilityReadCase {
	return []selectedCapabilityReadCase{
		{"provider_operations", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedProviderOperationsStorageForTest(ctx, owner, id, 1)
		}, 0, false, true},
		{"provider_capabilities", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedCompletionCapabilitiesStorageForTest(ctx, owner, id, "anthropic_api", 1, "owner", id)
		}, []SelectedCompletionCapabilityStorage(nil), false, true},
		{"preparation", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedPreparationStorageForTest(ctx, owner, id)
		}, SelectedPreparationStorage{}, true, false},
		{"startup_counts", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeStartupCountsStorageForTest(ctx, owner, id, id)
		}, SelectedClaudeStartupCounts{}, false, false},
		{"startup_surface", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeStartupSurfaceStorageForTest(ctx, owner, id, id)
		}, "", true, false},
		{"turn_surface", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeTurnCapabilityStorageForTest(ctx, owner, id, id)
		}, SelectedClaudeTurnCapabilityStorage{}, true, false},
		{"failure_diagnostics", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeAttemptDiagnosticsStorageForTest(ctx, owner, id)
		}, []SelectedClaudeAttemptDiagnosticStorage(nil), false, false},
		{"model_aliases", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedCompletionModelAliasStorageForTest(ctx, owner, id, "claude_cli")
		}, SelectedCompletionModelAliasStorage{}, false, false},
	}
}

func TestSelectedCapabilityReadPortsKeepIndependentCutsAndStrictAbsenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range selectedCapabilityReadCases() {
				got, err := tc.read(f.ctx, f.store, state.Identity.RunID)
				refused := backend == "sqlite" && tc.postgresOnly
				if (err != nil) != (tc.absent || refused) || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("%s borrowed unrelated construction or changed absence: %+v %v", tc.name, got, err)
				}
				if tc.absent && !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("%s changed exact missing-row error: %v", tc.name, err)
				}
			}
			wantBegun, wantCommitted := int64(8), int64(5)
			if backend == "sqlite" {
				wantBegun, wantCommitted = 6, 3
			}
			if counts := probe.Snapshot(); counts.Active != 0 || int64(counts.Total.Begun) != wantBegun || int64(counts.Total.ReadCommits) != wantCommitted || counts.Total.WriteCommits != 0 {
				t.Fatalf("capability evidence joined separate cuts or wrote storage: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("capability observations changed complete history or clocks")
			}
		})
	}
}

func TestSelectedCapabilityReadPortsRefuseRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, id string, cause error) {
		for _, tc := range selectedCapabilityReadCases() {
			got, err := tc.read(ctx, owner, id)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned capability evidence: %+v %v", tc.name, got, err)
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
			for _, tc := range selectedCapabilityReadCases() {
				if backend == "sqlite" && tc.postgresOnly {
					continue
				}
				got, err := tc.read(cancelled, f.store, state.Identity.RunID)
				if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("cancelled %s returned evidence: %+v %v", tc.name, got, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, state.Identity.RunID, nil)
		})
	}
}
