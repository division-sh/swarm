package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestChannelAnchorStorageObservationsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, struct{}{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
		assertChannelAnchorObservationRefusal(t, context.Background(), owner, nil)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			missing := uuid.NewString()
			if rows, err := ReadChannelAnchorCardsStorageForTest(ctx, fixture.store, missing, "stage_gate", "pending"); err != nil || len(rows) != 0 {
				t.Fatalf("absent card scope manufactured rows: %+v %v", rows, err)
			}
			if result, err := ReadChannelAnchorDecisionStorageForTest(ctx, fixture.store, missing); !errors.Is(err, sql.ErrNoRows) || result != (ChannelAnchorDecisionStorage{}) {
				t.Fatalf("absent card manufactured a verdict: %+v %v", result, err)
			}
			if result, found, err := ReadFirstSentSourceReceiptStorageForTest(ctx, fixture.store, missing); err != nil || found || result != "" {
				t.Fatalf("absent historical receipt manufactured a reference: %q %t %v", result, found, err)
			}
			if result, err := ReadStandingFlowStorageForTest(ctx, fixture.store, "missing-flow"); !errors.Is(err, sql.ErrNoRows) || result != (StandingFlowStorage{}) {
				t.Fatalf("absent standing flow manufactured owner facts: %+v %v", result, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertChannelAnchorObservationRefusal(t, cancelled, fixture.store, context.Canceled)
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertChannelAnchorObservationRefusal(t, ctx, fixture.store, nil)
		})
	}
}

func assertChannelAnchorObservationRefusal(t *testing.T, ctx context.Context, owner any, cause error) {
	t.Helper()
	missing := uuid.NewString()
	check := func(err error) {
		t.Helper()
		if err == nil || cause != nil && !errors.Is(err, cause) {
			t.Fatalf("unowned/failed read did not refuse with its cause: %v", err)
		}
	}
	rows, err := ReadChannelAnchorCardsStorageForTest(ctx, owner, missing, "stage_gate", "")
	check(err)
	if len(rows) != 0 {
		t.Fatal("failed card read returned evidence:", rows)
	}
	decision, err := ReadChannelAnchorDecisionStorageForTest(ctx, owner, missing)
	check(err)
	if decision != (ChannelAnchorDecisionStorage{}) {
		t.Fatal("failed decision read returned evidence:", decision)
	}
	reference, found, err := ReadFirstSentSourceReceiptStorageForTest(ctx, owner, missing)
	check(err)
	if found || reference != "" {
		t.Fatal("failed receipt read returned evidence:", reference, found)
	}
	standing, err := ReadStandingFlowStorageForTest(ctx, owner, "missing-flow")
	check(err)
	if standing != (StandingFlowStorage{}) {
		t.Fatal("failed standing read returned evidence:", standing)
	}
}
