package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestWorkflowStateObservationsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, struct{}{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
		assertWorkflowStateObservationRefusal(t, context.Background(), owner, nil)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			missing := uuid.NewString()
			if entity, found, err := ReadWorkflowStateEntityStorageForTest(ctx, fixture.store, missing, "missing", "waiting"); entity != "" || found || err != nil {
				t.Fatalf("absent physical header manufactured entity evidence: %q/%t/%v", entity, found, err)
			}
			if count, err := CountRunStateEntityStorageForTest(ctx, fixture.store, missing, "waiting"); count != 0 || err != nil {
				t.Fatalf("absent physical state manufactured count evidence: %d/%v", count, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertWorkflowStateObservationRefusal(t, cancelled, fixture.store, context.Canceled)
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertWorkflowStateObservationRefusal(t, ctx, fixture.store, nil)
		})
	}
}

func assertWorkflowStateObservationRefusal(t *testing.T, ctx context.Context, owner any, cause error) {
	t.Helper()
	missing := uuid.NewString()
	entity, found, err := ReadWorkflowStateEntityStorageForTest(ctx, owner, missing, "missing", "waiting")
	if err == nil || found || entity != "" || cause != nil && !errors.Is(err, cause) {
		t.Fatalf("failed header read manufactured entity facts: %q/%t/%v", entity, found, err)
	}
	count, err := CountRunStateEntityStorageForTest(ctx, owner, missing, "waiting")
	if err == nil || count != 0 || cause != nil && !errors.Is(err, cause) {
		t.Fatalf("failed state read manufactured cardinality: %d/%v", count, err)
	}
}
