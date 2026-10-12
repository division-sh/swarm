package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

func TestRetainedFanOutMetadataPortsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertRetainedFanOutMetadataUnavailable(t, selected)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, run := range []string{"", "invalid", uuid.Nil.String()} {
				if changed, err := SetRetainedFanOutCapsuleWireForTest(context.Background(), f.store, run, []byte("{}")); err == nil || changed != 0 {
					t.Fatalf("invalid run supplied capsule fault evidence: %d %v", changed, err)
				}
				if changed, err := SetRetainedFanOutCardinalityForTest(context.Background(), f.store, run, 4); err == nil || changed != 0 {
					t.Fatalf("invalid run supplied cardinality fault evidence: %d %v", changed, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertRetainedFanOutMetadataUnavailable(t, f.store)
		})
	}
}

func assertRetainedFanOutMetadataUnavailable(t *testing.T, selected any) {
	t.Helper()
	run := uuid.NewString()
	if changed, err := SetRetainedFanOutCapsuleWireForTest(context.Background(), selected, run, []byte("{}")); err == nil || changed != 0 {
		t.Fatalf("unavailable owner supplied capsule commit evidence: %d %v", changed, err)
	}
	if changed, err := SetRetainedFanOutCardinalityForTest(context.Background(), selected, run, 4); err == nil || changed != 0 {
		t.Fatalf("unavailable owner supplied cardinality commit evidence: %d %v", changed, err)
	}
	if got, err := ReadFanOutRunCapsuleWireForTest(context.Background(), selected, run); err == nil || got != nil {
		t.Fatalf("unavailable owner supplied capsule read evidence: %s %v", got, err)
	}
	if got, err := ReadFanOutRunClaimMetadataStorageForTest(context.Background(), selected, run); err == nil || got != (FanOutRunClaimMetadataStorage{}) {
		t.Fatalf("unavailable owner supplied claim/clock evidence: %+v %v", got, err)
	}
}
