package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestPinnedAuthoredWireFaultRefusesInvalidRawAndClosedOwnersBothStores(t *testing.T) {
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField, RunID: uuid.NewString(), EntityID: uuid.NewString(), MutationID: uuid.NewString(), Field: "items"}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if changed, err := SetPinnedAuthoredMutationWireForTest(context.Background(), selected, source, []byte("null")); err == nil || changed != 0 {
			t.Fatalf("raw/uninitialized owner %T returned source write receipt: %d %v", selected, changed, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, invalid := range []fanoutobligation.SourceRef{
				{},
				{Kind: fanoutobligation.SourceEventPayloadField, EventID: uuid.NewString(), Field: "items"},
				{Kind: source.Kind, RunID: "invalid", EntityID: source.EntityID, MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: uuid.Nil.String(), MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: source.EntityID, MutationID: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: source.EntityID, MutationID: source.MutationID, Field: " "},
			} {
				if changed, err := SetPinnedAuthoredMutationWireForTest(context.Background(), f.store, invalid, []byte("null")); err == nil || changed != 0 {
					t.Fatalf("invalid source returned commit receipt: %+v %d %v", invalid, changed, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if changed, err := SetPinnedAuthoredMutationWireForTest(context.Background(), f.store, source, []byte("null")); err == nil || changed != 0 {
				t.Fatalf("closed owner returned source write receipt: %d %v", changed, err)
			}
		})
	}
}
