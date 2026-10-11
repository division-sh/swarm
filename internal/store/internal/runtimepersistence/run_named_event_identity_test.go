package runtimepersistence

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestRunNamedEventIdentityRejectsUninitializedNativeOwners(t *testing.T) {
	for _, invalid := range []any{(*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
		if id, err := ReadRunNamedEventIdentityStorageForTest(context.Background(), invalid, uuid.NewString(), "fixture.ready"); err == nil || id != "" {
			t.Fatalf("uninitialized native owner %T supplied event identity: %q/%v", invalid, id, err)
		}
	}
}
