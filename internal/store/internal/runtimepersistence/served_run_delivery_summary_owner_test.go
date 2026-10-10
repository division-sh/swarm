package runtimepersistence

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestServedRunDeliverySummaryRequiresOriginalInitializedOwner(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
		if result, err := ReadServedRunDeliverySummaryForTest(context.Background(), selected, uuid.NewString()); err == nil || result.RunID != "" {
			t.Fatalf("invalid summary owner accepted: %T/result=%+v/error=%v", selected, result, err)
		}
	}
}
