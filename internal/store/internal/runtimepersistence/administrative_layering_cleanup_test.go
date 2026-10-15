package runtimepersistence

import (
	"context"
	"errors"
	"testing"
)

func TestAdministrativeSurroundingCleanupPreservesEveryUnexpectedNativeError(t *testing.T) {
	rollback := errors.New("private fixture rollback marker")
	cleanup := errors.New("native rollback/connection cleanup failure")
	for _, row := range []struct {
		name string
		err  error
		want error
	}{
		{"only-marker", rollback, nil},
		{"exact-cancellation-pair", errors.Join(context.Canceled, rollback), context.Canceled},
		{"unexpected-cleanup", cleanup, cleanup},
		{"marker-plus-cleanup", errors.Join(rollback, cleanup), nil},
		{"cancel-marker-plus-cleanup", errors.Join(errors.Join(context.Canceled, rollback), cleanup), nil},
		{"unrelated-cancel-error", errors.Join(context.Canceled, cleanup), nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			want := row.want
			if want == nil && row.err != rollback {
				want = row.err
			}
			if got := administrativeSurroundingCloseError(row.err, rollback, context.Canceled); got != want {
				t.Fatalf("unexpected native error was hidden/reclassified: got=%v want=%v", got, want)
			}
		})
	}
}
