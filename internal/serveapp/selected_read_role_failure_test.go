package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// This negative oracle supplies values, never SQL, a grant or a runtime. Native
// selected-store admission is exercised separately in storetest's both-store root.
type selectedRoleCleanupOracle struct{ failure error }

func (o selectedRoleCleanupOracle) InspectSnapshot(ctx context.Context, read func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(err, o.failure)
	}
	return errors.Join(read(ctx), o.failure)
}

func TestSelectedServedReadRoleCleanupNeverPublishesEvidence(t *testing.T) {
	failure := errors.New("inspection cleanup failure")
	for _, missing := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			if cancelled {
				cancel()
			}
			got, err := readServedSelectedRole(ctx, selectedRoleCleanupOracle{failure}, func(context.Context) (int, error) {
				if missing {
					return 0, sql.ErrNoRows
				}
				return 7, nil
			})
			cancel()
			if got != 0 || !errors.Is(err, failure) || (cancelled && !errors.Is(err, context.Canceled)) {
				t.Fatalf("failed inspection published evidence or lost cleanup: value=%d missing=%t cancelled=%t err=%v", got, missing, cancelled, err)
			}
		}
	}
	value, err := readServedSelectedRole(context.Background(), selectedRoleCleanupOracle{}, func(context.Context) (int, error) { return 0, sql.ErrNoRows })
	if value != 0 || err != sql.ErrNoRows {
		t.Fatalf("exact absence was not preserved: %d %v", value, err)
	}
	if _, err := readServedSelectedRole[int](context.Background(), nil, func(context.Context) (int, error) { t.Fatal("nil reader invoked read"); return 1, nil }); err == nil {
		t.Fatal("nil reader accepted")
	}
}
