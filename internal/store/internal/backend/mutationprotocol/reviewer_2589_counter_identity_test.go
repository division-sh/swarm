package mutationprotocol

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func TestReviewer2589CounterReadUsesPhysicalRunIdentity(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		const runID = "abcdefab-cdef-4abc-8def-abcdefabcdef"
		counterProbeSchema(t, db, runID)
		readID := runID
		if dialect == privateactivity.DialectPostgres {
			readID = strings.ToUpper(runID)
		}
		result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, faultMatrixNative(db, nil, dialect), func(ctx context.Context, attempt *Attempt) (int, error) {
			if err := counterProbeInsert(ctx, attempt, runID, "one"); err != nil {
				return 0, err
			}
			var count int
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if err := FlushEventCountBeforeRead(ctx, tx, readID); err != nil {
					return err
				}
				return tx.QueryRowContext(ctx, `SELECT event_count FROM runs WHERE run_id=$1`, readID).Scan(&count)
			})
			return count, err
		})
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		counterProbeRead(t, db, runID, 1)
		if got, ok := result.Value(); !ok || got != 1 {
			t.Fatalf("acknowledged snapshot count=%d, present=%v; durable physical count=1", got, ok)
		}
	})
}
