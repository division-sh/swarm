package runforkrevision

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type revisionSQL interface {
	queryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// This wrapper counts actual revision-owner calls, not statements inferred from
// finalizer invocations. It neither wraps rows nor changes transaction ownership.
type observedRevisionSQL struct{ *sql.Tx }

func revisionQueryOwner(ctx context.Context, tx *sql.Tx) revisionSQL {
	if !transactiontest.RevisionEnabled(ctx) {
		return tx
	}
	return observedRevisionSQL{tx}
}

func (q observedRevisionSQL) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	transactiontest.CountRevisionSQL(ctx, transactiontest.RevisionExec)
	return q.Tx.ExecContext(ctx, query, args...)
}

func (q observedRevisionSQL) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	transactiontest.CountRevisionSQL(ctx, transactiontest.RevisionQuery)
	return q.Tx.QueryContext(ctx, query, args...)
}

func (q observedRevisionSQL) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	transactiontest.CountRevisionSQL(ctx, transactiontest.RevisionQueryRow)
	return q.Tx.QueryRowContext(ctx, query, args...)
}
