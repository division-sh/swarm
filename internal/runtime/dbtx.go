package runtime

import (
	"context"
	"database/sql"
)

type sqlTxContextKey struct{}

func withSQLTxContext(ctx context.Context, tx *sql.Tx) context.Context {
	if tx == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlTxContextKey{}, tx)
}

func sqlTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(sqlTxContextKey{}).(*sql.Tx)
	return tx, ok && tx != nil
}

func dbExecContext(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	if tx, ok := sqlTxFromContext(ctx); ok {
		return tx.ExecContext(ctx, query, args...)
	}
	return db.ExecContext(ctx, query, args...)
}

func dbQueryContext(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	if tx, ok := sqlTxFromContext(ctx); ok {
		return tx.QueryContext(ctx, query, args...)
	}
	return db.QueryContext(ctx, query, args...)
}

func dbQueryRowContext(ctx context.Context, db *sql.DB, query string, args ...any) *sql.Row {
	if tx, ok := sqlTxFromContext(ctx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}
