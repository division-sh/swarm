package runtime

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
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

func withoutSQLTxContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithValue(ctx, sqlTxContextKey{}, (*sql.Tx)(nil))
}

func dbExecContext(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	exec := func() (sql.Result, error) {
		if tx, ok := sqlTxFromContext(ctx); ok {
			return tx.ExecContext(ctx, query, args...)
		}
		return db.ExecContext(ctx, query, args...)
	}
	res, err := exec()
	if err != nil && shouldSQLDebugLog() {
		log.Printf("runtime.sql.exec error=%v query=%q args=%d", err, compactSQLSnippet(query), len(args))
	}
	return res, err
}

func dbQueryContext(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	exec := func() (*sql.Rows, error) {
		if tx, ok := sqlTxFromContext(ctx); ok {
			return tx.QueryContext(ctx, query, args...)
		}
		return db.QueryContext(ctx, query, args...)
	}
	rows, err := exec()
	if err != nil && shouldSQLDebugLog() {
		log.Printf("runtime.sql.query error=%v query=%q args=%d", err, compactSQLSnippet(query), len(args))
	}
	return rows, err
}

func dbQueryRowContext(ctx context.Context, db *sql.DB, query string, args ...any) *sql.Row {
	if tx, ok := sqlTxFromContext(ctx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}

func shouldSQLDebugLog() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_SQL_DEBUG")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func compactSQLSnippet(q string) string {
	q = strings.Join(strings.Fields(strings.TrimSpace(q)), " ")
	if len(q) > 240 {
		return q[:240] + "..."
	}
	return q
}
