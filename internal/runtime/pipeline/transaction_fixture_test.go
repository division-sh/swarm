package pipeline

import (
	"context"
	"database/sql"
)

// These helpers model transaction state in test doubles only. Production
// runtime code has no transaction or callback authority in context.Context.
type testSQLTxContextKey struct{}
type testSQLConnContextKey struct{}
type testPostCommitActionsKey struct{}
type testRollbackActionsKey struct{}

type OwnerAction func(context.Context)

func WithPipelineSQLTxContext(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, testSQLTxContextKey{}, tx)
}

func PipelineSQLTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(testSQLTxContextKey{}).(*sql.Tx)
	return tx, ok && tx != nil
}

func sqlTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	return PipelineSQLTxFromContext(ctx)
}

func WithoutPipelineSQLTxContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, testSQLTxContextKey{}, (*sql.Tx)(nil))
}

func WithPipelineSQLConnContext(ctx context.Context, conn *sql.Conn) context.Context {
	return context.WithValue(ctx, testSQLConnContextKey{}, conn)
}

func PipelineSQLConnFromContext(ctx context.Context) (*sql.Conn, bool) {
	if ctx == nil {
		return nil, false
	}
	conn, ok := ctx.Value(testSQLConnContextKey{}).(*sql.Conn)
	return conn, ok && conn != nil
}

func withPipelinePostCommitActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return context.WithValue(ctx, testPostCommitActionsKey{}, actions)
}

func WithPipelinePostCommitActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return withPipelinePostCommitActions(ctx, actions)
}

func withPipelineRollbackActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return context.WithValue(ctx, testRollbackActionsKey{}, actions)
}

func WithPipelineRollbackActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return withPipelineRollbackActions(ctx, actions)
}

func queuePipelinePostCommitAction(ctx context.Context, fn OwnerAction) bool {
	actions, ok := ctx.Value(testPostCommitActionsKey{}).(*[]OwnerAction)
	if !ok || actions == nil || fn == nil {
		return false
	}
	*actions = append(*actions, fn)
	return true
}

func QueuePipelinePostCommitAction(ctx context.Context, fn OwnerAction) bool {
	return queuePipelinePostCommitAction(ctx, fn)
}

func queuePipelineTransactionPostCommitAction(ctx context.Context, fn OwnerAction) bool {
	return queuePipelinePostCommitAction(ctx, fn)
}

func queuePipelineRollbackAction(ctx context.Context, fn OwnerAction) bool {
	actions, ok := ctx.Value(testRollbackActionsKey{}).(*[]OwnerAction)
	if !ok || actions == nil || fn == nil {
		return false
	}
	*actions = append(*actions, fn)
	return true
}

func QueuePipelineRollbackAction(ctx context.Context, fn OwnerAction) bool {
	return queuePipelineRollbackAction(ctx, fn)
}

func flushPipelinePostCommitActions(actions []OwnerAction) {
	for _, action := range actions {
		if action != nil {
			action(context.Background())
		}
	}
}

func FlushPipelinePostCommitActions(actions []OwnerAction) {
	flushPipelinePostCommitActions(actions)
}

func flushPipelineRollbackActions(actions []OwnerAction) {
	for index := len(actions) - 1; index >= 0; index-- {
		if actions[index] != nil {
			actions[index](context.Background())
		}
	}
}

func FlushPipelineRollbackActions(actions []OwnerAction) {
	flushPipelineRollbackActions(actions)
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
