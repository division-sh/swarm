// Package runtimepipelinefixture contains transaction plumbing for tests that
// model selected-store commits without exposing those capabilities in runtime
// production packages.
package runtimepipelinefixture

import (
	"context"
	"database/sql"
)

type txKey struct{}
type connKey struct{}
type postCommitKey struct{}
type rollbackKey struct{}

type OwnerAction func(context.Context)

func WithSQLTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func SQLTx(ctx context.Context) (*sql.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	return tx, ok && tx != nil
}

func WithoutSQLTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, txKey{}, (*sql.Tx)(nil))
}

func WithSQLConn(ctx context.Context, conn *sql.Conn) context.Context {
	return context.WithValue(ctx, connKey{}, conn)
}

func SQLConn(ctx context.Context) (*sql.Conn, bool) {
	if ctx == nil {
		return nil, false
	}
	conn, ok := ctx.Value(connKey{}).(*sql.Conn)
	return conn, ok && conn != nil
}

func WithPostCommitActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return context.WithValue(ctx, postCommitKey{}, actions)
}

func WithRollbackActions(ctx context.Context, actions *[]OwnerAction) context.Context {
	return context.WithValue(ctx, rollbackKey{}, actions)
}

func QueuePostCommitAction(ctx context.Context, action OwnerAction) bool {
	actions, ok := ctx.Value(postCommitKey{}).(*[]OwnerAction)
	if !ok || actions == nil || action == nil {
		return false
	}
	*actions = append(*actions, action)
	return true
}

func QueueRollbackAction(ctx context.Context, action OwnerAction) bool {
	actions, ok := ctx.Value(rollbackKey{}).(*[]OwnerAction)
	if !ok || actions == nil || action == nil {
		return false
	}
	*actions = append(*actions, action)
	return true
}

func FlushPostCommitActions(actions []OwnerAction) {
	for _, action := range actions {
		if action != nil {
			action(context.Background())
		}
	}
}

func FlushRollbackActions(actions []OwnerAction) {
	for index := len(actions) - 1; index >= 0; index-- {
		if actions[index] != nil {
			actions[index](context.Background())
		}
	}
}
