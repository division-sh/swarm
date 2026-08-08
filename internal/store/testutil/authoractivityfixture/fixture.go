// Package authoractivityfixture exposes the selected-store story adapter only
// to tests that need to assemble an explicit transaction fixture.
package authoractivityfixture

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type stateKey struct{}

type state struct {
	tx        *sql.Tx
	mutation  *privateauthoractivity.Mutation
	finalized bool
}

func Begin(ctx context.Context, tx *sql.Tx, dialect Dialect) (context.Context, error) {
	privateDialect := privateauthoractivity.Dialect(dialect)
	mutation, err := privateauthoractivity.Begin(ctx, tx, privateDialect)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, stateKey{}, &state{tx: tx, mutation: mutation}), nil
}

func Finalize(ctx context.Context) error {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return fmt.Errorf("test author activity mutation is not active")
	}
	current.finalized = true
	return current.mutation.Finalize(ctx)
}

func Record(ctx context.Context, draft runtimeauthoractivity.Draft) error {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return fmt.Errorf("test author activity mutation is not active")
	}
	return current.mutation.Record(ctx, draft)
}

func PersistedOccurredAt(ctx context.Context, key string) (time.Time, bool, error) {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return time.Time{}, false, fmt.Errorf("test author activity mutation is not active")
	}
	return current.mutation.PersistedOccurredAt(ctx, key)
}

func PersistedAuthorSafeSummary(ctx context.Context, key string) (string, bool, error) {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return "", false, fmt.Errorf("test author activity mutation is not active")
	}
	return current.mutation.PersistedAuthorSafeSummary(ctx, key)
}

func Require(ctx context.Context) error {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return fmt.Errorf("test author activity mutation is not active")
	}
	return nil
}

func InMutation(ctx context.Context, tx *sql.Tx) bool {
	current, ok := fromContext(ctx)
	return ok && !current.finalized && current.tx == tx
}

func FinalizedMutation(ctx context.Context, tx *sql.Tx) bool {
	current, ok := fromContext(ctx)
	return ok && current.finalized && current.tx == tx
}

func Mutation(ctx context.Context) (runtimeauthoractivity.Mutation, bool) {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return nil, false
	}
	return current.mutation, true
}

func fromContext(ctx context.Context) (*state, bool) {
	if ctx == nil {
		return nil, false
	}
	current, ok := ctx.Value(stateKey{}).(*state)
	return current, ok && current != nil && current.mutation != nil && current.tx != nil
}
