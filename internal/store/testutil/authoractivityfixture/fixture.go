// Package authoractivityfixture exposes the selected-store story adapter only
// to tests that need to assemble an explicit transaction fixture.
package authoractivityfixture

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type stateKey struct{}

type state struct {
	tx        *sql.Tx
	mutation  runtimeauthoractivity.Mutation
	raw       *privateauthoractivity.Mutation
	finalized bool
	managed   bool
}

func Begin(ctx context.Context, tx *sql.Tx, dialect Dialect) (context.Context, error) {
	privateDialect := privateauthoractivity.Dialect(dialect)
	mutation, err := privateauthoractivity.Begin(ctx, tx, privateDialect)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, stateKey{}, &state{tx: tx, mutation: mutation, raw: mutation}), nil
}

// WithAttempt registers protocol-owned activity for legacy test fixture
// delegates. The protocol, not this context adapter, finalizes the mutation.
func WithAttempt(ctx context.Context, attempt *mutationprotocol.Attempt, tx *sql.Tx) (context.Context, error) {
	if ctx == nil || attempt == nil || tx == nil {
		return nil, fmt.Errorf("test author activity attempt and transaction are required")
	}
	if err := attempt.WithSQL(ctx, func(_ context.Context, native *sql.Tx) error {
		if native != tx {
			return fmt.Errorf("test author activity transaction does not belong to its mutation attempt")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, stateKey{}, &state{tx: tx, mutation: attempt, managed: true}), nil
}

func Finalize(ctx context.Context) error {
	current, ok := fromContext(ctx)
	if !ok || current.finalized {
		return fmt.Errorf("test author activity mutation is not active")
	}
	if current.managed {
		return fmt.Errorf("protocol-owned test author activity must be finalized by its mutation runner")
	}
	current.finalized = true
	return current.raw.Finalize(ctx)
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
