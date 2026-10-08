// Package authoractivityfixture exposes the selected-store story adapter only
// to tests that need to assemble an explicit transaction fixture.
package authoractivityfixture

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type stateKey struct{}

type state struct {
	tx       *sql.Tx
	mutation runtimeauthoractivity.Mutation
}

// WithAttempt registers protocol-owned activity for test delegates. Native
// settlement and the protocol, never this adapter, own the transaction lifetime.
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
	return context.WithValue(ctx, stateKey{}, &state{tx: tx, mutation: attempt}), nil
}

func Record(ctx context.Context, draft runtimeauthoractivity.Draft) error {
	current, ok := fromContext(ctx)
	if !ok {
		return fmt.Errorf("test author activity mutation is not active")
	}
	return current.mutation.Record(ctx, draft)
}

func PersistedOccurredAt(ctx context.Context, key string) (time.Time, bool, error) {
	current, ok := fromContext(ctx)
	if !ok {
		return time.Time{}, false, fmt.Errorf("test author activity mutation is not active")
	}
	return current.mutation.PersistedOccurredAt(ctx, key)
}

func Require(ctx context.Context) error {
	_, ok := fromContext(ctx)
	if !ok {
		return fmt.Errorf("test author activity mutation is not active")
	}
	return nil
}

func InMutation(ctx context.Context, tx *sql.Tx) bool {
	current, ok := fromContext(ctx)
	return ok && current.tx == tx
}

func Mutation(ctx context.Context) (runtimeauthoractivity.Mutation, bool) {
	current, ok := fromContext(ctx)
	if !ok {
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
