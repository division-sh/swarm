package authoractivity

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

type orderingTransactionKey struct{}

// The native settlement owner creates and closes this possession record. It
// carries no SQL operations or generation/run authorization to consumers.
type orderingTransaction struct {
	mu      sync.Mutex
	tx      *sql.Tx
	dialect Dialect
	caller  context.Context
	active  bool
	held    bool
	story   bool
	last    int64
}

// BindTransaction supplies transaction-local ordering possession without taking
// the lock. The existing first consumer still acquires it before domain work.
func BindTransaction(caller, ctx context.Context, tx *sql.Tx, dialect Dialect) (context.Context, func()) {
	if caller == nil {
		caller = context.Background()
	}
	if ctx == nil {
		ctx = caller
	}
	scope := &orderingTransaction{tx: tx, dialect: dialect, caller: caller, active: true}
	return context.WithValue(ctx, orderingTransactionKey{}, scope), func() {
		scope.mu.Lock()
		defer scope.mu.Unlock()
		scope.active = false
		scope.tx = nil
	}
}

func (m *Mutation) lock(ctx context.Context, story bool) error {
	if ctx == nil {
		return errors.New("author activity ordering context is required")
	}
	scope, ok := ctx.Value(orderingTransactionKey{}).(*orderingTransaction)
	if !ok || scope == nil {
		return errors.New("author activity requires native transaction ordering scope")
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if !scope.active {
		return sql.ErrTxDone
	}
	if scope.tx != m.tx || scope.dialect != m.dialect {
		return errors.New("author activity ordering scope belongs to another transaction or dialect")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope.held {
		// Native PostgreSQL may drain an admitted SQL operation with a separate
		// context. Reuse still observes the original caller's cancellation.
		if err := scope.caller.Err(); err != nil {
			return err
		}
		m.last = scope.last
	} else {
		if err := m.acquire(ctx); err != nil {
			return err
		}
		scope.last = m.last
		scope.held = true
	}
	if story {
		if scope.story {
			return errors.New("author activity transaction already owns its story batch")
		}
		scope.story = true
	}
	return nil
}
