package authoractivity

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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
// the lock. A story reserves its batch here and acquires ordering at finalization.
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

func (m *Mutation) reserve(ctx context.Context) error {
	return m.order(ctx, false)
}

func (m *Mutation) lock(ctx context.Context) error {
	return m.order(ctx, true)
}

func (m *Mutation) order(ctx context.Context, acquire bool) error {
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
	// Native PostgreSQL may drain SQL separately; the original caller still wins.
	if err := scope.caller.Err(); err != nil {
		return err
	}
	if !acquire {
		if scope.story {
			return errors.New("author activity transaction already owns its story batch")
		}
		scope.story = true
		m.last = scope.last
		return nil
	}
	reused := scope.held
	if reused {
		m.last = scope.last
	} else {
		acquisition := transactiontest.BeginOrderingAcquisition(ctx)
		if err := m.acquire(ctx); err != nil {
			acquisition.End(err)
			return err
		}
		acquisition.End(nil)
		scope.last = m.last
		scope.held = true
	}
	if reused {
		transactiontest.OrderingReused(ctx)
	}
	return nil
}
