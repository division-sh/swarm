package adminpersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type DestructiveResetSQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	deliveries  *deliveryadapter.Adapter
	operationMu sync.Mutex
}

func NewDestructiveResetSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*DestructiveResetSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || schemaGuard == nil {
		return nil, errors.New("destructive reset requires SQLite backend and schema authority")
	}
	deliveries, err := deliveryadapter.NewAdapter(deliveryadapter.DialectSQLite)
	if err != nil {
		return nil, err
	}
	return &DestructiveResetSQLiteOwner{backend: backend, schemaGuard: schemaGuard, deliveries: deliveries}, nil
}

func (s *DestructiveResetSQLiteOwner) ReadResetInventory(ctx context.Context) (out destructivereset.Inventory, err error) {
	if err := s.schemaGuard(); err != nil {
		return out, err
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readResetInventoryTx(ctx, tx, s.deliveries)
		return err
	})
	return out, err
}

// Selected SQLite already owns exclusive process possession. This lease only
// excludes sibling reset operations and never holds a SQLite write transaction.
func (s *DestructiveResetSQLiteOwner) AcquireDestructiveReset(ctx context.Context) (destructivereset.LockLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := s.schemaGuard(); err != nil {
		return nil, false, err
	}
	if !s.operationMu.TryLock() {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		s.operationMu.Unlock()
		return nil, false, err
	}
	return &sqliteResetLease{owner: s}, true, nil
}

type sqliteResetLease struct {
	owner *DestructiveResetSQLiteOwner
	once  sync.Once
}

func (l *sqliteResetLease) Release(context.Context) error {
	l.once.Do(l.owner.operationMu.Unlock)
	return nil
}
