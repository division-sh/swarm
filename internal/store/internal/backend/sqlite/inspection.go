package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
)

type inspectionContextKey struct{}

type inspectionSnapshot struct {
	backend *Backend
	tx      *sql.Tx
	active  *atomic.Bool
}

// InspectSnapshot binds existing domain readers to one read transaction. The
// selected inspection constructor additionally opens the database mode=ro.
func (b *Backend) InspectSnapshot(ctx context.Context, inspect func(context.Context) error) error {
	if inspect == nil {
		return errors.New("sqlite inspection callback is required")
	}
	return b.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		active := &atomic.Bool{}
		active.Store(true)
		defer active.Store(false)
		return inspect(context.WithValue(ctx, inspectionContextKey{}, inspectionSnapshot{backend: b, tx: tx, active: active}))
	})
}

func (b *Backend) inspectionTransaction(ctx context.Context) (*sql.Tx, error) {
	if ctx == nil {
		return nil, nil
	}
	snapshot, ok := ctx.Value(inspectionContextKey{}).(inspectionSnapshot)
	if !ok {
		return nil, nil
	}
	if snapshot.backend != b || snapshot.tx == nil {
		return nil, errors.New("sqlite inspection snapshot belongs to a different selected store")
	}
	if snapshot.active == nil || !snapshot.active.Load() {
		return nil, errors.New("sqlite inspection snapshot is closed")
	}
	return snapshot.tx, nil
}

func (b *Backend) refuseInspectionMutation(ctx context.Context) error {
	tx, err := b.inspectionTransaction(ctx)
	if err != nil {
		return err
	}
	if tx != nil {
		return errors.New("sqlite inspection snapshot does not authorize writes or connection acquisition")
	}
	return nil
}
