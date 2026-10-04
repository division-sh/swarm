package postgres

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

// InspectSnapshot binds existing domain readers to one read-only transaction.
// The context cannot be used to acquire a new session or authorize mutations.
func (b *Backend) InspectSnapshot(ctx context.Context, inspect func(context.Context) error) error {
	if inspect == nil {
		return errors.New("postgres inspection callback is required")
	}
	if b.inspectionDialer != nil {
		return b.withInspectionIO(ctx, func(native context.Context, cleanup func() error) error {
			return b.RunReadTransaction(native, func(_ context.Context, tx *sql.Tx) (err error) {
				defer func() { err = errors.Join(err, cleanup()) }()
				return b.inspectTransaction(ctx, tx, inspect)
			})
		})
	}
	return b.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return b.inspectTransaction(ctx, tx, inspect)
	})
}

func (b *Backend) inspectTransaction(ctx context.Context, tx *sql.Tx, inspect func(context.Context) error) error {
	active := &atomic.Bool{}
	active.Store(true)
	defer active.Store(false)
	return inspect(context.WithValue(ctx, inspectionContextKey{}, inspectionSnapshot{backend: b, tx: tx, active: active}))
}

func (b *Backend) inspectionSQLContext(ctx context.Context) context.Context {
	if b.inspectionDialer != nil {
		return context.WithoutCancel(ctx)
	}
	return ctx
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
		return nil, errors.New("postgres inspection snapshot belongs to a different selected store")
	}
	if snapshot.active == nil || !snapshot.active.Load() {
		return nil, errors.New("postgres inspection snapshot is closed")
	}
	return snapshot.tx, nil
}

func (b *Backend) refuseInspectionMutation(ctx context.Context) error {
	tx, err := b.inspectionTransaction(ctx)
	if err != nil {
		return err
	}
	if tx != nil {
		return errors.New("postgres inspection snapshot does not authorize writes or connection acquisition")
	}
	return nil
}
