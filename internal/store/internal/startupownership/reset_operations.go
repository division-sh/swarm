package startupownership

import (
	"context"
	"database/sql"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

func (s *postgresSession) AdmitResetOperation(ctx context.Context, req destructivereset.Request) (out destructivereset.Operation, err error) {
	err = s.lease.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.AdmitResetOperationTx(ctx, tx, req, false)
		return e
	})
	return
}
func (s *postgresSession) ReadResetOperation(ctx context.Context, id string) (out destructivereset.Operation, err error) {
	err = s.lease.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.ReadResetOperationTx(ctx, tx, id)
		return e
	})
	return
}
func (s *postgresSession) PendingResetOperations(ctx context.Context) (out []destructivereset.Operation, err error) {
	err = s.lease.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.PendingResetOperationsTx(ctx, tx)
		return e
	})
	return
}
func (s *postgresSession) AdvanceResetOperation(ctx context.Context, before, after destructivereset.Operation) error {
	return s.lease.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return adminpersistence.AdvanceResetOperationTx(ctx, tx, before, after, false)
	})
}

func (s *sqliteSession) AdmitResetOperation(ctx context.Context, req destructivereset.Request) (out destructivereset.Operation, err error) {
	err = s.owner.backend.RunTransaction(ctx, "record destructive reset operation", func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.AdmitResetOperationTx(ctx, tx, req, true)
		return e
	})
	return
}
func (s *sqliteSession) ReadResetOperation(ctx context.Context, id string) (out destructivereset.Operation, err error) {
	err = s.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.ReadResetOperationTx(ctx, tx, id)
		return e
	})
	return
}
func (s *sqliteSession) PendingResetOperations(ctx context.Context) (out []destructivereset.Operation, err error) {
	err = s.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = adminpersistence.PendingResetOperationsTx(ctx, tx)
		return e
	})
	return
}
func (s *sqliteSession) AdvanceResetOperation(ctx context.Context, before, after destructivereset.Operation) error {
	return s.owner.backend.RunTransaction(ctx, "record destructive reset operation", func(ctx context.Context, tx *sql.Tx) error {
		return adminpersistence.AdvanceResetOperationTx(ctx, tx, before, after, true)
	})
}
