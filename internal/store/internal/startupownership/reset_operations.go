package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

func (s *postgresSession) AdmitResetOperation(ctx context.Context, req destructivereset.Request) (out destructivereset.Operation, err error) {
	err = s.lease.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = admitResetOperationTx(ctx, tx, req, false)
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
		out, e = admitResetOperationTx(ctx, tx, req, true)
		return e
	})
	return
}

func admitResetOperationTx(ctx context.Context, tx *sql.Tx, req destructivereset.Request, sqlite bool) (destructivereset.Operation, error) {
	previous, err := adminpersistence.LookupResetOperationTx(ctx, tx, req)
	if err != nil {
		return destructivereset.Operation{}, err
	}
	if previous != nil {
		return *previous, nil
	}
	plan, exists, err := loadSourceSetTx(ctx, tx, sqlite)
	if err != nil {
		return destructivereset.Operation{}, err
	}
	var snapshot *agenttopology.SourceSetPlan
	if exists {
		snapshot = &plan
	}
	return adminpersistence.AdmitResetOperationTx(ctx, tx, req, snapshot, sqlite)
}

func validateResetSourceSetTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, topology *agenttopology.SourceSetCommitRequest, sqlite bool) error {
	op, err := adminpersistence.ReadResetOperationTx(ctx, tx, req.OperationID)
	if err != nil {
		return err
	}
	current, exists, err := loadSourceSetTx(ctx, tx, sqlite)
	if err != nil {
		return err
	}
	if exists != (op.SourceSet != nil) || (exists && !reflect.DeepEqual(&current, op.SourceSet)) {
		return errors.New("reset source topology differs from its admitted snapshot")
	}
	if topology != nil && (op.SourceSet == nil || topology.ExpectedRevision != op.SourceSet.Revision) {
		return errors.New("reset topology mutation does not consume its admitted revision")
	}
	if req.Result.IncludeSourceArtifacts && op.SourceSet != nil && len(op.SourceSet.Sources) != 0 && topology == nil {
		return errors.New("reset source deletion requires its atomic topology mutation")
	}
	return nil
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
