package channelonboarding

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

type teardownReservation struct {
	replay *domain.TeardownOperation
	logout domain.Operation
}

func exactTeardownReplayTx(ctx context.Context, tx *sql.Tx, d dialect, req domain.ReserveTeardownRequest) (*domain.TeardownOperation, error) {
	existing, found, err := loadTeardownByRequestKey(ctx, tx, d, req.RequestKeyHash, true)
	if err != nil || !found {
		return nil, err
	}
	if !teardownRequestMatches(existing, req) {
		return nil, domain.ErrConflict
	}
	return &existing, nil
}

func prepareTeardownReservationTx(ctx context.Context, tx *sql.Tx, d dialect, req domain.ReserveTeardownRequest) (teardownReservation, error) {
	var prepared teardownReservation
	var err error
	prepared.replay, err = exactTeardownReplayTx(ctx, tx, d, req)
	if err != nil || prepared.replay != nil {
		return prepared, err
	}
	if req.Kind == domain.TeardownLogout {
		var found bool
		prepared.logout, found, err = loadOperation(ctx, tx, d, req.Logout.OperationID, true)
		if err != nil {
			return prepared, err
		}
		if !found {
			return prepared, domain.ErrNotFound
		}
		if prepared.logout.PrincipalID != req.PrincipalID {
			return prepared, domain.ErrConflict
		}
	}
	if err := channeldelivery.LockPrincipalTx(ctx, tx, req.PrincipalID, d == dialectPostgres); err != nil {
		return prepared, err
	}
	// Recheck a reservation committed while waiting for the principal fence.
	prepared.replay, err = exactTeardownReplayTx(ctx, tx, d, req)
	if err != nil || prepared.replay != nil {
		return prepared, err
	}
	if req.Kind == domain.TeardownLogout {
		if err := admitLogoutTarget(prepared.logout, req); err != nil {
			return prepared, err
		}
	}
	return prepared, requireNoConflictingLogoutTx(ctx, tx, req, prepared.logout)
}

func teardownRequestMatches(op domain.TeardownOperation, req domain.ReserveTeardownRequest) bool {
	if op.RequestHash != req.RequestHash || op.Kind != req.Kind || op.PrincipalID != req.PrincipalID ||
		op.Scope != req.Scope || op.ExpectedBindingRevision != req.ExpectedBindingRevision || op.ExpectedProofRevision != req.ExpectedProofRevision {
		return false
	}
	return op.Logout == nil && req.Logout == nil || op.Logout != nil && req.Logout != nil && *op.Logout == *req.Logout
}

func admitLogoutTarget(op domain.Operation, req domain.ReserveTeardownRequest) error {
	if op.Phase == domain.PhaseFailed || op.Phase == domain.PhaseRetired || op.PrincipalID != req.PrincipalID ||
		op.Interface.Normalized() != req.Scope.Interface.Normalized() || op.ValidateSessionAccount() != nil ||
		op.SessionAccount == (operatorchannel.SessionAccountAdmission{}) {
		return domain.ErrConflict
	}
	if op.Revision != req.Logout.OperationRevision {
		return domain.ErrRevisionConflict
	}
	if !req.Logout.MatchesOperation(op) {
		return domain.ErrConflict
	}
	return nil
}

// Reservation and retirement serialize with other channel mutations through
// the existing principal fence, after locking the exact onboarding parent.
func requireNoConflictingLogoutTx(ctx context.Context, tx *sql.Tx, req domain.ReserveTeardownRequest, target domain.Operation) error {
	rows, err := tx.QueryContext(ctx, teardownSelect+` WHERE phase NOT IN ('succeeded','failed') ORDER BY teardown_id`)
	if err != nil {
		return err
	}
	var pending []domain.TeardownOperation
	for rows.Next() {
		op, _, err := scanTeardownRow(rows)
		if err != nil {
			return errors.Join(err, rows.Close())
		}
		pending = append(pending, op)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, op := range pending {
		if req.Kind == domain.TeardownLogout {
			if op.Kind == domain.TeardownLogout {
				if op.Logout.Account.ConnectionID == target.SessionConnectionID {
					return domain.ErrConflict
				}
				continue
			}
			if teardownScopeIncludesOperation(op.Scope, target) {
				return domain.ErrConflict
			}
		}
		if op.Kind != domain.TeardownLogout {
			continue
		}
		owner := domain.Operation{Interface: op.Scope.Interface, Coordinate: op.Logout.Coordinate}
		if teardownScopeIncludesOperation(req.Scope, owner) {
			return domain.ErrConflict
		}
	}
	return nil
}

func teardownScopeIncludesOperation(scope domain.TeardownScope, op domain.Operation) bool {
	if scope.Interface.Validate() == nil {
		return scope.Interface.Normalized() == op.Interface.Normalized()
	}
	return scope.BundleHash == op.Coordinate.BundleHash && scope.ContextPublicationGeneration == op.Coordinate.ContextPublicationGeneration
}

func fenceLogoutConnectionTx(ctx context.Context, tx *sql.Tx, d dialect, target domain.Operation, now time.Time) (int64, int64, error) {
	rows, err := tx.QueryContext(ctx, d.bind(operationSelect+` WHERE session_connection_id=? ORDER BY operation_id`), target.SessionConnectionID)
	if err != nil {
		return 0, 0, err
	}
	var ids []string
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return 0, 0, errors.Join(err, rows.Close())
		}
		if op.PrincipalID != target.PrincipalID || op.SessionAccount != target.SessionAccount || op.Provider != target.Provider {
			return 0, 0, errors.Join(domain.ErrConflict, rows.Close())
		}
		ids = append(ids, op.OperationID)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, 0, err
	}
	var operations, activations int64
	for _, id := range ids {
		result, err := tx.ExecContext(ctx, d.bind(`UPDATE channel_onboarding_operations SET phase='retired',operation_revision=operation_revision+1,
			failure_code='logout_reserved',failure_message='Explicit logout reserved for this connection',updated_at=?,completed_at=COALESCE(completed_at,?)
			WHERE operation_id=? AND phase<>'retired'`), now, now, id)
		if err != nil {
			return 0, 0, err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return 0, 0, err
		}
		operations += count
		result, err = tx.ExecContext(ctx, d.bind(`UPDATE connected_channel_activations SET status='retired',activation_revision=activation_revision+1,
			retirement_reason='logout_reserved',retired_at=?,updated_at=? WHERE operation_id=? AND status='current'`), now, now, id)
		if err != nil {
			return 0, 0, err
		}
		count, err = result.RowsAffected()
		if err != nil {
			return 0, 0, err
		}
		activations += count
	}
	return operations, activations, nil
}

func decodeLogoutTarget(kind domain.TeardownKind, connection, raw sql.NullString) (*domain.SessionLogoutTarget, error) {
	if kind != domain.TeardownLogout {
		if connection.Valid || raw.Valid {
			return nil, fmt.Errorf("%w: non-logout teardown carries a private session target", domain.ErrConflict)
		}
		return nil, nil
	}
	var target domain.SessionLogoutTarget
	decoder := json.NewDecoder(strings.NewReader(raw.String))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return nil, fmt.Errorf("%w: malformed logout target", domain.ErrConflict)
	}
	canonical, err := json.Marshal(target)
	if decoder.Decode(new(any)) != io.EOF || target.Validate() != nil || !connection.Valid || target.Account.ConnectionID != connection.String || err != nil || string(canonical) != raw.String {
		return nil, fmt.Errorf("%w: contradictory logout target", domain.ErrConflict)
	}
	return &target, nil
}
